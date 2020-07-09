// Copied from https://github.com/buildkite/lifecycled/blob/master/queue.go

package tagd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

const (
	longPollingWaitTimeSeconds = 20
)

// SQSClient for testing purposes
type SQSClient sqsiface.SQSAPI

// SNSClient for testing purposes
type SNSClient snsiface.SNSAPI

// Queue manages the SQS queue and SNS subscription.
type Queue struct {
	name            string
	url             string
	arn             string
	topicArn        string
	subscriptionArn string

	sqsClient SQSClient
	snsClient SNSClient
}

// NewQueue returns a new Queue.
func NewQueue(queueName, topicArn string, sqsClient SQSClient, snsClient SNSClient) (*Queue, error) {
	queue := &Queue{
		name:      queueName,
		topicArn:  topicArn,
		sqsClient: sqsClient,
		snsClient: snsClient,
	}
	// Only check for SNS topic existance if we want tagd to manage it
	if topicArn != "" {
		snsCtx, snsCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer snsCancel()
		if err := queue.TopicExists(snsCtx); err != nil {
			return nil, err
		}
	}
	sqsCtx, sqsCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer sqsCancel()
	qURL, err := queue.QueueExists(sqsCtx)
	if err != nil {
		return nil, err
	}
	queue.url = *qURL
	return queue, nil

}

// QueueExists returns the Queue URL and nil error if the sqs queue exists.
// If an error occurred, it returns nil with err.
func (q *Queue) QueueExists(ctx context.Context) (*string, error) {
	input := sqs.GetQueueUrlInput{
		QueueName: aws.String(q.name),
	}
	out, err := q.sqsClient.GetQueueUrlWithContext(ctx, &input)
	if err != nil {
		var aerr awserr.Error
		if errors.As(err, &aerr) {
			if aerr.Code() == sqs.ErrCodeQueueDoesNotExist {
				return nil, fmt.Errorf("queue %s doesn't exist", q.name)
			}
		}
		return nil, err
	}

	return out.QueueUrl, nil
}

// TopicExists returns nil error if the sns topic exists.
// If an error occurred, or the topic doesn't exist and error is returned.
func (q *Queue) TopicExists(ctx context.Context) error {
	input := &sns.GetTopicAttributesInput{
		TopicArn: aws.String(q.topicArn),
	}
	_, err := q.snsClient.GetTopicAttributesWithContext(ctx, input)
	if err != nil {
		var aerr awserr.Error
		if errors.As(err, &aerr) {
			if aerr.Code() == sns.ErrCodeNotFoundException {
				return fmt.Errorf("topic %s doesn't exist", q.topicArn)
			}
		}
		return err
	}
	return nil
}

// GetArn for the SQS queue.
func (q *Queue) getArn(ctx context.Context) (string, error) {
	if q.arn == "" {
		out, err := q.sqsClient.GetQueueAttributesWithContext(ctx, &sqs.GetQueueAttributesInput{
			AttributeNames: aws.StringSlice([]string{"QueueArn"}),
			QueueUrl:       aws.String(q.url),
		})
		if err != nil {
			return "", err
		}
		arn, ok := out.Attributes["QueueArn"]
		if !ok {
			return "", errors.New("No attribute QueueArn")
		}
		q.arn = aws.StringValue(arn)
	}
	return q.arn, nil
}

// Subscribe the queue to an SNS topic
func (q *Queue) Subscribe(ctx context.Context) error {
	if q.topicArn != "" {
		arn, err := q.getArn(ctx)
		if err != nil {
			return fmt.Errorf("failed to get queue ARN: %w", err)
		}
		out, err := q.snsClient.SubscribeWithContext(ctx, &sns.SubscribeInput{
			TopicArn: aws.String(q.topicArn),
			Protocol: aws.String("sqs"),
			Endpoint: aws.String(arn),
		})
		if err != nil {
			return fmt.Errorf("failed to subscribe to sqs: %w", err)
		}
		q.subscriptionArn = aws.StringValue(out.SubscriptionArn)
	}
	return nil
}

// GetMessages long polls for messages from the SQS queue.
func (q *Queue) GetMessages(ctx context.Context) ([]*sqs.Message, error) {
	out, err := q.sqsClient.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(q.url),
		MaxNumberOfMessages: aws.Int64(1),
		WaitTimeSeconds:     aws.Int64(longPollingWaitTimeSeconds),
		VisibilityTimeout:   aws.Int64(0),
	})
	if err != nil {
		// Ignore error if the context was cancelled (i.e. we are shutting down)
		if e, ok := err.(awserr.Error); ok && e.Code() == request.CanceledErrorCode {
			return nil, nil
		}
		return nil, err
	}
	return out.Messages, nil
}

// DeleteMessage from the queue.
func (q *Queue) DeleteMessage(ctx context.Context, receiptHandle string) error {
	_, err := q.sqsClient.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(q.url),
		ReceiptHandle: aws.String(receiptHandle),
	})
	if err != nil {
		if e, ok := err.(awserr.Error); ok && e.Code() == request.CanceledErrorCode {
			return nil
		}
		return err
	}
	return nil
}
