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
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	if err := queue.TopicExists(ctx); err != nil {
		return nil, err
	}
	ctx, _ = context.WithTimeout(context.Background(), 10*time.Second)
	if err := queue.QueueExists(ctx); err != nil {
		return nil, err
	}

	return queue, nil

}

// QueueExists returns true and nil error if the sqs queue exists.
// If an error occurred, it returns false with err.
func (q *Queue) QueueExists(ctx context.Context) error {
	input := sqs.GetQueueUrlInput{
		QueueName: aws.String(q.name),
	}
	_, err := q.sqsClient.GetQueueUrlWithContext(ctx, &input)
	if err != nil {
		var aerr awserr.Error
		if errors.As(err, &aerr) {
			if aerr.Code() == sqs.ErrCodeQueueDoesNotExist {
				return fmt.Errorf("queue %s doesn't exist", q.name)
			}
		}
		return err
	}
	return nil
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
func (q *Queue) getArn() (string, error) {
	if q.arn == "" {
		out, err := q.sqsClient.GetQueueAttributes(&sqs.GetQueueAttributesInput{
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
func (q *Queue) Subscribe() error {
	arn, err := q.getArn()
	if err != nil {
		return err
	}
	out, err := q.snsClient.Subscribe(&sns.SubscribeInput{
		TopicArn: aws.String(q.topicArn),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(arn),
	})
	if err != nil {
		return err
	}
	q.subscriptionArn = aws.StringValue(out.SubscriptionArn)
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

// Unsubscribe the queue from the SNS topic.
func (q *Queue) Unsubscribe() error {
	_, err := q.snsClient.Unsubscribe(&sns.UnsubscribeInput{
		SubscriptionArn: aws.String(q.subscriptionArn),
	})
	return err
}

// Delete the SQS queue.
func (q *Queue) Delete() error {
	_, err := q.sqsClient.DeleteQueue(&sqs.DeleteQueueInput{
		QueueUrl: aws.String(q.url),
	})
	if err != nil {
		// Ignore error if queue does not exist (which is what we want)
		if e, ok := err.(awserr.Error); !ok || e.Code() != sqs.ErrCodeQueueDoesNotExist {
			return err
		}
	}
	return nil
}
