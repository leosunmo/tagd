package tagd

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/ryanuber/go-glob"
	"go.uber.org/zap"
)

// Config for the tagd Daemon.
type Config struct {
	TaggingConfigs []TaggingConfig `yaml:"tagConfig"`
	Backfill       bool
	SNSTopicARN    string
	SQSQueueName   string
}

// TaggingConfig to specify which ASGs to monitor and tag
type TaggingConfig struct {
	ASGName   string            `yaml:"asgName"`
	Tags      map[string]string `yaml:"tags,omitempty"`
	KeyPrefix []string          `yaml:"keyPrefix,omitempty"`
}

type Daemon struct {
	config     *Config
	queue      *Queue
	sqsClient  SQSClient
	snsClient  SNSClient
	asgClient  AutoscalingClient
	ec2Client  EC2Client
	asgTaggers map[string]*AutoscalingTagger
	log        *zap.Logger
}

// New creates a new tagd Daemon.
func New(config *Config, sess *session.Session, logger *zap.Logger) (*Daemon, error) {
	return NewDaemon(
		config,
		sqs.New(sess),
		sns.New(sess),
		autoscaling.New(sess),
		ec2.New(sess),
		logger,
	)
}

// NewDaemon creates a new Daemon.
func NewDaemon(
	config *Config,
	sqsClient SQSClient,
	snsClient SNSClient,
	asgClient AutoscalingClient,
	ec2Client EC2Client,
	logger *zap.Logger,
) (*Daemon, error) {
	daemon := &Daemon{
		config:    config,
		sqsClient: sqsClient,
		snsClient: snsClient,
		asgClient: asgClient,
		ec2Client: ec2Client,
		log:       logger,
	}

	queue, err := NewQueue(
		config.SQSQueueName,
		config.SNSTopicARN,
		sqsClient,
		snsClient,
	)
	if err != nil {
		return nil, err
	}
	daemon.queue = queue

	daemon.asgTaggers = make(map[string]*AutoscalingTagger)

	// Give it a very generous 1 minute to page through all ASGs
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*1)
	defer cancel()
	asgNameList, err := daemon.listAutoscalingGroupNames(ctx)
	if err != nil {
		return nil, err
	}

	// Iterate over the configured ASGs and the actual ASGs and check for glob matches (or exact matches)
	for _, conf := range config.TaggingConfigs {
		for _, asgName := range asgNameList {
			if glob.Glob(conf.ASGName, asgName) {
				daemon.addTagger(asgName, &conf)
			}
		}
	}
	return daemon, nil
}

func (d *Daemon) Start(ctx context.Context) error {
	d.log.Info("Starting Daemon")

	// If the SNS topic is not empty, let Tagd subscribe and enable asg notifications
	if d.config.SNSTopicARN != "" {
		d.log.Debug("Subscribing SQS queue to SNS topic", zap.String("topic", d.queue.topicArn))
		if err := d.queue.Subscribe(ctx); err != nil {
			return err
		}

		d.log.Debug("Enabling notifications to ASGs")
		for _, asg := range d.asgTaggers {
			if err := asg.EnableNotifications(); err != nil {
				d.log.Error(fmt.Sprintf("failed to enable notifications for ASG %s", asg.asgName), zap.Error(err))
			}
		}
	}

	if d.config.Backfill {
		d.log.Debug("Backfilling enabled, processing...")
		// Iterate over all the ASGs and tag existing disks before we start listening to the SQS queue
		for _, asg := range d.asgTaggers {
			d.log.Info(fmt.Sprintf("Processing existing disks for ASG %s", asg.asgName))
			instances, err := asg.instances()
			if err != nil {
				d.log.Error(fmt.Sprintf("failed to look up instances for ASG %s", asg.asgName), zap.Error(err))
				continue
			}
			for i, instance := range instances {
				d.log.Info(fmt.Sprintf("[%d/%d] Tagging existing instance %s", i+1, len(instances), instance))
				asg.Handle(instance)
			}
		}
	}
	d.log.Debug("Listening to SQS Queue...")
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			d.log.Debug("Polling SQS for messages", zap.String("queueURL", d.queue.url))
			messages, err := d.queue.GetMessages(ctx)
			if err != nil {
				d.log.Warn("Failed to get messages from SQS", zap.Error(err))
			}
			for _, m := range messages {
				var env Envelope
				var msg Message

				if err := d.queue.DeleteMessage(ctx, aws.StringValue(m.ReceiptHandle)); err != nil {
					d.log.Warn("Failed to delete SQS message", zap.Error(err))
				}

				// unmarshal outer layer
				if err := json.Unmarshal([]byte(*m.Body), &env); err != nil {
					d.log.Error("Failed to unmarshal envelope", zap.Error(err))
					continue
				}

				d.log.Debug("Received an SQS message",
					zap.String("type", env.Type),
					zap.String("subject", env.Subject),
				)

				// unmarshal inner layer
				if err := json.Unmarshal([]byte(env.Message), &msg); err != nil {
					d.log.Error("Failed to unmarshal autoscaling message", zap.Error(err))
					continue
				}

				if _, exists := d.asgTaggers[msg.GroupName]; !exists {
					d.log.Debug(fmt.Sprintf("Skipping message, %s not a managed ASG", msg.GroupName))
					continue
				}

				if msg.Event != "autoscaling:EC2_INSTANCE_LAUNCH" {
					d.log.Debug(fmt.Sprintf("Skipping autoscaling event, %s not ECS_INSTANCE_LAUNCH", msg.Event))
					continue
				}

				d.asgTaggers[msg.GroupName].Handle(msg.EC2InstanceID)
			}
		}
	}
}

func (d *Daemon) listAutoscalingGroupNames(ctx context.Context) ([]string, error) {
	asgList := []string{}
	input := &autoscaling.DescribeAutoScalingGroupsInput{}
	err := d.asgClient.DescribeAutoScalingGroupsPagesWithContext(ctx, input, func(page *autoscaling.DescribeAutoScalingGroupsOutput, lastPage bool) bool {
		for _, asg := range page.AutoScalingGroups {
			asgList = append(asgList, *asg.AutoScalingGroupName)
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return asgList, nil
}

func (d *Daemon) addTagger(asgName string, tags *TaggingConfig) {
	d.asgTaggers[asgName] = NewAutoscalingTagger(asgName, tags, d.queue, d.asgClient, d.ec2Client, d.log)
}
