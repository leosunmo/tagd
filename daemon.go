package tagd

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"go.uber.org/zap"
)

// Config for the tagd Daemon.
type Config struct {
	TaggingConfigs []TaggingConfig `yaml:"tagConfig"`
	SNSTopicARN    string
	SQSQueueName   string
}

// TaggingConfig to specify which ASGs to monitor and tag
type TaggingConfig struct {
	ASGName string            `yaml:"asgName"`
	Tags    map[string]string `yaml:"tags,omitempty"`
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
	for _, conf := range config.TaggingConfigs {
		daemon.addTagger(conf.ASGName, conf.Tags)
	}
	return daemon, nil
}

func (d *Daemon) addTagger(asgName string, tags map[string]string) {
	d.asgTaggers[asgName] = NewAutoscalingTagger(asgName, tags, d.queue, d.asgClient, d.ec2Client, d.log)
}

func (d *Daemon) Start(ctx context.Context) error {
	d.log.Info("Starting Daemon")
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

	// Iterate over all the ASGs and tag existing disks before we start listening to the SQS queue
	for _, asg := range d.asgTaggers {
		d.log.Info(fmt.Sprintf("Processing existing disks for ASG %s", asg.asgName))
		instances, err := asg.Instances()
		if err != nil {
			d.log.Error(fmt.Sprintf("failed to look up instances for ASG %s", asg.asgName), zap.Error(err))
			continue
		}
		for i, instance := range instances {
			d.log.Info(fmt.Sprintf("[%d/%d] Tagging existing instance %s", i+1, len(instances), instance))
			asg.TagVolumes(instance)
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

				d.asgTaggers[msg.GroupName].Handle(msg)
			}
		}
	}
}
