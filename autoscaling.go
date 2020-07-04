// Copied from https://github.com/buildkite/lifecycled/blob/master/autoscaling.go

package tagd

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/aws/aws-sdk-go/service/autoscaling/autoscalingiface"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"
	"go.uber.org/zap"
)

// AutoscalingClient for testing purposes
type AutoscalingClient autoscalingiface.AutoScalingAPI

// EC2Client for testing purposes
type EC2Client ec2iface.EC2API

// Envelope ...
type Envelope struct {
	Type    string    `json:"Type"`
	Subject string    `json:"Subject"`
	Time    time.Time `json:"Time"`
	Message string    `json:"Message"`
}

// Service: AWS Auto Scaling
// Time: 2016-09-30T19:00:36.414Z
// RequestId: 4e6156f4-a9e2-4bda-a7fd-33f2ae528958
// Event: autoscaling:EC2_INSTANCE_LAUNCH
// AccountId: 123456789012
// AutoScalingGroupName: my-asg
// AutoScalingGroupARN: arn:aws:autoscaling:region:123456789012:autoScalingGroup...
// ActivityId: 4e6156f4-a9e2-4bda-a7fd-33f2ae528958
// Description: Launching a new EC2 instance: i-0598c7d356eba48d7
// Cause: At 2016-09-30T18:59:38Z a user request update of AutoScalingGroup constraints to ...
// StartTime: 2016-09-30T19:00:04.445Z
// EndTime: 2016-09-30T19:00:36.414Z
// StatusCode: InProgress
// StatusMessage:
// Progress: 50
// EC2InstanceId: i-0598c7d356eba48d7
// Details: {"Subnet ID":"subnet-id","Availability Zone":"zone"}

// Message ...
type Message struct {
	Time          time.Time `json:"Time"`
	GroupName     string    `json:"AutoScalingGroupName"`
	Event         string    `json:"Event"`
	Cause         string    `json:"Cause"`
	EC2InstanceID string    `json:"EC2InstanceId"`
}

// AutoscalingTagger monitors an ASG for events and processes them
type AutoscalingTagger struct {
	asgName     string
	tags        []map[string]string
	queue       *Queue
	autoscaling AutoscalingClient
	ec2Client   EC2Client
	log         *zap.Logger
}

// NewAutoscalingTagger returns a new AutoscalingTagger for an ASG
func NewAutoscalingTagger(asgName string, tags []map[string]string, queue *Queue, autoscaling AutoscalingClient, ec2Client EC2Client, logger *zap.Logger) *AutoscalingTagger {
	return &AutoscalingTagger{
		asgName:     asgName,
		queue:       queue,
		tags:        tags,
		autoscaling: autoscaling,
		ec2Client:   ec2Client,
		log:         logger,
	}
}

// Name returns a string describing the asg we're watching.
func (l *AutoscalingTagger) Name() string {
	return l.asgName
}

func (l *AutoscalingTagger) Handle(msg Message) error {
	err := l.TagVolumes(msg.EC2InstanceID)
	if err != nil {
		return err
	}
	return nil
}

func (l *AutoscalingTagger) EnableNotifications() error {
	l.log.Debug("Enabling SNS Notification", zap.String("asg", l.asgName))

	svc := l.autoscaling
	input := &autoscaling.PutNotificationConfigurationInput{
		AutoScalingGroupName: aws.String(l.asgName),
		NotificationTypes: []*string{
			aws.String("autoscaling:EC2_INSTANCE_LAUNCH"),
		},
		TopicARN: aws.String(l.queue.topicArn),
	}

	_, err := svc.PutNotificationConfiguration(input)
	if err != nil {
		return err
	}
	return nil
}

// Instances return all instance IDs belonging to the ASG
func (l *AutoscalingTagger) Instances() ([]string, error) {
	input := &autoscaling.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: aws.StringSlice([]string{l.asgName}),
		MaxRecords:            aws.Int64(100),
	}
	result, err := l.autoscaling.DescribeAutoScalingGroups(input)
	if err != nil {
		return []string{}, err
	}
	if len(result.AutoScalingGroups) > 1 {
		l.log.Warn(fmt.Sprintf("Instance lookup for ASG %s returned more than 1 ASG", l.asgName))
	}
	var instances []string
	for _, asg := range result.AutoScalingGroups {
		for _, instance := range asg.Instances {
			instances = append(instances, *instance.InstanceId)
		}
	}
	return instances, nil
}

// TagVolumes tags all volumes attached to instanceID with the configured tags for the AutoscalingTagger
func (l *AutoscalingTagger) TagVolumes(instanceID string) error {
	l.log.Info(fmt.Sprintf("Tagging disks attached to instance %s", instanceID), zap.String("asg", l.asgName))
	svc := l.ec2Client
	input := &ec2.DescribeVolumesInput{
		Filters: []*ec2.Filter{
			{
				Name: aws.String("attachment.instance-id"),
				Values: []*string{
					aws.String(instanceID),
				},
			},
		},
	}
	result, err := svc.DescribeVolumes(input)
	if err != nil {
		return err
	}

	if len(result.Volumes) == 0 {
		l.log.Debug(fmt.Sprintf("No volumes found on instance %s", instanceID))
		return nil
	}

	var volumeIDs []*string
	for _, vol := range result.Volumes {
		l.log.Debug(fmt.Sprintf("Found volume %s", *vol.VolumeId))
		volumeIDs = append(volumeIDs, vol.VolumeId)
	}

	volTags := l.buildTags()

	tagInput := &ec2.CreateTagsInput{
		Resources: volumeIDs,
		Tags:      volTags,
	}

	_, err = svc.CreateTags(tagInput)
	if err != nil {
		return err
	}
	l.log.Debug(fmt.Sprintf("Tagged %d volume(s) attached to %s", len(volumeIDs), instanceID))
	return nil
}

func (l *AutoscalingTagger) buildTags() []*ec2.Tag {
	ec2Tags := make([]*ec2.Tag, len(l.tags))
	for _, tagSet := range l.tags {
		for k, v := range tagSet {
			ec2Tag := &ec2.Tag{
				Key:   aws.String(k),
				Value: aws.String(v),
			}
			ec2Tags = append(ec2Tags, ec2Tag)
		}
	}
	return ec2Tags
}
