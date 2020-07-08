// Copied from https://github.com/buildkite/lifecycled/blob/master/autoscaling.go

package tagd

import (
	"fmt"
	"strings"
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
	tags        *TaggingConfig
	queue       *Queue
	autoscaling AutoscalingClient
	ec2Client   EC2Client
	log         *zap.Logger
}

// NewAutoscalingTagger returns a new AutoscalingTagger for an ASG
func NewAutoscalingTagger(asgName string, tags *TaggingConfig, queue *Queue, autoscaling AutoscalingClient, ec2Client EC2Client, logger *zap.Logger) *AutoscalingTagger {
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

func (l *AutoscalingTagger) Handle(InstanceID string) error {
	tags, err := l.buildTags(InstanceID)
	if err != nil {
		return err
	}
	err = l.tagVolumes(InstanceID, tags)
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

func (l *AutoscalingTagger) buildTags(instanceID string) (map[string]string, error) {
	l.log.Debug(fmt.Sprintf("Processing tags for instance %s", instanceID), zap.String("asg", l.asgName))
	// final product
	tagMap := make(map[string]string)

	svc := l.ec2Client
	input := ec2.DescribeTagsInput{
		MaxResults: aws.Int64(50), // We only do 50 tags, not sure if that's a sane default
		Filters: []*ec2.Filter{
			{
				Name: aws.String("resource-id"),
				Values: []*string{
					aws.String(instanceID),
				},
			},
		},
	}

	result, err := svc.DescribeTags(&input)
	if err != nil {
		return tagMap, err
	}
	// build tag map for easier handling
	instanceTagMap := make(map[string]string, len(result.Tags))
	for _, tagDesc := range result.Tags {
		instanceTagMap[*tagDesc.Key] = *tagDesc.Value
	}

	// process prefixed tags first as the statically configured ones should override
	for k, v := range instanceTagMap {
		for _, prefix := range l.tags.KeyPrefix {
			if strings.HasPrefix(strings.ToUpper(k), strings.ToUpper(prefix)) {
				tagMap[k] = v
			}
		}
	}
	// then add the statically configured ones.
	for staticK, staticV := range l.tags.Tags {
		tagMap[staticK] = staticV
	}
	return tagMap, nil
}

// Instances return all instance IDs belonging to the ASG
func (l *AutoscalingTagger) instances() ([]string, error) {
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
func (l *AutoscalingTagger) tagVolumes(instanceID string, tags map[string]string) error {
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

	err = l.TagResources(volumeIDs, tags)
	if err != nil {
		return err
	}

	l.log.Debug(fmt.Sprintf("Tagged %d volume(s) attached to %s", len(volumeIDs), instanceID))
	return nil
}

// TagResources takes a list of AWS resource IDs and tags them all with the provided tags
func (l *AutoscalingTagger) TagResources(resourceIDs []*string, tags map[string]string) error {
	ec2Tags := toEC2Tags(tags)
	svc := l.ec2Client

	tagInput := &ec2.CreateTagsInput{
		Resources: resourceIDs,
		Tags:      ec2Tags,
	}

	_, err := svc.CreateTags(tagInput)
	if err != nil {
		return err
	}
	return nil
}

func toEC2Tags(tags map[string]string) []*ec2.Tag {
	ec2Tags := make([]*ec2.Tag, len(tags))
	for k, v := range tags {
		ec2Tag := &ec2.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		}
		ec2Tags = append(ec2Tags, ec2Tag)
	}
	return ec2Tags
}
