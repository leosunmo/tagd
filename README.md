# Tagd

Tagd is a simple service make sure your AWS Autoscaling group (ASG) instance's disks are tagged correctly.

Instances created by ASGs can inherit the tags from the ASG, but it does not travel down to the instances' disks (unless you use Launch Templates).

The usual solution is to set up notifications on your ASGs and have something like a Lambda that tags the disks upon ECS_INSTANCE_LAUNCH events. This works fine most of the time.

If you don't want to use Lambdas or you can't easily set up SNS/SQS notifications on the ASGs, Tagd will do all of it for you.

An example is if you use [Kops](https://github.com/kubernetes/kops/). It currently doesn't support creating notificaions on the auto-generated ASGs for the Kubernetes masters and node groups. Tagd will add them for you.

## Usage
Config file example:
```yaml
tagConfig:
  - asgName: "my-asg"
    tags:
      KubernetesCluster: the-test-cluster
      "corp:department": development
      cool-tags: "yes"
  - asgName: "my-other-asg"
    tags:
      elasticsearch: "website-search"
      "corp:department": sales
```

Compile and run:
```
make build
bin/tagd -l info --sqs-queue-name asg-scaling-events --sns-topic-arn 'arn:aws:sns:us-west-2:1234567890:asg-scaling-events'
```

## TODO
- [ ] Add other handlers, for example tagging Kubernetes PVCs
- [ ] Make sns/sqs per-asg in config file
- [ ] tests using mocks