package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/leosunmo/tagd"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v2"
)

func main() {

	fs := pflag.NewFlagSet("default", pflag.ContinueOnError)
	fs.StringP("level", "l", "info", "log level: debug, info, warn, error or panic")
	fs.Bool("backfill", false, "Enable backfilling tags of existing resources")
	fs.String("config", "./config.yaml", "Configuration file for ASG Tagging")
	fs.String("sqs-queue-name", "", "Name of SQS queue to monitor for ASG events")
	fs.String("sns-topic-arn", "", "If not empty, tagd will set up ASG Notification and subscribe SQS to this SNS topic")

	// parse flags
	err := fs.Parse(os.Args[1:])
	switch {
	case err == pflag.ErrHelp:
		os.Exit(0)
	case err != nil:
		fmt.Fprintf(os.Stderr, "Error: %s\n\n", err.Error())
		fs.PrintDefaults()
		os.Exit(2)
	}

	// bind flags and environment variables
	viper.BindPFlags(fs)
	viper.SetEnvPrefix("TAGD")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	// configure logging
	logger, _ := initZap(viper.GetString("level"))
	defer logger.Sync()
	stdLog := zap.RedirectStdLog(logger)
	defer stdLog()

	snsCfg := viper.GetString("sns-topic-arn")
	sqsCfg := viper.GetString("sqs-queue-name")

	if sqsCfg == "" {
		fmt.Println("Please provide --sqs-queue-name")
		fs.PrintDefaults()
		os.Exit(1)
	}

	config, err := readConfigFile(viper.GetString("config"))
	if err != nil {
		fmt.Printf("failed to parse config file, %s\n", err.Error())
		fs.PrintDefaults()
		os.Exit(1)
	}

	config.Backfill = viper.GetBool("backfill")

	config.SNSTopicARN = snsCfg
	config.SQSQueueName = sqsCfg

	// Create AWS credentials
	sess, err := session.NewSession()
	if err != nil {
		logger.Fatal("Failed to create new aws session", zap.Error(err))
	}

	d, err := tagd.New(config, sess, logger)
	if err != nil {
		logger.Fatal("failed to create daemon", zap.Error(err))
	}

	sigs := make(chan os.Signal)
	defer close(sigs)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigs)

	// Create an execution context for the daemon that can be cancelled on OS signal
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for signal := range sigs {
			logger.Info(fmt.Sprintf("Received signal %s: shutting down...", signal.String()))
			cancel()
			break
		}
	}()

	if err := d.Start(ctx); err != nil && err != context.Canceled {
		logger.Error("Daemon failed", zap.Error(err))
	} else {
		logger.Info("Tagd Daemon stopped")
	}

}

func readConfigFile(path string) (*tagd.Config, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}

	yamlFile, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var config *tagd.Config

	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		return nil, err
	}
	return config, nil

}

func initZap(logLevel string) (*zap.Logger, error) {
	level := zap.NewAtomicLevelAt(zapcore.InfoLevel)
	switch logLevel {
	case "debug":
		level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	case "info":
		level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	case "warn":
		level = zap.NewAtomicLevelAt(zapcore.WarnLevel)
	case "error":
		level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)
	case "fatal":
		level = zap.NewAtomicLevelAt(zapcore.FatalLevel)
	case "panic":
		level = zap.NewAtomicLevelAt(zapcore.PanicLevel)
	}

	zapEncoderConfig := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	zapConfig := zap.Config{
		Level:       level,
		Development: false,
		Sampling: &zap.SamplingConfig{
			Initial:    100,
			Thereafter: 100,
		},
		Encoding:         "json",
		EncoderConfig:    zapEncoderConfig,
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}

	return zapConfig.Build()
}
