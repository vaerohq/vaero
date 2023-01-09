package sinks

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/vaerohq/vaero/log"
	"go.uber.org/zap"
)

type S3Sink struct {
	Bucket string
	Region string
}

// Init initializes the sink
func (s *S3Sink) Init(sinkConfig *SinkConfig) {
	s.Bucket = sinkConfig.Bucket
	s.Region = sinkConfig.Region
}

// Flush writes data out to the sink immediately
func (s *S3Sink) Flush(filename string, prefix string, eventList []string) {

	// Load AWS config using the AWS SDK's default external configurations
	var sdkConfig aws.Config
	var err error
	if s.Region != "" {
		sdkConfig, err = config.LoadDefaultConfig(context.TODO(),
			config.WithRegion(s.Region))
	} else {
		sdkConfig, err = config.LoadDefaultConfig(context.TODO())
	}
	if err != nil {
		log.Logger.Error("Could not load AWS credentials", zap.String("Error", err.Error()))
		return
	}

	s3Client := s3.NewFromConfig(sdkConfig)

	// Create content
	content := strings.Join(eventList, "\n")

	// Full path
	filename = filepath.Join(prefix, filename)

	// Store to S3
	_, err = s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(filename),
		Body:   strings.NewReader(content),
	})
	if err != nil {
		log.Logger.Error("Could not upload file to S3", zap.String("Error", err.Error()))
	}
}
