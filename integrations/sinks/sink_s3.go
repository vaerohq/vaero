package sinks

import (
	"context"
	"fmt"
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
	s.Region = sinkConfig.FilenameFormat
}

// Flush writes data out to the sink immediately
func (s *S3Sink) Flush(filename string, prefix string, eventList []string) {

	sdkConfig, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		fmt.Println("Couldn't load default configuration. Have you set up your AWS account?")
		fmt.Println(err)
		return
	}
	s3Client := s3.NewFromConfig(sdkConfig)

	content := strings.Join(eventList, "\n")

	_, err = s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(filename),
		Body:   strings.NewReader(content),
	})
	if err != nil {
		log.Logger.Error("Couldn't upload file to S3", zap.String("msg", err.Error()))
	}
}
