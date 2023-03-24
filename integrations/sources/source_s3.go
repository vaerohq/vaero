/*
Copyright © 2023 Vaero Inc. (https://www.vaero.co/)
*/
package sources

import (
	"bytes"
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/vaerohq/vaero/log"
	"go.uber.org/zap"
)

type S3Source struct {
	Bucket string
	Prefix string
	Region string
}

// Read returns an event list
func (source *S3Source) Read() []string {
	eventList := []string{}

	// Load AWS config using the AWS SDK's default external configurations
	var sdkConfig aws.Config
	var err error
	if source.Region != "" {
		sdkConfig, err = config.LoadDefaultConfig(context.TODO(),
			config.WithRegion(source.Region))
	} else {
		sdkConfig, err = config.LoadDefaultConfig(context.TODO())
	}
	if err != nil {
		log.Logger.Error("Could not load AWS credentials", zap.String("Error", err.Error()))
		return eventList
	}

	s3Client := s3.NewFromConfig(sdkConfig)

	//bucketName := "vaero-go-test"
	//folderName := "2023/01/02"

	result, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(source.Bucket),
		Prefix: aws.String(source.Prefix),
	})

	if err != nil {
		log.Logger.Error("Couldn't list objects in bucket", zap.String("Bucket", "bucket"), zap.String("Error", err.Error()))
	} else {
		contents := result.Contents

		// Iterate over all objects in the Bucket with the Prefix
		for idx := range contents {
			objectKey := *contents[idx].Key // key for an object

			rawObject, err := s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
				Bucket: aws.String(source.Bucket),
				Key:    aws.String(objectKey),
			})
			if err != nil {
				log.Logger.Error("Couldn't get object", zap.String("Bucket", source.Bucket), zap.String("Key", objectKey),
					zap.String("Error", err.Error()))
			}

			buf := new(bytes.Buffer)
			buf.ReadFrom(rawObject.Body)
			fileContent := buf.String()

			newEvents := strings.Split(fileContent, "\n")
			eventList = append(eventList, newEvents...)
		}
	}

	return eventList
}

// Type returns either "pull" or "push"
func (source *S3Source) Type() string {
	return "pull"
}

func (source *S3Source) CleanUp() {

}
