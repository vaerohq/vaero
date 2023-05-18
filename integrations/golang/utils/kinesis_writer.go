package main

import (
    "fmt"
    "os"
	"time"
)

import (
	"context"
    "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/kinesis"
)

func main() {

	streamName := "datastream0"
	region := "us-west-2"


	cfg, _ := config.LoadDefaultConfig(context.TODO())
	cfg.Region = region
	client := kinesis.NewFromConfig(cfg)


    
	/*
	// create a stream.
    createStreamInput := &kinesis.CreateStreamInput{
        StreamName: aws.String(streamName),
        ShardCount: aws.Int32(2),
    }
    _, err := client.CreateStream(context.TODO(), createStreamInput)
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }
	*/

    // Put some test data into the stream.
	currentTime := time.Now()
    for i := 0; i < 100; i++ {
        putRecordInput := &kinesis.PutRecordInput{
            StreamName: aws.String(streamName),
            Data:      []byte(fmt.Sprintf("%s record %d", currentTime, i)),
			PartitionKey: aws.String(fmt.Sprintf("%d", i)),
        }
        _, err := client.PutRecord(context.TODO(), putRecordInput)
        if err != nil {
            fmt.Println(err)
            os.Exit(1)
        }
    }

    fmt.Println("exiting")
}
