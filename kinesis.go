package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

func main2() {
	
	sess := session.Must(session.NewSession())
	client := kinesis.New(sess)

	// Get the shard ID for the specified stream and shard.
	shardID, err := client.GetShardID(client.GetShardIDInput{
		StreamName:             aws.String("my-stream"),
		StartingSequenceNumber: aws.String("0"),
	})	
	if err != nil {
		log.Fatal(err)
	}

	// Subscribe to the shard.
	resp, err := client.SubscribeToShard(&client.SubscribeToShardInput{
		StreamName:             aws.String("my-stream"),
		StartingSequenceNumber: aws.String("0"),
		ShardId:                shardID,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Get the shard iterator for the subscription.
	shardIterator := resp.ShardIterator

	// Create a new file to write the data to.
	f, err := os.Create("data.txt")
	if err != nil {
		log.Fatal(err)
	}

	// Read data from the shard iterator and write it to the file.
	for {
		data, err := client.GetRecords(&kinesis.GetRecordsInput{
			ShardIterator: shardIterator,
		})
		if err != nil {
			log.Fatal(err)
		}

		for _, record := range data.Records {
			fmt.Fprintf(f, "%s\n", record.Data)
		}

		// Get the next shard iterator.
		shardIterator = data.NextShardIterator

		// check if done
		if shardIterator == nil {
			break
		}
	}

	f.Close()
}
