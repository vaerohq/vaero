package gocdk

import "fmt"
import "log"

import "context"
import "strconv"

// for kinesis
import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	
)


 /*
 * Kinesis may be operated in two modes:
 * 1. a pull model (using GetRecord)
 * 2. a push model (from AWS -> Vaero) using SubscribeToShard
 * see discussion https://docs.aws.amazon.com/streams/latest/dev/building-consumers.html
 *
 * This client implements both, allowing for polling 1 shard, polling all shards, or listening to 1 shard
 *
 * If you configure this to poll/slisten only to a specific shard, it is your responsibility to
 * configure listeners for all available shards.
 *
 */


type KinesisService struct {
	client *kinesis.Client

	cursor *LocalFileCursor

	// configuration, all Services
	name string

	// configuration, AWS	
	awsRegion string

	// configuration, AWS service
	arn string
	shardID string
	kinesisAllShards bool
	kinesisDoListen bool
	consumerName string
	
	// cursor
	shardList []ShardInfo
	cursorRestart bool

	// initialization guard
	doneInitialization bool
}

// check if the provided config is valid
// required configuration: StreamName, ShardID
func (ks *KinesisService) CheckValidConfig(cc *ConnectorConfig) bool {
	pub := cc.GetPublic()

	if pub.Service != "kinesis" {
		log.Fatal("ERROR: config for ", pub.Service, " is not valid for kinesis")
	}

	if pub.KinesisDoListen {
		if pub.KinesisAllShards {
			log.Fatal("ERROR: config for kinesis specifies listening and all shards.  Please choose one.")
		}
		if pub.ShardID == "" {
			log.Fatal("ERROR: config for kinesis is missing required shard id for listening")
		}
		if pub.ConsumerName == "" {
			log.Fatal("ERROR: config for kinesis is missing required consumer name for listening")
		}
	
	} else {
		if pub.ARN == "" || (!pub.KinesisAllShards && pub.ShardID == "") {
			log.Fatal("ERROR: config for kinesis is missing arn or [shard id | all shards]")
		}
	}

	ks.kinesisDoListen = pub.KinesisDoListen
	ks.consumerName = pub.ConsumerName
	ks.kinesisAllShards = pub.KinesisAllShards
	ks.shardID = pub.ShardID
	ks.arn = pub.ARN
	ks.name = pub.Name

	ks.awsRegion = pub.AWSRegion

	return true
}

// load the cursor
func (ks *KinesisService) LoadCursor(forceRestart bool) {
	cursor_name := ks.name + "_kinesis"

	ks.cursor = NewLocalFileCursor(cursor_name)
	ks.cursor.RegisterType(ShardInfo{})
	ks.cursor.RegisterType([]ShardInfo{})


	if !forceRestart && ks.cursor.CheckIfSavedCursorExists() {
		ks.cursor.Deserialize()

	} else {
		// default cursor values
		ks.cursorRestart = true
	}
}

// save the cursor
func (ks *KinesisService) SaveCursor() {
	if ks.cursor == nil {
		ks.cursor = NewLocalFileCursor(ks.name + "_kinesis")
	}

	ks.cursor.Set("shardList", ks.shardList)
	ks.cursor.RegisterType(ShardInfo{})
	ks.cursor.RegisterType([]ShardInfo{})

	ks.cursor.Serialize()
}

type ShardInfo struct {
	ShardID string
	StartingSequenceNumber string

	MaxSequencePresent bool
	EndingSequenceNumber uint64
}


func (ks *KinesisService) Authorize() bool {

	// instantiate the kinesis client here
	cfg, _ := config.LoadDefaultConfig(context.TODO())
	cfg.Region = ks.awsRegion
	ks.client = kinesis.NewFromConfig(cfg)

	

	// if we are reading all shards, enumerate them
	// NB: shards may be closed if they have split; this apparently is only
	//     accidentally shared via the presence of an EndingSequenceNumber.
	//     which must be compared to our cursor to make sure we read everything before discarding a shard
	// elh TODO I can't figure out how to get this for just one shard, so wasteful in the single shard case
	shards, err := ks.client.ListShards(context.Background(), &kinesis.ListShardsInput{
		StreamARN: aws.String(ks.arn),
	})
	if err != nil {
		log.Fatal(err)
	}

	if ks.kinesisAllShards {
		ks.shardList = make([]ShardInfo, len(shards.Shards))
	} else {
		ks.shardList = make([]ShardInfo, 1)
	}

	for i, shard := range shards.Shards {
		if !ks.kinesisAllShards && *shard.ShardId != ks.shardID {
			continue
		}

		ks.shardList[i] = ShardInfo{
			ShardID: *shard.ShardId,
			StartingSequenceNumber: *shard.SequenceNumberRange.StartingSequenceNumber,
			MaxSequencePresent: false,
		}

		if  shard.SequenceNumberRange.EndingSequenceNumber != nil {
			ks.shardList[i].EndingSequenceNumber, err = strconv.ParseUint(*shard.SequenceNumberRange.EndingSequenceNumber, 10, 64)
			ks.shardList[i].MaxSequencePresent = true
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	ks.doneInitialization = true
	return true
}



func (ks *KinesisService) ReadStreamListen(sink StreamSink) {
	if !ks.doneInitialization {
		log.Fatal("ERROR: ReadStreaming called before Authorize")
	}
	if !ks.kinesisDoListen {
		log.Fatal("ERROR: ReadStreaming called when kinesisDoListen is false")
	}

	resp, err := ks.client.RegisterStreamConsumer(context.Background(), &kinesis.RegisterStreamConsumerInput{
        StreamARN: aws.String(ks.arn),
        ConsumerName: aws.String(ks.consumerName),
    })
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("INFO", "consumerARN", resp.Consumer.ConsumerARN)

	// NB: per https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/kinesis#Client.SubscribeToShard
	//     subscriptions last 5 minutes
	response, err := ks.client.SubscribeToShard(context.Background(), &kinesis.SubscribeToShardInput{
		ConsumerARN: resp.Consumer.ConsumerARN,
		ShardId:   aws.String(ks.shardID),
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("INFO", response)

	/*
	listener := client.Records().Subscribe(context.Background(), func(r events.Record) {
	for {
		listener 
	}
*/
	// elh TODO DeregisterStreamConsumer


}


// reads a stream then terminates when out of data
func (ks *KinesisService) ReadStream(sink StreamSink) {
	if (!ks.doneInitialization) {
		log.Fatal("ERROR: ReadStream called before Authorize")
	}
	
	if !ks.kinesisDoListen {
		ks.ReadStreamPoll(sink)
	} else {
		ks.ReadStreamListen(sink)
	}
}


// reads a stream via polling
func (ks *KinesisService) ReadStreamPoll(sink StreamSink) {
	iterator := kinesis.GetShardIteratorInput{
		StreamARN: aws.String(ks.arn),
	}

	ks.cursorRestart = true

	for shardidx, shardInfo := range ks.shardList {
		// fmt.Println("INFO: shard", i, shardInfo.ShardID)

		iterator.ShardId = aws.String(shardInfo.ShardID)
		iterator.StartingSequenceNumber = aws.String(shardInfo.StartingSequenceNumber)

		if ks.cursorRestart {
			iterator.ShardIteratorType = types.ShardIteratorTypeAtSequenceNumber
		} else {
			iterator.ShardIteratorType = types.ShardIteratorTypeAfterSequenceNumber
		}

		shardItr, err := ks.client.GetShardIterator(context.TODO(), &iterator)
		if err != nil {
			log.Fatal(err)
		}

		var itr *string = shardItr.ShardIterator

		// GetRecords, by design, may return [] even when there are unprocessed records
		// the right solution is to use a listener, not a poller; as a workaround, heuristic 10 calls in a row
		// with no data means we pause processing
		nullcount := 0
		nullcount_heuristic := 100
		max_seq := ""

		for {
			// Get the next batch of data from the Kinesis stream.
			data, err := ks.client.GetRecords(context.Background(), &kinesis.GetRecordsInput{
				StreamARN: aws.String(ks.arn),
				ShardIterator: itr,
			})
			if err != nil {
				log.Fatal(err)
			}

			if len(data.Records) > 0 {
				nullcount = 0
			} else {
				nullcount += 1
			}
		
			// sink the data
			for _, record := range data.Records {
				if max_seq == "" || max_seq < *record.SequenceNumber {
					max_seq = *record.SequenceNumber
				}

				out := Record{
					payload: string(record.Data),
				}
				sink.Write(out)
			}
		
			// Get the next shard iterator.
			itr = data.NextShardIterator
		
		
			if itr == nil || nullcount >= nullcount_heuristic {
				ks.shardList[shardidx].StartingSequenceNumber = max_seq
				break
			}
		}
	}
}

func has_work(ks KinesisService) bool {
	return true
}

func get_auth_header(ks KinesisService) map[string]string {
	return nil
}

func get_request_headers(ks KinesisService) map[string]string {
	return nil
}

func parse_response(ks KinesisService) {
	return
}



