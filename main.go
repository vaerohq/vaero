/*
Copyright © 2023 Vaero Inc. (https://www.vaero.co/)
*/
package main

import "fmt"

// import "github.com/vaerohq/vaero/cmd"


import (
	"github.com/vaerohq/vaero/gocdk"
)


func main() {
	// cmd.Execute()


	// run a kinesis stream, polling
	if false {
		fmt.Println("kinesis poll")
		config := gocdk.NewConnectorConfig().
			Service("kinesis").
			Name("kinesis stream 0_poll").
			AWSRegion("us-west-2").
			ARN("arn:aws:kinesis:us-west-2:272331482377:stream/datastream0").
			ShardID("shardId-000000000000").
			KinesisDoListen(false)

		httpConn := gocdk.NewHTTPConnector().
			Config(config)

		ks := httpConn.Earl()
		
		ks.CheckValidConfig(config)
		ks.Authorize()
		ks.LoadCursor(false)

		sink := gocdk.NewStdOutSink()

		ks.ReadStream(sink)
		ks.SaveCursor()
		fmt.Println("done")
	}

	// run a kinesis stream, listener
	if true {
		fmt.Println("kinesis listener")
		config := gocdk.NewConnectorConfig().
			Service("kinesis").
			Name("kinesis stream 0_listener").
			AWSRegion("us-west-2").
			ARN("arn:aws:kinesis:us-west-2:272331482377:stream/datastream0").
			ShardID("shardId-000000000000").
			KinesisDoListen(true).
			ConsumerName("vaero-consumer-0")

		httpConn := gocdk.NewHTTPConnector().
			Config(config)

		ks := httpConn.Earl()
		
		ks.CheckValidConfig(config)
		ks.Authorize()
		ks.LoadCursor(false)

		sink := gocdk.NewStdOutSink()

		ks.ReadStream(sink)
		ks.SaveCursor()
		fmt.Println("done")
	}



}
