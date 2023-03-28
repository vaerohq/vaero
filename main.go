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



	config := gocdk.NewConnectorConfig().
		Service("kinesis").
		Name("kinesis stream 0").
		AWSRegion("us-west-2").
		ARN("arn:aws:kinesis:us-west-2:272331482377:stream/datastream0").
		ShardID("shardId-000000000000").
		KinesisAllShards(false)


	httpConn := gocdk.NewHTTPConnector().
		Config(config)

	ks := httpConn.Earl()
	
	ks.CheckValidConfig(config)
	ks.Authorize()
	//ks.LoadCursor(ks.cursor, true)

	sink := gocdk.NewStdOutSink()

	ks.ReadStream(sink)
	ks.SaveCursor()
	fmt.Println("done")



}
