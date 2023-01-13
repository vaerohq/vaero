/*
Copyright © 2023 Vaero Inc. (https://www.vaero.co/)
*/
package sinks

import (
	"time"

	"github.com/google/uuid"
	"github.com/vaerohq/vaero/capsule"
)

type Sink interface {
	Init(*SinkConfig)
	Flush(string, string, []string)
}

type SinkConfig struct {
	Id              uuid.UUID
	Type            string
	Prefix          map[string]*SinkBuffer
	FlushChan       chan capsule.Capsule          // channel for sending flushed data (received by FlushNode)
	TimeChan        chan capsule.SinkTimerCapsule // channel for sending timer expiration (received by SinkNode)
	BatchMaxBytes   int
	BatchMaxTime    int
	Bucket          string
	FilenamePrefix  string
	FilenameFormat  string
	Region          string
	TimestampKey    string
	TimestampFormat string
}

type SinkBuffer struct {
	BufferList []string
	Size       int
	LastFlush  time.Time
}
