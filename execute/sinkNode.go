package execute

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lestrrat-go/strftime"
	"github.com/tidwall/gjson"
	"github.com/vaerohq/vaero/capsule"
	"github.com/vaerohq/vaero/integrations/sinks"
	"github.com/vaerohq/vaero/log"
	"github.com/vaerohq/vaero/settings"
	"go.uber.org/zap"
)

type SinkConfig struct {
	Id              uuid.UUID
	Type            string
	Prefix          map[string]*SinkBuffer
	FlushChan       chan capsule.Capsule          // channel for sending flushed data (received by FlushNode)
	TimeChan        chan capsule.SinkTimerCapsule // channel for sending timer expiration (received by SinkNode)
	BatchMaxBytes   int
	BatchMaxTime    int
	FilenamePrefix  string
	FilenameFormat  string
	TimestampKey    string
	TimestampFormat string
}

type SinkBuffer struct {
	BufferList []string
	Size       int
	LastFlush  time.Time
}

// initSinkNode performs initialize for the sink node
func initSinkNode(sinks map[uuid.UUID]*SinkConfig /*sinkTargets []uuid.UUID*/, taskGraph []OpTask, timeChan chan capsule.SinkTimerCapsule) {

	initSinksFromTaskGraph(sinks, taskGraph, timeChan)
}

// initSinks finds all the sinks in the task graph and initializes them
func initSinksFromTaskGraph(sinks map[uuid.UUID]*SinkConfig, taskGraph []OpTask, timeChan chan capsule.SinkTimerCapsule) {
	for _, v := range taskGraph {
		if v.Type == "sink" {
			// Create configuration for a sink
			sinks[v.Id] = &SinkConfig{Id: v.Id, Type: v.Op, Prefix: make(map[string]*SinkBuffer),
				FlushChan: make(chan capsule.Capsule, settings.DefChanBufferLen), TimeChan: timeChan,
				BatchMaxBytes: int(v.Args["batch_max_bytes"].(float64)), BatchMaxTime: int(v.Args["batch_max_time"].(float64)),
				FilenamePrefix: v.Args["filename_prefix"].(string), FilenameFormat: v.Args["filename_format"].(string),
				TimestampKey: v.Args["timestamp_key"].(string), TimestampFormat: strings.ToLower(v.Args["timestamp_format"].(string))}

			fmt.Printf("Sinkconfig %v\n", sinks[v.Id])

			// Create goroutine to flush to the sink
			go flushNode(sinks[v.Id])
		} else if v.Type == "branch" {
			for _, branch := range v.Branches {
				initSinksFromTaskGraph(sinks, branch, timeChan)
			}
		}
	}
}

// sinkBatch adds events to a sink buffer and flushes if needed
func sinkBatch(c *capsule.Capsule, sinks map[uuid.UUID]*SinkConfig) {

	// Identify sinkConfig
	sinkConfig := sinks[c.SinkId]
	eventList := c.EventList

	timeField := sinkConfig.TimestampKey
	prefixPat := sinkConfig.FilenamePrefix
	var layout string

	switch sinkConfig.TimestampFormat {
	case "rfc3339":
		layout = time.RFC3339
	default:
		layout = time.RFC3339
	}

	// Strftime formatter
	prefixFormatter, err := strftime.New(prefixPat, strftime.WithUnixSeconds('s'))
	if err != nil {
		log.Logger.Fatal(err.Error())
	}

	// For each event, distribute to correct buffer by using strftime to determine prefix
	for _, event := range eventList {

		// Get the timestamp and parse it to determine the file prefix
		timeString := gjson.Get(event, timeField)
		timestamp, err := time.Parse(layout, timeString.String())
		if err != nil {
			log.Logger.Fatal(err.Error())
		}
		prefix := prefixFormatter.FormatString(timestamp)

		// Access appropriate buffer based on prefix
		sinkBuffer, found := sinkConfig.Prefix[prefix]
		if !found {
			sinkBuffer = createSinkBuffer(sinkConfig, prefix) //&SinkBuffer{LastFlush: time.Now()}
			sinkConfig.Prefix[prefix] = sinkBuffer
		}

		// Append to selected buffer
		sinkAddToBuffer(sinkBuffer, sinkConfig, prefix, event)
	}
}

// sinkAddToBuffer adds an event to the buffer, and flushes if write out criteria is met
func sinkAddToBuffer(sinkBuffer *SinkBuffer, sinkConfig *SinkConfig, prefix string, event string) {

	// len gets the number of bytes in string, not the number of characters in the string
	if len(event)+sinkBuffer.Size <= sinkConfig.BatchMaxBytes {
		sinkBuffer.BufferList = append(sinkBuffer.BufferList, event)
		sinkBuffer.Size += len(event)
	} else {
		log.Logger.Info("Flush: MaxBytes")

		// Sends buffer to be flushed, and deletes buffer
		flushSinkBuffer(sinkConfig, prefix, sinkBuffer)

		// Create new buffer
		sinkBuffer = createSinkBuffer(sinkConfig, prefix)
		sinkConfig.Prefix[prefix] = sinkBuffer

		// Append to new buffer
		sinkBuffer.BufferList = append(sinkBuffer.BufferList, event)
		sinkBuffer.Size += len(event)
	}
}

func startSinkTimer(delay int, timeChan chan capsule.SinkTimerCapsule, tc capsule.SinkTimerCapsule) {
	time.Sleep(time.Second * time.Duration(delay))

	timeChan <- tc
}

func flushNode(sinkConfig *SinkConfig) {
	defer func() {
		log.Logger.Info("Closing sinkFlusher", zap.String("id", sinkConfig.Id.String()), zap.String("Type", sinkConfig.Type))
	}()

	// Choose sink type based on taskGraph
	var s sinks.Sink
	switch sinkConfig.Type {
	case "stdout":
		s = &sinks.StdoutSink{}
	case "s3":
		s = &sinks.S3Sink{}
	case "datadog":
		s = &sinks.DatadogSink{}
	case "elastic":
		s = &sinks.ElasticSink{}
	case "splunk":
		s = &sinks.SplunkSink{}
	default:
		log.Logger.Error("Unknown sink", zap.String("sink", sinkConfig.Type))
	}

	// Initialize sink
	s.Init()

	// Main loop
	for {
		event, ok := <-sinkConfig.FlushChan

		// Kill goroutine when the channel is closed
		if !ok {
			return
		}

		// Flush
		if len(event.EventList) > 0 {
			s.Flush(event.Prefix, event.EventList)
		}
	}
}

// closeSinks closes all the sinks
func closeSinks(snks map[uuid.UUID]*SinkConfig) {
	for _, sink := range snks {
		flushSinkBuffers(sink)
		close(sink.FlushChan)
	}
}

// flushSinkBuffers flushes all the buffers of a sink
func flushSinkBuffers(sink *SinkConfig) {
	for prefix, v := range sink.Prefix {
		flushSinkBuffer(sink, prefix, v)
	}
}

func flushSinkBuffer(sinkConfig *SinkConfig, prefix string, sinkBuffer *SinkBuffer) {
	// Flush buffered list to the sink
	sinkConfig.FlushChan <- capsule.Capsule{Prefix: prefix, EventList: sinkBuffer.BufferList}

	// Delete buffer
	sinkBuffer = nil
	delete(sinkConfig.Prefix, prefix)

	//fmt.Printf("Delete sinkbuffer for %v\n", prefix)
}

func createSinkBuffer(sinkConfig *SinkConfig, prefix string) *SinkBuffer {
	//fmt.Printf("createSinkBuffer for %v\n", prefix)

	sinkBuffer := &SinkBuffer{
		BufferList: []string{},
		Size:       0,
		LastFlush:  time.Now(),
	}

	go startSinkTimer(sinkConfig.BatchMaxTime, sinkConfig.TimeChan,
		capsule.SinkTimerCapsule{SinkId: sinkConfig.Id, Prefix: prefix, LastFlush: sinkBuffer.LastFlush})

	return sinkBuffer
}

func handleSinkTimer(tc capsule.SinkTimerCapsule, snks map[uuid.UUID]*SinkConfig) {

	sinkConfig := snks[tc.SinkId]
	sinkBuffer, found := snks[tc.SinkId].Prefix[tc.Prefix]

	// If the sinkBuffer has not been flushed since the time that this timer was send, then flush
	// Otherwise, ignore the timer
	if found && sinkBuffer.LastFlush == tc.LastFlush {
		log.Logger.Info("Flush: MaxTime")
		// Sends buffer to be flushed, and resets buffer
		flushSinkBuffer(sinkConfig, tc.Prefix, sinkBuffer)
	}
}
