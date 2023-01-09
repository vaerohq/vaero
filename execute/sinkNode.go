package execute

import (
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

// initSinkNode performs initialize for the sink node
func initSinkNode(snks map[uuid.UUID]*sinks.SinkConfig /*sinkTargets []uuid.UUID*/, taskGraph []OpTask, timeChan chan capsule.SinkTimerCapsule) {

	initSinksFromTaskGraph(snks, taskGraph, timeChan)
}

// initSinks finds all the sinks in the task graph and initializes them
func initSinksFromTaskGraph(snks map[uuid.UUID]*sinks.SinkConfig, taskGraph []OpTask, timeChan chan capsule.SinkTimerCapsule) {
	for _, v := range taskGraph {
		if v.Type == "sink" {
			// Set timestamp format
			var timestampFormat string
			switch strings.ToLower(v.Args["timestamp_format"].(string)) {
			case "rfc3339":
				timestampFormat = time.RFC3339
			case "unix":
				timestampFormat = time.UnixDate
			default:
				timestampFormat = time.RFC3339
			}

			// Create configuration for a sink
			snks[v.Id] = &sinks.SinkConfig{Id: v.Id, Type: v.Op, Prefix: make(map[string]*sinks.SinkBuffer),
				FlushChan: make(chan capsule.Capsule, settings.DefChanBufferLen), TimeChan: timeChan,
				BatchMaxBytes: int(v.Args["batch_max_bytes"].(float64)), BatchMaxTime: int(v.Args["batch_max_time"].(float64)),
				Bucket:         v.Args["bucket"].(string),
				FilenamePrefix: v.Args["filename_prefix"].(string), FilenameFormat: v.Args["filename_format"].(string),
				Region:       v.Args["region"].(string),
				TimestampKey: v.Args["timestamp_key"].(string), TimestampFormat: timestampFormat}

			//fmt.Printf("Sinkconfig %v\n", snks[v.Id])

			// Create goroutine to flush to the sink
			go flushNode(snks[v.Id])
		} else if v.Type == "branch" {
			for _, branch := range v.Branches {
				initSinksFromTaskGraph(snks, branch, timeChan)
			}
		}
	}
}

// sinkBatch adds events to a sink buffer and flushes if needed
func sinkBatch(c *capsule.Capsule, sinks map[uuid.UUID]*sinks.SinkConfig) {

	// Identify sinkConfig
	sinkConfig := sinks[c.SinkId]
	eventList := c.EventList

	timeField := sinkConfig.TimestampKey
	prefixPat := sinkConfig.FilenamePrefix
	layout := sinkConfig.TimestampFormat

	// Strftime formatter
	prefixFormatter, err := strftime.New(prefixPat, strftime.WithUnixSeconds('s'))
	if err != nil {
		log.Logger.Error("Failed to initialize strftime", zap.String("Error", err.Error()))
		return
	}

	// For each event, distribute to correct buffer by using strftime to determine prefix
	for _, event := range eventList {

		// Get the timestamp and parse it to determine the file prefix
		timeString := gjson.Get(event, timeField)
		timestamp, err := time.Parse(layout, timeString.String())
		if err != nil {
			log.Logger.Error("Failed to parse timestamp", zap.String("Error", err.Error()))
			continue
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
func sinkAddToBuffer(sinkBuffer *sinks.SinkBuffer, sinkConfig *sinks.SinkConfig, prefix string, event string) {

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

func flushNode(sinkConfig *sinks.SinkConfig) {
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
		return
	}

	// Initialize sink
	s.Init(sinkConfig)

	// Main loop
	for {
		event, ok := <-sinkConfig.FlushChan

		// Kill goroutine when the channel is closed
		if !ok {
			return
		}

		// Flush
		if len(event.EventList) > 0 {
			s.Flush(event.Filename, event.Prefix, event.EventList)
		}
	}
}

// closeSinks closes all the sinks
func closeSinks(snks map[uuid.UUID]*sinks.SinkConfig) {
	for _, sink := range snks {
		flushSinkBuffers(sink)
		close(sink.FlushChan)
	}
}

// flushSinkBuffers flushes all the buffers of a sink
func flushSinkBuffers(sink *sinks.SinkConfig) {
	for prefix, v := range sink.Prefix {
		flushSinkBuffer(sink, prefix, v)
	}
}

func flushSinkBuffer(sinkConfig *sinks.SinkConfig, prefix string, sinkBuffer *sinks.SinkBuffer) {
	// Generate filename
	filenameFormatter, err := strftime.New(sinkConfig.FilenameFormat, strftime.WithUnixSeconds('s'))
	if err != nil {
		log.Logger.Error("Could not create stfrtime formatter for flushing sink", zap.String("Error", err.Error()))
		return
	}
	layout := sinkConfig.TimestampFormat

	var filename string
	if len(sinkBuffer.BufferList) > 0 {
		lastEvent := sinkBuffer.BufferList[len(sinkBuffer.BufferList)-1]

		// Get the timestamp and parse it to determine the filename
		timeString := gjson.Get(lastEvent, sinkConfig.TimestampKey)
		timestamp, err := time.Parse(layout, timeString.String())
		if err != nil {
			log.Logger.Error("Could not parse timestamp", zap.String("Timestamp_key", sinkConfig.TimestampKey),
				zap.String("Error", err.Error()))
			filename = uuid.New().String()
		} else {
			filename = filenameFormatter.FormatString(timestamp)
		}
	} else {
		filename = uuid.New().String()
	}

	// Flush buffered list to the sink
	sinkConfig.FlushChan <- capsule.Capsule{Filename: filename, Prefix: prefix, EventList: sinkBuffer.BufferList}

	// Delete buffer
	sinkBuffer = nil
	delete(sinkConfig.Prefix, prefix)

	//fmt.Printf("Delete sinkbuffer for %v\n", prefix)
}

func createSinkBuffer(sinkConfig *sinks.SinkConfig, prefix string) *sinks.SinkBuffer {
	//fmt.Printf("createSinkBuffer for %v\n", prefix)

	sinkBuffer := &sinks.SinkBuffer{
		BufferList: []string{},
		Size:       0,
		LastFlush:  time.Now(),
	}

	go startSinkTimer(sinkConfig.BatchMaxTime, sinkConfig.TimeChan,
		capsule.SinkTimerCapsule{SinkId: sinkConfig.Id, Prefix: prefix, LastFlush: sinkBuffer.LastFlush})

	return sinkBuffer
}

func handleSinkTimer(tc capsule.SinkTimerCapsule, snks map[uuid.UUID]*sinks.SinkConfig) {

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
