package execute

import (
	"time"

	"github.com/lestrrat-go/strftime"
	"github.com/tidwall/gjson"
	"github.com/vaerohq/vaero/capsule"
	"github.com/vaerohq/vaero/integrations/sinks"
	"github.com/vaerohq/vaero/log"
	"github.com/vaerohq/vaero/settings"
	"go.uber.org/zap"
)

type SinkConfig struct {
	Id            int
	Type          string
	Prefix        map[string]*SinkBuffer
	BatchMaxBytes int
	BatchMaxTime  int
	FlushChan     chan capsule.Capsule          // channel for sending flushed data (received by FlushNode)
	TimeChan      chan capsule.SinkTimerCapsule // channel for sending timer expiration (received by SinkNode)
}

type SinkBuffer struct {
	BufferList []string
	Size       int
	LastFlush  time.Time
}

// initSinkNode performs initialize for the sink node
func initSinkNode(sinks map[int]*SinkConfig, sinkTargets []int, timeChan chan capsule.SinkTimerCapsule) {

	// Initialize all sinks
	for _, sinkTarget := range sinkTargets {
		// Create configuration for a sink
		sinks[sinkTarget] = &SinkConfig{Id: sinkTarget, Type: "stdout", BatchMaxBytes: 2_500, BatchMaxTime: 2,
			Prefix: make(map[string]*SinkBuffer), FlushChan: make(chan capsule.Capsule, settings.DefChanBufferLen),
			TimeChan: timeChan}

		// Create goroutine to flush to the sink
		go flushNode(sinks[sinkTarget])
	}
}

// sinkBatch adds events to a sink buffer and flushes if needed
func sinkBatch(c *capsule.Capsule, sinks map[int]*SinkConfig) {

	// These are temp variables that will be user specified
	var timeField string = "time"     // the path to the timestamp
	var layout string = time.RFC3339  // pattern of the timestamp
	var prefixPat string = "%Y/%m/%H" // prefix pattern

	// Strftime formatter
	prefixFormatter, err := strftime.New(prefixPat, strftime.WithUnixSeconds('s'))

	if err != nil {
		log.Logger.Fatal(err.Error())
	}

	// Identify sinkConfig
	sinkConfig := sinks[c.SinkId]
	eventList := c.EventList

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
			sinkBuffer = &SinkBuffer{LastFlush: time.Now()}
			sinkConfig.Prefix[prefix] = sinkBuffer

			go startSinkTimer(sinkConfig.BatchMaxTime, sinkConfig.TimeChan,
				capsule.SinkTimerCapsule{SinkId: sinkConfig.Id, Prefix: prefix, LastFlush: sinkBuffer.LastFlush})
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

		// Sends buffer to be flushed, and resets buffer
		flushSinkBuffer(sinkConfig, prefix, sinkBuffer)

		// Append to freshly reset buffer
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
		log.Logger.Info("Closing sinkFlusher", zap.Int("id", sinkConfig.Id))
	}()

	// Choose sink type based on taskGraph
	var s sinks.Sink
	switch sinkConfig.Type {
	case "stdout":
		s = &sinks.StdoutSink{}
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
func closeSinks(snks map[int]*SinkConfig) {
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

	// Reset buffer
	sinkBuffer.BufferList = []string{}
	sinkBuffer.Size = 0
	sinkBuffer.LastFlush = time.Now()

	go startSinkTimer(sinkConfig.BatchMaxTime, sinkConfig.TimeChan,
		capsule.SinkTimerCapsule{SinkId: sinkConfig.Id, Prefix: prefix, LastFlush: sinkBuffer.LastFlush})
}

func handleSinkTimer(tc capsule.SinkTimerCapsule, snks map[int]*SinkConfig) {

	sinkConfig := snks[tc.SinkId]
	sinkBuffer := snks[tc.SinkId].Prefix[tc.Prefix]

	// If the sinkBuffer has not been flushed since the time that this timer was send, then flush
	// Otherwise, ignore the timer
	if sinkBuffer.LastFlush == tc.LastFlush {
		log.Logger.Info("Flush: MaxTime")
		// Sends buffer to be flushed, and resets buffer
		flushSinkBuffer(sinkConfig, tc.Prefix, sinkBuffer)
	}
}
