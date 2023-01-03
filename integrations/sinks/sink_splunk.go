package sinks

import (
	"fmt"
	"strings"

	"github.com/vaerohq/vaero/log"
	"go.uber.org/zap"
)

type SplunkSink struct {
}

// Init initializes the sink
func (s *SplunkSink) Init(sinkConfig *SinkConfig) {

}

// Flush writes data out to the sink immediately
func (s *SplunkSink) Flush(filename string, prefix string, eventList []string) {
	/*
	 * Replace here
	 */

	log.Logger.Info("Flush to Splunk", zap.String("prefix", prefix))
	fmt.Printf("%v\n", strings.Join(eventList, "\n"))
}
