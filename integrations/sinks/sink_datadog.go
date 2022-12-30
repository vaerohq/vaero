package sinks

import (
	"fmt"
	"strings"

	"github.com/vaerohq/vaero/log"
	"go.uber.org/zap"
)

type DatadogSink struct {
}

// Init initializes the sink
func (s *DatadogSink) Init() {

}

// Flush writes data out to the sink immediately
func (s *DatadogSink) Flush(prefix string, eventList []string) {
	/*
	 * Replace here
	 */

	log.Logger.Info("Flush to Datadog", zap.String("prefix", prefix))
	fmt.Printf("%v\n", strings.Join(eventList, "\n"))
}
