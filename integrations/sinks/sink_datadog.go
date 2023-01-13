/*
Copyright © 2023 Vaero Inc. (https://www.vaero.co/)
*/
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
func (s *DatadogSink) Init(sinkConfig *SinkConfig) {

}

// Flush writes data out to the sink immediately
func (s *DatadogSink) Flush(filename string, prefix string, eventList []string) {
	/*
	 * Replace here
	 */

	log.Logger.Info("Flush to Datadog", zap.String("Prefix", prefix))
	fmt.Printf("%v\n", strings.Join(eventList, "\n"))
}
