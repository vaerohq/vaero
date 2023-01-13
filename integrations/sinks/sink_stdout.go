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

type StdoutSink struct {
}

// Init initializes the sink
func (s *StdoutSink) Init(sinkConfig *SinkConfig) {

}

// Flush writes data out to the sink immediately
func (s *StdoutSink) Flush(filename string, prefix string, eventList []string) {
	log.Logger.Info("Flush to Stdout", zap.String("Prefix", prefix))
	fmt.Printf("%v\n", strings.Join(eventList, "\n"))
}
