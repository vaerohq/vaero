package sinks

import (
	"fmt"
	"strings"

	"github.com/vaerohq/vaero/log"
	"go.uber.org/zap"
)

type ElasticSink struct {
}

// Init initializes the sink
func (s *ElasticSink) Init() {

}

// Flush writes data out to the sink immediately
func (s *ElasticSink) Flush(prefix string, eventList []string) {
	/*
	 * Replace here
	 */

	log.Logger.Info("Flush to Elastic", zap.String("prefix", prefix))
	fmt.Printf("%v\n", strings.Join(eventList, "\n"))
}
