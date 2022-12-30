package sinks

import (
	"fmt"
	"strings"

	"github.com/vaerohq/vaero/log"
	"go.uber.org/zap"
)

type S3Sink struct {
}

// Init initializes the sink
func (s *S3Sink) Init() {

}

// Flush writes data out to the sink immediately
func (s *S3Sink) Flush(prefix string, eventList []string) {
	/*
	 * Replace here
	 */

	log.Logger.Info("Flush to S3", zap.String("prefix", prefix))
	fmt.Printf("%v\n", strings.Join(eventList, "\n"))
}
