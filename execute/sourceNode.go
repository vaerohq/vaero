package execute

import (
	"github.com/vaerohq/vaero/integrations/sources"
	"github.com/vaerohq/vaero/log"
	"go.uber.org/zap"
)

func identifySource(taskGraph []OpTask) sources.Source {
	var source sources.Source

	// check the first task of the task graph to identify the source
	if len(taskGraph) <= 0 {
		log.Logger.Fatal("Task graph is empty")
	}

	if taskGraph[0].Type != "source" {
		log.Logger.Fatal("Task graph does not start with a source")
	}

	switch taskGraph[0].Op {
	case "random":
		source = &sources.RandomSource{}
	default:
		log.Logger.Fatal("Source not found", zap.String("source", taskGraph[0].Op))
	}

	return source
}
