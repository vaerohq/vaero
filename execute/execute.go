package execute

import (
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/vaerohq/vaero/capsule"
	"github.com/vaerohq/vaero/integrations/sources"
	"github.com/vaerohq/vaero/log"
	"github.com/vaerohq/vaero/settings"
)

type Executor struct {
}

// RunJob runs a job for the taskGraph. The job runs as a set of forever-running goroutines until stopped.
func (executor *Executor) RunJob(interval int, taskGraph int) {
	log.Logger.Info("RunJob", zap.Int("interval", interval), zap.Int("taskGraph", taskGraph))

	var done chan int = make(chan int)
	var srcOut chan capsule.Capsule = make(chan capsule.Capsule, settings.DefChanBufferLen)
	var tnOut chan capsule.Capsule = make(chan capsule.Capsule, settings.DefChanBufferLen)

	go sourceNode(done, srcOut)
	go transformNode(srcOut, tnOut)
	go sinkNode(tnOut)

	// Test killing all goroutines
	time.Sleep(time.Second * 8)
	done <- 1
}

func sourceNode(done chan int, srcOut chan capsule.Capsule) {
	var source sources.Source
	source = &sources.RandomSource{}

	defer func() {
		close(srcOut)
		log.Logger.Info("Closing sourceNode")
	}()

	// main loop
	count := 0 // temp
	for {
		select {
		case _ = <-done:
			return

		default:
			capsule := capsule.Capsule{EventList: source.Read()} // read from source, and create capsule
			srcOut <- capsule                                    // send capsule to transformNode
			// capsule and eventList unsafe to access after sending

			// TEMP
			if count%2 == 0 {
				time.Sleep(time.Second * 0)
			} else {
				time.Sleep(time.Second * 3)
			}
			count++
		}
	}
}

func transformNode(srcOut chan capsule.Capsule, tnOut chan capsule.Capsule) {
	defer func() {
		close(tnOut)
		log.Logger.Info("Closing transformNode")
	}()

	taskGraph := []string{"add", "delete", "rename"}

	// main loop
	for {
		event, ok := <-srcOut

		// Kill goroutine when the channel is closed
		if !ok {
			return
		}

		fmt.Printf("TransformNode received: %v\n", event.EventList)

		// Perform transformations
		fmt.Println("Parse")
		eventList := transformProcess(event.EventList, taskGraph)

		tnOut <- capsule.Capsule{SinkId: 1001, EventList: eventList}
		// capsule and eventList unsafe to access after sending
	}
}

func sinkNode(tnOut chan capsule.Capsule) {
	// sinks map stores all sinks
	var snks = make(map[int]*SinkConfig)

	// channel for timers
	timeChan := make(chan capsule.SinkTimerCapsule, settings.DefChanBufferLen)

	defer func() {
		closeSinks(snks)
		log.Logger.Info("Closing sinkNode")
	}()

	sinkTargets := []int{1001, 1002} // temp

	initSinkNode(snks, sinkTargets, timeChan)

	// main loop
	for {
		select {

		// Receive timer event
		case tc, _ := <-timeChan:

			handleSinkTimer(tc, snks)

		// Receive new events from transformNode
		case event, ok := <-tnOut:

			// Kill goroutine when the channel is closed
			if !ok {
				return
			}

			sinkBatch(&event, snks)
		}
	}
}
