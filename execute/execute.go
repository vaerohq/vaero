package execute

import (
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/google/uuid"
	"github.com/vaerohq/vaero/capsule"
	"github.com/vaerohq/vaero/log"
	"github.com/vaerohq/vaero/settings"
)

type Executor struct {
}

// RunJob runs a job for the taskGraph. The job runs as a set of forever-running goroutines until stopped.
func (executor *Executor) RunJob(interval int, taskGraph []OpTask) {
	log.Logger.Info("RunJob", zap.Int("interval", interval))

	var done chan int = make(chan int)
	var srcOut chan capsule.Capsule = make(chan capsule.Capsule, settings.DefChanBufferLen)
	var tnOut chan capsule.Capsule = make(chan capsule.Capsule, settings.DefChanBufferLen)

	go sourceNode(done, srcOut, taskGraph)
	go transformNode(srcOut, tnOut, taskGraph)
	go sinkNode(tnOut, taskGraph)

	// Test killing all goroutines
	time.Sleep(time.Second * 8)
	done <- 1
}

func sourceNode(done chan int, srcOut chan capsule.Capsule, taskGraph []OpTask) {
	source := identifySource(taskGraph)

	defer func() {
		close(srcOut)
		log.Logger.Info("Closing sourceNode")
	}()

	// main loop
	//count := 0 // temp
	for {
		select {
		case _ = <-done:
			return

		default:
			capsule := capsule.Capsule{EventList: source.Read()} // read from source, and create capsule
			srcOut <- capsule                                    // send capsule to transformNode
			// capsule and eventList unsafe to access after sending

			// TEMP
			time.Sleep(time.Second * 4)
			/*
				if count%2 == 0 {
					time.Sleep(time.Second * 0)
				} else {
					time.Sleep(time.Second * 3)
				}
				count++
			*/
		}
	}
}

func transformNode(srcOut chan capsule.Capsule, tnOut chan capsule.Capsule, taskGraph []OpTask) {
	defer func() {
		close(tnOut)
		log.Logger.Info("Closing transformNode")
	}()

	// main loop
	for {
		event, ok := <-srcOut

		// Kill goroutine when the channel is closed
		if !ok {
			return
		}

		fmt.Printf("TransformNode received: %v\n", event.EventList)

		// Perform transformations
		transformProcess(event.EventList, taskGraph, tnOut)
	}
}

func sinkNode(tnOut chan capsule.Capsule, taskGraph []OpTask) {
	// sinks map stores all sinks
	var snks = make(map[uuid.UUID]*SinkConfig)

	// channel for timers
	timeChan := make(chan capsule.SinkTimerCapsule, settings.DefChanBufferLen)

	defer func() {
		closeSinks(snks)
		log.Logger.Info("Closing sinkNode")
	}()

	initSinkNode(snks, taskGraph, timeChan)

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
