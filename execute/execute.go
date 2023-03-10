/*
Copyright © 2023 Vaero Inc. (https://www.vaero.co/)
*/
package execute

import (
	"time"

	"go.uber.org/zap"

	"github.com/google/uuid"
	"github.com/vaerohq/vaero/capsule"
	"github.com/vaerohq/vaero/integrations/sinks"
	"github.com/vaerohq/vaero/log"
	"github.com/vaerohq/vaero/settings"
)

type Executor struct {
}

type ControlChannels struct {
	Done chan int
}

var pipeControls map[int]ControlChannels = map[int]ControlChannels{}

// StopJob stops a job by sending the done signal
func (executor *Executor) StopJob(id int) {
	log.Logger.Info("Stop Job", zap.Int("Id", id))

	pipeControls[id].Done <- 1
}

// RunJob runs a job for the taskGraph. The job runs as a set of forever-running goroutines until stopped.
func (executor *Executor) RunJob(id int, interval int, taskGraph []OpTask) {
	log.Logger.Info("Run Job", zap.Int("Id", id), zap.Int("Interval", interval))

	var done chan int = make(chan int)
	var srcOut chan capsule.Capsule = make(chan capsule.Capsule, settings.Config.DefaultChanBufferLen)
	var tnOut chan capsule.Capsule = make(chan capsule.Capsule, settings.Config.DefaultChanBufferLen)

	go sourceNode(done, srcOut, taskGraph)
	go transformNode(srcOut, tnOut, taskGraph)
	go sinkNode(tnOut, taskGraph)

	pipeControls[id] = ControlChannels{Done: done}
}

func sourceNode(done chan int, srcOut chan capsule.Capsule, taskGraph []OpTask) {

	// check the first task of the task graph to identify the source
	if len(taskGraph) <= 0 {
		log.Logger.Error("Task graph is empty")
		return
	}

	if taskGraph[0].Type != "source" {
		log.Logger.Error("Task graph does not start with a source")
		return
	}

	sourceConfig := initSourceConfig(&taskGraph[0])
	source, err := createSource(sourceConfig.SourceTask, srcOut)

	if err != nil {
		log.Logger.Error("Failed to identify source", zap.String("Error", err.Error()))
	}

	defer func() {
		source.CleanUp()
		close(srcOut)
		log.Logger.Info("Closing sourceNode")
	}()

	if source.Type() == "pull" {
		// main loop
		//count := 0 // temp
		for {
			select {
			case _ = <-done:
				return

			default:
				// Refresh secrets if needed
				if len(sourceConfig.SourceTask.Secret) != 0 &&
					time.Now().Sub(sourceConfig.LastSecretsRefresh) > sourceConfig.SecretsCacheTime {
					newSecrets, err := getSecrets(sourceConfig.SourceTask.Secret)
					if err != nil {
						log.Logger.Error("Error refreshing secrets", zap.String("Error", err.Error()))
					} else {
						log.Logger.Info("Refreshed secrets")
						applySecrets(sourceConfig.SourceTask, newSecrets)
						source = updateSource(source, sourceConfig.SourceTask)
					}
					sourceConfig.LastSecretsRefresh = time.Now()
				}

				// Read from source
				sourceConfig.LastExecution = time.Now()
				capsule := capsule.Capsule{EventList: source.Read()} // read from source, and create capsule
				srcOut <- capsule                                    // send capsule to transformNode
				// capsule and eventList unsafe to access after sending

				// Delay for interval
				delta := sourceConfig.Interval - time.Now().Sub(sourceConfig.LastExecution)
				if delta > 0 {
					//fmt.Printf("Delay for %v\n", delta)
					time.Sleep(delta)
				}

				// TEMP
				//time.Sleep(time.Second * 4)

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
	} else if source.Type() == "push" {
		source.Read()

		<-done // wait for done signal
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

		// Perform transformations
		transformProcess(event.EventList, taskGraph, tnOut)
	}
}

func sinkNode(tnOut chan capsule.Capsule, taskGraph []OpTask) {
	// sinks map stores all sinks
	var snks = make(map[uuid.UUID]*sinks.SinkConfig)

	// channel for timers
	timeChan := make(chan capsule.SinkTimerCapsule, settings.Config.DefaultChanBufferLen)

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
