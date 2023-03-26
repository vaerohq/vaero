/*
Copyright © 2023 Vaero Inc. (https://www.vaero.co/)
*/
package execute

import (
	"strings"

	"github.com/vaerohq/vaero/capsule"
	"github.com/vaerohq/vaero/log"
	"github.com/vaerohq/vaero/transform"
)

func transformProcess(eventList []string, taskGraph []OpTask, tnOut chan capsule.Capsule) []string {
	for _, v := range taskGraph {
		// task is a transform, so perform the task
		if v.Type == "tn" {
			switch v.Op {
			case "add":
				eventList = transform.AddAll(eventList, v.Args["path"].(string), v.Args["value"])
			case "delete":
				eventList = transform.DeleteAll(eventList, v.Args["path"].(string))
			case "filter_regexp":
				eventList = transform.FilterRegExpAll(eventList, v.Args["path"].(string), v.Args["regex"].(string))
			case "mask":
				eventList = transform.MaskAll(eventList, v.Args["path"].(string), v.Args["regex"].(string), v.Args["replace_expr"].(string))
			case "parse_regexp":
				eventList = transform.ParseRegExpAll(eventList, v.Args["path"].(string), v.Args["regex"].(string))
			case "rename":
				eventList = transform.RenameAll(eventList, v.Args["path"].(string), v.Args["new_path"].(string))
			default:
				log.Logger.Error("Unknown op")
			}
		} else if v.Type == "branch" { // Branch

			// Iterate over all branches, the first branch receives the eventList. Each additional branch receives a copy
			// of the eventList.

			// Must make copies of eventList before the eventList is manipulated
			copyList := make([][]string, len(v.Branches))
			for idx := range v.Branches {
				if idx == 0 {
					// First branch uses the original data
					copyList[idx] = eventList
				} else {
					// Make a copy for later branches
					copyList[idx] = make([]string, len(eventList))
					copy(copyList[idx], eventList)
				}
			}

			// Perform transforms
			for idx, branch := range v.Branches {
				transformProcess(copyList[idx], branch, tnOut)
			}
		} else if v.Type == "sink" { // When reach a sink, transmit to the sinkNode with the sinkId as a tag

			tnOut <- capsule.Capsule{SinkId: v.Id, EventList: eventList}
			// capsule and eventList unsafe to access after sending
		}
	}

	return eventList
}

// copyEventList makes a copy of the event list by deep copying all strings
// Deprecated: This is not needed. Even though Go copies string pointrs shallowly, when strings
// are edited the underlying pointer is changed, so other copies are protected from changes.
func copyEventList(eventList []string) []string {
	copyList := make([]string, len(eventList))

	for idx, v := range eventList {
		copyList[idx] = strings.Clone(v)
	}

	return copyList
}
