package execute

import (
	"fmt"

	"github.com/vaerohq/vaero/transform"
)

func transformProcess(eventList []string, taskGraph []string) []string {
	for _, v := range taskGraph {
		switch v {
		case "add":
			fmt.Println("add")
			eventList = transform.AddAll(eventList, "newField.sub.graph", 538)
			eventList = transform.AddAll(eventList, "newField.sub.graph2", "We're not in Kansas anymore")
			eventList = transform.AddAll(eventList, "newField.sub.graph3", []string{"one", "two", "three"})
		case "delete":
			fmt.Println("delete")
			//eventList = transform.DeleteAll(eventList, "severity")
			eventList = transform.DeleteAll(eventList, "newField.sub.graph2")
		case "rename":
			fmt.Println("rename")
			eventList = transform.RenameAll(eventList, "newField.sub.graph3", "totally.new.field")
		default:
			fmt.Println("unknown op")
		}
	}

	return eventList
}
