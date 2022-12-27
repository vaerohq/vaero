package transform

import (
	"github.com/tidwall/sjson"
	"github.com/vaerohq/vaero/log"
)

// Add function adds value at path in json
func Add(json string, path string, val interface{}) string {
	result, err := sjson.Set(json, path, val)

	if err != nil {
		log.Logger.Fatal(err.Error())
	}

	return result
}

// AddAll function adds value at path in each json in eventList
func AddAll(eventList []string, path string, val interface{}) []string {
	for idx := range eventList {
		eventList[idx] = Add(eventList[idx], path, val)
	}
	return eventList
}
