package transform

import (
	"github.com/tidwall/sjson"
	"github.com/vaerohq/vaero/log"
	"go.uber.org/zap"
)

// Delete function deletes value at path in json
func Delete(json string, path string) string {
	result, err := sjson.Delete(json, path)

	if err != nil {
		log.Logger.Error("Delete transform failed", zap.String("Error", err.Error()))
	}

	return result
}

// DeleteAll function deletes value at path in each json in eventList
func DeleteAll(eventList []string, path string) []string {
	for idx := range eventList {
		eventList[idx] = Delete(eventList[idx], path)
	}
	return eventList
}
