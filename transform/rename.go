package transform

import (
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"github.com/vaerohq/vaero/log"
	"go.uber.org/zap"
)

// Rename function renames a field from path to newPath
func Rename(json string, path string, newPath string) string {
	value := gjson.Get(json, path)

	var result string
	var err error

	result, err = sjson.Set(json, newPath, value.Value())

	if err != nil {
		log.Logger.Error("Rename transform failed (set)", zap.String("Error", err.Error()))
	}

	result, err = sjson.Delete(result, path)

	if err != nil {
		log.Logger.Error("Rename transform failed (delete)", zap.String("Error", err.Error()))
	}

	return result
}

// RenameAll function renames a field from path to newPath in each json in eventList
func RenameAll(eventList []string, path string, newPath string) []string {
	for idx := range eventList {
		eventList[idx] = Rename(eventList[idx], path, newPath)
	}
	return eventList
}
