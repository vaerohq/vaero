/*
Copyright © 2023 Vaero Inc. (https://www.vaero.co/)
*/
package transform

import (
	"github.com/tidwall/gjson"
)

// Select returns the value of a selected field of the event
func Select(json string, path string) string {
	value := gjson.Get(json, path)

	return value.String()
}

// SelectAll selects on all logs in eventList
func SelectAll(eventList []string, path string) []string {
	for idx := range eventList {
		eventList[idx] = Select(eventList[idx], path)
	}
	return eventList
}
