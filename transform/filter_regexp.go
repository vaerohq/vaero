/*
Copyright © 2023 Vaero Inc. (https://www.vaero.co/)
*/
package transform

import (
	"regexp"

	"github.com/tidwall/gjson"
)

// FilterRegExp returns true if the value in path matches the regular expression
func FilterRegExp(json string, path string, regex string) bool {
	value := gjson.Get(json, path)

	match, _ := regexp.MatchString(regex, value.String())

	return match
}

// FilterRegExpAll filters all logs in eventList based on matching a regular expression on a field
func FilterRegExpAll(eventList []string, path string, regex string) []string {
	var newEventList []string

	for idx := range eventList {
		if FilterRegExp(eventList[idx], path, regex) {
			newEventList = append(newEventList, eventList[idx])
		}
	}
	return newEventList
}
