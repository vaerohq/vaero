/*
Copyright © 2023 Vaero Inc. (https://www.vaero.co/)
*/
package transform

import (
	"regexp"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"github.com/vaerohq/vaero/log"
	"go.uber.org/zap"
)

// ParseRegExp adds a new field for each capture group in the regex, with the matching content as the value
func ParseRegExp(json string, path string, regex string) string {
	value := gjson.Get(json, path)

	re, err := regexp.Compile(regex)

	if err != nil {
		log.Logger.Error("Mask regular expression failed to compile", zap.String("Error", err.Error()))
	}

	match := re.FindStringSubmatch(value.String())
	result := json

	for idx, name := range re.SubexpNames() {
		if idx != 0 && name != "" {
			result, err = sjson.Set(result, name, match[idx])

			if err != nil {
				log.Logger.Error("Error adding parsed field to event", zap.String("Error", err.Error()))
			}
		}
	}

	return result
}

// FilterRegExpAll filters all logs in eventList based on matching a regular expression on a field
func ParseRegExpAll(eventList []string, path string, regex string) []string {
	for idx := range eventList {
		eventList[idx] = ParseRegExp(eventList[idx], path, regex)
	}
	return eventList
}
