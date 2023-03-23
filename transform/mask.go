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

// Mask masks the portion of the value matching the regex with the replace expression
func Mask(json string, path string, regex string, replaceExpr string) string {
	value := gjson.Get(json, path)

	re, err := regexp.Compile(regex)

	if err != nil {
		log.Logger.Error("Mask regular expression failed to compile", zap.String("Error", err.Error()))
	}

	maskedString := re.ReplaceAllString(value.String(), replaceExpr)

	var result string
	result, err = sjson.Set(json, path, maskedString)

	if err != nil {
		log.Logger.Error("Mask transform failed to set value", zap.String("Error", err.Error()))
	}

	return result
}

// MaskAll masks all the log events in the event list
func MaskAll(eventList []string, path string, regex string, replaceExpr string) []string {
	for idx := range eventList {
		eventList[idx] = Mask(eventList[idx], path, regex, replaceExpr)
	}
	return eventList
}
