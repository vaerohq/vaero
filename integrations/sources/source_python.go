package sources

import (
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/vaerohq/vaero/log"
	"github.com/vaerohq/vaero/settings"
	"go.uber.org/zap"
)

// PythonSourceRead executes python_source_driver.py with the specified source as a parameter to
// read events from that source
func PythonSourceRead(sourceName string, interval int, host string, token string, name string,
	max_calls_per_period int, limit_period int, max_retries int) []string {

	moduleName := "integrations.python.python_source_driver"

	// Generate command
	cmd := exec.Command("python", "-m", moduleName, "--op", sourceName,
		"--host", host, "--token", token, "--name", name,
		"--max_calls_per_period", strconv.Itoa(max_calls_per_period),
		"--limit_period", strconv.Itoa(limit_period),
		"--max_retries", strconv.Itoa(max_retries))

	// Activate virtual environment if selected
	if settings.Config.PythonPath != "" {
		cmd.Path = filepath.Join(settings.Config.PythonPath, "python")
	}

	// Run command
	rawOut, err := cmd.Output()

	if err != nil {
		log.Logger.Error("Error executing python source driver", zap.String("Error", err.Error()))
		return []string{}
	}

	//fmt.Printf("RAW OUTPUT: %v", string(rawOut)) // DEBUG

	// Trim output to just include event list
	r1 := regexp.MustCompile(`(?s)^.*__Python Source Driver Output__`)
	jsonList := r1.ReplaceAllString(string(rawOut), ``)

	r2 := regexp.MustCompile(`(?s)__End Python Source Driver Output__.*$`)
	jsonList = r2.ReplaceAllString(jsonList, ``)

	//fmt.Printf("STRING: %s\n", string(jsonList)) // DEBUG

	// Break into events
	eventList := EventBreakJSONArray(jsonList)

	// DEBUG
	/*
		fmt.Printf("ARRAY %v:\n", len(eventList))
		for _, e := range eventList {
			fmt.Printf("%v\n", e)
		}
	*/

	return eventList
}

// EventBreakJSONArray receives as input a json representation of an array and returns
// an array of the json elements as strings
func EventBreakJSONArray(json string) []string {
	eventList := []string{}

	// remove start and end [ ]
	r1 := regexp.MustCompile(`(?s)^\s*\[`)
	json = r1.ReplaceAllString(string(json), ``)

	r2 := regexp.MustCompile(`(?s)\]\s*$`)
	json = r2.ReplaceAllString(json, ``)

	level := 0 // track braces {}
	start := 0 // starting character of the event
	for idx, c := range json {
		if c == '{' {
			level += 1
		} else if c == '}' {
			level -= 1
		} else if c == ',' && level == 0 {
			event := json[start:idx]
			eventList = append(eventList, event)
			start = idx + 1
		}
	}

	// Add the last element which lacks an ending ','
	if start < len(json) {
		event := json[start:]
		eventList = append(eventList, event)
		start = len(json)
	}

	return eventList
}
