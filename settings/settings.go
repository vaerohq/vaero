/*
Copyright © 2023 Vaero Inc. (https://www.vaero.co/)
*/
package settings

type GlobalConfig struct {
	// DefaultChanBufferLen defines the default length of channel buffers. Each message on a channel
	// is a slice of events, so this is effectively the number of slices, not of individual events
	DefaultChanBufferLen int

	// Level for logging
	LogLevel string

	// PollPipelineChangesFreq is the number of seconds between polls of the admin routine to check for changes
	// in the jobs table
	PollPipelineChangesFreq int

	// Path to the folder containing the version of Python to use
	PythonPath string
}

var Config GlobalConfig
