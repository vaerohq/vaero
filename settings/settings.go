package settings

type GlobalConfig struct {
	// DefaultChanBufferLen defines the default length of channel buffers. Each message on a channel
	// is a slice of events, so this is effectively the number of slices, not of individual events
	DefaultChanBufferLen int

	// Path to the version of Python to use
	PythonVenv string
}

var Config GlobalConfig
