package settings

// DefChanBufferLen defines the default length of channel buffers. Each message on a channel
// is a slice of events, so this is effectively the number of slices, not of individual events
var DefChanBufferLen = 1000
