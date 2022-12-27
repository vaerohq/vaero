package capsule

import "time"

// Capsule is a capsule for transmitting data from the SourceNode to the TransformationNode to the SinkNode
type Capsule struct {
	SinkId    int    // only needed when sending to SinkNode, otherwise 0
	Prefix    string // only needed when sending to SinkFlushNode, otherwise empty string
	EventList []string
}

// SinkTimerCapsule is a capsule for sending a timer to flush before MaxBatchTime is exceeded
type SinkTimerCapsule struct {
	SinkId    int
	Prefix    string
	LastFlush time.Time
}
