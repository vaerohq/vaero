package sinks

type Sink interface {
	Init()
	Flush(string, []string)
}
