package sources

type OktaSource struct {
	Interval             int
	Host                 string
	Token                string
	Name                 string
	Max_calls_per_period int
	Limit_period         int
	Max_retries          int
}

// Read returns an event list
func (source *OktaSource) Read() []string {
	eventList := PythonSourceRead("okta", source.Interval, source.Host, source.Token,
		source.Name, source.Max_calls_per_period, source.Limit_period, source.Max_retries)

	return eventList
}

// Type returns either "pull" or "push"
func (source *OktaSource) Type() string {
	return "pull"
}

func (source *OktaSource) CleanUp() {

}
