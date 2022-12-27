package sources

import (
	"fmt"
	"time"
)

type RandomSource struct {
	//Count int
}

func (source *RandomSource) Read() []string {
	eventList := []string{
		fmt.Sprintf(`{"hostname" : "Alderaan", "t" : true, "f" : false, "msg" : "Toto, I've got a feeling we're not in Kansas anymore", "severity" : "info", "time" : "%s"}`, time.Now().Format(time.RFC3339)),
		fmt.Sprintf(`{"hostname" : "Bantha", "t" : true, "f" : false, "msg" : "Here's looking at you, kid", "severity" : "debug", "time" : "%s"}`, time.Now().Format(time.RFC3339)),
		fmt.Sprintf(`{"hostname" : "Cantina", "t" : true, "f" : false, "msg" : "Go ahead, make my day", "severity" : "alert", "time" : "%s"}`, time.Now().Format(time.RFC3339)),
		fmt.Sprintf(`{"hostname" : "Dagobah", "t" : true, "f" : false, "msg" : "The stuff that dreams are made of", "severity" : "warning", "time" : "%s"}`, time.Now().Format(time.RFC3339)),
		fmt.Sprintf(`{"hostname" : "Endor", "t" : true, "f" : false, "msg" : "Louis, I think this is the beginning of a beautiful friendship", "severity" : "alert", "time" : "%s"}`, time.Now().Format(time.RFC3339)),
		fmt.Sprintf(`{"hostname" : "Falcon", "t" : true, "f" : false, "msg" : "There's no place like home", "severity" : "info", "time" : "%s"}`, time.Now().Format(time.RFC3339)),
		fmt.Sprintf(`{"hostname" : "Greedo", "t" : true, "f" : false, "msg" : "Today, I consider myself the luckiest man on the face of the earth", "severity" : "warning", "time" : "%s"}`, time.Now().Format(time.RFC3339)),
		fmt.Sprintf(`{"hostname" : "Hoth", "t" : true, "f" : false, "msg" : "Every time a bell rings an angel gets his wings", "severity" : "info", "time" : "%s"}`, time.Now().Format(time.RFC3339)),
	}
	return eventList
}
