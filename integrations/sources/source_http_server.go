package sources

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/tidwall/sjson"
	"github.com/vaerohq/vaero/capsule"
	"github.com/vaerohq/vaero/log"
	"go.uber.org/zap"
)

type HTTPServerSource struct {
	Endpoint     string
	EventBreaker string
	Name         string
	Port         int
	SrcOut       chan capsule.Capsule
	Srv          *http.Server
}

// Read and transmit event list to srcOut when data is received
func (source *HTTPServerSource) Read() []string {
	port := fmt.Sprintf(":%d", source.Port)
	source.Srv = &http.Server{Addr: port}

	endpoint := fmt.Sprintf("%s", source.Endpoint)
	http.HandleFunc(endpoint, func(w http.ResponseWriter, r *http.Request) {
		source.httpHandler(w, r)
	})

	go func() {
		source.Srv.ListenAndServe()
	}()

	return []string{} // have to return something to match interface function signature
}

// Type returns either "pull" or "push"
func (source *HTTPServerSource) Type() string {
	return "push"
}

func (source *HTTPServerSource) CleanUp() {
	log.Logger.Info("Shut down http server")
	source.Srv.Shutdown(context.TODO())
}

// httpHandler handles a request
func (source *HTTPServerSource) httpHandler(w http.ResponseWriter, req *http.Request) {
	bodyBytes, err := io.ReadAll(req.Body)

	if err != nil {
		log.Logger.Error("Could not read http request", zap.String("Error", err.Error()))
		return
	}
	bodyString := string(bodyBytes)

	// Event break
	var eventList []string
	switch strings.ToLower(source.EventBreaker) {
	case "jsonarray":
		eventList = EventBreakJSONArray(bodyString)
	default:
		eventList = EventBreakJSONArray(bodyString)
	}

	// Automatically add fields
	for idx := range eventList {
		eventList[idx], err = sjson.Set(eventList[idx], "timestamp", time.Now().Format(time.RFC3339)) // timestamp

		if err != nil {
			log.Logger.Error("Error adding fields to received http logs", zap.String("Error", err.Error()))
		}

		eventList[idx], err = sjson.Set(eventList[idx], "remoteaddr", req.RemoteAddr) // remote address

		if err != nil {
			log.Logger.Error("Error adding fields to received http logs", zap.String("Error", err.Error()))
		}
	}

	//fmt.Printf("Event break %v\n", eventList)

	// Send into pipeline
	capsule := capsule.Capsule{EventList: eventList} // create capsule
	source.SrcOut <- capsule                         // send capsule to transformNode
	// capsule and eventList unsafe to access after sending
}
