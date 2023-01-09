package execute

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"os/exec"
	"time"

	"github.com/tidwall/gjson"
	"github.com/vaerohq/vaero/capsule"
	"github.com/vaerohq/vaero/integrations/sources"
	"github.com/vaerohq/vaero/log"
	"go.uber.org/zap"
)

type SourceConfig struct {
	SourceTask         *OpTask
	Interval           time.Duration
	LastExecution      time.Time
	LastSecretsRefresh time.Time
	SecretsCacheTime   time.Duration
	SecretsTimeout     time.Duration
}

func initSourceConfig(sourceTask *OpTask) SourceConfig {
	sourceConfig := SourceConfig{SourceTask: sourceTask}

	val, ok := sourceTask.Args["interval"]

	if ok {
		sourceConfig.Interval = time.Duration(val.(float64)) * time.Second
	}

	val, ok = sourceTask.Secret["cache_time_seconds"]
	if ok {
		sourceConfig.SecretsCacheTime = time.Duration(val.(float64)) * time.Second
	}

	val, ok = sourceTask.Secret["timeout_seconds"]
	if ok {
		sourceConfig.SecretsTimeout = time.Duration(val.(float64)) * time.Second
	}

	//fmt.Printf("sourceConfig %v\n", sourceConfig)

	return sourceConfig
}

func createSource(sourceTask *OpTask, srcOut chan capsule.Capsule) (sources.Source, error) {
	var source sources.Source

	switch sourceTask.Op {
	case "http_server":
		source = &sources.HTTPServerSource{
			Endpoint:     sourceTask.Args["endpoint"].(string),
			EventBreaker: sourceTask.Args["event_breaker"].(string),
			Name:         sourceTask.Args["name"].(string),
			Port:         int(sourceTask.Args["port"].(float64)),
			SrcOut:       srcOut,
		}
	case "okta":
		source = &sources.OktaSource{
			Interval:             int(sourceTask.Args["interval"].(float64)),
			Host:                 sourceTask.Args["host"].(string),
			Token:                sourceTask.Args["token"].(string),
			Name:                 sourceTask.Args["name"].(string),
			Max_calls_per_period: int(sourceTask.Args["max_calls_per_period"].(float64)),
			Limit_period:         int(sourceTask.Args["limit_period"].(float64)),
			Max_retries:          int(sourceTask.Args["max_retries"].(float64)),
		}
	case "random":
		source = &sources.RandomSource{
			Name: sourceTask.Args["name"].(string),
		}
	default:
		log.Logger.Error("Source not found", zap.String("Source", sourceTask.Op))
		return nil, errors.New("Source not found")
	}

	return source, nil
}

func updateSource(source sources.Source, task *OpTask) sources.Source {
	var updatedSource sources.Source

	switch task.Op {
	case "okta":
		updatedSource = &sources.OktaSource{
			Interval:             int(task.Args["interval"].(float64)),
			Host:                 task.Args["host"].(string),
			Token:                task.Args["token"].(string),
			Name:                 task.Args["name"].(string),
			Max_calls_per_period: int(task.Args["max_calls_per_period"].(float64)),
			Limit_period:         int(task.Args["limit_period"].(float64)),
			Max_retries:          int(task.Args["max_retries"].(float64)),
		}
	case "random":
		// nothing to update
	default:
		log.Logger.Error("Source not found", zap.String("Source", task.Op))
	}

	return updatedSource
}

// getSecrets runs the command to retrieve secrets and returns the json parsed output from the command as a map
func getSecrets(secret map[string]interface{}) (map[string]interface{}, error) {
	// Generate command
	cmd := exec.Command(secret["command"].(string))

	// Connect stdin and stdout
	stdin, err := cmd.StdinPipe()
	if err != nil {
		log.Logger.Error("Error opening stdin pipe", zap.String("Error", err.Error()))
		return make(map[string]interface{}), errors.New("Error opening stdin pipe")
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Logger.Error("Error opening stdout pipe", zap.String("Error", err.Error()))
		return make(map[string]interface{}), errors.New("Error opening stdout pipe")
	}
	defer stdout.Close()

	// Execute the command
	if err := cmd.Start(); err != nil {
		log.Logger.Error("Error executing secrets script", zap.String("Error", err.Error()))
		return make(map[string]interface{}), errors.New("Error executing secrets script")
	}

	// Write to stdin
	jsonSecrets, err := json.Marshal(secret["secrets"])
	if err != nil {
		log.Logger.Error("Error writing to stdin pipe", zap.String("Error", err.Error()))
		return make(map[string]interface{}), errors.New("Error writing to stdin pipe")
	}
	io.WriteString(stdin, string(jsonSecrets))
	stdin.Close()

	// Read stdout of the command
	buf, err := ioutil.ReadAll(stdout)
	if err != nil {
		log.Logger.Error("Error reading stdout pipe", zap.String("Error", err.Error()))
		return make(map[string]interface{}), errors.New("Error reading stdout pipe")
	}

	// Wait for the command to complete
	if err := cmd.Wait(); err != nil {
		log.Logger.Error("Error waiting for secrets script", zap.String("Error", err.Error()))
		return make(map[string]interface{}), errors.New("Error waiting for secrets script")
	}

	//fmt.Printf("Retrieved secrets %v\n", string(buf))

	// Parse in format {"arg1" : value1, "arg2" : value2}
	secretsMap := gjson.Parse(string(buf)).Value().(map[string]interface{})

	return secretsMap, nil
}

// applySecrets adds the new secrets as arguments to the source task
func applySecrets(sourceTask *OpTask, newSecrets map[string]interface{}) {
	for k, v := range newSecrets {
		sourceTask.Args[k] = v
		//fmt.Printf("Assigned to source task[%v] = %v", k, v)
	}
}
