package cmd

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mitchellh/mapstructure"
	"github.com/tidwall/gjson"
	"github.com/vaerohq/vaero/execute"
	"github.com/vaerohq/vaero/log"
	"github.com/vaerohq/vaero/settings"
	"go.uber.org/zap"
)

// jobsTable is the name of the sql table for jobs
const jobsTable = "jobs"

type ControlDB struct {
	db *sql.DB
}

var c ControlDB

var executor execute.Executor

type PipelineEntry struct {
	Id           int
	Interval     int
	Alive        int
	Spec         string
	Status       string
	TaskGraphStr string
}

// InitSettings initializes settings from config file
func InitSettings() {
	configFile := "vaero.cfg"

	// Check that config file exists, otherwise create
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		log.Logger.Info("Creating config file")
		newFile, err := os.Create(configFile)

		if err != nil {
			log.Logger.Fatal("Error creating config file", zap.String("Filename", configFile))
		}
		newFile.Close()

	} else if err != nil {
		log.Logger.Fatal("Error reading config file", zap.String("Filename", configFile))
	}

	// Set defaults
	settings.Config = settings.GlobalConfig{
		DefaultChanBufferLen:    1000,
		LogLevel:                "Info",
		PollPipelineChangesFreq: 1,
		PythonPath:              "",
	}

	// Read config file into global settings
	if _, err := toml.DecodeFile(configFile, &settings.Config); err != nil {
		log.Logger.Fatal("Could not read config file", zap.String("Filename", configFile))
	}

	// Set log level
	switch strings.ToLower(settings.Config.LogLevel) {
	case "error":
		log.LogLevel.SetLevel(zap.ErrorLevel)
	case "info":
		log.LogLevel.SetLevel(zap.InfoLevel)
	case "warn":
		log.LogLevel.SetLevel(zap.WarnLevel)
	}
}

// CheckPython checks if Python3 is installed
func CheckPython() {

	// Run python
	cmd := exec.Command("python", "-V")

	// Activate virtual environment if selected
	if settings.Config.PythonPath != "" {
		cmd.Path = filepath.Join(settings.Config.PythonPath, "python")
	}

	// Run command
	output, err := cmd.Output()

	if err != nil {
		log.Logger.Fatal("Python not found. Python 3.X must be installed and accessible from command 'python'.")
	}

	// Check for Python 3
	r := regexp.MustCompile(`Python 3\..*`)

	if !r.MatchString(string(output)) {
		log.Logger.Fatal("Python 3.X must be installed and accessible from command 'python'", zap.String("Version", string(output)))
	}

	log.Logger.Info("Python found", zap.String("Version", string(output)))

	// Check for all required Python packages
	CheckPythonPackage("tomli")
}

// CheckPythonPackage checks if the specified Python package is installed
func CheckPythonPackage(pkg string) bool {
	// Run python
	importString := fmt.Sprintf("import %s", pkg)
	cmd := exec.Command("python", "-c", importString)

	// Activate virtual environment if selected
	if settings.Config.PythonPath != "" {
		cmd.Path = filepath.Join(settings.Config.PythonPath, "python")
	}

	// Run command
	_, err := cmd.Output()

	if err != nil {
		errorString := fmt.Sprintf("Required Python package %s not found. Please install with pip install.", pkg)
		log.Logger.Fatal(errorString, zap.String("Package", pkg))
		return false
	}

	return true
}

// InitTables creates Vaero's DB tables if they do not exist
func (c *ControlDB) InitTables() {
	var err error

	// Check if ./data directory exists
	if _, err := os.Stat("./data/"); err != nil {
		if os.IsNotExist(err) {
			// Create ./data directory
			err = os.Mkdir("data", 0755) // r, x by all, w by owner

			if err != nil {
				log.Logger.Fatal("Failed to create data directory", zap.String("Error", err.Error()))
			}
		} else {
			log.Logger.Fatal("Failed to access data directory", zap.String("Error", err.Error()))
		}
	}

	// Open DB file. Creates file if it doesn't exist.
	c.db, err = sql.Open("sqlite3", "./data/vaero.db")
	if err != nil {
		log.Logger.Fatal("Failed to open database", zap.String("Error", err.Error()))
	}

	sqlStmt := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (id INTEGER NOT NULL PRIMARY KEY, interval INTEGER,
			task_graph TEXT, spec TEXT, status TEXT CHECK( status IN ("running", "staged", "stopped", "stopping") ), alive INTEGER);
		`, jobsTable)

	_, err = c.db.Exec(sqlStmt)
	if err != nil {
		log.Logger.Fatal("Create table failed", zap.String("Error", err.Error()))
	}
}

// AddHandler adds the job as staged
func (c *ControlDB) AddHandler(specName string) {

	// Check if spec file exists
	if _, err := os.Stat(specName); err != nil {
		if os.IsNotExist(err) {
			log.Logger.Fatal("File not found", zap.String("Filename", specName))
		} else {
			log.Logger.Fatal("Could not open file", zap.String("Error", err.Error()))
		}
	}

	// Run the Python spec file and read stdout
	moduleName := convertToModuleName(specName) // to import sibling or higher modules, we need to use python -m flag and module name format

	log.Logger.Info("Converted file name to module name", zap.String("file name", specName), zap.String("module name", moduleName))

	// Generate command
	cmd := exec.Command("python", "-m", moduleName)

	// Activate virtual environment if selected
	if settings.Config.PythonPath != "" {
		cmd.Path = filepath.Join(settings.Config.PythonPath, "python")
	}

	// Run command
	output, err := cmd.Output()

	if err != nil {
		log.Logger.Fatal("Could not run Python pipeline specification", zap.String("Error", err.Error()))
	}
	taskGraphStr := string(output)

	// Add to pipelines database
	sqlStmt := fmt.Sprintf(`
		INSERT INTO %s (interval, task_graph, spec, status, alive)
		values(?, ?, ?, ?, ?)
		`, jobsTable)

	stmt, err := c.db.Prepare(sqlStmt)
	defer stmt.Close()

	// Read interval
	interval := gjson.Get(taskGraphStr, "0.args.interval").Int() // Interval is stored in the first task, which is the source
	status := "staged"
	alive := 1

	_, err = stmt.Exec(interval, taskGraphStr, specName, status, alive)
	if err != nil {
		log.Logger.Fatal("Could not add pipeline to database", zap.String("Error", err.Error()))
	}

	// output
	fmt.Printf("Added pipeline from %s \n", specName)
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', tabwriter.Debug)
	fmt.Fprintf(w, "Interval\tTask Graph\tFile\tStatus\tAlive\n")
	fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%d\n", interval, taskGraphStr, specName, status, alive)
	w.Flush()
}

// convertToModuleName converts a file path to be usable with python -m flag
// it converts path/pipe.py to path.pipe
func convertToModuleName(specName string) string {

	// remove file extension
	r := regexp.MustCompile(`\.[^.]{1,3}$`)
	result := r.ReplaceAllLiteralString(specName, ``)

	// convert / to .
	result = strings.Replace(result, `/`, `.`, -1)

	return result
}

// DeleteHandler deletes the job with id. If not found, do nothing.
func (c *ControlDB) DeleteHandler(id int) {

	entry, ok := c.selectFromJobsDB(id)

	if !ok {
		fmt.Printf("Pipeline %d not found\n", id)
		return
	}

	// Stop a pipeline if running, otherwise it will be orphaned when deleted
	if entry.Status == "running" {
		c.StopHandler(id)
	}

	for {
		entry, ok = c.selectFromJobsDB(id)

		// Check if stopped
		if entry.Status == "stopped" || entry.Status == "staged" {

			// Delete
			sqlStmt := fmt.Sprintf(`
			DELETE FROM %s WHERE id = ?
			`, jobsTable)

			stmt, err := c.db.Prepare(sqlStmt)
			defer stmt.Close()

			_, err = stmt.Exec(id)
			if err != nil {
				log.Logger.Error("Could not delete pipeline from database", zap.String("Error", err.Error()))
			}

			// output
			fmt.Printf("Deleted pipeline %d\n", id)
			break
		} else if !ok { // Error if job is not found
			break
		}

		time.Sleep(time.Second)
	}
}

// DetailHandler displays the details of the job with id. If not found, it displays a not found message.
func (c *ControlDB) DetailHandler(id int) {

	entry, ok := c.selectFromJobsDB(id)

	if !ok {
		fmt.Printf("Pipeline %d not found\n", id)
		return
	}

	singleEntryArr := []PipelineEntry{entry}

	printPipelineEntriesTable(singleEntryArr)
}

// ListHandler lists all jobs
func (c *ControlDB) ListHandler() {

	pipelineEntries := c.selectAllFromJobsDB()

	printPipelineEntriesTable(pipelineEntries)
}

// StartHandler starts all jobs that are staged
func (c *ControlDB) StartHandler() {

	// Run admin routine to poll jobs table and update running pipelines
	go adminRoutine(c)

	// WAIT FOREVER (all goroutines will die when the main routine ends)
	//var input string
	//fmt.Scanln(&input)
	exit := make(chan string)
	<-exit
}

// StopHandler stops the job with id by setting alive to 0. If not found, do nothing.
func (c *ControlDB) StopHandler(id int) {
	// Check if job exists
	_, ok := c.selectFromJobsDB(id)

	if !ok {
		fmt.Printf("Pipeline %d not found\n", id)
		return
	}

	// Stop job
	sqlStmt := fmt.Sprintf(`
		UPDATE %s SET status = ?, alive = ? WHERE id = ?
		`, jobsTable)

	stmt, err := c.db.Prepare(sqlStmt)
	defer stmt.Close()

	_, err = stmt.Exec("stopping", 0, id)
	if err != nil {
		log.Logger.Fatal("Update row failed", zap.String("Error", err.Error()))
	}

	// output
	fmt.Printf("Stopping pipeline %d\n", id)
}

// adminRoutine is a long running goroutine that regularly checks the jobs table for new, stopped, or deleted jobs
// and applies those changes
func adminRoutine(c *ControlDB) {
	for {
		entries := c.selectAllFromJobsDB()

		for _, entry := range entries {
			if entry.Status == "staged" {
				/*
					log.Logger.Info("Start new run",
						zap.Int("id", entry.Id),
						zap.Int("interval", entry.Interval),
						zap.String("taskGraph", entry.TaskGraphStr),
						zap.String("spec", entry.Spec),
						zap.String("status", entry.Status),
						zap.Int("alive", entry.Alive),
					)
				*/
				fmt.Printf("Starting pipeline %d %d %s %s %s %d\n", entry.Id,
					entry.Interval, entry.TaskGraphStr, entry.Spec, entry.Status, entry.Alive)

				taskGraph := genTaskGraph(entry.TaskGraphStr)

				// Initiate run here
				executor.RunJob(entry.Id, entry.Interval, taskGraph)

				c.updateJobStatus(entry.Id, "running")
			} else if entry.Status == "stopping" {
				// Stop job here
				executor.StopJob(entry.Id)

				c.updateJobStatus(entry.Id, "stopped")
				fmt.Printf("Stopped pipeline %d\n", entry.Id)
			}
		}

		// Poll frequency
		time.Sleep(time.Duration(settings.Config.PollPipelineChangesFreq) * time.Second)
	}
}

// printPipelineEntriesTable prints a table to display pipeline entries
func printPipelineEntriesTable(entries []PipelineEntry) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', tabwriter.Debug)
	fmt.Fprintf(w, "Id\tInterval\tTask Graph\tFile\tStatus\tAlive\n")
	for _, entry := range entries {
		fmt.Fprintf(w, "%d\t%d\t%s\t%s\t%s\t%d\n", entry.Id, entry.Interval, entry.TaskGraphStr, entry.Spec, entry.Status, entry.Alive)
	}
	w.Flush()
}

// selectFromJobsDB returns the results of selecting of the jobs table
func (c *ControlDB) selectFromJobsDB(id int) (PipelineEntry, bool) {
	// Query
	sqlStmt := fmt.Sprintf(`
	SELECT * FROM %s WHERE id = %d
	`, jobsTable, id)

	rows, err := c.db.Query(sqlStmt)
	if err != nil {
		log.Logger.Fatal("Database query failed", zap.String("Error", err.Error()))
	}
	defer rows.Close()

	// Iterate on results
	var pipelineEntry PipelineEntry
	ok := false
	for rows.Next() {
		var id, interval, alive int
		var spec, status, taskGraphStr string
		err = rows.Scan(&id, &interval, &taskGraphStr, &spec, &status, &alive)
		if err != nil {
			log.Logger.Error("Failed to scan database row", zap.String("Error", err.Error()))
			continue
		}
		pipelineEntry = PipelineEntry{
			Id:           id,
			Interval:     interval,
			TaskGraphStr: taskGraphStr,
			Spec:         spec,
			Status:       status,
			Alive:        alive,
		}
		ok = true
	}
	err = rows.Err()
	if err != nil {
		log.Logger.Fatal("Failed to read database query results", zap.String("Error", err.Error()))
	}
	return pipelineEntry, ok
}

// selectAllFromJobsDB returns the results of a select all query on the jobs table
func (c *ControlDB) selectAllFromJobsDB() []PipelineEntry {
	// Query
	sqlStmt := fmt.Sprintf(`
	SELECT * FROM %s
	`, jobsTable)
	rows, err := c.db.Query(sqlStmt)
	if err != nil {
		log.Logger.Fatal("Database query failed", zap.String("Error", err.Error()))
	}
	defer rows.Close()

	// Iterate on results and display
	pipelineEntries := []PipelineEntry{}
	for rows.Next() {
		var id, interval, alive int
		var spec, status, taskGraphStr string
		err = rows.Scan(&id, &interval, &taskGraphStr, &spec, &status, &alive)
		if err != nil {
			log.Logger.Error("Failed to scan database row", zap.String("Error", err.Error()))
			continue
		}
		pipelineEntries = append(pipelineEntries, PipelineEntry{
			Id:           id,
			Interval:     interval,
			TaskGraphStr: taskGraphStr,
			Spec:         spec,
			Status:       status,
			Alive:        alive,
		})
	}
	err = rows.Err()
	if err != nil {
		log.Logger.Fatal("Failed to read database query results", zap.String("Error", err.Error()))
	}

	return pipelineEntries
}

// updateJobStatus updates the status of the specified job to the specified status
func (c *ControlDB) updateJobStatus(id int, status string) {
	sqlStmt := fmt.Sprintf(`
	UPDATE %s SET status = ? WHERE id = ?
	`, jobsTable)

	stmt, err := c.db.Prepare(sqlStmt)
	defer stmt.Close()

	_, err = stmt.Exec(status, id)
	if err != nil {
		log.Logger.Error("Update row failed", zap.String("Error", err.Error()))
	}
}

// genTaskGraph generates a task graph of OpTasks from a taskGraphStr
func genTaskGraph(taskGraphStr string) []execute.OpTask {
	jsonGraph := gjson.Parse(taskGraphStr).Value().([]interface{})

	taskGraph := genTaskGraphHelper(jsonGraph)

	//fmt.Printf("Task graph %v", taskGraph)

	return taskGraph
}

func genTaskGraphHelper(jsonGraph []interface{}) []execute.OpTask {
	taskGraph := []execute.OpTask{}
	for _, v := range jsonGraph {
		switch tk := v.(type) {
		case map[string]interface{}: // handle regular ops
			var op execute.OpTask
			mapstructure.Decode(tk, &op)
			op.Id = uuid.New()
			taskGraph = append(taskGraph, op)
		case []interface{}: // handle arrays, which represent branching
			var op execute.OpTask = execute.OpTask{Type: "branch", Branches: make([][]execute.OpTask, 0), Id: uuid.New()}

			for _, sub := range tk {
				op.Branches = append(op.Branches, genTaskGraphHelper(sub.([]interface{})))
			}
			taskGraph = append(taskGraph, op)
		}
	}

	return taskGraph
}
