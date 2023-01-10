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
		DefaultChanBufferLen: 1000,
		PythonVenv:           "",
	}

	// Read into global settings
	if _, err := toml.DecodeFile(configFile, &settings.Config); err != nil {
		log.Logger.Fatal("Could not read config file", zap.String("Filename", configFile))
	}

	fmt.Printf("Settings %v\n", settings.Config)
}

// CheckPython checks if Python3 is installed
func CheckPython() {

	// Run python
	cmd := exec.Command("python", "-V")

	// Activate virtual environment if selected
	if settings.Config.PythonVenv != "" {
		cmd.Path = filepath.Join(settings.Config.PythonVenv, "python")
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
	if settings.Config.PythonVenv != "" {
		cmd.Path = filepath.Join(settings.Config.PythonVenv, "python")
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
			task_graph TEXT, spec TEXT, status TEXT CHECK( status IN ("stopped", "staged", "running") ), alive INTEGER);
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
	if settings.Config.PythonVenv != "" {
		cmd.Path = filepath.Join(settings.Config.PythonVenv, "python")
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
	sqlStmt := fmt.Sprintf(`
		DELETE FROM %s WHERE id = ?
		`, jobsTable)

	stmt, err := c.db.Prepare(sqlStmt)
	defer stmt.Close()

	_, err = stmt.Exec(id)
	if err != nil {
		log.Logger.Fatal("Could not delete pipeline from database", zap.String("Error", err.Error()))
	}

	// output
	fmt.Printf("Deleted pipeline %d\n", id)
}

// DetailHandler displays the details of the job with id. If not found, it displays a not found message.
func (c *ControlDB) DetailHandler(id int) {
	// Query
	sqlStmt := fmt.Sprintf(`
		SELECT * FROM %s WHERE id = %d
		`, jobsTable, id)

	rows, err := c.db.Query(sqlStmt)
	if err != nil {
		log.Logger.Fatal("Database query failed", zap.String("Error", err.Error()))
	}
	defer rows.Close()

	// Iterate on results and display
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', tabwriter.Debug)
	fmt.Fprintf(w, "Id\tInterval\tTask Graph\tFile\tStatus\tAlive\n")
	for rows.Next() {
		var id, interval, alive int
		var spec, status, taskGraphStr string
		err = rows.Scan(&id, &interval, &taskGraphStr, &spec, &status, &alive)
		if err != nil {
			log.Logger.Fatal("Failed to scan database row", zap.String("Error", err.Error()))
		}
		//fmt.Printf("%d %d %d %s %s %d\n", id, interval, task_graph, spec, status, alive)
		fmt.Fprintf(w, "%d\t%d\t%s\t%s\t%s\t%d\n", id, interval, taskGraphStr, spec, status, alive)
	}
	w.Flush()
	err = rows.Err()
	if err != nil {
		log.Logger.Fatal("Failed to read database query results", zap.String("Error", err.Error()))
	}
}

// ListHandler lists all jobs
func (c *ControlDB) ListHandler() {
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
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', tabwriter.Debug)
	fmt.Fprintf(w, "Id\tInterval\tTask Graph\tFile\tStatus\tAlive\n")
	for rows.Next() {
		var id, interval, alive int
		var spec, status, taskGraphStr string
		err = rows.Scan(&id, &interval, &taskGraphStr, &spec, &status, &alive)
		if err != nil {
			log.Logger.Fatal("Failed to scan database row", zap.String("Error", err.Error()))
		}
		//fmt.Printf("%d %d %s %s %s %d\n", id, interval, taskGraphStr, spec, status, alive)
		fmt.Fprintf(w, "%d\t%d\t%s\t%s\t%s\t%d\n", id, interval, taskGraphStr, spec, status, alive)
	}
	w.Flush()
	err = rows.Err()
	if err != nil {
		log.Logger.Fatal("Failed to read database query results", zap.String("Error", err.Error()))
	}
}

// StartHandler starts all jobs that are staged
func (c *ControlDB) StartHandler() {
	// Query
	sqlStmt := fmt.Sprintf(`
		SELECT * FROM %s
		`, jobsTable)

	rows, err := c.db.Query(sqlStmt)
	if err != nil {
		log.Logger.Fatal("Database query failed", zap.String("Error", err.Error()))
	}
	defer rows.Close()

	fmt.Println("Starting log pipelines")

	// Iterate on results
	for rows.Next() {
		var id, interval, alive int
		var spec, status, taskGraphStr string
		err = rows.Scan(&id, &interval, &taskGraphStr, &spec, &status, &alive)
		if err != nil {
			log.Logger.Error("Failed to scan database row", zap.String("Error", err.Error()))
		}
		if status == "staged" {
			log.Logger.Info("Start new run",
				zap.Int("id", id),
				zap.Int("interval", interval),
				zap.String("taskGraph", taskGraphStr),
				zap.String("spec", spec),
				zap.String("status", status),
				zap.Int("alive", alive),
			)

			taskGraph := genTaskGraph(taskGraphStr)

			// Initiate run here
			executor.RunJob(interval, taskGraph)

			// Update status to running
			defer func(id int) {
				sqlStmt := fmt.Sprintf(`
				UPDATE %s SET status = "running" WHERE id = ?
				`, jobsTable)

				stmt, err := c.db.Prepare(sqlStmt)
				defer stmt.Close()

				_, err = stmt.Exec(id)
				if err != nil {
					log.Logger.Fatal("Update row failed", zap.String("Error", err.Error()))
				}
			}(id)
		}
	}
	err = rows.Err()
	if err != nil {
		log.Logger.Error("Failed to read database query results", zap.String("Error", err.Error()))
	}

	// WAIT
	var input string
	fmt.Scanln(&input)
}

// StopHandler stops the job with id by setting alive to 0. If not found, do nothing.
func (c *ControlDB) StopHandler(id int) {
	sqlStmt := fmt.Sprintf(`
		UPDATE %s SET status = ?, alive = ? WHERE id = ?
		`, jobsTable)

	stmt, err := c.db.Prepare(sqlStmt)
	defer stmt.Close()

	_, err = stmt.Exec("stopped", 0, id)
	if err != nil {
		log.Logger.Fatal("Update row failed", zap.String("Error", err.Error()))
	}

	// output
	fmt.Printf("Stopped pipeline %d\n", id)
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
