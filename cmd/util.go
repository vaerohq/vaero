package cmd

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

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

// CheckPython checks if Python3 is installed
func CheckPython() {

	// Run python
	cmd := exec.Command("python", "-V")

	// Activate virtual environment if selected
	if settings.PythonVenv != "" {
		cmd.Path = filepath.Join(settings.PythonVenv, "python")
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
				log.Logger.Fatal(err.Error())
			}
		} else {
			log.Logger.Fatal(err.Error())
		}
	}

	// Open DB file. Creates file if it doesn't exist.
	c.db, err = sql.Open("sqlite3", "./data/vaero.db")
	if err != nil {
		log.Logger.Fatal(err.Error())
	}

	sqlStmt := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (id INTEGER NOT NULL PRIMARY KEY, interval INTEGER,
			task_graph TEXT, spec TEXT, status TEXT CHECK( status IN ("staged", "running") ), alive INTEGER);
		`, jobsTable)

	_, err = c.db.Exec(sqlStmt)
	if err != nil {
		log.Logger.Fatal(err.Error())
	}
}

// AddHandler adds the job as staged
func (c *ControlDB) AddHandler(specName string) {

	// Check if spec file exists
	if _, err := os.Stat(specName); err != nil {
		if os.IsNotExist(err) {
			log.Logger.Fatal("File not found", zap.String("Filename", specName))
		} else {
			log.Logger.Fatal(err.Error())
		}
	}

	// Run the Python spec file and read stdout
	moduleName := convertToModuleName(specName) // to import sibling or higher modules, we need to use python -m flag and module name format

	log.Logger.Info("Converted file name to module name", zap.String("file name", specName), zap.String("module name", moduleName))

	// Generate command
	cmd := exec.Command("python", "-m", moduleName)

	// Activate virtual environment if selected
	if settings.PythonVenv != "" {
		cmd.Path = filepath.Join(settings.PythonVenv, "python")
	}

	// Run command
	output, err := cmd.Output()

	if err != nil {
		log.Logger.Fatal(err.Error())
	}
	taskGraphStr := string(output)
	log.Logger.Info("Generated task graph", zap.String("task graph", taskGraphStr))

	// Add back later
	sqlStmt := fmt.Sprintf(`
		INSERT INTO %s (interval, task_graph, spec, status, alive)
		values(?, ?, ?, ?, ?)
		`, jobsTable)

	stmt, err := c.db.Prepare(sqlStmt)
	defer stmt.Close()

	_, err = stmt.Exec(10, taskGraphStr, specName, "staged", 1)
	if err != nil {
		log.Logger.Fatal(err.Error())
	}
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
		log.Logger.Fatal(err.Error())
	}
}

// DetailHandler displays the details of the job with id. If not found, it displays a not found message.
func (c *ControlDB) DetailHandler(id int) {
	sqlStmt := fmt.Sprintf(`
		SELECT * FROM %s WHERE id = %d
		`, jobsTable, id)

	rows, err := c.db.Query(sqlStmt)
	if err != nil {
		log.Logger.Fatal(err.Error())
	}
	defer rows.Close()
	for rows.Next() {
		var id, interval, task_graph, alive int
		var spec, status string
		err = rows.Scan(&id, &interval, &task_graph, &spec, &status, &alive)
		if err != nil {
			log.Logger.Fatal(err.Error())
		}
		fmt.Printf("%d %d %d %s %s %d\n", id, interval, task_graph, spec, status, alive)
	}
	err = rows.Err()
	if err != nil {
		log.Logger.Fatal(err.Error())
	}
}

// ListHandler lists all jobs
func (c *ControlDB) ListHandler() {
	sqlStmt := fmt.Sprintf(`
		SELECT * FROM %s
		`, jobsTable)

	rows, err := c.db.Query(sqlStmt)
	if err != nil {
		log.Logger.Fatal(err.Error())
	}
	defer rows.Close()
	for rows.Next() {
		var id, interval, alive int
		var spec, status, taskGraphStr string
		err = rows.Scan(&id, &interval, &taskGraphStr, &spec, &status, &alive)
		if err != nil {
			log.Logger.Fatal(err.Error())
		}
		fmt.Printf("%d %d %s %s %s %d\n", id, interval, taskGraphStr, spec, status, alive)
	}
	err = rows.Err()
	if err != nil {
		log.Logger.Fatal(err.Error())
	}
}

// StartHandler starts all jobs that are staged
func (c *ControlDB) StartHandler() {
	sqlStmt := fmt.Sprintf(`
		SELECT * FROM %s
		`, jobsTable)

	rows, err := c.db.Query(sqlStmt)
	if err != nil {
		log.Logger.Fatal(err.Error())
	}
	defer rows.Close()
	for rows.Next() {
		var id, interval, alive int
		var spec, status, taskGraphStr string
		err = rows.Scan(&id, &interval, &taskGraphStr, &spec, &status, &alive)
		if err != nil {
			log.Logger.Fatal(err.Error())
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
					log.Logger.Fatal(err.Error())
				}
			}(id)
		}
	}
	err = rows.Err()
	if err != nil {
		log.Logger.Fatal(err.Error())
	}

	// WAIT
	var input string
	fmt.Scanln(&input)
}

// StopHandler stops the job with id by setting alive to 0. If not found, do nothing.
func (c *ControlDB) StopHandler(id int) {
	sqlStmt := fmt.Sprintf(`
		UPDATE %s SET alive = ? WHERE id = ?
		`, jobsTable)

	stmt, err := c.db.Prepare(sqlStmt)
	defer stmt.Close()

	_, err = stmt.Exec(0, id)
	if err != nil {
		log.Logger.Fatal(err.Error())
	}
}

// genTaskGraph generates a task graph of OpTasks from a taskGraphStr
func genTaskGraph(taskGraphStr string) []execute.OpTask {
	jsonGraph := gjson.Parse(taskGraphStr).Value().([]interface{})

	taskGraph := genTaskGraphHelper(jsonGraph)

	fmt.Printf("Task graph %v", taskGraph)

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
