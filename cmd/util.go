package cmd

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
	"github.com/vaerohq/vaero/execute"
	"github.com/vaerohq/vaero/log"
	"go.uber.org/zap"
)

// jobsTable is the name of the sql table for jobs
const jobsTable = "jobs"

type ControlDB struct {
	db *sql.DB
}

var c ControlDB

var executor execute.Executor

// InitTables creates Vaero's DB tables if they do not exist
func (c *ControlDB) InitTables() {
	var err error

	c.db, err = sql.Open("sqlite3", "./data/vaero.db")
	if err != nil {
		log.Logger.Fatal(err.Error())
	}

	sqlStmt := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (id INTEGER NOT NULL PRIMARY KEY, interval INTEGER,
			task_graph BLOB, spec TEXT, status TEXT CHECK( status IN ("staged", "running") ), alive INTEGER);
		`, jobsTable)

	_, err = c.db.Exec(sqlStmt)
	if err != nil {
		log.Logger.Fatal(err.Error())
	}

	/*
		sqlStmt := fmt.Sprintf(`
		SELECT name FROM sqlite_master WHERE type='table' and name='%s';
		`, jobsTable)

		rows, err := db.Query(sqlStmt)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		if rows.Next() {
			fmt.Println("Table found")
		} else {
			fmt.Println("Table not found")

			sqlStmt := fmt.Sprintf(`
			CREATE TABLE %s (id integer not null primary key, name text);;
			`, jobsTable)
		}
	*/
}

// AddHandler adds the job as staged
func (c *ControlDB) AddHandler(specName string) {
	sqlStmt := fmt.Sprintf(`
		INSERT INTO %s (interval, task_graph, spec, status, alive)
		values(?, ?, ?, ?, ?)
		`, jobsTable)

	stmt, err := c.db.Prepare(sqlStmt)
	defer stmt.Close()

	_, err = stmt.Exec(10, -1, specName, "staged", 1)
	if err != nil {
		log.Logger.Fatal(err.Error())
	}

	/*
		sqlStmt := `
		create table foo (id integer not null primary key, name text);
		delete from foo;
		`
		_, err = db.Exec(sqlStmt)
		if err != nil {
			log.Printf("%q: %s\n", err, sqlStmt)
			return
		}

		tx, err := db.Begin()
		if err != nil {
			log.Fatal(err)
		}
		stmt, err := tx.Prepare("insert into foo(id, name) values(?, ?)")
		if err != nil {
			log.Fatal(err)
		}
		defer stmt.Close()
		for i := 0; i < 100; i++ {
			_, err = stmt.Exec(i, fmt.Sprintf("こんにちは世界%03d", i))
			if err != nil {
				log.Fatal(err)
			}
		}
		err = tx.Commit()
		if err != nil {
			log.Fatal(err)
		}

		rows, err := db.Query("select id, name from foo")
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var id int
			var name string
			err = rows.Scan(&id, &name)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(id, name)
		}
		err = rows.Err()
		if err != nil {
			log.Fatal(err)
		}

		stmt, err = db.Prepare("select name from foo where id = ?")
		if err != nil {
			log.Fatal(err)
		}
		defer stmt.Close()
		var name string
		err = stmt.QueryRow("3").Scan(&name)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(name)

		_, err = db.Exec("delete from foo")
		if err != nil {
			log.Fatal(err)
		}

		_, err = db.Exec("insert into foo(id, name) values(1, 'foo'), (2, 'bar'), (3, 'baz')")
		if err != nil {
			log.Fatal(err)
		}

		rows, err = db.Query("select id, name from foo")
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var id int
			var name string
			err = rows.Scan(&id, &name)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(id, name)
		}
		err = rows.Err()
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("Add")
	*/
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
		var id, interval, taskGraph, alive int
		var spec, status string
		err = rows.Scan(&id, &interval, &taskGraph, &spec, &status, &alive)
		if err != nil {
			log.Logger.Fatal(err.Error())
		}
		if status == "staged" {
			//fmt.Printf("Start new run of: %d %d %d %s %s %d\n", id, interval, taskGraph, spec, status, alive)
			log.Logger.Info("Start new run",
				zap.Int("id", id),
				zap.Int("interval", interval),
				zap.Int("taskGraph", taskGraph),
				zap.String("spec", spec),
				zap.String("status", status),
				zap.Int("alive", alive),
			)

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
