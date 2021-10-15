/*
Copyright 2021 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package task

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	_ "modernc.org/sqlite"
)

const (
	taskTableInitStmt = `CREATE TABLE IF NOT EXISTS task (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    gmt_created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    gmt_modified TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, 
    trace_id VARCHAR(128) NOT NULL UNIQUE,
    operation VARCHAR(64) NOT NULL, 
    details TEXT NOT NULL,
    checksum VARCHAR(128) NOT NULL,
    progress INTEGER NOT NULL DEFAULT 0,
    status INTEGER NOT NULL DEFAULT 0,
    error_msg TEXT NOT NULL DEFAULT ''
)`

	taskModifiedTimeTriggerInitStmt = `CREATE TRIGGER IF NOT EXISTS task_gmt_modified_trigger AFTER UPDATE ON task
BEGIN UPDATE task SET gmt_modified = CURRENT_TIMESTAMP WHERE id = NEW.id; END`
)

type TaskStatus int

const (
	Pending   TaskStatus = 0
	Running   TaskStatus = 1
	Complete  TaskStatus = 2
	Error     TaskStatus = 3
	Canceling TaskStatus = 4
	Cancel    TaskStatus = 5
)

type Task struct {
	Id          int64
	GmtCreated  time.Time
	GmtModified time.Time
	TraceId     string
	Operation   string
	Details     string
	CheckSum    string
	Progress    int
	Status      TaskStatus
	ErrMsg      string
}

func (t *Task) String() string {
	b, _ := json.Marshal(t)
	return string(b)
}

type Manager interface {
	GetCheckSum(s string) string
	CreateTask(task *Task) error
	UpdateTaskStatus(task *Task) error
	CasTaskStatus(task *Task, old TaskStatus) (bool, error)
	GetTaskById(id int64) (*Task, error)
	GetTaskByTraceId(traceId string) (*Task, error)
	GetLastTaskByOperationAndCheckSum(op, checksum string) (*Task, error)
	DeleteTask(task *Task) error
	ListTasks(status ...TaskStatus) ([]Task, error)
	Close() error
}

type taskManager struct {
	dbFile string
	db     *sql.DB
}

func (t *taskManager) init() error {
	db, err := t.getDb()
	if err != nil {
		return err
	}

	if _, err = db.Exec(taskTableInitStmt); err != nil {
		return err
	}

	if _, err = db.Exec(taskModifiedTimeTriggerInitStmt); err != nil {
		return err
	}

	return nil
}

func (t *taskManager) getDb() (*sql.DB, error) {
	if t.db == nil {
		db, err := sql.Open("sqlite", t.dbFile)
		if err != nil {
			return nil, err
		}
		t.db = db
	}
	return t.db, nil
}

func (t *taskManager) checksum(s string) string {
	h := sha1.New()
	_, _ = io.WriteString(h, s)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (t *taskManager) GetCheckSum(s string) string {
	return t.checksum(s)
}

func (t *taskManager) CreateTask(task *Task) error {
	db, err := t.getDb()
	if err != nil {
		return err
	}

	if len(task.TraceId) == 0 {
		task.TraceId = uuid.New().String()
	}
	task.Progress = 0
	task.Status = Pending
	task.CheckSum = t.checksum(task.Details)

	rs, err := db.Exec(`INSERT INTO task (trace_id, operation, details, checksum, progress, status) VALUES (?, ?, ?, ?, ?, ?)`,
		task.TraceId, task.Operation, task.Details, task.CheckSum, task.Progress, task.Status)
	if err != nil {
		return err
	}

	id, err := rs.LastInsertId()
	if err != nil {
		return err
	}
	task.Id = id

	return nil
}

func (t *taskManager) UpdateTaskStatus(task *Task) error {
	db, err := t.getDb()
	if err != nil {
		return err
	}

	rs, err := db.Exec(`UPDATE task SET status = ?, progress = ?, error_msg = ? WHERE id = ?`,
		task.Status, task.Progress, task.ErrMsg, task.Id)
	if err != nil {
		return err
	}
	rowsAffected, err := rs.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return errors.New("task not found")
	}
	return nil
}

func (t *taskManager) CasTaskStatus(task *Task, old TaskStatus) (bool, error) {
	db, err := t.getDb()
	if err != nil {
		return false, err
	}

	// Begin an RC transaction
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return false, err
	}

	rs, err := tx.Exec(`UPDATE task SET status = ?, progress = ?, error_msg = ? WHERE id = ? AND status = ?`,
		task.Status, task.Progress, task.ErrMsg, task.Id, old)
	if err != nil {
		_ = tx.Commit()
		return false, err
	}
	rowsAffected, err := rs.RowsAffected()
	if err != nil {
		_ = tx.Commit()
		return false, err
	}
	if rowsAffected == 0 {
		r := tx.QueryRow(`SELECT status FROM task WHERE id = ?`, task.Id)
		if err := r.Scan(&task.Status); err != nil {
			return false, err
		}
		_ = tx.Commit()
		return false, nil
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	return true, err
}

func (t *taskManager) GetTaskById(id int64) (*Task, error) {
	db, err := t.getDb()
	if err != nil {
		return nil, err
	}

	task := &Task{}
	err = db.QueryRow(`SELECT id, gmt_created, gmt_modified, trace_id, operation, details, checksum, progress, status, error_msg FROM task 
WHERE id = ?`, id).Scan(&task.Id, &task.GmtCreated, &task.GmtModified, &task.TraceId, &task.Operation, &task.Details, &task.CheckSum, &task.Progress, &task.Status, &task.ErrMsg)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return task, nil
}

func (t *taskManager) GetTaskByTraceId(traceId string) (*Task, error) {
	db, err := t.getDb()
	if err != nil {
		return nil, err
	}

	task := &Task{}
	err = db.QueryRow(`SELECT id, gmt_created, gmt_modified, trace_id, operation, details, checksum, progress, status, error_msg FROM task 
WHERE trace_id = ?`, traceId).Scan(&task.Id, &task.GmtCreated, &task.GmtModified, &task.TraceId, &task.Operation, &task.Details, &task.CheckSum, &task.Progress, &task.Status, &task.ErrMsg)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return task, nil
}

func (t *taskManager) GetLastTaskByOperationAndCheckSum(op, checksum string) (*Task, error) {
	db, err := t.getDb()
	if err != nil {
		return nil, err
	}

	task := &Task{}
	err = db.QueryRow(`SELECT id, gmt_created, gmt_modified, trace_id, operation, details, checksum, progress, status, error_msg FROM task 
WHERE operation = ? AND checksum = ? ORDER BY gmt_created DESC LIMIT 1`, op, checksum).
		Scan(&task.Id, &task.GmtCreated, &task.GmtModified, &task.TraceId, &task.Operation, &task.Details, &task.CheckSum, &task.Progress, &task.Status, &task.ErrMsg)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return task, nil
}

func (t *taskManager) DeleteTask(task *Task) error {
	db, err := t.getDb()
	if err != nil {
		return err
	}

	_, err = db.Exec(`DELETE FROM task WHERE id = ?`, task.Id)
	return err
}

func (t *taskManager) ListTasks(status ...TaskStatus) ([]Task, error) {
	if len(status) == 0 {
		return make([]Task, 0), nil
	}

	db, err := t.getDb()
	if err != nil {
		return nil, err
	}

	values := make([]string, len(status))
	for i := range status {
		values[i] = strconv.FormatInt(int64(status[i]), 10)
	}

	stmt := fmt.Sprintf(`SELECT id, gmt_created, gmt_modified, trace_id, operation, details, checksum, progress, status, error_msg FROM task WHERE status IN (%s)`,
		strings.Join(values, ","))

	rs, err := db.Query(stmt)
	if err != nil {
		return nil, err
	}
	defer rs.Close()

	tasks := make([]Task, 0, 0)
	for rs.Next() {
		var task Task
		err = rs.Scan(&task.Id, &task.GmtCreated, &task.GmtModified, &task.TraceId, &task.Operation, &task.Details, &task.CheckSum, &task.Progress, &task.Status, &task.ErrMsg)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}

	return tasks, nil
}

func (t *taskManager) Close() error {
	if t.db != nil {
		return t.db.Close()
	}
	return nil
}

func newTaskManager(dbFile string) (*taskManager, error) {
	tm := &taskManager{
		dbFile: dbFile,
	}

	if err := tm.init(); err != nil {
		defer tm.Close()
		return nil, err
	}

	return tm, nil
}

func NewTaskManager(dbFile string) (Manager, error) {
	return newTaskManager(dbFile)
}
