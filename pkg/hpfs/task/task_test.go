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
	"fmt"
	"os"
	"testing"

	"github.com/google/uuid"
)

const testFile = "/tmp/tm.db"

func runTaskManagerTest(t *testing.T, testFunc func(t *testing.T, tm Manager)) {
	tm, err := NewTaskManager(testFile)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(testFile)
	defer tm.Close()

	testFunc(t, tm)
}

func TestTaskManager_CreateTask(t *testing.T) {
	runTaskManagerTest(t, func(t *testing.T, tm Manager) {
		task := &Task{
			Operation: "DOWNLOAD",
			Details:   "{}",
		}

		if err := tm.CreateTask(task); err != nil {
			t.Fatalf("failed to create new task: " + err.Error())
		}
	})
}

func TestTaskManager_UpdateTaskStatusAndProgress(t *testing.T) {
	runTaskManagerTest(t, func(t *testing.T, tm Manager) {
		task := &Task{
			Operation: "DOWNLOAD",
			Details:   "{}",
		}

		if err := tm.CreateTask(task); err != nil {
			t.Fatalf("failed to create new task: " + err.Error())
		}

		task.Progress = 0
		task.Status = Error
		task.ErrMsg = "failed to run task"
		if err := tm.UpdateTaskStatus(task); err != nil {
			t.Fatalf("failed to update task: " + err.Error())
		}

		task.Progress = 0
		task.Status = Complete
		task.ErrMsg = ""
		if err := tm.UpdateTaskStatus(task); err != nil {
			t.Fatalf("failed to update task: " + err.Error())
		}
	})
}

func TestTaskManager_DeleteTask(t *testing.T) {
	runTaskManagerTest(t, func(t *testing.T, tm Manager) {
		task := &Task{
			Operation: "DOWNLOAD",
			Details:   "{}",
		}

		if err := tm.CreateTask(task); err != nil {
			t.Fatalf("failed to create new task: " + err.Error())
		}

		if err := tm.DeleteTask(task); err != nil {
			t.Fatalf("failed to delete task: " + err.Error())
		}
	})
}

func TestTaskManager_GetLastTaskByOperationAndCheckSum(t *testing.T) {
	runTaskManagerTest(t, func(t *testing.T, tm Manager) {
		task := &Task{
			Operation: "DOWNLOAD",
			Details:   "{}",
		}

		if err := tm.CreateTask(task); err != nil {
			t.Fatalf("failed to create new task: " + err.Error())
		}

		details := "{}"
		checksum := tm.GetCheckSum(details)
		task, err := tm.GetLastTaskByOperationAndCheckSum("DOWNLOAD", checksum)
		if err != nil {
			t.Fatalf("failed to get last task by operation and checksum: " + err.Error())
		}

		if task.Operation != "DOWNLOAD" || task.CheckSum != checksum && task.Details != details {
			t.Fatalf("task invalid")
		}
	})
}

func TestTaskManager_InsertDuplicateTraceId(t *testing.T) {
	runTaskManagerTest(t, func(t *testing.T, tm Manager) {
		task := &Task{
			Operation: "DOWNLOAD",
			Details:   "{}",
			TraceId:   uuid.New().String(),
		}

		if err := tm.CreateTask(task); err != nil {
			t.Fatalf("failed to create new task: " + err.Error())
		}

		if err := tm.CreateTask(task); err == nil {
			t.Fatalf("duplicate trace id but inserted, expect error")
		} else {
			fmt.Println("as expected: " + err.Error())
		}
	})
}

func TestTaskManager_ListTasks(t *testing.T) {
	runTaskManagerTest(t, func(t *testing.T, tm Manager) {
		task := &Task{
			Operation: "DOWNLOAD",
			Details:   "{}",
		}

		if err := tm.CreateTask(task); err != nil {
			t.Fatalf("failed to create new task: " + err.Error())
		}

		tasks, err := tm.ListTasks(Pending)
		if err != nil {
			t.Fatalf("failed to list tasks: " + err.Error())
		}

		if len(tasks) == 0 {
			t.Fatalf("no tasks with pending status")
		}
	})
}
