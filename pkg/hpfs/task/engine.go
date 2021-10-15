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
	"errors"
	"fmt"
	"sync"

	"github.com/go-logr/logr"
)

var (
	ErrTaskNotFound     = errors.New("task not found")
	ErrTaskNotSubmitted = errors.New("task not submitted")
	ErrTaskCanceled     = errors.New("task canceled")
	ErrTaskAlreadyRun   = errors.New("task already completed or failed")
)

type RunFunc func(ctx context.Context, t *Task) error

type runTask struct {
	task   Task
	ctx    context.Context
	cancel context.CancelFunc
	run    RunFunc
}

// Task status FSM:
//     Pending -> Running -> Complete
//                        -> Error
//                        -> Canceling -> Cancel
//             -> Cancel
type Engine interface {
	Recover() error
	Get(traceId string) (*Task, error)
	Submit(t *Task) (*Task, error)
	Wait(t *Task) error
	Cancel(t *Task) error
}

type engine struct {
	logger  logr.Logger
	mgr     Manager
	mu      sync.Mutex
	running map[int64]*runTask
	runFn   RunFunc
}

func (e *engine) Recover() error {
	e.logger.Info("recovering tasks...")

	e.mu.Lock()
	defer e.mu.Unlock()

	// Re-submit all pending and running tasks.
	tasks, err := e.mgr.ListTasks(Pending, Running)
	if err != nil {
		return err
	}

	taskIds := make([]int64, len(tasks))
	for i := range tasks {
		taskIds[i] = tasks[i].Id
	}
	e.logger.Info("found tasks to recover", "cnt", len(tasks), "task-ids", taskIds)

	for i := range tasks {
		if err = e.submit(&tasks[i], true); err != nil {
			return err
		}
	}

	e.logger.Info("all tasks resubmitted")

	return nil
}

func (e *engine) isTaskSubmitted(t *Task) bool {
	_, ok := e.running[t.Id]
	return ok
}

func (e *engine) clean(rt *runTask) {
	e.mu.Lock()
	defer e.mu.Unlock()

	delete(e.running, rt.task.Id)
}

func (e *engine) run(rt *runTask) {
	logger := e.logger.WithValues("id", rt.task.Id, "trace-id", rt.task.TraceId)

	defer e.clean(rt)

	// Try start.
	logger.Info("trying to start task", "task", rt.task)

	rt.task.Status = Running
	ok, err := e.mgr.CasTaskStatus(&rt.task, Pending)
	if err != nil {
		logger.Error(err, "failed to start task")
		panic(err)
	}
	if !ok && rt.task.Status != Running {
		panic(fmt.Sprintf("task status invalid: %d", rt.task.Status))
	}

	handleTaskCompletePersistent := func(ok bool, err error) {
		if err != nil {
			logger.Error(err, "failed to update task's status")
			panic(err)
		}
		if !ok {
			logger.Info("failed to update task's status", "task-status", rt.task.Status)

			if rt.task.Status != Canceling {
				panic(fmt.Sprintf("task status invalid: %d", rt.task.Status))
			} else {
				logger.Info("cancel task in canceling status", "task-status", rt.task.Status)

				rt.task.Status = Cancel
				err = e.mgr.UpdateTaskStatus(&rt.task)
				if err != nil {
					logger.Error(err, "failed to update task's status")
					panic(err)
				}
			}
		}
	}

	// Run and handle error.
	t := rt.task
	if err := rt.run(rt.ctx, &t); err != nil {
		logger.Error(err, "failed to run task, try update the task status to error")

		rt.task.Status = Error
		rt.task.ErrMsg = err.Error()
		ok, err = e.mgr.CasTaskStatus(&rt.task, Running)
		handleTaskCompletePersistent(ok, err)

		logger.Info("task complete with error!")
		return
	} else {
		rt.task.Status = Complete
		rt.task.Progress = 100
		ok, err = e.mgr.CasTaskStatus(&rt.task, Running)
		handleTaskCompletePersistent(ok, err)

		logger.Info("task completed!")
	}
}

func (e *engine) Get(traceId string) (*Task, error) {
	return e.mgr.GetTaskByTraceId(traceId)
}

func (e *engine) checkTraceId(t *Task) {
	if len(t.TraceId) == 0 {
		panic("trace id's empty")
	}
}

func (e *engine) checkEquals(before, now *Task) error {
	if before.Operation != now.Operation ||
		before.TraceId != now.TraceId ||
		before.Details != now.Details {
		return errors.New("task not equal")
	}
	return nil
}

func (e *engine) submit(t *Task, recover bool) error {
	// Should be invoked while e.mu locked.

	// Return immediately if task is already submitted
	// and running.
	if e.isTaskSubmitted(t) {
		return nil
	}

	// Otherwise check the status.
	switch t.Status {
	case Running:
		if !recover {
			panic("never reach here")
		}
		break
	case Canceling:
		if !recover {
			panic("never reach here")
		}
		// Just update to cancel if it's canceling
		t.Status = Cancel
		if err := e.mgr.UpdateTaskStatus(t); err != nil {
			return err
		}
	case Complete, Error:
		return ErrTaskAlreadyRun
	case Cancel:
		return ErrTaskCanceled
	case Pending:
		break
	default:
		panic(fmt.Sprintf("unrecognized task status: %d", t.Status))
	}

	// Start the task.
	ctx, cancel := context.WithCancel(context.Background())
	rt := &runTask{
		task:   *t,
		ctx:    ctx,
		cancel: cancel,
		run:    e.runFn,
	}
	e.running[rt.task.Id] = rt

	go e.run(rt)

	return nil
}

func (e *engine) Submit(t *Task) (*Task, error) {
	e.checkTraceId(t)

	// If task exists, check and use the before task. Otherwise create one.
	before, err := e.Get(t.TraceId)
	if err != nil {
		return nil, err
	}
	if before != nil {
		err := e.checkEquals(before, t)
		if err != nil {
			return nil, err
		}
		t = before
	} else {
		err := e.mgr.CreateTask(t)
		if err != nil {
			return nil, err
		}
	}

	// Lock to avoid concurrent modification.
	e.mu.Lock()
	defer e.mu.Unlock()

	// Do submit.
	if err = e.submit(t, false); err != nil {
		return nil, err
	}

	return t, err
}

func (e *engine) getRunTask(t *Task) *runTask {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.running[t.Id]
}

func (e *engine) Wait(t *Task) error {
	e.mu.Lock()

	// In memory always newer than in manager.
	rt := e.running[t.Id]
	if rt != nil {
		e.mu.Unlock()
		<-rt.ctx.Done()
		return rt.ctx.Err()
	}

	// Currently locked, so the task can not be submitted.
	defer e.mu.Unlock()

	// Now either not submitted or completed.
	t, err := e.mgr.GetTaskById(t.Id)
	if err != nil {
		return err
	}
	if t == nil {
		return ErrTaskNotFound
	}

	switch t.Status {
	case Complete, Cancel, Canceling:
		return nil
	case Error:
		return errors.New(t.ErrMsg)
	case Pending:
		return ErrTaskNotSubmitted
	case Running:
		panic("never reach here.")
	default:
		panic(fmt.Sprintf("unrecognized task status: %d", t.Status))
	}
}

func (e *engine) Cancel(t *Task) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Currently locked, so the task can not be submitted.
	// Now either not submitted or completed.
	t, err := e.mgr.GetTaskById(t.Id)
	if err != nil {
		return err
	}
	if t == nil {
		return ErrTaskNotFound
	}

	logger := e.logger.WithValues("id", t.Id, "trace-id", t.TraceId, "status", t.Status)
	doCancel := func() {
		rt := e.running[t.Id]
		if rt != nil {
			logger.Info("cancel with context cancel func...")
			rt.cancel()
		}
	}

	switch t.Status {
	case Complete, Cancel, Error:
		return nil
	case Pending:
		t.Status = Cancel
		if err = e.mgr.UpdateTaskStatus(t); err != nil {
			return err
		}
		return nil
	case Running:
		t.Status = Canceling
		if err = e.mgr.UpdateTaskStatus(t); err != nil {
			return err
		}

		doCancel()
		return nil
	case Canceling:
		doCancel()
		return nil
	default:
		panic(fmt.Sprintf("unrecognized task status: %d", t.Status))
	}
}

func NewEngine(mgr Manager, runFn RunFunc, logger logr.Logger) Engine {
	return &engine{
		logger:  logger,
		mgr:     mgr,
		running: make(map[int64]*runTask),
		runFn:   runFn,
	}
}
