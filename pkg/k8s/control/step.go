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

package control

import (
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type ExecuteFunc = func(rc ReconcileContext, flow Flow) (reconcile.Result, error)

type Step interface {
	Name() string
	Execute(rc ReconcileContext, flow Flow) (reconcile.Result, error)
}

type Task struct {
	steps             []Step
	deferredSteps     []Step
	stepIndex         int
	deferredStepIndex int
}

func (t *Task) newStep(step Step) *Task {
	t.steps = append(t.steps, step)
	return t
}

func (t *Task) newDeferredStep(step Step) *Task {
	t.deferredSteps = append(t.deferredSteps, step)
	return t
}

func (t *Task) hasNextStep() bool {
	return t.stepIndex < len(t.steps)
}

func (t *Task) hasNextDeferredStep() bool {
	return t.deferredStepIndex < len(t.deferredSteps)
}

func (t *Task) nextStep() Step {
	step := t.steps[t.stepIndex]
	t.stepIndex++
	return step
}

func (t *Task) nextDeferredStep() Step {
	step := t.deferredSteps[t.deferredStepIndex]
	t.deferredStepIndex++
	return step
}

func NewTask() *Task {
	return &Task{
		steps:             make([]Step, 0),
		deferredSteps:     make([]Step, 0),
		stepIndex:         0,
		deferredStepIndex: 0,
	}
}

type step struct {
	name string
	f    ExecuteFunc
}

func (s *step) Name() string {
	return s.name
}

func (s *step) Execute(rc ReconcileContext, flow Flow) (reconcile.Result, error) {
	return s.f(rc, flow)
}

func NewStep(name string, f ExecuteFunc) Step {
	return &step{
		name: name,
		f:    f,
	}
}

type BindFunc func(t *Task, deferred ...bool)

func NewStepBinder(step Step) BindFunc {
	return func(t *Task, deferred ...bool) {
		if len(deferred) > 0 && deferred[0] {
			t.newDeferredStep(step)
		} else {
			t.newStep(step)
		}
	}
}

func ExtractStepsFromBindFunc(binders ...BindFunc) []Step {
	task := &Task{}
	for _, b := range binders {
		b(task)
	}
	return task.steps
}

func CombineBinders(binders ...BindFunc) BindFunc {
	return func(t *Task, deferred ...bool) {
		for _, b := range binders {
			b(t, deferred...)
		}
	}
}

func When(cond bool, a ...BindFunc) BindFunc {
	return func(t *Task, deferred ...bool) {
		if cond {
			Block(a...)(t, deferred...)
		}
	}
}

func Branch(cond bool, a, b BindFunc) BindFunc {
	return func(t *Task, deferred ...bool) {
		if cond {
			a(t, deferred...)
		} else {
			b(t, deferred...)
		}
	}
}

func Block(binds ...BindFunc) BindFunc {
	if len(binds) == 1 {
		return binds[0]
	}
	return func(t *Task, deferred ...bool) {
		for _, bind := range binds {
			bind(t, deferred...)
		}
	}
}
