/*
Copyright 2022 Alibaba Group Holding Limited.

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

package exec

import (
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/change/driver/context"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/change/driver/plan"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
)

type Executor interface {
	Step() *plan.Step
	Done() bool
	Execute(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error)
}

type baseExec struct {
	ec    *context.ExecutionContext
	step  plan.Step
	index int
}

func (exec *baseExec) MarkDone() {
	exec.ec.MarkStepIndex(exec.index + 1)
}

func (exec *baseExec) Done() bool {
	return exec.ec.StepIndex > exec.index
}

func (exec *baseExec) Step() *plan.Step {
	return &exec.step
}

func newBaseExec(ec *context.ExecutionContext, index int, step plan.Step) baseExec {
	return baseExec{
		ec:    ec,
		step:  step,
		index: index,
	}
}

func NewExecutor(ec *context.ExecutionContext, index int, step plan.Step) Executor {
	switch step.Type {
	case plan.StepTypeSnapshot:
		return &SnapshotExec{baseExec: newBaseExec(ec, index, step)}
	case plan.StepTypeBumpGen:
		return &BumpGenExec{baseExec: newBaseExec(ec, index, step)}
	case plan.StepTypeUpdate:
		return &UpdateExec{baseExec: newBaseExec(ec, index, step)}
	case plan.StepTypeReplace:
		return &ReplaceExec{baseExec: newBaseExec(ec, index, step)}
	case plan.StepTypeCreate:
		return &CreateExec{baseExec: newBaseExec(ec, index, step)}
	case plan.StepTypeDelete:
		return &DeleteExec{baseExec: newBaseExec(ec, index, step)}
	default:
		panic("undefined step type")
	}
}

func NewExecutorsForPlan(ec *context.ExecutionContext, plan plan.Plan) []Executor {
	executors := make([]Executor, len(plan.Steps))
	for i := range plan.Steps {
		executors[i] = NewExecutor(ec, i, plan.Steps[i])
	}
	return executors
}
