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
	"fmt"

	"github.com/go-logr/logr"

	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type ConditionFunc func(rc ReconcileContext, log logr.Logger) (bool, error)

type Condition interface {
	Name() string
	Evaluate(rc ReconcileContext, log logr.Logger) (bool, error)
}

type stepIf struct {
	cond Condition
	step Step
}

func (s *stepIf) Name() string {
	return fmt.Sprintf("StepIf-" + s.cond.Name() + "-" + s.step.Name())
}

func (s *stepIf) Execute(rc ReconcileContext, flow Flow) (reconcile.Result, error) {
	return s.executeIf(rc, flow, s.cond)
}

func (s *stepIf) executeIf(rc ReconcileContext, flow Flow, cond Condition) (reconcile.Result, error) {
	condVal, err := cond.Evaluate(rc, flow.Logger())
	if err != nil {
		return flow.Error(err, "Evaluate condition failed.")
	}

	if condVal {
		flow.Logger().Info("Condition matches.", "step-if", s.step.Name())
		return s.step.Execute(rc, flow.WithLoggerValues("step-if", s.step.Name()))
	} else {
		return flow.Pass()
	}
}

func NewStepIf(cond Condition, step Step) Step {
	return &stepIf{
		cond: cond,
		step: step,
	}
}

func NewStepIfBinder(cond Condition, step Step) BindFunc {
	return NewStepBinder(NewStepIf(cond, step))
}

type cond struct {
	name string
	f    ConditionFunc
}

func (c *cond) Name() string {
	return c.name
}

func (c *cond) Evaluate(rc ReconcileContext, log logr.Logger) (bool, error) {
	return c.f(rc, log)
}

func NewCondition(name string, f ConditionFunc) Condition {
	return &cond{name: name, f: f}
}

type cachedCond struct {
	cond   Condition
	result *bool
}

func (c *cachedCond) Name() string {
	return c.cond.Name()
}

func (c *cachedCond) Evaluate(rc ReconcileContext, log logr.Logger) (bool, error) {
	if c.result != nil {
		return *c.result, nil
	}
	result, err := c.cond.Evaluate(rc, log)
	c.result = &result
	return result, err
}

// NewCachedCondition returns a condition which evaluates only once and
// caches the result. Note the error will be returned only when the first time
// the Evaluate method is invoked. Any sequential calls return the result
// with a nil error.
func NewCachedCondition(cond Condition) Condition {
	return &cachedCond{
		cond:   cond,
		result: nil,
	}
}
