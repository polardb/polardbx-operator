package common

import (
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type ConditionFunc func(rc *Context, log logr.Logger) (bool, error)
type StepFunc func(rc *Context, flow control.Flow) (reconcile.Result, error)

func NewStepBinder(name string, f StepFunc) control.BindFunc {
	return control.NewStepBinder(
		control.NewStep(
			name, func(rc control.ReconcileContext, flow control.Flow) (reconcile.Result, error) {
				return f(rc.(*Context), flow)
			},
		),
	)
}

func NewStepIfBinder(conditionName string, condFunc ConditionFunc, binders ...control.BindFunc) control.BindFunc {
	condition := control.NewCachedCondition(
		control.NewCondition(conditionName, func(rc control.ReconcileContext, log logr.Logger) (bool, error) {
			return condFunc(rc.(*Context), log)
		}),
	)

	ifBinders := make([]control.BindFunc, len(binders))
	for i := range binders {
		ifBinders[i] = control.NewStepIfBinder(condition, control.ExtractStepsFromBindFunc(binders[i])[0])
	}

	return control.CombineBinders(ifBinders...)
}
