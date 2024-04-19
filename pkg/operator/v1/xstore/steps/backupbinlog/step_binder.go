package backupbinlog

import (
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type ConditionFunc func(rc *xstorev1reconcile.BackupBinlogContext, log logr.Logger) (bool, error)
type StepFunc func(rc *xstorev1reconcile.BackupBinlogContext, flow control.Flow) (reconcile.Result, error)

func NewStepBinder(name string, f StepFunc) control.BindFunc {
	return control.NewStepBinder(
		control.NewStep(
			name, func(rc control.ReconcileContext, flow control.Flow) (reconcile.Result, error) {
				return f(rc.(*xstorev1reconcile.BackupBinlogContext), flow)
			},
		),
	)
}

func NewStepIfBinder(conditionName string, condFunc ConditionFunc, binders ...control.BindFunc) control.BindFunc {
	condition := control.NewCachedCondition(
		control.NewCondition(conditionName, func(rc control.ReconcileContext, log logr.Logger) (bool, error) {
			return condFunc(rc.(*xstorev1reconcile.BackupBinlogContext), log)
		}),
	)

	ifBinders := make([]control.BindFunc, len(binders))
	for i := range binders {
		ifBinders[i] = control.NewStepIfBinder(condition, control.ExtractStepsFromBindFunc(binders[i])[0])
	}

	return control.CombineBinders(ifBinders...)
}
