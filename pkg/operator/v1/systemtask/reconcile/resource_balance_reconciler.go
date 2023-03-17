package resource_balance

import (
	v1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/api/v1/systemtask"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/systemtask/common"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/systemtask/steps"
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type ResourceBalanceReconciler struct {
}

func (r *ResourceBalanceReconciler) Reconcile(rc *common.Context, log logr.Logger, request reconcile.Request) (reconcile.Result, error) {
	systemTask := rc.MustGetSystemTask()
	log = log.WithValues("phase", systemTask.Status.Phase)
	task := r.newReconcileTask(rc, systemTask, log)
	return control.NewExecutor(log).Execute(rc, task)
}

func (r *ResourceBalanceReconciler) newReconcileTask(rc *common.Context, systemTask *v1.SystemTask, log logr.Logger) *control.Task {
	task := control.NewTask()
	// Deferred steps, will always be executed in the deferred sequence.
	defer steps.PersistentSystemTask(task, true)
	switch systemTask.Status.Phase {
	case systemtask.InitPhase:
		steps.CheckAllXStoreHealth(task)
		steps.TransferPhaseTo(systemtask.RebuildTaskPhase, true)(task)
	case systemtask.RebuildTaskPhase:
		steps.CreateBalanceTaskIfNeed(task)
		control.When(steps.IsRebuildFinish(rc), steps.TransferPhaseTo(systemtask.BalanceRolePhase, true))(task)
	case systemtask.BalanceRolePhase:
		steps.BalanceRole(task)
	case systemtask.SuccessPhase:

	}
	return task
}
