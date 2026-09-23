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
