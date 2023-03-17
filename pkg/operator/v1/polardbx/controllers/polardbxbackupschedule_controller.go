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

package controllers

import (
	"context"
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/hint"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/config"
	polardbxreconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/steps/backup/schedule"
	"github.com/go-logr/logr"
	"golang.org/x/time/rate"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"time"
)

type PolarDBXBackupScheduleReconciler struct {
	BaseRc *control.BaseReconcileContext
	Logger logr.Logger
	config.LoaderFactory
	MaxConcurrency int
}

func (r *PolarDBXBackupScheduleReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log := r.Logger.WithValues("namespace", request.Namespace, "polardbxbackupschedule", request.Name)

	if hint.IsNamespacePaused(request.Namespace) {
		log.Info("Reconcile is paused, skip")
		return reconcile.Result{}, nil
	}
	rc := polardbxreconcile.NewContext(
		control.NewBaseReconcileContextFrom(r.BaseRc, ctx, request),
		r.LoaderFactory(),
	)
	rc.SetPolarDBXBackupScheduleKey(request.NamespacedName)
	defer rc.Close()

	backupSchedule, err := rc.GetPolarDBXBackupSchedule()
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Object not found, might be deleted, just ignore.")
			return reconcile.Result{}, nil
		}
		log.Error(err, "Unable to get polardbx backup schedule object.")
		return reconcile.Result{}, err
	}

	if backupSchedule.Spec.Suspend {
		log.Info("Backup schedule suspended, just skip.")
		return reconcile.Result{}, nil
	}

	return r.reconcile(rc, backupSchedule, log)
}

func (r *PolarDBXBackupScheduleReconciler) reconcile(rc *polardbxreconcile.Context, backupSchedule *polardbxv1.PolarDBXBackupSchedule, log logr.Logger) (reconcile.Result, error) {
	task := r.newReconcileTask(rc, backupSchedule, log)
	return control.NewExecutor(log).Execute(rc, task)
}

func (r *PolarDBXBackupScheduleReconciler) newReconcileTask(rc *polardbxreconcile.Context, backupSchedule *polardbxv1.PolarDBXBackupSchedule, log logr.Logger) *control.Task {
	task := control.NewTask()

	defer schedule.PersistPolarDBXBackupScheduleStatus(task, true)

	schedule.CleanOutdatedBackupSet(task)

	schedule.CheckNextScheduleTime(task)

	schedule.CheckUnderwayBackup(task)

	schedule.DispatchBackupTask(task)

	return task
}

func (r *PolarDBXBackupScheduleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: r.MaxConcurrency,
			RateLimiter: workqueue.NewMaxOfRateLimiter(
				workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 300*time.Second),
				// 60 qps, 10 bucket size.  This is only for retry speed. It's only the overall factor (not per item).
				&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(60), 10)},
			),
		}).
		For(&polardbxv1.PolarDBXBackupSchedule{}).
		Complete(r)
}
