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

package controllers

import (
	"context"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/hint"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/config"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/helper"
	polardbxreconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	"k8s.io/apimachinery/pkg/types"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/time/rate"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	monitorsteps "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/steps/monitor"
)

type PolarDBXMonitorReconciler struct {
	BaseRc *control.BaseReconcileContext

	Client client.Client
	Logger logr.Logger

	config.LoaderFactory
	MaxConcurrency int
}

func (r *PolarDBXMonitorReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logger := r.Logger.WithValues("namespace", request.Namespace, "polardbxmonitor", request.Name)

	if hint.IsNamespacePaused(request.Namespace) {
		logger.Info("Monitor reconciling is paused, skip")
		return reconcile.Result{}, nil
	}

	rc := polardbxreconcile.NewContext(
		control.NewBaseReconcileContextFrom(r.BaseRc, ctx, request),
		r.LoaderFactory(),
	)

	rc.SetPolardbxMonitorKey(request.NamespacedName)

	monitor, err := rc.GetPolarDBXMonitor()

	if err != nil {
		logger.Error(err, "Unable get polardbxmonitor: "+request.NamespacedName.String())
		return reconcile.Result{}, err
	}

	// monitor := rc.MustGetPolarDBXMonitor()

	rc.SetPolarDBXKey(types.NamespacedName{
		Namespace: monitor.Namespace,
		Name:      monitor.Spec.ClusterName,
	})

	defer rc.Close()

	return r.reconcile(rc, monitor, logger)
}

func (r *PolarDBXMonitorReconciler) newReconcileTask(rc *polardbxreconcile.Context, monitor *polardbxv1.PolarDBXMonitor, log logr.Logger) *control.Task {
	task := control.NewTask()

	defer monitorsteps.PersistPolarDBXMonitor(task, true)

	switch monitor.Status.MonitorStatus {
	case polardbxv1polardbx.MonitorStatusPending:
		monitorsteps.TransferMonitorStatusTo(polardbxv1polardbx.MonitorStatusCreating, true)(task)
	case polardbxv1polardbx.MonitorStatusCreating:
		monitorsteps.CheckServiceMonitorExists(task)
		monitorsteps.CreateServiceMonitorIfNeeded(task)
		monitorsteps.SyncPolarDBXMonitorSpecToStatus(task)
		monitorsteps.TransferMonitorStatusTo(polardbxv1polardbx.MonitorStatusMonitoring, false)(task)
	case polardbxv1polardbx.MonitorStatusMonitoring:
		control.When(helper.IsMonitorConfigChanged(monitor),
			monitorsteps.TransferMonitorStatusTo(polardbxv1polardbx.MonitorStatusUpdating, true))(task)
	case polardbxv1polardbx.MonitorStatusUpdating:
		monitorsteps.UpdateServiceMonitorIfNeeded(task)
		monitorsteps.SyncPolarDBXMonitorSpecToStatus(task)
		monitorsteps.TransferMonitorStatusTo(polardbxv1polardbx.MonitorStatusMonitoring, false)(task)
	}

	return task
}

func (r *PolarDBXMonitorReconciler) reconcile(rc *polardbxreconcile.Context, polardbxmonitor *polardbxv1.PolarDBXMonitor, log logr.Logger) (reconcile.Result, error) {
	log = log.WithValues("status", polardbxmonitor.Status.MonitorStatus)

	task := r.newReconcileTask(rc, polardbxmonitor, log)
	return control.NewExecutor(log).Execute(rc, task)
}

func (r *PolarDBXMonitorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: r.MaxConcurrency,
			RateLimiter: workqueue.NewMaxOfRateLimiter(
				workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 300*time.Second),
				// 60 qps, 10 bucket size.  This is only for retry speed. It's only the overall factor (not per item).
				&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(60), 10)},
			),
		}).
		For(&polardbxv1.PolarDBXMonitor{}).
		Complete(r)
}
