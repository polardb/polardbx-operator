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
	"time"

	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/helper"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"k8s.io/apimachinery/pkg/types"

	"github.com/alibaba/polardbx-operator/pkg/operator/v1/config"

	"github.com/alibaba/polardbx-operator/pkg/operator/hint"

	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	polardbxreconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	parametersteps "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/steps/parameter"

	"github.com/go-logr/logr"
	"golang.org/x/time/rate"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
)

type PolarDBXParameterReconciler struct {
	Client         client.Client
	Logger         logr.Logger
	MaxConcurrency int
	BaseRc         *control.BaseReconcileContext
	config.LoaderFactory
}

func (r *PolarDBXParameterReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logger := r.Logger.WithValues("namespace", request.Namespace, "polardbxparameter", request.Name)

	if hint.IsNamespacePaused(request.Namespace) {
		r.Logger.Info("Reconciling is paused, skip")
		return reconcile.Result{}, nil
	}

	rc := polardbxreconcile.NewContext(
		control.NewBaseReconcileContextFrom(r.BaseRc, ctx, request),
		r.LoaderFactory(),
	)
	defer rc.Close()

	rc.SetPolardbxParameterKey(request.NamespacedName)

	parameter, err := rc.GetPolarDBXParameter()
	if err != nil {
		logger.Error(err, "Unable get polardbxparameter: "+request.NamespacedName.String())
		return reconcile.Result{}, err
	}

	rc.SetPolarDBXKey(types.NamespacedName{
		Namespace: parameter.Namespace,
		Name:      parameter.Spec.ClusterName,
	})

	polardbx, err := rc.GetPolarDBX()
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Object not found, might be deleted. Just ignore.")
			return reconcile.Result{}, nil
		}
		logger.Error(err, "Unable to get polardbx object.")
		return reconcile.Result{}, err
	}

	// Schedule after 2 seconds.
	return r.reconcile(rc, parameter, polardbx, logger)
}

func (r *PolarDBXParameterReconciler) reconcile(rc *polardbxreconcile.Context, polardbxparameter *polardbxv1.PolarDBXParameter, polardbx *polardbxv1.PolarDBXCluster, log logr.Logger) (reconcile.Result, error) {
	task := r.newReconcileTask(rc, polardbxparameter, polardbx, log)
	return control.NewExecutor(log).Execute(rc, task)
}

func (r *PolarDBXParameterReconciler) newReconcileTask(rc *polardbxreconcile.Context, parameter *polardbxv1.PolarDBXParameter, polardbx *polardbxv1.PolarDBXCluster, log logr.Logger) *control.Task {
	task := control.NewTask()

	defer parametersteps.PersistPolarDBXParameterStatus(task, true)
	defer parametersteps.PersistPolarDBXParameter(task, true)

	if polardbx.Status.Phase == polardbxv1polardbx.PhaseRunning {
		switch parameter.Status.Phase {
		case polardbxv1polardbx.ParameterStatusNew:
			parametersteps.SyncPolarDBXParameterStatus(task)
			parametersteps.TransferParameterPhaseTo(polardbxv1polardbx.ParameterStatusModifying, true)(task)
		case polardbxv1polardbx.ParameterStatusRunning:
			// Schedule after 10 seconds.
			defer control.ScheduleAfter(10*time.Second)(task, true)

			parametersteps.SyncPolarDBXParameterStatus(task)
			control.When(helper.IsParameterChanged(parameter),
				parametersteps.TransferParameterPhaseTo(polardbxv1polardbx.ParameterStatusModifying, true),
			)(task)

		case polardbxv1polardbx.ParameterStatusModifying:
			// perform different operations depending on the parameter type
			parametersteps.GetParametersRoleMap(task)
			parametersteps.GetRolesToRestart(task)

			parametersteps.SyncCNRestartType(task)
			parametersteps.SyncDNRestartType(task)
			parametersteps.SyncGMSRestartType(task)

			control.Block(
				parametersteps.SyncCnParameters,
				parametersteps.UpdateCnConfigMap,
			)(task)

			control.Block(
				parametersteps.SyncDnParameters,
				parametersteps.SyncGMSParameters,
			)(task)

			parametersteps.CNRestart(task)
			parametersteps.DNRestart(task)
			parametersteps.GMSRestart(task)

			parametersteps.SyncPolarDBXParameterStatus(task)
			parametersteps.TransferParameterPhaseTo(polardbxv1polardbx.ParameterStatusRunning, true)(task)

		}
	}

	return task
}

func (r *PolarDBXParameterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: r.MaxConcurrency,
			RateLimiter: workqueue.NewMaxOfRateLimiter(
				workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 300*time.Second),
				// 10 qps, 100 bucket size.  This is only for retry speed. It's only the overall factor (not per item).
				&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
			),
		}).
		For(&polardbxv1.PolarDBXParameter{}).
		Complete(r)
}
