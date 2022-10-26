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
	xstorev1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/hint"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/config"
	backupxstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/backupreconciler"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin"
	"github.com/go-logr/logr"
	"golang.org/x/time/rate"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"time"
)

type XStoreBackupReconciler struct {
	BaseRc *control.BaseReconcileContext
	Logger logr.Logger
	config.LoaderFactory

	MaxConcurrency int
}

func (r *XStoreBackupReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log := r.Logger.WithValues("namespace", request.Namespace, "xstore", request.Name)

	if hint.IsNamespacePaused(request.Namespace) {
		log.Info("Reconciling is paused, skip")
		return reconcile.Result{}, nil
	}
	rc := backupxstorev1reconcile.NewContext(
		control.NewBaseReconcileContextFrom(r.BaseRc, ctx, request),
	)

	defer rc.Close()

	// Verify the existence of the xstore.
	xstoreBackup, err := rc.GetXStoreBackup()
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Object for XStoreBackup isn't found, might be deleted!")
			return reconcile.Result{}, nil
		} else {
			log.Error(err, "Unable to get object for XStoreBackup")
			return reconcile.Result{}, err
		}
	}

	engine := xstoreBackup.Spec.Engine
	reconciler := plugin.GetXStoreBackupReconciler(engine)

	if reconciler == nil {
		log.Info("No reconciler found, abort!", "engine", engine)
		return reconcile.Result{}, nil
	}

	return reconciler.Reconcile(rc, log.WithValues("engine", engine), request)
}

func (r *XStoreBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: r.MaxConcurrency,
			RateLimiter: workqueue.NewMaxOfRateLimiter(
				workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 300*time.Second),
				// 10 qps, 100 bucket size.  This is only for retry speed. It's only the overall factor (not per item).
				&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
			),
		}).
		For(&xstorev1.XStoreBackup{}).
		Watches(&source.Kind{Type: &xstorev1.PolarDBXBackup{}}, &handler.EnqueueRequestForObject{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}
