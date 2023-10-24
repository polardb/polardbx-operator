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
	"errors"
	"github.com/alibaba/polardbx-operator/pkg/operator/hint"
	polarxJson "github.com/alibaba/polardbx-operator/pkg/util/json"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/time/rate"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/config"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"

	_ "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin/engines"
)

type XStoreReconciler struct {
	BaseRc *control.BaseReconcileContext
	Logger logr.Logger
	config.LoaderFactory

	MaxConcurrency int
}

func (r *XStoreReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log := r.Logger.WithValues("namespace", request.Namespace, "xstore", request.Name)
	defer func() {
		err := recover()
		if err != nil {
			log.Error(errors.New(polarxJson.Convert2JsonString(err)), "")
		}
	}()
	if hint.IsNamespacePaused(request.Namespace) {
		log.Info("Reconciling is paused, skip")
		return reconcile.Result{}, nil
	}

	rc := xstorev1reconcile.NewContext(
		control.NewBaseReconcileContextFrom(r.BaseRc, ctx, request),
		r.LoaderFactory(),
	)
	rc.SetXStoreKey(request.NamespacedName)
	defer rc.Close()

	xstore, err := rc.GetXStore()
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Object not found, might be deleted. Just ignore.")
			return reconcile.Result{}, nil
		}
		log.Error(err, "Unable to get xstore object.")
		return reconcile.Result{}, err
	}

	engine := xstore.Spec.Engine
	reconciler := plugin.GetXStoreReconciler(engine)

	if reconciler == nil {
		log.Info("No reconciler found, abort!", "engine", engine)
		return reconcile.Result{}, nil
	}

	return reconciler.Reconcile(rc, log.WithValues("engine", engine), request)
}

func (r *XStoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: r.MaxConcurrency,
			RateLimiter: workqueue.NewMaxOfRateLimiter(
				workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 60*time.Second),
				// 60 qps, 10 bucket size.  This is only for retry speed. It's only the overall factor (not per item).
				&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(60), 10)},
			),
		}).
		For(&polardbxv1.XStore{}).
		Owns(&corev1.Pod{}).  // Watches owned pods.
		Owns(&batchv1.Job{}). // Watches owned jobs.
		Complete(r)
}
