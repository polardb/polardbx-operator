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
	"strings"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/time/rate"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
)

type PolarDBXClusterKnobsReconciler struct {
	Client         client.Client
	Logger         logr.Logger
	MaxConcurrency int
}

func (r *PolarDBXClusterKnobsReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logger := r.Logger.WithValues("namespace", request.Namespace, "polardbxclusterknobs", request.Name)

	var knobs polardbxv1.PolarDBXClusterKnobs
	if err := r.Client.Get(ctx, request.NamespacedName, &knobs); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Object not found, might be deleted.")
			return reconcile.Result{}, nil
		} else {
			return reconcile.Result{}, err
		}
	}

	if knobs.Annotations["skip"] != "" {
		logger.Info("Skip reconcile...")
		return reconcile.Result{}, nil
	}

	// Trigger the update.
	knobs.Status.LastUpdated = metav1.Now()
	if err := r.Client.Update(ctx, &knobs); client.IgnoreNotFound(err) != nil {
		if apierrors.IsConflict(err) {
			logger.Info("Update conflict, just retry.")
		} else if apierrors.IsInternalError(err) && strings.Contains(err.Error(), "webhook") {
			logger.Info("Update internal error and is webhook error, retry after 2 seconds.")
			return reconcile.Result{RequeueAfter: 2 * time.Second}, nil
		} else {
			logger.Error(err, "Error while updating knobs...")
			return reconcile.Result{}, err
		}
	}

	// Schedule after 2 seconds.
	return reconcile.Result{RequeueAfter: 2 * time.Second}, nil
}

func (r *PolarDBXClusterKnobsReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: r.MaxConcurrency,
			RateLimiter: workqueue.NewMaxOfRateLimiter(
				workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 300*time.Second),
				// 10 qps, 100 bucket size.  This is only for retry speed. It's only the overall factor (not per item).
				&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
			),
		}).
		For(&polardbxv1.PolarDBXClusterKnobs{}).
		Complete(r)
}
