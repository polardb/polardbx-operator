package controllers

import (
	"context"
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/api/v1/systemtask"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/hint"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/config"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/systemtask/common"
	resource_balance "github.com/alibaba/polardbx-operator/pkg/operator/v1/systemtask/reconcile"
	"github.com/go-logr/logr"
	"golang.org/x/time/rate"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"time"
)

func init() {
	common.Register(systemtask.BalanceResource, &resource_balance.ResourceBalanceReconciler{})
}

type SystemTaskReconciler struct {
	BaseRc *control.BaseReconcileContext
	Logger logr.Logger
	config.LoaderFactory

	MaxConcurrency int
}

func (r *SystemTaskReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log := r.Logger.WithValues("namespace", request.Namespace, "systemtask", request.Name)

	if hint.IsNamespacePaused(request.Namespace) {
		log.Info("Reconciling is paused, skip")
		return reconcile.Result{}, nil
	}
	rc := common.NewContext(
		control.NewBaseReconcileContextFrom(r.BaseRc, ctx, request),
		r.LoaderFactory(),
	)
	rc.SetKey(request.NamespacedName)
	defer rc.Close()

	systemTask := rc.MustGetSystemTask()
	reconciler := common.MusterFindReconciler(systemTask.Spec.TaskType)
	return reconciler.Reconcile(rc, log.WithValues("SystemTaskType", systemTask.Spec.TaskType), request)
}

func (r *SystemTaskReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: r.MaxConcurrency,
			RateLimiter: workqueue.NewMaxOfRateLimiter(
				workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 10*time.Second),
				// 60 qps, 10 bucket size.  This is only for retry speed. It's only the overall factor (not per item).
				&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(60), 10)},
			),
		}).
		For(&polardbxv1.SystemTask{}).
		Complete(r)
}
