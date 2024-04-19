package controllers

import (
	"context"
	xstorev1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/hint"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/config"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	"github.com/go-logr/logr"
	"golang.org/x/time/rate"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"time"
)

type XStoreBackupBinlogReconciler struct {
	BaseRc *control.BaseReconcileContext
	Logger logr.Logger
	config.LoaderFactory
	MaxConcurrency int
}

func (r *XStoreBackupBinlogReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log := r.Logger.WithValues("namespace", request.Namespace, "xstore-backupbinlog", request.Name)
	if hint.IsNamespacePaused(request.Namespace) {
		log.Info("Reconciling is paused, skip")
		return reconcile.Result{}, nil
	}

	rc := xstorev1reconcile.NewBackupBinlogContext(
		control.NewBaseReconcileContextFrom(r.BaseRc, ctx, request),
	)
	defer rc.Close()

	// Verify the existence of the xstore.
	xstoreBackupBinlog, err := rc.GetXStoreBackupBinlog()
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Object for XStoreBackupBinlog isn't found, might be deleted!")
			return reconcile.Result{}, nil
		} else {
			log.Error(err, "Unable to get object for XStoreBackupBinlog")
			return reconcile.Result{}, err
		}
	}

	// Record the context of the corresponding xstore
	xstore, err := rc.GetXStore()
	if err != nil {
		log.Error(err, "Unable to get corresponding xstore")
		return reconcile.Result{}, err
	}
	xstoreRequest := request
	xstoreRequest.Name = xstore.Name
	xstoreRc := xstorev1reconcile.NewContext(
		control.NewBaseReconcileContextFrom(r.BaseRc, ctx, xstoreRequest),
		r.LoaderFactory(),
	)
	xstoreRc.SetXStoreKey(xstoreRequest.NamespacedName)
	rc.SetXStoreContext(xstoreRc)

	engine := xstoreBackupBinlog.Spec.Engine
	reconciler := plugin.GetXStoreBackupBinlogReconciler(engine)

	if reconciler == nil {
		log.Info("No reconciler found, abort!", "engine", engine)
		return reconcile.Result{}, nil
	}

	return reconciler.Reconcile(rc, log.WithValues("engine", engine), request)
}

func (r *XStoreBackupBinlogReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: r.MaxConcurrency,
			RateLimiter: workqueue.NewMaxOfRateLimiter(
				workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 300*time.Second),
				// 60 qps, 10 bucket size.  This is only for retry speed. It's only the overall factor (not per item).
				&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(60), 10)},
			),
		}).
		For(&xstorev1.XStoreBackupBinlog{}).
		Complete(r)
}
