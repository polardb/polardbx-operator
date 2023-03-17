package controllers

import (
	"context"
	"errors"
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/hint"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/config"
	polardbxreconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	backupbinlog "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/steps/backupbinlog"
	polarxJson "github.com/alibaba/polardbx-operator/pkg/util/json"
	"github.com/go-logr/logr"
	"golang.org/x/time/rate"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"time"
)

type PolarDBXBackupBinlogReconciler struct {
	BaseRc *control.BaseReconcileContext
	Logger logr.Logger
	config.LoaderFactory

	MaxConcurrency int
}

func (r *PolarDBXBackupBinlogReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log := r.Logger.WithValues("namespace", request.Namespace, "polardbxcluster", request.Name)
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

	rc := polardbxreconcile.NewContext(
		control.NewBaseReconcileContextFrom(r.BaseRc, ctx, request),
		r.LoaderFactory(),
	)
	rc.SetBackupBinlogKey(request.NamespacedName)
	defer rc.Close()

	backupBinlog := rc.MustGetPolarDBXBackupBinlog()

	rc.SetPolarDBXKey(types.NamespacedName{
		Namespace: request.Namespace,
		Name:      backupBinlog.Spec.PxcName,
	})
	return r.reconcile(rc, backupBinlog, log)
}

func (r *PolarDBXBackupBinlogReconciler) reconcile(rc *polardbxreconcile.Context, backupBinlog *polardbxv1.PolarDBXBackupBinlog, log logr.Logger) (reconcile.Result, error) {
	log = log.WithValues("phase", backupBinlog.Status.Phase)

	task := r.newReconcileTask(rc, backupBinlog, log)
	return control.NewExecutor(log).Execute(rc, task)
}

func (r *PolarDBXBackupBinlogReconciler) newReconcileTask(rc *polardbxreconcile.Context, backup *polardbxv1.PolarDBXBackupBinlog, log logr.Logger) *control.Task {
	log = log.WithValues("phase", backup.Status.Phase)
	task := control.NewTask()
	defer backupbinlog.PersistentBackupBinlog(task, true)

	backupbinlog.WhenDeleting(
		backupbinlog.TransferPhaseTo(polardbxv1.BackupBinlogPhaseDeleting, true),
	)(task)

	switch backup.Status.Phase {
	case polardbxv1.BackupBinlogPhaseNew:
		backupbinlog.InitFromPxc(task)
		backupbinlog.AddFinalizer(task)
		backupbinlog.TransferPhaseTo(polardbxv1.BackupBinlogPhaseRunning)(task)
	case polardbxv1.BackupBinlogPhaseRunning:
		backupbinlog.WhenPxcExist(
			backupbinlog.SyncInfo,
			backupbinlog.ReconcileHeartbeatJob,
			backupbinlog.UpdateObservedGeneration,
			backupbinlog.RunningRoute,
		)(task)
	case polardbxv1.BackupBinlogPhaseCheckExpiredFile:
		backupbinlog.TryDeleteExpiredFiles(task)
		backupbinlog.TransferPhaseTo(polardbxv1.BackupBinlogPhaseRunning)(task)
	case polardbxv1.BackupBinlogPhaseDeleting:
		backupbinlog.WhenPxcExist(
			backupbinlog.CleanFromPxc,
			backupbinlog.TryDeleteHeartbeatJob,
			backupbinlog.CloseBackupBinlog,
		)(task)
		backupbinlog.TryDeleteExpiredFiles(task)
		backupbinlog.ConfirmRemoteEmptyFiles(task)
		backupbinlog.RemoveFinalizer(task)
	}

	return task
}

func (r *PolarDBXBackupBinlogReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: r.MaxConcurrency,
			RateLimiter: workqueue.NewMaxOfRateLimiter(
				workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 300*time.Second),
				// 60 qps, 10 bucket size.  This is only for retry speed. It's only the overall factor (not per item).
				&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(60), 10)},
			),
		}).
		For(&polardbxv1.PolarDBXBackupBinlog{}).
		Complete(r)
}
