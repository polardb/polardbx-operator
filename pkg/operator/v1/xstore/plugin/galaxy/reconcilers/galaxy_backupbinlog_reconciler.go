package reconcilers

import (
	xstorev1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	backupsteps "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/steps/backupbinlog"
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type GalaxyBackupBinlogReconciler struct {
}

func (r *GalaxyBackupBinlogReconciler) Reconcile(rc *xstorev1reconcile.BackupBinlogContext, log logr.Logger, request reconcile.Request) (reconcile.Result, error) {
	backupbinlog := rc.MustGetXStoreBackupBinlog()
	log = log.WithValues("phase", backupbinlog.Status.Phase)

	task, err := r.newReconcileTask(rc, backupbinlog, log)
	if err != nil {
		log.Error(err, "Failed to build reconcile task.")
		return reconcile.Result{}, err
	}
	return control.NewExecutor(log).Execute(rc, task)
}

func (r *GalaxyBackupBinlogReconciler) newReconcileTask(rc *xstorev1reconcile.BackupBinlogContext, xstoreBackupBinlog *xstorev1.XStoreBackupBinlog, log logr.Logger) (*control.Task, error) {
	log = log.WithValues("phase", xstoreBackupBinlog.Status.Phase)
	task := control.NewTask()

	defer backupsteps.PersistentBackupBinlog(task, true)

	backupsteps.WhenDeleting(
		backupsteps.UpdatePhaseTemplate(xstorev1.XStoreBackupBinlogPhaseDeleting),
	)(task)
	switch xstoreBackupBinlog.Status.Phase {
	case xstorev1.XStoreBackupBinlogPhaseNew:
		backupsteps.InitFromXStore(task)
		backupsteps.AddFinalizer(task)
		backupsteps.UpdatePhaseTemplate(xstorev1.XStoreBackupBinlogPhaseRunning)(task)
	case xstorev1.XStoreBackupBinlogPhaseRunning:
		backupsteps.WhenXStoreExist(
			backupsteps.SyncInfo,
			backupsteps.RunningRoute,
		)(task)
	case xstorev1.XStoreBackupBinlogPhaseCheckExpiredFile:
		backupsteps.TryDeleteExpiredFiles(task)
		backupsteps.UpdatePhaseTemplate(xstorev1.XStoreBackupBinlogPhaseRunning)(task)
	case xstorev1.XStoreBackupBinlogPhaseDeleting:
		backupsteps.WhenXStoreExist(
			backupsteps.CleanFromXStore,
			backupsteps.CloseBackupBinlog,
		)(task)
		backupsteps.TryDeleteExpiredFiles(task)
		backupsteps.ConfirmRemoteEmptyFiles(task)
		backupsteps.RemoveFinalizer(task)
	}
	return task, nil
}
