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

package reconcilers

import (
	xstorev1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	backupsteps "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/steps/backup"
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"time"
)

type GalaxyBackupReconciler struct {
}

func (r *GalaxyBackupReconciler) Reconcile(rc *xstorev1reconcile.BackupContext, log logr.Logger, request reconcile.Request) (reconcile.Result, error) {
	backup := rc.MustGetXStoreBackup()
	log = log.WithValues("phase", backup.Status.Phase)

	isStandard := true
	var err error
	if backup.GetDeletionTimestamp().IsZero() {
		isStandard, err = rc.GetXStoreIsStandard()
		if err != nil {
			log.Error(err, "Unable to get corresponding xstore")
			return reconcile.Result{}, err
		}
	}
	task, err := r.newReconcileTask(rc, backup, log, isStandard)
	if err != nil {
		log.Error(err, "Failed to build reconcile task.")
		return reconcile.Result{}, err
	}
	return control.NewExecutor(log).Execute(rc, task)
}

func (r *GalaxyBackupReconciler) newReconcileTask(rc *xstorev1reconcile.BackupContext, xstoreBackup *xstorev1.XStoreBackup, log logr.Logger, isStandard bool) (*control.Task, error) {

	task := control.NewTask()

	defer backupsteps.PersistentStatusChanges(task, true)
	defer backupsteps.PersistentXstoreBackup(task, true)

	backupsteps.WhenDeletedAndNotDeleting(
		backupsteps.UpdatePhaseTemplate(xstorev1.XStoreBackupDeleting, true),
	)(task)

	switch xstoreBackup.Status.Phase {
	case xstorev1.XStoreBackupNew:
		backupsteps.AddFinalizer(task)
		backupsteps.UpdateBackupStartInfo(task)
		backupsteps.CreateBackupConfigMap(task)
		backupsteps.StartXStoreFullBackupJob(task)
		backupsteps.UpdatePhaseTemplate(xstorev1.XStoreFullBackuping)(task)
	case xstorev1.XStoreFullBackuping:
		backupsteps.WaitFullBackupJobFinished(task)
		control.Branch(isStandard,
			backupsteps.UpdatePhaseTemplate(xstorev1.XStoreBinlogWaiting),
			backupsteps.UpdatePhaseTemplate(xstorev1.XStoreBackupCollecting))(task)
	case xstorev1.XStoreBackupCollecting:
		backupsteps.WaitBinlogOffsetCollected(task)
		backupsteps.StartCollectBinlogJob(task)
		backupsteps.WaitCollectBinlogJobFinished(task)
		backupsteps.UpdatePhaseTemplate(xstorev1.XStoreBinlogBackuping)(task)
	case xstorev1.XStoreBinlogBackuping:
		backupsteps.WaitPXCSeekCpJobFinished(task)
		backupsteps.StartBinlogBackupJob(task)
		backupsteps.WaitBinlogBackupJobFinished(task)
		backupsteps.UpdateBackupStatus(task)
		backupsteps.ExtractLastEventTimestamp(task)
		backupsteps.UpdatePhaseTemplate(xstorev1.XStoreBinlogWaiting)(task)
	case xstorev1.XStoreBinlogWaiting:
		control.When(!isStandard, backupsteps.WaitPXCBinlogBackupFinished)(task)
		backupsteps.SaveXStoreSecrets(task)
		control.Branch(isStandard,
			backupsteps.UpdatePhaseTemplate(xstorev1.XStoreMetadataBackuping),
			backupsteps.UpdatePhaseTemplate(xstorev1.XStoreBackupFinished),
		)(task)
	case xstorev1.XStoreMetadataBackuping:
		defer control.ScheduleAfter(10*time.Second)(task, true)
		backupsteps.UploadXStoreMetadata(task)
		backupsteps.UpdateBackupStatus(task)
		backupsteps.UpdatePhaseTemplate(xstorev1.XStoreBackupFinished)(task)
	case xstorev1.XStoreBackupFinished:
		backupsteps.RemoveFullBackupJob(task)
		backupsteps.RemoveCollectBinlogJob(task)
		backupsteps.RemoveBinlogBackupJob(task)
		backupsteps.RemoveXSBackupOverRetention(task)
		log.Info("Finished phase.")
	case xstorev1.XStoreBackupDeleting:
		control.When(isStandard, backupsteps.CleanRemoteBackupFiles)(task)
		backupsteps.RemoveFinalizer(task)
	default:
		log.Info("Unrecognized phase.")
	}

	return task, nil
}
