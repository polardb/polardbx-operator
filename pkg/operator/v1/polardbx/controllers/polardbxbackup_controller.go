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
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/hint"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/config"
	polardbxreconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	commonsteps "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/steps/backup/common"
	"github.com/go-logr/logr"
	"golang.org/x/time/rate"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"time"
)

type PolarDBXBackupReconciler struct {
	BaseRc *control.BaseReconcileContext
	Logger logr.Logger
	config.LoaderFactory

	MaxConcurrency int
}

func (r *PolarDBXBackupReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log := r.Logger.WithValues("namespace", request.Namespace, "polardbxcluster", request.Name)

	if hint.IsNamespacePaused(request.Namespace) {
		log.Info("Reconciling is paused, skip")
		return reconcile.Result{}, nil
	}

	rc := polardbxreconcile.NewContext(
		control.NewBaseReconcileContextFrom(r.BaseRc, ctx, request),
		r.LoaderFactory(),
	)
	rc.SetPolarDBXBackupKey(request.NamespacedName)
	defer rc.Close()

	polardbxBackup, err := rc.GetPolarDBXBackup()
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("The polardbx backup object not found, might be deleted. Just ignore.")
			return reconcile.Result{}, nil
		}
		log.Error(err, "Unable to get polardbx backup object.")
		return reconcile.Result{}, err
	}

	rc.SetPolarDBXKey(types.NamespacedName{
		Namespace: request.Namespace,
		Name:      polardbxBackup.Spec.Cluster.Name,
	})

	return r.reconcile(rc, polardbxBackup, log)
}

func (r *PolarDBXBackupReconciler) newReconcileTask(rc *polardbxreconcile.Context, backup *polardbxv1.PolarDBXBackup, log logr.Logger) *control.Task {

	log = log.WithValues("phase", backup.Status.Phase)

	task := control.NewTask()
	defer commonsteps.PersistentStatusChanges(task, true)

	switch backup.Status.Phase {
	case polardbxv1.BackupNew:
		commonsteps.UpdateBackupStartInfo(task)
		//locked binlog purge
		commonsteps.LockXStoreBinlogPurge(task)
		commonsteps.CreateBackupJobsForXStore(task)
		commonsteps.TransferPhaseTo(polardbxv1.FullBackuping, false)(task)
	case polardbxv1.FullBackuping:
		commonsteps.WaitAllBackupJobsFinished(task)
		if backup.Status.Phase == polardbxv1.BackupFailed {
			commonsteps.TransferPhaseTo(polardbxv1.BackupFailed, false)(task)
		} else {
			commonsteps.TransferPhaseTo(polardbxv1.BackupCollecting, false)(task)
		}
	case polardbxv1.BackupCollecting:
		commonsteps.CollectBinlogStartIndex(task)
		commonsteps.DrainCommittingTrans(task)
		commonsteps.SendHeartBeat(task)
		commonsteps.WaitHeartbeatSentToFollower(task)
		commonsteps.CollectBinlogEndIndex(task)
		commonsteps.TransferPhaseTo(polardbxv1.BackupCalculating, false)(task)
	case polardbxv1.BackupCalculating:
		commonsteps.WaitAllCollectBinlogJobFinished(task)
		commonsteps.PrepareSeekCpJobContext(task)
		commonsteps.CreateSeekCpJob(task)
		commonsteps.WaitUntilSeekCpJobFinished(task)
		commonsteps.TransferPhaseTo(polardbxv1.BinlogBackuping, false)(task)
	case polardbxv1.BinlogBackuping:
		commonsteps.WaitAllBinlogJobFinished(task)
		commonsteps.SavePXCSecrets(task)
		commonsteps.TransferPhaseTo(polardbxv1.BackupFinished, false)(task)
	case polardbxv1.BackupFinished:
		commonsteps.UnLockXStoreBinlogPurge(task)
		commonsteps.RemoveSeekCpJob(task)
		commonsteps.RemoveBackupOverRetention(task)
		log.Info("Finished phase.")
	case polardbxv1.BackupFailed:
		commonsteps.DeleteBackupJobsOnFailure(task)
		log.Info("Failed phase.")
	default:
		log.Info("Unrecognized phase for pxc backup")
	}
	return task
}

func (r *PolarDBXBackupReconciler) reconcile(rc *polardbxreconcile.Context, polardbxBackup *polardbxv1.PolarDBXBackup, log logr.Logger) (reconcile.Result, error) {
	log = log.WithValues("phase", polardbxBackup.Status.Phase)

	task := r.newReconcileTask(rc, polardbxBackup, log)
	return control.NewExecutor(log).Execute(rc, task)
}

func (r *PolarDBXBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: r.MaxConcurrency,
			RateLimiter: workqueue.NewMaxOfRateLimiter(
				workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 300*time.Second),
				// 10 qps, 100 bucket size.  This is only for retry speed. It's only the overall factor (not per item).
				&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
			),
		}).
		For(&polardbxv1.PolarDBXBackup{}).
		Owns(&polardbxv1.XStoreBackup{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}
