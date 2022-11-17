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
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/hint"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/config"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	followersteps "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/steps/follower"
	polarxJson "github.com/alibaba/polardbx-operator/pkg/util/json"
	"github.com/go-logr/logr"
	"golang.org/x/time/rate"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"time"
)

type XStoreFollowerReconciler struct {
	BaseRc *control.BaseReconcileContext
	Logger logr.Logger
	config.LoaderFactory
	MaxConcurrency int
}

func (r *XStoreFollowerReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log := r.Logger.WithValues("namespace", request.Namespace, "xstore_follower", request.Name)
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

	rc := xstorev1reconcile.NewFollowerContext(
		control.NewBaseReconcileContextFrom(r.BaseRc, ctx, request),
		r.LoaderFactory(),
	)
	rc.SetFollowerKey(request.NamespacedName)

	xstoreRequest := request
	xstoreRequest.Name = rc.MustGetXStoreFollower().Spec.XStoreName
	xstoreRc := xstorev1reconcile.NewContext(
		control.NewBaseReconcileContextFrom(r.BaseRc, ctx, xstoreRequest),
		r.LoaderFactory(),
	)
	xstoreRc.SetXStoreKey(xstoreRequest.NamespacedName)
	rc.SetXStoreContext(xstoreRc)

	defer rc.Close()
	defer xstoreRc.Close()
	task, err := r.newReconcileTask(rc, log)
	if err != nil {
		log.Error(err, "Failed to build reconcile task.")
		return reconcile.Result{}, err
	}
	return control.NewExecutor(log).Execute(rc, task)
}

func (r *XStoreFollowerReconciler) newReconcileTask(rc *xstorev1reconcile.FollowerContext, log logr.Logger) (*control.Task, error) {
	task := control.NewTask()
	xstoreFollower := rc.MustGetXStoreFollower()
	defer followersteps.PersistentXStoreFollower(task, true)
	followersteps.CheckDeletionStatusAndRedirect(task)
	switch xstoreFollower.Status.Phase {
	case xstore.FollowerPhaseNew:
		followersteps.AddFinalizer(task)
		followersteps.CheckXStore(task)
		followersteps.CheckNode(task)
		followersteps.TryLoadTargetPod(task)
		followersteps.TryCreateTmpPodIfRemote(task)
		followersteps.TryWaitForTmpPodScheduledIfRemote(task)
		followersteps.CheckIfTargetPodNotLeader(task)
		followersteps.ClearAndMarkElectionWeight(task)
		followersteps.UpdatePhaseTemplate(xstore.FollowerPhaseCheck)(task)
	case xstore.FollowerPhaseCheck:
		control.When(!followersteps.IsNotLogger(xstoreFollower), followersteps.UpdatePhaseTemplate(xstore.FollowerPhaseBeforeRestore))(task)
		followersteps.TryChooseFromPod(task)
		followersteps.TryLoadFromPod(task)
		followersteps.UpdatePhaseTemplate(xstore.FollowerPhaseBackupPrepare)(task)
	case xstore.FollowerPhaseBackupPrepare:
		followersteps.DisableFromPodPurgeLog(task)
		followersteps.CleanBackupJob(task)
		followersteps.UpdatePhaseTemplate(xstore.FollowerPhaseBackupStart)(task)
	case xstore.FollowerPhaseBackupStart:
		followersteps.StartBackupJob(task)
		followersteps.UpdatePhaseTemplate(xstore.FollowerPhaseMonitorBackup)(task)
	case xstore.FollowerPhaseMonitorBackup:
		followersteps.MonitorBackupJob(task)
		followersteps.UpdatePhaseTemplate(xstore.FollowerPhaseBeforeRestore)(task)
	case xstore.FollowerPhaseBeforeRestore:
		followersteps.BeforeRestoreJob(task)
		followersteps.MonitorCurrentJob(task)
		followersteps.KillAllProcessOnRebuildPod(task)
		followersteps.CleanDataDirRestoreJob(task)
		followersteps.MonitorCurrentJob(task)
		control.When(followersteps.IsNotLogger(xstoreFollower), followersteps.UpdatePhaseTemplate(xstore.FollowerPhaseRestore))(task)
		control.When(!followersteps.IsNotLogger(xstoreFollower), followersteps.UpdatePhaseTemplate(xstore.FollowerPhaseLoggerRebuild))(task)
	case xstore.FollowerPhaseRestore:
		followersteps.PrepareRestoreJob(task)
		followersteps.MonitorCurrentJob(task)
		followersteps.MoveRestoreJob(task)
		followersteps.MonitorCurrentJob(task)
		followersteps.FlushConsensusMeta(task)
		followersteps.MonitorCurrentJob(task)
		followersteps.AfterRestoreJob(task)
		followersteps.MonitorCurrentJob(task)
		followersteps.CleanBackupJob(task)
		followersteps.SetKillAllOnce(task)
		followersteps.KillAllProcessOnRebuildPod(task)
		followersteps.TryExchangeTargetPod(task)
		followersteps.TryDeleteTmpPodIfRemote(task)
		followersteps.WaitForTargetPodReady(task)
		followersteps.EnableFromPodPurgeLog(task)
		followersteps.RecoverElectionWeight(task)
		followersteps.TryCleanHostPathVolumeIfRemote(task)
		followersteps.UpdatePhaseTemplate(xstore.FollowerPhaseSuccess)(task)
	case xstore.FollowerPhaseLoggerRebuild:
		followersteps.LoadLeaderLogPosition(task)
		followersteps.InitializeLogger(task)
		followersteps.MonitorCurrentJob(task)
		followersteps.AfterRestoreJob(task)
		followersteps.MonitorCurrentJob(task)
		followersteps.SetKillAllOnce(task)
		followersteps.KillAllProcessOnRebuildPod(task)
		followersteps.TryExchangeTargetPod(task)
		followersteps.TryDeleteTmpPodIfRemote(task)
		followersteps.WaitForTargetPodReady(task)
		followersteps.RecoverElectionWeight(task)
		followersteps.TryCleanHostPathVolumeIfRemote(task)
		followersteps.UpdatePhaseTemplate(xstore.FollowerPhaseSuccess)(task)
	case xstore.FollowerPhaseDeleting:
		followersteps.RecoverElectionWeight(task)
		followersteps.TryCleanHostPathVolumeIfRemote(task)
		followersteps.RemoveFinalizer(task)
	case xstore.FollowerPhaseSuccess:
		followersteps.TryCleanHostPathVolumeIfRemote(task)
	case xstore.FollowerPhaseFailed:

	}

	return task, nil
}

func (r *XStoreFollowerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: r.MaxConcurrency,
			RateLimiter: workqueue.NewMaxOfRateLimiter(
				workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 30*time.Second),
				// 10 qps, 100 bucket size.  This is only for retry speed. It's only the overall factor (not per item).
				&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(60), 100)},
			),
		}).
		For(&polardbxv1.XStoreFollower{}).
		Owns(&corev1.Pod{}).  // Watches owned pods.
		Owns(&batchv1.Job{}). // Watches owned jobs.
		Complete(r)
}
