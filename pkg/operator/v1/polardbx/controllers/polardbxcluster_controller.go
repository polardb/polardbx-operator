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
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/steps/instance/pitr"
	"time"

	"github.com/alibaba/polardbx-operator/pkg/operator/hint"

	"github.com/go-logr/logr"
	"golang.org/x/time/rate"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/alibaba/polardbx-operator/pkg/debug"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/config"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/helper"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	polardbxreconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	instancesteps "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/steps/instance"
	checksteps "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/steps/instance/check"
	commonsteps "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/steps/instance/common"
	finalizersteps "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/steps/instance/finalizer"
	gmssteps "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/steps/instance/gms"
	guidesteps "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/steps/instance/guide"
	rebalancesteps "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/steps/instance/rebalance"
	restartsteps "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/steps/instance/restart"
)

type PolarDBXReconciler struct {
	BaseRc *control.BaseReconcileContext
	Logger logr.Logger
	config.LoaderFactory
	MaxConcurrency int
}

func (r *PolarDBXReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log := r.Logger.WithValues("namespace", request.Namespace, "polardbxcluster", request.Name)

	if hint.IsNamespacePaused(request.Namespace) {
		log.Info("Reconciling is paused, skip")
		return reconcile.Result{}, nil
	}

	rc := polardbxreconcile.NewContext(
		control.NewBaseReconcileContextFrom(r.BaseRc, ctx, request),
		r.LoaderFactory(),
	)
	rc.SetPolarDBXKey(request.NamespacedName)
	defer rc.Close()

	polardbx, err := rc.GetPolarDBX()
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Object not found, might be deleted. Just ignore.")
			return reconcile.Result{}, nil
		}
		log.Error(err, "Unable to get polardbx object.")
		return reconcile.Result{}, err
	}

	return r.reconcile(rc, polardbx, log)
}

func (r *PolarDBXReconciler) newReconcileTask(rc *polardbxreconcile.Context, polardbx *polardbxv1.PolarDBXCluster, log logr.Logger) *control.Task {
	task := control.NewTask()

	readonly := polardbx.Spec.Readonly

	defer commonsteps.PersistentStatus(task, true)
	defer commonsteps.PersistentPolarDBXCluster(task, true)
	defer commonsteps.UpdateDisplayReplicas(task, true)

	// Abort immediately if operator is hinted to be forbidden.
	control.AbortWhen(helper.IsOperatorHintFound(polardbx, polardbxmeta.HintForbidden),
		"Hint forbidden found, abort.")(task)

	// Maintain the records in GMS for stateless pods.
	finalizersteps.HandleFinalizerForStatelessPods(task)

	// Goto deleting when the object is deleted.
	commonsteps.WhenDeletedAndNotDeleting(
		commonsteps.TransferPhaseTo(polardbxv1polardbx.PhaseDeleting, true),
	)(task)

	// Always check DNs when cluster is considered running.
	if helper.IsPhaseIn(polardbx, []polardbxv1polardbx.Phase{
		polardbxv1polardbx.PhaseRunning,
		polardbxv1polardbx.PhaseLocked}...,
	) {
		commonsteps.CheckDNs(task)
	}

	//try set runmode
	instancesteps.TrySetRunMode(task)

	// Let's construct the complex state machine.
	switch polardbx.Status.Phase {
	case polardbxv1polardbx.PhaseNew:
		guidesteps.ManipulateSpecAccordingToGuides(task)
		finalizersteps.SetupGuardFinalizer(task)
		commonsteps.InitializePolardbxLabel(task)
		commonsteps.GenerateRandInStatus(task)
		commonsteps.InitializeServiceName(task)
		commonsteps.TransferPhaseTo(polardbxv1polardbx.PhasePending, true)(task)

	case polardbxv1polardbx.PhasePending:
		control.When(polardbx.Spec.Restore != nil,
			commonsteps.CreateDummyBackupObject,
			pitr.LoadLatestBackupSetByTime,
			commonsteps.SyncSpecFromBackupSet)(task)
		checksteps.CheckStorageEngines(task)
		commonsteps.UpdateSnapshotAndObservedGeneration(task)
		instancesteps.CreateSecretsIfNotFound(task)
		instancesteps.CreateServicesIfNotFound(task)
		instancesteps.CreateConfigMapsIfNotFound(task)
		commonsteps.InitializeParameterTemplate(task)

		control.Branch(polardbx.Spec.Restore != nil,
			commonsteps.TransferPhaseTo(polardbxv1polardbx.PhaseRestoring, true),
			commonsteps.TransferPhaseTo(polardbxv1polardbx.PhaseCreating, true),
		)(task)

	case polardbxv1polardbx.PhaseCreating, polardbxv1polardbx.PhaseRestoring:
		// Update every time.
		commonsteps.UpdateSnapshotAndObservedGeneration(task)
		instancesteps.SyncDnReplicasAndCheckControllerRef(task)

		// Create readonly polardbx in InitReadonly list
		instancesteps.CreateOrReconcileReadonlyPolardbx(task)

		control.When(pitr.IsPitrRestore(polardbx),
			pitr.PreparePitrBinlogs,
			pitr.WaitPreparePitrBinlogs)(task)

		// Create GMS and DNs.
		instancesteps.CreateOrReconcileGMS(task)
		instancesteps.CreateOrReconcileDNs(task)

		// After GMS' ready, do initialization.
		control.Block(
			instancesteps.WaitUntilGMSReady,
			control.Branch(helper.IsPhaseIn(polardbx, polardbxv1polardbx.PhaseCreating),
				gmssteps.InitializeSchemas,
				gmssteps.RestoreSchemas,
			),
			control.When(!readonly,
				gmssteps.CreateAccounts,
				gmssteps.SyncDynamicConfigs(true),
			),
		)(task)

		// When all DNs' are ready, enable them in GMS.
		control.Block(
			instancesteps.WaitUntilDNsReady,
			gmssteps.EnableDNs,
		)(task)

		// If DN's replicas changed, we must remove the trailing DNs and disable them.
		control.Block(
			gmssteps.DisableTrailingDNs,
			instancesteps.RemoveTrailingDNs,
		)(task)

		// Then all the stateless components.
		control.Block(
			control.When(readonly,
				instancesteps.WaitUntilPrimaryCNDeploymentsRolledOut,
			),
			instancesteps.CreateOrReconcileCNs,
			instancesteps.CreateOrReconcileCDCs,
			instancesteps.WaitUntilCNDeploymentsRolledOut,
			instancesteps.WaitUntilCDCDeploymentsRolledOut,
			instancesteps.CreateFileStorage,
		)(task)

		// Go to clean works.
		control.Block(
			control.When(!debug.IsDebugEnabled(), commonsteps.UpdateDisplayDetailedVersion),
			control.When(polardbx.Status.Phase == polardbxv1polardbx.PhaseRestoring, commonsteps.CleanDummyBackupObject),
			commonsteps.UpdateDisplayStorageSize,
			control.When(pitr.IsPitrRestore(polardbx), pitr.CleanPreparePitrBinlogJob),
		)(task)

		commonsteps.TransferPhaseTo(polardbxv1polardbx.PhaseRunning, true)(task)

	case polardbxv1polardbx.PhaseRunning, polardbxv1polardbx.PhaseLocked:
		// Schedule after 10 seconds.
		defer control.ScheduleAfter(10*time.Second)(task, true)

		// Restart polardbx when restart parameters changed
		control.When(rc.GetPolarDBXRestarting(),
			restartsteps.GetRestartingPods,
			commonsteps.TransferPhaseTo(polardbxv1polardbx.PhaseRestarting, true),
		)(task)

		// Recalculate the storage size and sync dynamic configs.
		control.Block(
			commonsteps.UpdateDisplayStorageSize,
			// gmssteps.SyncDynamicConfigs(false),
		)(task)

		// Deal with lock.
		control.Branch(helper.IsPhaseIn(polardbx, polardbxv1polardbx.PhaseRunning),
			control.When(helper.IsAnnotationIndicatesToLock(polardbx),
				gmssteps.LockReadWrite,
				commonsteps.TransferPhaseTo(polardbxv1polardbx.PhaseLocked, true),
			),
			control.When(helper.IsAnnotationIndicatesToUnlock(polardbx),
				gmssteps.UnlockReadWrite,
				commonsteps.TransferPhaseTo(polardbxv1polardbx.PhaseRunning, true),
			),
		)(task)

		// Detect changes.
		control.When(helper.IsTopologyOrStaticConfigChanges(polardbx),
			commonsteps.TransferPhaseTo(polardbxv1polardbx.PhaseUpgrading, true),
		)(task)

		// Always reconcile the stateless components (mainly for rebuilt).
		instancesteps.CreateOrReconcileCNs(task)
		instancesteps.CreateOrReconcileCDCs(task)

		//sync cn label to pod without rebuild pod
		instancesteps.TrySyncCnLabelToPodsDirectly(task)

		// Update snapshot and observed generation.
		commonsteps.UpdateSnapshotAndObservedGeneration(task)
		instancesteps.SyncDnReplicasAndCheckControllerRef(task)
	case polardbxv1polardbx.PhaseUpgrading:
		// Update storage size and configs.
		control.Block(
			commonsteps.UpdateDisplayStorageSize,
			instancesteps.SyncDnReplicasAndCheckControllerRef,
			// gmssteps.SyncDynamicConfigs(false),
		)(task)

		switch polardbx.Status.Stage {
		case polardbxv1polardbx.StageEmpty:
			// Before doing rebalancing, the controller always trying to update
			// the CN/CDC deployments and DN stores.

			// Update before doing update.
			commonsteps.UpdateSnapshotAndObservedGeneration(task)

			control.Block(
				// GMS, update & wait
				control.When(!readonly,
					instancesteps.CreateOrReconcileGMS,
					instancesteps.WaitUntilGMSReady,
				),

				control.When(readonly,
					instancesteps.WaitUntilPrimaryCNDeploymentsRolledOut,
				),
				instancesteps.CreateOrReconcileCNs,
				instancesteps.CreateOrReconcileCDCs,
				// Only add or update, never remove.
				instancesteps.CreateOrReconcileDNs,

				instancesteps.WaitUntilDNsReady,
				instancesteps.WaitUntilCNDeploymentsRolledOut,
				instancesteps.WaitUntilCDCDeploymentsRolledOut,
				instancesteps.CreateFileStorage,
			)(task)

			// Prepare to rebalance data after DN stores are reconciled if necessary.
			commonsteps.TransferStageTo(polardbxv1polardbx.StageRebalanceStart, true)(task)

		case polardbxv1polardbx.StageRebalanceStart:
			// Prepare rebalance task context.
			rebalancesteps.PrepareRebalanceTaskContext(task)

			// Enable added DN stores.
			gmssteps.EnableDNs(task)

			// Wait terminated CN/CDCs to be finalized in GMS to avoid DDL problems.
			instancesteps.WaitUntilCNPodsStable(task)
			instancesteps.WaitUntilCDCPodsStable(task)
			// Start to rebalance data after DN stores are reconciled if necessary.
			rebalancesteps.StartRebalanceTask(task)

			// Transfer to rebalance stage.
			commonsteps.TransferStageTo(polardbxv1polardbx.StageRebalanceWatch, true)(task)

		case polardbxv1polardbx.StageRebalanceWatch:
			// Watch and update progress.
			rebalancesteps.WatchRebalanceTaskAntUpdateProgress(10 * time.Second)(task)

			// Ensure all trailing DNs are drained.
			rebalancesteps.EnsureTrailingDNsAreDrainedOrBlock(task)

			// Go to clean.
			commonsteps.TransferStageTo(polardbxv1polardbx.StageClean, true)(task)

		case polardbxv1polardbx.StageClean:
			// Reset rebalance task and status.
			rebalancesteps.ResetRebalanceTask(task)

			// If DN's replicas changed, we must remove the trailing DNs and disable them.
			control.Block(
				gmssteps.DisableTrailingDNs,
				instancesteps.RemoveTrailingDNs,
			)(task)

			control.When(!debug.IsDebugEnabled(), commonsteps.UpdateDisplayDetailedVersion)(task)

			commonsteps.TransferPhaseTo(polardbxv1polardbx.PhaseRunning, true)(task)
		}
	case polardbxv1polardbx.PhaseDeleting:
		finalizersteps.CleanGMS(task)
		// Block until other finalizers are removed.
		finalizersteps.BlockBeforeOtherSystemsFinalized(task)

		// Remove finalizers on other resources.
		finalizersteps.RemoveResidualFinalizersOnPods(task)
		finalizersteps.RemoveFinalizersOnStores(task)

		// Remove guard finalizers to let self can be removed.
		finalizersteps.RemoveGuardFinalizer(task)
	case polardbxv1polardbx.PhaseRestarting:
		// Restart CN by deleting pods
		control.Branch(
			restartsteps.IsRollingRestart(polardbx),
			restartsteps.RollingRestartPods,
			restartsteps.RestartingPods,
		)(task)

		restartsteps.ClosePolarDBXRestartPhase(task)
		commonsteps.TransferPhaseTo(polardbxv1polardbx.PhaseRunning, true)(task)
	case polardbxv1polardbx.PhaseFailed:
	case polardbxv1polardbx.PhaseUnknown:

	}

	return task
}

func (r *PolarDBXReconciler) reconcile(rc *polardbxreconcile.Context, polardbx *polardbxv1.PolarDBXCluster, log logr.Logger) (reconcile.Result, error) {
	log = log.WithValues("phase", polardbx.Status.Phase, "stage", polardbx.Status.Stage)

	task := r.newReconcileTask(rc, polardbx, log)
	return control.NewExecutor(log).Execute(rc, task)
}

func mapRequestsWhenStatelessPodDeletedOrFailed(object client.Object) []reconcile.Request {
	if polardbxName, ok := object.GetLabels()[polardbxmeta.LabelName]; ok {
		pod := object.(*corev1.Pod)

		// Only for CN & CDC pods.
		role := pod.Labels[polardbxmeta.LabelRole]
		if role != polardbxmeta.RoleCN && role != polardbxmeta.RoleCDC {
			return nil
		}

		// Enqueue requests only for deleted pods & evicted pods
		if !object.GetDeletionTimestamp().IsZero() ||
			pod.Status.Phase == corev1.PodFailed {
			return []reconcile.Request{
				{
					NamespacedName: types.NamespacedName{
						Name:      polardbxName,
						Namespace: pod.GetNamespace(),
					},
				},
			}
		}
	}
	return nil
}

func (r *PolarDBXReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: r.MaxConcurrency,
			RateLimiter: workqueue.NewMaxOfRateLimiter(
				workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 300*time.Second),
				// 60 qps, 10 bucket size.  This is only for retry speed. It's only the overall factor (not per item).
				&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(60), 10)},
			),
		}).
		For(&polardbxv1.PolarDBXCluster{}).
		// Watches owned XStores.
		Owns(&polardbxv1.XStore{}).
		// Watches owned Deployments.
		Owns(&appsv1.Deployment{}).
		// Watches owned Parameters
		Owns(&polardbxv1.PolarDBXParameter{}).
		// Watches deleted or failed CN/CDC Pods.
		Watches(
			&source.Kind{Type: &corev1.Pod{}},
			handler.EnqueueRequestsFromMapFunc(mapRequestsWhenStatelessPodDeletedOrFailed),
		).
		Complete(r)
}
