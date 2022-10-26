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
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/alibaba/polardbx-operator/pkg/debug"
	"github.com/alibaba/polardbx-operator/pkg/featuregate"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/change/driver/planner"
	changereconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/change/driver/reconcile"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/factory"
	galaxyfactory "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin/galaxy/factory"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	xstoreplugincommonsteps "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin/common/steps"
	galaxyinstancesteps "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin/galaxy/steps/instance"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	instancesteps "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/steps/instance"
)

type GalaxyReconciler struct {
}

func (r *GalaxyReconciler) Reconcile(rc *xstorev1reconcile.Context, log logr.Logger, request reconcile.Request) (reconcile.Result, error) {
	xstore := rc.MustGetXStore()
	log = log.WithValues("phase", xstore.Status.Phase, "stage", xstore.Status.Stage)

	task, err := r.newReconcileTask(rc, xstore, log)
	if err != nil {
		log.Error(err, "Failed to build reconcile task.")
		return reconcile.Result{}, err
	}

	return control.NewExecutor(log).Execute(rc, task)
}

func (r *GalaxyReconciler) newReconcileTask(rc *xstorev1reconcile.Context, xstore *polardbxv1.XStore, log logr.Logger) (*control.Task, error) {
	task := control.NewTask()
	readonly := xstore.Spec.Readonly

	// Deferred steps, will always be executed in the deferred sequence.
	defer instancesteps.PersistentStatus(task, true)
	defer instancesteps.PersistentXStore(task, true)
	defer instancesteps.UpdateDisplayStatus(task, true)

	// Weave steps according to current status, aka. construct a huge state machine.
	instancesteps.AbortReconcileIfHintFound(task)
	instancesteps.MoveToPhaseDeletingIfDeleted(task)

	switch xstore.Status.Phase {
	case polardbxv1xstore.PhaseNew:
		instancesteps.CheckTopologySpec(task)

		// Update to render display status.
		instancesteps.UpdateObservedGeneration(task)
		instancesteps.UpdateObservedTopologyAndConfig(task)

		instancesteps.GenerateRandInStatus(task)
		instancesteps.FillServiceNameIfNotProvided(task)
		instancesteps.InjectFinalizerOnXStore(task)
		instancesteps.UpdatePhaseTemplate(polardbxv1xstore.PhasePending)(task)
	case polardbxv1xstore.PhasePending:

		// Try to create the supporting resources like
		//   * Service for network in/outside cluster
		//   * Secret for storing accounts
		//   * ConfigMap for sharing information and templates among nodes
		instancesteps.CreateSecret(task)
		galaxyinstancesteps.CreateServices(task)
		galaxyinstancesteps.CreateOrUpdateConfigMaps(task)

		// Prepare as much host path volumes as specified in the topology.
		instancesteps.PrepareHostPathVolumes(task)

		// Update the observed generation at the end of pending phase.
		instancesteps.UpdateObservedGeneration(task)
		instancesteps.UpdateObservedTopologyAndConfig(task)
		instancesteps.InitializeParameterTemplate(task)

		if xstore.Spec.Restore == nil {
			instancesteps.UpdatePhaseTemplate(polardbxv1xstore.PhaseCreating)(task)
		} else {
			instancesteps.CheckXStoreRestoreSpec(task)
			instancesteps.UpdatePhaseTemplate(polardbxv1xstore.PhaseRestoring)(task)
		}
	case polardbxv1xstore.PhaseCreating:
		// Create pods and save volumes/ports into status.
		galaxyinstancesteps.CreatePodsAndServices(task)
		instancesteps.BindHostPathVolumesToHost(task)
		instancesteps.BindPodPorts(task)

		// Wait until pods' scheduled.
		instancesteps.WaitUntilPodsScheduled(task)

		// Sync blkio cgroups.
		instancesteps.SyncBlkioCgroupResourceLimits(task)

		// Unblock bootstrap.
		xstoreplugincommonsteps.UnblockBootstrap(task)

		// Wait until leader ready.
		instancesteps.WaitUntilCandidatesAndVotersReady(task)

		// Role reconciliation.
		instancesteps.ReconcileConsensusRoleLabels(task)
		instancesteps.WaitUntilLeaderElected(task)
		control.When(readonly,
			instancesteps.AddLearnerNodesToClusterOnLeader,
			instancesteps.RestoreToLearner,
		)(task)
		// Try to initialize things on leader.
		control.When(!readonly,
			instancesteps.CreateAccounts,
		)(task)
		xstoreplugincommonsteps.SetVoterElectionWeightToOne(task)

		// Check connectivity and set engine version into status.
		control.Branch(debug.IsDebugEnabled(),
			instancesteps.QueryAndUpdateEngineVersion,          // Query the engine version via command. (DEBUG)
			instancesteps.CheckConnectivityAndSetEngineVersion, // Updates the engine version by accessing directly
		)(task)

		// Go to phase "Running".
		instancesteps.UpdatePhaseTemplate(polardbxv1xstore.PhaseRunning)(task)
	case polardbxv1xstore.PhaseRestoring:
		switch xstore.Status.Stage {
		case polardbxv1xstore.StageEmpty:
			// Create pods and save volumes/ports into status.
			galaxyinstancesteps.CreatePodsAndServices(task)
			instancesteps.BindHostPathVolumesToHost(task)
			instancesteps.BindPodPorts(task)

			// Wait until pods' scheduled.
			instancesteps.WaitUntilPodsScheduled(task)

			xstoreplugincommonsteps.SyncNodesInfoAndKeepBlock(task)
			instancesteps.PrepareRestoreJobContext(task)
			instancesteps.StartRestoreJob(task)
			instancesteps.WaitUntilRestoreJobFinished(task)
			// Unblock bootstrap.
			xstoreplugincommonsteps.UnblockBootstrap(task)

			// Wait until leader ready.
			instancesteps.WaitUntilCandidatesAndVotersReady(task)

			// Role reconciliation.
			instancesteps.ReconcileConsensusRoleLabels(task)

			instancesteps.WaitUntilLeaderElected(task)

			instancesteps.StartRecoverJob(task)
			instancesteps.WaitUntilRecoverJobFinished(task)

			// Check connectivity and set engine version into status.
			control.Branch(debug.IsDebugEnabled(),
				instancesteps.QueryAndUpdateEngineVersion,          // Query the engine version via command. (DEBUG)
				instancesteps.CheckConnectivityAndSetEngineVersion, // Updates the engine version by accessing directly.
			)(task)

			instancesteps.UpdateStageTemplate(polardbxv1xstore.StageClean)(task)
		case polardbxv1xstore.StageClean:
			// clean up restore context
			instancesteps.RemoveRestoreJob(task)
			instancesteps.RemoveRecoverJob(task)

			// Go to phase "Running".
			instancesteps.UpdateStageTemplate(polardbxv1xstore.StageEmpty)(task)
			instancesteps.UpdatePhaseTemplate(polardbxv1xstore.PhaseRunning)(task)
		}
	case polardbxv1xstore.PhaseRunning:
		switch xstore.Status.Stage {
		case polardbxv1xstore.StageEmpty:
			// Restart xstore when restart parameters changed
			control.When(rc.GetXStoreRestarting(),
				instancesteps.GetRestartingPods,
				instancesteps.UpdatePhaseTemplate(polardbxv1xstore.PhaseRestarting, true),
			)(task)

			// Update my.cnf.override when xstore parameters changed
			control.When(rc.GetXStoreUpdateConfingMap(),
				control.Block(instancesteps.UpdatePhaseTemplate(polardbxv1xstore.PhaseUpgrading),
					instancesteps.UpdateStageTemplate(polardbxv1xstore.StageUpdate, true),
				),
			)(task)

			// Role reconciliation.
			if featuregate.EnableGalaxyClusterMode.Enabled() {
				instancesteps.ReconcileConsensusRoleLabels(task)
			} else {
				galaxyinstancesteps.DummyReconcileConsensusRoleLabels(task)
			}

			// Goto repair if deleted pods found. We do not do repair for running pods
			// with host failure. It should be handled manually as the Kubernetes
			// guides (i.e. manually cordon the node evict the pods on it).
			instancesteps.WhenPodsDeletedFound(
				instancesteps.UpdatePhaseTemplate(polardbxv1xstore.PhaseRepairing),
				control.Retry("Start repairing..."),
			)(task)

			// Wait until leader ready.
			instancesteps.WaitUntilCandidatesAndVotersReady(task)

			// Role reconciliation.
			instancesteps.ReconcileConsensusRoleLabels(task)
			control.When(readonly,
				instancesteps.AddLearnerNodesToClusterOnLeader,
			)(task)
			instancesteps.WaitUntilLeaderElected(task)

			// Purge logs with interval specified (but not less than 2 minutes).
			logPurgeInterval := 2 * time.Minute
			if xstore.Spec.Config.Dynamic.LogPurgeInterval != nil {
				specifiedDuration := xstore.Spec.Config.Dynamic.LogPurgeInterval.Duration
				if specifiedDuration >= 2*time.Minute {
					logPurgeInterval = specifiedDuration
				}
			}
			galaxyinstancesteps.PurgeLogsTemplate(logPurgeInterval)(task)

			// Update host path volume sizes.
			instancesteps.UpdateHostPathVolumeSizesTemplate(time.Minute)(task)

			// Branch disk quota exceeds, lock all candidates and requeue immediately.
			instancesteps.WhenDiskQuotaExceeds(
				instancesteps.UpdateStageTemplate(polardbxv1xstore.StageLocking),
				control.Retry("Start locking..."),
			)(task)

			instancesteps.WhenEngineConfigChanged(
				//Sync engine config map
				instancesteps.SyncEngineConfigMap,
			)(task)

			// Sync my.cnf from my.cnf.override
			instancesteps.UpdateMycnfParameters(task)

			// Goto upgrading if topology changed. (not breaking the task flow)
			instancesteps.WhenTopologyChanged(
				instancesteps.UpdatePhaseTemplate(polardbxv1xstore.PhaseUpgrading),
			)(task)

			//Goto upgrading if some dynamic config changed
			instancesteps.WhenDynamicConfigChanged(
				instancesteps.UpdatePhaseTemplate(polardbxv1xstore.PhaseUpgrading),
				control.Retry("Start PhaseUpgrading..."),
			)(task)

			// Update the observed generation at the end of running phase.
			instancesteps.UpdateObservedGeneration(task)
			instancesteps.UpdateObservedTopologyAndConfig(task)

			control.RetryAfter(10*time.Second, "Loop while running")(task)
		case polardbxv1xstore.StageLocking:
			// Set super-read-only on all candidates.
			galaxyinstancesteps.SetSuperReadOnlyOnAllCandidates(task)

			instancesteps.UpdateStageTemplate(polardbxv1xstore.StageEmpty)(task)
			instancesteps.UpdatePhaseTemplate(polardbxv1xstore.PhaseLocked)(task)
		}
	case polardbxv1xstore.PhaseLocked:
		// Purge logs every 30 seconds.
		galaxyinstancesteps.PurgeLogsTemplate(30 * time.Second)(task)

		// Update volume sizes every 30 seconds.
		instancesteps.UpdateHostPathVolumeSizesTemplate(30 * time.Second)(task)

		// Unset super-read-only on all candidates.
		instancesteps.WhenDiskQuotaNotExceeds(
			galaxyinstancesteps.UnsetSuperReadOnlyOnAllCandidates,
			instancesteps.UpdatePhaseTemplate(polardbxv1xstore.PhaseRunning),
		)(task)

		// Block wait, requeue every 30 seconds.
		control.RetryAfter(30*time.Second, "Check every 30 seconds...")(task)
	case polardbxv1xstore.PhaseUpgrading, polardbxv1xstore.PhaseRepairing:
		selfHeal := xstore.Status.Phase == polardbxv1xstore.PhaseRepairing

		switch xstore.Status.Stage {
		case polardbxv1xstore.StageEmpty:
			ec, err := instancesteps.LoadExecutionContext(rc)
			if err != nil {
				return nil, err
			}
			if ec == nil {
				ec, err = instancesteps.NewExecutionContext(rc, xstore, selfHeal)
			}

			defer instancesteps.TrackAndLazyUpdateExecuteContext(ec)(task, true)
			if featuregate.EnableGalaxyClusterMode.Enabled() {
				instancesteps.ReconcileConsensusRoleLabels(task)
			} else {
				galaxyinstancesteps.DummyReconcileConsensusRoleLabels(task)
			}

			// Set pod factory.
			ec.PodFactory = &galaxyfactory.ExtraPodFactoryGalaxy{
				Delegate: &factory.DefaultExtraPodFactory{},
			}

			pl := planner.NewPlanner(rc, ec, selfHeal)
			if err := pl.Prepare(); err != nil {
				return nil, fmt.Errorf("failed to prepare planner: %w", err)
			}
			if pl.NeedRebuild() {
				if err := pl.Build(); err != nil {
					return nil, fmt.Errorf("failed to build plan: %w", err)
				}
				control.Retry("Plan's built, start executing...")(task)
			} else {
				control.Branch(ec.Completed(),
					// Update the snapshots on completion.
					control.Block(
						instancesteps.UpdateObservedGeneration,
						instancesteps.UpdateObservedTopologyAndConfig,
						instancesteps.UpdateStageTemplate(polardbxv1xstore.StageClean, true),
					),

					// Execute the plan.
					changereconcile.BuildExecutionBlock(ec),
				)(task)
			}
		case polardbxv1xstore.StageClean:
			instancesteps.DeleteExecutionContext(task)
			instancesteps.UpdatePhaseTemplate(polardbxv1xstore.PhaseRunning, true)(task)
		case polardbxv1xstore.StageUpdate:
			instancesteps.GetParametersRoleMap(task)
			instancesteps.UpdateXStoreConfigMap(task)
			instancesteps.SetGlobalVariables(task)
			instancesteps.CloseXStoreUpdatePhase(task)
			instancesteps.UpdatePhaseTemplate(polardbxv1xstore.PhaseRunning, true)(task)
		}

	case polardbxv1xstore.PhaseDeleting:
		//try clean xStore follower job
		instancesteps.CleanRebuildJob(task)

		control.When(readonly,
			instancesteps.DropLearnerOnLeader,
		)(task)
		// Cancel all async tasks.
		instancesteps.CancelAsyncTasks(task)

		// Wait until async tasks completed (to avoid race on data).
		instancesteps.WaitUntilAsyncTasksCanceled(task)

		// Mark all pods as deleted to avoid restart.
		instancesteps.DeleteAllPods(task)

		// Delete host path volumes.
		instancesteps.DeleteHostPathVolumes(task)

		// Then let it go.
		instancesteps.RemoveFinalizerFromXStore(task)
	case polardbxv1xstore.PhaseRestarting:
		instancesteps.WhenPodsDeletedFound(
			instancesteps.UpdatePhaseTemplate(polardbxv1xstore.PhaseRepairing),
			control.Retry("Start repairing..."),
		)(task)
		// Restart DN by deleting pods
		control.Branch(
			instancesteps.IsRollingRestart(xstore),
			instancesteps.RollingRestartPods,
			instancesteps.RestartingPods,
		)(task)
		instancesteps.CloseXStoreRestartPhase(task)
		instancesteps.UpdatePhaseTemplate(polardbxv1xstore.PhaseRunning, true)(task)
	case polardbxv1xstore.PhaseFailed:
		log.Info("Failed.")
	case polardbxv1xstore.PhaseUnknown:
		log.Info("Unknown.")
	default:
		log.Info("Unknown.")
	}

	return task, nil
}
