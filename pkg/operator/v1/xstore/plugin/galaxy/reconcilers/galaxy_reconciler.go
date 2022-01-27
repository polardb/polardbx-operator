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
	"time"

	"github.com/alibaba/polardbx-operator/pkg/debug"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

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

	task := r.newReconcileTask(rc, xstore, log)
	return control.NewExecutor(log).Execute(rc, task)
}

func (r *GalaxyReconciler) newReconcileTask(rc *xstorev1reconcile.Context, xstore *polardbxv1.XStore, log logr.Logger) *control.Task {
	task := control.NewTask()

	// Deferred steps, will always be executed in the deferred sequence.
	defer instancesteps.PersistentStatus(task, true)
	defer instancesteps.PersistentXStore(task, true)
	defer instancesteps.UpdateDisplayStatus(task, true)

	// Weave steps according to current status, aka. construct a huge state machine.
	instancesteps.AbortReconcileIfHintFound(task)
	instancesteps.MoveToPhaseDeletingIfDeleted(task)

	switch xstore.Status.Phase {
	case polardbxv1xstore.PhaseNew:
		galaxyinstancesteps.CheckTopologySpec(task)

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

		// TODO Transfer to creating or restoring phase.
		instancesteps.UpdatePhaseTemplate(polardbxv1xstore.PhaseCreating)(task)
	case polardbxv1xstore.PhaseCreating:
		// Create pods and save volumes/ports into status.
		galaxyinstancesteps.CreatePodsAndHeadlessServices(task)
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

		// Dummy role reconciliation.
		galaxyinstancesteps.DummyReconcileConsensusRoleLabels(task)

		instancesteps.WaitUntilLeaderElected(task)

		// Try to initialize things on leader.
		instancesteps.CreateAccounts(task)

		// Check connectivity and set engine version into status.
		control.Branch(debug.IsDebugEnabled(),
			instancesteps.QueryAndUpdateEngineVersion,          // Query the engine version via command. (DEBUG)
			instancesteps.CheckConnectivityAndSetEngineVersion, // Updates the engine version by accessing directly.
		)(task)

		// Go to phase "Running".
		instancesteps.UpdatePhaseTemplate(polardbxv1xstore.PhaseRunning)(task)
	case polardbxv1xstore.PhaseRestoring:
		// TODO impl restoring
	case polardbxv1xstore.PhaseRunning:
		switch xstore.Status.Stage {
		case polardbxv1xstore.StageEmpty:
			galaxyinstancesteps.DummyReconcileConsensusRoleLabels(task)

			// Goto repair if deleted pods found. We do not do repair for running pods
			// with host failure. It should be handled manually as the Kubernetes
			// guides (i.e. manually cordon the node evict the pods on it).
			instancesteps.WhenPodsDeletedFound(
				instancesteps.UpdatePhaseTemplate(polardbxv1xstore.PhaseRepairing),
				control.Retry("Start repairing..."),
			)(task)

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

			// Goto upgrading if topology changed. (not breaking the task flow)
			instancesteps.WhenTopologyChanged(
				instancesteps.UpdatePhaseTemplate(polardbxv1xstore.PhaseUpgrading),
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
	case polardbxv1xstore.PhaseUpgrading:
		// TODO impl upgrading
	case polardbxv1xstore.PhaseRepairing:
		// TODO impl repairing
	case polardbxv1xstore.PhaseDeleting:
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
	case polardbxv1xstore.PhaseFailed:
		log.Info("Failed.")
	case polardbxv1xstore.PhaseUnknown:
		log.Info("Unknown.")
	default:
		log.Info("Unknown.")
	}

	return task
}
