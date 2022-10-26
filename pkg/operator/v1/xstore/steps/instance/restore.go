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

package instance

import (
	"fmt"
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	xstorev1 "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin/common/channel"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	"github.com/alibaba/polardbx-operator/pkg/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"time"
)

type RestoreJobContext struct {
	BackupFilePath      string                   `json:"backupFilePath,omitempty"`
	BackupCommitIndex   *int64                   `json:"backupCommitIndex,omitempty"`
	BinlogDirPath       string                   `json:"binlogDirPath,omitempty"`
	BinlogEndOffsetPath string                   `json:"binlogEndOffsetPath,omitempty"`
	IndexesPath         string                   `json:"indexesPath,omitempty"`
	CpFilePath          string                   `json:"cpfilePath,omitempty"`
	StorageName         polardbxv1.BackupStorage `json:"storageName,omitempty"`
	Sink                string                   `json:"sink,omitempty"`
}

var CheckXStoreRestoreSpec = xstorev1reconcile.NewStepBinder("CheckXStoreRestoreSpec",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()
		if xstore.Spec.Restore == nil {
			return flow.Pass()
		}

		restoreSpec := xstore.Spec.Restore

		if len(restoreSpec.From.XStoreName) == 0 {
			return flow.Wait("Restore XStoreName invalid")
		}

		if len(restoreSpec.BackupSet) == 0 && len(restoreSpec.Time) == 0 {
			return flow.Wait("Restore spec invalid, failed to determine how to restore!")
		}
		if len(restoreSpec.BackupSet) == 0 && restoreSpec.BackupSet == "" {
			_, err := rc.ParseRestoreTime()
			if err != nil {
				return flow.Error(err, "Unable to parse restore time!")
			}
		}
		return flow.Pass()
	})

var StartRestoreJob = xstorev1reconcile.NewStepBinder("StartRestoreJob",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		const restoreJobKey = "restore"
		restoreJobContext := &RestoreJobContext{}
		err := rc.GetTaskContext(restoreJobKey, &restoreJobContext)
		if err != nil {
			return flow.Error(err, "Unable to get task context for restore")
		}

		xstore := rc.MustGetXStore()

		// Create restore job for each pod.
		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.Error(err, "Unable to get pods for xcluster.")
		}

		jobCreated := false
		for _, pod := range pods {
			job, err := rc.GetXStoreJob(util.GetStableNameSuffix(xstore, pod.Name) + "-restore")
			if client.IgnoreNotFound(err) != nil {
				return flow.Error(err, "Unable to get restore data job.", "pod", pod.Name)
			}

			// If not found, create one.
			if job == nil {
				job = newRestoreDataJob(xstore, &pod)
				if err := rc.SetControllerRefAndCreate(job); err != nil {
					return flow.Error(err, "Unable to create job to restore data", "pod", pod.Name)
				}
				jobCreated = true
			}
		}

		if jobCreated {
			return flow.Wait("Restore data jobs created! Waiting for completion...")
		}

		return flow.Continue("Restore data jobs started!")
	})

var WaitUntilRestoreJobFinished = xstorev1reconcile.NewStepBinder("WaitUntilRestoreJobFinished",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()

		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.Error(err, "Unable to get pods for xcluster.")
		}
		for _, pod := range pods {
			job, err := rc.GetXStoreJob(util.GetStableNameSuffix(xstore, pod.Name) + "-restore")
			if err != nil {
				return flow.Error(err, "Unable to get xstore restore data job", "pod", pod.Name)
			}

			if !k8shelper.IsJobCompleted(job) {
				return flow.Wait("Job's not completed! Wait... ", "job", job.Name, "pod", pod.Name)
			}
		}

		return flow.Continue("Restore Job completed!")
	})

var PrepareRestoreJobContext = xstorev1reconcile.NewStepBinder("PrepareRestoreJobContext",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		const restoreJobKey = "restore"

		// Check if exists, exit if true.

		exists, err := rc.IsTaskContextExists(restoreJobKey)
		if err != nil {
			return flow.Error(err, "Unable to determine job context for restore!")
		}
		if exists {
			return flow.Pass()
		}

		// Prepare context.
		xstore := rc.MustGetXStore()

		fromXStoreName := xstore.Spec.Restore.From.XStoreName
		backup := &polardbxv1.XStoreBackup{}
		if xstore.Spec.Restore.BackupSet == "" || len(xstore.Spec.Restore.BackupSet) == 0 {
			// Parse restore time.
			restoreTime := rc.MustParseRestoreTime()
			// Get last backup
			backup, err = rc.GetLastCompletedXStoreBackup(map[string]string{
				xstoremeta.LabelName: fromXStoreName,
			}, restoreTime)
			if err != nil {
				return flow.Error(err, "Unable to get last usable backup", "restore-time", restoreTime)
			}
			// Quit if last backup not found
			if backup == nil {
				// Update the phase to failed
				rc.UpdateXStoreCondition(&xstorev1.Condition{
					Type:    xstorev1.Restorable,
					Status:  corev1.ConditionFalse,
					Reason:  "BackupNotFound",
					Message: "Last usable backup isn't found! Restore time is " + restoreTime.String(),
				})
				xstore.Status.Phase = xstorev1.PhaseFailed

				return flow.Wait("Last usable backup isn't found!", "restore-time", restoreTime)
			}
		} else {
			xstoreBackupKey := types.NamespacedName{Namespace: rc.Namespace(), Name: xstore.Spec.Restore.BackupSet}
			err := rc.Client().Get(rc.Context(), xstoreBackupKey, backup)
			if err != nil {
				return flow.Error(err, "Unable to get xstoreBackup by BackupSet")
			}
		}
		//Update sharedchannel
		sharedCm, err := rc.GetXStoreConfigMap(convention.ConfigMapTypeShared)
		if err != nil {
			return flow.Error(err, "Unable to get shared config map.")
		}

		sharedChannel, err := parseChannelFromConfigMap(sharedCm)
		if err != nil {
			return flow.Error(err, "Unable to parse shared channel from config map.")
		}

		sharedChannel.UpdateLastBackupBinlogIndex(&backup.Status.CommitIndex)
		sharedCm.Data[channel.SharedChannelKey] = sharedChannel.String()
		err = rc.Client().Update(rc.Context(), sharedCm)
		if err != nil {
			return flow.Error(err, "Unable to update shared config map.")
		}

		backupRootPath := backup.Status.BackupRootPath
		fullBackupPath := fmt.Sprintf("%s/%s/%s.xbstream",
			backupRootPath, polardbxmeta.FullBackupPath, fromXStoreName)
		binlogEndOffsetPath := fmt.Sprintf("%s/%s/%s-end",
			backupRootPath, polardbxmeta.BinlogOffsetPath, fromXStoreName)
		indexesPath := fmt.Sprintf("%s/%s", backupRootPath, polardbxmeta.BinlogIndexesName)
		binlogBackupDir := fmt.Sprintf("%s/%s/%s",
			backupRootPath, polardbxmeta.BinlogBackupPath, fromXStoreName)
		cpFilePath := fmt.Sprintf("%s/%s/%s",
			backupRootPath, polardbxmeta.BinlogOffsetPath, polardbxmeta.SeekCpName)

		// Save.
		if err := rc.SaveTaskContext(restoreJobKey, &RestoreJobContext{
			BackupFilePath:      fullBackupPath,
			BackupCommitIndex:   &backup.Status.CommitIndex,
			BinlogDirPath:       binlogBackupDir,
			BinlogEndOffsetPath: binlogEndOffsetPath,
			IndexesPath:         indexesPath,
			CpFilePath:          cpFilePath,
			StorageName:         backup.Spec.StorageProvider.StorageName,
			Sink:                backup.Spec.StorageProvider.Sink,
		}); err != nil {
			return flow.Error(err, "Unable to save job context for restore!")
		}
		return flow.Continue("Job context for restore prepared!")
	})

var RemoveRestoreJob = xstorev1reconcile.NewStepBinder("RemoveRestoreJob",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()

		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.Error(err, "Unable to get pods for xcluster.")
		}
		for _, pod := range pods {
			job, err := rc.GetXStoreJob(util.GetStableNameSuffix(xstore, pod.Name) + "-restore")
			if err != nil {
				return flow.Error(err, "Unable to get xstore restore data job", "pod", pod.Name)
			}
			if job == nil {
				return flow.Continue("Restore job removed!")
			}

			err = rc.Client().Delete(rc.Context(), job, client.PropagationPolicy(metav1.DeletePropagationBackground))
			if client.IgnoreNotFound(err) != nil {
				return flow.Error(err, "Unable to remove restore job", "job-name", job.Name)
			}
		}
		return flow.Continue("Restore job removed!")
	})

func parseChannelFromConfigMap(cm *corev1.ConfigMap) (*channel.SharedChannel, error) {
	sharedChannel := &channel.SharedChannel{}
	err := sharedChannel.Load(cm.Data[channel.SharedChannelKey])
	if err != nil {
		return nil, err
	}
	return sharedChannel, nil
}

var StartRecoverJob = xstorev1reconcile.NewStepBinder("StartRecoverJob",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()

		if xstore.Labels[polardbxmeta.LabelRole] == polardbxmeta.RoleGMS {
			return flow.Continue("GMS don not need recover data", "xstore-name", xstore.Name)
		}
		const restoreJobKey = "restore"
		restoreJobContext := &RestoreJobContext{}
		err := rc.GetTaskContext(restoreJobKey, &restoreJobContext)
		if err != nil {
			return flow.Error(err, "Unable to get task context for restore")
		}

		// Create restore job for leader pod.
		leaderPod, err := rc.TryGetXStoreLeaderPod()
		if err != nil {
			return flow.Error(err, "Unable to get leaderPod for xcluster.")
		}
		if leaderPod == nil {
			return flow.RetryAfter(5*time.Second, "Leader pod not found")
		}
		jobCreated := false

		job, err := rc.GetXStoreJob(util.GetStableNameSuffix(xstore, leaderPod.Name) + "-recover")
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to get recover data job.", "pod", leaderPod.Name)
		}

		// If not found, create one.
		if job == nil {
			secret, err := rc.GetXStoreAccountPassword(convention.SuperAccount)
			if err != nil {
				flow.Error(err, "Unable to get secret", "xstore-name", xstore.Name)
			}
			job = newRecoverDataJob(xstore, leaderPod, secret)
			if err := rc.SetControllerRefAndCreate(job); err != nil {
				return flow.Error(err, "Unable to create job to recover data", "pod", leaderPod.Name)
			}
			jobCreated = true
		}

		if jobCreated {
			return flow.Wait("Recover data jobs created! Waiting for completion...")
		}

		return flow.Continue("Recover data jobs started!")
	})

var WaitUntilRecoverJobFinished = xstorev1reconcile.NewStepBinder("WaitUntilRecoverJobFinished",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()
		if xstore.Labels[polardbxmeta.LabelRole] == polardbxmeta.RoleGMS {
			return flow.Continue("GMS don not need recover data", "xstore-name", xstore.Name)
		}
		leaderPod, err := rc.TryGetXStoreLeaderPod()
		if err != nil {
			return flow.Error(err, "Unable to get leaderPod for xcluster.")
		}
		if leaderPod == nil {
			return flow.RetryAfter(5*time.Second, "Leader pod not found")
		}
		job, err := rc.GetXStoreJob(util.GetStableNameSuffix(xstore, leaderPod.Name) + "-recover")

		if err != nil {
			return flow.Error(err, "Unable to get xstore recover data job", "pod", leaderPod.Name)
		}

		if !k8shelper.IsJobCompleted(job) {
			return flow.Wait("Job's not completed! Wait... ", "job", job.Name, "pod", leaderPod.Name)
		}

		return flow.Continue("Recover Job completed!")
	})

var RemoveRecoverJob = xstorev1reconcile.NewStepBinder("RemoveRecoverJob",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()
		if xstore.Labels[polardbxmeta.LabelRole] == polardbxmeta.RoleGMS {
			return flow.Continue("GMS don not need remove recover job", "xstore-name", xstore.Name)
		}
		leaderPod, err := rc.TryGetXStoreLeaderPod()
		if err != nil {
			return flow.Error(err, "Unable to get leaderPod for xcluster.")
		}
		job, err := rc.GetXStoreJob(util.GetStableNameSuffix(xstore, leaderPod.Name) + "-recover")
		if err != nil {
			return flow.Error(err, "Unable to get xstore recover data job", "pod", leaderPod.Name)
		}
		if job == nil {
			return flow.Continue("Recover job already removed!")
		}
		err = rc.Client().Delete(rc.Context(), job, client.PropagationPolicy(metav1.DeletePropagationBackground))
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to remove recover job", "job-name", job.Name)
		}
		return flow.Continue("Recover job removed!")
	})
