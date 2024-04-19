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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/api/v1/polardbx"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	xstorev1 "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/filestream"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/factory"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin/common/channel"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	"github.com/alibaba/polardbx-operator/pkg/util/name"
	polarxPath "github.com/alibaba/polardbx-operator/pkg/util/path"
	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"time"
)

type RestoreJobContext struct {
	BackupFilePath      string                 `json:"backupFilePath,omitempty"`
	BackupCommitIndex   *int64                 `json:"backupCommitIndex,omitempty"`
	BinlogDirPath       string                 `json:"binlogDirPath,omitempty"`
	BinlogEndOffsetPath string                 `json:"binlogEndOffsetPath,omitempty"`
	IndexesPath         string                 `json:"indexesPath,omitempty"`
	CpFilePath          string                 `json:"cpfilePath,omitempty"`
	StorageName         polardbx.BackupStorage `json:"storageName,omitempty"`
	Sink                string                 `json:"sink,omitempty"`
	PitrEndpoint        string                 `json:"pitrEndpoint,omitempty"`
	PitrXStore          string                 `json:"pitrXStore,omitempty"`
	PxcXStore           *bool                  `json:"pxcXStore,omitempty"`
	KeyringPath         string                 `json:"keyringPath,omitempty"`
	KeyringFilePath     string                 `json:"keyringFilePath,omitempty"`
}

// helper function to download metadata backup from remote storage
func downloadMetadataBackup(rc *xstorev1reconcile.Context) (*factory.MetadataBackup, error) {
	xstore := rc.MustGetXStore()
	filestreamClient, err := rc.GetFilestreamClient()
	filestreamClient.InitWaitChan()
	if err != nil {
		return nil, errors.New("failed to get filestream client, error: " + err.Error())
	}
	filestreamAction, err := polardbxv1polardbx.NewBackupStorageFilestreamAction(xstore.Spec.Restore.StorageProvider.StorageName)

	downloadActionMetadata := filestream.ActionMetadata{
		Action:    filestreamAction.Download,
		Sink:      xstore.Spec.Restore.StorageProvider.Sink,
		RequestId: uuid.New().String(),
		Filename:  polarxPath.NewPathFromStringSequence(xstore.Spec.Restore.From.BackupSetPath, "metadata"),
	}
	var downloadBuffer bytes.Buffer
	recvBytes, err := filestreamClient.Download(&downloadBuffer, downloadActionMetadata)
	if err != nil {
		return nil, errors.New("download metadata failed, error: " + err.Error())
	}
	if recvBytes == 0 {
		return nil, errors.New("no byte received, please check storage config and target path")
	}
	metadata := &factory.MetadataBackup{}
	err = json.Unmarshal(downloadBuffer.Bytes(), &metadata)
	if err != nil {
		return nil, errors.New("failed to parse metadata, error: " + err.Error())
	}
	return metadata, nil
}

func NewDummyXstoreBackup(rc *xstorev1reconcile.Context, metadata *factory.MetadataBackup) (*polardbxv1.XStoreBackup, error) {
	xstore := rc.MustGetXStore()

	if metadata == nil {
		return nil, errors.New("not enough information to create dummy xstore backup")
	}
	xstoreMetadata := metadata.XstoreMetadataList[0]
	xstoreBackup := &polardbxv1.XStoreBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name: name.NewSplicedName(
				name.WithTokens(metadata.BackupSetName, "dummy"),
				name.WithPrefix("dummy-xstore-backup"),
			),
			Namespace: xstore.Namespace,
			Annotations: map[string]string{
				meta.AnnotationDummyBackup: "true",
			},
		},
		Spec: polardbxv1.XStoreBackupSpec{
			XStore: polardbxv1.XStoreReference{
				Name: xstoreMetadata.Name,
				UID:  xstoreMetadata.UID,
			},
			StorageProvider: *xstore.Spec.Restore.StorageProvider,
		},
		Status: polardbxv1.XStoreBackupStatus{
			Phase:          polardbxv1.XStoreBackupDummy,
			CommitIndex:    xstoreMetadata.LastCommitIndex,
			BackupRootPath: metadata.BackupRootPath,
			TargetPod:      xstoreMetadata.TargetPod,
		},
	}
	return xstoreBackup, nil
}

func NewDummySecretBackup(rc *xstorev1reconcile.Context, metadata *factory.MetadataBackup) (*corev1.Secret, error) {
	xstore := rc.MustGetXStore()
	var dummySecretName string
	var sourceSecrets *[]polardbxv1polardbx.PrivilegeItem
	accounts := make(map[string]string)
	dummySecretName = name.NewSplicedName(
		name.WithTokens(metadata.BackupSetName, "dummy"),
		name.WithPrefix("dummy-xstore-secret"),
	)
	xstoreMetadata := metadata.XstoreMetadataList[0]
	sourceSecrets = &xstoreMetadata.Secrets
	for _, item := range *sourceSecrets {
		accounts[item.Username] = item.Password
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dummySecretName,
			Namespace: xstore.Namespace,
			Labels:    convention.ConstLabels(xstore),
		},
		StringData: accounts,
	}, nil

}

var CreateDummyBackupObject = xstorev1reconcile.NewStepBinder("CreateDummyBackupObject",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()
		if xstore.Spec.Restore.BackupSet != "" {
			return flow.Continue("Backup set is specified, no need to create dummy backup object")
		}
		if xstore.Spec.Restore.From.BackupSetPath == "" {
			return flow.Continue("BackupSetPath is not specified, no need to create dummy backup object")
		}

		metadata, err := downloadMetadataBackup(rc)

		if err != nil {
			UpdatePhaseTemplate(polardbxv1xstore.PhaseFailed)
			return flow.Error(err, "Failed to download metadata from backup set path",
				"path", xstore.Spec.Restore.From.BackupSetPath)
		}

		// Create dummy xstore backup
		xstorebackup, err := NewDummyXstoreBackup(rc, metadata)
		if err != nil {
			return flow.Error(err, "Failed to create dummy xstore backup")
		}
		xstorebackupStatus := xstorebackup.Status.DeepCopy()
		err = rc.SetControllerRefAndCreate(xstorebackup)
		if err != nil {
			return flow.Error(err, "Failed to create dummy xstore backup")
		}
		xstorebackup.Status = *xstorebackupStatus

		xstoreSecretBackup, err := NewDummySecretBackup(rc, metadata)
		if err != nil {
			return flow.Error(err, "Failed to new dummy xstore secret backup", "xstore", xstore.Name)
		}
		err = rc.SetControllerToOwnerAndCreate(xstorebackup, xstoreSecretBackup)
		if err != nil {
			return flow.Error(err, "Failed to create dummy xstore secret backup", "xstore", xstore.Name)
		}

		// Update status of xstore backup
		err = rc.Client().Status().Update(rc.Context(), xstorebackup)
		if err != nil {
			return flow.Error(err, "Failed to update dummy xstore backup status")
		}

		// The dummy backup object will be used in the later restore by setting it as backup set
		xstore.Spec.Restore.BackupSet = xstorebackup.Name
		xstore.Spec.Restore.From.XStoreName = xstorebackup.Spec.XStore.Name
		rc.MarkXStoreChanged()
		return flow.Continue("Dummy backup object created!")
	})

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

var LoadLatestBackupSetByTime = xstorev1reconcile.NewStepBinder("LoadLatestBackupSetByTime",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()
		if xstore.Spec.Restore.BackupSet == "" || len(xstore.Spec.Restore.BackupSet) == 0 {
			backup, err := rc.GetLastCompletedXStoreBackup(map[string]string{
				xstoremeta.LabelName: xstore.Spec.Restore.From.XStoreName,
			}, rc.MustParseRestoreTime())
			if err != nil {
				return flow.Error(err, "failed to get last completed xstore backup")
			}
			xstore.Spec.Restore.BackupSet = backup.Name
			rc.MarkXStoreChanged()
		}
		return flow.Continue("LoadLatestBackupSetByTime continue")
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
			job, err := rc.GetXStoreJob(name.GetStableNameSuffix(xstore, pod.Name) + "-restore")
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
			job, err := rc.GetXStoreJob(name.GetStableNameSuffix(xstore, pod.Name) + "-restore")
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
			// TODO(dengli): with metadata backup
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
				return flow.Error(err, "Can not get xstore backup set", "backup set key", xstoreBackupKey)
			}
		}
		if backup == nil {
			return flow.Error(errors.New("xstore backup obejct is null"), "Can not get xstore backup set")
		}
		backupRootPath := backup.Status.BackupRootPath
		lastCommitIndex := backup.Status.CommitIndex

		//Update sharedchannel
		sharedCm, err := rc.GetXStoreConfigMap(convention.ConfigMapTypeShared)
		if err != nil {
			return flow.Error(err, "Unable to get shared config map.")
		}

		sharedChannel, err := parseChannelFromConfigMap(sharedCm)
		if err != nil {
			return flow.Error(err, "Unable to parse shared channel from config map.")
		}

		sharedChannel.UpdateLastBackupBinlogIndex(&lastCommitIndex)
		sharedCm.Data[channel.SharedChannelKey] = sharedChannel.String()
		err = rc.Client().Update(rc.Context(), sharedCm)
		if err != nil {
			return flow.Error(err, "Unable to update shared config map.")
		}

		fullBackupPath := fmt.Sprintf("%s/%s/%s.xbstream",
			backupRootPath, polardbxmeta.FullBackupPath, fromXStoreName)
		binlogEndOffsetPath := fmt.Sprintf("%s/%s/%s-end",
			backupRootPath, polardbxmeta.BinlogOffsetPath, fromXStoreName)
		indexesPath := fmt.Sprintf("%s/%s", backupRootPath, polardbxmeta.BinlogIndexesName)
		binlogBackupDir := fmt.Sprintf("%s/%s/%s",
			backupRootPath, polardbxmeta.BinlogBackupPath, fromXStoreName)
		cpFilePath := fmt.Sprintf("%s/%s/%s",
			backupRootPath, polardbxmeta.BinlogOffsetPath, polardbxmeta.SeekCpName)
		_, pxcXStore := xstore.Labels[polardbxmeta.LabelName]
		keyringPath := ""
		keyringFilePath := ""

		//DN或标准版 且TDE开启恢复的时候下载keyring
		if xstore.Status.TdeStatus == true && xstore.Labels[polardbxmeta.LabelRole] != polardbxmeta.RoleGMS {
			tdeCm, err := rc.GetXStoreConfigMap(convention.ConfigMapTypeTde)
			if err != nil {
				return flow.Error(err, "Unable to get tde config map.")
			}
			keyringPath = fmt.Sprintf("%s/%s/%s",
				backupRootPath, polardbxmeta.KeyringPath, backup.Spec.XStore.Name)
			keyringFilePath = tdeCm.Data[convention.KeyringPath]
		}
		// Save.
		if err := rc.SaveTaskContext(restoreJobKey, &RestoreJobContext{
			BackupFilePath:      fullBackupPath,
			BackupCommitIndex:   &lastCommitIndex,
			BinlogDirPath:       binlogBackupDir,
			BinlogEndOffsetPath: binlogEndOffsetPath,
			IndexesPath:         indexesPath,
			CpFilePath:          cpFilePath,
			StorageName:         backup.Spec.StorageProvider.StorageName,
			Sink:                backup.Spec.StorageProvider.Sink,
			PitrEndpoint:        xstore.Spec.Restore.PitrEndpoint,
			PitrXStore:          xstore.Spec.Restore.From.XStoreName,
			PxcXStore:           &pxcXStore,
			KeyringPath:         keyringPath,
			KeyringFilePath:     keyringFilePath,
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
			job, err := rc.GetXStoreJob(name.GetStableNameSuffix(xstore, pod.Name) + "-restore")
			if err != nil && !apierrors.IsNotFound(err) {
				return flow.Error(err, "Unable to get xstore restore data job", "pod", pod.Name)
			}
			if job != nil {
				err = rc.Client().Delete(rc.Context(), job, client.PropagationPolicy(metav1.DeletePropagationBackground))
				if client.IgnoreNotFound(err) != nil {
					return flow.Error(err, "Unable to remove restore job", "job-name", job.Name)
				}
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

		job, err := rc.GetXStoreJob(name.GetStableNameSuffix(xstore, leaderPod.Name) + "-recover")
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
		job, err := rc.GetXStoreJob(name.GetStableNameSuffix(xstore, leaderPod.Name) + "-recover")

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
		job, err := rc.GetXStoreJob(name.GetStableNameSuffix(xstore, leaderPod.Name) + "-recover")
		if err != nil && !apierrors.IsNotFound(err) {
			return flow.Error(err, "Unable to get xstore recover data job", "pod", leaderPod.Name)
		}
		if job != nil {
			err = rc.Client().Delete(rc.Context(), job, client.PropagationPolicy(metav1.DeletePropagationBackground))
			if client.IgnoreNotFound(err) != nil {
				return flow.Error(err, "Unable to remove recover job", "job-name", job.Name)
			}
		}
		return flow.Continue("Recover job removed!")
	})

var CleanDummyBackupObject = xstorev1reconcile.NewStepBinder("CleanDummyBackupObject",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()
		xstoreBackup, err := rc.GetXStoreBackupByName(xstore.Spec.Restore.BackupSet)
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Failed to get polardbx backup", "backup set", xstoreBackup.Name)
		}
		if xstoreBackup == nil || xstoreBackup.Annotations[xstoremeta.AnnotationDummyBackup] != "true" {
			return flow.Continue("Dummy backup object not exists, just skip")
		}
		if err := rc.Client().Delete(rc.Context(), xstoreBackup); err != nil {
			return flow.Error(err, "Failed to delete dummy backup object")
		}
		return flow.Continue("Dummy backup object cleaned!")
	},
)
