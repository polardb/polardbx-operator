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

package common

import (
	"bytes"
	"fmt"
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	xstorev1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/debug"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	backupbuilder "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/steps/backup/xstorejobbuilder"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/command"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	"github.com/alibaba/polardbx-operator/pkg/util"
	xstorectrlerrors "github.com/alibaba/polardbx-operator/pkg/util/error"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"strconv"
	"strings"
	"time"
)

type SeekCpJobContext struct {
	RemoteCpPath string `json:"remoteCpPath,omitempty"`
	TxEventsDir  string `json:"txEventsDir,omitempty"`
	IndexesPath  string `json:"indexesPath,omitempty"`
	DnNameList   string `json:"dnNameList,omitempty"`
	StorageName  string `json:"storageName,omitempty"`
	Sink         string `json:"sink,omitempty"`
}

var UpdateBackupStartInfo = polardbxv1reconcile.NewStepBinder("UpdateBackupStartInfo",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		backup := rc.MustGetPolarDBXBackup()

		nowTime := metav1.Now()
		backup.Status.StartTime = &nowTime
		if backup.Labels == nil {
			backup.Labels = make(map[string]string)
		}
		backup.Labels[polardbxmeta.LabelName] = backup.Spec.Cluster.Name
		backup.Status.BackupRootPath = util.BackupRootPath(backup)
		if err := rc.UpdatePolarDBXBackup(); err != nil {
			return flow.Error(err, "Unable to update PXC backup.")
		}

		return flow.Continue("Update backup start info")
	})

var CreateBackupJobsForXStore = polardbxv1reconcile.NewStepBinder("CreateBackupsForDNAndGMS",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		backup := rc.MustGetPolarDBXBackup()
		// List current existing backups and refill the map (no matter what it
		// records before, clear it)
		var xstoreBackups polardbxv1.XStoreBackupList
		err := rc.Client().List(rc.Context(), &xstoreBackups, client.InNamespace(rc.Namespace()), client.MatchingLabels{
			polardbxmeta.LabelName:      backup.Spec.Cluster.Name,
			polardbxmeta.LabelTopBackup: backup.Name,
		})
		if err != nil {
			return flow.Error(err, "Unable to list xstore backup")
		}
		backup.Status.Backups = make(map[string]string)
		backup.Status.XStores = make([]string, 0)
		for _, xstoreBackup := range xstoreBackups.Items {
			backup.Status.Backups[xstoreBackup.Spec.XStore.Name] = xstoreBackup.Name
			backup.Status.XStores = append(backup.Status.XStores, xstoreBackup.Spec.XStore.Name)
		}

		//list each DN and GMS
		var xstoreList polardbxv1.XStoreList
		err = rc.Client().List(rc.Context(), &xstoreList, client.InNamespace(rc.Namespace()), client.MatchingLabels{
			polardbxmeta.LabelName: backup.Spec.Cluster.Name,
		})
		if err != nil {
			return flow.Error(err, "Unable to list xstore List")
		}

		// For each DN and GMS not having a backup, create a backup.
		for _, xstore := range xstoreList.Items {
			if _, ok := backup.Status.Backups[xstore.Name]; ok {
				continue
			}

			xstoreBackup, err := backupbuilder.NewXStoreBackup(rc.Scheme(), backup, &xstore)
			if err != nil {
				return flow.Error(err, "Unable to build new physical backup for xstore", "xstore", xstore.Name)
			}

			if err = rc.Client().Create(rc.Context(), xstoreBackup); err != nil {
				return flow.Error(err, "Unable to create physical backup for xstore", "xstore", xstore.Name)
			}
			backup.Status.XStores = append(backup.Status.XStores, xstoreBackup.Spec.XStore.Name)
			backup.Status.Backups[xstore.Name] = xstoreBackup.Name
		}
		return flow.Continue("Create backups for dn and gms")
	})

var WaitAllBackupJobsFinished = polardbxv1reconcile.NewStepBinder("WaitAllBackupJobsFinished",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		backup := rc.MustGetPolarDBXBackup()
		var xstoreBackups polardbxv1.XStoreBackupList
		err := rc.Client().List(rc.Context(), &xstoreBackups, client.InNamespace(rc.Namespace()), client.MatchingLabels{
			polardbxmeta.LabelName:      backup.Spec.Cluster.Name,
			polardbxmeta.LabelTopBackup: backup.Name,
		})
		if err != nil {
			return flow.Error(err, "Unable to list xstore backup")
		}

		//Check backups.
		if len(xstoreBackups.Items) < len(backup.Status.Backups) {
			flow.Logger().Info("Backup Failed", "expect-size:", len(backup.Status.Backups), "actual-size", len(xstoreBackups.Items))
			backup.Status.Phase = polardbxv1.BackupFailed
			backup.Status.Phase = "Backup broken detected"
			return flow.Continue("Backup Failed")
		}

		for _, xstoreBackup := range xstoreBackups.Items {
			if xstoreBackup.Status.Phase != polardbxv1.XStoreBackupCollecting {
				return flow.Wait("XStore backup is still collecting!", "xstore-name", xstoreBackup.Name)
			}
		}

		flow.Logger().Info("XStore Backup completed!", "pxcBackup-name", backup.Name)
		return flow.Continue("Create backups for dn and gms")
	})

var DeleteBackupJobsOnFailure = polardbxv1reconcile.NewStepBinder("DeleteBackupJobs",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		backup := rc.MustGetPolarDBXBackup()

		var xstoreBackups polardbxv1.XStoreBackupList
		err := rc.Client().List(rc.Context(), &xstoreBackups, client.InNamespace(rc.Namespace()), client.MatchingLabels{
			polardbxmeta.LabelName:      backup.Spec.Cluster.Name,
			polardbxmeta.LabelTopBackup: backup.Name,
		})
		if err != nil {
			return flow.Error(err, "Unable to list xstore backup")
		}

		for _, xstoreBackup := range xstoreBackups.Items {
			if err = rc.Client().Delete(rc.Context(), &xstoreBackup); err != nil {
				if apierrors.IsNotFound(err) {
					continue
				}
				return flow.Error(err, "Unable to delete xstore backup", "xstore", xstoreBackup.Spec.XStore.Name, "physical-backup", xstoreBackup.Name)
			}
		}
		if backup.Spec.CleanPolicy == polardbxv1.CleanPolicyOnFailure {
			flow.Logger().Info("Delete the failed backup!")
			if err := rc.Client().Delete(rc.Context(), backup); err != nil {
				if apierrors.IsNotFound(err) {
					flow.Logger().Info("Already deleted!")
				} else {
					return flow.Error(err, "Unable to delete the failed backup")
				}
			}
		}
		return flow.Continue("delete backup jobs")
	})

var PersistentStatusChanges = polardbxv1reconcile.NewStepBinder("PersistentStatusChanges",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		if debug.IsDebugEnabled() {
			if err := rc.UpdatePolarDBXBackupStatus(); err != nil {
				return flow.Error(err, "Unable to update status for xstore backup.")
			}
			return flow.Continue("PXC Backup status updated!")
		}
		if rc.IsPXCBackupStatusChanged() {
			if err := rc.UpdatePolarDBXBackupStatus(); err != nil {
				return flow.Error(err, "Unable to update status for xstore backup.")
			}
			return flow.Continue("PXC Backup status updated!")
		}
		return flow.Continue("PXC Backup status not changed!")
	})

var LockXStoreBinlogPurge = polardbxv1reconcile.NewStepBinder("LockBinlogPurge",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		backup := rc.MustGetPolarDBXBackup()
		var xstoreList polardbxv1.XStoreList
		err := rc.Client().List(rc.Context(), &xstoreList, client.InNamespace(rc.Namespace()), client.MatchingLabels{
			polardbxmeta.LabelName: backup.Spec.Cluster.Name,
		})
		if err != nil {
			return flow.Error(err, "Unable to get XStore list")
		}
		for _, xstore := range xstoreList.Items {
			xstore.Labels[xstoremeta.LabelBinlogPurgeLock] = xstoremeta.BinlogPurgeLock
			err = rc.Client().Update(rc.Context(), &xstore)
			if err != nil {
				return flow.Error(err, "Update xstore to binlog PurgeLocked Failed!")
			}
		}
		return flow.Continue("Binlog purge locked!")
	})

var UnLockXStoreBinlogPurge = polardbxv1reconcile.NewStepBinder("UnLockBinlogPurge",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		backup := rc.MustGetPolarDBXBackup()
		var xstoreList polardbxv1.XStoreList
		err := rc.Client().List(rc.Context(), &xstoreList, client.InNamespace(rc.Namespace()), client.MatchingLabels{
			polardbxmeta.LabelName: backup.Spec.Cluster.Name,
		})
		if err != nil {
			return flow.Error(err, "Unable to get XStore list")
		}
		for _, xstore := range xstoreList.Items {
			xstore.Labels[xstoremeta.LabelBinlogPurgeLock] = xstoremeta.BinlogPurgeUnlock
			err = rc.Client().Update(rc.Context(), &xstore)
			if err != nil {
				return flow.Error(err, "Update xstore to binlog PurgeLocked Failed!")
			}
		}
		return flow.Continue("Binlog purge unlocked!")
	})

var CollectBinlogStartIndex = polardbxv1reconcile.NewStepBinder("CollectBinlogStartIndex",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		backupPodList, err := rc.GetXStoreBackupPods()
		pxcBackup, err := rc.GetPolarDBXBackup()
		if err != nil {
			return flow.Error(err, "Unable to get XStore list")
		}
		for _, backupPod := range backupPodList {
			groupManager, xstore, err := rc.GetPolarDBXGroupManagerByXStore(backupPod)
			if err != nil {
				return flow.Error(err, "get DataSource Failed")
			}

			binlogOffset, err := groupManager.GetBinlogOffset()
			if err != nil {
				return flow.Error(err, "get binlogoffset Failed")
			}

			err = rc.Close()
			if err != nil {
				return flow.Error(err, "Close Database Failed")
			}

			binlogOffset = fmt.Sprintf("%s\ntimestamp:%s", binlogOffset, time.Now().Format("2006-01-02 15:04:05"))
			backupRootPath := pxcBackup.Status.BackupRootPath
			remotePath := fmt.Sprintf("%s/%s/%s-start", backupRootPath, polardbxmeta.BinlogOffsetPath, xstore.Name)
			command := command.NewCanonicalCommandBuilder().Collect().
				UploadOffset(binlogOffset, remotePath, string(pxcBackup.Spec.StorageProvider.StorageName), pxcBackup.Spec.StorageProvider.Sink).Build()
			stdout := &bytes.Buffer{}
			stderr := &bytes.Buffer{}
			err = rc.ExecuteCommandOn(&backupPod, "engine", command, control.ExecOptions{
				Logger:  flow.Logger(),
				Stdin:   nil,
				Stdout:  stdout,
				Stderr:  stderr,
				Timeout: 1 * time.Minute,
			})
			if err != nil {
				if ee, ok := xstorectrlerrors.ExitError(err); ok {
					if ee.ExitStatus() != 0 {
						return flow.Retry("Failed to upload binlog start index", "pod", backupPod.Name, "exit-status", ee.ExitStatus())
					}
				}
				return flow.Error(err, "Failed to upload binlog start index", "pod", backupPod.Name, "stdout", stdout.String(), "stderr", stderr.String())
			}
		}

		return flow.Continue("Collect Binlog Start Offset!")
	})

var CollectBinlogEndIndex = polardbxv1reconcile.NewStepBinder("CollectBinlogEndIndex",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		backupPodList, err := rc.GetXStoreBackupPods()
		pxcBackup, err := rc.GetPolarDBXBackup()
		if err != nil {
			return flow.Error(err, "Unable to get XStore list")
		}
		for _, backupPod := range backupPodList {
			groupManager, xstore, err := rc.GetPolarDBXGroupManagerByXStore(backupPod)
			if err != nil {
				return flow.Error(err, "get DN DataSource Failed", "podName:", backupPod)
			}
			binlogOffset, err := groupManager.GetBinlogOffset()
			if err != nil {
				return flow.Error(err, "get binlogoffset Failed")
			}
			err = rc.Close()
			if err != nil {
				return flow.Error(err, "Close Database Failed")
			}

			binlogOffset = fmt.Sprintf("%s\ntimestamp:%s", binlogOffset, time.Now().Format("2006-01-02 15:04:05"))
			backupRootPath := pxcBackup.Status.BackupRootPath
			remotePath := fmt.Sprintf("%s/%s/%s-end", backupRootPath, polardbxmeta.BinlogOffsetPath, xstore.Name)
			command := command.NewCanonicalCommandBuilder().Collect().
				UploadOffset(binlogOffset, remotePath, string(pxcBackup.Spec.StorageProvider.StorageName), pxcBackup.Spec.StorageProvider.Sink).Build()
			stdout := &bytes.Buffer{}
			stderr := &bytes.Buffer{}
			err = rc.ExecuteCommandOn(&backupPod, "engine", command, control.ExecOptions{
				Logger:  flow.Logger(),
				Stdin:   nil,
				Stdout:  stdout,
				Stderr:  stderr,
				Timeout: 1 * time.Minute,
			})
			if err != nil {
				if ee, ok := xstorectrlerrors.ExitError(err); ok {
					if ee.ExitStatus() != 0 {
						return flow.Retry("Failed to upload binlog end index", "pod", backupPod.Name, "exit-status", ee.ExitStatus())
					}
				}
				return flow.Error(err, "Failed to upload binlog end index", "pod", backupPod.Name, "stdout", stdout.String(), "stderr", stderr.String())
			}
		}
		return flow.Continue("Collect Binlog End Offset!")
	})

var DrainCommittingTrans = polardbxv1reconcile.NewStepBinder("DrainCommittingTrans",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		backup := rc.MustGetPolarDBXBackup()

		cnManager, err := rc.GetPolarDBXCNGroupManager(backup)
		defer rc.Close()
		if err != nil {
			return flow.Error(err, "get CN DataSource Failed")
		}
		err = cnManager.IsTransCommited("TRX_ID", "INNODB_TRX")
		if err != nil {
			return flow.Error(err, "Drain Committing Trans Failed")
		}
		return flow.Continue("Drain Committing Trans!")
	})

var SendHeartBeat = polardbxv1reconcile.NewStepBinder("SendHeartBeat",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		backup := rc.MustGetPolarDBXBackup()

		cnManager, err := rc.GetPolarDBXCNGroupManager(backup)
		defer rc.Close()
		if err != nil {
			return flow.Error(err, "get CN DataSource Failed")
		}

		// In case that there is no cdc in the cluster
		heartbeatTableDDL := "/*+TDDL:cmd_extra(ENABLE_ASYNC_DDL=FALSE)*/\n" +
			"CREATE TABLE IF NOT EXISTS `__cdc_heartbeat__` (\n" +
			"  `id` bigint(20) NOT NULL AUTO_INCREMENT BY GROUP,\n" +
			"  `sname` varchar(10) DEFAULT NULL,\n" +
			"  `gmt_modified` datetime(3) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`id`)\n) ENGINE = InnoDB AUTO_INCREMENT = 1666172319 DEFAULT CHARSET = utf8mb4 DEFAULT COLLATE = utf8mb4_0900_ai_ci  broadcast"
		err = cnManager.CreateTable("__cdc__", heartbeatTableDDL)
		if err != nil {
			return flow.Error(err, "Create Heartbeat table failed: "+err.Error())
		}

		sname := strconv.FormatInt(time.Now().Unix(), 10)
		err = cnManager.SendHeartBeat(sname)
		if err != nil {
			return flow.Error(err, "Send HeartBeat Failed")
		}

		backup.Status.HeartBeatName = sname
		return flow.Continue("HeartBeat Send!")
	})

var WaitAllCollectBinlogJobFinished = polardbxv1reconcile.NewStepBinder("WaitAllCollectBinlogJobFinished",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		backup := rc.MustGetPolarDBXBackup()
		xstoreBackupList := polardbxv1.XStoreBackupList{}
		err := rc.Client().List(rc.Context(), &xstoreBackupList, client.InNamespace(backup.Namespace), client.MatchingLabels{
			polardbxmeta.LabelTopBackup: backup.Name,
		})
		if err != nil {
			return flow.Error(err, "Unable to get  xstoreList!")
		}
		for _, xstoreBackup := range xstoreBackupList.Items {
			if xstoreBackup.Status.Phase != xstorev1.XStoreBinlogBackuping {
				return flow.Wait("xstorebackup is still collecting binlog", "xstoreBackupName", xstoreBackup.Name)
			}
		}
		return flow.Continue("All xstorebackups have collected binlog")

	})

var PrepareSeekCpJobContext = polardbxv1reconcile.NewStepBinder("PrepareSeekCpJobContext",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		const seekcpJobKey = "seekcp"
		exists, err := rc.IsTaskContextExists(seekcpJobKey)
		if err != nil {
			return flow.Error(err, "Unable to determine job context for restore!")
		}
		if exists {
			return flow.Pass()
		}
		polardbxBackup := rc.MustGetPolarDBXBackup()

		backupRootPath := polardbxBackup.Status.BackupRootPath
		remoteCpPath := fmt.Sprintf("%s/%s/%s",
			backupRootPath, polardbxmeta.BinlogOffsetPath, polardbxmeta.SeekCpName)
		txEventsDir := fmt.Sprintf("%s/%s", backupRootPath, polardbxmeta.CollectBinlogPath)
		indexesPath := fmt.Sprintf("%s/%s", backupRootPath, polardbxmeta.BinlogIndexesName)

		if err := rc.SaveTaskContext(seekcpJobKey, &SeekCpJobContext{
			RemoteCpPath: remoteCpPath,
			TxEventsDir:  txEventsDir,
			IndexesPath:  indexesPath,
			DnNameList:   strings.Join(polardbxBackup.Status.XStores, ","),
			StorageName:  string(polardbxBackup.Spec.StorageProvider.StorageName),
			Sink:         polardbxBackup.Spec.StorageProvider.Sink,
		}); err != nil {
			return flow.Error(err, "Unable to save job context for seekcp!")
		}
		return flow.Continue("Job context for seekcp prepared!")
	})

var CreateSeekCpJob = polardbxv1reconcile.NewStepBinder("CreateSeekCpJob",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		const seekcpJobKey = "seekcp"
		seekCpJobContext := SeekCpJobContext{}
		err := rc.GetTaskContext(seekcpJobKey, &seekCpJobContext)
		if err != nil {
			return flow.Error(err, "Unable to get task context for seekcp")
		}

		polardbxBackup := rc.MustGetPolarDBXBackup()

		job, err := rc.GetSeekCpJob()
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to get SeekCp job!")
		}
		if job != nil {
			return flow.Continue("SeekCp job already started!", "job-name", job.Name)
		}

		jobName := GenerateJobName(polardbxBackup, "seekcp")
		xstoreBackupList, err := rc.GetXStoreBackups()
		if err != nil {
			return flow.Error(err, "Unable to get XStoreBackupList!")
		}
		var targetPod corev1.Pod
		for _, xstoreBackup := range xstoreBackupList.Items {
			if len(xstoreBackup.Status.TargetPod) > 0 {
				targetPodName := types.NamespacedName{Namespace: rc.Namespace(), Name: xstoreBackup.Status.TargetPod}
				err := rc.Client().Get(rc.Context(), targetPodName, &targetPod)
				if targetPod.Labels[polardbxmeta.LabelRole] == polardbxmeta.RoleGMS {
					// do not pick gms to avoid download issue of heartbeat
					continue
				}
				if err == nil {
					break
				}
			}
		}
		if len(targetPod.Name) == 0 {
			return flow.Error(err, "Unable to get targetPod!")
		}
		job, err = newSeekCpJob(polardbxBackup, &targetPod, jobName)
		if err != nil {
			return flow.Error(err, "Unable to create SeekCpJob")
		}
		if err = rc.SetControllerRefAndCreateToBackup(job); err != nil {
			return flow.Error(err, "Unable to create job to initialize data")
		}
		return flow.Continue("Create SeekCp Job!", "targetPod", targetPod.Name)
	})

var WaitUntilSeekCpJobFinished = polardbxv1reconcile.NewStepBinder("WaitUtilSeekCpJobFinished",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		job, err := rc.GetSeekCpJob()
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to get Seekcp binlog job!")
		}
		if job == nil {
			return flow.Continue("Seekcp binlog job removed!")
		}

		if !k8shelper.IsJobCompleted(job) {
			return flow.Wait("Seekcp binlog is still running!", "job-name", job.Name)
		}

		flow.Logger().Info("Seekcp binlog job completed!", "job-name", job.Name)

		return flow.Continue("SeekCp Job Finished!")
	})

var WaitAllBinlogJobFinished = polardbxv1reconcile.NewStepBinder("WaitAllBinlogJobFinished",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		backup := rc.MustGetPolarDBXBackup()
		xstoreBackupList := polardbxv1.XStoreBackupList{}
		err := rc.Client().List(rc.Context(), &xstoreBackupList, client.InNamespace(backup.Namespace), client.MatchingLabels{
			polardbxmeta.LabelTopBackup: backup.Name,
		})
		if err != nil {
			return flow.Error(err, "Unable to get  xstoreList!")
		}

		for _, xstoreBackup := range xstoreBackupList.Items {
			if xstoreBackup.Status.Phase != xstorev1.XStoreBinlogWaiting {
				flow.Wait("xstorebackup is still backup binlog", "xstoreBackupName", xstoreBackup.Name)
			}

		}

		// record backup set timestamp per xstore
		if backup.Status.BackupSetTimestamp == nil {
			backup.Status.BackupSetTimestamp = make(map[string]*metav1.Time)
		}
		for _, xstoreBackup := range xstoreBackupList.Items {
			backup.Status.BackupSetTimestamp[xstoreBackup.Spec.XStore.Name] = xstoreBackup.Status.BackupSetTimestamp
			if backup.Status.LatestRecoverableTimestamp == nil ||
				xstoreBackup.Status.BackupSetTimestamp.Unix() > backup.Status.LatestRecoverableTimestamp.Unix() {
				backup.Status.LatestRecoverableTimestamp = xstoreBackup.Status.BackupSetTimestamp
			}
		}

		nowTime := metav1.Now()
		backup.Status.EndTime = &nowTime
		return flow.Continue("All xstorebackups have backup binlog")
	})

var RemoveSeekCpJob = polardbxv1reconcile.NewStepBinder("RemoveSeekCpJob",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		job, err := rc.GetSeekCpJob()
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to get seekcp job!")
		}
		if job == nil {
			return flow.Continue("SeekCp already job removed!")
		}

		err = rc.Client().Delete(rc.Context(), job, client.PropagationPolicy(metav1.DeletePropagationBackground))
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to remove seekcp job", "job-name", job.Name)
		}

		return flow.Continue("SeekCp job removed!", "job-name", job.Name)
	})

var RemoveBackupOverRetention = polardbxv1reconcile.NewStepBinder("RemoveBackupOverRetention",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		backup := rc.MustGetPolarDBXBackup()
		if backup.Spec.RetentionTime.Duration.Seconds() > 0 {
			toCleanTime := backup.Status.EndTime.Add(backup.Spec.RetentionTime.Duration)
			now := time.Now()
			if now.After(toCleanTime) {
				flow.Logger().Info("Ready to delete the backup!")
				if err := rc.Client().Delete(rc.Context(), backup); err != nil {
					if apierrors.IsNotFound(err) {
						flow.Logger().Info("Already deleted!")
					} else {
						return flow.Error(err, "Unable to delete the backup!")
					}
				}
			} else {
				waitDuration := toCleanTime.Sub(now)
				return flow.RetryAfter(waitDuration, "Not to delete backup now!")
			}
		} else {
			flow.Logger().Info("Ready to delete the backup!")
			if err := rc.Client().Delete(rc.Context(), backup); err != nil {
				if apierrors.IsNotFound(err) {
					flow.Logger().Info("Already deleted!")
				} else {
					return flow.Error(err, "Unable to delete the backup!")
				}
			}
		}
		return flow.Continue("PolarDBX backup deleted!", "PolarDBXBackup-name", backup.Name)
	})

var SavePXCSecrets = polardbxv1reconcile.NewStepBinder("SavePXCSecrets",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		backup := rc.MustGetPolarDBXBackup()
		backupSecret, err := rc.GetSecret(backup.Name)
		if backupSecret != nil {
			return flow.Continue("Already have backup secret")
		}

		secret, err := rc.GetSecret(backup.Spec.Cluster.Name)
		if err != nil {
			return flow.Error(err, "Unable to get secret for pxc", "pxc_name", backup.Spec.Cluster.Name)
		}
		backupSecret, err = rc.NewSecretFromPolarDBX(secret)
		if err != nil {
			return flow.Error(err, "Unable to new account secret while backuping")
		}
		err = rc.SetControllerRefAndCreateToBackup(backupSecret)
		if err != nil {
			return flow.Error(err, "Unable to create account secret while backuping")
		}
		return flow.Continue("PXC Secrets Saved!")
	})
