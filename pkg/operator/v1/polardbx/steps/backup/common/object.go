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
	"encoding/json"
	"errors"
	"fmt"
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	xstorev1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/filestream"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/convention"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/factory"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	xstoreconvention "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	"github.com/alibaba/polardbx-operator/pkg/util/path"
	"github.com/alibaba/polardbx-operator/pkg/util/slice"
	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"math"
	"modernc.org/mathutil"
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

var PersistentPolarDBXBackup = polardbxv1reconcile.NewStepBinder("PersistentPolarDBXBackup",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		if rc.IsPolarDBXChanged() {
			if err := rc.UpdatePolarDBXBackup(); err != nil {
				return flow.Error(err, "Unable to update spec for PolarDBX backup.")
			}
			return flow.Continue("PolarDBX backup spec updated.")
		}
		return flow.Continue("PolarDBX backup spec did not change.")
	})

var PersistentStatusChanges = polardbxv1reconcile.NewStepBinder("PersistentStatusChanges",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		if rc.IsPXCBackupStatusChanged() {
			if err := rc.UpdatePolarDBXBackupStatus(); err != nil {
				return flow.Error(err, "Unable to update status for PolarDBX backup.")
			}
			return flow.Continue("PolarDBX backup status updated.")
		}
		return flow.Continue("PolarDBX backup status did not change.")
	})

var UpdateBackupStartInfo = polardbxv1reconcile.NewStepBinder("UpdateBackupStartInfo",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		backup := rc.MustGetPolarDBXBackup()

		nowTime := metav1.Now()
		backup.Status.StartTime = &nowTime
		if backup.Labels == nil {
			backup.Labels = make(map[string]string)
		}
		backup.Labels[polardbxmeta.LabelName] = backup.Spec.Cluster.Name
		backup.Status.BackupRootPath = path.NewPathFromStringSequence(
			polardbxmeta.BackupPath,
			backup.Labels[polardbxmeta.LabelName],
			fmt.Sprintf("%s-%s", backup.Name, backup.Status.StartTime.Format("20060102150405")),
		)

		// record topology of original polardbx
		polardbx, err := rc.GetPolarDBX()
		if err != nil {
			return flow.Error(err, "Unable to get original polardbx")
		}
		backup.Spec.Cluster.UID = polardbx.UID
		backup.Status.ClusterSpecSnapshot = polardbx.Spec.DeepCopy()

		// mark to update spec
		rc.MarkPolarDBXChanged()

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

			objectFactory := factory.NewObjectFactory(rc)
			xstoreBackup, err := objectFactory.NewXStoreBackup(&xstore)
			if err != nil {
				return flow.Error(err, "Unable to build new physical backup for xstore", "xstore", xstore.Name)
			}

			err = rc.SetControllerRefAndCreateToBackup(xstoreBackup)
			if err != nil {
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
		polardbxName := backup.Spec.Cluster.Name
		backupList, err := rc.GetPolarDBXBackupListByPolarDBXName(polardbxName)
		if err != nil {
			return flow.Error(err, "Failed to list backup list", "PolarDBX name", polardbxName)
		}
		for _, item := range backupList.Items {
			if slice.NotIn(item.Status.Phase, polardbxv1.BackupFailed, polardbxv1.BackupFinished,
				polardbxv1.BackupDeleting, polardbxv1.BackupDummy) {
				return flow.RetryAfter(1*time.Minute, "Backup is still underway", "backup name", item.Name)
			}
		}

		var xstoreList polardbxv1.XStoreList
		err = rc.Client().List(rc.Context(), &xstoreList, client.InNamespace(rc.Namespace()), client.MatchingLabels{
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
		if err != nil {
			return flow.Error(err, "Unable to get XStore backup list")
		}
		pxcBackup, err := rc.GetPolarDBXBackup()
		if err != nil {
			return flow.Error(err, "Unable to get PolarDBXBackup")
		}

		pxcBackup.Status.CollectStartIndexMap = make(map[string]string)
		for _, backupPod := range backupPodList {
			groupManager, err := rc.GetXstoreGroupManagerByPod(&backupPod)
			if err != nil {
				return flow.Error(err, "get datasource failed", "pod", backupPod.Name)
			}
			if groupManager == nil {
				return flow.Error(errors.New("group manager is nil"),
					"get group manager failed", "pod", backupPod.Name)
			}

			binlogOffset, err := groupManager.GetBinlogOffset()
			if err != nil {
				return flow.Error(err, "get binlog offset failed", "pod", backupPod.Name)
			}

			pxcBackup.Status.CollectStartIndexMap[backupPod.Name] = binlogOffset
		}
		return flow.Continue("Collect Binlog Start Offset!")
	})

var DrainCommittingTrans = polardbxv1reconcile.NewStepBinder("DrainCommittingTrans",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		cnManager, err := rc.GetPolarDBXGroupManager()
		if err != nil {
			return flow.Error(err, "get CN DataSource Failed")
		}
		flow.Logger().Info("Waiting trans commited")
		err = cnManager.IsTransCommited("TRX_ID", "INNODB_TRX")
		if err != nil {
			return flow.Error(err, "Drain Committing Trans Failed")
		}
		return flow.Continue("Drain Committing Trans!")
	})

var SendHeartBeat = polardbxv1reconcile.NewStepBinder("SendHeartBeat",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		backup := rc.MustGetPolarDBXBackup()
		cnManager, err := rc.GetPolarDBXGroupManager()
		if err != nil {
			return flow.Error(err, "Get CN dataSource failed")
		}

		// In case that there is no cdc in the cluster
		heartbeatTableDDL := "/*+TDDL:cmd_extra(ENABLE_ASYNC_DDL=FALSE)*/\n" +
			"CREATE TABLE IF NOT EXISTS `__cdc_heartbeat__` (\n" +
			"  `id` bigint(20) NOT NULL AUTO_INCREMENT BY GROUP,\n" +
			"  `sname` varchar(10) DEFAULT NULL,\n" +
			"  `gmt_modified` datetime(3) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`id`)\n) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 broadcast"
		err = cnManager.CreateTable("__cdc__", heartbeatTableDDL)
		if err != nil {
			return flow.Error(err, "Create heartbeat table failed: "+err.Error())
		}

		sname := strconv.FormatInt(time.Now().Unix(), 10)
		err = cnManager.SendHeartBeat(sname)
		if err != nil {
			return flow.Error(err, "Send heartBeat failed")
		}

		backup.Status.HeartBeatName = sname
		return flow.Continue("HeartBeat send!")
	})

var WaitHeartbeatSentToFollower = polardbxv1reconcile.NewStepBinder("WaitHeartbeatSentToFollower",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		backupPodList, err := rc.GetXStoreBackupPods()
		if err != nil {
			return flow.Error(err, "Unable to get backup pods")
		}
		pxcBackup, err := rc.GetPolarDBXBackup()
		if err != nil {
			return flow.Error(err, "Unable to get PolarDBXBackup")
		}

		// check whether backup happened on follower, if so, wait heartbeat sent to them
		// waitingPodMap records followers with their leader
		waitingPodMap := make(map[*corev1.Pod]*corev1.Pod)
		for i, backupPod := range backupPodList {
			if backupPod.Labels[xstoremeta.LabelRole] == xstoremeta.RoleLeader {
				continue
			}

			// get its leader
			var xstore xstorev1.XStore
			xstoreName := types.NamespacedName{
				Namespace: backupPod.Namespace,
				Name:      backupPod.Labels[xstoremeta.LabelName],
			}
			flow.Logger().Info("Try to get xstore for backup pod", "pod", backupPod.Name, "xstore", xstoreName)
			err := rc.Client().Get(rc.Context(), xstoreName, &xstore)
			if err != nil {
				return flow.Error(err, "Unable to get xstore for backup pod", "backup pod", backupPod.Name)
			}
			leaderPod, err := rc.GetLeaderOfDN(&xstore)
			if err != nil {
				return flow.Error(err, "Unable to get leader for backup pod", "backup pod", backupPod.Name)
			}
			waitingPodMap[&backupPodList[i]] = leaderPod
		}
		if len(waitingPodMap) == 0 {
			return flow.Continue("No backup happened on follower, skip waiting")
		}

		// get APPLIED_INDEX of leader, and wait APPLIED_INDEX of follower sync with it
		flow.Logger().Info("Waiting heartbeat sent to follower pods")
		timeout := time.After(120 * time.Minute)   // timeout after 120 min TODO(dengli): calculate timeout by delay
		tick := time.Tick(1 * time.Minute)         // check period
		targetIndex := make(map[*corev1.Pod]int64) // record APPLIED_INDEX of leader per follower
		for {
			select {
			case <-timeout:
				return flow.Error(errors.New("timeout"), "waiting heartbeat sync timeout")
			case <-tick:
				for waitingPod, leaderPod := range waitingPodMap {
					manager, err := rc.GetXstoreGroupManagerByPod(leaderPod)
					if err != nil || manager == nil {
						return flow.Error(err, "unable to connect to leader", "leader pod", leaderPod.Name)
					}

					clusterStatusList, err := manager.ShowClusterStatus()
					if err != nil {
						return flow.Error(err, "unable to get cluster status", "leader pod", leaderPod.Name)
					}

					// leader may have changed, just abort the backup
					if len(clusterStatusList) < 3 {
						return flow.Error(errors.New("invalid cluster status"),
							"node of xpaxos cluster is less than 3, leader may have changed",
							"leader pod", leaderPod.Name)
					}

					var currentIndex int64 = math.MaxInt64
					for _, status := range clusterStatusList {
						if status.Role == "Leader" {
							if _, ok := targetIndex[waitingPod]; !ok {
								targetIndex[waitingPod] = status.AppliedIndex
							}
						} else if status.Role == "Follower" { // to avoid checking per pod, just wait for all followers
							currentIndex = mathutil.MinInt64(currentIndex, status.AppliedIndex)
						}
					}
					if currentIndex >= targetIndex[waitingPod] {
						delete(waitingPodMap, waitingPod)
						flow.Logger().Info("Waiting finished", "waiting pod", waitingPod.Name,
							"target index", targetIndex[waitingPod], "current index", currentIndex)
						continue
					}

					// Try to record sync progress, skip if failed to update
					pxcBackup.Status.Message = fmt.Sprintf(
						"Waiting for sync of heartbeat, pod: %s, target index: %d, current index: %d",
						waitingPod.Name, targetIndex[waitingPod], currentIndex)
					flow.Logger().Info(pxcBackup.Status.Message)
					if err := rc.UpdatePolarDBXBackupStatus(); err != nil {
						flow.Logger().Error(err, "Unable to update progress of syncing heartbeat.")
					}
				}
				if len(waitingPodMap) == 0 {
					pxcBackup.Status.Message = ""
					return flow.Continue("Heartbeat has already been sent to all the followers")
				}
			}
		}
	})

var CollectBinlogEndIndex = polardbxv1reconcile.NewStepBinder("CollectBinlogEndIndex",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		backupPodList, err := rc.GetXStoreBackupPods()
		if err != nil {
			return flow.Error(err, "Unable to get XStore backup list")
		}
		pxcBackup, err := rc.GetPolarDBXBackup()
		if err != nil {
			return flow.Error(err, "Unable to get PolarDBXBackup")
		}

		pxcBackup.Status.CollectEndIndexMap = make(map[string]string)
		for _, backupPod := range backupPodList {
			groupManager, err := rc.GetXstoreGroupManagerByPod(&backupPod)
			if err != nil {
				return flow.Error(err, "get dn datasource failed", "pod:", backupPod)
			}
			if groupManager == nil {
				return flow.Error(errors.New("group manager is nil"),
					"get group manager failed", "pod", backupPod.Name)
			}

			binlogOffset, err := groupManager.GetBinlogOffset()
			if err != nil {
				return flow.Error(err, "get binlog offset failed", "pod", backupPod.Name)
			}
			pxcBackup.Status.CollectEndIndexMap[backupPod.Name] = binlogOffset
		}
		return flow.Continue("Collect Binlog End Offset!")
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
		job, err := rc.GetSeekCpJob()
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to get SeekCp job!")
		}
		if job != nil {
			return flow.Continue("SeekCp job already started!", "job-name", job.Name)
		}

		exists, err := rc.IsTaskContextExists(string(xstoreconvention.BackupJobTypeSeekcp))
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

		if err := rc.SaveTaskContext(string(xstoreconvention.BackupJobTypeSeekcp), &SeekCpJobContext{
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
		job, err := rc.GetSeekCpJob()
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to get SeekCp job!")
		}
		if job != nil {
			return flow.Continue("SeekCp job already started!", "job-name", job.Name)
		}

		seekCpJobContext := SeekCpJobContext{}
		err = rc.GetTaskContext(string(xstoreconvention.BackupJobTypeSeekcp), &seekCpJobContext)
		if err != nil {
			return flow.Error(err, "Unable to get task context for seekcp")
		}

		polardbxBackup := rc.MustGetPolarDBXBackup()
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
		job, err = newSeekCpJob(polardbxBackup, &targetPod)
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
			// wait until the job object could be found
			return flow.Wait("Seekcp job may not have been created yet, wait a while.")
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
				return flow.RetryAfter(1*time.Minute, "XstoreBackup is still performing binlog backup",
					"XstoreBackup name", xstoreBackup.Name)
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
		return flow.Continue("PolarDBX secret saved!")
	})

var UploadClusterMetadata = polardbxv1reconcile.NewStepBinder("UploadClusterMetadata",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		// collect all the metadata and formatted them into a single json file
		pxcBackup := rc.MustGetPolarDBXBackup()
		polardbx, err := rc.GetPolarDBX()
		if err != nil {
			return flow.Error(err, "Unable to get original polardbx")
		}
		metadata := factory.MetadataBackup{
			PolarDBXClusterMetadata: factory.PolarDBXClusterMetadata{
				Name: polardbx.Name,
				UID:  polardbx.UID,
				Spec: pxcBackup.Status.ClusterSpecSnapshot.DeepCopy(),
			},
			XstoreMetadataList:         make([]factory.XstoreMetadata, 0, len(pxcBackup.Status.Backups)),
			BackupSetName:              pxcBackup.Name,
			BackupRootPath:             pxcBackup.Status.BackupRootPath,
			StartTime:                  pxcBackup.Status.StartTime,
			EndTime:                    pxcBackup.Status.EndTime,
			LatestRecoverableTimestamp: pxcBackup.Status.LatestRecoverableTimestamp,
		}

		// check and record current serviceType according to service
		service, err := rc.GetPolarDBXService(convention.ServiceTypeReadWrite)
		if err != nil {
			return flow.Error(err, "Unable to get polardbx service")
		}
		metadata.PolarDBXClusterMetadata.Spec.ServiceType = service.Spec.Type

		// xstore metadata and secrets
		pxcSecret, err := rc.GetSecret(pxcBackup.Name)
		if err != nil || pxcSecret == nil {
			return flow.Error(err, "Unable to get secret for pxc", "pxc name", pxcBackup.Name)
		}
		metadata.PolarDBXClusterMetadata.Secrets = make([]polardbxv1polardbx.PrivilegeItem, 0, len(pxcSecret.Data))
		for user, passwd := range pxcSecret.Data {
			metadata.PolarDBXClusterMetadata.Secrets = append(
				metadata.PolarDBXClusterMetadata.Secrets,
				polardbxv1polardbx.PrivilegeItem{
					Username: user,
					Password: string(passwd),
				})
		}
		for xstoreName, xstoreBackupName := range pxcBackup.Status.Backups {
			var xstore xstorev1.XStore
			err := rc.Client().Get(rc.Context(), types.NamespacedName{Namespace: rc.Namespace(), Name: xstoreName}, &xstore)
			if err != nil {
				return flow.Error(err, "Unable to get xstore by name", "xstore name", xstoreName)
			}
			xstoreSecret, err := rc.GetSecret(xstoreBackupName)
			if client.IgnoreNotFound(err) != nil {
				return flow.Error(err, "Unable to get secret for xstore", "xstore name", xstoreName)
			} else if xstoreSecret == nil {
				return flow.RetryAfter(5*time.Second, "Wait for the creation of xstore secret bacup",
					"xstore name", xstoreName)
			}
			xstoreBackup, err := rc.GetXstoreBackupByName(xstoreBackupName)
			if err != nil || xstoreBackup == nil {
				return flow.Error(err, "Unable to get backup for xstore", "xstore name", xstoreName)
			}

			xstoreMetadata := factory.XstoreMetadata{
				Name:            xstoreName,
				UID:             xstore.UID,
				BackupName:      xstoreBackupName,
				LastCommitIndex: xstoreBackup.Status.CommitIndex,
				Secrets:         make([]polardbxv1polardbx.PrivilegeItem, 0, len(xstoreSecret.Data)),
				TargetPod:       xstoreBackup.Status.TargetPod,
			}
			for user, passwd := range xstoreSecret.Data {
				xstoreMetadata.Secrets = append(
					xstoreMetadata.Secrets,
					polardbxv1polardbx.PrivilegeItem{
						Username: user,
						Password: string(passwd),
					})
			}
			metadata.XstoreMetadataList = append(metadata.XstoreMetadataList, xstoreMetadata)
		}

		// parse metadata to json slice
		jsonString, err := json.Marshal(metadata)
		if err != nil {
			return flow.RetryErr(err, "Failed to marshal metadata, retry to upload metadata")
		}

		// init filestream client and upload formatted metadata
		filestreamClient, err := rc.GetFilestreamClient()
		metadataBackupPath := fmt.Sprintf("%s/metadata", metadata.BackupRootPath)
		if err != nil {
			return flow.RetryAfter(10*time.Second, "Failed to get filestream client, error: "+err.Error())
		}
		filestreamAction, err := polardbxv1polardbx.NewBackupStorageFilestreamAction(pxcBackup.Spec.StorageProvider.StorageName)
		if err != nil {
			return flow.RetryAfter(10*time.Second, "Unsupported storage provided")
		}
		actionMetadata := filestream.ActionMetadata{
			Action:    filestreamAction.Upload,
			Sink:      pxcBackup.Spec.StorageProvider.Sink,
			RequestId: uuid.New().String(),
			Filename:  metadataBackupPath,
		}
		sendBytes, err := filestreamClient.Upload(bytes.NewReader(jsonString), actionMetadata)
		if err != nil {
			return flow.RetryAfter(10*time.Second, "Upload metadata failed, error: "+err.Error())
		}
		flow.Logger().Info("Uploading metadata finished", "sent bytes", sendBytes)
		return flow.Continue("Metadata uploaded.")
	})
