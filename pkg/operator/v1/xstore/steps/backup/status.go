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

package backup

import (
	"bytes"
	"errors"
	"fmt"
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	xstorev1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/debug"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	xstoreconvention "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	xstorectrlerrors "github.com/alibaba/polardbx-operator/pkg/util/error"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"strconv"
	"time"
)

type BackupJobContext struct {
	BinlogBackupDir     string `json:"binlogBackupDir,omitempty"`
	IndexesPath         string `json:"indexesPath,omitempty"`
	BinlogEndOffsetPath string `json:"binlogEndOffsetPath,omitempty"`
	FullBackupPath      string `json:"fullBackupPath,omitempty"`
	CollectFilePath     string `json:"collectFilePath,omitempty"`
	OffsetFileName      string `json:"offsetFileName,omitempty"`
	StorageName         string `json:"storageName,omitempty"`
	Sink                string `json:"sink,omitempty"`
}

func UpdatePhaseTemplate(phase xstorev1.XStoreBackupPhase, requeue ...bool) control.BindFunc {
	return NewStepBinder("UpdatePhaseTo"+string(phase),
		func(rc *xstorev1reconcile.BackupContext, flow control.Flow) (reconcile.Result, error) {
			xstoreBackup := rc.MustGetXStoreBackup()

			xstoreBackup.Status.Phase = phase
			return flow.Continue(" Phase xstore backup updated!", "phase-new", phase)
		})
}

var PersistentStatusChanges = NewStepBinder("PersistentStatusChanges",
	func(rc *xstorev1reconcile.BackupContext, flow control.Flow) (reconcile.Result, error) {
		if debug.IsDebugEnabled() {
			xstoreBackup := rc.MustGetXStoreBackup()
			err := rc.Client().Status().Update(rc.Context(), xstoreBackup)
			if err != nil {
				return flow.Error(err, "Unable to update status for")
			}
			return flow.Continue("Backup status updated!")
		}
		if rc.IsXStoreBackupStatusChanged() {
			if err := rc.UpdateXStoreBackupStatus(); err != nil {
				return flow.Error(err, "Unable to update status for xstore backup.")
			}
			return flow.Continue("Backup status updated!")
		}
		return flow.Continue("Backup status not changed!")
	})

var UpdateBackupStartInfo = NewStepBinder("UpdateBackupStartInfo",
	func(rc *xstorev1reconcile.BackupContext, flow control.Flow) (reconcile.Result, error) {
		xstoreBackup := rc.MustGetXStoreBackup()

		if xstoreBackup.Status.StartTime == nil {
			nowTime := metav1.Now()
			xstoreBackup.Status.StartTime = &nowTime
		}
		if xstoreBackup.Labels == nil {
			xstoreBackup.Labels = make(map[string]string)
			xstoreBackup.Labels[xstoremeta.LabelName] = xstoreBackup.Spec.XStore.Name
		}
		pxcBackup, err := rc.GetPolarDBXBackup()
		if err != nil {
			return flow.Error(err, "Unable to get pxc backup")
		}
		if pxcBackup.Status.BackupRootPath == "" { // In case that pxc backup status has not been updated
			return flow.RetryAfter(5*time.Second,
				"Status of pxc backup has not been updated, wait for 5 seconds and retry")
		}
		xstoreBackup.Status.BackupRootPath = pxcBackup.Status.BackupRootPath
		if err := rc.UpdateXStoreBackup(); err != nil {
			return flow.Error(err, "Unable to update xstore backup.")
		}
		return flow.Continue("Update backup start info!")
	})

var CreateBackupConfigMap = NewStepBinder("CreateBackupConfigMap",
	func(rc *xstorev1reconcile.BackupContext, flow control.Flow) (reconcile.Result, error) {
		const backupJobkey = "backup"
		exists, err := rc.IsTaskContextExists(backupJobkey)
		if err != nil {
			return flow.Error(err, "Unable to get task context for backup")
		}
		if exists {
			return flow.Pass()
		}

		backup := rc.MustGetXStoreBackup()
		backupRootPath := backup.Status.BackupRootPath
		fullBackupPath := fmt.Sprintf("%s/%s/%s.xbstream",
			backupRootPath, polardbxmeta.FullBackupPath, backup.Spec.XStore.Name)
		binlogEndOffsetPath := fmt.Sprintf("%s/%s/%s-end",
			backupRootPath, polardbxmeta.BinlogOffsetPath, backup.Spec.XStore.Name)
		indexesPath := fmt.Sprintf("%s/%s", backupRootPath, polardbxmeta.BinlogIndexesName)
		binlogBackupDir := fmt.Sprintf("%s/%s/%s",
			backupRootPath, polardbxmeta.BinlogBackupPath, backup.Spec.XStore.Name)
		collectFilePath := fmt.Sprintf("%s/%s/%s.evs",
			backupRootPath, polardbxmeta.CollectBinlogPath, backup.Spec.XStore.Name)
		offsetFileName := fmt.Sprintf("%s/%s/%s",
			backupRootPath, polardbxmeta.BinlogOffsetPath, backup.Spec.XStore.Name)

		if err := rc.SaveTaskContext(backupJobkey, &BackupJobContext{
			BinlogBackupDir:     binlogBackupDir,
			IndexesPath:         indexesPath,
			BinlogEndOffsetPath: binlogEndOffsetPath,
			FullBackupPath:      fullBackupPath,
			CollectFilePath:     collectFilePath,
			OffsetFileName:      offsetFileName,
			StorageName:         string(backup.Spec.StorageProvider.StorageName),
			Sink:                backup.Spec.StorageProvider.Sink,
		}); err != nil {
			return flow.Error(err, "Unable to save job context for backup!")
		}
		return flow.Continue("Job context for backup prepared!")
	})

var StartXStoreFullBackupJob = NewStepBinder("StartXStoreFullBackupJob",
	func(rc *xstorev1reconcile.BackupContext, flow control.Flow) (reconcile.Result, error) {
		const backupJobKey = "backup"
		backupJobContext := &BackupJobContext{}
		err := rc.GetTaskContext(backupJobKey, &backupJobContext)
		if err != nil {
			return flow.Error(err, "Unable to get task context for backup")
		}

		// retry until target pod found, ops allowed here
		xstoreBackup := rc.MustGetXStoreBackup()
		targetPod, err := rc.GetXStoreTargetPod()
		if err != nil {
			return flow.RetryAfter(5*time.Second, "Unable to find target pod, error: "+err.Error())
		}
		if targetPod == nil {
			return flow.RetryAfter(5*time.Second, "Unable to find target pod, error: target pod status abnormal")
		}

		if targetPod.Labels[xstoremeta.LabelRole] == xstoremeta.RoleLeader { // warning when backup on leader pod
			flow.Logger().Info("Warning: performing backup on leader", "leader pod", targetPod.Name)
		}

		job, err := rc.GetXStoreBackupJob()
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to get full backup job!")
		}
		if job != nil {
			return flow.Continue("Full Backup job already started!", "job-name", job.Name)
		}

		jobName := xstoreconvention.NewBackupJobName(targetPod, xstoreconvention.BackupJobTypeFullBackup)
		xstoreBackup.Status.TargetPod = targetPod.Name

		job, e := newBackupJob(xstoreBackup, targetPod, jobName)
		if e != nil {
			return flow.Error(err, "Unable to newFullBackupJob")
		}

		if err := rc.SetControllerRefAndCreate(job); err != nil {
			return flow.Error(err, "Unable to create job to initialize data")
		}

		return flow.Continue("Full Backup job started!", "job-name", jobName)
	})

var WaitFullBackupJobFinished = NewStepBinder("WaitFullBackupJobFinished",
	func(rc *xstorev1reconcile.BackupContext, flow control.Flow) (reconcile.Result, error) {
		xstoreBackup := rc.MustGetXStoreBackup()

		job, err := rc.GetXStoreBackupJob()
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to get full backup job!")
		}
		if job == nil {
			return flow.Continue("Full Backup job removed!")
		}

		if !k8shelper.IsJobCompleted(job) {
			return flow.Wait("Full Backup job is still running!", "job-name", job.Name)
		}

		flow.Logger().Info("Full Backup job completed!", "job-name", job.Name)

		targetPod, err := rc.GetXStoreTargetPod()
		if err != nil {
			flow.Error(err, "Unable to get targetPod")
		}
		command := []string{"cat", "/data/mysql/tmp/" + job.Name + ".idx"}
		stdout := &bytes.Buffer{}
		stderr := &bytes.Buffer{}

		err = rc.ExecuteCommandOn(targetPod, "engine", command, control.ExecOptions{
			Logger: flow.Logger(),
			Stdin:  nil,
			Stdout: stdout,
			Stderr: stderr,
		})
		if err != nil {
			if ee, ok := xstorectrlerrors.ExitError(err); ok {
				if ee.ExitStatus() != 0 {
					return flow.Wait("Failed to cat full backup job index", "pod", targetPod.Name, "exit-status", ee.ExitStatus())
				}
			}
			return flow.Error(err, "Failed to cat full backup job index", "pod", targetPod.Name, "stdout", stdout.String(), "stderr", stderr.String())
		}
		xstoreBackup.Status.CommitIndex, err = strconv.ParseInt(stdout.String(), 10, 64)
		if err != nil {
			return flow.Error(err, "Failed to parse int for stdout", "pod", targetPod.Name, "stdout", stdout.String())
		}
		return flow.Continue("Full Backup job wait finished!", "job-name", job.Name)
	})

var RemoveFullBackupJob = NewStepBinder("RemoveFullBackupJob",
	func(rc *xstorev1reconcile.BackupContext, flow control.Flow) (reconcile.Result, error) {
		job, err := rc.GetXStoreBackupJob()
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to get full backup job!")
		}
		if job == nil {
			return flow.Continue("Full backup job already removed!")
		}

		err = rc.Client().Delete(rc.Context(), job, client.PropagationPolicy(metav1.DeletePropagationBackground))
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to remove full backup job", "job-name", job.Name)
		}

		return flow.Continue("Full backup job removed!", "job-name", job.Name)
	})

var WaitBinlogOffsetCollected = NewStepBinder("WaitBinlogCollected",
	func(rc *xstorev1reconcile.BackupContext, flow control.Flow) (reconcile.Result, error) {
		polardbxBackup, err := rc.GetPolarDBXBackup()
		if err != nil {
			flow.Error(err, "Unable to find polardbxBackup")
		}
		if polardbxBackup.Status.Phase != polardbxv1.BackupCalculating {
			return flow.RetryAfter(5*time.Second, "Wait polardbx backup Collected", "pxcBackup", polardbxBackup.Name)
		}
		return flow.Continue("Binlog Collected!")
	})

var StartCollectBinlogJob = NewStepBinder("StartCollectBinlogJob",
	func(rc *xstorev1reconcile.BackupContext, flow control.Flow) (reconcile.Result, error) {
		const backupJobKey = "backup"
		backupJobContext := &BackupJobContext{}
		err := rc.GetTaskContext(backupJobKey, &backupJobContext)
		if err != nil {
			return flow.Error(err, "Unable to get task context for backup")
		}
		xstore, err := rc.GetXStore()
		if err != nil {
			return flow.Error(err, "Unable to find xstore")
		}
		if xstore.Labels[polardbxmeta.LabelRole] == polardbxmeta.RoleGMS {
			return flow.Continue("GMS don't need to collect binlog job!", "xstore-name:", xstore.Name)
		}
		xstoreBackup := rc.MustGetXStoreBackup()
		targetPod, err := rc.GetXStoreTargetPod()
		if err != nil {
			return flow.Error(err, "Unable to find target pod!")
		}
		if targetPod == nil {
			return flow.Wait("Unable to find target pod!")
		}

		job, err := rc.GetCollectBinlogJob()
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to get collect job!")
		}
		if job != nil {
			return flow.Continue("Collect job already started!", "job-name", job.Name)
		}
		polardbxBackup, err := rc.GetPolarDBXBackup()
		if err != nil {
			return flow.Error(err, "Unable to get pxcBackup!")
		}
		jobName := xstoreconvention.NewBackupJobName(targetPod, xstoreconvention.BackupJobTypeCollect)

		job, err = newCollectJob(xstoreBackup, targetPod, *polardbxBackup, jobName)
		if err != nil {
			return flow.Error(err, "Unable to create CollectJob")
		}

		if err = rc.SetControllerRefAndCreate(job); err != nil {
			return flow.Error(err, "Unable to create job to initialize data")
		}

		// wait 10 seconds to ensure that job has been created
		return flow.RetryAfter(10*time.Second, "collect binlog job started!", "job-name", jobName)
	})

var WaitCollectBinlogJobFinished = NewStepBinder("WaitCollectBinlogJobFinished",
	func(rc *xstorev1reconcile.BackupContext, flow control.Flow) (reconcile.Result, error) {
		xstore, err := rc.GetXStore()
		if err != nil {
			return flow.Error(err, "Unable to find xstore")
		}
		if xstore.Labels[polardbxmeta.LabelRole] == polardbxmeta.RoleGMS {
			return flow.Continue("GMS don't need to collect binlog job!", "xstore-name:", xstore.Name)
		}

		// in case that collect job not found, allow retry ${probeLimit} times, by default the limit is 5
		probeLimit := 5
		xstoreBackup := rc.MustGetXStoreBackup()
		if limitAnnotation, ok := xstoreBackup.Annotations[xstoremeta.AnnotationCollectJobProbeLimit]; ok {
			if tempLimit, err := strconv.Atoi(limitAnnotation); err != nil {
				probeLimit = tempLimit // only update when valid annotation parsed
			}
		}
		flow.Logger().Info("fetch collect job probe limit from annotation", "limit", probeLimit)

		job, err := rc.GetCollectBinlogJob()
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to get collect binlog job!")
		}
		if job == nil {
			if probeLimit--; probeLimit >= 0 { // update probe limit and record into xsb
				xstoreBackup.Annotations[xstoremeta.AnnotationCollectJobProbeLimit] = strconv.Itoa(probeLimit)
				if err := rc.UpdateXStoreBackup(); err != nil {
					return flow.Error(err, "Unable to update collect job probe limit")
				}
				return flow.Retry("Retry to get collect binlog job")
			}
			return flow.Error(errors.New("collect binlog job abnormal"), "Collect binlog job not found, retry limits reached!")
		}

		if !k8shelper.IsJobCompleted(job) {
			return flow.Wait("Collect binlog is still running!", "job-name", job.Name)
		}
		flow.Logger().Info("Collect binlog job completed!", "job-name", job.Name)

		return flow.Continue("Collect binlog wait finished!", "job-name", job.Name)
	})

var RemoveCollectBinlogJob = NewStepBinder("RemoveCollectBinlogJob",
	func(rc *xstorev1reconcile.BackupContext, flow control.Flow) (reconcile.Result, error) {
		job, err := rc.GetCollectBinlogJob()
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to get collect binlog job!")
		}
		if job == nil {
			return flow.Continue("Collect binlog job already removed!")
		}

		err = rc.Client().Delete(rc.Context(), job, client.PropagationPolicy(metav1.DeletePropagationBackground))
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to remove collect binlog job", "job-name", job.Name)
		}

		return flow.Continue("Collect binlog job removed!", "job-name", job.Name)
	})

var WaitPXCSeekCpJobFinished = NewStepBinder("WaitPXCSeekCpJobFinished",
	func(rc *xstorev1reconcile.BackupContext, flow control.Flow) (reconcile.Result, error) {
		polardbxBackup, err := rc.GetPolarDBXBackup()
		if err != nil {
			flow.Error(err, "Unable to find polardbxBackup")
		}
		if polardbxBackup.Status.Phase != polardbxv1.BinlogBackuping {
			return flow.RetryAfter(5*time.Second, "Wait polardbx backup Calculating", "polardbxbackup", polardbxBackup.Name)
		}
		if err != nil {
			flow.Error(err, "Unable to get binlogOffset!")
		}
		return flow.Continue("Binlog Collected!")
	})

var StartBinlogBackupJob = NewStepBinder("StartBinlogBackupJob",
	func(rc *xstorev1reconcile.BackupContext, flow control.Flow) (reconcile.Result, error) {
		const backupJobKey = "backup"
		backupJobContext := &BackupJobContext{}
		err := rc.GetTaskContext(backupJobKey, &backupJobContext)
		if err != nil {
			return flow.Error(err, "Unable to get task context for backup")
		}

		xstoreBackup := rc.MustGetXStoreBackup()
		targetPod, err := rc.GetXStoreTargetPod()
		if err != nil {
			return flow.Error(err, "Unable to find target pod!")
		}
		if targetPod == nil {
			return flow.Wait("Unable to find target pod!")
		}

		job, err := rc.GetBackupBinlogJob()
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to get collect job!")
		}
		if job != nil {
			return flow.Continue("Collect job already started!", "job-name", job.Name)
		}

		jobName := xstoreconvention.NewBackupJobName(targetPod, xstoreconvention.BackupJobTypeBinlogBackup)

		if targetPod.Labels[polardbxmeta.LabelRole] == polardbxmeta.RoleGMS {
			job, err = newBinlogBackupJob(xstoreBackup, targetPod, jobName, true)
		} else {
			job, err = newBinlogBackupJob(xstoreBackup, targetPod, jobName, false)
		}
		if err != nil {
			return flow.Error(err, "Unable to create CollectJob")
		}

		if err = rc.SetControllerRefAndCreate(job); err != nil {
			return flow.Error(err, "Unable to create job to initialize data")
		}

		return flow.Continue("collect binlog job started!", "job-name", jobName)
	})

var WaitBinlogBackupJobFinished = NewStepBinder("WaitBinlogBackupJobFinished",
	func(rc *xstorev1reconcile.BackupContext, flow control.Flow) (reconcile.Result, error) {
		job, err := rc.GetBackupBinlogJob()
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to get binlog backup job!")
		}
		if job == nil {
			flow.Logger().Info("Binlog backup job nil!", "err", err)
			return flow.Continue("Binlog backup job removed!")
		}
		if !k8shelper.IsJobCompleted(job) {
			return flow.Wait("Binlog backup job is still running!", "job-name", job.Name)
		}
		return flow.Continue("Binlog backup job wait finished!", "job-name", job.Name)
	})

var ExtractLastEventTimestamp = NewStepBinder("ExtractLastEventTimestamp",
	func(rc *xstorev1reconcile.BackupContext, flow control.Flow) (reconcile.Result, error) {
		backup := rc.MustGetXStoreBackup()
		nowTime := metav1.Now()
		backup.Status.EndTime = &nowTime

		targetPod, err := rc.GetXStoreTargetPod()
		if err != nil {
			flow.Error(err, "Unable to get targetPod")
		}
		Command := []string{"cat", "/data/mysql/backup/binlogbackup/last_event_timestamp"}
		stdout := &bytes.Buffer{}
		stderr := &bytes.Buffer{}
		err = rc.ExecuteCommandOn(targetPod, "engine", Command, control.ExecOptions{
			Logger: flow.Logger(),
			Stdin:  nil,
			Stdout: stdout,
			Stderr: stderr,
		})
		if err != nil {
			if ee, ok := xstorectrlerrors.ExitError(err); ok {
				if ee.ExitStatus() != 0 {
					return flow.Wait("Failed to cat last event timestamp", "pod", targetPod.Name, "exit-status", ee.ExitStatus())
				}
			}
			return flow.Error(err, "Failed to cat last event timestamp", "pod", targetPod.Name, "stdout", stdout.String(), "stderr", stderr.String())
		}
		output := stdout.String()
		timestampNum, err := strconv.ParseInt(output, 10, 64)
		if err != nil {
			return flow.Error(err, "Invalid last event timestamp", "pod", targetPod.Name, "error", err)
		}
		timestamp := metav1.Unix(timestampNum, 0)
		backup.Status.BackupSetTimestamp = &timestamp
		return flow.Continue("Extract binlog last event timestamp finished!", "pod", targetPod.Name)
	})

var RemoveBinlogBackupJob = NewStepBinder("RemoveBinlogBackupJob",
	func(rc *xstorev1reconcile.BackupContext, flow control.Flow) (reconcile.Result, error) {
		job, err := rc.GetBackupBinlogJob()
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to get binlog backup job!")
		}
		if job == nil {
			return flow.Continue("Binlog backup job already removed!")
		}

		err = rc.Client().Delete(rc.Context(), job, client.PropagationPolicy(metav1.DeletePropagationBackground))
		if client.IgnoreNotFound(err) != nil {
			return flow.Error(err, "Unable to remove binlog backup job", "job-name", job.Name)
		}

		return flow.Continue("Binlog backup job removed!", "job-name", job.Name)
	})

var RemoveXSBackupOverRetention = NewStepBinder("RemoveXSBackupOverRetention",
	func(rc *xstorev1reconcile.BackupContext, flow control.Flow) (reconcile.Result, error) {
		backup := rc.MustGetXStoreBackup()
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
		return flow.Continue("PolarDBX backup deleted!", "XSBackup-name", backup.Name)
	})

var WaitPXCBinlogBackupFinished = NewStepBinder("WaitPXCBinlogBackupFinished",
	func(rc *xstorev1reconcile.BackupContext, flow control.Flow) (reconcile.Result, error) {
		polardbxBackup, err := rc.GetPolarDBXBackup()
		if err != nil {
			flow.Error(err, "Unable to find get PolarDBX backup")
		}
		if polardbxBackup.Status.Phase != polardbxv1.MetadataBackuping {
			return flow.RetryAfter(5*time.Second, "Wait until PolarDBX binlog backup finished", "pxc backup", polardbxBackup.Name)
		}
		return flow.Continue("PolarDBX binlog backup finished.")
	})

var SaveXStoreSecrets = NewStepBinder("SaveXStoreSecrets",
	func(rc *xstorev1reconcile.BackupContext, flow control.Flow) (reconcile.Result, error) {
		backup := rc.MustGetXStoreBackup()
		backupSecret, err := rc.GetSecret(backup.Name)
		if backupSecret != nil {
			return flow.Continue("Already have backup secret")
		}

		secret, err := rc.GetSecret(backup.Spec.XStore.Name)
		if err != nil {
			return flow.Error(err, "Unable to get secret for xstore", "xstore_name", backup.Spec.XStore.Name)
		}
		backupSecret, err = rc.NewSecretFromXStore(secret)
		if err != nil {
			return flow.Error(err, "Unable to new account secret while backuping")
		}
		err = rc.SetControllerRefAndCreate(backupSecret)
		if err != nil {
			return flow.Error(err, "Unable to create account secret while backuping")
		}
		return flow.Continue("XStore Secret Saved!")
	})
