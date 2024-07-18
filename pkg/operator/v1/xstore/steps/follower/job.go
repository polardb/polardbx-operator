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

package follower

import (
	"context"
	"errors"
	"fmt"
	xstorev1 "github.com/alibaba/polardbx-operator/api/v1/xstore"
	hpfs "github.com/alibaba/polardbx-operator/pkg/hpfs/proto"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/meta/core/gms/security"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/config"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	polarxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	. "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin/galaxy/galaxy"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"path/filepath"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"strconv"
	"strings"
)

type JobContext struct {
	jobName       string
	jobTask       JobTask
	jobTargetPod  *corev1.Pod
	config        config.Config
	otherPod      *corev1.Pod
	parentContext *xstorev1reconcile.FollowerContext
	engine        string
}

var (
	JobCommands = map[JobTask]func(JobContext) []string{
		JobTaskBackup:              JobCommandBackupFunc,
		JobTaskBackupKeyring:       JobCommandBackupKeyringFunc,
		JobTaskRestorePrepare:      JobCommandPrepareRestoreFunc,
		JobTaskBeforeRestore:       JobCommandBeforeRestoreFunc,
		JobTaskAfterRestore:        JobCommandAfterRestoreFunc,
		JobTaskFlushConsensusMeta:  JobCommandFlushConsensusMetaFunc,
		JobTaskRestoreCleanDataDir: JobCommandCleanDataDirRestoreFunc,
		JobTaskRestoreMoveBack:     JobCommandMoveRestoreFunc,
		JobTaskInitLogger:          JobCommandInitLoggerFunc,
		JobTaskRestoreKeyring:      JobCommandRestoreKeyringFunc,
	}
	JobArgs = map[JobTask]func(JobContext) []string{
		JobTaskBackup:              JobArgBackupFunc,
		JobTaskBackupKeyring:       JobArgBackupKeyringFunc,
		JobTaskRestorePrepare:      JobArgPrepareRestoreFunc,
		JobTaskBeforeRestore:       JobArgBeforeRestoreFunc,
		JobTaskAfterRestore:        JobArgAfterRestoreFunc,
		JobTaskFlushConsensusMeta:  JobArgFlushConsensusFunc,
		JobTaskRestoreCleanDataDir: JobArgCleanDataDirRestoreFunc,
		JobTaskRestoreMoveBack:     JobArgMoveRestoreFunc,
		JobTaskInitLogger:          JobArgInitLoggerFunc,
		JobTaskRestoreKeyring:      JobArgRestoreKeyringFunc,
	}
	BackupToolBinFilePaths = map[string]string{
		galaxy.Engine: GalaxyEngineBackupBinFilepath,
	}
	BackupSetPrepareArgs = map[string]string{
		galaxy.Engine: GalaxyEngineBackupSetPrepareArg,
	}
	BackupExtraArgs = map[string]string{
		galaxy.Engine: GalaxyEngineBackupExtraArgs,
	}
	BackupStreamTypeArgs = map[string]string{
		galaxy.Engine: GalaxyEngineBackupStreamArgs,
	}
	TargetDirArgs = map[string]string{
		galaxy.Engine: GalaxyEngineTargetDirArgs,
	}
)

func JobCommandBackupFunc(ctx JobContext) []string {
	return []string{"/usr/bin/bash"}
}

func JobCommandBackupKeyringFunc(ctx JobContext) []string {
	return []string{"/usr/bin/bash"}
}

func JobCommandRestoreKeyringFunc(ctx JobContext) []string {
	return []string{"/usr/bin/bash"}
}

func JobCommandCleanDataDirRestoreFunc(ctx JobContext) []string {
	return []string{"/usr/bin/bash"}
}

func JobCommandPrepareRestoreFunc(ctx JobContext) []string {
	return []string{"/usr/bin/bash"}
}

func JobCommandMoveRestoreFunc(ctx JobContext) []string {
	return []string{"/usr/bin/bash"}
}

func JobCommandBeforeRestoreFunc(ctx JobContext) []string {
	return []string{"/usr/bin/bash"}
}

func JobCommandAfterRestoreFunc(ctx JobContext) []string {
	return []string{"/usr/bin/bash"}
}

func JobCommandFlushConsensusMetaFunc(ctx JobContext) []string {
	return []string{"/usr/bin/bash"}
}

func JobCommandInitLoggerFunc(ctx JobContext) []string {
	return []string{"/usr/bin/bash"}
}

func JobArgBackupFunc(ctx JobContext) []string {
	return []string{
		"-c",
		"touch /tmp/rebuild.log && tail -f /tmp/rebuild.log & " + " `/tools/xstore/current/venv/bin/python3 /tools/xstore/current/cli.py engine xtrabackup` --defaults-file=/data/mysql/conf/my.cnf --backup " + BackupExtraArgs[ctx.engine] + " --user=root --socket='/data/mysql/run/mysql.sock' " + " --xtrabackup-plugin-dir=`/tools/xstore/current/venv/bin/python3 /tools/xstore/current/cli.py engine xtrabackup_plugin` " + BackupStreamTypeArgs[ctx.engine] + " " + TargetDirArgs[ctx.engine] + "/tmp/backup 2>/tmp/rebuild.log " +
			"| /tools/xstore/current/bin/polardbx-filestream-client " + BackupStreamTypeArgs[ctx.engine] + " --meta.action=uploadRemote " + fmt.Sprintf(" --meta.instanceId='%s' ", GetFileStreamInstanceId(ctx.otherPod)) +
			fmt.Sprintf(" --meta.filename='%s' ", FileStreamBackupFilename) + fmt.Sprintf(" --destNodeName='%s' ", ctx.otherPod.Spec.NodeName) + " --hostInfoFilePath=/tools/xstore/hdfs-nodes.json && /tools/xstore/current/venv/bin/python3 /tools/xstore/current/cli.py process check_std_err_complete --filepath=/tmp/rebuild.log ",
	}
}

func JobArgBackupKeyringFunc(ctx JobContext) []string {
	keyringPath := ctx.parentContext.MustGetXStore().Spec.TDE.KeyringPath
	return []string{
		"-c",
		" /tools/xstore/current/venv/bin/python3 /tools/xstore/current/cli.py engine filestream --action=uploadRemote" + fmt.Sprintf(" --local_file='%s' ", keyringPath) + fmt.Sprintf(" --instance_id='%s' ", GetFileStreamInstanceId(ctx.otherPod)) +
			fmt.Sprintf(" --filename='%s' ", FileStreamKeyringFilename) + fmt.Sprintf(" --destnode_name='%s' ", ctx.otherPod.Spec.NodeName),
	}
}

func JobArgBeforeRestoreFunc(ctx JobContext) []string {
	return []string{"-c",
		"/tools/xstore/current/venv/bin/python3 /tools/xstore/current/cli.py engine set_engine_enable --disable",
	}
}

func JobArgCleanDataDirRestoreFunc(ctx JobContext) []string {
	return []string{
		"-c",
		"/tools/xstore/current/venv/bin/python3 /tools/xstore/current/entrypoint.py --restore-prepare --initialize --force",
	}
}

func JobArgMoveRestoreFunc(ctx JobContext) []string {
	backupDir := filepath.Join(FileStreamRootDir, GetFileStreamInstanceId(ctx.jobTargetPod), FileStreamBackupFilename)
	return []string{
		"-c",
		" `/tools/xstore/current/venv/bin/python3 /tools/xstore/current/cli.py engine xtrabackup` --defaults-file=/data/mysql/conf/my.cnf --force-non-empty-directories --move-back " + TargetDirArgs[ctx.engine] + backupDir,
	}
}

func JobArgPrepareRestoreFunc(ctx JobContext) []string {
	backupDir := filepath.Join(FileStreamRootDir, GetFileStreamInstanceId(ctx.jobTargetPod), FileStreamBackupFilename)
	keyringPath := ctx.parentContext.MustGetXStore().Spec.TDE.KeyringPath
	return []string{
		"-c",
		" `/tools/xstore/current/venv/bin/python3 /tools/xstore/current/cli.py engine xtrabackup` " + fmt.Sprintf(" %s ", BackupSetPrepareArgs[ctx.engine]) + " --xtrabackup-plugin-dir=/tools/xstore/current/xtrabackup/8.0-2/xcluster_xtrabackup80/lib/plugin --keyring-file-data=" + keyringPath + " --use-memory=1G " + TargetDirArgs[ctx.engine] + backupDir,
	}
}

func JobArgAfterRestoreFunc(ctx JobContext) []string {
	return []string{"-c",
		"/tools/xstore/current/venv/bin/python3 /tools/xstore/current/cli.py engine set_engine_enable --enable",
	}
}

func JobArgFlushConsensusFunc(ctx JobContext) []string {
	backupBinlogInfoFilepath := filepath.Join(FileStreamRootDir, GetFileStreamInstanceId(ctx.jobTargetPod), FileStreamBackupFilename, "xtrabackup_binlog_info")
	learnerFlag := ""
	if ctx.parentContext.MustGetXStoreFollower().Spec.Role == xstorev1.FollowerRoleLearner {
		learnerFlag = "--learner"
	}
	return []string{"-c",
		"chown -R  mysql:mysql  /data/mysql && chown -R  mysql:mysql  /data-log/mysql && rm -fR /data/mysql/log/mysql_bin.* && /tools/xstore/current/venv/bin/python3 /tools/xstore/current/cli.py engine reset_meta --learner --recover-index-filepath=" + backupBinlogInfoFilepath + " " + learnerFlag,
	}
}

func JobArgInitLoggerFunc(ctx JobContext) []string {
	return []string{"-c",
		fmt.Sprintf("rm -f /data/mysql/initialized && /tools/xstore/current/venv/bin/python3 /tools/xstore/current/entrypoint.py --initialize --force  --cluster-start-index=%d", ctx.parentContext.GetCommitIndex()),
	}
}

func JobArgRestoreKeyringFunc(ctx JobContext) []string {
	keyringPath := filepath.Dir(ctx.parentContext.MustGetXStore().Spec.TDE.KeyringPath)
	keyringFilepath := filepath.Join(FileStreamRootDir, GetFileStreamInstanceId(ctx.jobTargetPod), FileStreamKeyringFilename)
	return []string{"-c",
		fmt.Sprintf("mkdir -p %s && chown -R  mysql:mysql %s && mv %s %s", keyringPath, keyringPath, keyringFilepath, keyringPath),
	}

}
func GetFileStreamInstanceId(pod *corev1.Pod) string {
	return fmt.Sprintf("%s-%s", pod.Namespace, pod.Name)
}

func GetFileStreamDir(pod *corev1.Pod) string {
	return filepath.Join(FileStreamRootDir, GetFileStreamInstanceId(pod), FileStreamBackupFilename)
}

func newJobName(task JobTask, targetPod *corev1.Pod, xfName string) string {
	hashStr := strconv.FormatUint(security.Hash(targetPod.Name+xfName), 16)
	suffix := ""
	if val, ok := targetPod.Labels[polarxmeta.LabelRole]; ok {
		suffix = suffix + val
	}
	if val, ok := targetPod.Labels[polarxmeta.LabelDNIndex]; ok {
		suffix = suffix + val
	}
	jobName := fmt.Sprintf("job%s%s%s", string(task), hashStr, suffix)
	if len(jobName) > 63 {
		jobName = jobName[:62] + "0"
	}
	return jobName
}

func newJob(ctx JobContext) *batchv1.Job {
	podSpec := ctx.jobTargetPod.Spec.DeepCopy()
	//generate volumes
	volumes := podSpec.Volumes
	hostPathType := corev1.HostPathDirectory
	filestreamVolume := corev1.Volume{
		Name: "filestream",
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: ctx.config.Store().HostPathFilestreamVolumeRoot(),
				Type: &hostPathType,
			},
		},
	}
	volumes = append(volumes, filestreamVolume)
out:
	for i, volume := range volumes {
		if volume.Name == PodInfoVolumeName {
			for j, item := range volume.DownwardAPI.Items {
				if item.Path == PodInfoNamePath && volume.DownwardAPI != nil {
					item.FieldRef.FieldPath = PodInfoNameFieldPath
					volume.DownwardAPI.Items[j] = item
					volumes[i] = volume
					break out
				}
			}
		}
	}

	engineContainer := k8shelper.GetContainerFromPodSpec(podSpec, "engine")
	engineContainer.Env = append(engineContainer.Env, corev1.EnvVar{
		Name:  JobEnvName,
		Value: string(ctx.jobTask),
	})
	engineContainer.VolumeMounts = append(engineContainer.VolumeMounts, corev1.VolumeMount{
		Name:      "filestream",
		MountPath: "/filestream",
	})
	engineContainer.Ports = nil
	engineContainer.Resources.Requests = corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("0"),
		corev1.ResourceMemory: resource.MustParse("0Mi"),
	}
	engineContainer.Command = JobCommands[ctx.jobTask](ctx)
	engineContainer.Args = JobArgs[ctx.jobTask](ctx)
	engineContainer.ReadinessProbe = nil
	engineContainer.StartupProbe = nil
	engineContainer.LivenessProbe = nil
	if engineContainer.Lifecycle != nil {
		engineContainer.Lifecycle.PreStop = nil
	}
	// generate volume mounts
	var jobParallelism int32 = 1
	var completions int32 = 1
	var backOffLimit int32 = 0
	var jobTTL int32 = 3600 * 24 * 7
	job := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ctx.jobName,
			Namespace: ctx.jobTargetPod.Namespace,
			Labels: map[string]string{
				JobLabelXStoreName:    ctx.jobTargetPod.Labels[xstoremeta.LabelName],
				JobLabelTargetPodName: ctx.jobTargetPod.Labels[xstoremeta.LabelPod],
				JobLabelTask:          string(ctx.jobTask),
			},
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: &jobTTL,
			Parallelism:             &jobParallelism,
			Completions:             &completions,
			BackoffLimit:            &backOffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: ctx.jobTargetPod.Namespace,
					Labels: map[string]string{
						JobLabelXStoreName:    ctx.jobTargetPod.Labels[xstoremeta.LabelName],
						JobLabelTargetPodName: ctx.jobTargetPod.Labels[xstoremeta.LabelPod],
						JobLabelTask:          string(ctx.jobTask),
						JobLabelName:          ctx.jobName,
					},
				},
				Spec: corev1.PodSpec{
					NodeName:      podSpec.NodeName,
					Volumes:       volumes,
					Containers:    append([]corev1.Container{}, *engineContainer),
					RestartPolicy: corev1.RestartPolicyNever,
				},
			},
		},
	}
	return &job
}

func RemovePath(ctx context.Context, hpfsClient hpfs.HpfsServiceClient, nodeName string, path string) error {
	resp, err := hpfsClient.RemoveDirectory(ctx, &hpfs.RemoveDirectoryRequest{
		Host:    &hpfs.Host{NodeName: nodeName},
		Options: &hpfs.RemoveOptions{Recursive: true, IgnoreIfNotExists: true},
		Path:    path,
	})
	if err != nil {
		return err
	}
	if resp.Status.Code != hpfs.Status_OK {
		return errors.New("status not ok: " + resp.Status.Code.String())
	}
	return nil
}

var CleanBackupJob = NewStepBinder("CleanBackupJob", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	targetPod, err := rc.GetPodByName(rc.MustGetXStoreFollower().Status.RebuildPodName)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return flow.Pass()
		}
		return flow.RetryErr(err, "GetXStorePod Failed")
	}

	//clean the file stream remote backup dir
	hpfsClient, err := rc.XStoreContext().GetHpfsClient()
	if err != nil {
		return flow.RetryErr(err, "Failed to get hpfs client")
	}
	fileStreamDir := GetFileStreamDir(targetPod)
	err = RemovePath(rc.Context(), hpfsClient, targetPod.Spec.NodeName, fileStreamDir)
	if err != nil {
		return flow.RetryErr(err, "Failed to remove Path", "NodeName", targetPod.Spec.NodeName, "path", fileStreamDir)
	}
	return flow.Continue("CleanBackupJob Success.")
})

var StartBackupKeyringJob = NewStepBinder("StartBackupKeyringJob", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	if checkIfJobSkip(rc, JobTaskBackupKeyring) {
		return flow.Pass()
	}
	xstoreContext := rc.XStoreContext()
	xstore := xstoreContext.MustGetXStore()
	if xstore.Status.TdeStatus == false || xstore.Labels[polardbxmeta.LabelRole] == polardbxmeta.RoleGMS {
		return flow.Pass()
	}
	fromPod, err := xstoreContext.GetXStorePod(rc.MustGetXStoreFollower().Spec.FromPodName)
	if err != nil {
		return flow.RetryErr(err, "GetXStorePod Failed")
	}
	targetPod, err := rc.GetPodByName(rc.MustGetXStoreFollower().Status.RebuildPodName)
	if err != nil {
		return flow.RetryErr(err, "GetXStorePod Failed")
	}
	jobName := newJobName(JobTaskBackupKeyring, fromPod, rc.MustGetXStoreFollower().GetName())
	jobContext := JobContext{
		jobName:       jobName,
		jobTask:       JobTaskBackupKeyring,
		jobTargetPod:  fromPod,
		config:        rc.XStoreContext().Config(),
		otherPod:      targetPod,
		parentContext: rc,
		engine:        xstoreContext.MustGetXStore().Spec.Engine,
	}
	job := newJob(jobContext)
	err = rc.SetControllerRefAndCreate(job)
	if err != nil {
		return flow.RetryErr(err, "Create BackupJob Failed")
	}
	xstoreFollower := rc.MustGetXStoreFollower()
	xstoreFollower.Status.BackupJobName = jobName
	xstoreFollower.Status.CurrentJobName = jobName
	xstoreFollower.Status.CurrentJobTask = string(JobTaskBackupKeyring)
	rc.MarkChanged()
	return flow.Continue("JobTaskBackupKeyring Success.")
})

var StartBackupJob = NewStepBinder("StartBackupJob", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	if checkIfJobSkip(rc, JobTaskBackup) {
		return flow.Pass()
	}
	xstoreContext := rc.XStoreContext()
	fromPod, err := xstoreContext.GetXStorePod(rc.MustGetXStoreFollower().Spec.FromPodName)
	if err != nil {
		return flow.RetryErr(err, "GetXStorePod Failed")
	}
	targetPod, err := rc.GetPodByName(rc.MustGetXStoreFollower().Status.RebuildPodName)
	if err != nil {
		return flow.RetryErr(err, "GetXStorePod Failed")
	}
	jobName := newJobName(JobTaskBackup, fromPod, rc.MustGetXStoreFollower().GetName())
	jobContext := JobContext{
		jobName:       jobName,
		jobTask:       JobTaskBackup,
		jobTargetPod:  fromPod,
		config:        rc.XStoreContext().Config(),
		otherPod:      targetPod,
		parentContext: rc,
		engine:        xstoreContext.MustGetXStore().Spec.Engine,
	}
	job := newJob(jobContext)
	err = rc.SetControllerRefAndCreate(job)
	if err != nil {
		return flow.RetryErr(err, "Create BackupJob Failed")
	}
	xstoreFollower := rc.MustGetXStoreFollower()
	xstoreFollower.Status.BackupJobName = jobName
	xstoreFollower.Status.CurrentJobName = jobName
	xstoreFollower.Status.CurrentJobTask = string(JobTaskBackup)
	rc.MarkChanged()
	return flow.Continue("StartBackupJob Success.")
})

var MonitorBackupJob = NewStepBinder("MonitorBackupJob", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	//check if job has finished
	return CheckJobStatus(rc, flow, rc.MustGetXStoreFollower().Status.BackupJobName)
})

func CheckJobStatus(rc *xstorev1reconcile.FollowerContext, flow control.Flow, jobName string) (reconcile.Result, error) {
	var job batchv1.Job
	jobKey := types.NamespacedName{
		Namespace: rc.Namespace(),
		Name:      jobName,
	}
	err := rc.Client().Get(rc.Context(), jobKey, &job)
	if err != nil {
		return flow.RetryErr(fmt.Errorf("failed to get job"), "", "jobKey", jobKey.String())
	}
	if job.Status.Failed > 0 {
		rc.MustGetXStoreFollower().Status.Phase = xstorev1.FollowerPhaseFailed
		rc.MustGetXStoreFollower().Status.Message = fmt.Sprintf("job failed. jobKey %s", jobKey.String())
		rc.MarkChanged()
		return flow.RetryErr(fmt.Errorf("job failed"), "", "jobKey", jobKey.String())
	}
	if job.Status.Succeeded > 0 {
		return flow.Continue("job succeeded")
	}
	return flow.Retry("MonitorBackupJob. Job has not finished")
}

var BeforeRestoreJob = NewStepBinder("BeforeRestoreJob", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	if checkIfJobSkip(rc, JobTaskBeforeRestore) {
		return flow.Pass()
	}
	_, err := CreateJob(rc, JobTaskBeforeRestore)
	if err != nil {
		return flow.RetryErr(err, "Create Job Failed")
	}
	return flow.Continue("BeforeRestoreJob Success.")
})

func CreateJob(rc *xstorev1reconcile.FollowerContext, jobTask JobTask) (string, error) {
	targetPod, err := rc.GetPodByName(rc.MustGetXStoreFollower().Status.RebuildPodName)
	if err != nil {
		return "", err
	}
	jobName := newJobName(jobTask, targetPod, rc.MustGetXStoreFollower().GetName())
	jobContext := JobContext{
		jobName:       jobName,
		jobTask:       jobTask,
		jobTargetPod:  targetPod,
		config:        rc.XStoreContext().Config(),
		parentContext: rc,
		engine:        rc.XStoreContext().MustGetXStore().Spec.Engine,
	}
	job := newJob(jobContext)
	err = rc.SetControllerRefAndCreate(job)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return "", err
	}
	xstoreFollower := rc.MustGetXStoreFollower()
	xstoreFollower.Status.CurrentJobName = jobName
	xstoreFollower.Status.CurrentJobTask = string(jobTask)
	rc.MarkChanged()
	return jobName, nil
}

var CleanDataDirRestoreJob = NewStepBinder("CleanDataDirRestoreJob", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	if checkIfJobSkip(rc, JobTaskRestoreCleanDataDir) {
		return flow.Pass()
	}
	jobName, err := CreateJob(rc, JobTaskRestoreCleanDataDir)
	if err != nil {
		return flow.RetryErr(err, "Create Job Failed")
	}
	rc.MustGetXStoreFollower().Status.RestoreJobName = jobName
	rc.MarkChanged()
	return flow.Continue("CleanDataDirRestoreJob Success.")
})

var PrepareRestoreJob = NewStepBinder("PrepareRestoreJob", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	if checkIfJobSkip(rc, JobTaskRestorePrepare) {
		return flow.Pass()
	}
	jobName, err := CreateJob(rc, JobTaskRestorePrepare)
	if err != nil {
		return flow.RetryErr(err, "Create Job Failed")
	}
	rc.MustGetXStoreFollower().Status.RestoreJobName = jobName
	rc.MarkChanged()
	return flow.Continue("PrepareRestoreJob Success.")
})

var MoveRestoreJob = NewStepBinder("MoveRestoreJob", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	if checkIfJobSkip(rc, JobTaskRestoreMoveBack) {
		return flow.Pass()
	}
	jobName, err := CreateJob(rc, JobTaskRestoreMoveBack)
	if err != nil {
		return flow.RetryErr(err, "Create Job Failed")
	}
	rc.MustGetXStoreFollower().Status.RestoreJobName = jobName
	rc.MarkChanged()
	return flow.Continue("MoveRestoreJob Success.")
})

var AfterRestoreJob = NewStepBinder("AfterRestoreJob", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	if checkIfJobSkip(rc, JobTaskAfterRestore) {
		return flow.Pass()
	}
	_, err := CreateJob(rc, JobTaskAfterRestore)
	if err != nil {
		return flow.RetryErr(err, "Create Job Failed")
	}
	return flow.Continue("AfterRestoreJob Success.")
})

var RestoreKeyringJob = NewStepBinder("RestoreKeyringJob", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	if checkIfJobSkip(rc, JobTaskRestoreKeyring) {
		return flow.Pass()
	}
	xstoreContext := rc.XStoreContext()
	xstore := xstoreContext.MustGetXStore()
	if xstore.Status.TdeStatus == false || xstore.Labels[polardbxmeta.LabelRole] == polardbxmeta.RoleGMS {
		return flow.Pass()
	}
	_, err := CreateJob(rc, JobTaskRestoreKeyring)
	if err != nil {
		return flow.RetryErr(err, "Create Job Failed")
	}
	return flow.Continue("RestoreKeyringJob Success.")
})

func checkIfJobSkip(rc *xstorev1reconcile.FollowerContext, jobTask JobTask) bool {
	statusJobTask := JobTask(rc.MustGetXStoreFollower().Status.CurrentJobTask)
	if statusJobTask == "" {
		return false
	}
	return JobTaskOrderIndexMap[jobTask] <= JobTaskOrderIndexMap[statusJobTask]
}

var MonitorCurrentJob = NewStepBinder("MonitorCurrentJOB", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	//check if job has finished
	return CheckJobStatus(rc, flow, rc.MustGetXStoreFollower().Status.CurrentJobName)
})

var FlushConsensusMeta = NewStepBinder("FlushConsensusMeta", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	if checkIfJobSkip(rc, JobTaskFlushConsensusMeta) {
		return flow.Pass()
	}
	_, err := CreateJob(rc, JobTaskFlushConsensusMeta)
	if err != nil {
		return flow.RetryErr(err, "Create Job Failed")
	}
	return flow.Continue("FlushConsensusMeta Success.")
})

var InitializeLogger = NewStepBinder("InitializeLogger", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	if checkIfJobSkip(rc, JobTaskInitLogger) {
		return flow.Pass()
	}
	_, err := CreateJob(rc, JobTaskInitLogger)
	if err != nil {
		return flow.RetryErr(err, "Create Job Failed")
	}
	return flow.Continue("InitializeLogger Success.")
})
