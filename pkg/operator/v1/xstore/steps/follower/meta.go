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

type JobTask string

const (
	JobTaskBackup              JobTask = "backup"
	JobTaskBackupKeyring       JobTask = "backup-keyring"
	JobTaskRestoreCleanDataDir JobTask = "clean-restore"
	JobTaskRestorePrepare      JobTask = "prepare-restore"
	JobTaskRestoreMoveBack     JobTask = "move-restore"
	JobTaskBeforeRestore       JobTask = "before-restore"
	JobTaskFlushConsensusMeta  JobTask = "flush-meta"
	JobTaskAfterRestore        JobTask = "after-restore"
	JobTaskInitLogger          JobTask = "init-logger"
	JobTaskRestoreKeyring      JobTask = "restore-keyring"
)

var (
	JobTaskOrderIndexMap = map[JobTask]int{}
)

func init() {
	jobList := []JobTask{JobTaskBackupKeyring, JobTaskBackup, JobTaskBeforeRestore, JobTaskRestoreCleanDataDir, JobTaskRestoreKeyring, JobTaskRestorePrepare, JobTaskRestoreMoveBack, JobTaskFlushConsensusMeta, JobTaskInitLogger, JobTaskAfterRestore}
	for i, jobTask := range jobList {
		JobTaskOrderIndexMap[jobTask] = i
	}
}

const (
	JobLabelPrefix        = "xstorefollowerjob"
	JobLabelXStoreName    = JobLabelPrefix + "/xstore_name"
	JobLabelTargetPodName = JobLabelPrefix + "/target_pod_name"
	JobLabelTask          = JobLabelPrefix + "/task"
	JobLabelName          = JobLabelPrefix + "/job_name"
)

const (
	JobEnvName = "XSTORE_FOLLOWER_JOB"
)
