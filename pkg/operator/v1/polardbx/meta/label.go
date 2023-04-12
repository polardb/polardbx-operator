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

package meta

const (
	LabelName            = "polardbx/name"
	LabelUid             = "polardbx/uid"
	LabelRand            = "polardbx/rand"
	LabelRole            = "polardbx/role"
	LabelCNType          = "polardbx/cn-type"
	LabelDNIndex         = "polardbx/dn-index"
	LabelTopologyRule    = "polardbx/topology-rule"
	LabelGeneration      = "polardbx/generation"
	LabelPortLock        = "polardbx/port-lock"
	LabelGroup           = "polardbx/group"
	LabelHash            = "polardbx/hash"
	LabelTopBackup       = "polardbx/top-backup"
	LabelBackupXStore    = "polardbx/xstore"
	LabelBackupXStoreUID = "polardbx/xstore-uid"
	LabelBinlogPurgeLock = "polardbx/binlogpurge-lock"
	LabelPrimaryName     = "polardbx/primary-name"
	LabelType            = "polardbx/type"
	LabelAuditLog        = "polardbx/enableAuditLog"
	LabelBackupSchedule  = "polardbx/backup-schedule"
	LabelBackupBinlog    = "polardbx/backupBinlog"
	LabelJobType         = "polardbx/jobType"
)

const (
	SeekCpJobLabelPXCName    = "seekcp-job/pxc"
	SeekCpJobLabelBackupName = "seekcp-job/backup"
	// SeekCpJobLabelPodName denotes the pod on which seekcp job performed
	SeekCpJobLabelPodName = "seekcp-job/pod"
)

const (
	RoleGMS      = "gms"
	RoleCN       = "cn"
	RoleDN       = "dn"
	RoleCDC      = "cdc"
	RoleColumnar = "columnar"
)

const (
	BackupPath        = "polardbx-backup"
	FullBackupPath    = "fullbackup"
	BinlogOffsetPath  = "binlogoffset"
	CollectBinlogPath = "collect"
	BinlogBackupPath  = "binlogbackup"
	SeekCpName        = "set.cp"
	BinlogIndexesName = "indexes"
)

func AssertRoleIn(role string, candidates ...string) {
	for _, c := range candidates {
		if role == c {
			return
		}
	}
	panic("invalid role: " + role)
}

const (
	CNTypeRW = "rw"
	CNTypeRO = "ro"
)

const (
	TypeMaster   = "master"
	TypeReadonly = "readonly"
)

type PxcJobType string

const (
	HeartbeatJobType         PxcJobType = "PitrHeartbeat"
	PitrPrepareBinlogJobType PxcJobType = "PitrPrepareBinlogs"
)
