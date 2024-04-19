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

import (
	"strings"

	corev1 "k8s.io/api/core/v1"

	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
)

const (
	LabelName         = "xstore/name"
	LabelUid          = "xstore/uid"
	LabelRand         = "xstore/rand"
	LabelRole         = "xstore/role"
	LabelPod          = "xstore/pod"
	LabelNodeRole     = "xstore/node-role"
	LabelServiceType  = "xstore/service"
	LabelNodeSet      = "xstore/node-set"
	LabelGeneration   = "xstore/generation"
	LabelPortLock     = "xstore/port-lock"
	LabelHash         = "xstore/hash"
	LabelConfigHash   = "xstore/config-hash"
	LabelPrimaryName  = "xstore/primary-name"
	LabelRebuildTask  = "xstore/rebuild-task"
	LabelOriginName   = "xstore/origin-name"
	LabelTargetXStore = "xstore/target-name"
	LabelTmp          = "xstore/tmp"
	LabelAutoRebuild  = "xstore/auto-rebuild"
	LabelJobType      = "xstore/jobType"
	LabelBackupBinlog = "xstore/backupBinlog"
)

const (
	LabelXStoreBackupName       = "xstore/backup"
	LabelXStoreBinlogBackupName = "xstore/binlogbackup"
	LabelBinlogPurgeLock        = "xstore/binlogpurge-lock"
	LabelXStoreCollectName      = "xstore/collect"
)

const (
	BinlogPurgeLock   = "locked"
	BinlogPurgeUnlock = "unlock"
)

// Valid roles
const (
	RoleLeader    = "leader"
	RoleFollower  = "follower"
	RoleCandidate = "candidate" // Only appears in voting progress
	RoleLogger    = "logger"
	RoleLearner   = "learner"
)

const (
	XStoreBackupPath = "xstore-backup"
)

func PodRole2DefaultRole(nodeRole polardbxv1xstore.NodeRole) string {
	switch nodeRole {
	case polardbxv1xstore.RoleCandidate:
		return RoleFollower
	case polardbxv1xstore.RoleVoter:
		return RoleLogger
	case polardbxv1xstore.RoleLearner:
		return RoleLearner
	default:
		panic("invalid pod role: " + nodeRole)
	}
}

func IsRoleLeader(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	l, ok := pod.Labels[LabelRole]
	if !ok {
		return false
	}
	return l == RoleLeader
}

func IsRoleFollower(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	l, ok := pod.Labels[LabelRole]
	if !ok {
		return false
	}
	return l == RoleFollower
}

func IsPodRoleCandidate(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	l, ok := pod.Labels[LabelNodeRole]
	if !ok {
		return false
	}
	return l == strings.ToLower(string(polardbxv1xstore.RoleCandidate))
}

func IsPodRoleVoter(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	l, ok := pod.Labels[LabelNodeRole]
	if !ok {
		return false
	}
	return l == strings.ToLower(string(polardbxv1xstore.RoleVoter))
}

func IsPodRoleLearner(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	l, ok := pod.Labels[LabelNodeRole]
	if !ok {
		return false
	}
	return l == strings.ToLower(string(polardbxv1xstore.RoleLearner))
}

// Valid service types.
const (
	ServiceTypeReadWrite       = "readwrite"
	ServiceTypeReadOnly        = "readonly"
	ServiceTypeMetrics         = "metrics"
	ServiceTypeClusterIp       = "clusterIp"
	ServiceTypeStaticClusterIp = "staticClusterIp"
)

type XStoreJobType string

const (
	PitrPrepareBinlogForXStoreJobType XStoreJobType = "PitrPrepareBinlogsForXStore"
)
