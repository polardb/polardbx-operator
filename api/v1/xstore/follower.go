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

package xstore

type FollowerPhase string

const (
	FollowerPhaseNew           FollowerPhase = ""
	FollowerPhaseCheck         FollowerPhase = "FollowerPhaseCheck"
	FollowerPhaseBackupPrepare FollowerPhase = "FollowerPhaseBackupPrepare"
	FollowerPhaseBackupStart   FollowerPhase = "FollowerPhaseBackupStart"
	FollowerPhaseBackup        FollowerPhase = "FollowerPhaseBackup"
	FollowerPhaseLoggerRebuild FollowerPhase = "FollowerPhaseLoggerRebuild"
	FollowerPhaseMonitorBackup FollowerPhase = "FollowerPhaseMonitorBackup"
	FollowerPhaseBeforeRestore FollowerPhase = "FollowerPhaseBeforeRestore"
	FollowerPhaseRestore       FollowerPhase = "FollowerPhaseRestore"
	FollowerPhaseAfterRestore  FollowerPhase = "FollowerPhaseAfterRestore"
	FollowerPhaseSuccess       FollowerPhase = "FollowerPhaseSuccess"
	FollowerPhaseWaitSwitch    FollowerPhase = "FollowerPhaseWaitSwitch"
	FollowerPhaseFailed        FollowerPhase = "FollowerPhaseFailed"
	FollowerPhaseLoggerCreate  FollowerPhase = "FollowerPhaseLoggerCreate"
	FollowerCreateTmpPod       FollowerPhase = "FollowerCreateRemotePod"
	FollowerPhaseDeleting      FollowerPhase = "FollowerPhaseDeleting"
)

func IsEndPhase(phase FollowerPhase) bool {
	return phase == FollowerPhaseSuccess || phase == FollowerPhaseFailed || phase == FollowerPhaseDeleting
}

type FollowerRole string

const (
	FollowerRoleLearner  FollowerRole = "learner"
	FollowerRoleFollower FollowerRole = "follower"
	FollowerRoleLogger   FollowerRole = "logger"
)
