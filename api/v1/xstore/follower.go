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

type FollowerRole string

const (
	FollowerRoleLearner  FollowerRole = "learner"
	FollowerRoleFollower FollowerRole = "follower"
	FollowerRoleLogger   FollowerRole = "logger"
)
