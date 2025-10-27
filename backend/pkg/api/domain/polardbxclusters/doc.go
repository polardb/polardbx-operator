package polardbxclusters

// Package polardbxclusters provides the unified entrypoint for the logical cluster domain.
//
// Domain boundaries (BFF):
// - Cluster orchestration & operations (scale/upgrade/log-config/alerts-summary/pods)
// - Backups / BackupSchedules / BackupBinlogs (full/incremental backups)
// - Parameters / ParameterTemplates
// - Prechange / Restore / PITR
// - ClusterKnobs (performance tuning switches)
//
// Phase 1 (low risk): provide thin route wrappers forwarding to existing handlers;
// keep paths and request/response formats unchanged.
//
// Related CRDs (api/v1):
// - PolarDBXCluster
// - PolarDBXBackup / PolarDBXBackupSchedule / PolarDBXBackupBinlog
// - PolarDBXParameter / PolarDBXParameterTemplate
//
// Typical alias routes (add aliases, do not change existing routes):
// - /api/v1/crd/polardbxclusters
// - /api/v1/crd/polardbxbackups
// - /api/v1/crd/polardbxbackupschedules
// - /api/v1/crd/polardbxbackupbinlogs
// - /api/v1/crd/polardbxparameters
// - /api/v1/crd/polardbxparametertemplates
//
// Next phases: gradually move orchestration into services; unify K8s access via k8srepo;
// consolidate constants/labels/field names in meta.
