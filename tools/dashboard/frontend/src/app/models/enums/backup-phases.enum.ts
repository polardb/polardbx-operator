/**
 * Backup Phase Enums
 * 
 * These enums are aligned with the backend Go type definitions in api/v1/
 * 
 * @see api/v1/polardbxbackup_types.go
 * @see api/v1/xstorebackup_types.go
 * @see api/v1/polardbxbackupbinlog_types.go
 */

/**
 * PolarDBX Backup Phases
 * Corresponds to PolarDBXBackupPhase in api/v1/polardbxbackup_types.go
 */
export enum PolarDBXBackupPhase {
  /** Initial state when backup is created */
  New = 'New',
  
  /** Performing full backup of data */
  FullBackuping = 'FullBackuping',
  
  /** Collecting backup files from all nodes */
  BackupCollecting = 'BackupCollecting',
  
  /** Calculating backup statistics and metadata */
  BackupCalculating = 'BackupCalculating',
  
  /** Backing up binlog files */
  BinlogBackuping = 'BinlogBackuping',
  
  /** Backing up metadata */
  MetadataBackuping = 'MetadataBackuping',
  
  /** Backup completed successfully */
  Finished = 'Finished',
  
  /** Backup failed */
  Failed = 'Failed',
  
  /** Backup is being deleted */
  Deleting = 'Deleting',
  
  /** Empty/unset phase */
  Empty = ''
}

/**
 * XStore Backup Phases
 * Corresponds to XStoreBackupPhase in api/v1/xstorebackup_types.go
 */
export enum XStoreBackupPhase {
  /** Initial state when backup is created */
  New = 'New',
  
  /** Performing full backup of XStore data */
  FullBackuping = 'FullBackuping',
  
  /** Collecting backup files */
  BackupCollecting = 'BackupCollecting',
  
  /** Backing up binlog files */
  BinlogBackuping = 'BinlogBackuping',
  
  /** Waiting for binlog backup completion */
  BinlogWaiting = 'BinlogWaiting',
  
  /** Backing up metadata */
  MetadataBackuping = 'MetadataBackuping',
  
  /** Backup completed successfully */
  Finished = 'Finished',
  
  /** Backup failed */
  Failed = 'Failed',
  
  /** Backup is being deleted */
  Deleting = 'Deleting',
  
  /** Empty/unset phase */
  Empty = ''
}

/**
 * Backup Binlog Phases
 * Note: Backend type is plain string in api/v1/polardbxbackupbinlog_types.go
 * These are the observed values in practice
 */
export enum BackupBinlogPhase {
  /** Empty/initial state */
  Empty = '',
  
  /** Binlog backup is running normally */
  Running = 'running',
  
  /** Checking for expired binlog files */
  CheckExpiredFile = 'checkExpiredFile',
  
  /** Deleting expired binlog files */
  Deleting = 'deleting'
}

type BackupPhaseCategory = 'pending' | 'in-progress' | 'completed' | 'failed' | 'deleting' | 'maintenance' | 'unknown';

interface BackupPhaseMeta {
  label: string;
  color: 'success' | 'warning' | 'danger' | 'info' | 'secondary';
  category: BackupPhaseCategory;
}

const BACKUP_PHASE_META: Record<string, BackupPhaseMeta> = {
  [PolarDBXBackupPhase.New]: { label: '新建', color: 'secondary', category: 'pending' },
  [PolarDBXBackupPhase.FullBackuping]: { label: '全量备份中', color: 'info', category: 'in-progress' },
  [PolarDBXBackupPhase.BackupCollecting]: { label: '收集备份中', color: 'info', category: 'in-progress' },
  [PolarDBXBackupPhase.BackupCalculating]: { label: '计算备份信息', color: 'info', category: 'in-progress' },
  [PolarDBXBackupPhase.BinlogBackuping]: { label: 'Binlog备份中', color: 'info', category: 'in-progress' },
  [PolarDBXBackupPhase.MetadataBackuping]: { label: '元数据备份中', color: 'info', category: 'in-progress' },
  [XStoreBackupPhase.BinlogWaiting]: { label: '等待Binlog备份', color: 'info', category: 'in-progress' },
  [PolarDBXBackupPhase.Finished]: { label: '已完成', color: 'success', category: 'completed' },
  [PolarDBXBackupPhase.Failed]: { label: '失败', color: 'danger', category: 'failed' },
  [PolarDBXBackupPhase.Deleting]: { label: '删除中', color: 'warning', category: 'deleting' },
  [BackupBinlogPhase.Running]: { label: '运行中', color: 'info', category: 'maintenance' },
  [BackupBinlogPhase.CheckExpiredFile]: { label: '检查过期文件', color: 'info', category: 'maintenance' },
  [BackupBinlogPhase.Deleting]: { label: '删除中', color: 'warning', category: 'deleting' },
  [PolarDBXBackupPhase.Empty]: { label: '未知', color: 'secondary', category: 'unknown' }
};

function getBackupPhaseMeta(
  phase: PolarDBXBackupPhase | XStoreBackupPhase | BackupBinlogPhase | string
): BackupPhaseMeta | undefined {
  return BACKUP_PHASE_META[phase];
}

/**
 * Helper function to check if a phase represents a completed state
 */
export function isBackupCompleted(phase: PolarDBXBackupPhase | XStoreBackupPhase | string): boolean {
  const meta = getBackupPhaseMeta(phase);
  if (meta) {
    return meta.category === 'completed';
  }
  return phase === PolarDBXBackupPhase.Finished || phase === 'Finished';
}

/**
 * Helper function to check if a phase represents a failed state
 */
export function isBackupFailed(phase: PolarDBXBackupPhase | XStoreBackupPhase | string): boolean {
  const meta = getBackupPhaseMeta(phase);
  if (meta) {
    return meta.category === 'failed';
  }
  return phase === PolarDBXBackupPhase.Failed || phase === 'Failed';
}

/**
 * Helper function to check if a phase represents an in-progress state
 */
export function isBackupInProgress(phase: PolarDBXBackupPhase | XStoreBackupPhase | string): boolean {
  const meta = getBackupPhaseMeta(phase);
  if (meta) {
    return meta.category === 'in-progress';
  }
  return false;
}

/**
 * Helper function to check if backup is in a terminal state (finished or failed)
 */
export function isBackupTerminal(phase: PolarDBXBackupPhase | XStoreBackupPhase | string): boolean {
  const meta = getBackupPhaseMeta(phase);
  if (meta) {
    return meta.category === 'completed' || meta.category === 'failed';
  }
  return isBackupCompleted(phase) || isBackupFailed(phase);
}

/**
 * Get display label for backup phase
 */
export function getBackupPhaseLabel(phase: string): string {
  const meta = getBackupPhaseMeta(phase);
  if (meta) {
    return meta.label;
  }
  return phase || '未知';
}

/**
 * Get color/severity for backup phase
 */
export function getBackupPhaseColor(phase: string): 'success' | 'warning' | 'danger' | 'info' | 'secondary' {
  const meta = getBackupPhaseMeta(phase);
  if (meta) {
    return meta.color;
  }
  return 'secondary';
}
