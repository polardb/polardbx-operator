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

/**
 * Helper function to check if a phase represents a completed state
 */
export function isBackupCompleted(phase: PolarDBXBackupPhase | XStoreBackupPhase | string): boolean {
  return phase === PolarDBXBackupPhase.Finished || 
         phase === XStoreBackupPhase.Finished ||
         phase === 'Finished';
}

/**
 * Helper function to check if a phase represents a failed state
 */
export function isBackupFailed(phase: PolarDBXBackupPhase | XStoreBackupPhase | string): boolean {
  return phase === PolarDBXBackupPhase.Failed || 
         phase === XStoreBackupPhase.Failed ||
         phase === 'Failed';
}

/**
 * Helper function to check if a phase represents an in-progress state
 */
export function isBackupInProgress(phase: PolarDBXBackupPhase | XStoreBackupPhase | string): boolean {
  const inProgressPhases = [
    PolarDBXBackupPhase.FullBackuping,
    PolarDBXBackupPhase.BackupCollecting,
    PolarDBXBackupPhase.BackupCalculating,
    PolarDBXBackupPhase.BinlogBackuping,
    PolarDBXBackupPhase.MetadataBackuping,
    XStoreBackupPhase.FullBackuping,
    XStoreBackupPhase.BackupCollecting,
    XStoreBackupPhase.BinlogBackuping,
    XStoreBackupPhase.BinlogWaiting,
    XStoreBackupPhase.MetadataBackuping,
    'FullBackuping',
    'BackupCollecting',
    'BackupCalculating',
    'BinlogBackuping',
    'BinlogWaiting',
    'MetadataBackuping'
  ];
  return inProgressPhases.includes(phase as any);
}

/**
 * Helper function to check if backup is in a terminal state (finished or failed)
 */
export function isBackupTerminal(phase: PolarDBXBackupPhase | XStoreBackupPhase | string): boolean {
  return isBackupCompleted(phase) || isBackupFailed(phase);
}

/**
 * Get display label for backup phase
 */
export function getBackupPhaseLabel(phase: string): string {
  switch (phase) {
    case PolarDBXBackupPhase.New:
    case XStoreBackupPhase.New:
      return '新建';
    case PolarDBXBackupPhase.FullBackuping:
    case XStoreBackupPhase.FullBackuping:
      return '全量备份中';
    case PolarDBXBackupPhase.BackupCollecting:
    case XStoreBackupPhase.BackupCollecting:
      return '收集备份中';
    case PolarDBXBackupPhase.BackupCalculating:
      return '计算备份信息';
    case PolarDBXBackupPhase.BinlogBackuping:
    case XStoreBackupPhase.BinlogBackuping:
      return 'Binlog备份中';
    case XStoreBackupPhase.BinlogWaiting:
      return '等待Binlog备份';
    case PolarDBXBackupPhase.MetadataBackuping:
    case XStoreBackupPhase.MetadataBackuping:
      return '元数据备份中';
    case PolarDBXBackupPhase.Finished:
    case XStoreBackupPhase.Finished:
      return '已完成';
    case PolarDBXBackupPhase.Failed:
    case XStoreBackupPhase.Failed:
      return '失败';
    case PolarDBXBackupPhase.Deleting:
    case XStoreBackupPhase.Deleting:
      return '删除中';
    case BackupBinlogPhase.Running:
      return '运行中';
    case BackupBinlogPhase.CheckExpiredFile:
      return '检查过期文件';
    case PolarDBXBackupPhase.Empty:
    case XStoreBackupPhase.Empty:
    case BackupBinlogPhase.Empty:
      return '未知';
    default:
      return phase || '未知';
  }
}

/**
 * Get color/severity for backup phase
 */
export function getBackupPhaseColor(phase: string): 'success' | 'warning' | 'danger' | 'info' | 'secondary' {
  if (isBackupCompleted(phase)) {
    return 'success';
  }
  if (isBackupFailed(phase)) {
    return 'danger';
  }
  if (isBackupInProgress(phase)) {
    return 'info';
  }
  if (phase === PolarDBXBackupPhase.Deleting || phase === XStoreBackupPhase.Deleting || phase === BackupBinlogPhase.Deleting) {
    return 'warning';
  }
  return 'secondary';
}
