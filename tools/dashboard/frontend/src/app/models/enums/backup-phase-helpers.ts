/**
 * Backup Phase Mapping Helpers
 * 
 * Provides backward compatibility between old UI phase values and new backend phases
 */

import { PolarDBXBackupPhase, XStoreBackupPhase } from './backup-phases.enum';

/**
 * Maps backend phase values to UI-friendly status values
 * Provides backward compatibility for components expecting old phase values
 */
export function mapBackupPhaseToUIStatus(
  phase?: PolarDBXBackupPhase | XStoreBackupPhase | string
): 'Running' | 'Completed' | 'Failed' | 'Pending' | 'Deleting' | string {
  if (!phase) return 'Pending';
  
  // Map backend phases to UI statuses
  switch (phase) {
    case PolarDBXBackupPhase.New:
    case XStoreBackupPhase.New:
    case 'New':
      return 'Pending';
      
    case PolarDBXBackupPhase.FullBackuping:
    case PolarDBXBackupPhase.BackupCollecting:
    case PolarDBXBackupPhase.BackupCalculating:
    case PolarDBXBackupPhase.BinlogBackuping:
    case PolarDBXBackupPhase.MetadataBackuping:
    case XStoreBackupPhase.FullBackuping:
    case XStoreBackupPhase.BackupCollecting:
    case XStoreBackupPhase.BinlogBackuping:
    case XStoreBackupPhase.BinlogWaiting:
    case XStoreBackupPhase.MetadataBackuping:
    case 'FullBackuping':
    case 'BackupCollecting':
    case 'BackupCalculating':
    case 'BinlogBackuping':
    case 'BinlogWaiting':
    case 'MetadataBackuping':
      return 'Running';
      
    case PolarDBXBackupPhase.Finished:
    case XStoreBackupPhase.Finished:
    case 'Finished':
      return 'Completed';
      
    case PolarDBXBackupPhase.Failed:
    case XStoreBackupPhase.Failed:
    case 'Failed':
      return 'Failed';
      
    case PolarDBXBackupPhase.Deleting:
    case XStoreBackupPhase.Deleting:
    case 'Deleting':
      return 'Deleting';
      
    case PolarDBXBackupPhase.Empty:
    case XStoreBackupPhase.Empty:
    case '':
      return 'Pending';
      
    default:
      return phase;
  }
}

/**
 * Check if backup is in running state
 */
export function isBackupRunning(phase?: string): boolean {
  return mapBackupPhaseToUIStatus(phase) === 'Running';
}

/**
 * Check if backup is completed
 */
export function isBackupCompletedStatus(phase?: string): boolean {
  return mapBackupPhaseToUIStatus(phase) === 'Completed';
}

/**
 * Check if backup is pending
 */
export function isBackupPending(phase?: string): boolean {
  return mapBackupPhaseToUIStatus(phase) === 'Pending';
}

/**
 * Check if backup has failed
 */
export function isBackupFailedStatus(phase?: string): boolean {
  return mapBackupPhaseToUIStatus(phase) === 'Failed';
}

/**
 * Check if backup can be deleted
 */
export function canDeleteBackup(phase?: string): boolean {
  const status = mapBackupPhaseToUIStatus(phase);
  return status !== 'Running' && status !== 'Deleting';
}

/**
 * Check if backup can be restored from
 */
export function canRestoreFromBackup(phase?: string): boolean {
  const status = mapBackupPhaseToUIStatus(phase);
  return status === 'Completed';
}

/**
 * Get display label for phase (backward compatible)
 */
export function getPhaseDisplayLabel(phase?: string): string {
  const status = mapBackupPhaseToUIStatus(phase);
  
  switch (status) {
    case 'Running': return '运行中';
    case 'Completed': return '已完成';
    case 'Failed': return '失败';
    case 'Pending': return '等待中';
    case 'Deleting': return '删除中';
    default: return phase || '未知';
  }
}

/**
 * Get status color (backward compatible)
 */
export function getPhaseStatusColor(phase?: string): 'success' | 'processing' | 'error' | 'default' | 'warning' {
  const status = mapBackupPhaseToUIStatus(phase);
  
  switch (status) {
    case 'Running': return 'processing';
    case 'Completed': return 'success';
    case 'Failed': return 'error';
    case 'Pending': return 'default';
    case 'Deleting': return 'warning';
    default: return 'default';
  }
}

/**
 * Get status badge style (backward compatible)
 */
export function getPhaseStatusStyle(phase?: string): string {
  const status = mapBackupPhaseToUIStatus(phase);
  
  switch (status) {
    case 'Running': return 'active';
    case 'Completed': return 'success';
    case 'Failed': return 'error';
    case 'Pending': return 'default';
    case 'Deleting': return 'warning';
    default: return 'default';
  }
}
