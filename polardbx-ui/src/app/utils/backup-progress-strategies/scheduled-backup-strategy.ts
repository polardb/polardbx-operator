/**
 * 定时备份任务进度计算策略
 * 定时任务可能包含多个备份周期
 */

import { BaseProgressStrategy } from './base-strategy';
import { BackupProgressMetadata } from '../../models/backup-progress.model';

export class ScheduledBackupStrategy extends BaseProgressStrategy {
  readonly name = 'Scheduled Backup';
  
  canApply(metadata: BackupProgressMetadata): boolean {
    return true;
  }
  
  calculateProgress(metadata: BackupProgressMetadata): number {
    const { phase, progress } = metadata;
    
    // 优先使用后端提供的进度
    if (progress?.percentage !== undefined) {
      return this.clampProgress(progress.percentage);
    }
    
    // 定时备份通常显示当前周期的进度
    switch (phase) {
      case 'Pending':
        return 0;
      case 'Running':
        // 如果有字节数信息
        if (progress?.processedBytes && progress?.totalBytes && progress.totalBytes > 0) {
          return this.clampProgress((progress.processedBytes / progress.totalBytes) * 100);
        }
        // 如果有文件数信息
        if (progress?.processedFiles && progress?.totalFiles && progress.totalFiles > 0) {
          return this.clampProgress((progress.processedFiles / progress.totalFiles) * 100);
        }
        return 50;
      case 'Completed':
        return 100;
      case 'Failed':
      case 'Cancelled':
      case 'Paused':
        return 0;
      default:
        return 0;
    }
  }
}
