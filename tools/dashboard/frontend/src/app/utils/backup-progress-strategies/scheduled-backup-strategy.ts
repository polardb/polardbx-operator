/**
 * Scheduled backup task progress calculation strategy
 * Scheduled tasks may include multiple backup cycles
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
    
    // Prefer backend-provided progress
    if (progress?.percentage !== undefined) {
      return this.clampProgress(progress.percentage);
    }
    
    // Scheduled backups usually show progress of current cycle
    switch (phase) {
      case 'Pending':
        return 0;
      case 'Running':
        // If byte count information is available
        if (progress?.processedBytes && progress?.totalBytes && progress.totalBytes > 0) {
          return this.clampProgress((progress.processedBytes / progress.totalBytes) * 100);
        }
        // If file count information is available
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
