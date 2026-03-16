/**
 * Binlog backup progress calculation strategy
 * For continuous binlog backups, may be stream processing
 */

import { BaseProgressStrategy } from './base-strategy';
import { BackupProgressMetadata } from '../../models/backup-progress.model';

export class BinlogBackupStrategy extends BaseProgressStrategy {
  readonly name = 'Binlog Backup';
  
  canApply(metadata: BackupProgressMetadata): boolean {
    return true;
  }
  
  calculateProgress(metadata: BackupProgressMetadata): number {
    const { phase, progress } = metadata;
    
    // Binlog backups may be continuous, progress calculation differs
    if (progress?.percentage !== undefined) {
      return this.clampProgress(progress.percentage);
    }
    
    // Calculate based on bytes (Binlog is usually backed up as byte stream)
    if (progress?.processedBytes && progress?.totalBytes && progress.totalBytes > 0) {
      return this.clampProgress((progress.processedBytes / progress.totalBytes) * 100);
    }
    
    // Based on phase
    switch (phase) {
      case 'Pending':
        return 0;
      case 'Running':
        // Binlog backups may be continuous, show an active state
        return 50;
      case 'Completed':
        return 100;
      case 'Failed':
      case 'Cancelled':
        return 0;
      default:
        return 0;
    }
  }
}
