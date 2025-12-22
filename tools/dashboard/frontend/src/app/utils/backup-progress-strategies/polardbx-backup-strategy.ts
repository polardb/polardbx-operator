/**
 * PolarDBX manual backup progress calculation strategy
 * Reference backend API: /api/platform/backups
 */

import { BaseProgressStrategy } from './base-strategy';
import { BackupProgressMetadata } from '../../models/backup-progress.model';

export class PolarDBXBackupStrategy extends BaseProgressStrategy {
  readonly name = 'PolarDBX Backup';
  
  canApply(metadata: BackupProgressMetadata): boolean {
    // Check if this is a PolarDBX backup type
    return true; // Default strategy, always available
  }
  
  calculateProgress(metadata: BackupProgressMetadata): number {
    const { phase, progress } = metadata;
    
    // Prefer backend-provided progress
    if (progress?.percentage !== undefined) {
      return this.clampProgress(progress.percentage);
    }
    
    // Rough estimation based on phase
    switch (phase) {
      case 'Pending':
        return 0;
      case 'Running':
        // Default to 50% when running, unless more detailed information is available
        if (progress?.processedBytes && progress?.totalBytes) {
          return this.clampProgress((progress.processedBytes / progress.totalBytes) * 100);
        }
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
