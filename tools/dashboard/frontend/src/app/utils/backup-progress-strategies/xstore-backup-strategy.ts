/**
 * XStore backup progress calculation strategy
 * Reference backend API: /api/platform/xstore-backups
 */

import { BaseProgressStrategy } from './base-strategy';
import { BackupProgressMetadata } from '../../models/backup-progress.model';

export class XStoreBackupStrategy extends BaseProgressStrategy {
  readonly name = 'XStore Backup';
  
  canApply(metadata: BackupProgressMetadata): boolean {
    // Can determine if this is an XStore backup by identifier in metadata
    return true;
  }
  
  calculateProgress(metadata: BackupProgressMetadata): number {
    const { phase, subPhase, progress } = metadata;
    
    // Prefer backend-provided progress
    if (progress?.percentage !== undefined) {
      return this.clampProgress(progress.percentage);
    }
    
    // XStore backups usually have more detailed sub-phases
    if (subPhase) {
      return this.calculateSubPhaseProgress(subPhase, progress);
    }
    
    // Estimate based on main phase
    switch (phase) {
      case 'Pending':
        return 0;
      case 'Running':
        // Calculate based on bytes or file count
        if (progress?.processedBytes && progress?.totalBytes && progress.totalBytes > 0) {
          return this.clampProgress((progress.processedBytes / progress.totalBytes) * 100);
        }
        if (progress?.processedFiles && progress?.totalFiles && progress.totalFiles > 0) {
          return this.clampProgress((progress.processedFiles / progress.totalFiles) * 100);
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
  
  /**
   * Calculate progress based on sub-phase
   */
  private calculateSubPhaseProgress(subPhase: string, progress?: any): number {
    // XStore-specific sub-phase weights
    const weights: Record<string, number> = {
      'Initializing': 5,
      'Validating': 10,
      'Snapshotting': 25,
      'Transferring': 45,
      'Compressing': 5,
      'Verifying': 5,
      'Finalizing': 5
    };
    
    const weight = weights[subPhase] || 50;
    
    // If there's more detailed progress within the current sub-phase, refine it
    if (progress?.percentage) {
      const previousWeight = this.getPreviousWeight(subPhase, weights);
      const currentWeight = weights[subPhase] || 0;
      return this.clampProgress(previousWeight + (progress.percentage / 100) * currentWeight);
    }
    
    return weight;
  }
  
  /**
   * Get cumulative weight before the current sub-phase
   */
  private getPreviousWeight(currentPhase: string, weights: Record<string, number>): number {
    const order = [
      'Initializing',
      'Validating',
      'Snapshotting',
      'Transferring',
      'Compressing',
      'Verifying',
      'Finalizing'
    ];
    
    const index = order.indexOf(currentPhase);
    if (index <= 0) return 0;
    
    return order.slice(0, index).reduce((sum, phase) => sum + (weights[phase] || 0), 0);
  }
}
