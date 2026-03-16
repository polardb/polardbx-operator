/**
 * Restore job progress calculation strategy
 * Restore process may include: download, decompress, restore, verify, etc.
 */

import { BaseProgressStrategy } from './base-strategy';
import { BackupProgressMetadata } from '../../models/backup-progress.model';

export class RestoreJobStrategy extends BaseProgressStrategy {
  readonly name = 'Restore Job';
  
  canApply(metadata: BackupProgressMetadata): boolean {
    return true;
  }
  
  calculateProgress(metadata: BackupProgressMetadata): number {
    const { phase, subPhase, progress } = metadata;
    
    // Prefer backend-provided progress
    if (progress?.percentage !== undefined) {
      return this.clampProgress(progress.percentage);
    }
    
    // Restore job sub-phase weights
    if (subPhase) {
      return this.calculateRestoreSubPhaseProgress(subPhase, progress);
    }
    
    // Based on main phase
    switch (phase) {
      case 'Pending':
        return 0;
      case 'Running':
        // Calculate based on bytes (restore usually has clear data volume)
        if (progress?.processedBytes && progress?.totalBytes && progress.totalBytes > 0) {
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
  
  /**
   * Calculate restore job sub-phase progress
   */
  private calculateRestoreSubPhaseProgress(subPhase: string, progress?: any): number {
    // Restore job specific sub-phase weights
    const weights: Record<string, number> = {
      'Initializing': 5,      // Initialize
      'Downloading': 30,      // Download backup
      'Decompressing': 10,    // Decompress
      'Restoring': 40,        // Restore data
      'Verifying': 10,        // Verify
      'Finalizing': 5         // Finalize
    };
    
    const weight = weights[subPhase] || 50;
    
    // If detailed progress is available, refine it
    if (progress?.percentage) {
      const previousWeight = this.getPreviousWeight(subPhase, weights);
      const currentWeight = weights[subPhase] || 0;
      return this.clampProgress(previousWeight + (progress.percentage / 100) * currentWeight);
    }
    
    return weight;
  }
  
  /**
   * Get cumulative weight of previous phases
   */
  private getPreviousWeight(currentPhase: string, weights: Record<string, number>): number {
    const order = [
      'Initializing',
      'Downloading',
      'Decompressing',
      'Restoring',
      'Verifying',
      'Finalizing'
    ];
    
    const index = order.indexOf(currentPhase);
    if (index <= 0) return 0;
    
    return order.slice(0, index).reduce((sum, phase) => sum + (weights[phase] || 0), 0);
  }
}
