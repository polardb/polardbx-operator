/**
 * Backup progress calculation utility class
 * Provides unified progress calculation logic
 * Reference: Velero, Kubernetes Job Controller
 */

import {
  BackupPhase,
  BackupSubPhase,
  BackupProgressMetadata,
  ProgressInfo,
  ProgressCalculationOptions,
  ProgressStatus
} from '../models/backup-progress.model';

/**
 * Default phase weights (sum should be 100)
 */
const DEFAULT_PHASE_WEIGHTS: Record<string, number> = {
  // Main phase weights
  'Pending': 0,
  'Running': 50,
  'Completed': 100,
  'Failed': 0,
  'Cancelled': 0,
  'Paused': 0,
  'Unknown': 0,
  
  // Sub-phase weights (allocation within Running phase)
  'Initializing': 5,
  'Validating': 10,
  'Snapshotting': 20,
  'Transferring': 40,
  'Compressing': 10,
  'Encrypting': 5,
  'Verifying': 5,
  'Finalizing': 3,
  'CleaningUp': 2
};

export class BackupProgressCalculator {
  /**
   * Calculate backup progress percentage
   */
  static calculateProgress(
    metadata: BackupProgressMetadata,
    options: ProgressCalculationOptions = {}
  ): number {
    // Prefer directly provided progress information
    if (metadata.progress?.percentage !== undefined) {
      return this.clampProgress(metadata.progress.percentage);
    }

    // Calculate based on bytes
    if (options.useBytes && metadata.progress?.totalBytes) {
      return this.calculateByteProgress(metadata.progress);
    }

    // Calculate based on file count
    if (options.useFiles && metadata.progress?.totalFiles) {
      return this.calculateFileProgress(metadata.progress);
    }

    // Calculate based on phase weights
    if (options.usePhaseWeights !== false) {
      return this.calculatePhaseProgress(metadata, options.phaseWeights);
    }

    // Smart estimation
    if (options.enableSmartEstimation) {
      return this.smartEstimate(metadata);
    }

    // Return default progress
    return options.defaultProgress ?? this.getFallbackProgress(metadata.phase);
  }

  /**
   * Calculate progress based on bytes
   */
  private static calculateByteProgress(progress: ProgressInfo): number {
    const { processedBytes = 0, totalBytes = 0 } = progress;
    if (totalBytes === 0) return 0;
    return this.clampProgress((processedBytes / totalBytes) * 100);
  }

  /**
   * Calculate progress based on file count
   */
  private static calculateFileProgress(progress: ProgressInfo): number {
    const { processedFiles = 0, totalFiles = 0 } = progress;
    if (totalFiles === 0) return 0;
    return this.clampProgress((processedFiles / totalFiles) * 100);
  }

  /**
   * Calculate progress based on phase weights
   */
  private static calculatePhaseProgress(
    metadata: BackupProgressMetadata,
    customWeights?: Record<string, number>
  ): number {
    const weights = customWeights || DEFAULT_PHASE_WEIGHTS;
    
    // Get main phase weight
    const phaseWeight = weights[metadata.phase] ?? 0;
    
    // If there's a sub-phase, calculate progress within the sub-phase
    if (metadata.subPhase && metadata.phase === 'Running') {
      const subPhaseWeight = weights[metadata.subPhase] ?? 0;
      const baseProgress = weights['Initializing'] ?? 0; // Cumulative weight of previous sub-phases
      
      // Calculate sum of weights for all sub-phases before the current one
      const previousSubPhaseWeight = this.getPreviousSubPhaseWeight(metadata.subPhase, weights);
      
      return this.clampProgress(previousSubPhaseWeight + subPhaseWeight / 2);
    }
    
    return this.clampProgress(phaseWeight);
  }

  /**
   * Get the sum of weights for all sub-phases before the current one
   */
  private static getPreviousSubPhaseWeight(
    currentSubPhase: BackupSubPhase,
    weights: Record<string, number>
  ): number {
    const subPhaseOrder: BackupSubPhase[] = [
      'Initializing',
      'Validating',
      'Snapshotting',
      'Transferring',
      'Compressing',
      'Encrypting',
      'Verifying',
      'Finalizing',
      'CleaningUp'
    ];
    
    const currentIndex = subPhaseOrder.indexOf(currentSubPhase);
    if (currentIndex === -1) return 0;
    
    return subPhaseOrder
      .slice(0, currentIndex)
      .reduce((sum, phase) => sum + (weights[phase] ?? 0), 0);
  }

  /**
   * Smart progress estimation
   * Comprehensive judgment based on time, phase, historical data, etc.
   */
  private static smartEstimate(metadata: BackupProgressMetadata): number {
    const { phase, startTime, progress } = metadata;
    
    // If transfer rate is available, estimate based on it
    if (progress?.transferRate && progress?.totalBytes && progress?.processedBytes !== undefined) {
      const remainingBytes = progress.totalBytes - progress.processedBytes;
      const estimatedSeconds = remainingBytes / progress.transferRate;
      const totalSeconds = (progress.totalBytes / progress.transferRate);
      const elapsedSeconds = totalSeconds - estimatedSeconds;
      return this.clampProgress((elapsedSeconds / totalSeconds) * 100);
    }
    
    // Estimate based on running time (rough)
    if (startTime) {
      const elapsed = Date.now() - new Date(startTime).getTime();
      const estimatedTotal = this.estimateTotalDuration(phase);
      if (estimatedTotal > 0) {
        return Math.min(this.clampProgress((elapsed / estimatedTotal) * 100), 95); // Maximum 95%
      }
    }
    
    // Return phase default value
    return this.getFallbackProgress(phase);
  }

  /**
   * Estimate total duration (milliseconds)
   */
  private static estimateTotalDuration(phase: BackupPhase): number {
    // These are empirical values, should be based on historical data in practice
    const estimates: Record<BackupPhase, number> = {
      'Pending': 0,
      'Running': 30 * 60 * 1000, // 30 minutes
      'Completed': 0,
      'Failed': 0,
      'Cancelled': 0,
      'Paused': 0,
      'Unknown': 0
    };
    return estimates[phase] ?? 0;
  }

  /**
   * Get fallback progress value
   */
  private static getFallbackProgress(phase: BackupPhase): number {
    const fallbacks: Record<BackupPhase, number> = {
      'Pending': 0,
      'Running': 50,
      'Completed': 100,
      'Failed': 0,
      'Cancelled': 0,
      'Paused': 0,
      'Unknown': 0
    };
    return fallbacks[phase] ?? 0;
  }

  /**
   * 计算进度状态 (用于UI显示)
   */
  static calculateProgressStatus(metadata: BackupProgressMetadata): ProgressStatus {
    const { phase } = metadata;
    
    switch (phase) {
      case 'Completed':
        return 'success';
      case 'Failed':
      case 'Cancelled':
        return 'exception';
      case 'Running':
        return 'active';
      case 'Pending':
      case 'Paused':
      case 'Unknown':
      default:
        return 'normal';
    }
  }

  /**
   * 格式化进度文本
   */
  static formatProgressText(
    percentage: number,
    metadata: BackupProgressMetadata,
    options: {
      showPhase?: boolean;
      showSubPhase?: boolean;
      showBytes?: boolean;
      showFiles?: boolean;
      showRate?: boolean;
      showTimeRemaining?: boolean;
    } = {}
  ): string {
    const parts: string[] = [];
    
    // Percentage
    parts.push(`${percentage.toFixed(0)}%`);
    
    // Phase information
    if (options.showPhase && metadata.phase) {
      const phaseText = this.getPhaseDisplayText(metadata.phase);
      parts.push(phaseText);
    }
    
    if (options.showSubPhase && metadata.subPhase) {
      const subPhaseText = this.getSubPhaseDisplayText(metadata.subPhase);
      parts.push(subPhaseText);
    }
    
    // Byte information
    if (options.showBytes && metadata.progress?.processedBytes !== undefined) {
      const bytesText = this.formatBytes(metadata.progress.processedBytes, metadata.progress.totalBytes);
      parts.push(bytesText);
    }
    
    // File information
    if (options.showFiles && metadata.progress?.processedFiles !== undefined) {
      const filesText = `${metadata.progress.processedFiles}/${metadata.progress.totalFiles || '?'} files`;
      parts.push(filesText);
    }
    
    // Transfer rate
    if (options.showRate && metadata.progress?.transferRate) {
      const rateText = `${this.formatBytes(metadata.progress.transferRate)}/s`;
      parts.push(rateText);
    }
    
    // Time remaining
    if (options.showTimeRemaining && metadata.progress?.estimatedTimeRemaining) {
      const timeText = this.formatDuration(metadata.progress.estimatedTimeRemaining);
      parts.push(`Remaining ${timeText}`);
    }
    
    return parts.join(' · ');
  }

  /**
   * Get phase display text
   */
  private static getPhaseDisplayText(phase: BackupPhase): string {
    const texts: Record<BackupPhase, string> = {
      'Pending': 'Pending',
      'Running': 'Running',
      'Completed': 'Completed',
      'Failed': 'Failed',
      'Cancelled': 'Cancelled',
      'Paused': 'Paused',
      'Unknown': 'Unknown'
    };
    return texts[phase] ?? phase;
  }

  /**
   * Get sub-phase display text
   */
  private static getSubPhaseDisplayText(subPhase: BackupSubPhase): string {
    const texts: Record<BackupSubPhase, string> = {
      'Initializing': 'Initializing',
      'Validating': 'Validating',
      'Snapshotting': 'Snapshotting',
      'Transferring': 'Transferring',
      'Compressing': 'Compressing',
      'Encrypting': 'Encrypting',
      'Verifying': 'Verifying',
      'Finalizing': 'Finalizing',
      'CleaningUp': 'Cleaning Up'
    };
    return texts[subPhase] ?? subPhase;
  }

  /**
   * Format byte size
   */
  private static formatBytes(bytes: number, total?: number): string {
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let size = bytes;
    let unitIndex = 0;
    
    while (size >= 1024 && unitIndex < units.length - 1) {
      size /= 1024;
      unitIndex++;
    }
    
    const formatted = size.toFixed(2) + ' ' + units[unitIndex];
    
    if (total !== undefined && total > 0) {
      let totalSize = total;
      let totalUnitIndex = 0;
      
      while (totalSize >= 1024 && totalUnitIndex < units.length - 1) {
        totalSize /= 1024;
        totalUnitIndex++;
      }
      
      return `${formatted} / ${totalSize.toFixed(2)} ${units[totalUnitIndex]}`;
    }
    
    return formatted;
  }

  /**
   * Format duration
   */
  private static formatDuration(seconds: number): string {
    if (seconds < 60) {
      return `${Math.round(seconds)}s`;
    } else if (seconds < 3600) {
      const minutes = Math.floor(seconds / 60);
      return `${minutes}m`;
    } else {
      const hours = Math.floor(seconds / 3600);
      const minutes = Math.floor((seconds % 3600) / 60);
      return minutes > 0 ? `${hours}h ${minutes}m` : `${hours}h`;
    }
  }

  /**
   * Clamp progress value to 0-100 range
   */
  private static clampProgress(value: number): number {
    return Math.max(0, Math.min(100, value));
  }

  /**
   * Calculate estimated time remaining
   */
  static calculateEstimatedTimeRemaining(metadata: BackupProgressMetadata): number | undefined {
    const { progress, startTime } = metadata;
    
    if (!progress || !startTime) {
      return undefined;
    }
    
    // Calculate based on transfer rate
    if (progress.transferRate && progress.totalBytes && progress.processedBytes !== undefined) {
      const remainingBytes = progress.totalBytes - progress.processedBytes;
      return remainingBytes / progress.transferRate;
    }
    
    // Estimate based on elapsed time and progress percentage
    if (progress.percentage && progress.percentage > 0) {
      const elapsed = (Date.now() - new Date(startTime).getTime()) / 1000;
      const total = (elapsed / progress.percentage) * 100;
      return total - elapsed;
    }
    
    return undefined;
  }
}
