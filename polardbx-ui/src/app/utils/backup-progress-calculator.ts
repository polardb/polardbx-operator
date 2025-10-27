/**
 * 备份进度计算工具类
 * 提供统一的进度计算逻辑
 * 参考: Velero, Kubernetes Job Controller
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
 * 默认阶段权重 (总和应为100)
 */
const DEFAULT_PHASE_WEIGHTS: Record<string, number> = {
  // 主阶段权重
  'Pending': 0,
  'Running': 50,
  'Completed': 100,
  'Failed': 0,
  'Cancelled': 0,
  'Paused': 0,
  'Unknown': 0,
  
  // 子阶段权重 (在Running阶段内的分配)
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
   * 计算备份进度百分比
   */
  static calculateProgress(
    metadata: BackupProgressMetadata,
    options: ProgressCalculationOptions = {}
  ): number {
    // 如果直接提供了进度信息，优先使用
    if (metadata.progress?.percentage !== undefined) {
      return this.clampProgress(metadata.progress.percentage);
    }

    // 基于字节数计算
    if (options.useBytes && metadata.progress?.totalBytes) {
      return this.calculateByteProgress(metadata.progress);
    }

    // 基于文件数计算
    if (options.useFiles && metadata.progress?.totalFiles) {
      return this.calculateFileProgress(metadata.progress);
    }

    // 基于阶段权重计算
    if (options.usePhaseWeights !== false) {
      return this.calculatePhaseProgress(metadata, options.phaseWeights);
    }

    // 智能估算
    if (options.enableSmartEstimation) {
      return this.smartEstimate(metadata);
    }

    // 返回默认进度
    return options.defaultProgress ?? this.getFallbackProgress(metadata.phase);
  }

  /**
   * 基于字节数计算进度
   */
  private static calculateByteProgress(progress: ProgressInfo): number {
    const { processedBytes = 0, totalBytes = 0 } = progress;
    if (totalBytes === 0) return 0;
    return this.clampProgress((processedBytes / totalBytes) * 100);
  }

  /**
   * 基于文件数计算进度
   */
  private static calculateFileProgress(progress: ProgressInfo): number {
    const { processedFiles = 0, totalFiles = 0 } = progress;
    if (totalFiles === 0) return 0;
    return this.clampProgress((processedFiles / totalFiles) * 100);
  }

  /**
   * 基于阶段权重计算进度
   */
  private static calculatePhaseProgress(
    metadata: BackupProgressMetadata,
    customWeights?: Record<string, number>
  ): number {
    const weights = customWeights || DEFAULT_PHASE_WEIGHTS;
    
    // 获取主阶段权重
    const phaseWeight = weights[metadata.phase] ?? 0;
    
    // 如果有子阶段，计算子阶段内的进度
    if (metadata.subPhase && metadata.phase === 'Running') {
      const subPhaseWeight = weights[metadata.subPhase] ?? 0;
      const baseProgress = weights['Initializing'] ?? 0; // 前面子阶段的累计权重
      
      // 计算当前子阶段之前的所有子阶段权重总和
      const previousSubPhaseWeight = this.getPreviousSubPhaseWeight(metadata.subPhase, weights);
      
      return this.clampProgress(previousSubPhaseWeight + subPhaseWeight / 2);
    }
    
    return this.clampProgress(phaseWeight);
  }

  /**
   * 获取当前子阶段之前的所有子阶段权重总和
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
   * 智能估算进度
   * 基于时间、阶段、历史数据等综合判断
   */
  private static smartEstimate(metadata: BackupProgressMetadata): number {
    const { phase, startTime, progress } = metadata;
    
    // 如果有传输速率，可以估算
    if (progress?.transferRate && progress?.totalBytes && progress?.processedBytes !== undefined) {
      const remainingBytes = progress.totalBytes - progress.processedBytes;
      const estimatedSeconds = remainingBytes / progress.transferRate;
      const totalSeconds = (progress.totalBytes / progress.transferRate);
      const elapsedSeconds = totalSeconds - estimatedSeconds;
      return this.clampProgress((elapsedSeconds / totalSeconds) * 100);
    }
    
    // 基于运行时间估算 (粗略)
    if (startTime) {
      const elapsed = Date.now() - new Date(startTime).getTime();
      const estimatedTotal = this.estimateTotalDuration(phase);
      if (estimatedTotal > 0) {
        return Math.min(this.clampProgress((elapsed / estimatedTotal) * 100), 95); // 最多95%
      }
    }
    
    // 返回阶段默认值
    return this.getFallbackProgress(phase);
  }

  /**
   * 估算总耗时 (毫秒)
   */
  private static estimateTotalDuration(phase: BackupPhase): number {
    // 这些是经验值，实际应该基于历史数据
    const estimates: Record<BackupPhase, number> = {
      'Pending': 0,
      'Running': 30 * 60 * 1000, // 30分钟
      'Completed': 0,
      'Failed': 0,
      'Cancelled': 0,
      'Paused': 0,
      'Unknown': 0
    };
    return estimates[phase] ?? 0;
  }

  /**
   * 获取兜底进度值
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
    
    // 百分比
    parts.push(`${percentage.toFixed(0)}%`);
    
    // 阶段信息
    if (options.showPhase && metadata.phase) {
      const phaseText = this.getPhaseDisplayText(metadata.phase);
      parts.push(phaseText);
    }
    
    if (options.showSubPhase && metadata.subPhase) {
      const subPhaseText = this.getSubPhaseDisplayText(metadata.subPhase);
      parts.push(subPhaseText);
    }
    
    // 字节信息
    if (options.showBytes && metadata.progress?.processedBytes !== undefined) {
      const bytesText = this.formatBytes(metadata.progress.processedBytes, metadata.progress.totalBytes);
      parts.push(bytesText);
    }
    
    // 文件信息
    if (options.showFiles && metadata.progress?.processedFiles !== undefined) {
      const filesText = `${metadata.progress.processedFiles}/${metadata.progress.totalFiles || '?'} 文件`;
      parts.push(filesText);
    }
    
    // 传输速率
    if (options.showRate && metadata.progress?.transferRate) {
      const rateText = `${this.formatBytes(metadata.progress.transferRate)}/s`;
      parts.push(rateText);
    }
    
    // 剩余时间
    if (options.showTimeRemaining && metadata.progress?.estimatedTimeRemaining) {
      const timeText = this.formatDuration(metadata.progress.estimatedTimeRemaining);
      parts.push(`剩余 ${timeText}`);
    }
    
    return parts.join(' · ');
  }

  /**
   * 获取阶段显示文本
   */
  private static getPhaseDisplayText(phase: BackupPhase): string {
    const texts: Record<BackupPhase, string> = {
      'Pending': '等待中',
      'Running': '运行中',
      'Completed': '已完成',
      'Failed': '失败',
      'Cancelled': '已取消',
      'Paused': '已暂停',
      'Unknown': '未知'
    };
    return texts[phase] ?? phase;
  }

  /**
   * 获取子阶段显示文本
   */
  private static getSubPhaseDisplayText(subPhase: BackupSubPhase): string {
    const texts: Record<BackupSubPhase, string> = {
      'Initializing': '初始化',
      'Validating': '验证中',
      'Snapshotting': '快照中',
      'Transferring': '传输中',
      'Compressing': '压缩中',
      'Encrypting': '加密中',
      'Verifying': '校验中',
      'Finalizing': '完成中',
      'CleaningUp': '清理中'
    };
    return texts[subPhase] ?? subPhase;
  }

  /**
   * 格式化字节大小
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
   * 格式化时长
   */
  private static formatDuration(seconds: number): string {
    if (seconds < 60) {
      return `${Math.round(seconds)}秒`;
    } else if (seconds < 3600) {
      const minutes = Math.floor(seconds / 60);
      return `${minutes}分钟`;
    } else {
      const hours = Math.floor(seconds / 3600);
      const minutes = Math.floor((seconds % 3600) / 60);
      return minutes > 0 ? `${hours}小时${minutes}分钟` : `${hours}小时`;
    }
  }

  /**
   * 限制进度范围在 0-100
   */
  private static clampProgress(value: number): number {
    return Math.max(0, Math.min(100, value));
  }

  /**
   * 计算预计剩余时间
   */
  static calculateEstimatedTimeRemaining(metadata: BackupProgressMetadata): number | undefined {
    const { progress, startTime } = metadata;
    
    if (!progress || !startTime) {
      return undefined;
    }
    
    // 基于传输速率计算
    if (progress.transferRate && progress.totalBytes && progress.processedBytes !== undefined) {
      const remainingBytes = progress.totalBytes - progress.processedBytes;
      return remainingBytes / progress.transferRate;
    }
    
    // 基于已用时间和进度百分比估算
    if (progress.percentage && progress.percentage > 0) {
      const elapsed = (Date.now() - new Date(startTime).getTime()) / 1000;
      const total = (elapsed / progress.percentage) * 100;
      return total - elapsed;
    }
    
    return undefined;
  }
}
