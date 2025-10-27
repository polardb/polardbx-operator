/**
 * 恢复任务进度计算策略
 * 恢复过程可能包括：下载、解压、恢复、验证等步骤
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
    
    // 优先使用后端提供的进度
    if (progress?.percentage !== undefined) {
      return this.clampProgress(progress.percentage);
    }
    
    // 恢复任务的子阶段权重
    if (subPhase) {
      return this.calculateRestoreSubPhaseProgress(subPhase, progress);
    }
    
    // 基于主阶段
    switch (phase) {
      case 'Pending':
        return 0;
      case 'Running':
        // 根据字节数计算（恢复时通常有明确的数据量）
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
   * 计算恢复任务子阶段进度
   */
  private calculateRestoreSubPhaseProgress(subPhase: string, progress?: any): number {
    // 恢复任务特定的子阶段权重
    const weights: Record<string, number> = {
      'Initializing': 5,      // 初始化
      'Downloading': 30,      // 下载备份
      'Decompressing': 10,    // 解压
      'Restoring': 40,        // 恢复数据
      'Verifying': 10,        // 验证
      'Finalizing': 5         // 完成
    };
    
    const weight = weights[subPhase] || 50;
    
    // 如果有详细进度，则细化
    if (progress?.percentage) {
      const previousWeight = this.getPreviousWeight(subPhase, weights);
      const currentWeight = weights[subPhase] || 0;
      return this.clampProgress(previousWeight + (progress.percentage / 100) * currentWeight);
    }
    
    return weight;
  }
  
  /**
   * 获取前置阶段累计权重
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
