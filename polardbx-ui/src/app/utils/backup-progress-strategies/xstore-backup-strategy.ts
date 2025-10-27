/**
 * XStore 备份进度计算策略
 * 参考后端 API: /api/platform/xstore-backups
 */

import { BaseProgressStrategy } from './base-strategy';
import { BackupProgressMetadata } from '../../models/backup-progress.model';

export class XStoreBackupStrategy extends BaseProgressStrategy {
  readonly name = 'XStore Backup';
  
  canApply(metadata: BackupProgressMetadata): boolean {
    // 可以通过元数据中的标识判断是否为 XStore 备份
    return true;
  }
  
  calculateProgress(metadata: BackupProgressMetadata): number {
    const { phase, subPhase, progress } = metadata;
    
    // 优先使用后端提供的进度
    if (progress?.percentage !== undefined) {
      return this.clampProgress(progress.percentage);
    }
    
    // XStore 备份通常有更详细的子阶段
    if (subPhase) {
      return this.calculateSubPhaseProgress(subPhase, progress);
    }
    
    // 基于主阶段估算
    switch (phase) {
      case 'Pending':
        return 0;
      case 'Running':
        // 根据字节数或文件数计算
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
   * 基于子阶段计算进度
   */
  private calculateSubPhaseProgress(subPhase: string, progress?: any): number {
    // XStore 特定的子阶段权重
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
    
    // 如果在当前子阶段内有更详细的进度，则细化
    if (progress?.percentage) {
      const previousWeight = this.getPreviousWeight(subPhase, weights);
      const currentWeight = weights[subPhase] || 0;
      return this.clampProgress(previousWeight + (progress.percentage / 100) * currentWeight);
    }
    
    return weight;
  }
  
  /**
   * 获取当前子阶段之前的累计权重
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
