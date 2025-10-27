/**
 * PolarDBX 手动备份进度计算策略
 * 参考后端 API: /api/platform/backups
 */

import { BaseProgressStrategy } from './base-strategy';
import { BackupProgressMetadata } from '../../models/backup-progress.model';

export class PolarDBXBackupStrategy extends BaseProgressStrategy {
  readonly name = 'PolarDBX Backup';
  
  canApply(metadata: BackupProgressMetadata): boolean {
    // 检查是否为 PolarDBX 备份类型
    return true; // 默认策略，总是可用
  }
  
  calculateProgress(metadata: BackupProgressMetadata): number {
    const { phase, progress } = metadata;
    
    // 优先使用后端提供的进度
    if (progress?.percentage !== undefined) {
      return this.clampProgress(progress.percentage);
    }
    
    // 基于阶段的粗略估算
    switch (phase) {
      case 'Pending':
        return 0;
      case 'Running':
        // 运行中默认显示50%，除非有更详细的信息
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
