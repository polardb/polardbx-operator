/**
 * Binlog 备份进度计算策略
 * 针对连续的binlog备份，可能是流式处理
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
    
    // Binlog 备份可能是持续的，进度计算方式不同
    if (progress?.percentage !== undefined) {
      return this.clampProgress(progress.percentage);
    }
    
    // 基于字节数计算（Binlog通常以字节流方式备份）
    if (progress?.processedBytes && progress?.totalBytes && progress.totalBytes > 0) {
      return this.clampProgress((progress.processedBytes / progress.totalBytes) * 100);
    }
    
    // 基于阶段
    switch (phase) {
      case 'Pending':
        return 0;
      case 'Running':
        // Binlog 备份可能是连续的，显示一个活跃状态
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
