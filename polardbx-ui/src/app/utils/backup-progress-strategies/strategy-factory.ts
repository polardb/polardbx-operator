/**
 * 策略工厂：根据备份类型选择合适的进度计算策略
 */

import { ProgressStrategy } from './base-strategy';
import { PolarDBXBackupStrategy } from './polardbx-backup-strategy';
import { XStoreBackupStrategy } from './xstore-backup-strategy';
import { BinlogBackupStrategy } from './binlog-backup-strategy';
import { RestoreJobStrategy } from './restore-job-strategy';
import { ScheduledBackupStrategy } from './scheduled-backup-strategy';

export enum BackupType {
  POLARDBX = 'polardbx',
  XSTORE = 'xstore',
  BINLOG = 'binlog',
  RESTORE = 'restore',
  SCHEDULED = 'scheduled'
}

export class ProgressStrategyFactory {
  private static strategies = new Map<BackupType, ProgressStrategy>([
    [BackupType.POLARDBX, new PolarDBXBackupStrategy()],
    [BackupType.XSTORE, new XStoreBackupStrategy()],
    [BackupType.BINLOG, new BinlogBackupStrategy()],
    [BackupType.RESTORE, new RestoreJobStrategy()],
    [BackupType.SCHEDULED, new ScheduledBackupStrategy()]
  ]);
  
  /**
   * 获取指定类型的策略
   */
  static getStrategy(type: BackupType): ProgressStrategy {
    const strategy = this.strategies.get(type);
    if (!strategy) {
      // 默认使用 PolarDBX 策略
      return this.strategies.get(BackupType.POLARDBX)!;
    }
    return strategy;
  }
  
  /**
   * 注册自定义策略
   */
  static registerStrategy(type: BackupType, strategy: ProgressStrategy): void {
    this.strategies.set(type, strategy);
  }
  
  /**
   * 获取所有可用策略
   */
  static getAllStrategies(): Map<BackupType, ProgressStrategy> {
    return new Map(this.strategies);
  }
}
