/**
 * Strategy factory: select appropriate progress calculation strategy based on backup type
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
   * Get strategy for specified type
   */
  static getStrategy(type: BackupType): ProgressStrategy {
    const strategy = this.strategies.get(type);
    if (!strategy) {
      // Default to PolarDBX strategy
      return this.strategies.get(BackupType.POLARDBX)!;
    }
    return strategy;
  }
  
  /**
   * Register custom strategy
   */
  static registerStrategy(type: BackupType, strategy: ProgressStrategy): void {
    this.strategies.set(type, strategy);
  }
  
  /**
   * Get all available strategies
   */
  static getAllStrategies(): Map<BackupType, ProgressStrategy> {
    return new Map(this.strategies);
  }
}
