/**
 * 备份进度计算策略基类
 */

import { BackupProgressMetadata } from '../../models/backup-progress.model';

export interface ProgressStrategy {
  /**
   * 策略名称
   */
  readonly name: string;
  
  /**
   * 计算进度百分比
   */
  calculateProgress(metadata: BackupProgressMetadata): number;
  
  /**
   * 判断是否适用此策略
   */
  canApply(metadata: BackupProgressMetadata): boolean;
}

export abstract class BaseProgressStrategy implements ProgressStrategy {
  abstract readonly name: string;
  
  abstract calculateProgress(metadata: BackupProgressMetadata): number;
  
  abstract canApply(metadata: BackupProgressMetadata): boolean;
  
  /**
   * 限制进度在0-100范围内
   */
  protected clampProgress(value: number): number {
    return Math.max(0, Math.min(100, value));
  }
}
