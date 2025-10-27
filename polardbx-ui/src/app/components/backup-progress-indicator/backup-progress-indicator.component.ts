import { Component, Input, OnInit, OnChanges, SimpleChanges, ChangeDetectionStrategy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { NzProgressModule } from 'ng-zorro-antd/progress';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzIconModule } from 'ng-zorro-antd/icon';

import { 
  BackupProgressMetadata, 
  ProgressDisplayOptions, 
  ProgressStatus 
} from '../../models/backup-progress.model';
import { 
  BackupProgressCalculator 
} from '../../utils/backup-progress-calculator';
import { 
  ProgressStrategyFactory, 
  BackupType 
} from '../../utils/backup-progress-strategies';

/**
 * 统一备份进度指示器组件
 * 
 * 特性：
 * - 支持多种备份类型（PolarDBX, XStore, Binlog, Restore, Scheduled）
 * - 自动选择合适的进度计算策略
 * - 显示进度百分比、阶段文本、时间估算、数据大小、传输速率
 * - 支持多种显示模式（line, circle, dashboard）
 * 
 * 参考设计：
 * - Kubernetes Dashboard 的 Job 进度显示
 * - GitLab CI/CD 的管道进度
 * - Velero 备份进度展示
 * - AWS Backup 控制台
 * 
 * 使用示例：
 * ```html
 * <app-backup-progress-indicator
 *   [metadata]="backupMetadata"
 *   [type]="'xstore'"
 *   [size]="'small'"
 *   [showDetails]="true">
 * </app-backup-progress-indicator>
 * ```
 */
@Component({
  selector: 'app-backup-progress-indicator',
  standalone: true,
  imports: [
    CommonModule,
    NzProgressModule,
    NzToolTipModule,
    NzIconModule
  ],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="backup-progress-indicator" [class.compact]="size === 'small'">
      <!-- 进度条 -->
      <nz-progress
        [nzType]="displayOptions.mode || 'line'"
        [nzPercent]="progressPercentage"
        [nzStatus]="progressStatus"
        [nzSize]="size"
        [nzShowInfo]="displayOptions.showPercentage !== false"
        [nzFormat]="customFormat"
        [nzStrokeColor]="displayOptions.color">
      </nz-progress>

      <!-- 详细信息 -->
      <div *ngIf="showDetails && metadata" class="progress-details">
        <!-- 阶段信息 -->
        <div *ngIf="metadata.phase || metadata.subPhase" class="phase-info">
          <span class="phase-label">
            <i nz-icon [nzType]="getPhaseIcon()" [nzTheme]="'outline'"></i>
            {{ getPhaseText() }}
          </span>
          <span *ngIf="metadata.subPhase" class="sub-phase-label">
            {{ getSubPhaseText() }}
          </span>
        </div>

        <!-- 传输信息 -->
        <div *ngIf="displayOptions.showByteCount && metadata.progress?.processedBytes !== undefined" 
             class="transfer-info">
          <ng-container *ngIf="metadata.progress as progress">
            <span class="bytes-info">
              {{ formatBytes(progress.processedBytes!, progress.totalBytes) }}
            </span>
            <span *ngIf="displayOptions.showRate && progress.transferRate" 
                  class="rate-info">
              {{ formatBytes(progress.transferRate) }}/s
            </span>
          </ng-container>
        </div>

        <!-- 文件计数 -->
        <div *ngIf="displayOptions.showFileCount && metadata.progress?.processedFiles !== undefined"
             class="file-count-info">
          <ng-container *ngIf="metadata.progress as progress">
            <i nz-icon nzType="file" nzTheme="outline"></i>
            {{ progress.processedFiles }}/{{ progress.totalFiles || '?' }} 文件
          </ng-container>
        </div>

        <!-- 剩余时间 -->
        <div *ngIf="displayOptions.showTimeRemaining && estimatedTimeRemaining"
             class="time-remaining-info">
          <i nz-icon nzType="clock-circle" nzTheme="outline"></i>
          剩余 {{ formatDuration(estimatedTimeRemaining) }}
        </div>

        <!-- 错误信息 -->
        <div *ngIf="metadata.errorMessage" class="error-info">
          <i nz-icon nzType="exclamation-circle" nzTheme="fill"></i>
          {{ metadata.errorMessage }}
        </div>

        <!-- 警告信息 -->
        <div *ngIf="metadata.warnings && metadata.warnings.length > 0" class="warning-info">
          <i nz-icon nzType="warning" nzTheme="fill"></i>
          {{ metadata.warnings[0] }}
          <span *ngIf="metadata.warnings.length > 1" class="more-warnings">
            (+{{ metadata.warnings.length - 1 }} more)
          </span>
        </div>
      </div>

      <!-- Tooltip 提示 -->
      <div *ngIf="tooltipContent" class="progress-tooltip">
        <i nz-icon 
           nzType="info-circle" 
           nzTheme="outline"
           [nz-tooltip]="tooltipContent">
        </i>
      </div>
    </div>
  `,
  styles: [`
    .backup-progress-indicator {
      display: flex;
      flex-direction: column;
      gap: 8px;
    }

    .backup-progress-indicator.compact {
      gap: 4px;
    }

    .progress-details {
      display: flex;
      flex-direction: column;
      gap: 6px;
      font-size: 12px;
      color: rgba(0, 0, 0, 0.65);
    }

    .phase-info {
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .phase-label {
      display: flex;
      align-items: center;
      gap: 4px;
      font-weight: 500;
      color: rgba(0, 0, 0, 0.85);
    }

    .sub-phase-label {
      color: rgba(0, 0, 0, 0.45);
      font-size: 11px;
    }

    .transfer-info,
    .file-count-info,
    .time-remaining-info {
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .bytes-info,
    .rate-info {
      font-family: 'Consolas', 'Monaco', monospace;
    }

    .rate-info {
      color: #1890ff;
    }

    .error-info {
      display: flex;
      align-items: center;
      gap: 4px;
      color: #ff4d4f;
      font-size: 12px;
    }

    .warning-info {
      display: flex;
      align-items: center;
      gap: 4px;
      color: #faad14;
      font-size: 12px;
    }

    .more-warnings {
      color: rgba(0, 0, 0, 0.45);
      font-size: 11px;
    }

    .progress-tooltip {
      position: absolute;
      top: 0;
      right: 0;
      cursor: help;
    }
  `]
})
export class BackupProgressIndicatorComponent implements OnInit, OnChanges {
  /** 进度元数据 */
  @Input() metadata!: BackupProgressMetadata;
  
  /** 备份类型 */
  @Input() type: BackupType = BackupType.POLARDBX;
  
  /** 尺寸 */
  @Input() size: 'small' | 'default' = 'default';
  
  /** 是否显示详细信息 */
  @Input() showDetails = false;
  
  /** 显示选项 */
  @Input() displayOptions: ProgressDisplayOptions = {};
  
  /** Tooltip 内容 */
  @Input() tooltipContent?: string;
  
  /** 计算后的进度百分比 */
  progressPercentage = 0;
  
  /** 进度状态 */
  progressStatus: ProgressStatus = 'normal';
  
  /** 预计剩余时间（秒） */
  estimatedTimeRemaining?: number;
  
  ngOnInit(): void {
    this.updateProgress();
  }
  
  ngOnChanges(changes: SimpleChanges): void {
    if (changes['metadata'] || changes['type']) {
      this.updateProgress();
    }
  }
  
  /**
   * 更新进度
   */
  private updateProgress(): void {
    if (!this.metadata) {
      this.progressPercentage = 0;
      this.progressStatus = 'normal';
      return;
    }
    
    // 使用策略计算进度
    const strategy = ProgressStrategyFactory.getStrategy(this.type);
    this.progressPercentage = strategy.calculateProgress(this.metadata);
    
    // 计算进度状态
    this.progressStatus = BackupProgressCalculator.calculateProgressStatus(this.metadata);
    
    // 计算预计剩余时间
    this.estimatedTimeRemaining = BackupProgressCalculator.calculateEstimatedTimeRemaining(this.metadata);
  }
  
  /**
   * 自定义进度格式化
   */
  customFormat = (percent: number): string => {
    if (this.displayOptions.customFormat) {
      return this.displayOptions.customFormat(percent, this.metadata);
    }
    
    if (percent === 100) {
      return '完成';
    }
    
    return `${percent.toFixed(0)}%`;
  };
  
  /**
   * 获取阶段图标
   */
  getPhaseIcon(): string {
    const phase = this.metadata?.phase;
    const icons: Record<string, string> = {
      'Pending': 'clock-circle',
      'Running': 'loading',
      'Completed': 'check-circle',
      'Failed': 'close-circle',
      'Cancelled': 'stop',
      'Paused': 'pause-circle',
      'Unknown': 'question-circle'
    };
    return icons[phase || ''] || 'info-circle';
  }
  
  /**
   * 获取阶段文本
   */
  getPhaseText(): string {
    return BackupProgressCalculator.formatProgressText(
      this.progressPercentage,
      this.metadata,
      { showPhase: true }
    );
  }
  
  /**
   * 获取子阶段文本
   */
  getSubPhaseText(): string {
    return BackupProgressCalculator.formatProgressText(
      this.progressPercentage,
      this.metadata,
      { showSubPhase: true }
    );
  }
  
  /**
   * 格式化字节
   */
  formatBytes(bytes: number, total?: number): string {
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
  formatDuration(seconds: number): string {
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
}
