import { Component, OnDestroy, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzTabsModule } from 'ng-zorro-antd/tabs';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzDatePickerModule } from 'ng-zorro-antd/date-picker';
import { NzTimePickerModule } from 'ng-zorro-antd/time-picker';
import { NzStepsModule } from 'ng-zorro-antd/steps';
import { NzResultModule } from 'ng-zorro-antd/result';
import { NzMessageService } from 'ng-zorro-antd/message';
import { FormsModule } from '@angular/forms';
import { LoadingKeys } from '../../services/loading.service';
import { PITRRequest } from '../../models/restore.model';
import { ApiService } from '../../services/api.service';
import { LoadingService } from '../../services/loading.service';
import { Subscription, interval, of } from 'rxjs';
import { catchError, startWith, switchMap } from 'rxjs/operators';

@Component({
  selector: 'app-pitr-management',
  standalone: true,
  imports: [
    CommonModule,
    NzCardModule,
    NzButtonModule,
    NzIconModule,
    NzTableModule,
    NzTabsModule,
    NzFormModule,
    NzInputModule,
    NzSelectModule,
    NzToolTipModule,
    NzGridModule,
    NzAlertModule,
    NzSpinModule,
    NzDatePickerModule,
    NzTimePickerModule,
    NzStepsModule,
    NzResultModule,
    FormsModule
  ],
  template: `
    <div class="pitr-management">
      <div class="page-header">
        <div class="header-content">
          <h1 class="page-title">
            <i nz-icon nzType="history" class="page-icon"></i>
            时间点恢复 (PITR)
          </h1>
          <p class="page-description">精确到秒级的时间点恢复管理，支持恢复到任意历史时刻</p>
        </div>
      </div>

      <div class="page-content">
        <nz-tabset [nzTabPosition]="'top'" [nzType]="'card'">
          <!-- PITR 配置标签页 -->
          <nz-tab nzTitle="PITR 配置">
            <ng-template nz-tab>
              <i nz-icon nzType="setting" style="margin-right: 4px;"></i>
              PITR 配置
            </ng-template>
            
            <nz-card class="config-card" nzTitle="恢复参数配置" [nzExtra]="configExtra">
              <ng-template #configExtra>
                <i nz-icon nzType="tool" class="section-icon"></i>
              </ng-template>
              
              <nz-alert 
                nzType="info" 
                nzMessage="时间点恢复说明" 
                nzDescription="PITR 允许您将数据库恢复到任意指定的时间点，基于连续的日志流进行精确恢复。请确保目标时间点在可恢复范围内。"
                nzShowIcon
                [nzCloseable]="true"
                class="info-alert">
              </nz-alert>

              <form class="pitr-form">
                <nz-row [nzGutter]="16">
                  <nz-col [nzSpan]="12">
                    <nz-form-item>
                      <nz-form-label [nzSpan]="6" nzRequired>命名空间</nz-form-label>
                      <nz-form-control [nzSpan]="18">
                        <input nz-input [(ngModel)]="namespace" name="namespace" placeholder="集群所在的命名空间" />
                      </nz-form-control>
                    </nz-form-item>
                  </nz-col>
                  <nz-col [nzSpan]="12">
                    <nz-form-item>
                      <nz-form-label [nzSpan]="6" nzRequired>集群名称</nz-form-label>
                      <nz-form-control [nzSpan]="18">
                        <input nz-input [(ngModel)]="clusterName" name="cluster" placeholder="要恢复的PolarDB-X集群" />
                      </nz-form-control>
                    </nz-form-item>
                  </nz-col>
                </nz-row>
                
                <nz-row [nzGutter]="16">
                  <nz-col [nzSpan]="12">
                    <nz-form-item>
                      <nz-form-label [nzSpan]="6" nzRequired>恢复时间</nz-form-label>
                      <nz-form-control [nzSpan]="18">
                        <nz-date-picker 
                          [(ngModel)]="selectedDate" 
                          name="date"
                          nzShowTime 
                          nzFormat="yyyy-MM-dd HH:mm:ss"
                          nzPlaceHolder="选择恢复时间点"
                          (ngModelChange)="onDateChange()"
                          style="width: 100%;">
                        </nz-date-picker>
                      </nz-form-control>
                    </nz-form-item>
                  </nz-col>
                  <nz-col [nzSpan]="12">
                    <nz-form-item>
                      <nz-form-label [nzSpan]="6">时区</nz-form-label>
                      <nz-form-control [nzSpan]="18">
                        <nz-select [(ngModel)]="timezone" name="tz" nzPlaceHolder="选择时区" (ngModelChange)="onTimezoneChange()">
                          <nz-option nzValue="UTC" nzLabel="UTC (协调世界时)"></nz-option>
                          <nz-option nzValue="Asia/Shanghai" nzLabel="Asia/Shanghai (北京时间)"></nz-option>
                          <nz-option nzValue="America/New_York" nzLabel="America/New_York (纽约时间)"></nz-option>
                          <nz-option nzValue="Europe/London" nzLabel="Europe/London (伦敦时间)"></nz-option>
                        </nz-select>
                      </nz-form-control>
                    </nz-form-item>
                  </nz-col>
                </nz-row>

                <nz-row [nzGutter]="16" *ngIf="pitrTime">
                  <nz-col [nzSpan]="24">
                    <nz-form-item>
                      <nz-form-label [nzSpan]="3">时间字符串</nz-form-label>
                      <nz-form-control [nzSpan]="21">
                        <input nz-input [value]="pitrTime" [nz-tooltip]="'将使用 yyyy-MM-ddTHH:mm:ssZ 格式时间（按所选时区解释）'" readonly />
                      </nz-form-control>
                    </nz-form-item>
                  </nz-col>
                </nz-row>
              </form>

              <div class="action-buttons">
                <button nz-button nzType="default" (click)="reset()">
                  <i nz-icon nzType="reload"></i>
                  重置表单
                </button>
                <button nz-button nzType="default" (click)="previewConfig()">
                  <i nz-icon nzType="eye"></i>
                  预览配置
                </button>
                <button nz-button nzType="primary" nzDanger (click)="startPITR()" 
                        [nzLoading]="loadingService.isLoading(loadingKeys.CLUSTER_PITR)" 
                        [disabled]="!isFormValid()">
                  <i nz-icon nzType="play-circle"></i>
                  执行 PITR 恢复
                </button>
              </div>
            </nz-card>
          </nz-tab>

          <!-- 恢复进度标签页 -->
          <nz-tab nzTitle="恢复进度">
            <ng-template nz-tab>
              <i nz-icon nzType="clock-circle" style="margin-right: 4px;"></i>
              恢复进度
            </ng-template>
            
            <nz-card class="progress-card" nzTitle="恢复状态监控" [nzExtra]="progressExtra">
              <ng-template #progressExtra>
                <i nz-icon nzType="dashboard" class="section-icon"></i>
              </ng-template>
              
              <nz-steps [nzCurrent]="currentStep" [nzStatus]="stepStatus" class="recovery-steps">
                <nz-step nzTitle="验证参数" nzDescription="验证恢复时间点和集群状态"></nz-step>
                <nz-step nzTitle="准备恢复" nzDescription="初始化恢复环境和资源"></nz-step>
                <nz-step nzTitle="数据恢复" nzDescription="从日志流恢复到指定时间点"></nz-step>
                <nz-step nzTitle="验证完成" nzDescription="验证恢复结果和数据一致性"></nz-step>
              </nz-steps>

              <div class="progress-info">
                <nz-alert 
                  *ngIf="!hasActiveRecovery"
                  nzType="info" 
                  nzMessage="暂无活动的恢复任务" 
                  nzDescription="请在 PITR 配置标签页发起新的恢复任务。"
                  nzShowIcon>
                </nz-alert>
                
                <div *ngIf="hasActiveRecovery" class="recovery-details">
                  <h4>当前恢复任务</h4>
                  <p><strong>集群:</strong> {{ clusterName || '-' }}</p>
                  <p><strong>目标集群:</strong> {{ targetClusterName || '-' }}</p>
                  <p><strong>目标时间:</strong> {{ pitrTime || '-' }}</p>
                  <p><strong>状态:</strong> {{ getStepStatusText() }}</p>
                  <p *ngIf="statusMessage"><strong>详情:</strong> {{ statusMessage }}</p>
                </div>
              </div>
            </nz-card>
          </nz-tab>

          <!-- 恢复历史标签页 -->
          <nz-tab nzTitle="恢复历史">
            <ng-template nz-tab>
              <i nz-icon nzType="history" style="margin-right: 4px;"></i>
              恢复历史
            </ng-template>
            
            <nz-card class="history-card" nzTitle="PITR 恢复历史" [nzExtra]="historyExtra">
              <ng-template #historyExtra>
                <button nz-button nzType="default" (click)="loadHistory()">
                  <i nz-icon nzType="reload"></i>
                  刷新列表
                </button>
              </ng-template>
              
              <nz-result 
                nzIcon="file-search"
                nzTitle="恢复历史功能开发中"
                nzSubTitle="PITR 恢复历史记录和审计功能正在完善中，敬请期待。">
                <div nz-result-extra>
                  <button nz-button nzType="primary" (click)="loadHistory()">
                    <i nz-icon nzType="reload"></i>
                    重新加载
                  </button>
                </div>
              </nz-result>
            </nz-card>
          </nz-tab>
        </nz-tabset>
              </div>
    </div>
  `,
  styles: [`
    .pitr-management {
      padding: 16px;
      background: #ffffff;
    }
    
    .page-header {
      margin-bottom: 16px;
    }
    
    .header-content {
      max-width: 1120px;
      margin: 0 auto;
    }
    
    .page-title {
      color: rgba(0, 0, 0, 0.87);
      font-size: 18px;
      font-weight: 500;
      margin: 0 0 4px 0;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    
    .page-icon {
      font-size: 20px;
      color: #1890ff;
    }
    
    .page-description {
      color: rgba(0, 0, 0, 0.6);
      font-size: 14px;
      margin: 0;
      line-height: 1.5;
    }
    
    .page-content {
      max-width: 1120px;
      margin: 0 auto;
    }
    
    .config-card, .progress-card, .history-card {
      background: #fff;
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border: 1px solid #e0e0e0;
      margin-bottom: 16px;
    }
    
    .section-icon {
      font-size: 16px;
      color: #1890ff;
    }
    
    .info-alert {
      margin-bottom: 24px;
    }
    
    .pitr-form {
      margin-bottom: 24px;
    }
    
    .action-buttons {
      display: flex;
      justify-content: flex-end;
      gap: 12px;
      padding-top: 16px;
      border-top: 1px solid #e0e0e0;
    }
    
    .recovery-steps {
      margin-bottom: 24px;
    }
    
    .progress-info {
      margin-top: 24px;
    }
    
    .recovery-details {
      background: #fafafa;
      padding: 16px;
      border-radius: 6px;
      border: 1px solid #e0e0e0;
    }
    
    .recovery-details h4 {
      margin: 0 0 12px 0;
      color: rgba(0, 0, 0, 0.85);
      font-size: 14px;
      font-weight: 500;
    }
    
    .recovery-details p {
      margin: 4px 0;
      color: rgba(0, 0, 0, 0.65);
      font-size: 13px;
    }
    
    /* 响应式设计 */
    @media (max-width: 1200px) {
      .page-content {
        max-width: 100%;
        padding: 0 8px;
      }
    }
    
    @media (max-width: 768px) {
      .pitr-management {
        padding: 8px;
      }
      
      .action-buttons {
        flex-direction: column;
      }
    }
  `]
})
export class PITRManagementComponent implements OnInit, OnDestroy {
  
  loadingKeys = LoadingKeys;
  namespace = 'default';
  clusterName = '';
  pitrTime = '';
  timezone = 'UTC';
  selectedDate: Date | null = null;
  targetClusterName = '';
  
  // Progress tracking
  currentStep = 0;
  stepStatus: 'wait' | 'process' | 'finish' | 'error' = 'wait';
  hasActiveRecovery = false;
  statusMessage = '';
  private pollSub?: Subscription;
  
  constructor(
    private apiService: ApiService,
    public loadingService: LoadingService,
    private messageService: NzMessageService
  ) {}

  ngOnInit(): void {
    // Component initialization
  }

  ngOnDestroy(): void {
    this.stopPolling();
  }

  private formatRestoreTime(date: Date, timezone: string): string {
    const tz = String(timezone || 'UTC').trim() || 'UTC';
    try {
      const parts = new Intl.DateTimeFormat('en-US', {
        timeZone: tz,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false
      }).formatToParts(date);

      const get = (type: string): string =>
        parts.find(p => p.type === type)?.value || '';

      const year = get('year');
      const month = get('month');
      const day = get('day');
      const hour = get('hour');
      const minute = get('minute');
      const second = get('second');

      if (year && month && day && hour && minute && second) {
        return `${year}-${month}-${day}T${hour}:${minute}:${second}Z`;
      }
    } catch {
      // Fall through to ISO fallback
    }

    // Fallback: UTC ISO string without milliseconds.
    return date.toISOString().replace(/\.\d{3}Z$/, 'Z');
  }

  onTimezoneChange(): void {
    this.onDateChange();
  }

  onDateChange(): void {
    if (this.selectedDate) {
      this.pitrTime = this.formatRestoreTime(this.selectedDate, this.timezone);
    } else {
      this.pitrTime = '';
    }
  }

  startPITR(): void {
    if (!this.namespace || !this.clusterName || !this.pitrTime) {
      this.messageService.warning('请填写完整的恢复参数');
      return;
    }
    
    const req: PITRRequest = {
      time: this.pitrTime,
      timezone: this.timezone
    };
    
    this.hasActiveRecovery = true;
    this.currentStep = 0;
    this.stepStatus = 'process';
    this.statusMessage = '';
    
    this.apiService.initiatePITR(this.namespace, this.clusterName, req).subscribe({
      next: (resp: any) => {
        const target = (resp && (resp.targetCluster || resp.targetName)) ? String(resp.targetCluster || resp.targetName) : `${this.clusterName}-pitr`;
        this.targetClusterName = target;
        this.messageService.success('PITR 恢复任务已成功发起');
        this.currentStep = 1;
        this.startPolling();
      },
      error: (error) => {
        console.error('PITR 恢复失败:', error);
        const msg = error?.error?.message || error?.message || 'PITR 恢复任务发起失败，请检查参数并重试';
        this.messageService.error(msg);
        this.stepStatus = 'error';
        this.hasActiveRecovery = false;
      }
    });
  }

  private startPolling(): void {
    this.stopPolling();

    const ns = String(this.namespace || '').trim();
    const target = String(this.targetClusterName || '').trim();
    if (!ns || !target) {
      return;
    }

    this.pollSub = interval(3000)
      .pipe(
        startWith(0),
        switchMap(() =>
          this.apiService.getRestoreStatus(ns, target).pipe(
            catchError((err) => {
              const code = err?.status;
              if (code === 404) {
                this.statusMessage = '目标集群尚未创建，等待中…';
              } else {
                this.statusMessage = err?.error?.message || err?.message || '获取恢复状态失败';
              }
              return of(null);
            })
          )
        )
      )
      .subscribe((status: any) => {
        if (!this.hasActiveRecovery) {
          return;
        }
        if (!status) {
          this.stepStatus = 'process';
          return;
        }

        const phase = String(status?.phase || '').toLowerCase();
        const stage = String(status?.stage || '');
        this.statusMessage = stage || status?.message || '';

        if (phase === 'failed') {
          this.currentStep = 3;
          this.stepStatus = 'error';
          this.messageService.error('PITR 恢复失败，请检查恢复作业详情');
          this.hasActiveRecovery = false;
          this.stopPolling();
          return;
        }

        if (phase === 'completed') {
          this.currentStep = 3;
          this.stepStatus = 'finish';
          this.messageService.success('PITR 恢复已完成');
          this.hasActiveRecovery = false;
          this.stopPolling();
          return;
        }

        if (phase === 'creating') {
          this.currentStep = 2;
          this.stepStatus = 'process';
          return;
        }

        if (phase === 'pending') {
          this.currentStep = 1;
          this.stepStatus = 'process';
          return;
        }

        // Unknown / Running / Restoring etc.
        this.stepStatus = 'process';
      });
  }

  private stopPolling(): void {
    if (this.pollSub) {
      this.pollSub.unsubscribe();
      this.pollSub = undefined;
    }
  }

  reset(): void {
    this.namespace = 'default';
    this.clusterName = '';
    this.pitrTime = '';
    this.timezone = 'UTC';
    this.selectedDate = null;
    this.targetClusterName = '';
    this.hasActiveRecovery = false;
    this.currentStep = 0;
    this.stepStatus = 'wait';
    this.statusMessage = '';
    this.stopPolling();
  }

  isFormValid(): boolean {
    return !!(this.namespace && this.clusterName && this.pitrTime);
  }

  previewConfig(): void {
    const config = {
      namespace: this.namespace,
      clusterName: this.clusterName,
      pitrTime: this.pitrTime,
      timezone: this.timezone
    };
    console.log('PITR Configuration Preview:', config);
    this.messageService.info('配置预览已输出到控制台');
  }

  getStepStatusText(): string {
    const steps = ['验证参数中', '准备恢复中', '数据恢复中', '验证完成中'];
    return steps[this.currentStep] || '等待中';
  }

  loadHistory(): void {
    this.messageService.info('恢复历史功能开发中，敬请期待');
  }
}
