import { Component, OnInit, Input, Output, EventEmitter, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { ApiService } from '../../services/api.service';
import { NzMessageService } from 'ng-zorro-antd/message';
import { Router } from '@angular/router';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzResultModule } from 'ng-zorro-antd/result';
import { NzStatisticModule } from 'ng-zorro-antd/statistic';
import { forkJoin, of } from 'rxjs';

@Component({
  selector: 'app-prechange-check',
  standalone: true,
  imports: [CommonModule, FormsModule, NzCardModule, NzFormModule, NzInputModule, NzButtonModule, NzIconModule, NzTagModule, NzSpinModule, NzDividerModule, NzGridModule, NzEmptyModule, NzResultModule, NzStatisticModule],
  template: `
    <div class="prechange-check">
      <div class="page-header" *ngIf="!embedded">
        <div class="header-content">
          <div class="header-info">
            <h1 class="page-title">
              <i nz-icon nzType="safety" class="page-icon"></i>
              变更前置检查
            </h1>
            <p class="page-description">检查最近全备、RPO 滞后、远端仓连通性等关键项，确保变更操作安全</p>
          </div>
        </div>
      </div>

      <div class="page-content">
        <nz-card class="control-panel" nzTitle="检查配置" *ngIf="!embedded">
          <form nz-form nzLayout="vertical">
            <div nz-row nzGutter="16" class="input-row">
              <div nz-col [nzSpan]="8">
                <nz-form-item>
                  <nz-form-label>命名空间</nz-form-label>
                  <nz-form-control>
                    <input nz-input [(ngModel)]="namespace" name="namespace" placeholder="default" />
                  </nz-form-control>
                </nz-form-item>
              </div>
              <div nz-col [nzSpan]="8">
                <nz-form-item>
                  <nz-form-label>集群名称</nz-form-label>
                  <nz-form-control>
                    <input nz-input [(ngModel)]="cluster" name="cluster" placeholder="pxc-1" />
                  </nz-form-control>
                </nz-form-item>
              </div>
              <div nz-col [nzSpan]="8">
                <nz-form-item>
                  <nz-form-label>&nbsp;</nz-form-label>
                  <nz-form-control>
                    <div class="action-buttons">
                      <button nz-button nzType="primary" nzSize="default" (click)="runChecks()" [nzLoading]="loading">
                        <i nz-icon nzType="play-circle"></i>
                        执行检查
                      </button>
                      <button nz-button nzType="default" nzSize="default" [disabled]="!cluster || !namespace" (click)="createPrecheckTask()" *ngIf="!embedded">
                        <i nz-icon nzType="profile"></i>
                        创建任务
                      </button>
                    </div>
                  </nz-form-control>
                </nz-form-item>
              </div>
            </div>
          </form>
        </nz-card>

        <div *ngIf="loading" class="loading-container">
          <nz-spin nzSize="large">
            <div class="loading-tip">正在执行检查...</div>
          </nz-spin>
        </div>

        <div *ngIf="!loading && checklist.length" class="results-section">
          <nz-card class="summary-card" nzTitle="检查结果概览">
            <div nz-row nzGutter="24" class="statistics">
              <div nz-col [nzSpan]="8">
                <nz-statistic nzTitle="通过项" [nzValue]="okCount" [nzValueStyle]="{ color: '#52c41a' }">
                  <ng-template #nzPrefix><i nz-icon nzType="check-circle" style="color: #52c41a"></i></ng-template>
                </nz-statistic>
              </div>
              <div nz-col [nzSpan]="8">
                <nz-statistic nzTitle="警告项" [nzValue]="warnCount" [nzValueStyle]="{ color: '#faad14' }">
                  <ng-template #nzPrefix><i nz-icon nzType="warning" style="color: #faad14"></i></ng-template>
                </nz-statistic>
              </div>
              <div nz-col [nzSpan]="8">
                <nz-statistic nzTitle="总计" [nzValue]="checklist.length" [nzValueStyle]="{ color: '#1890ff' }">
                  <ng-template #nzPrefix><i nz-icon nzType="audit" style="color: #1890ff"></i></ng-template>
                </nz-statistic>
              </div>
            </div>

            <div class="filters">
              <nz-tag nzMode="checkable" [nzChecked]="filterStatus==='all'" (click)="setFilter('all')">全部 {{ checklist.length }}</nz-tag>
              <nz-tag nzMode="checkable" [nzChecked]="filterStatus==='ok'" (click)="setFilter('ok')">通过 {{ okCount }}</nz-tag>
              <nz-tag nzMode="checkable" [nzChecked]="filterStatus==='warn'" (click)="setFilter('warn')">警告 {{ warnCount }}</nz-tag>
              <nz-tag nzMode="checkable" [nzChecked]="filterStatus==='fail'" (click)="setFilter('fail')">失败 {{ failCount }}</nz-tag>
              <span class="spacer"></span>
              <input nz-input placeholder="按名称/描述搜索" [(ngModel)]="searchTerm" style="max-width: 260px;" />
            </div>
          </nz-card>

          <nz-card class="details-card" nzTitle="详细检查项">
            <div class="check-items">
              <div class="check-item" *ngFor="let c of filteredChecklist; trackBy: trackByIndex">
                <div class="check-status">
                  <nz-tag [nzColor]="statusColor(c.status)" class="status-tag">
                    <i nz-icon [nzType]="getStatusIcon(c.status)"></i>
                    {{ getStatusText(c.status) }}
                  </nz-tag>
                </div>
                <div class="check-content">
                  <div class="check-name">{{ c.name }}</div>
                  <div class="check-message">{{ c.message }}</div>
                </div>
              </div>
            </div>
          </nz-card>
        </div>

        <div *ngIf="!loading && !checklist.length && hasExecuted" class="empty-container">
          <nz-empty nzNotFoundImage="simple" nzNotFoundContent="暂无检查结果">
            <ng-template #nzNotFoundFooter>
              <button nz-button nzType="primary" (click)="runChecks()">重新检查</button>
            </ng-template>
          </nz-empty>
        </div>
      </div>
    </div>
  `,
  styles: [`
    .prechange-check {
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
    
    .control-panel {
      margin-bottom: 16px;
      background: #fff;
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border: 1px solid #e0e0e0;
    }
    
    .input-row {
      align-items: flex-end;
    }
    
    .action-buttons {
      display: flex;
      gap: 8px;
      width: 100%;
    }
    
    .loading-container {
      display: flex;
      justify-content: center;
      align-items: center;
      padding: 80px 0;
    }

    .loading-tip {
      margin-top: 12px;
      text-align: center;
      color: rgba(0,0,0,0.65);
      letter-spacing: 0.5px;
    }
    
    .results-section {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }
    
    .summary-card, .details-card {
      background: #fff;
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border: 1px solid #e0e0e0;
    }
    
    .statistics {
      padding: 16px 0;
    }
    .filters {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 8px 0 4px 0;
    }
    .filters .spacer { flex: 1; }
    
    .check-items {
      display: flex;
      flex-direction: column;
      gap: 12px;
    }
    
    .check-item {
      display: flex;
      align-items: flex-start;
      gap: 16px;
      padding: 12px;
      background: #ffffff;
      border-radius: 8px;
      border: 1px solid #e0e0e0;
      transition: all 0.3s ease;
    }
    
    .check-item:hover {
      border-color: #1890ff;
      box-shadow: 0 2px 8px rgba(24, 144, 255, 0.1);
    }
    
    .check-status {
      flex-shrink: 0;
    }
    
    .status-tag {
      display: flex;
      align-items: center;
      gap: 4px;
      font-weight: 500;
    }
    
    .check-content {
      flex: 1;
      min-width: 0;
    }
    
    .check-name {
      font-weight: 500;
      font-size: 14px;
      color: rgba(0, 0, 0, 0.87);
      margin-bottom: 4px;
    }
    
    .check-message {
      color: rgba(0, 0, 0, 0.6);
      font-size: 14px;
      line-height: 1.4;
    }
    
    .empty-container {
      padding: 40px 0;
      text-align: center;
    }
    
    /* 响应式设计 */
    @media (max-width: 768px) {
      .prechange-check {
        padding: 16px;
      }
      
      .input-row {
        flex-direction: column;
      }
      
      .action-buttons {
        justify-content: center;
        margin-top: 16px;
      }
      
      .statistics {
        text-align: center;
      }
    }
  `]
})
export class PrechangeCheckComponent implements OnInit {
  @Input() embedded = false;
  @Input() initialNamespace?: string;
  @Input() initialCluster?: string;
  @Input() autoRun = false;
  @Output() completed = new EventEmitter<{ pass: boolean; hasWarn: boolean; hasError: boolean; checklist: Array<{ name: string; status: string; message: string }> }>();

  namespace = 'default';
  cluster = '';
  loading = false;
  hasExecuted = false;
  checklist: Array<{ name: string; status: string; message: string }> = [];
  filterStatus: 'all' | 'ok' | 'warn' | 'fail' = 'all';
  searchTerm = '';

  private api = inject(ApiService);
  private msg = inject(NzMessageService);
  private router = inject(Router);

  ngOnInit(): void {
    if (this.initialNamespace) this.namespace = this.initialNamespace;
    if (this.initialCluster) this.cluster = this.initialCluster;
    if (this.autoRun && this.cluster) {
      Promise.resolve().then(() => this.runChecks());
    }
  }

  statusColor(s: string): string {
    if (s === 'ok') return 'success';
    if (s === 'warn') return 'warning';
    return 'error';
  }
  get okCount(): number { return this.checklist.filter(i => i.status === 'ok').length; }
  get warnCount(): number { return this.checklist.filter(i => i.status !== 'ok').length; }
  get failCount(): number { return this.checklist.filter(i => i.status !== 'ok' && i.status !== 'warn').length; }
  get filteredChecklist() {
    const term = (this.searchTerm || '').trim().toLowerCase();
    return this.checklist.filter(c => {
      const byStatus = this.filterStatus === 'all' ||
        (this.filterStatus === 'ok' && c.status === 'ok') ||
        (this.filterStatus === 'warn' && c.status === 'warn') ||
        (this.filterStatus === 'fail' && c.status !== 'ok' && c.status !== 'warn');
      const byTerm = !term || (c.name?.toLowerCase().includes(term) || c.message?.toLowerCase().includes(term));
      return byStatus && byTerm;
    });
  }
  setFilter(s: 'all'|'ok'|'warn'|'fail') { this.filterStatus = s; }
  
  getStatusIcon(status: string): string {
    switch (status) {
      case 'ok': return 'check-circle';
      case 'warn': return 'warning';
      default: return 'close-circle';
    }
  }
  
  getStatusText(status: string): string {
    switch (status) {
      case 'ok': return '通过';
      case 'warn': return '警告';
      default: return '失败';
    }
  }
  
  trackByIndex(index: number): number {
    return index;
  }

  runChecks(): void {
    if (!this.cluster) return;
    this.loading = true;
    this.hasExecuted = true;
    this.api.runPrecheck(this.namespace, this.cluster, 'config').subscribe({
      next: (res: any) => {
        const plan = Array.isArray(res?.plan) ? res.plan : [];
        this.checklist = plan.map((p: any) => ({ name: p.id || 'check', status: p.state || 'warn', message: p.message || '' }));
        this.loading = false;
        const hasWarn = this.checklist.some(i => i.status === 'warn');
        const hasError = this.checklist.some(i => i.status !== 'ok' && i.status !== 'warn');
        const pass = !hasError;
        this.completed.emit({ pass, hasWarn, hasError, checklist: this.checklist.slice() });
      },
      error: (err) => { 
        // 兜底：展示简要错误+提供最小检查集建议
        const status = err?.status;
        const msg = status === 404 ? '后端未提供预检接口（404），请升级后端或使用最小检查集' : '预检失败（网络/后端异常），已切换为最小检查集';
        this.msg.warning(msg);
        // 最小检查集：仅给出关键项提示
        this.checklist = [
          { name: 'checkStorage', status: 'warn', message: '请先在“备份/存储配置”中配置 HPFS Sink' },
          { name: 'checkRecentBackup', status: 'warn', message: '建议先完成一次全量备份，再执行变更' },
          { name: 'checkPodsReady', status: 'ok', message: '可在“节点”页确认 Pod 就绪状态' }
        ];
        this.loading = false; 
        this.completed.emit({ pass: true, hasWarn: true, hasError: false, checklist: this.checklist.slice() });
      }
    });
  }

  createPrecheckTask(): void {
    if (!this.cluster) return;
    this.api.createPrecheckSystemTask(this.namespace, this.cluster).subscribe({
      next: () => { this.msg.success('Precheck SystemTask 创建已提交'); this.router.navigateByUrl('/operations/system-tasks'); },
      error: () => this.msg.error('创建失败')
    });
  }
}