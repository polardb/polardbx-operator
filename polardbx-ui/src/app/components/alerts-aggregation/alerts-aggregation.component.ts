import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormBuilder, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { ApiService } from '../../services/api.service';
import { GlobalInstallProgressComponent } from '../global-install-progress/global-install-progress.component';

@Component({
  selector: 'app-alerts-aggregation',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    NzCardModule,
    NzFormModule,
    NzInputModule,
    NzButtonModule,
    NzIconModule,
    NzTableModule,
    NzTagModule,
    NzSpinModule,
    NzToolTipModule,
    NzGridModule,
    NzEmptyModule,
    NzAlertModule,
    GlobalInstallProgressComponent
  ],
  template: `
    <div class="alerts-aggregation">
      <app-global-install-progress></app-global-install-progress>
      <div class="page-header">
        <div class="header-content">
          <h1 class="page-title">
            <i nz-icon nzType="bell" class="page-icon"></i>
            告警聚合
          </h1>
          <p class="page-description">聚合展示 Alertmanager 和 Kubernetes Events 的活动告警信息</p>
        </div>
      </div>

      <div class="page-content">
        <nz-card class="filters-card" nzTitle="筛选条件" [nzExtra]="filtersExtra">
          <ng-template #filtersExtra>
            <i nz-icon nzType="filter" class="section-icon"></i>
          </ng-template>
          
          <form [formGroup]="form" class="filters-form">
            <nz-row [nzGutter]="16" nzAlign="bottom">
              <nz-col [nzSpan]="5">
                <div class="filter-item">
                  <label class="filter-label">命名空间</label>
                  <input nz-input formControlName="namespace" placeholder="所有命名空间" class="filter-input" />
                </div>
              </nz-col>
              <nz-col [nzSpan]="5">
                <div class="filter-item">
                  <label class="filter-label">集群</label>
                  <input nz-input formControlName="cluster" placeholder="所有集群" class="filter-input" />
                </div>
              </nz-col>
              <nz-col [nzSpan]="8">
                <div class="filter-item">
                  <label class="filter-label">Alertmanager URL</label>
                  <input nz-input formControlName="alertmanager" placeholder="http://alertmanager:9093" class="filter-input" />
                </div>
              </nz-col>
              <nz-col [nzSpan]="6">
                <div class="filter-actions">
                  <button nz-button nzType="primary" (click)="applyFilters()" [nzLoading]="loading">
                    <i nz-icon nzType="search"></i>
                    查询
                  </button>
                  <button nz-button nzType="default" (click)="clearFilters()">
                    <i nz-icon nzType="close"></i>
                    清空
                  </button>
                </div>
              </nz-col>
            </nz-row>
            
            <div class="external-actions" *ngIf="alertmanagerURL">
              <nz-alert nzType="info" nzShowIcon class="alertmanager-info">
                <div class="alert-content">
                  <span>当前连接: {{ alertmanagerURL }}</span>
                  <a [href]="alertmanagerURL" target="_blank" rel="noopener" nz-button nzType="link" nzSize="small">
                    <i nz-icon nzType="link"></i>
                    在 Alertmanager 中打开
                  </a>
                </div>
              </nz-alert>
            </div>
          </form>
        </nz-card>

        <nz-card class="results-card" [nzTitle]="resultsTitle" [nzExtra]="resultsExtra">
          <ng-template #resultsTitle>
            告警列表 <span class="item-count" *ngIf="items.length">({{ items.length }} 条)</span>
          </ng-template>
          <ng-template #resultsExtra>
            <i nz-icon nzType="notification" class="section-icon"></i>
          </ng-template>

          <div *ngIf="loading" class="loading-container">
            <nz-spin nzSize="large">
              <div class="loading-tip">正在查询告警信息...</div>
            </nz-spin>
          </div>

          <nz-table #alertsTable [nzData]="items" nzSize="middle" [nzShowPagination]="true" [nzPageSize]="20" 
                    *ngIf="!loading && items.length; else emptyState">
            <thead>
              <tr>
                <th nzWidth="160px">时间</th>
                <th nzWidth="100px">级别</th>
                <th nzWidth="120px">来源</th>
                <th nzWidth="140px">命名空间</th>
                <th nzWidth="140px">集群</th>
                <th>告警信息</th>
              </tr>
            </thead>
            <tbody>
              <tr *ngFor="let alert of alertsTable.data">
                <td>
                  <span class="timestamp">{{ alert.timestamp | date:'MM-dd HH:mm:ss' }}</span>
                </td>
                <td>
                  <nz-tag [nzColor]="getSeverityColor(alert.severity)">
                    {{ alert.severity || 'info' }}
                  </nz-tag>
                </td>
                <td>
                  <nz-tag [nzColor]="alert.source === 'alertmanager' ? 'blue' : 'default'">
                    {{ alert.source }}
                  </nz-tag>
                </td>
                <td>
                  <span class="namespace">{{ alert.labels?.namespace || '-' }}</span>
                </td>
                <td>
                  <span class="cluster">{{ alert.labels?.cluster || alert.labels?.involvedObject || alert.labels?.namespace || '-' }}</span>
                </td>
                <td>
                  <span class="message" [nz-tooltip]="alert.message" nzTooltipPlacement="topLeft">
                    {{ alert.message }}
                  </span>
                </td>
              </tr>
            </tbody>
          </nz-table>
          
          <ng-template #emptyState>
            <nz-empty [nzNotFoundImage]="'simple'" nzNotFoundDescription="暂无告警信息">
              <div class="empty-actions">
                <button nz-button nzType="primary" (click)="applyFilters()">刷新查询</button>
              </div>
            </nz-empty>
          </ng-template>
        </nz-card>
      </div>
    </div>
  `,
  styles: [`
    .alerts-aggregation {
      padding: 16px;
      background: #f5f5f5;
      min-height: 100vh;
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
      display: flex;
      flex-direction: column;
      gap: 16px;
    }
    
    .filters-card, .results-card {
      background: #fff;
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border: 1px solid #e0e0e0;
    }
    
    .section-icon {
      font-size: 16px;
      color: #1890ff;
    }
    
    .filters-form {
      margin-bottom: 16px;
    }
    
    .filter-item {
      display: flex;
      flex-direction: column;
      gap: 4px;
    }
    
    .filter-label {
      font-size: 14px;
      color: rgba(0, 0, 0, 0.85);
      font-weight: 500;
      line-height: 1.5;
    }
    
    .filter-input {
      height: 32px;
    }
    
    .filter-actions {
      display: flex;
      align-items: center;
      gap: 8px;
      height: 32px;
    }
    
    .external-actions {
      margin-top: 16px;
    }
    
    .alertmanager-info .alert-content {
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    
    .item-count {
      color: rgba(0, 0, 0, 0.45);
      font-weight: normal;
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
    
    .timestamp {
      font-family: 'Monaco', 'Menlo', monospace;
      font-size: 12px;
      color: rgba(0, 0, 0, 0.65);
    }
    
    .namespace, .cluster {
      color: rgba(0, 0, 0, 0.85);
      font-size: 13px;
    }
    
    .message {
      color: rgba(0, 0, 0, 0.85);
      font-size: 13px;
      display: -webkit-box;
      -webkit-line-clamp: 2;
      -webkit-box-orient: vertical;
      overflow: hidden;
      text-overflow: ellipsis;
      max-width: 300px;
      line-height: 1.4;
    }
    
    .empty-actions {
      margin-top: 16px;
    }
    
    /* 响应式设计 */
    @media (max-width: 1200px) {
      .page-content {
        max-width: 100%;
        padding: 0 8px;
      }
    }
    
    @media (max-width: 768px) {
      .alerts-aggregation {
        padding: 8px;
      }
      
      .filter-actions {
        flex-direction: column;
        height: auto;
        align-items: stretch;
      }
      
      .alertmanager-info .alert-content {
        flex-direction: column;
        align-items: flex-start;
        gap: 8px;
      }
      
      .message {
        max-width: 200px;
      }
    }
  `]
})
export class AlertsAggregationComponent implements OnInit {
  form: FormGroup;
  items: any[] = [];
  displayedColumns = ['time', 'severity', 'source', 'namespace', 'cluster', 'message'];
  loading = false;
  alertmanagerURL = '';

  private fb = inject(FormBuilder);
  private api = inject(ApiService);

  constructor() {
    this.form = this.fb.group({
      namespace: [''],
      cluster: [''],
      alertmanager: ['']
    });
  }

  ngOnInit(): void {
    this.alertmanagerURL = localStorage.getItem('alertmanagerURL') || '';
    this.applyFilters();
  }

  getSeverityColor(level?: string): string {
    const v = (level || '').toLowerCase();
    if (v === 'critical' || v === 'error') return 'red';
    if (v === 'warn' || v === 'warning') return 'orange';
    if (v === 'ok') return 'green';
    return 'blue';
  }

  applyFilters(): void {
    const val = this.form.value || {} as any;
    this.loading = true;
    const am = val.alertmanager || this.alertmanagerURL || undefined;
    this.api.listAlerts({ namespace: val.namespace || undefined, cluster: val.cluster || undefined, alertmanager: am }).subscribe({
      next: (res: any) => {
        const items = Array.isArray(res) ? res : (res?.items || []);
        this.items = items.map((a: any) => ({
          source: a.source,
          severity: a.severity,
          labels: a.labels || {},
          message: a.message,
          timestamp: a.timestamp ? new Date(a.timestamp) : (a.time ? new Date(a.time) : undefined)
        }));
        this.loading = false;
      },
      error: () => { this.items = []; this.loading = false; }
    });
  }

  clearFilters(): void {
    this.form.reset({ namespace: '', cluster: '', alertmanager: '' });
    this.applyFilters();
  }
}