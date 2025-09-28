import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzStatisticModule } from 'ng-zorro-antd/statistic';
import { NzProgressModule } from 'ng-zorro-antd/progress';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzMessageService } from 'ng-zorro-antd/message';
import { ApiService } from '../../services/api.service';

@Component({
  selector: 'app-backup-overview',
  standalone: true,
  imports: [
    CommonModule,
    NzCardModule,
    NzButtonModule,
    NzIconModule,
    NzTagModule,
    NzSpinModule,
    NzGridModule,
    NzStatisticModule,
    NzProgressModule,
    NzAlertModule
  ],
  template: `
    <div class="backup-overview">
      <div class="page-header">
        <div class="header-content">
          <h1 class="page-title">
            <i nz-icon nzType="dashboard" class="page-icon"></i>
            备份概览
          </h1>
          <p class="page-description">展示过去 24 小时备份关键指标和存储连通性状态</p>
        </div>
      </div>

      <div class="page-content">
        <nz-card class="overview-card" nzTitle="备份总览" [nzExtra]="cardExtra">
          <ng-template #cardExtra>
            <div class="card-actions">
              <button nz-button nzType="default" (click)="load()" [nzLoading]="loading">
                <i nz-icon nzType="reload"></i>
                刷新数据
              </button>
              <a *ngIf="grafanaLink" [href]="grafanaLink" target="_blank" rel="noopener" nz-button nzType="link">
                <i nz-icon nzType="link"></i>
                Grafana 监控
              </a>
            </div>
          </ng-template>

          <div *ngIf="loading" class="loading-container">
            <nz-spin nzSize="large"></nz-spin>
            <div class="loading-text">正在加载备份统计数据...</div>
          </div>

          <div *ngIf="!loading" class="statistics-container">
            <nz-row [nzGutter]="16">
              <nz-col [nzSpan]="6">
                <nz-card class="stat-card success-rate">
                  <nz-statistic 
                    nzTitle="24小时成功率" 
                    [nzValue]="kpi?.successRate24h ?? 0" 
                    nzSuffix="%"
                    [nzValueStyle]="{ color: getSuccessRateColor() }">
                    <ng-template #nzPrefix>
                      <i nz-icon nzType="check-circle" [style.color]="getSuccessRateColor()"></i>
                    </ng-template>
                  </nz-statistic>
                  <nz-progress 
                    [nzPercent]="kpi?.successRate24h ?? 0" 
                    [nzStrokeColor]="getSuccessRateColor()"
                    nzSize="small"
                    [nzShowInfo]="false">
                  </nz-progress>
                </nz-card>
              </nz-col>
              
              <nz-col [nzSpan]="6">
                <nz-card class="stat-card">
                  <nz-statistic 
                    nzTitle="当前运行中" 
                    [nzValue]="kpi?.running ?? 0"
                    [nzValueStyle]="{ color: '#1890ff' }">
                    <ng-template #nzPrefix>
                      <i nz-icon nzType="play-circle" style="color: #1890ff;"></i>
                    </ng-template>
                  </nz-statistic>
                </nz-card>
              </nz-col>
              
              <nz-col [nzSpan]="6">
                <nz-card class="stat-card">
                  <nz-statistic 
                    nzTitle="24小时失败" 
                    [nzValue]="kpi?.failed24h ?? 0"
                    [nzValueStyle]="{ color: '#f5222d' }">
                    <ng-template #nzPrefix>
                      <i nz-icon nzType="close-circle" style="color: #f5222d;"></i>
                    </ng-template>
                  </nz-statistic>
                </nz-card>
              </nz-col>
              
              <nz-col [nzSpan]="6">
                <nz-card class="stat-card">
                  <nz-statistic 
                    nzTitle="24小时总数" 
                    [nzValue]="kpi?.totalBackups24h ?? 0"
                    [nzValueStyle]="{ color: '#52c41a' }">
                    <ng-template #nzPrefix>
                      <i nz-icon nzType="file-sync" style="color: #52c41a;"></i>
                    </ng-template>
                  </nz-statistic>
                </nz-card>
              </nz-col>
            </nz-row>
            
            <nz-row [nzGutter]="16" class="secondary-stats">
              <nz-col [nzSpan]="12">
                <nz-card class="stat-card connectivity">
                  <div class="connectivity-content">
                    <div class="connectivity-header">
                      <h4>存储连通性</h4>
                      <nz-tag [nzColor]="getConnectivityColor()">
                        {{ getConnectivityText() }}
                      </nz-tag>
                    </div>
                    <div class="connectivity-details">
                      <i nz-icon [nzType]="getConnectivityIcon()" [style.color]="getConnectivityColor()"></i>
                      <span>{{ getConnectivityDescription() }}</span>
                    </div>
                  </div>
                </nz-card>
              </nz-col>
              
              <nz-col [nzSpan]="12" *ngIf="kpi?.totalStorageBytes !== undefined">
                <nz-card class="stat-card storage">
                  <nz-statistic 
                    nzTitle="总存储用量" 
                    [nzValue]="getStorageSize()" 
                    [nzSuffix]="getStorageUnit()"
                    [nzValueStyle]="{ color: '#722ed1' }">
                    <ng-template #nzPrefix>
                      <i nz-icon nzType="hdd" style="color: #722ed1;"></i>
                    </ng-template>
                  </nz-statistic>
                </nz-card>
              </nz-col>
            </nz-row>
          </div>

          <nz-alert 
            *ngIf="generatedAt" 
            nzType="info" 
            nzShowIcon 
            class="meta-alert">
            <div class="meta-content">
              <i nz-icon nzType="clock-circle"></i>
              <span>数据生成时间：{{ generatedAt | date:'yyyy-MM-dd HH:mm:ss' }}</span>
            </div>
          </nz-alert>
        </nz-card>
      </div>
    </div>
  `,
  styles: [`
    .backup-overview {
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
    }
    
    .overview-card {
      background: #ffffff;
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border: 1px solid #e0e0e0;
    }
    
    .card-actions {
      display: flex;
      gap: 8px;
      align-items: center;
    }
    
    .loading-container { display: flex; justify-content: center; align-items: center; padding: 80px 0; gap: 12px; }
    .loading-text { color: rgba(0,0,0,0.65); letter-spacing: 0.5px; }
    
    .statistics-container {
      margin-bottom: 16px;
    }
    
    .stat-card {
      height: 120px;
      border: 1px solid #e0e0e0;
      border-radius: 6px;
      background: #fafafa;
      transition: all 0.3s ease;
    }
    
    .stat-card:hover {
      box-shadow: 0 4px 12px rgba(0,0,0,0.1);
      border-color: #1890ff;
    }
    
    .stat-card.success-rate {
      background: linear-gradient(135deg, #f6ffed 0%, #f0f9ff 100%);
    }
    
    .secondary-stats {
      margin-top: 16px;
    }
    
    .connectivity-content {
      height: 100%;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
    }
    
    .connectivity-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 12px;
    }
    
    .connectivity-header h4 {
      margin: 0;
      font-size: 14px;
      color: rgba(0, 0, 0, 0.85);
      font-weight: 500;
    }
    
    .connectivity-details {
      display: flex;
      align-items: center;
      gap: 8px;
      color: rgba(0, 0, 0, 0.65);
      font-size: 13px;
    }
    
    .meta-alert {
      margin-top: 16px;
    }
    
    .meta-content {
      display: flex;
      align-items: center;
      gap: 8px;
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
      .backup-overview {
        padding: 8px;
      }
      
      .card-actions {
        flex-direction: column;
        align-items: stretch;
      }
      
      .stat-card {
        height: auto;
        min-height: 100px;
      }
    }
    
    @media (max-width: 576px) {
      .statistics-container nz-col {
        margin-bottom: 8px;
      }
    }
  `]
})
export class BackupOverviewComponent implements OnInit {
  private api = inject(ApiService);
  private message = inject(NzMessageService);
  
  loading = false;
  kpi: any = null;
  generatedAt = '';
  grafanaURL = '';
  grafanaLink = '';

  ngOnInit(): void {
    this.grafanaURL = localStorage.getItem('grafanaURL') || '';
    this.grafanaLink = this.grafanaURL ? `${this.grafanaURL}/d/polardbx-monitor?orgId=1&var-namespace=default` : '';
    this.load();
  }

  load(): void {
    this.loading = true;
    this.api.getBackupOverview({ namespace: 'default', evaluateConnectivity: true, connectivityMode: 'present' })
      .subscribe({
        next: (res) => {
          this.kpi = res?.kpi || {};
          this.generatedAt = res?.generatedAt || '';
          this.loading = false;
          this.message.success('备份概览数据已更新');
        },
        error: (error) => {
          console.error('加载备份概览失败:', error);
          this.kpi = null; 
          this.loading = false;
          this.message.error('加载备份概览数据失败，请稍后重试');
        }
      });
  }

  getSuccessRateColor(): string {
    const rate = this.kpi?.successRate24h ?? 0;
    if (rate >= 95) return '#52c41a';
    if (rate >= 80) return '#faad14';
    return '#f5222d';
  }

  getConnectivityColor(): string {
    const status = this.kpi?.storageConnectivityStatus;
    return status === 'ok' ? 'green' : 'red';
  }

  getConnectivityText(): string {
    const status = this.kpi?.storageConnectivityStatus;
    return status === 'ok' ? '正常' : '异常';
  }

  getConnectivityIcon(): string {
    const status = this.kpi?.storageConnectivityStatus;
    return status === 'ok' ? 'check-circle' : 'close-circle';
  }

  getConnectivityDescription(): string {
    const status = this.kpi?.storageConnectivityStatus;
    const connectivity = this.kpi?.storageConnectivity ?? 'unknown';
    
    if (status === 'ok') {
      return `存储连接正常 (${connectivity})`;
    } else {
      return `存储连接异常 (${connectivity})`;
    }
  }

  getStorageSize(): number {
    const bytes = this.kpi?.totalStorageBytes ?? 0;
    if (bytes >= 1024 * 1024 * 1024) {
      return Math.round(bytes / (1024 * 1024 * 1024) * 100) / 100;
    } else if (bytes >= 1024 * 1024) {
      return Math.round(bytes / (1024 * 1024) * 100) / 100;
    } else {
      return Math.round(bytes / 1024 * 100) / 100;
    }
  }

  getStorageUnit(): string {
    const bytes = this.kpi?.totalStorageBytes ?? 0;
    if (bytes >= 1024 * 1024 * 1024) {
      return 'GB';
    } else if (bytes >= 1024 * 1024) {
      return 'MB';
    } else {
      return 'KB';
    }
  }
}