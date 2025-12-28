import { Component, OnInit, OnDestroy, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Router } from '@angular/router';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule, NzIconService } from 'ng-zorro-antd/icon';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzStatisticModule } from 'ng-zorro-antd/statistic';
import { NzProgressModule } from 'ng-zorro-antd/progress';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzSwitchModule } from 'ng-zorro-antd/switch';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzBadgeModule } from 'ng-zorro-antd/badge';
import { FormsModule } from '@angular/forms';
import { NzMessageService } from 'ng-zorro-antd/message';
import { ApiService } from '../../services/api.service';

// Import icons
import {
  DashboardOutline,
  ReloadOutline,
  PlusOutline,
  UnorderedListOutline,
  FieldTimeOutline,
  SyncOutline,
  AreaChartOutline,
  CloudServerOutline,
  CheckCircleOutline,
  CloseCircleOutline,
  LoadingOutline,
  CloudUploadOutline,
  EyeOutline,
  ClockCircleOutline
} from '@ant-design/icons-angular/icons';
import { Subject, interval, forkJoin } from 'rxjs';
import { takeUntil, switchMap, startWith } from 'rxjs/operators';

interface ClusterBackupInfo {
  clusterName: string;
  namespace: string;
  latestBackup?: {
    name: string;
    phase: string;
    startTime?: string;
    endTime?: string;
    latestRecoverableTimestamp?: string;
  };
  nextScheduledTime?: string;
  rpoSeconds?: number;
}

@Component({
  selector: 'app-backup-overview',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    NzCardModule,
    NzButtonModule,
    NzIconModule,
    NzTagModule,
    NzSpinModule,
    NzGridModule,
    NzStatisticModule,
    NzProgressModule,
    NzAlertModule,
    NzTableModule,
    NzToolTipModule,
    NzEmptyModule,
    NzSwitchModule,
    NzDividerModule,
    NzBadgeModule
  ],
  template: `
    <div class="page-wrapper backup-overview">
      <div class="page-header">
        <div class="header-content">
          <h1 class="page-title">
            <i nz-icon nzType="dashboard" class="page-icon"></i>
            备份概览
          </h1>
          <p class="page-description">展示过去 24 小时备份关键指标、存储连通性和各集群备份状态</p>
        </div>
        <div class="header-actions">
          <label class="auto-refresh-label">
            <nz-switch [(ngModel)]="autoRefresh" (ngModelChange)="toggleAutoRefresh()"></nz-switch>
            <span>自动刷新 (30s)</span>
          </label>
          <button nz-button nzType="default" (click)="load()" [nzLoading]="loading">
            <i nz-icon nzType="reload"></i>
            刷新数据
          </button>
        </div>
      </div>

      <div class="page-content">
        <!-- Quick actions -->
        <nz-card class="quick-actions-card">
          <div class="quick-actions">
            <button nz-button nzType="primary" (click)="navigateTo('/backup/manual-backups')">
              <i nz-icon nzType="plus"></i>
              创建备份
            </button>
            <button nz-button nzType="default" (click)="navigateTo('/backup/manual-backups')">
              <i nz-icon nzType="unordered-list"></i>
              备份列表
            </button>
            <button nz-button nzType="default" (click)="navigateTo('/backup/backup-schedules')">
              <i nz-icon nzType="field-time"></i>
              备份计划
            </button>
            <button nz-button nzType="default" (click)="navigateTo('/backup/backup-binlogs')">
              <i nz-icon nzType="sync"></i>
              Binlog 管理
            </button>
            <a *ngIf="grafanaLink" [href]="grafanaLink" target="_blank" rel="noopener" nz-button nzType="link">
              <i nz-icon nzType="area-chart"></i>
              Grafana 监控
            </a>
          </div>
        </nz-card>

        <!-- KPI summary -->
        <nz-card class="overview-card" nzTitle="24小时备份统计">
          <div *ngIf="loading && !kpi" class="loading-container">
            <nz-spin nzSize="large"></nz-spin>
            <div class="loading-text">正在加载备份统计数据...</div>
          </div>

          <div *ngIf="kpi" class="statistics-container">
            <nz-row [nzGutter]="16">
              <nz-col [nzXs]="24" [nzSm]="12" [nzMd]="6">
                <nz-card class="stat-card success-rate" [nzBordered]="false">
                  <nz-statistic 
                    nzTitle="成功率" 
                    [nzValue]="kpi?.successRate24h ?? 0" 
                    nzSuffix="%"
                    [nzValueStyle]="{ color: getSuccessRateColor() }">
                  </nz-statistic>
                  <nz-progress 
                    [nzPercent]="kpi?.successRate24h ?? 0" 
                    [nzStrokeColor]="getSuccessRateColor()"
                    nzSize="small"
                    [nzShowInfo]="false">
                  </nz-progress>
                </nz-card>
              </nz-col>
              
              <nz-col [nzXs]="24" [nzSm]="12" [nzMd]="6">
                <nz-card class="stat-card" [nzBordered]="false">
                  <nz-statistic 
                    nzTitle="运行中备份" 
                    [nzValue]="kpi?.running ?? 0"
                    [nzValueStyle]="{ color: 'var(--primary-color)' }">
                  </nz-statistic>
                  <div class="stat-extra" *ngIf="kpi?.running > 0">
                    <nz-badge nzStatus="processing" nzText="正在备份"></nz-badge>
                  </div>
                </nz-card>
              </nz-col>
              
              <nz-col [nzXs]="24" [nzSm]="12" [nzMd]="6">
                <nz-card class="stat-card" [class.warning]="kpi?.failed24h > 0" [nzBordered]="false">
                  <nz-statistic 
                    nzTitle="失败备份" 
                    [nzValue]="kpi?.failed24h ?? 0"
                    [nzValueStyle]="{ color: kpi?.failed24h > 0 ? '#f5222d' : '#52c41a' }">
                  </nz-statistic>
                  <div class="stat-extra" *ngIf="kpi?.failed24h > 0">
                    <a (click)="navigateTo('/backup/manual-backups')" class="error-link">查看详情</a>
                  </div>
                </nz-card>
              </nz-col>
              
              <nz-col [nzXs]="24" [nzSm]="12" [nzMd]="6">
                <nz-card class="stat-card" [nzBordered]="false">
                  <nz-statistic 
                    nzTitle="备份总数" 
                    [nzValue]="kpi?.totalBackups24h ?? 0"
                    [nzValueStyle]="{ color: '#722ed1' }">
                  </nz-statistic>
                </nz-card>
              </nz-col>
            </nz-row>
            
            <!-- Storage connectivity -->
            <nz-row [nzGutter]="16" class="secondary-stats">
              <nz-col [nzSpan]="24">
                <nz-card class="connectivity-card" [nzBordered]="false">
                  <div class="connectivity-content">
                    <div class="connectivity-header">
                      <h4><i nz-icon nzType="cloud-server"></i> 存储连通性</h4>
                      <nz-tag [nzColor]="getConnectivityColor()">
                        <i nz-icon [nzType]="getConnectivityIcon()"></i>
                        {{ getConnectivityText() }}
                      </nz-tag>
                    </div>
                    <div class="connectivity-details">
                      {{ getConnectivityDescription() }}
                    </div>
                  </div>
                </nz-card>
              </nz-col>
            </nz-row>
          </div>
        </nz-card>

        <!-- Cluster backup state table -->
        <nz-card class="cluster-state-card" nzTitle="集群备份状态">
          <ng-template #clusterExtra>
            <span class="cluster-count">共 {{ clusters.length }} 个集群</span>
          </ng-template>
          
          <nz-table
            #clusterTable
            [nzData]="clusters"
            [nzLoading]="clusterLoading"
            [nzPageSize]="10"
            [nzShowSizeChanger]="true"
            nzSize="middle"
            [nzFrontPagination]="true"
          >
            <thead>
              <tr>
                <th>集群名称</th>
                <th>命名空间</th>
                <th>最近备份</th>
                <th>备份状态</th>
                <th>下次计划</th>
                <th nz-tooltip nzTooltipTitle="恢复点目标，表示最近可恢复时间与当前时间的差距">RPO</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              <tr *ngFor="let cluster of clusterTable.data">
                <td>
                  <a (click)="navigateTo('/clusters/' + cluster.namespace + '/' + cluster.clusterName)">
                    {{ cluster.clusterName }}
                  </a>
                </td>
                <td><nz-tag>{{ cluster.namespace }}</nz-tag></td>
                <td>
                  <span *ngIf="cluster.latestBackup">
                    <span nz-tooltip [nzTooltipTitle]="'备份名: ' + cluster.latestBackup.name">
                      {{ cluster.latestBackup.startTime | date:'MM-dd HH:mm' }}
                    </span>
                  </span>
                  <span *ngIf="!cluster.latestBackup" class="no-data">无备份</span>
                </td>
                <td>
                  <nz-tag *ngIf="cluster.latestBackup" [nzColor]="getPhaseColor(cluster.latestBackup.phase)">
                    {{ getPhaseText(cluster.latestBackup.phase) }}
                  </nz-tag>
                  <span *ngIf="!cluster.latestBackup" class="no-data">-</span>
                </td>
                <td>
                  <span *ngIf="cluster.nextScheduledTime">
                    {{ cluster.nextScheduledTime | date:'MM-dd HH:mm' }}
                  </span>
                  <span *ngIf="!cluster.nextScheduledTime" class="no-data">未配置</span>
                </td>
                <td>
                  <span *ngIf="cluster.rpoSeconds !== null && cluster.rpoSeconds !== undefined" 
                        [class.rpo-warning]="cluster.rpoSeconds > 86400"
                        [class.rpo-critical]="cluster.rpoSeconds > 172800">
                    {{ formatRPO(cluster.rpoSeconds) }}
                  </span>
                  <span *ngIf="cluster.rpoSeconds === null || cluster.rpoSeconds === undefined" class="no-data">-</span>
                </td>
                <td>
                  <button nz-button nzType="link" nzSize="small" 
                          (click)="createBackupForCluster(cluster.namespace, cluster.clusterName)"
                          nz-tooltip nzTooltipTitle="立即备份">
                    <i nz-icon nzType="cloud-upload"></i>
                  </button>
                  <nz-divider nzType="vertical"></nz-divider>
                  <button nz-button nzType="link" nzSize="small"
                          (click)="navigateTo('/backup/manual-backups')"
                          nz-tooltip nzTooltipTitle="查看备份">
                    <i nz-icon nzType="eye"></i>
                  </button>
                </td>
              </tr>
            </tbody>
          </nz-table>
          
          <nz-empty *ngIf="!clusterLoading && clusters.length === 0"
                    nzNotFoundContent="暂无集群"
                    [nzNotFoundFooter]="emptyFooter">
            <ng-template #emptyFooter>
              <button nz-button nzType="primary" (click)="navigateTo('/clusters/create')">
                <i nz-icon nzType="plus"></i>
                创建集群
              </button>
            </ng-template>
          </nz-empty>
        </nz-card>

        <!-- Metadata -->
        <div class="meta-info" *ngIf="generatedAt">
          <i nz-icon nzType="clock-circle"></i>
          数据更新时间：{{ generatedAt | date:'yyyy-MM-dd HH:mm:ss' }}
          <span *ngIf="autoRefresh" class="auto-refresh-indicator">
            <nz-badge nzStatus="processing"></nz-badge> 自动刷新中
          </span>
        </div>
      </div>
    </div>
  `,
  styles: [`
    .page-wrapper {
      display: flex;
      flex-direction: column;
      gap: 16px;
      padding: 24px;
      min-height: 100%;
      background: transparent;
    }
    
    .page-header {
      margin-bottom: 0;
      background: #fff;
      padding: 16px;
      border-radius: 10px;
      box-shadow: 0 2px 10px rgba(15, 23, 42, 0.04);
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      flex-wrap: wrap;
      gap: 16px;
    }
    
    .header-content {
      flex: 1;
      min-width: 300px;
    }
    
    .header-actions {
      display: flex;
      gap: 16px;
      align-items: center;
    }
    
    .auto-refresh-label {
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 13px;
      color: rgba(0,0,0,0.65);
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
      color: var(--primary-color, #ff6a00);
    }
    
    .page-description {
      color: rgba(0, 0, 0, 0.6);
      font-size: 14px;
      margin: 0;
    }
    
    .page-content {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }
    
    .quick-actions-card {
      background: #ffffff;
      border-radius: 8px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.06);
    }
    
    .quick-actions {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
    }
    
    .overview-card, .cluster-state-card {
      background: #ffffff;
      border-radius: 8px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.06);
    }
    
    .loading-container {
      display: flex;
      justify-content: center;
      align-items: center;
      padding: 60px 0;
      gap: 12px;
    }
    
    .loading-text {
      color: rgba(0,0,0,0.65);
    }
    
    .statistics-container nz-col {
      margin-bottom: 16px;
    }
    
    .stat-card {
      background: #fafafa;
      border-radius: 8px;
      padding: 16px;
      height: 100%;
      transition: all 0.3s;
    }
    
    .stat-card:hover {
      box-shadow: 0 4px 12px rgba(0,0,0,0.1);
    }
    
    .stat-card.success-rate {
      background: linear-gradient(135deg, #f6ffed 0%, #e6f7ff 100%);
    }
    
    .stat-card.warning {
      background: #fff2f0;
    }
    
    .stat-extra {
      margin-top: 8px;
    }
    
    .error-link {
      color: #f5222d;
      cursor: pointer;
    }
    
    .secondary-stats {
      margin-top: 0;
    }
    
    .connectivity-card {
      background: #fafafa;
      border-radius: 8px;
      padding: 16px;
    }
    
    .connectivity-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 8px;
    }
    
    .connectivity-header h4 {
      margin: 0;
      font-size: 14px;
      color: rgba(0, 0, 0, 0.85);
      display: flex;
      align-items: center;
      gap: 8px;
    }
    
    .connectivity-details {
      color: rgba(0, 0, 0, 0.65);
      font-size: 13px;
    }
    
    .cluster-count {
      color: rgba(0,0,0,0.45);
      font-size: 13px;
    }
    
    .no-data {
      color: rgba(0,0,0,0.25);
    }
    
    .rpo-warning {
      color: #faad14;
    }
    
    .rpo-critical {
      color: #f5222d;
      font-weight: 500;
    }
    
    .meta-info {
      display: flex;
      align-items: center;
      gap: 8px;
      color: rgba(0,0,0,0.45);
      font-size: 12px;
      justify-content: flex-end;
      padding: 8px 0;
    }
    
    .auto-refresh-indicator {
      display: flex;
      align-items: center;
      gap: 4px;
      margin-left: 16px;
    }
    
    @media (max-width: 768px) {
      .page-header {
        flex-direction: column;
      }
      
      .header-actions {
        width: 100%;
        justify-content: space-between;
      }
      
      .quick-actions {
        flex-direction: column;
      }
      
      .quick-actions button, .quick-actions a {
        width: 100%;
      }
    }
  `]
})
export class BackupOverviewComponent implements OnInit, OnDestroy {
  private api = inject(ApiService);
  private message = inject(NzMessageService);
  private router = inject(Router);
  private iconService = inject(NzIconService);
  private destroy$ = new Subject<void>();
  
  loading = false;
  clusterLoading = false;
  kpi: any = null;
  clusters: ClusterBackupInfo[] = [];
  generatedAt = '';
  grafanaURL = '';
  grafanaLink = '';
  autoRefresh = true;

  constructor() {
    // Register icons
    this.iconService.addIcon(
      DashboardOutline,
      ReloadOutline,
      PlusOutline,
      UnorderedListOutline,
      FieldTimeOutline,
      SyncOutline,
      AreaChartOutline,
      CloudServerOutline,
      CheckCircleOutline,
      CloseCircleOutline,
      LoadingOutline,
      CloudUploadOutline,
      EyeOutline,
      ClockCircleOutline
    );
  }

  ngOnInit(): void {
    this.grafanaURL = localStorage.getItem('grafanaURL') || '';
    this.grafanaLink = this.grafanaURL ? `${this.grafanaURL}/d/polardbx-backup?orgId=1` : '';
    this.load();
    this.setupAutoRefresh();
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  setupAutoRefresh(): void {
    interval(30000).pipe(
      takeUntil(this.destroy$),
      startWith(0)
    ).subscribe(() => {
      if (this.autoRefresh && !this.loading) {
        this.load(true);
      }
    });
  }

  toggleAutoRefresh(): void {
    if (this.autoRefresh) {
      this.message.info('已开启自动刷新 (30秒)');
    } else {
      this.message.info('已关闭自动刷新');
    }
  }

  load(silent = false): void {
    if (!silent) {
      this.loading = true;
      this.clusterLoading = true;
    }
    
    forkJoin({
      overview: this.api.getBackupOverview({ evaluateConnectivity: true }),
      clusterState: this.api.getClusterBackupState()
    }).subscribe({
      next: ({ overview, clusterState }) => {
        this.kpi = overview?.kpi || {};
        this.generatedAt = overview?.generatedAt || '';
        this.clusters = clusterState?.clusters || [];
        this.loading = false;
        this.clusterLoading = false;
        if (!silent) {
          this.message.success('备份概览数据已更新');
        }
      },
      error: (error) => {
        console.error('加载备份概览失败:', error);
        this.loading = false;
        this.clusterLoading = false;
        this.message.error('加载备份概览数据失败');
      }
    });
  }

  navigateTo(path: string): void {
    this.router.navigate([path]);
  }

  createBackupForCluster(namespace: string, clusterName: string): void {
    this.router.navigate(['/backup/manual-backups'], { 
      queryParams: { action: 'create', namespace, cluster: clusterName } 
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
    if (!status || status === 'unknown') return 'blue';
    return status === 'ok' ? 'green' : 'red';
  }

  getConnectivityText(): string {
    const status = this.kpi?.storageConnectivityStatus;
    if (!status || status === 'unknown') return '检测中';
    return status === 'ok' ? '正常' : '异常';
  }

  getConnectivityIcon(): string {
    const status = this.kpi?.storageConnectivityStatus;
    if (!status || status === 'unknown') return 'loading';
    return status === 'ok' ? 'check-circle' : 'close-circle';
  }

  getConnectivityDescription(): string {
    const rawConnectivity = this.kpi?.storageConnectivity ?? '';
    const connectivity = this.normalizePlaceholder(rawConnectivity);
    const status = this.kpi?.storageConnectivityStatus;
    
    if (!status || status === 'unknown') {
      return '正在检测存储连通性...';
    } else if (status === 'ok') {
      // Backend may return an empty detail (e.g., probing disabled or no additional info available).
      return connectivity
        ? `存储服务连接正常：${connectivity}`
        : '存储服务连接正常';
    } else {
      const detail = connectivity || '请检查 HPFS 配置';
      return `存储连接异常：${detail}`;
    }
  }

  /**
   * Backward compatibility for legacy placeholder values.
   * New backend versions should not return placeholder literals.
   * If a field is missing/unavailable, treat it as an empty string (i.e., "not provided" => "do not display").
   */
  private normalizePlaceholder(value: any): string {
    if (value === 'pending_implementation' || value === null || value === undefined) {
      return '';
    }
    return String(value);
  }

  getPhaseColor(phase: string): string {
    const p = (phase || '').toLowerCase();
    if (p === 'finished' || p === 'succeeded' || p === 'completed') return 'green';
    if (p === 'failed') return 'red';
    if (p.includes('backing') || p.includes('collecting') || p.includes('calculating')) return 'processing';
    return 'default';
  }

  getPhaseText(phase: string): string {
    const map: Record<string, string> = {
      '': '新建',
      'fullbackuping': '全量备份中',
      'collecting': '收集中',
      'calculating': '计算中',
      'binlogbackuping': 'Binlog备份中',
      'metadatabackuping': '元数据备份中',
      'finished': '已完成',
      'succeeded': '已完成',
      'completed': '已完成',
      'failed': '失败',
      'deleting': '删除中'
    };
    return map[(phase || '').toLowerCase()] || phase || '未知';
  }

  formatRPO(seconds: number): string {
    if (seconds < 60) return `${seconds}秒`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}分钟`;
    if (seconds < 86400) return `${Math.floor(seconds / 3600)}小时`;
    return `${Math.floor(seconds / 86400)}天`;
  }
}
