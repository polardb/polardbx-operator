import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzTabsModule } from 'ng-zorro-antd/tabs';
import { NzProgressModule } from 'ng-zorro-antd/progress';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzSwitchModule } from 'ng-zorro-antd/switch';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzDrawerModule } from 'ng-zorro-antd/drawer';
import { NzDescriptionsModule } from 'ng-zorro-antd/descriptions';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzCheckboxModule } from 'ng-zorro-antd/checkbox';
import { ApiService } from '../../services/api.service';
import { LoadingService, LoadingKeys } from '../../services/loading.service';
import { RestoreJob, RestoreJobWithStatus } from '../../models/restore.model';
import { Subject, interval, from, of } from 'rxjs';
import { switchMap, takeUntil, concatMap, toArray, catchError } from 'rxjs/operators';

@Component({
  selector: 'app-restore-job-management',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    NzCardModule,
    NzButtonModule,
    NzIconModule,
    NzTableModule,
    NzTabsModule,
    NzProgressModule,
    NzTagModule,
    NzDividerModule,
    NzToolTipModule,
    NzFormModule,
    NzInputModule,
    NzSwitchModule,
    NzSpinModule,
    NzEmptyModule,
    NzDrawerModule,
    NzDescriptionsModule,
    NzGridModule,
    NzAlertModule,
    NzCheckboxModule
  ],
  template: `
    <div class="restore-job-management">
      <!-- 页面头部 -->
      <div class="page-header">
        <div class="header-content">
          <h1 class="page-title">
            <i nz-icon nzType="history" class="page-icon"></i>
            恢复任务管理
          </h1>
          <p class="page-description">管理恢复作业与进度，支持备份恢复和PITR恢复</p>
        </div>
      </div>

      <div class="page-content">

        <nz-card 
          class="list-card" 
          nzTitle="恢复任务列表" 
          [nzExtra]="listExtra"
          [nzLoading]="loadingService.isLoading(loadingKeys.RESTORE_JOB_LIST)">
          <ng-template #listExtra>
            <div class="extra-actions" style="display:flex; gap:8px; align-items:center;">
              <button nz-button nzType="default" nzSize="small" (click)="refreshAll()">
                <i nz-icon nzType="reload"></i>
                刷新
              </button>
              <nz-switch [(ngModel)]="listAutoRefresh" (ngModelChange)="toggleAutoRefresh($event)">自动刷新</nz-switch>
              <button nz-button nzType="primary" nzDanger nzSize="small" [disabled]="!hasCancelableSelection()" (click)="cancelBatch()">
                <i nz-icon nzType="close"></i>
                批量取消
              </button>
            </div>
          </ng-template>

          <nz-spin *ngIf="loadingService.isLoading(loadingKeys.RESTORE_JOB_LIST)" nzSimple></nz-spin>

          <div class="list-content">
            <div class="filter-toolbar">
              <div class="search-field" style="display:flex; gap:8px; align-items:center;">
                <input nz-input placeholder="输入集群名称" [(ngModel)]="searchTerm" (ngModelChange)="applyFilters()" />
                <button nz-button nzShape="circle" nzSize="small" *ngIf="searchTerm" (click)="clearSearch()">
                  <i nz-icon nzType="close"></i>
                </button>
              </div>

              <div class="filters-line">
                <div class="filter-group">
                  <span class="filter-label">状态:</span>
                  <button nz-button [nzType]="statusFilter==='all' ? 'primary':'default'" (click)="setStatusFilter('all')">全部 ({{statusCounts.all}})</button>
                  <button nz-button [nzType]="statusFilter==='ongoing' ? 'primary':'default'" (click)="setStatusFilter('ongoing')">进行中 ({{statusCounts.ongoing}})</button>
                  <button nz-button [nzType]="statusFilter==='completed' ? 'primary':'default'" (click)="setStatusFilter('completed')">已完成 ({{statusCounts.completed}})</button>
                  <button nz-button [nzType]="statusFilter==='failed' ? 'primary':'default'" (click)="setStatusFilter('failed')">失败 ({{statusCounts.failed}})</button>
                </div>

                <div class="filter-group">
                  <span class="filter-label">类型:</span>
                  <button nz-button [nzType]="typeFilter==='all' ? 'primary':'default'" (click)="setTypeFilter('all')">全部</button>
                  <button nz-button [nzType]="typeFilter==='backup' ? 'primary':'default'" (click)="setTypeFilter('backup')">备份恢复</button>
                  <button nz-button [nzType]="typeFilter==='pitr' ? 'primary':'default'" (click)="setTypeFilter('pitr')">PITR</button>
                </div>
                <div class="toolbar-spacer"></div>
                <span class="last-updated">上次更新：{{ lastUpdated | date:'HH:mm:ss' }}</span>
              </div>
            </div>

            <div class="table-container">
              <nz-table #nzTable [nzData]="dataSource" [nzFrontPagination]="true" [nzPageSize]="pageSize" [nzShowPagination]="(dataSource.length||0) > pageSize">
                <thead>
                  <tr>
                    <th style="width: 40px;">
                      <label nz-checkbox [ngModel]="isAllSelected()" (ngModelChange)="toggleSelectAll($event)"></label>
                    </th>
                    <th>集群名称</th>
                    <th>状态</th>
                    <th>类型</th>
                    <th>创建时间</th>
                    <th>操作</th>
                  </tr>
                </thead>
                <tbody>
                  <tr *ngFor="let j of nzTable.data" (click)="selectJob(j)" class="table-row" [class.selected]="selectedJob?.clusterName === j.clusterName">
                    <td (click)="$event.stopPropagation()">
                      <label nz-checkbox [(ngModel)]="selection[makeKey(j)]" (ngModelChange)="onRowSelectChange(j, $event)"></label>
                    </td>
                    <td>
                      <div class="cluster-info">
                        <i nz-icon nzType="database" class="cluster-icon"></i>
                        <span class="cluster-name">{{ j.clusterName }}</span>
                      </div>
                    </td>
                    <td>
                      <nz-tag [nzColor]="getProgressClass(j) === 'phase-warn' ? 'error' : (getProgressClass(j) === 'phase-primary' ? 'processing' : 'default')">
                        {{ j.phase || '-' }}
                      </nz-tag>
                    </td>
                    <td>{{ getRestoreType(j) === 'pitr' ? 'PITR' : '备份恢复' }}</td>
                    <td>
                      <div class="time-info">
                        <span class="time-date">{{ getJobCreationTime(j) | date:'MM-dd' }}</span>
                        <span class="time-time">{{ getJobCreationTime(j) | date:'HH:mm' }}</span>
                      </div>
                    </td>
                    <td (click)="$event.stopPropagation()">
                      <button nz-button nzType="link" nzSize="small" (click)="selectJob(j)"><i nz-icon nzType="eye"></i></button>
                      <button nz-button nzType="link" nzSize="small" [disabled]="j.phase === 'Completed' || j.phase === 'Failed'" (click)="cancelJob(j)"><i nz-icon nzType="close"></i></button>
                    </td>
                  </tr>
                </tbody>
              </nz-table>

              <div *ngIf="(dataSource?.length || 0) === 0" class="empty-list">
                <div class="empty-inner">
                  <i nz-icon nzType="inbox" class="empty-icon"></i>
                  <p class="empty-text">暂无恢复任务</p>
                </div>
              </div>
            </div>
          </div>
        </nz-card>

      <!-- 右侧详情面板 (大屏) -->
      <div class="detail-panel" *ngIf="!isSmallScreen && selectedJob; else emptyState">
        <nz-card [nzTitle]="'任务详情 · ' + selectedJob.clusterName">
          <div nz-card-extra>
            <a *ngIf="grafanaLinkForSelected() as gLink; else noGrafana"
               [href]="gLink" target="_blank" rel="noopener" nz-button nzType="link">
              <i nz-icon nzType="external-link"></i>
              在 Grafana 打开
            </a>
            <ng-template #noGrafana></ng-template>
            <button nz-button nzSize="small" (click)="toggleRaw()">
              <i nz-icon nzType="code"></i>
              原始JSON
            </button>
            <button nz-button nzSize="small" (click)="copySelectedJson()">
              <i nz-icon nzType="copy"></i>
              复制
            </button>
            <button nz-button nzType="primary" nzSize="small" *ngIf="isFailed(selectedJob)" (click)="diagnoseSelected()">
              <i nz-icon nzType="medicine-box"></i>
              诊断
            </button>
          </div>
          
          <div class="detail-content">
            <div class="status-section">
              <div class="status-header">
                <nz-tag [nzColor]="getProgressClass(selectedJob) === 'phase-warn' ? 'error' : (getProgressClass(selectedJob) === 'phase-primary' ? 'processing' : 'default')" class="status-chip-large">
                  {{ selectedJob.phase || '-' }}
                </nz-tag>
                <span class="progress-text">{{ getProgress(selectedJob) }}%</span>
              </div>
              <nz-progress [nzPercent]="getProgress(selectedJob)" [nzStatus]="getProgressClass(selectedJob) === 'phase-warn' ? 'exception' : 'active'"></nz-progress>
              <div *ngIf="isFailed(selectedJob)" style="margin-top:8px;">
                <nz-alert nzType="error" [nzMessage]="getErrorMessage(selectedJob)" nzShowIcon></nz-alert>
              </div>
            </div>

            <div class="info-section">
              <h4 class="section-title"><i nz-icon nzType="info-circle"></i> 基本信息</h4>
              <nz-descriptions nzBordered [nzColumn]="2">
                <nz-descriptions-item nzTitle="源集群">{{ selectedJob.sourceCluster || '-' }}</nz-descriptions-item>
                <nz-descriptions-item nzTitle="命名空间">{{ selectedJob.namespace || '-' }}</nz-descriptions-item>
                <nz-descriptions-item nzTitle="恢复类型">{{ getRestoreType(selectedJob!) === 'pitr' ? 'PITR恢复' : '备份恢复' }}</nz-descriptions-item>
                <nz-descriptions-item nzTitle="当前阶段">{{ selectedJob.stage || '-' }}</nz-descriptions-item>
                <nz-descriptions-item nzTitle="开始时间">{{ getJobCreationTime(selectedJob) | date:'yyyy-MM-dd HH:mm:ss' }}</nz-descriptions-item>
                <nz-descriptions-item nzTitle="可取消">{{ (selectedJob.phase !== 'Completed' && selectedJob.phase !== 'Failed') ? '是' : '否' }}</nz-descriptions-item>
              </nz-descriptions>
            </div>

            <div class="conditions-section" *ngIf="selectedJob.conditions?.length">
              <h4 class="section-title"><i nz-icon nzType="calendar"></i> 状态条件</h4>
              <div class="conditions-list">
                <div class="condition-item" *ngFor="let c of selectedJob.conditions">
                  <div class="condition-header">
                    <span class="condition-type">{{ c.type }}</span>
                    <span class="condition-status" [ngClass]="getConditionStatusClass(c.status)">{{ c.status }}</span>
                    <span class="condition-time">{{ c.lastTransitionTime | date:'MM-dd HH:mm:ss' }}</span>
                  </div>
                  <div class="condition-details" *ngIf="c.reason || c.message">
                    <span class="condition-reason" *ngIf="c.reason">{{ c.reason }}</span>
                    <span class="condition-message" *ngIf="c.message">{{ c.message }}</span>
                  </div>
                </div>
              </div>
            </div>

            <div *ngIf="!selectedJob.conditions?.length" class="no-conditions">
              <nz-alert nzType="info" nzMessage="暂无状态条件" nzShowIcon></nz-alert>
            </div>

            <div class="raw-section" *ngIf="showRaw">
              <pre class="raw-json">{{ selectedJob | json }}</pre>
            </div>
          </div>
        </nz-card>
      </div>

      <!-- 小屏抽屉详情 -->
      <nz-drawer [nzVisible]="isSmallScreen && !!selectedJob" [nzTitle]="selectedJob ? ('任务详情 · ' + selectedJob.clusterName) : ''" [nzWidth]="'100%'" (nzOnClose)="closeDrawer()">
        <div *ngIf="selectedJob" class="detail-content">
          <div class="status-section">
            <div class="status-header">
              <nz-tag [nzColor]="getProgressClass(selectedJob) === 'phase-warn' ? 'error' : (getProgressClass(selectedJob) === 'phase-primary' ? 'processing' : 'default')" class="status-chip-large">
                {{ selectedJob.phase || '-' }}
              </nz-tag>
              <span class="progress-text">{{ getProgress(selectedJob) }}%</span>
            </div>
            <nz-progress [nzPercent]="getProgress(selectedJob)" [nzStatus]="getProgressClass(selectedJob) === 'phase-warn' ? 'exception' : 'active'"></nz-progress>
            <div *ngIf="isFailed(selectedJob)" style="margin-top:8px;">
              <nz-alert nzType="error" [nzMessage]="getErrorMessage(selectedJob)" nzShowIcon></nz-alert>
            </div>
          </div>

          <div class="info-section">
            <h4 class="section-title"><i nz-icon nzType="info-circle"></i> 基本信息</h4>
            <nz-descriptions nzBordered [nzColumn]="1">
              <nz-descriptions-item nzTitle="源集群">{{ selectedJob.sourceCluster || '-' }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="命名空间">{{ selectedJob.namespace || '-' }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="恢复类型">{{ getRestoreType(selectedJob!) === 'pitr' ? 'PITR恢复' : '备份恢复' }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="当前阶段">{{ selectedJob.stage || '-' }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="开始时间">{{ getJobCreationTime(selectedJob) | date:'yyyy-MM-dd HH:mm:ss' }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="可取消">{{ (selectedJob.phase !== 'Completed' && selectedJob.phase !== 'Failed') ? '是' : '否' }}</nz-descriptions-item>
            </nz-descriptions>
          </div>

          <div class="conditions-section" *ngIf="selectedJob.conditions?.length">
            <h4 class="section-title"><i nz-icon nzType="calendar"></i> 状态条件</h4>
            <div class="conditions-list">
              <div class="condition-item" *ngFor="let c of selectedJob.conditions">
                <div class="condition-header">
                  <span class="condition-type">{{ c.type }}</span>
                  <span class="condition-status" [ngClass]="getConditionStatusClass(c.status)">{{ c.status }}</span>
                  <span class="condition-time">{{ c.lastTransitionTime | date:'MM-dd HH:mm:ss' }}</span>
                </div>
                <div class="condition-details" *ngIf="c.reason || c.message">
                  <span class="condition-reason" *ngIf="c.reason">{{ c.reason }}</span>
                  <span class="condition-message" *ngIf="c.message">{{ c.message }}</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </nz-drawer>

      <!-- 空状态 -->
      <ng-template #emptyState>
        <div class="empty-state">
          <i nz-icon nzType="file-search" class="empty-state-icon"></i>
          <h3>选择任务查看详情</h3>
          <p>点击左侧任务列表中的任意一行来查看详细信息</p>
        </div>
      </ng-template>
    </div>
  `,
  styles: [`
    .restore-job-management { display: grid; grid-template-columns: 1fr; gap: 16px; padding: 20px; background: #f5f5f5; box-sizing: border-box; min-height: 100vh; }
    
    .page-header {
      margin-bottom: 24px;
    }
    
    .page-title {
      font-size: 20px !important;
      font-weight: 600 !important;
      color: rgba(0, 0, 0, 0.88) !important;
      margin: 0 0 8px 0;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    
    .page-description {
      color: rgba(0, 0, 0, 0.65);
      margin: 0;
      font-size: 14px;
    }
    
    .page-content {
      display: grid;
      grid-template-columns: 1fr;
      gap: 16px;
    }
    
    .page-header-card { margin-bottom: 4px; grid-column: 1 / -1; }
    .list-panel { width: 100%; }
    .detail-panel { width: 100%; }

    @media (min-width: 1200px) {
      .page-content { grid-template-columns: 52% 1fr; }
      .list-panel { min-width: 620px; }
    }

    .list-card, .detail-card { box-shadow: 0 4px 12px rgba(0,0,0,0.06); border-radius: 12px; }
    .loading-bar { height: 3px; }
    .list-content { padding: 0 8px 8px 8px; }

    .filter-toolbar { display: grid; gap: 12px; padding: 12px; background: #ffffff; border-bottom: 1px solid #eee; border-radius: 8px; }
    .filters-line { display: flex; gap: 16px; align-items: center; flex-wrap: wrap; }
    .filter-group { display: flex; align-items: center; gap: 8px; }
    .filter-label { color: #666; font-size: 13px; }
    .chip-group .mat-mdc-chip { cursor: pointer; }
    .toolbar-spacer { flex: 1; }
    .meta { display: flex; align-items: center; gap: 8px; color: #999; }
    .last-updated { font-size: 12px; }
    .search-field { width: 100%; max-width: 360px; }

    .table-container { max-height: calc(100vh - 360px); overflow-y: auto; background: #ffffff; border: 1px solid #e5e7eb; border-radius: 10px; }
    .restore-table { width: 100%; background: white; }
    .table-header { background: #fafafa; font-weight: 600; color: #333; }
    .table-container .mat-mdc-header-row { position: sticky; top: 0; z-index: 2; background: #fafafa; border-bottom: 1px solid #e5e7eb; }
    .table-row { cursor: pointer; transition: background .2s ease; border-bottom: 1px solid #f0f0f0; }
    .table-row:hover { background: #f8f9ff; }
    .table-row.selected { background: #e3f2fd; border-left: 4px solid #2196f3; }

    .cluster-info { display: flex; align-items: center; gap: 8px; }
    .cluster-icon { color: #666; font-size: 20px; }
    .status-chip { font-weight: 500; border-radius: 16px; padding: 4px 12px; font-size: 12px; }

    .table-container mat-paginator { border-top: 1px solid #e5e7eb; padding: 4px 8px; background: #ffffff; }

    .detail-content { padding: 16px 24px 24px; }
    .status-section { margin-bottom: 16px; padding: 16px; background: #f8f9fa; border-radius: 12px; border: 1px solid #e9ecef; }
    .status-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 12px; }
    .status-chip-large { font-weight: 600; font-size: 14px; padding: 8px 16px; border-radius: 20px; }
    .progress-text { font-weight: 600; color: #666; font-size: 16px; }
    .progress-bar { height: 8px; border-radius: 4px; }

    .info-section { margin-bottom: 16px; }
    .section-title { display: flex; align-items: center; gap: 8px; font-size: 16px; font-weight: 600; color: #333; margin: 8px 0 12px; padding-bottom: 8px; border-bottom: 2px solid #e3f2fd; }
    .info-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; }
    .info-item { display: flex; flex-direction: column; gap: 4px; padding: 12px; background: #fafafa; border-radius: 8px; border-left: 4px solid #2196f3; }
    .info-label { font-size: 12px; color: #666; font-weight: 500; letter-spacing: .5px; }
    .info-value { font-size: 14px; color: #333; font-weight: 500; }

    .conditions-section { margin-bottom: 16px; }
    .conditions-list { display: flex; flex-direction: column; gap: 12px; }
    .condition-item { padding: 16px; background: #fafafa; border-radius: 8px; border-left: 4px solid #ff9800; }
    .condition-header { display: flex; align-items: center; gap: 12px; margin-bottom: 8px; }
    .condition-type { font-weight: 600; color: #333; flex: 1; }
    .condition-status { padding: 4px 8px; border-radius: 12px; font-size: 12px; font-weight: 500; }
    .condition-status.true { background: #e8f5e8; color: #2e7d32; }
    .condition-status.false { background: #ffebee; color: #c62828; }
    .condition-time { font-size: 12px; color: #666; }

    .no-conditions { display: flex; align-items: center; gap: 8px; padding: 20px; color: #999; justify-content: center; background: #fafafa; }

    .raw-section { margin-top: 16px; }
    .raw-json { max-height: 280px; overflow: auto; background: #0f172a; color: #e2e8f0; padding: 12px; border-radius: 8px; font-size: 12px; }

    .phase-warn { background: #ffebee !important; color: #c62828 !important; }
    .phase-primary { background: #e3f2fd !important; color: #1976d2 !important; }
    .phase-accent { background: #f5f5f5 !important; color: #666 !important; }

    /* Empty list styling */
    .empty-list { display: flex; justify-content: center; align-items: center; height: 240px; }
    .empty-inner { display: flex; flex-direction: column; align-items: center; gap: 8px; color: #9aa1a9; }
    .empty-icon { font-size: 44px; width: 44px; height: 44px; color: #c0c6cc; }
    .empty-text { margin: 0; font-size: 14px; color: #97a0aa; }
  `]
})
export class RestoreJobManagementComponent implements OnInit, OnDestroy {
  loadingKeys = LoadingKeys;
  displayedColumns = ['clusterName', 'phase', 'restoreType', 'created', 'actions'];
  dataSource: RestoreJob[] = [];
  selectedJob?: RestoreJobWithStatus;
  private destroy$ = new Subject<void>();

  allJobs: RestoreJob[] = [];
  searchTerm = '';
  statusFilter: 'all' | 'ongoing' | 'completed' | 'failed' = 'all';
  typeFilter: 'all' | 'backup' | 'pitr' = 'all';
  statusCounts = { all: 0, ongoing: 0, completed: 0, failed: 0 };
  lastUpdated: Date = new Date();
  showRaw = false;
  listAutoRefresh = true;
  pageSize = 10;
  // 选择与小屏
  selection: Record<string, boolean> = {};
  isSmallScreen = typeof window !== 'undefined' ? window.innerWidth < 1200 : false;

  constructor(
    private apiService: ApiService,
    public loadingService: LoadingService,
    private message: NzMessageService
  ) {}

  ngOnInit(): void {
    this.loadJobs();
    this.startListPolling();
    try { window.addEventListener('resize', this.onResize); } catch {}
  }

  loadJobs(): void {
    this.apiService.listRestoreJobs().subscribe({
      next: (jobs) => {
        const mapped = (jobs || []).map(j => ({
          ...j,
          phase: this.mapPhaseToCN((j as any)?.['phase'])
        } as RestoreJob));
        this.allJobs = mapped;
        this.computeCounts();
        this.applyFilters();
        this.lastUpdated = new Date();
        if (this.selectedJob && this.isTerminal(this.selectedJob)) {
          // 停止轮询仅指详情轮询
        }
      },
      error: () => { this.allJobs = []; this.dataSource = []; this.computeCounts(); }
    });
  }

  refreshAll(): void { this.loadJobs(); }
  clearSearch(): void { this.searchTerm = ''; this.applyFilters(); }
  setStatusFilter(k: 'all'|'ongoing'|'completed'|'failed'): void { this.statusFilter = k; this.applyFilters(); }
  setTypeFilter(k: 'all'|'backup'|'pitr'): void { this.typeFilter = k; this.applyFilters(); }

  toggleAutoRefresh(checked: boolean): void {
    this.listAutoRefresh = checked;
    if (checked) this.startListPolling(); else this.stopListPolling();
  }

  startListPolling(): void {
    this.stopListPolling();
    if (!this.listAutoRefresh) return;
    interval(10000).pipe(takeUntil(this.destroy$)).subscribe(() => this.loadJobs());
  }

  stopListPolling(): void {
    this.destroy$.next();
  }

  applyFilters(): void {
    let result = [...this.allJobs];
    const term = (this.searchTerm || '').trim().toLowerCase();
    if (term) {
      result = result.filter(j => (j.clusterName || '').toLowerCase().includes(term));
    }
    if (this.typeFilter !== 'all') {
      result = result.filter(j => this.typeFilter === 'pitr' ? this.getRestoreType(j) === 'pitr' : this.getRestoreType(j) !== 'pitr');
    }
    if (this.statusFilter !== 'all') {
      result = result.filter(j => {
        const p = j.phase || '';
        if (this.statusFilter === 'ongoing') return p === '已提交' || p === '创建中';
        if (this.statusFilter === 'completed') return p === '已完成';
        if (this.statusFilter === 'failed') return p === '失败';
        return true;
      });
    }
    this.dataSource = result;
  }

  computeCounts(): void {
    const jobs = this.allJobs as RestoreJob[];
    const ongoing = jobs.filter(j => ((j.phase || '') === '已提交') || ((j.phase || '') === '创建中')).length;
    const completed = jobs.filter(j => (j.phase || '') === '已完成').length;
    const failed = jobs.filter(j => (j.phase || '') === '失败').length;
    this.statusCounts = { all: jobs.length, ongoing, completed, failed };
  }

  selectJob(job: RestoreJob): void {
    this.selectedJob = job as RestoreJobWithStatus;
    this.startPolling();
    this.reloadSelected();
    if (this.isSmallScreen) {
      // 抽屉通过 selectedJob 控制打开
    }
  }

  cancelJob(job: RestoreJob): void {
    if (!confirm(`确定取消恢复任务 (cluster=${job.clusterName}) 吗？`)) return;
    this.apiService.cancelRestoreJob(job.namespace || 'default', job.clusterName).subscribe({
      next: () => { this.message.success('取消请求已提交'); this.loadJobs(); },
      error: () => { this.message.error('取消失败'); }
    });
  }

  phaseClass(phase?: string): string {
    const p = (phase || '').toLowerCase();
    if (p === 'failed') return 'phase-warn';
    if (p === 'completed' || p === 'running' || p === '已完成' || p === '运行中' || p === '创建中' || p === '恢复中') return 'phase-primary';
    return 'phase-accent';
  }

  private mapPhaseToCN(phase?: string): string {
    const p = (phase || '').toLowerCase();
    switch (p) {
      case 'pending':
        return '已提交';
      case 'restoring':
      case 'creating':
        return '创建中';
      case 'running':
      case 'completed':
        return '已完成';
      case 'failed':
        return '失败';
      default:
        return phase || '-';
    }
  }

  private mapPhaseOrder(phase?: string): number {
    const p = (phase || '').toLowerCase();
    if (p === 'failed' || p === '失败') return 3;
    if (p === 'completed' || p === '已完成') return 2;
    if (p === 'creating' || p === 'restoring' || p === '创建中' || p === '恢复中') return 1;
    return 0; // 已提交/其他
  }

  getProgress(job?: RestoreJob): number {
    const p = ((job?.phase || '') as string).toLowerCase();
    switch (p) {
      case 'pending':
      case '已提交':
        return 10;
      case 'creating':
      case 'restoring':
      case '创建中':
      case '恢复中':
        return 60;
      case 'running':
      case 'completed':
      case '已完成':
        return 100;
      case 'failed':
      case '失败':
        return 0;
    }
    return 30;
  }

  reloadSelected(): void {
    if (!this.selectedJob) return;
    this.apiService.getRestoreJob(this.selectedJob.namespace || 'default', this.selectedJob.clusterName as string).subscribe({
      next: (full) => {
        this.selectedJob = full;
        const idx = this.allJobs.findIndex(j => j.clusterName === (full as any)?.['clusterName'] && j.namespace === (full as any)?.['namespace']);
        if (idx >= 0) {
          const clone = [...this.allJobs];
          clone[idx] = full as any;
          this.allJobs = clone;
          this.computeCounts();
          this.applyFilters();
        }
        if (this.isTerminal(full)) this.stopPolling();
      }
    });
  }

  // 小屏抽屉关闭
  closeDrawer(): void {
    this.selectedJob = undefined;
  }

  onResize = () => {
    try { this.isSmallScreen = window.innerWidth < 1200; } catch {}
  };

  startPolling(): void {
    this.stopPolling();
    if (!this.selectedJob) return;
    interval(5000).pipe(
      takeUntil(this.destroy$),
      switchMap(() => this.apiService.getRestoreJob(this.selectedJob!.namespace || 'default', this.selectedJob!.clusterName as string))
    ).subscribe({ next: (full) => {
      this.selectedJob = full;
      if (this.isTerminal(full)) this.stopPolling();
    }});
  }

  stopPolling(): void {
    this.destroy$.next();
  }

  cancelSelected(): void {
    if (!this.selectedJob) return;
    this.cancelJob(this.selectedJob);
  }

  isTerminal(job: RestoreJob): boolean {
    const p = ((job.phase || '') as string).toLowerCase();
    return p === 'failed' || p === 'completed' || p === '已完成' || p === '失败';
  }

  getProgressClass(job?: RestoreJob): string {
    const p = ((job?.phase || '') as string).toLowerCase();
    if (p === 'failed' || p === '失败') return 'phase-warn';
    if (p === 'completed' || p === '已完成' || p === 'running') return 'phase-primary';
    return 'phase-accent';
  }

  getConditionStatusClass(status?: string): string {
    return status?.toLowerCase() || 'unknown';
  }

  getRestoreType(job: RestoreJob | RestoreJobWithStatus): 'pitr' | 'backup' {
    return job?.restoreSpec?.time ? 'pitr' : 'backup';
  }

  toggleRaw(): void { this.showRaw = !this.showRaw; }

  copySelectedJson(): void {
    if (!this.selectedJob) return;
    const text = JSON.stringify(this.selectedJob, null, 2);
    if (navigator?.clipboard?.writeText) {
      navigator.clipboard.writeText(text).then(() => {
        this.message.success('已复制 JSON');
      }).catch(() => this.message.error('复制失败'));
    } else {
      const ta = document.createElement('textarea');
      ta.value = text;
      document.body.appendChild(ta);
      ta.select();
      try { document.execCommand('copy'); this.message.success('已复制 JSON'); } catch {}
      document.body.removeChild(ta);
    }
  }

  isFailed(job?: RestoreJob): boolean {
    const p = ((job?.phase || '') as string).toLowerCase();
    return p === 'failed' || p === '失败';
  }

  getErrorMessage(job?: RestoreJob): string {
    if (!job) return '任务失败，请查看状态条件';
    const conds = (job as any)?.conditions as any[] || [];
    const errorCond = conds.find(c => (c?.message || '').trim());
    return (errorCond?.message as string) || '任务失败，请查看状态条件';
  }

  diagnoseSelected(): void {
    if (!this.selectedJob) return;
    const ns = this.selectedJob.namespace || 'default';
    const cluster = this.selectedJob.clusterName || '';
    // 跳转到诊断页并自动触发，便于查看历史与下载
    const url = `/operations/diagnostics?namespace=${encodeURIComponent(ns)}&cluster=${encodeURIComponent(cluster)}&autoStart=1`;
    window.location.href = url;
  }

  grafanaLinkForSelected(): string | null {
    const base = localStorage.getItem('grafanaURL') || '';
    if (!base || !this.selectedJob) return null;
    const ns = this.selectedJob.namespace || 'default';
    const cluster = this.selectedJob.clusterName || '';
    const clusterParam = cluster ? `&var-cluster=${encodeURIComponent(cluster)}` : '';
    return `${base}/d/polardbx-monitor?orgId=1&var-namespace=${encodeURIComponent(ns)}${clusterParam}`;
  }

  getJobCreationTime(job: RestoreJob): string | null {
    // Try to get creationTimestamp from extended type
    const extendedJob = job as any;
    return extendedJob?.creationTimestamp || null;
  }

  // ---------- 多选与批量取消 ----------
  makeKey(job: RestoreJob): string { return `${job.namespace || 'default'}/${job.clusterName}`; }
  isAllSelected(): boolean {
    const keys = (this.dataSource || []).map(j => this.makeKey(j));
    if (keys.length === 0) return false;
    return keys.every(k => !!this.selection[k]);
  }
  toggleSelectAll(checked: boolean): void {
    const keys = (this.dataSource || []).map(j => this.makeKey(j));
    keys.forEach(k => this.selection[k] = checked);
  }
  onRowSelectChange(job: RestoreJob, checked: boolean): void {
    this.selection[this.makeKey(job)] = checked;
  }
  getSelectedJobs(): RestoreJob[] {
    const map = this.selection || {};
    return (this.dataSource || []).filter(j => !!map[this.makeKey(j)]);
  }
  hasCancelableSelection(): boolean {
    return this.getSelectedJobs().some(j => !this.isTerminal(j));
  }
  clearSelection(): void { this.selection = {}; }
  cancelBatch(): void {
    const targets = this.getSelectedJobs().filter(j => !this.isTerminal(j));
    if (targets.length === 0) { this.message.info('未选择可取消的任务'); return; }
    if (!confirm(`确定批量取消选中的 ${targets.length} 个任务吗？`)) return;
    from(targets).pipe(
      concatMap(j => this.apiService.cancelRestoreJob(j.namespace || 'default', j.clusterName).pipe(
        catchError(() => { this.message.error(`取消失败: ${j.clusterName}`); return of(null); })
      )),
      toArray()
    ).subscribe({
      next: () => { this.message.success('批量取消请求已提交'); this.clearSelection(); this.loadJobs(); },
      error: () => { this.message.error('批量取消过程中发生错误'); this.loadJobs(); }
    });
  }

  ngOnDestroy(): void {
    this.stopPolling();
    this.stopListPolling();
    try { window.removeEventListener('resize', this.onResize); } catch {}
  }
}