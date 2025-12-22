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
import { Subject, interval, from, of, Subscription } from 'rxjs';
import { switchMap, takeUntil, concatMap, toArray, catchError } from 'rxjs/operators';
import { BackupPhase, BackupProgressMetadata, BackupSubPhase } from '../../models/backup-progress.model';
import { BackupProgressIndicatorComponent } from '../backup-progress-indicator/backup-progress-indicator.component';
import { BackupType } from '../../utils/backup-progress-strategies';

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
    NzCheckboxModule,
    BackupProgressIndicatorComponent
  ],
  template: `
    <div class="page-wrapper restore-job-management">
      <div class="page-header">
        <div class="title-block">
          <h2>
            <i nz-icon nzType="history" nzTheme="outline" class="page-icon"></i>
            恢复任务管理
          </h2>
          <p>管理恢复作业与进度，支持备份恢复和 PITR 恢复</p>
        </div>
      </div>

      <div class="page-content">
        <div class="content-grid">
            <div class="list-column">
              <nz-card
                class="list-card"
                nzTitle="恢复任务列表"
                [nzExtra]="listExtra"
                [nzLoading]="loadingService.isLoading(loadingKeys.RESTORE_JOB_LIST)">
                <ng-template #listExtra>
                  <div class="list-actions">
                    <button nz-button nzType="default" nzSize="small" (click)="refreshAll()" [disabled]="loadingService.isLoading(loadingKeys.RESTORE_JOB_LIST)">
                      <i nz-icon nzType="reload"></i>
                      刷新
                    </button>
                    <nz-switch [(ngModel)]="listAutoRefresh" (ngModelChange)="toggleAutoRefresh($event)">
                      自动刷新
                    </nz-switch>
                    <button nz-button nzType="primary" nzDanger nzSize="small" [disabled]="!hasCancelableSelection()" (click)="cancelBatch()">
                      <i nz-icon nzType="close"></i>
                      批量取消
                    </button>
                  </div>
                </ng-template>

                <div class="filters-panel">
                  <div class="list-toolbar">
                    <div class="search-box">
                      <i nz-icon nzType="search"></i>
                      <input nz-input placeholder="输入集群名称" [(ngModel)]="searchTerm" (ngModelChange)="applyFilters()" />
                      <button
                        nz-button
                        nzSize="small"
                        class="search-clear"
                        *ngIf="searchTerm"
                        (click)="clearSearch()">
                        <i nz-icon nzType="close"></i>
                      </button>
                    </div>

                    <div class="toolbar-meta">
                      <span class="last-updated">上次更新：{{ lastUpdated | date:'HH:mm:ss' }}</span>
                    </div>
                  </div>

                  <div class="filter-chips">
                    <div class="chip-group">
                      <span class="chip-label">状态</span>
                      <button nz-button nzSize="small" [nzType]="statusFilter==='all' ? 'primary':'default'" (click)="setStatusFilter('all')">全部 ({{statusCounts.all}})</button>
                      <button nz-button nzSize="small" [nzType]="statusFilter==='ongoing' ? 'primary':'default'" (click)="setStatusFilter('ongoing')">进行中 ({{statusCounts.ongoing}})</button>
                      <button nz-button nzSize="small" [nzType]="statusFilter==='completed' ? 'primary':'default'" (click)="setStatusFilter('completed')">已完成 ({{statusCounts.completed}})</button>
                      <button nz-button nzSize="small" [nzType]="statusFilter==='failed' ? 'primary':'default'" (click)="setStatusFilter('failed')">失败 ({{statusCounts.failed}})</button>
                    </div>
                    <div class="chip-group">
                      <span class="chip-label">类型</span>
                      <button nz-button nzSize="small" [nzType]="typeFilter==='all' ? 'primary':'default'" (click)="setTypeFilter('all')">全部</button>
                      <button nz-button nzSize="small" [nzType]="typeFilter==='backup' ? 'primary':'default'" (click)="setTypeFilter('backup')">备份恢复</button>
                      <button nz-button nzSize="small" [nzType]="typeFilter==='pitr' ? 'primary':'default'" (click)="setTypeFilter('pitr')">PITR</button>
                    </div>
                  </div>
                </div>

                <div class="selection-bar" *ngIf="getSelectedJobs().length as selectedCount">
                  <span>已选择 {{ selectedCount }} 项</span>
                  <button nz-button nzType="link" nzSize="small" (click)="clearSelection()">
                    <i nz-icon nzType="delete"></i>
                    清除选择
                  </button>
                </div>

                <div class="table-wrapper">
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
                            <i nz-icon nzType="database"></i>
                            <span>{{ j.clusterName }}</span>
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

                  <div *ngIf="(dataSource?.length || 0) === 0" class="empty-placeholder">
                    <nz-empty nzNotFoundImage="simple" nzNotFoundContent="暂无恢复任务"></nz-empty>
                  </div>
                </div>
              </nz-card>
            </div>

            <aside class="detail-column" *ngIf="!isSmallScreen">
              <ng-container *ngIf="selectedJob; else detailEmpty">
                <nz-card class="detail-card" [nzTitle]="'任务详情 · ' + selectedJob.clusterName" [nzExtra]="detailExtra">
                  <ng-template #detailExtra>
                    <div class="detail-actions">
                      <a *ngIf="grafanaLinkForSelected() as gLink" [href]="gLink" target="_blank" rel="noopener" nz-button nzType="link">
                        <i nz-icon nzType="export"></i>
                        在 Grafana 打开
                      </a>
                      <button nz-button nzSize="small" (click)="toggleRaw()">
                        <i nz-icon nzType="code"></i>
                        原始 JSON
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
                  </ng-template>

                  <div class="detail-body">
                    <section class="status-card">
                      <div class="status-head">
                        <nz-tag [nzColor]="getProgressClass(selectedJob) === 'phase-warn' ? 'error' : (getProgressClass(selectedJob) === 'phase-primary' ? 'processing' : 'default')">
                          {{ selectedJob.phase || '-' }}
                        </nz-tag>
                        <span class="progress-text">{{ getProgress(selectedJob) }}%</span>
                      </div>
                      <app-backup-progress-indicator
                        [metadata]="toBackupMetadata(selectedJob)"
                        [type]="BackupType.RESTORE"
                        size="small"
                        [showDetails]="true">
                      </app-backup-progress-indicator>
                      <nz-alert *ngIf="isFailed(selectedJob)" nzType="error" [nzMessage]="getErrorMessage(selectedJob)" nzShowIcon></nz-alert>
                    </section>

                    <section class="info-card">
                      <h4><i nz-icon nzType="info-circle"></i> 基本信息</h4>
                      <nz-descriptions nzBordered [nzColumn]="2">
                        <nz-descriptions-item nzTitle="源集群">{{ selectedJob.sourceCluster || '-' }}</nz-descriptions-item>
                        <nz-descriptions-item nzTitle="命名空间">{{ selectedJob.namespace || '-' }}</nz-descriptions-item>
                        <nz-descriptions-item nzTitle="恢复类型">{{ getRestoreType(selectedJob!) === 'pitr' ? 'PITR恢复' : '备份恢复' }}</nz-descriptions-item>
                        <nz-descriptions-item nzTitle="当前阶段">{{ selectedJob.stage || '-' }}</nz-descriptions-item>
                        <nz-descriptions-item nzTitle="开始时间">{{ getJobCreationTime(selectedJob) | date:'yyyy-MM-dd HH:mm:ss' }}</nz-descriptions-item>
                        <nz-descriptions-item nzTitle="可取消">{{ !isTerminal(selectedJob) ? '是' : '否' }}</nz-descriptions-item>
                      </nz-descriptions>
                    </section>

                    <section class="conditions-card" *ngIf="selectedJob.conditions?.length; else noConditions">
                      <h4><i nz-icon nzType="calendar"></i> 状态条件</h4>
                      <div class="conditions-list">
                        <div class="condition-item" *ngFor="let c of selectedJob.conditions">
                          <div class="condition-meta">
                            <span class="condition-type">{{ c.type }}</span>
                            <span class="condition-status" [ngClass]="getConditionStatusClass(c.status)">{{ c.status }}</span>
                            <span class="condition-time">{{ c.lastTransitionTime | date:'MM-dd HH:mm:ss' }}</span>
                          </div>
                          <div class="condition-detail" *ngIf="c.reason || c.message">
                            <span class="condition-reason" *ngIf="c.reason">{{ c.reason }}</span>
                            <span class="condition-message" *ngIf="c.message">{{ c.message }}</span>
                          </div>
                        </div>
                      </div>
                    </section>
                    <ng-template #noConditions>
                      <nz-alert nzType="info" nzMessage="暂无状态条件" nzShowIcon></nz-alert>
                    </ng-template>

                    <section class="raw-card" *ngIf="showRaw">
                      <pre>{{ selectedJob | json }}</pre>
                    </section>
                  </div>
                </nz-card>
              </ng-container>
            </aside>
          </div>
        </div>

        <ng-template #detailEmpty>
          <nz-card class="detail-card detail-empty-card">
            <nz-empty nzNotFoundImage="simple" nzNotFoundContent="选择任务查看详情">
              <ng-template #nzNotFoundFooter>
                <p class="empty-hint">点击左侧任务列表中的任意一行来查看详细信息</p>
              </ng-template>
            </nz-empty>
          </nz-card>
        </ng-template>

      <nz-drawer
        class="detail-drawer"
        [nzVisible]="isSmallScreen && !!selectedJob"
        [nzTitle]="selectedJob ? ('任务详情 · ' + selectedJob.clusterName) : ''"
        [nzWidth]="'100%'"
        (nzOnClose)="closeDrawer()">
        <div *ngIf="selectedJob" class="drawer-body">
          <section class="status-card">
            <div class="status-head">
              <nz-tag [nzColor]="getProgressClass(selectedJob) === 'phase-warn' ? 'error' : (getProgressClass(selectedJob) === 'phase-primary' ? 'processing' : 'default')">
                {{ selectedJob.phase || '-' }}
              </nz-tag>
              <span class="progress-text">{{ getProgress(selectedJob) }}%</span>
            </div>
            <app-backup-progress-indicator
              [metadata]="toBackupMetadata(selectedJob)"
              [type]="BackupType.RESTORE"
              size="default"
              [showDetails]="true">
            </app-backup-progress-indicator>
            <nz-alert *ngIf="isFailed(selectedJob)" nzType="error" [nzMessage]="getErrorMessage(selectedJob)" nzShowIcon></nz-alert>
          </section>

          <section class="info-card">
            <h4><i nz-icon nzType="info-circle"></i> 基本信息</h4>
            <nz-descriptions nzBordered [nzColumn]="1">
              <nz-descriptions-item nzTitle="源集群">{{ selectedJob.sourceCluster || '-' }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="命名空间">{{ selectedJob.namespace || '-' }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="恢复类型">{{ getRestoreType(selectedJob!) === 'pitr' ? 'PITR恢复' : '备份恢复' }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="当前阶段">{{ selectedJob.stage || '-' }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="开始时间">{{ getJobCreationTime(selectedJob) | date:'yyyy-MM-dd HH:mm:ss' }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="可取消">{{ !isTerminal(selectedJob) ? '是' : '否' }}</nz-descriptions-item>
            </nz-descriptions>
          </section>

          <section class="conditions-card" *ngIf="selectedJob.conditions?.length">
            <h4><i nz-icon nzType="calendar"></i> 状态条件</h4>
            <div class="conditions-list">
              <div class="condition-item" *ngFor="let c of selectedJob.conditions">
                <div class="condition-meta">
                  <span class="condition-type">{{ c.type }}</span>
                  <span class="condition-status" [ngClass]="getConditionStatusClass(c.status)">{{ c.status }}</span>
                  <span class="condition-time">{{ c.lastTransitionTime | date:'MM-dd HH:mm:ss' }}</span>
                </div>
                <div class="condition-detail" *ngIf="c.reason || c.message">
                  <span class="condition-reason" *ngIf="c.reason">{{ c.reason }}</span>
                  <span class="condition-message" *ngIf="c.message">{{ c.message }}</span>
                </div>
              </div>
            </div>
          </section>
        </div>
      </nz-drawer>
    </div>
  `,
  styles: [`
    .page-wrapper {
      display: flex;
      flex-direction: column;
      gap: 16px;
      padding: 24px;
      min-height: 100%;
      background: transparent; /* 外层背景由 layout/inner-content 负责 */
    }

    .page-header {
      background: #fff;
      padding: 16px;
      border-radius: 10px;
      box-shadow: 0 2px 10px rgba(15, 23, 42, 0.04);
    }

    .title-block h2 {
      margin: 0 0 8px;
      font-size: 22px;
      font-weight: 600;
      color: #1f1f1f;
      display: flex;
      align-items: center;
      gap: 10px;
    }

    .title-block p {
      margin: 0;
      color: #595959;
      line-height: 1.6;
    }

    .page-icon {
      font-size: 22px;
      color: #1890ff;
    }

    .page-content {
      width: 100%;
      display: flex;
      flex-direction: column;
      gap: 16px;
    }

    .content-grid {
      display: grid;
      grid-template-columns: minmax(0, 1fr);
      gap: 16px;
      align-items: flex-start;
    }

    @media (min-width: 1100px) {
      .content-grid {
        grid-template-columns: 1.8fr 1fr;
      }
    }

    .list-card {
      border-radius: 10px;
      box-shadow: 0 2px 10px rgba(15, 23, 42, 0.04);
      border: 1px solid #e0e3e8;
    }

    .list-actions {
      display: flex;
      gap: 12px;
      align-items: center;
      flex-wrap: wrap;
    }

    .list-actions nz-switch {
      display: flex;
      align-items: center;
    }

    .list-toolbar {
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      gap: 12px;
    }

    .filters-panel {
      border: 1px solid #eef1f5;
      background: #fafbfc;
      border-radius: 10px;
      padding: 12px;
      margin-bottom: 12px;
      display: flex;
      flex-direction: column;
      gap: 12px;
    }

    .search-box {
      flex: 1 1 320px;
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 0 12px;
      border: 1px solid #e0e3e8;
      border-radius: 10px;
      background: #fff;
    }

    .search-box i {
      color: rgba(0,0,0,0.45);
    }

    .search-box input {
      background: transparent;
      border: 0;
      box-shadow: none;
    }

    .search-box input:focus {
      border: 0;
      box-shadow: none;
    }

    .search-clear {
      border-radius: 6px;
      padding: 0 8px;
      display: flex;
      align-items: center;
      justify-content: center;
    }

    .toolbar-meta {
      display: flex;
      align-items: center;
      gap: 8px;
      color: rgba(0,0,0,0.45);
      font-size: 12px;
    }

    .filter-chips {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
    }

    .chip-group {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 6px 10px;
      border-radius: 10px;
      background: #fff;
      border: 1px solid #eef1f5;
      flex-wrap: wrap;
    }

    .chip-label {
      font-size: 12px;
      color: rgba(0,0,0,0.45);
    }

    .chip-group button {
      border-radius: 6px;
    }

    .table-wrapper {
      border: 1px solid #e0e3e8;
      border-radius: 10px;
      overflow: hidden;
      background: #fff;
    }

    nz-table {
      overflow: hidden;
    }

    nz-table ::ng-deep thead > tr > th {
      background: #fafbfc;
      font-weight: 600;
    }

    .table-row {
      cursor: pointer;
      transition: background 0.2s ease;
    }

    .table-row:hover {
      background: #f3f9ff;
    }

    .table-row.selected {
      background: #e6f4ff;
    }

    .cluster-info {
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .cluster-info i {
      font-size: 18px;
      color: rgba(0,0,0,0.45);
    }

    .time-info {
      display: flex;
      flex-direction: column;
      gap: 2px;
    }

    .time-date {
      font-weight: 500;
      color: rgba(0,0,0,0.65);
    }

    .time-time {
      font-size: 12px;
      color: rgba(0,0,0,0.45);
    }

    .empty-placeholder {
      padding: 32px 0;
      text-align: center;
    }

    .detail-column {
      position: sticky;
      top: 24px;
    }

    .detail-card {
      border-radius: 10px;
      box-shadow: 0 2px 10px rgba(15, 23, 42, 0.04);
      border: 1px solid #e0e3e8;
    }

    .detail-empty-card {
      min-height: 320px;
      display: flex;
      align-items: center;
      justify-content: center;
    }

    .empty-hint {
      margin: 8px 0 0;
      color: rgba(0,0,0,0.45);
      font-size: 12px;
    }

    .selection-bar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 8px 12px;
      border-radius: 10px;
      border: 1px dashed #d9d9d9;
      background: #fff;
      margin-bottom: 12px;
      color: rgba(0,0,0,0.65);
    }

    .detail-actions {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }

    .detail-body {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }

    .status-card,
    .info-card,
    .conditions-card,
    .raw-card {
      padding: 16px;
      border-radius: 10px;
      border: 1px solid #eef1f5;
      background: #fafbfc;
    }

    .status-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 12px;
      gap: 12px;
    }

    .progress-text {
      font-weight: 600;
      color: rgba(0,0,0,0.65);
    }

    .info-card h4,
    .conditions-card h4,
    .raw-card h4 {
      display: flex;
      align-items: center;
      gap: 8px;
      margin: 0 0 12px;
      font-size: 16px;
      color: rgba(0,0,0,0.75);
    }

    .conditions-list {
      display: flex;
      flex-direction: column;
      gap: 12px;
    }

    .condition-item {
      padding: 12px;
      border-radius: 10px;
      border: 1px solid #eef1f5;
      background: #fff;
      display: flex;
      flex-direction: column;
      gap: 8px;
    }

    .condition-meta {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }

    .condition-type {
      font-weight: 600;
      color: rgba(0,0,0,0.75);
      flex: 1;
    }

    .condition-status.true {
      color: #2e7d32;
    }

    .condition-status.false {
      color: #c62828;
    }

    .condition-time {
      font-size: 12px;
      color: rgba(0,0,0,0.45);
    }

    .raw-card pre {
      max-height: 240px;
      overflow: auto;
      margin: 0;
      background: #0f172a;
      color: #e2e8f0;
      padding: 12px;
      border-radius: 6px;
      font-size: 12px;
    }

    .detail-drawer .drawer-body {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }

    .detail-drawer .status-card,
    .detail-drawer .info-card,
    .detail-drawer .conditions-card {
      background: #fafafa;
    }

    @media (max-width: 767px) {
      .page-wrapper {
        padding: 16px;
      }

      .content-grid {
        gap: 12px;
      }

      .list-actions {
        justify-content: flex-start;
      }

      .search-box {
        flex: 1 1 100%;
      }

      .chip-group {
        width: 100%;
        justify-content: center;
      }
    }

    .phase-warn { background: #ffebee !important; color: #c62828 !important; }
    .phase-primary { background: #e3f2fd !important; color: #1976d2 !important; }
    .phase-accent { background: #f5f5f5 !important; color: #666 !important; }
  `]
})
export class RestoreJobManagementComponent implements OnInit, OnDestroy {
  loadingKeys = LoadingKeys;
  BackupType = BackupType;
  displayedColumns = ['clusterName', 'phase', 'restoreType', 'created', 'actions'];
  dataSource: RestoreJob[] = [];
  selectedJob?: RestoreJobWithStatus;
  private destroy$ = new Subject<void>();
  private listPollingSub?: Subscription;
  private detailPollingSub?: Subscription;

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
    this.listPollingSub = interval(10000).pipe(takeUntil(this.destroy$)).subscribe(() => this.loadJobs());
  }

  stopListPolling(): void {
    this.listPollingSub?.unsubscribe();
    this.listPollingSub = undefined;
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
        if (this.statusFilter === 'ongoing') return p === '已提交' || p === '创建中' || p === '恢复中';
        if (this.statusFilter === 'completed') return p === '已完成';
        if (this.statusFilter === 'failed') return p === '失败';
        return true;
      });
    }
    this.dataSource = result;
  }

  computeCounts(): void {
    const jobs = this.allJobs as RestoreJob[];
    const ongoing = jobs.filter(j => ((j.phase || '') === '已提交') || ((j.phase || '') === '创建中') || ((j.phase || '') === '恢复中')).length;
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
    if (phase === '已提交' || phase === '创建中' || phase === '恢复中' || phase === '已完成' || phase === '失败') {
      return phase;
    }
    switch (p) {
      case 'pending':
        return '已提交';
      case 'creating':
        return '创建中';
      case 'restoring':
        return '恢复中';
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

  toBackupMetadata(job?: RestoreJob): BackupProgressMetadata {
    if (!job) {
      return {
        phase: 'Pending',
        progress: { percentage: 0 }
      };
    }

    const phase = this.mapRestorePhaseToBackupPhase(job.phase);
    const percentage = this.getProgress(job);

    return {
      phase,
      subPhase: undefined,
      startTime: (job as any).createdTime,
      completionTime: undefined,
      progress: {
        percentage: this.normalizePercentage(percentage)
      },
      errorMessage: phase === 'Failed' ? (job as any).message : undefined,
      warnings: [],
      message: (job as any).message
    };
  }

  private mapRestorePhaseToBackupPhase(phase?: string): BackupPhase {
    const p = ((phase || '') as string).toLowerCase();
    switch (p) {
      case 'completed':
      case '已完成':
        return 'Completed';
      case 'failed':
      case '失败':
        return 'Failed';
      case 'creating':
      case 'restoring':
      case 'running':
      case '创建中':
      case '恢复中':
        return 'Running';
      case 'pending':
      case '已提交':
        return 'Pending';
      default:
        return 'Pending';
    }
  }

  private normalizePercentage(value: number | undefined): number {
    if (value === undefined || value === null || isNaN(value)) {
      return 0;
    }
    return Math.max(0, Math.min(100, Math.round(value)));
  }

  reloadSelected(): void {
    if (!this.selectedJob) return;
    this.apiService.getRestoreJob(this.selectedJob.namespace || 'default', this.selectedJob.clusterName as string).subscribe({
      next: (full) => {
        (full as any).phase = this.mapPhaseToCN((full as any)?.phase);
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
    this.detailPollingSub = interval(5000)
      .pipe(
        takeUntil(this.destroy$),
        switchMap(() => this.apiService.getRestoreJob(this.selectedJob!.namespace || 'default', this.selectedJob!.clusterName as string))
      )
      .subscribe({
        next: (full) => {
          (full as any).phase = this.mapPhaseToCN((full as any)?.phase);
          this.selectedJob = full;
          if (this.isTerminal(full)) this.stopPolling();
        }
      });
  }

  stopPolling(): void {
    this.detailPollingSub?.unsubscribe();
    this.detailPollingSub = undefined;
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
    this.destroy$.next();
    this.destroy$.complete();
    try { window.removeEventListener('resize', this.onResize); } catch {}
  }
}
