import { Component, inject, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormBuilder, FormGroup, Validators, ReactiveFormsModule } from '@angular/forms';
import { FormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';
import { ApiService } from '../../services/api.service';
import { LoadingService, LoadingKeys } from '../../services/loading.service';
import { PolarDBXBackup } from '../../models/backup.model';
import { NamespaceService } from '../../services/namespace.service';
import { Subject, timer, of } from 'rxjs';
import { catchError, takeUntil } from 'rxjs/operators';
import { BackupPhase, BackupProgressMetadata, BackupSubPhase } from '../../models/backup-progress.model';
import { BackupProgressIndicatorComponent } from '../backup-progress-indicator/backup-progress-indicator.component';
import { BackupType } from '../../utils/backup-progress-strategies';
import { 
  mapBackupPhaseToUIStatus, 
  isBackupRunning, 
  isBackupCompletedStatus,
  isBackupFailedStatus,
  getPhaseDisplayLabel,
  getPhaseStatusColor,
  getPhaseStatusStyle,
  canDeleteBackup
} from '../../models/enums/backup-phase-helpers';

// Ant Design Zorro imports
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzTabsModule } from 'ng-zorro-antd/tabs';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzSwitchModule } from 'ng-zorro-antd/switch';
import { NzProgressModule } from 'ng-zorro-antd/progress';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzStepsModule } from 'ng-zorro-antd/steps';
import { NzPopconfirmModule } from 'ng-zorro-antd/popconfirm';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzStatisticModule } from 'ng-zorro-antd/statistic';
import { NzDescriptionsModule } from 'ng-zorro-antd/descriptions';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzDrawerModule } from 'ng-zorro-antd/drawer';

@Component({
  selector: 'app-backup-management',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    FormsModule,
    RouterModule,
    NzCardModule,
    NzFormModule,
    NzInputModule,
    NzSelectModule,
    NzButtonModule,
    NzIconModule,
    NzTableModule,
    NzTabsModule,
    NzGridModule,
    NzDividerModule,
    NzTagModule,
    NzToolTipModule,
    NzSwitchModule,
    NzProgressModule,
    NzEmptyModule,
    NzStepsModule,
    NzPopconfirmModule,
    NzSpinModule,
    NzStatisticModule,
    NzDescriptionsModule,
    NzAlertModule,
    NzDrawerModule,
    BackupProgressIndicatorComponent
  ],
  template: `
    <div class="page-wrapper backup-management-container">
      <div class="page-header">
        <div class="header-content">
          <h1 class="page-title">
            <i nz-icon nzType="save" class="page-icon"></i>
            存储备份管理
          </h1>
          <p class="page-description">管理 PolarDB-X 集群的全量备份，支持手动创建和存储连通性检测</p>
        </div>
      </div>

      <div class="page-content">
        <!-- 统计概览 -->
        <div class="stats-section">
          <nz-card class="overview-card" [nzBodyStyle]="{ padding: '16px' }" nzBordered="false">
            <div nz-row [nzGutter]="16">
            <div nz-col [nzSpan]="6">
              <nz-card class="stat-card">
                <nz-statistic 
                  nzTitle="总备份数量" 
                  [nzValue]="backups.length" 
                  [nzValueStyle]="{ color: 'var(--primary-color)' }">
                  <ng-template #nzPrefix>
                    <i nz-icon nzType="save"></i>
                  </ng-template>
                </nz-statistic>
              </nz-card>
            </div>
            <div nz-col [nzSpan]="6">
              <nz-card class="stat-card">
                <nz-statistic 
                  nzTitle="运行中" 
                  [nzValue]="getRunningBackupsCount()" 
                  [nzValueStyle]="{ color: '#52c41a' }">
                  <ng-template #nzPrefix>
                    <i nz-icon nzType="reload"></i>
                  </ng-template>
                </nz-statistic>
              </nz-card>
            </div>
            <div nz-col [nzSpan]="6">
              <nz-card class="stat-card">
                <nz-statistic 
                  nzTitle="已完成" 
                  [nzValue]="getCompletedBackupsCount()" 
                  [nzValueStyle]="{ color: 'var(--primary-color)' }">
                  <ng-template #nzPrefix>
                    <i nz-icon nzType="check-circle"></i>
                  </ng-template>
                </nz-statistic>
              </nz-card>
            </div>
            <div nz-col [nzSpan]="6">
              <nz-card class="stat-card">
                <nz-statistic 
                  nzTitle="失败" 
                  [nzValue]="getFailedBackupsCount()" 
                  [nzValueStyle]="{ color: '#ff4d4f' }">
                  <ng-template #nzPrefix>
                    <i nz-icon nzType="close-circle"></i>
                  </ng-template>
                </nz-statistic>
              </nz-card>
            </div>
            </div>
          </nz-card>
        </div>

        <nz-tabset class="main-tabs" [nzTabPosition]="'top'" [nzSelectedIndex]="selectedTabIndex" (nzSelectedIndexChange)="onTabIndexChange($event)">
          <!-- 备份列表 -->
          <nz-tab nzTitle="备份列表">
            <ng-template nz-tab>
              <div class="tab-content">
                <nz-card 
                  class="list-card" 
                  nzTitle="全量备份" 
                  [nzExtra]="listExtra"
                  [nzLoading]="loadingService.isLoading(LoadingKeys.BACKUPS_LIST)">
                  <ng-template #listExtra>
                    <div class="extra-actions">
                      <nz-select 
                        [(ngModel)]="selectedListCluster" 
                        nzPlaceHolder="选择集群"
                        style="min-width: 220px;"
                        (ngModelChange)="onListClusterChange($event)">
                        <nz-option 
                          *ngFor="let cluster of availableClusters" 
                          [nzValue]="cluster.metadata.name" 
                          [nzLabel]="cluster.metadata.name">
                        </nz-option>
                      </nz-select>
                      <button 
                        nz-button 
                        nzType="default" 
                        nzSize="small" 
                        (click)="refreshBackups()"
                        [nzLoading]="loadingService.isLoading(LoadingKeys.BACKUPS_LIST)">
                        <i nz-icon nzType="reload"></i>
                    刷新
                  </button>
                      <button 
                        nz-button 
                        nzType="primary" 
                        nzSize="small" 
                        (click)="switchToCreateTab()">
                        <i nz-icon nzType="plus"></i>
                    新建备份
                  </button>
                      <a 
                        *ngIf="grafanaURL" 
                        [href]="grafanaURL" 
                        target="_blank" 
                        rel="noopener"
                        nz-button 
                        nzType="link" 
                        nzSize="small">
                        <i nz-icon nzType="line-chart"></i>
                        Grafana 监控
                  </a>
                </div>
                  </ng-template>
                  
                  <div class="list-content">
                    <nz-table 
                      #basicTable 
                      [nzData]="backups" 
                      [nzLoading]="loadingService.isLoading(LoadingKeys.BACKUPS_LIST)"
                      [nzPageSize]="10"
                      [nzShowPagination]="backups.length > 10"
                      [nzScroll]="{ x: '1200px' }">
                      <thead>
                        <tr>
                          <th nzWidth="200px">备份名称</th>
                          <th nzWidth="120px">集群</th>
                          <th nzWidth="100px">状态</th>
                          <th nzWidth="120px">进度</th>
                          <th nzWidth="100px">大小</th>
                          <th nzWidth="140px">存储连通性</th>
                          <th nzWidth="160px">创建时间</th>
                          <th nzWidth="150px" nzRight>操作</th>
                        </tr>
                      </thead>
                      <tbody>
                        <tr *ngFor="let backup of basicTable.data">
                          <td>
                            <div class="backup-name-cell">
                              <span class="resource-name">{{ backup.metadata.name }}</span>
                              <div *ngIf="isRunningEx(backup)" class="progress-indicator">
                                <app-backup-progress-indicator
                                  [metadata]="toBackupMetadata(backup)"
                                  [type]="BackupType.POLARDBX"
                                  size="small"
                                  [showDetails]="false">
                                </app-backup-progress-indicator>
                      </div>
                      </div>
                          </td>
                          <td>
                            <nz-tag nzColor="blue">{{ backup.spec.cluster.name || '-' }}</nz-tag>
                          </td>
                          <td>
                            <nz-tag [nzColor]="getBackupStatusColor(backup)">
                              {{ mapPhaseText(backup) }}
                            </nz-tag>
                          </td>
                          <td>
                            <div class="progress-cell">
                              <app-backup-progress-indicator
                                [metadata]="toBackupMetadata(backup)"
                                [type]="BackupType.POLARDBX"
                                size="small"
                                [showDetails]="false">
                              </app-backup-progress-indicator>
                            </div>
                          </td>
                          <td>
                            <span class="size-text">{{ formatSize(backup) }}</span>
                          </td>
                          <td>
                            <nz-tag 
                              [nzColor]="getStorageConnectivityColor(backup)"
                              [nz-tooltip]="getStorageConnectivityTooltip(backup)">
                              {{ getStorageConnectivityText(backup) }}
                            </nz-tag>
                          </td>
                          <td>
                            <span class="date-text">{{ formatDate(backup.metadata.creationTimestamp) }}</span>
                          </td>
                          <td nzRight>
                            <div class="action-buttons">
                              <button 
                                nz-button 
                                nzType="text" 
                                nzSize="small"
                                nz-tooltip="查看详情"
                                (click)="showBackupDetails(backup)">
                                <i nz-icon nzType="eye"></i>
                      </button>
                              <button 
                                *ngIf="canDelete(backup)"
                                nz-button 
                                nzType="text" 
                                nzSize="small"
                                nz-tooltip="删除"
                                nz-popconfirm
                                nzPopconfirmTitle="确定要删除这个备份吗？"
                                (nzOnConfirm)="deleteBackup(backup)">
                                <i nz-icon nzType="delete"></i>
                      </button>
                              <button 
                                *ngIf="canForceDelete(backup)"
                                nz-button 
                                nzType="text" 
                                nzSize="small" 
                                nzDanger
                                nz-tooltip="强制删除"
                                nz-popconfirm
                                nzPopconfirmTitle="确定要强制删除这个备份吗？此操作不可恢复。"
                                (nzOnConfirm)="forceDeleteBackup(backup)">
                                <i nz-icon nzType="delete"></i>
                      </button>
                            </div>
                          </td>
                        </tr>
                      </tbody>
                    </nz-table>
                    
                    <div *ngIf="!loadingService.isLoading(LoadingKeys.BACKUPS_LIST) && backups.length === 0" class="empty-state">
                      <nz-empty 
                        nzNotFoundImage="simple" 
                        nzNotFoundContent="暂无存储备份记录（请先选择目标集群）">
                        <div nz-empty-footer>
                          <button nz-button nzType="primary" (click)="switchToCreateTab()">
                            <i nz-icon nzType="plus"></i>
                            创建备份
                          </button>
              </div>
                      </nz-empty>
                    </div>
                  </div>
                </nz-card>
              </div>
            </ng-template>
          </nz-tab>

          <!-- 创建备份 -->
          <nz-tab nzTitle="创建备份">
            <ng-template nz-tab>
              <div class="tab-content">
                <div class="configuration-wrapper">
                  <!-- 创建步骤指引 -->
                  <nz-card class="steps-card" nzTitle="创建步骤">
                    <nz-steps [nzCurrent]="createStepIndex" nzSize="small">
                      <nz-step nzTitle="基本信息" nzDescription="名称和集群"></nz-step>
                      <nz-step nzTitle="备份配置" nzDescription="存储和策略"></nz-step>
                      <nz-step nzTitle="高级选项" nzDescription="可选配置"></nz-step>
                      <nz-step nzTitle="创建确认" nzDescription="提交备份"></nz-step>
                    </nz-steps>
                  </nz-card>

                  <!-- 配置表单 -->
                  <div class="config-sections">
                    <!-- 基本信息 -->
                    <nz-card *ngIf="createStepIndex === 0" class="config-card" nzTitle="基本信息" [nzExtra]="basicExtra">
                      <ng-template #basicExtra>
                        <i nz-icon nzType="info-circle" class="section-icon"></i>
                      </ng-template>
                      <form [formGroup]="createForm" nz-form nzLayout="vertical">
                        <div nz-row nzGutter="16">
                          <div nz-col [nzSpan]="8">
                            <nz-form-item>
                              <nz-form-label nzRequired>备份名称</nz-form-label>
                              <nz-form-control nzHasFeedback nzErrorTip="请输入有效的备份名称">
                                <input 
                                  nz-input 
                                  formControlName="name" 
                                  placeholder="my-backup-20241215" />
                              </nz-form-control>
                            </nz-form-item>
                    </div>
                          <div nz-col [nzSpan]="8">
                            <nz-form-item>
                              <nz-form-label nzRequired>命名空间</nz-form-label>
                              <nz-form-control nzErrorTip="请选择命名空间">
                                <nz-select 
                                  formControlName="namespace" 
                                  nzPlaceHolder="选择命名空间">
                                  <nz-option 
                                    *ngFor="let ns of availableNamespaces" 
                                    [nzValue]="ns" 
                                    [nzLabel]="ns">
                                  </nz-option>
                                </nz-select>
                              </nz-form-control>
                            </nz-form-item>
                    </div>
                          <div nz-col [nzSpan]="8">
                            <nz-form-item>
                              <nz-form-label nzRequired>目标集群</nz-form-label>
                              <nz-form-control nzErrorTip="请选择要备份的集群">
                                <nz-select 
                                  formControlName="clusterName" 
                                  nzPlaceHolder="选择集群"
                                  (ngModelChange)="onClusterChange($event)">
                                  <nz-option 
                                    *ngFor="let cluster of availableClusters" 
                                    [nzValue]="cluster.metadata.name" 
                                    [nzLabel]="cluster.metadata.name">
                                  </nz-option>
                                </nz-select>
                              </nz-form-control>
                            </nz-form-item>
                    </div>
                        </div>
                      </form>
                    </nz-card>

                    <!-- 备份配置 -->
                    <nz-card *ngIf="createStepIndex === 1" class="config-card" nzTitle="备份配置" [nzExtra]="storageExtra">
                      <ng-template #storageExtra>
                        <i nz-icon nzType="cloud-server" class="section-icon"></i>
                      </ng-template>
                      <form [formGroup]="createForm" nz-form nzLayout="vertical">
                        <div nz-row nzGutter="16">
                          <div nz-col [nzSpan]="12">
                            <nz-form-item>
                              <nz-form-label 
                                nzRequired
                                nz-tooltip 
                                nzTooltipTitle="存储提供商名称，需在 HPFS 配置中定义">
                                存储提供商
                              </nz-form-label>
                              <nz-form-control nzErrorTip="请输入存储提供商名称">
                                <input 
                                  nz-input 
                                  formControlName="storageName" 
                                  placeholder="my-storage-provider" />
                              </nz-form-control>
                            </nz-form-item>
                    </div>
                          <div nz-col [nzSpan]="12">
                            <nz-form-item>
                              <nz-form-label 
                                nzRequired
                                nz-tooltip 
                                nzTooltipTitle="存储 Sink 名称，对应 HPFS 配置中的 sink">
                                存储 Sink
                              </nz-form-label>
                              <nz-form-control nzErrorTip="请输入存储 Sink 名称">
                                <input 
                                  nz-input 
                                  formControlName="sink" 
                                  placeholder="backup-sink" />
                              </nz-form-control>
                            </nz-form-item>
                          </div>
                        </div>
                        <div nz-row>
                          <div nz-col [nzSpan]="24">
                            <nz-divider nzText="配置提示" nzOrientation="left"></nz-divider>
                            <nz-alert
                              nzType="info"
                              nzShowIcon
                              nzMessage="存储配置说明"
                              nzDescription="存储配置需要预先在 polardbx-hpfs-config ConfigMap 中定义，支持 S3、OSS、SFTP 等多种存储类型。">
                            </nz-alert>
                          </div>
                        </div>
                      </form>
                    </nz-card>

                    <!-- 高级选项 -->
                    <nz-card *ngIf="createStepIndex === 2" class="config-card" nzTitle="高级选项" [nzExtra]="advancedExtra">
                      <ng-template #advancedExtra>
                        <i nz-icon nzType="control" class="section-icon"></i>
                      </ng-template>
                      <form [formGroup]="createForm" nz-form nzLayout="vertical">
                        <div nz-row nzGutter="16">
                          <div nz-col [nzSpan]="12">
                            <nz-form-item>
                              <nz-form-label 
                                nz-tooltip 
                                nzTooltipTitle="备份任务的优先级，影响资源分配">
                                任务优先级
                              </nz-form-label>
                              <nz-form-control>
                                <nz-select 
                                  formControlName="priority" 
                                  nzPlaceHolder="选择优先级">
                                  <nz-option nzValue="low" nzLabel="低优先级"></nz-option>
                                  <nz-option nzValue="normal" nzLabel="普通优先级"></nz-option>
                                  <nz-option nzValue="high" nzLabel="高优先级"></nz-option>
                                </nz-select>
                              </nz-form-control>
                            </nz-form-item>
                          </div>
                          <div nz-col [nzSpan]="12">
                            <nz-form-item>
                              <nz-form-label 
                                nz-tooltip 
                                nzTooltipTitle="启用压缩可减少存储空间占用">
                                启用压缩
                              </nz-form-label>
                              <nz-form-control>
                                <nz-switch 
                                  formControlName="enableCompression"
                                  nzCheckedChildren="开" 
                                  nzUnCheckedChildren="关">
                                </nz-switch>
                              </nz-form-control>
                            </nz-form-item>
                          </div>
                        </div>
                        <div nz-row>
                          <div nz-col [nzSpan]="24">
                            <nz-form-item>
                              <nz-form-label>备份描述</nz-form-label>
                              <nz-form-control>
                                <textarea 
                                  nz-input 
                                  formControlName="description" 
                                  placeholder="备份说明（可选）"
                                  [nzAutosize]="{ minRows: 3, maxRows: 6 }">
                                </textarea>
                              </nz-form-control>
                            </nz-form-item>
                          </div>
                        </div>
                      </form>
                    </nz-card>
                    <!-- 创建确认 -->
                    <nz-card *ngIf="createStepIndex === 3" class="config-card" nzTitle="确认与提交">
                      <nz-descriptions nzBordered [nzColumn]="1">
                        <nz-descriptions-item nzTitle="命名空间">{{ createForm.value.namespace }}</nz-descriptions-item>
                        <nz-descriptions-item nzTitle="目标集群">{{ createForm.value.clusterName }}</nz-descriptions-item>
                        <nz-descriptions-item nzTitle="备份名称">{{ createForm.value.name || '-' }}</nz-descriptions-item>
                        <nz-descriptions-item nzTitle="存储提供商">{{ createForm.value.storageName }}</nz-descriptions-item>
                        <nz-descriptions-item nzTitle="存储 Sink">{{ createForm.value.sink }}</nz-descriptions-item>
                        <nz-descriptions-item nzTitle="任务优先级">{{ createForm.value.priority }}</nz-descriptions-item>
                        <nz-descriptions-item nzTitle="启用压缩">{{ createForm.value.enableCompression ? '是' : '否' }}</nz-descriptions-item>
                        <nz-descriptions-item nzTitle="描述">{{ createForm.value.description || '-' }}</nz-descriptions-item>
                      </nz-descriptions>
                    </nz-card>
                    </div>

                  <!-- 操作按钮 -->
                  <div class="action-bar">
                    <button nz-button nzSize="large" (click)="resetCreateForm()">
                      <i nz-icon nzType="reload"></i>
                      重置表单
                    </button>
                    <button 
                      nz-button 
                      nzSize="large"
                      (click)="prevCreateStep()"
                      [disabled]="createStepIndex === 0">
                      <i nz-icon nzType="arrow-left"></i>
                      上一步
                    </button>
                    <button 
                      *ngIf="createStepIndex < 3"
                      nz-button 
                      nzType="primary" 
                      nzSize="large"
                      (click)="nextCreateStep()"
                      [disabled]="!canGoNext()">
                      下一步
                      <i nz-icon nzType="arrow-right"></i>
                    </button>
                    <button 
                      *ngIf="createStepIndex === 3"
                      nz-button 
                      nzType="primary" 
                      nzSize="large" 
                      [nzLoading]="loadingService.isLoading(LoadingKeys.BACKUP_CREATE)"
                      [disabled]="!canSubmit()"
                      (click)="confirmAndSubmit()">
                      <i nz-icon nzType="cloud-download"></i>
                      提交备份
                    </button>
                  </div>
                </div>
              </div>
            </ng-template>
          </nz-tab>
        </nz-tabset>
    </div>

      <!-- 备份详情抽屉 -->
      <nz-drawer
        [nzVisible]="detailsDrawerVisible"
        nzPlacement="right"
        nzTitle="备份详情"
        [nzWidth]="600"
        (nzOnClose)="closeDetailsDrawer()">
        <div *nzDrawerContent>
        <div *ngIf="selectedBackup">
            <nz-descriptions nzBordered [nzColumn]="1">
              <nz-descriptions-item nzTitle="备份名称">
                {{ selectedBackup.metadata.name }}
              </nz-descriptions-item>
              <nz-descriptions-item nzTitle="命名空间">
                <nz-tag nzColor="blue">{{ selectedBackup.metadata.namespace }}</nz-tag>
              </nz-descriptions-item>
              <nz-descriptions-item nzTitle="集群">
                {{ selectedBackup.spec.cluster.name || '-' }}
              </nz-descriptions-item>
              <nz-descriptions-item nzTitle="状态">
                <nz-tag [nzColor]="getBackupStatusColor(selectedBackup)">
                  {{ mapPhaseText(selectedBackup) }}
                </nz-tag>
              </nz-descriptions-item>
              <nz-descriptions-item nzTitle="进度">
                <app-backup-progress-indicator
                  [metadata]="toBackupMetadata(selectedBackup)"
                  [type]="BackupType.POLARDBX"
                  size="small"
                  [showDetails]="true">
                </app-backup-progress-indicator>
              </nz-descriptions-item>
              <nz-descriptions-item nzTitle="存储连通性">
                <nz-tag 
                  [nzColor]="getStorageConnectivityColor(selectedBackup)"
                  [nz-tooltip]="getStorageConnectivityTooltip(selectedBackup)">
                  {{ getStorageConnectivityText(selectedBackup) }}
                </nz-tag>
              </nz-descriptions-item>
              <nz-descriptions-item nzTitle="备份大小">
                {{ formatSize(selectedBackup) }}
              </nz-descriptions-item>
              <nz-descriptions-item nzTitle="创建时间">
                {{ formatDate(selectedBackup.metadata.creationTimestamp) }}
              </nz-descriptions-item>
            </nz-descriptions>
        </div>
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
      background: transparent;
    }
    
    .page-header {
      margin-bottom: 0;
      background: #fff;
      padding: 16px;
      border-radius: 10px;
      box-shadow: 0 2px 10px rgba(15, 23, 42, 0.04);
    }
    
    .header-content {
      width: 100%;
      max-width: none;
      margin: 0;
    }
    
    .page-description {
      color: rgba(0, 0, 0, 0.6);
      font-size: 14px;
      margin: 0;
      line-height: 1.5;
    }
    
    .page-content {
      width: 100%;
      max-width: none;
      margin: 0;
    }
    
    .stats-section {
      margin-bottom: 16px;
    }

    .overview-card {
      background: #ffffff;
      border-radius: 12px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border: 1px solid #e0e0e0;
    }
    
    .stat-card {
      text-align: center;
      border-radius: 8px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.06);
    }
    
    .main-tabs {
      background: #ffffff;
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border: 1px solid #e0e0e0;
    }
    
    .tab-content {
      padding: 16px;
    }
    
    .list-card {
      border: none;
      box-shadow: none;
    }
    
    .extra-actions {
      display: flex;
      gap: 8px;
    }
    
    .list-content {
      margin-top: 16px;
    }
    
    .backup-name-cell {
      display: flex;
      flex-direction: column;
      gap: 4px;
    }
    
    .resource-name {
      font-weight: 500;
      color: var(--primary-color, #ff6a00);
    }
    
    .progress-indicator {
      width: 100%;
    }
    
    .progress-cell {
      width: 100px;
    }
    
    .size-text {
      font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
      font-size: 12px;
    }
    
    .date-text {
      font-size: 12px;
      color: #666;
    }
    
    .action-buttons {
      display: flex;
      gap: 4px;
    }
    
    .empty-state {
      text-align: center;
      padding: 40px 0;
    }
    
    .configuration-wrapper {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }
    
    .steps-card {
      background: #ffffff;
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border: 1px solid #e0e0e0;
    }
    
    .config-sections {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }
    
    .config-card {
      background: #ffffff;
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border: 1px solid #e0e0e0;
      overflow: hidden;
    }
    
    .section-icon {
      font-size: 16px;
      color: var(--primary-color, #ff6a00);
    }
    
    .action-bar {
      display: flex;
      justify-content: center;
      gap: 16px;
      padding: 16px;
      background: #ffffff;
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border: 1px solid #e0e0e0;
    }
    
    /* Responsive design */
    @media (max-width: 768px) {
      .page-wrapper { padding: 16px; }
      
      .extra-actions {
        flex-direction: column;
      }
      
      .action-bar {
        flex-direction: column;
        align-items: center;
      }
      
      .action-bar button {
        width: 100%;
        max-width: 200px;
      }
    }
  `]
})
export class BackupManagementComponent implements OnInit, OnDestroy {
  private destroy$ = new Subject<void>();
  private apiService = inject(ApiService);
  private namespaceService = inject(NamespaceService);
  private fb = inject(FormBuilder);
  private msg = inject(NzMessageService);
  
  loadingService = inject(LoadingService);
  LoadingKeys = LoadingKeys;
  BackupType = BackupType;

  backups: PolarDBXBackup[] = [];
  private backupMetrics: Record<string, { phase?: string; progress?: number; estimated?: boolean; sizeBytes?: number; sizeEstimated?: boolean; children?: { total: number; finished: number; failed: number } }> = {};
  availableNamespaces: string[] = [];
  availableClusters: any[] = [];
  selectedListCluster: string | null = null;
  selectedBackup: PolarDBXBackup | null = null;
  detailsDrawerVisible = false;
  currentNamespace = 'default';
  grafanaURL = '';
  selectedTabIndex = 0;
  createStepIndex = 0;
  
  // Global storage connectivity status
  globalStorageStatus: 'ok' | 'error' | 'unknown' = 'unknown';
  globalStorageDetail = '';
    
  createForm: FormGroup = this.fb.group({
    name: ['', [Validators.pattern(/^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/)]],
    namespace: ['default', [Validators.required]],
    clusterName: ['', [Validators.required]],
    storageName: ['', [Validators.required]],
    sink: ['', [Validators.required]],
    priority: ['normal'],
    enableCompression: [true],
    description: ['']
  });

  progressFormat = (percent: number): string => {
    return percent === 100 ? '完成' : `${percent}%`;
  };

  ngOnInit(): void {
    this.namespaceService.activeNamespace$
      .pipe(takeUntil(this.destroy$))
      .subscribe((namespace: string | null) => {
        this.currentNamespace = namespace || 'default';
        this.createForm.patchValue({ namespace: this.currentNamespace });
        this.loadBackups();
        this.loadClusters();
        this.loadGlobalStorageStatus(); // Load storage connectivity status
      });

    this.loadNamespaces();

    // Load Grafana URL (best-effort; do not block page).
    this.apiService.getGrafanaConfig().pipe(
      catchError(() => of(null))
    ).subscribe((cfg: any) => {
      const url = (cfg?.url || '').toString().trim();
      this.grafanaURL = url;
    });

    // Poll backup metrics to render real progress (lightweight).
    timer(0, 15_000).pipe(takeUntil(this.destroy$)).subscribe(() => {
      this.refreshBackupMetrics();
    });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  private keyOf(backup: PolarDBXBackup): string {
    return `${backup.metadata.namespace}/${backup.metadata.name}`;
  }

  async loadBackups(): Promise<void> {
    try {
      this.loadingService.setLoading(LoadingKeys.BACKUPS_LIST, true);
      if (!this.selectedListCluster) {
        this.backups = [];
        return;
      }
      const response = await this.apiService.getBackups(this.currentNamespace, this.selectedListCluster).toPromise();
      this.backups = response || [];
      this.refreshBackupMetrics();
    } catch (error) {
      console.error('加载备份列表失败:', error);
      this.msg.error('加载备份列表失败');
      this.backups = [];
    } finally {
      this.loadingService.setLoading(LoadingKeys.BACKUPS_LIST, false);
    }
  }

  private refreshBackupMetrics(): void {
    const list = (this.backups || []).filter(b => {
      const ui = mapBackupPhaseToUIStatus(b.status?.phase);
      const key = this.keyOf(b);
      // Always refresh live phases; fetch once for stable phases (to get sizeBytes on Finished).
      if (ui === 'Running' || ui === 'Pending' || ui === 'Deleting') return true;
      return !this.backupMetrics[key];
    });
    for (const b of list) {
      const key = this.keyOf(b);
      this.apiService.getBackupMetrics(b.metadata.namespace, b.metadata.name, { silent: true }).pipe(
        catchError(() => of(null as any))
      ).subscribe((m: any) => {
        if (!m) return;
        this.backupMetrics[key] = {
          phase: m?.phase,
          progress: typeof m?.progress === 'number' ? m.progress : undefined,
          estimated: !!m?.estimated,
          sizeBytes: typeof m?.sizeBytes === 'number' ? m.sizeBytes : undefined,
          sizeEstimated: !!m?.sizeEstimated,
          children: m?.children
        };
      });
    }
  }

  async loadNamespaces(): Promise<void> {
    try {
      const response = await this.apiService.listSystemNamespaces().toPromise();
      this.availableNamespaces = (response?.items || []).map(ns => ns.name);
    } catch (error) {
      console.error('加载命名空间失败:', error);
      this.availableNamespaces = ['default'];
    }
  }

  // Load global storage connectivity status
  loadGlobalStorageStatus(): void {
    this.apiService.getBackupOverview({ 
      namespace: this.currentNamespace, 
      evaluateConnectivity: true 
    }).subscribe({
      next: (res) => {
        this.globalStorageStatus = res?.kpi?.storageConnectivityStatus || 'unknown';
        this.globalStorageDetail = res?.kpi?.storageConnectivity || '';
        console.log('[Backup Management] Storage Status:', {
          status: this.globalStorageStatus,
          detail: this.globalStorageDetail
        });
      },
      error: (error) => {
        console.error('加载存储连通性状态失败:', error);
        this.globalStorageStatus = 'unknown';
        this.globalStorageDetail = '无法获取状态';
      }
    });
  }

  async loadClusters(): Promise<void> {
    try {
      const response = await this.apiService.getClusters().toPromise();
      this.availableClusters = response || [];
      if (!this.selectedListCluster && this.availableClusters.length > 0) {
        this.selectedListCluster = this.availableClusters[0]?.metadata?.name || null;
        await this.loadBackups();
      }
    } catch (error) {
      console.error('加载集群列表失败:', error);
      this.availableClusters = [];
    }
  }

  async submitCreate(): Promise<void> {
    if (!this.createForm.valid) {
      this.markFormGroupTouched(this.createForm);
            return;
    }

    try {
      this.loadingService.setLoading(LoadingKeys.BACKUP_CREATE, true);
      const formValue = this.createForm.value;
      const retentionTime = (localStorage.getItem('backupRetentionTime') || '240h').trim();
      
      const body: any = {
        apiVersion: 'polardbx.aliyun.com/v1',
        kind: 'PolarDBXBackup',
        metadata: {
          name: formValue.name || undefined,
          namespace: formValue.namespace
        },
        spec: {
          cluster: { name: formValue.clusterName },
          retentionTime: retentionTime,
          cleanPolicy: 'Retain',
          storageProvider: {
            storageName: formValue.storageName,
            sink: formValue.sink
          },
          preferredBackupRole: 'follower'
        }
      };

      await this.apiService.createBackup(formValue.namespace, formValue.clusterName, body).toPromise();
      this.msg.success('备份任务创建成功');
      this.resetCreateForm();
      // Switch back to list tab
      this.selectedTabIndex = 0;
      if (this.selectedListCluster === formValue.clusterName) {
        this.loadBackups();
      }
    } catch (error) {
      console.error('创建备份失败:', error);
      this.msg.error('创建备份失败');
    } finally {
      this.loadingService.setLoading(LoadingKeys.BACKUP_CREATE, false);
    }
    }

    refreshBackups(): void {
    this.loadBackups();
  }

  // Step-by-step wizard logic
  canGoNext(): boolean {
    if (this.createStepIndex === 0) {
      const controls = this.createForm.controls;
      return (controls['namespace'].valid && controls['clusterName'].valid && controls['name'].valid !== false);
    }
    if (this.createStepIndex === 1) {
      const controls = this.createForm.controls;
      return controls['storageName'].valid && controls['sink'].valid;
    }
    if (this.createStepIndex === 2) {
      return true;
    }
    return true;
  }

  canSubmit(): boolean {
    return this.createForm.valid && this.createStepIndex === 3;
  }

  nextCreateStep(): void {
    if (!this.canGoNext()) {
      this.markFormGroupTouched(this.createForm);
      return;
    }
    this.createStepIndex = Math.min(3, this.createStepIndex + 1);
  }

  prevCreateStep(): void {
    this.createStepIndex = Math.max(0, this.createStepIndex - 1);
  }

  async confirmAndSubmit(): Promise<void> {
    if (!this.canSubmit()) return;
    const v = this.createForm.value;
    const confirmMsg = `请确认提交备份：\n\n` +
      `命名空间：${v.namespace}\n` +
      `目标集群：${v.clusterName}\n` +
      `备份名称：${v.name || '-'}\n` +
      `存储提供商：${v.storageName}\n` +
      `存储 Sink：${v.sink}`;
    if (confirm(confirmMsg)) {
      await this.submitCreate();
      this.createStepIndex = 0;
    }
  }

  switchToCreateTab(): void {
    this.selectedTabIndex = 1;
  }

  onTabIndexChange(index: number): void {
    this.selectedTabIndex = index;
  }

  onListClusterChange(value: string): void {
    this.selectedListCluster = value || null;
    this.loadBackups();
  }

  onClusterChange(clusterName: string): void {
    // Handle logic when cluster changes
    console.log('Selected cluster:', clusterName);
  }

  resetCreateForm(): void {
    this.createForm.reset({
      name: '',
      namespace: this.currentNamespace,
      clusterName: '',
      storageName: '',
      sink: '',
      priority: 'normal',
      enableCompression: true,
      description: ''
    });
    // Reset step to first step and clear validation state
    this.createStepIndex = 0;
    Object.keys(this.createForm.controls).forEach(key => {
      const ctrl = this.createForm.get(key);
      ctrl?.markAsPristine();
      ctrl?.markAsUntouched();
      ctrl?.updateValueAndValidity();
    });
  }

  showBackupDetails(backup: PolarDBXBackup): void {
    this.selectedBackup = backup;
    this.detailsDrawerVisible = true;
  }

  closeDetailsDrawer(): void {
    this.detailsDrawerVisible = false;
    this.selectedBackup = null;
  }

  async forceDeleteBackup(backup: PolarDBXBackup): Promise<void> {
    try {
      await this.apiService.forceDeleteBackup(backup.metadata.namespace!, backup.metadata.name).toPromise();
      this.msg.success('强制删除成功');
      await this.loadBackups();
    } catch (error) {
      console.error('强制删除失败:', error);
      this.msg.error('强制删除失败');
    }
  }

  // Statistics methods
  getRunningBackupsCount(): number {
    return this.backups.filter(backup => this.isRunningEx(backup)).length;
  }

  getCompletedBackupsCount(): number {
    return this.backups.filter(backup => isBackupCompletedStatus(backup.status?.phase)).length;
  }

  getFailedBackupsCount(): number {
    return this.backups.filter(backup => isBackupFailedStatus(backup.status?.phase)).length;
  }

  // Status and display methods
    isRunningEx(backup: PolarDBXBackup): boolean {
    return isBackupRunning(backup.status?.phase);
  }

  toBackupMetadata(backup: PolarDBXBackup): BackupProgressMetadata {
    const phase = this.mapToBackupPhase(backup.status?.phase);
    const startTime = backup.status?.startTime || undefined;
    const completionTime = backup.status?.endTime || backup.status?.completionTime || undefined;
    const percentage = this.getProgressPercent(backup);
    
    return {
      phase,
      subPhase: this.mapToBackupSubPhase(backup.status?.phase),
      startTime,
      completionTime,
      progress: {
        percentage: this.normalizePercentage(percentage),
        processedBytes: undefined,
        totalBytes: undefined,
        processedFiles: undefined,
        totalFiles: undefined,
        transferRate: undefined
      },
      errorMessage: backup.status?.phase === 'Failed' ? backup.status?.message : undefined,
      warnings: [],
      message: backup.status?.message
    };
  }

  private mapToBackupPhase(phase?: string): BackupPhase {
    switch (phase) {
      case 'Completed':
      case 'Finished':  // Backend may return Finished
        return 'Completed';
      case 'Failed':
        return 'Failed';
      case 'Running':
        return 'Running';
      case 'Pending':
        return 'Pending';
      default:
        // If no phase, infer from other information
        return 'Pending';
    }
  }

  private mapToBackupSubPhase(phase?: string): BackupSubPhase | undefined {
    // PolarDBX backups don't have detailed sub-phases in the current model
    // Return undefined for now
    return undefined;
  }

  private normalizePercentage(value: number | undefined): number {
    if (value === undefined || value === null || isNaN(value)) {
      return 0;
    }
    return Math.max(0, Math.min(100, Math.round(value)));
  }

  canDelete(backup: PolarDBXBackup): boolean {
    return canDeleteBackup(backup.status?.phase);
  }

  canForceDelete(backup: PolarDBXBackup): boolean {
    const ui = mapBackupPhaseToUIStatus(backup.status?.phase);
    return ui === 'Deleting' || !!backup.metadata?.deletionTimestamp;
  }

  async deleteBackup(backup: PolarDBXBackup): Promise<void> {
    try {
      await this.apiService.deleteBackup(backup.metadata.namespace!, backup.metadata.name).toPromise();
      this.msg.success('删除成功');
      await this.loadBackups();
    } catch (error) {
      console.error('删除失败:', error);
      this.msg.error('删除失败（如长时间卡住可尝试强制删除）');
    }
  }

  mapPhaseText(backup: PolarDBXBackup): string {
    return getPhaseDisplayLabel(backup.status?.phase);
  }

  getBackupStatusColor(backup: PolarDBXBackup): string {
    return getPhaseStatusColor(backup.status?.phase);
  }

  getProgressPercent(backup: PolarDBXBackup): number {
    const m = this.backupMetrics[this.keyOf(backup)];
    if (m && typeof m.progress === 'number' && isFinite(m.progress)) {
      return Math.max(0, Math.min(100, Math.round(m.progress)));
    }

    if (isBackupRunning(backup.status?.phase)) {
      return 50;
    }
    if (isBackupCompletedStatus(backup.status?.phase)) {
      return 100;
    }
    return 0;
  }

  getProgressStatus(backup: PolarDBXBackup): 'success' | 'exception' | 'active' | 'normal' {
    return getPhaseStatusStyle(backup.status?.phase) as 'success' | 'exception' | 'active' | 'normal';
  }

  formatSize(backup: PolarDBXBackup): string {
    const m = this.backupMetrics[this.keyOf(backup)];
    const bytes = m?.sizeBytes;
    if (typeof bytes === 'number' && isFinite(bytes) && bytes > 0) {
      return this.formatBytes(bytes);
    }
    const total = m?.children?.total ?? 0;
    const finished = m?.children?.finished ?? 0;
    if (total > 0) {
      return `${finished}/${total} XStore`;
    }

    // PolarDBX backup size information is not in the main object
    // Need to calculate from associated XStore backups
    // Currently temporarily display backup path or XStore count as hint
    const xstoreCount = backup.status?.xstores?.length || 0;
    if (xstoreCount > 0) {
      return `${xstoreCount} 个 XStore`;
    }
    
    // If backend adds aggregated size field in the future, can read like this:
    const status = backup.status as any;
    const totalSize = status?.totalBackupSize || status?.backupSize;
    if (totalSize && typeof totalSize === 'number' && totalSize > 0) {
      return this.formatBytes(totalSize);
    }
    
    return '-';
  }

  private formatBytes(bytes: number): string {
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    if (bytes === 0) return '0 B';
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    const value = bytes / Math.pow(1024, i);
    return `${value.toFixed(2)} ${sizes[i]}`;
  }

  formatDate(dateString?: string): string {
    if (!dateString) return '-';
    return new Date(dateString).toLocaleString('zh-CN');
  }
  
  // Get backup completion time (using actual endTime field)
  getCompletionTime(backup: PolarDBXBackup): string {
    const endTime = backup.status?.endTime || backup.status?.completionTime;
    return this.formatDate(endTime);
  }
  
  // Get backup duration
  getDuration(backup: PolarDBXBackup): string {
    const startTime = backup.status?.startTime;
    const endTime = backup.status?.endTime || backup.status?.completionTime;
    
    if (!startTime || !endTime) return '-';
    
    try {
      const start = new Date(startTime).getTime();
      const end = new Date(endTime).getTime();
      const durationMs = end - start;
      
      const seconds = Math.floor(durationMs / 1000);
      const minutes = Math.floor(seconds / 60);
      const hours = Math.floor(minutes / 60);
      
      if (hours > 0) {
        return `${hours}小时${minutes % 60}分钟`;
      } else if (minutes > 0) {
        return `${minutes}分钟${seconds % 60}秒`;
      } else {
        return `${seconds}秒`;
      }
    } catch (e) {
      return '-';
    }
  }

  getStorageConnectivityText(backup: PolarDBXBackup): string {
    const phase = backup.status?.phase;
    
    // Prioritize global status, combined with backup phase judgment
    if (this.globalStorageStatus === 'ok' || isBackupCompletedStatus(phase)) {
      return '连通正常';
    } else if (this.globalStorageStatus === 'error') {
      return '连通异常';
    } else if (isBackupRunning(phase)) {
      return '检测中';
    }
    return '未知';
  }

  getStorageConnectivityColor(backup: PolarDBXBackup): string {
    if (this.globalStorageStatus === 'ok' || isBackupCompletedStatus(backup.status?.phase)) {
      return 'success';
    } else if (this.globalStorageStatus === 'error') {
      return 'error';
    } else if (isBackupRunning(backup.status?.phase)) {
      return 'processing';
    }
    return 'default';
  }

  getStorageConnectivityTooltip(backup: PolarDBXBackup): string {
    if (this.globalStorageStatus === 'ok' || isBackupCompletedStatus(backup.status?.phase)) {
      return `存储连接正常 (${this.globalStorageDetail || '备份已完成'})`;
    } else if (this.globalStorageStatus === 'error') {
      return `存储连接异常: ${this.globalStorageDetail || '未知错误'}`;
    } else if (isBackupRunning(backup.status?.phase)) {
      return '备份进行中，正在检测存储连通性';
    }
    return '存储连通性未知';
  }

  private markFormGroupTouched(formGroup: FormGroup): void {
    Object.keys(formGroup.controls).forEach(key => {
      const control = formGroup.get(key);
      control?.markAsTouched();
      control?.updateValueAndValidity();
    });
  }
}
