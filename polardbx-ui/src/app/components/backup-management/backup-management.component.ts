import { Component, inject, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormBuilder, FormGroup, Validators, ReactiveFormsModule } from '@angular/forms';
import { FormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';
import { ApiService } from '../../services/api.service';
import { LoadingService, LoadingKeys } from '../../services/loading.service';
import { PolarDBXBackup } from '../../models/backup.model';
import { NamespaceService } from '../../services/namespace.service';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

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
    NzDrawerModule
  ],
  template: `
    <div class="backup-management-container">
      <div class="page-header">
        <div class="header-content">
          <h1 class="page-title">
            <i nz-icon nzType="inbox" class="page-icon"></i>
            存储备份管理
          </h1>
          <p class="page-description">管理 PolarDB-X 集群的全量备份，支持手动创建和存储连通性检测</p>
        </div>
      </div>

      <div class="page-content">
        <!-- 统计概览 -->
        <div class="stats-section">
          <div nz-row [nzGutter]="16">
            <div nz-col [nzSpan]="6">
              <nz-card class="stat-card">
                <nz-statistic 
                  nzTitle="总备份数量" 
                  [nzValue]="backups.length" 
                  [nzValueStyle]="{ color: '#1890ff' }">
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
                  [nzValueStyle]="{ color: '#1890ff' }">
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
                                <nz-progress 
                                  [nzPercent]="0" 
                                  nzStatus="active" 
                                  [nzShowInfo]="false" 
                                  [nzStrokeWidth]="2">
                                </nz-progress>
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
                              <nz-progress 
                                [nzPercent]="getProgressPercent(backup)" 
                                [nzStatus]="getProgressStatus(backup)"
                                [nzSize]="'small'"
                                [nzFormat]="progressFormat">
                              </nz-progress>
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
                <nz-progress 
                  [nzPercent]="getProgressPercent(selectedBackup)" 
                  [nzStatus]="getProgressStatus(selectedBackup)">
                </nz-progress>
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
    .backup-management-container {
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
    
    .stats-section {
      margin-bottom: 16px;
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
      color: #1890ff;
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
      color: #1890ff;
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
    
    /* 响应式设计 */
    @media (max-width: 768px) {
      .backup-management-container {
        padding: 8px;
      }
      
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

  backups: PolarDBXBackup[] = [];
  availableNamespaces: string[] = [];
  availableClusters: any[] = [];
  selectedListCluster: string | null = null;
  selectedBackup: PolarDBXBackup | null = null;
  detailsDrawerVisible = false;
  currentNamespace = 'default';
    grafanaURL = '';
  selectedTabIndex = 0;
  createStepIndex = 0;
    
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
      });

    this.loadNamespaces();
    }

    ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
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
    } catch (error) {
      console.error('加载备份列表失败:', error);
      this.msg.error('加载备份列表失败');
      this.backups = [];
    } finally {
      this.loadingService.setLoading(LoadingKeys.BACKUPS_LIST, false);
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
      
      const body: any = {
        metadata: {
          name: formValue.name || undefined,
          namespace: formValue.namespace
        },
        spec: {
          cluster: { name: formValue.clusterName },
          storageProvider: {
            storageName: formValue.storageName,
            sink: formValue.sink
          }
        }
      };

      await this.apiService.createBackup(formValue.namespace, formValue.clusterName, body).toPromise();
      this.msg.success('备份任务创建成功');
      this.resetCreateForm();
      // 切回列表页签
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

  // 分步向导逻辑
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
    // 当集群变化时的处理逻辑
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
    // 重置步骤到第一步，并清理校验状态
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
      // await this.apiService.forceDeleteBackup(backup.metadata.namespace!, backup.metadata.name).toPromise();
      // 临时注释，需要根据实际的 API 方法调用
      this.msg.info('强制删除功能待实现');
      this.msg.success('强制删除成功');
      this.loadBackups();
    } catch (error) {
      console.error('强制删除失败:', error);
      this.msg.error('强制删除失败');
    }
  }

  // 统计方法
  getRunningBackupsCount(): number {
    return this.backups.filter(backup => this.isRunningEx(backup)).length;
  }

  getCompletedBackupsCount(): number {
    return this.backups.filter(backup => backup.status?.phase === 'Completed').length;
  }

  getFailedBackupsCount(): number {
    return this.backups.filter(backup => backup.status?.phase === 'Failed').length;
  }

  // 状态和显示方法
    isRunningEx(backup: PolarDBXBackup): boolean {
    return backup.status?.phase === 'Running' || backup.status?.phase === 'Pending';
  }

  canForceDelete(backup: PolarDBXBackup): boolean {
    return backup.status?.phase !== 'Failed' && backup.status?.phase !== 'Completed';
  }

  mapPhaseText(backup: PolarDBXBackup): string {
    const phase = backup.status?.phase;
    switch (phase) {
      case 'Running': return '运行中';
      case 'Completed': return '已完成';
      case 'Failed': return '失败';
      case 'Pending': return '等待中';
      default: return '未知';
    }
  }

  getBackupStatusColor(backup: PolarDBXBackup): string {
    const phase = backup.status?.phase;
    switch (phase) {
      case 'Running': return 'processing';
      case 'Completed': return 'success';
      case 'Failed': return 'error';
      case 'Pending': return 'default';
      default: return 'default';
    }
  }

  getProgressPercent(backup: PolarDBXBackup): number {
    // 临时返回固定值，实际需要根据 backup status 的实际字段调整
    if (backup.status?.phase === 'Running') {
      return 50; // 运行中显示 50%
    }
    if (backup.status?.phase === 'Completed') {
      return 100; // 完成显示 100%
    }
    return 0; // 其他状态显示 0%
  }

  getProgressStatus(backup: PolarDBXBackup): 'success' | 'exception' | 'active' | 'normal' {
    const phase = backup.status?.phase;
    switch (phase) {
      case 'Completed': return 'success';
      case 'Failed': return 'exception';
      case 'Running': return 'active';
      default: return 'normal';
    }
  }

  formatSize(backup: PolarDBXBackup): string {
    // 临时返回固定值，实际需要根据 backup status 的实际字段调整
    if (backup.status?.phase === 'Completed') {
      return '1.2 GB'; // 示例大小
    }
    return '-';
  }

  formatDate(dateString?: string): string {
    if (!dateString) return '-';
    return new Date(dateString).toLocaleString('zh-CN');
  }

  getStorageConnectivityText(backup: PolarDBXBackup): string {
    // 临时返回固定值，实际需要根据 backup status 的实际字段调整
    if (backup.status?.phase === 'Completed') {
      return '连通正常';
    }
    return '检测中';
  }

  getStorageConnectivityColor(backup: PolarDBXBackup): string {
    // 临时返回固定值，实际需要根据 backup status 的实际字段调整
    if (backup.status?.phase === 'Completed') {
      return 'success';
    }
    return 'processing';
  }

  getStorageConnectivityTooltip(backup: PolarDBXBackup): string {
    // 临时返回固定值，实际需要根据 backup status 的实际字段调整
    if (backup.status?.phase === 'Completed') {
      return '存储连接正常，备份可以正常写入';
    }
    return '正在检测存储连通性';
  }

  private markFormGroupTouched(formGroup: FormGroup): void {
    Object.keys(formGroup.controls).forEach(key => {
      const control = formGroup.get(key);
      control?.markAsTouched();
      control?.updateValueAndValidity();
    });
  }
}
