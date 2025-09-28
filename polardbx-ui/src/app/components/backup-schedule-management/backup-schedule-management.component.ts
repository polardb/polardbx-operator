import { Component, OnInit, OnDestroy, TemplateRef, ViewChild } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReactiveFormsModule, FormBuilder, FormGroup, Validators } from '@angular/forms';
import { NzTabsModule } from 'ng-zorro-antd/tabs';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzMessageModule } from 'ng-zorro-antd/message';
import { NzModalService } from 'ng-zorro-antd/modal';
import { NzModalModule } from 'ng-zorro-antd/modal';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzSwitchModule } from 'ng-zorro-antd/switch';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzCollapseModule } from 'ng-zorro-antd/collapse';
import { NzMenuModule } from 'ng-zorro-antd/menu';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzInputNumberModule } from 'ng-zorro-antd/input-number';
import { NzDescriptionsModule } from 'ng-zorro-antd/descriptions';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzStatisticModule } from 'ng-zorro-antd/statistic';
import { FormsModule } from '@angular/forms';
import { Subject } from 'rxjs';
import { takeUntil, finalize } from 'rxjs/operators';

import { ApiService } from '../../services/api.service';
import { LoadingService, LoadingKeys } from '../../services/loading.service';
import {
  PolarDBXBackupSchedule,
  CreateBackupScheduleRequest,
  PREDEFINED_CRON_SCHEDULES,
  STORAGE_PROVIDER_OPTIONS,
  CLEAN_POLICY_OPTIONS,
  BACKUP_ROLE_OPTIONS,
  BackupStorage,
  CleanPolicyType
} from '../../models/backup-schedule.model';

@Component({
  selector: 'app-backup-schedule-management',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    NzTabsModule,
    NzTableModule,
    NzCardModule,
    NzButtonModule,
    NzIconModule,
    NzInputModule,
    NzSelectModule,
    NzFormModule,
    NzTagModule,
    NzSpinModule,
    NzToolTipModule,
    NzSwitchModule,
    NzDividerModule,
    NzCollapseModule,
    NzMenuModule,
    NzGridModule,
    NzInputNumberModule,
    NzDescriptionsModule,
    NzEmptyModule,
    NzStatisticModule,
    FormsModule,
    NzModalModule,
    NzMessageModule
  ],
  template: `
    <div class="backup-schedule-management">
      <!-- 页面头部 -->
      <div class="page-header">
        <div class="header-content">
          <h1 class="page-title">
            <i nz-icon nzType="calendar" class="page-icon"></i>
            定时备份管理
          </h1>
          <p class="page-description">为 PolarDB-X 集群配置自动备份计划</p>
        </div>
      </div>

      <div class="page-content">
        <!-- 统计概览 -->
        <div class="stats-section">
          <div nz-row [nzGutter]="16">
            <div nz-col [nzSpan]="6">
              <nz-card class="stat-card">
                <nz-statistic
                  nzTitle="总计划数量"
                  [nzValue]="backupSchedules.length"
                  [nzValueStyle]="{ color: '#1890ff' }">
                  <ng-template #nzPrefix>
                    <i nz-icon nzType="calendar"></i>
                  </ng-template>
                </nz-statistic>
              </nz-card>
            </div>
            <div nz-col [nzSpan]="6">
              <nz-card class="stat-card">
                <nz-statistic
                  nzTitle="活动中"
                  [nzValue]="getActiveSchedulesCount()"
                  [nzValueStyle]="{ color: '#52c41a' }">
                  <ng-template #nzPrefix>
                    <i nz-icon nzType="play-circle"></i>
                  </ng-template>
                </nz-statistic>
              </nz-card>
            </div>
            <div nz-col [nzSpan]="6">
              <nz-card class="stat-card">
                <nz-statistic
                  nzTitle="已暂停"
                  [nzValue]="getSuspendedSchedulesCount()"
                  [nzValueStyle]="{ color: '#faad14' }">
                  <ng-template #nzPrefix>
                    <i nz-icon nzType="pause-circle"></i>
                  </ng-template>
                </nz-statistic>
              </nz-card>
            </div>
            <div nz-col [nzSpan]="6">
              <nz-card class="stat-card">
                <nz-statistic
                  nzTitle="下次运行"
                  [nzValue]="getNextRunTime()"
                  [nzValueStyle]="{ color: '#722ed1' }">
                  <ng-template #nzPrefix>
                    <i nz-icon nzType="clock-circle"></i>
                  </ng-template>
                </nz-statistic>
              </nz-card>
            </div>
          </div>
        </div>

        <nz-tabset class="main-tabs" [nzTabPosition]="'top'" [nzSelectedIndex]="selectedTabIndex" (nzSelectedIndexChange)="onTabIndexChange($event)">
          <!-- 备份计划列表 -->
          <nz-tab nzTitle="备份计划">
            <ng-template nz-tab>
              <div class="tab-content">
                <nz-card
                  class="list-card"
                  nzTitle="定时备份计划"
                  [nzExtra]="listExtra"
                  [nzLoading]="loadingService.isLoading(loadingKeys.BACKUP_SCHEDULE_LIST)">
                  <ng-template #listExtra>
                    <div class="extra-actions">
                      <button
                        nz-button
                        nzType="default"
                        nzSize="small"
                        (click)="refreshSchedules()"
                        [nzLoading]="loadingService.isLoading(loadingKeys.BACKUP_SCHEDULE_LIST)">
                        <i nz-icon nzType="reload"></i>
                        刷新
                      </button>
                      <button
                        nz-button
                        nzType="primary"
                        nzSize="small"
                        (click)="switchToCreateTab()">
                        <i nz-icon nzType="plus"></i>
                        新建计划
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
                      [nzData]="backupSchedules"
                      [nzLoading]="loadingService.isLoading(loadingKeys.BACKUP_SCHEDULE_LIST)"
                      [nzPageSize]="10"
                      [nzShowPagination]="backupSchedules.length > 10"
                      [nzScroll]="{ x: '1200px' }">
                      <thead>
                        <tr>
                          <th nzWidth="200px">名称</th>
                          <th nzWidth="120px">命名空间</th>
                          <th nzWidth="150px">目标集群</th>
                          <th nzWidth="180px">计划</th>
                          <th nzWidth="100px">状态</th>
                          <th nzWidth="150px">上次备份</th>
                          <th nzWidth="150px">下次运行</th>
                          <th nzWidth="180px" nzAlign="center">操作</th>
                        </tr>
                      </thead>
                      <tbody>
                        <tr *ngFor="let schedule of basicTable.data">
                          <td>
                            <div class="schedule-name">
                              <i nz-icon
                                 [nzType]="schedule.spec.suspend ? 'pause-circle' : 'play-circle'"
                                 [style.color]="schedule.spec.suspend ? '#faad14' : '#52c41a'">
                              </i>
                              <span style="margin-left: 8px;">{{ schedule.metadata.name }}</span>
                            </div>
                          </td>
                          <td>
                            <nz-tag nzColor="blue">{{ schedule.metadata.namespace }}</nz-tag>
                          </td>
                          <td>
                            <nz-tag nzColor="purple">{{ schedule.spec.backupSpec.cluster.name }}</nz-tag>
                          </td>
                          <td>
                            <div class="schedule-info">
                              <code style="background: #f5f5f5; padding: 2px 6px; border-radius: 4px;">{{ schedule.spec.schedule }}</code>
                              <div style="font-size: 12px; color: #8c8c8c; margin-top: 2px;">{{ getCronDescription(schedule.spec.schedule) }}</div>
                            </div>
                          </td>
                          <td>
                            <nz-tag [nzColor]="schedule.spec.suspend ? 'orange' : 'green'">
                              {{ schedule.spec.suspend ? '已暂停' : '活动' }}
                            </nz-tag>
                          </td>
                          <td>
                            <div style="font-size: 13px;">
                              {{ getLastBackupTime(schedule) || '-' }}
                            </div>
                          </td>
                          <td>
                            <div style="font-size: 13px;">
                              {{ getNextRunTime(schedule) || '-' }}
                            </div>
                          </td>
                          <td nzAlign="center">
                            <div class="action-buttons">
                              <button
                                nz-button
                                nzType="link"
                                nzSize="small"
                                nz-tooltip="查看详情"
                                (click)="viewSchedule(schedule)">
                                <i nz-icon nzType="eye"></i>
                              </button>
                              <button
                                nz-button
                                nzType="link"
                                nzSize="small"
                                nz-tooltip="编辑计划"
                                (click)="editSchedule(schedule)">
                                <i nz-icon nzType="edit"></i>
                              </button>
                              <button
                                nz-button
                                nzType="link"
                                nzSize="small"
                                nz-tooltip="{{ schedule.spec.suspend ? '启用' : '暂停' }}"
                                (click)="toggleSchedule(schedule)">
                                <i nz-icon [nzType]="schedule.spec.suspend ? 'play-circle' : 'pause-circle'"></i>
                              </button>
                              <button
                                nz-button
                                nzType="link"
                                nzDanger
                                nzSize="small"
                                nz-tooltip="删除计划"
                                (click)="deleteSchedule(schedule)">
                                <i nz-icon nzType="delete"></i>
                              </button>
                            </div>
                          </td>
                        </tr>
                      </tbody>
                    </nz-table>
                  </div>
                </nz-card>
              </div>
            </ng-template>
          </nz-tab>
          <!-- 创建备份计划 -->
          <nz-tab nzTitle="创建计划">
            <ng-template nz-tab>
              <div class="tab-content">
                <nz-card class="form-card" nzTitle="{{ editingSchedule ? '编辑备份计划' : '创建新备份计划' }}">
                  <form nz-form [formGroup]="scheduleForm" nzLayout="vertical" class="schedule-form">
                    <!-- 基本配置 -->
                  <nz-collapse [nzBordered]="false" nzExpandIconPosition="end">
                      <nz-collapse-panel [nzActive]="true" nzHeader="基本配置" [nzDisabled]="false">
                        <div nz-row [nzGutter]="16">
                          <div nz-col [nzSpan]="12">
                            <nz-form-item>
                              <nz-form-label nzRequired>计划名称</nz-form-label>
                              <nz-form-control nzErrorTip="请输入计划名称">
                                <input nz-input formControlName="name" placeholder="输入计划名称" />
                              </nz-form-control>
                            </nz-form-item>
                          </div>
                          <div nz-col [nzSpan]="12">
                            <nz-form-item>
                              <nz-form-label nzRequired>目标集群</nz-form-label>
                              <nz-form-control nzErrorTip="请选择目标集群">
                                <nz-select formControlName="cluster" nzPlaceHolder="选择要备份的集群">
                                  <nz-option *ngFor="let cluster of availableClusters"
                                           [nzValue]="cluster.metadata.name"
                                           [nzLabel]="cluster.metadata.name">
                                  </nz-option>
                                </nz-select>
                              </nz-form-control>
                            </nz-form-item>
                          </div>
                        </div>

                        <div nz-row [nzGutter]="16">
                          <div nz-col [nzSpan]="12">
                            <nz-form-item>
                              <nz-form-label nzRequired>备份计划</nz-form-label>
                              <nz-form-control nzErrorTip="请设置备份计划">
                                <nz-select formControlName="schedule" nzPlaceHolder="选择备份频率" (ngModelChange)="onScheduleChange($event)">
                                  <nz-option *ngFor="let option of predefinedSchedules"
                                           [nzValue]="option.value"
                                           [nzLabel]="option.label">
                                  </nz-option>
                                  <nz-option nzValue="custom" nzLabel="自定义 Cron 表达式"></nz-option>
                                </nz-select>
                              </nz-form-control>
                            </nz-form-item>
                          </div>
                          <div nz-col [nzSpan]="12" *ngIf="isCustomSchedule">
                            <nz-form-item>
                              <nz-form-label nzRequired>Cron 表达式</nz-form-label>
                              <nz-form-control nzErrorTip="请输入有效的 Cron 表达式">
                                <input nz-input formControlName="customSchedule" placeholder="0 2 * * *" />
                              </nz-form-control>
                            </nz-form-item>
                          </div>
                        </div>

                        <div nz-row [nzGutter]="16">
                          <div nz-col [nzSpan]="24">
                            <nz-form-item>
                              <nz-form-control>
                                <label nz-checkbox formControlName="suspend">创建时暂停计划</label>
                              </nz-form-control>
                            </nz-form-item>
                          </div>
                        </div>
                      </nz-collapse-panel>

                      <!-- 存储配置 -->
                      <nz-collapse-panel nzHeader="存储配置" [nzActive]="false">
                        <div nz-row [nzGutter]="16">
                          <div nz-col [nzSpan]="12">
                            <nz-form-item>
                              <nz-form-label nzRequired>存储提供商</nz-form-label>
                              <nz-form-control>
                                <nz-select formControlName="storageProvider" nzPlaceHolder="选择存储类型">
                                  <nz-option *ngFor="let provider of storageProviders"
                                           [nzValue]="provider.value"
                                           [nzLabel]="provider.label">
                                  </nz-option>
                                </nz-select>
                              </nz-form-control>
                            </nz-form-item>
                          </div>
                          <div nz-col [nzSpan]="12">
                            <nz-form-item>
                              <nz-form-label>存储配置名称</nz-form-label>
                              <nz-form-control>
                                <input nz-input formControlName="storageName" placeholder="可选，不填写则使用默认" />
                              </nz-form-control>
                            </nz-form-item>
                          </div>
                        </div>
                      </nz-collapse-panel>

                      <!-- 清理策略 -->
                      <nz-collapse-panel nzHeader="清理策略" [nzActive]="false">
                        <div nz-row [nzGutter]="16">
                          <div nz-col [nzSpan]="12">
                            <nz-form-item>
                              <nz-form-label>清理策略</nz-form-label>
                              <nz-form-control>
                                <nz-select formControlName="cleanPolicy" nzPlaceHolder="选择清理策略">
                                  <nz-option *ngFor="let policy of cleanPolicies"
                                           [nzValue]="policy.value"
                                           [nzLabel]="policy.label">
                                  </nz-option>
                                </nz-select>
                              </nz-form-control>
                            </nz-form-item>
                          </div>
                          <div nz-col [nzSpan]="12" *ngIf="needsRetentionValue">
                            <nz-form-item>
                              <nz-form-label>保留数量/天数</nz-form-label>
                              <nz-form-control>
                                <nz-input-number formControlName="retentionValue"
                                               [nzMin]="1"
                                               [nzMax]="365"
                                               nzPlaceHolder="输入保留值">
                                </nz-input-number>
                              </nz-form-control>
                            </nz-form-item>
                          </div>
                        </div>
                      </nz-collapse-panel>
                    </nz-collapse>

                    <!-- 操作按钮 -->
                  <div class="form-actions">
                      <button nz-button
                              nzType="primary"
                              [nzLoading]="loadingService.isLoading(loadingKeys.BACKUP_SCHEDULE_CREATE)"
                              [disabled]="!scheduleForm.valid"
                              (click)="submit()">
                        <i nz-icon [nzType]="editingSchedule ? 'edit' : 'plus'"></i>
                        {{ editingSchedule ? '更新计划' : '创建计划' }}
                      </button>
                      <button nz-button
                              nzType="default"
                              (click)="switchToListTab()"
                              style="margin-left: 12px;">
                        <i nz-icon nzType="arrow-left"></i>
                        返回列表
                      </button>
                      <button nz-button
                              nzType="default"
                              (click)="resetForm()"
                              style="margin-left: 12px;">
                        <i nz-icon nzType="reload"></i>
                        重置
                      </button>
                    </div>
                  </form>
                </nz-card>
              </div>
            </ng-template>
          </nz-tab>
        </nz-tabset>
      </div>

      <!-- 加载模板 -->
      <ng-template #loadingTemplate>
        <div class="loading-container">
          <nz-spin nzSize="large" nzTip="加载中..."></nz-spin>
        </div>
      </ng-template>
    </div>
  `,
  styles: [`
    .backup-schedule-management {
      padding: 16px;
      background: #f5f5f5;
      min-height: 100vh;
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
      background: #fff;
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border: 1px solid #e0e0e0;
    }

    .tab-content {
      padding: 16px;
    }

    .list-card {
      border-radius: 8px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.06);
    }

    .extra-actions {
      display: flex;
      gap: 8px;
      align-items: center;
      flex-wrap: wrap;
    }

    .schedule-name {
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .schedule-info {
      display: flex;
      flex-direction: column;
      gap: 4px;
    }

    .action-buttons {
      display: flex;
      gap: 4px;
      justify-content: center;
    }

    .form-card {
      border-radius: 8px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.06);
    }

    .schedule-form {
      .ant-collapse {
        border: none;
        background: transparent;
      }

      .ant-collapse > .ant-collapse-item {
        border-bottom: 1px solid #f0f0f0;
      }

      .ant-collapse-header {
        font-weight: 600;
        color: #262626;
      }
    }

    .form-actions {
      margin-top: 24px;
      text-align: center;
    }

    .loading-container {
      text-align: center;
      padding: 60px 20px;
    }

    /* 响应式设计 */
    @media (max-width: 768px) {
      .backup-schedule-management {
        padding: 12px;
      }

      .tab-content {
        padding: 12px;
      }

      .extra-actions {
        flex-direction: column;
        align-items: stretch;
      }

      .action-buttons {
        flex-direction: column;
        gap: 8px;
      }
    }
  `]
})
export class BackupScheduleManagementComponent implements OnInit, OnDestroy {
  private destroy$ = new Subject<void>();

  // Form and data
  scheduleForm!: FormGroup;
  backupSchedules: PolarDBXBackupSchedule[] = [];
  availableClusters: { metadata: { name: string } }[] = [];
  editingSchedule: PolarDBXBackupSchedule | null = null;

  // Tab management
  selectedTabIndex = 0;

  // Options data
  predefinedSchedules = PREDEFINED_CRON_SCHEDULES;
  storageProviders = STORAGE_PROVIDER_OPTIONS;
  cleanPolicies = CLEAN_POLICY_OPTIONS;
  backupRoleOptions = BACKUP_ROLE_OPTIONS;

  // UI state
  grafanaURL = '';
  isCustomSchedule = false;
  needsRetentionValue = false;

  loadingKeys = LoadingKeys;

  constructor(
    private apiService: ApiService,
    public loadingService: LoadingService,
    private fb: FormBuilder,
    private message: NzMessageService,
    private modal: NzModalService
  ) {
    this.initializeForm();
  }

  ngOnInit(): void {
    this.loadSchedules();
    this.loadClusters();
    this.grafanaURL = localStorage.getItem('grafanaURL') || '';
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  // Initialization methods
  private initializeForm(): void {
    this.scheduleForm = this.fb.group({
      name: ['', [Validators.required, Validators.pattern(/^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/)]],
      cluster: ['', Validators.required],
      schedule: ['0 2 * * *', Validators.required],
      customSchedule: [''],
      suspend: [false],
      storageProvider: ['oss'],
      storageName: [''],
      cleanPolicy: ['Retain'],
      retentionValue: [7]
    });

    // Watch form changes
    this.scheduleForm.get('cleanPolicy')?.valueChanges.subscribe(policy => {
      this.needsRetentionValue = ['Delete', 'RetainByCount', 'RetainByDays'].includes(policy);
    });
  }

  // Data loading methods
  private loadSchedules(): void {
    this.apiService.getBackupSchedules()
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: (schedules) => {
          this.backupSchedules = schedules;
        },
        error: (error) => {
          this.message.error(`加载备份计划失败: ${error.error?.message || error.message}`);
        }
      });
  }

  private loadClusters(): void {
    this.apiService.getClusters()
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: (clusters) => {
          this.availableClusters = clusters;
        },
        error: (error) => {
          this.message.error(`加载集群列表失败: ${error.error?.message || error.message}`);
        }
      });
  }

  // Statistics methods
  getActiveSchedulesCount(): number {
    return this.backupSchedules.filter(s => !s.spec.suspend).length;
  }

  getSuspendedSchedulesCount(): number {
    return this.backupSchedules.filter(s => s.spec.suspend).length;
  }

  getNextRunTime(schedule?: PolarDBXBackupSchedule): string {
    // 简化：如 schedule.status.nextBackupTime 提供，优先显示；否则退化为占位
    if (schedule?.status?.nextBackupTime) return this.formatDate(schedule.status.nextBackupTime);
    return '—';
  }

  getLastBackupTime(schedule: PolarDBXBackupSchedule): string {
    return schedule.status?.lastBackupTime ? this.formatDate(schedule.status.lastBackupTime) : '-';
  }

  // Form handling methods
  onScheduleChange(value: string): void {
    this.isCustomSchedule = value === 'custom';
  }

  resetForm(): void {
    this.scheduleForm.reset();
    this.editingSchedule = null;
    this.initializeForm();
  }

  submit(): void {
    if (this.scheduleForm.invalid) return;

    const formValue = this.scheduleForm.value;
    const request: CreateBackupScheduleRequest = {
      name: formValue.name,
      namespace: 'default',
      clusterName: formValue.cluster,
      schedule: this.isCustomSchedule ? formValue.customSchedule : formValue.schedule,
      suspend: formValue.suspend,
      cleanPolicy: formValue.cleanPolicy as CleanPolicyType,
      storageProvider: formValue.storageProvider ? {
        storageName: formValue.storageName || formValue.storageProvider,
        sink: ''
      } : undefined,
      preferredBackupRole: 'follower'
    };

    const operation = this.editingSchedule
      ? this.apiService.updateBackupSchedule(this.editingSchedule.metadata.namespace || 'default', {
          apiVersion: 'polardbx.aliyun.com/v1',
          kind: 'PolarDBXBackupSchedule',
          metadata: { name: this.editingSchedule.metadata.name, namespace: this.editingSchedule.metadata.namespace || 'default' },
          spec: {
            schedule: request.schedule,
            suspend: !!request.suspend,
            backupSpec: {
              cluster: { name: request.clusterName },
              cleanPolicy: request.cleanPolicy,
              storageProvider: request.storageProvider,
              preferredBackupRole: request.preferredBackupRole
            }
          }
        } as any)
      : this.apiService.createBackupSchedule('default', request);

    operation
      .pipe(
        takeUntil(this.destroy$),
        finalize(() => this.loadingService.setLoading(this.loadingKeys.BACKUP_SCHEDULE_CREATE, false))
      )
      .subscribe({
        next: () => {
          this.message.success(this.editingSchedule ? '备份计划更新成功' : '备份计划创建成功');
          this.resetForm();
          this.switchToListTab();
          this.loadSchedules();
        },
        error: (error) => {
          this.message.error(`操作失败: ${error.error?.message || error.message}`);
        }
      });
  }

  // Action methods
  refreshSchedules(): void {
    this.loadSchedules();
    this.message.success('备份计划列表已刷新');
  }

  viewSchedule(schedule: PolarDBXBackupSchedule): void {
    // Show details in a modal
    this.message.info(`查看备份计划: ${schedule.metadata.name}`);
  }

  editSchedule(schedule: PolarDBXBackupSchedule): void {
    this.editingSchedule = schedule;
    this.scheduleForm.patchValue({
      name: schedule.metadata.name,
      cluster: schedule.spec.backupSpec.cluster.name,
      schedule: schedule.spec.schedule,
      suspend: schedule.spec.suspend,
      storageProvider: schedule.spec.backupSpec.storageProvider?.storageName || 'oss',
      cleanPolicy: schedule.spec.backupSpec.cleanPolicy
    });
    this.switchToCreateTab();
  }

  toggleSchedule(schedule: PolarDBXBackupSchedule): void {
    const newSuspendState = !schedule.spec.suspend;

    this.modal.confirm({
      nzTitle: '确认操作',
      nzContent: `确定${newSuspendState ? '暂停' : '恢复'}备份计划 "${schedule.metadata.name}" 吗？`,
      nzOkText: '确定',
      nzCancelText: '取消',
      nzOnOk: () => {
        const ns = schedule.metadata.namespace || 'default';
        const updated = {
          ...schedule,
          spec: { ...schedule.spec, suspend: newSuspendState }
        } as PolarDBXBackupSchedule;
        this.apiService.updateBackupSchedule(ns, updated)
          .pipe(takeUntil(this.destroy$))
          .subscribe({
            next: () => {
              this.message.success(`备份计划已${newSuspendState ? '暂停' : '恢复'}`);
              this.loadSchedules();
            },
            error: (error) => {
              this.message.error(`操作失败: ${error.error?.message || error.message}`);
            }
          });
      }
    });
  }

  deleteSchedule(schedule: PolarDBXBackupSchedule): void {
    this.modal.confirm({
      nzTitle: '确认删除',
      nzContent: `确定删除备份计划 "${schedule.metadata.name}" 吗？此操作不可恢复。`,
      nzOkText: '确定删除',
      nzOkType: 'primary',
      nzOkDanger: true,
      nzCancelText: '取消',
      nzOnOk: () => {
        const ns = schedule.metadata.namespace || 'default';
        this.apiService.deleteBackupSchedule(ns, schedule.metadata.name)
          .pipe(takeUntil(this.destroy$))
          .subscribe({
            next: () => {
              this.message.success('备份计划删除成功');
              this.loadSchedules();
            },
            error: (error) => {
              this.message.error(`删除失败: ${error.error?.message || error.message}`);
            }
          });
      }
    });
  }

  // Tab navigation methods
  onTabIndexChange(index: number): void {
    this.selectedTabIndex = index;
  }

  switchToCreateTab(): void {
    this.selectedTabIndex = 1;
  }

  switchToListTab(): void {
    this.selectedTabIndex = 0;
  }

  // Utility methods
  getCronDescription(cronExpression: string): string {
    const predefined = this.predefinedSchedules.find((s: any) => s.value === cronExpression);
    return predefined && predefined.description ? predefined.description : '自定义计划';
  }

  formatDate(dateString?: string): string {
    if (!dateString) return 'N/A';
    return new Date(dateString).toLocaleString();
  }
}
