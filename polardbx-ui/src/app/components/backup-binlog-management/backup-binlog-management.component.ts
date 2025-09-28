import { Component, OnInit, OnDestroy, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReactiveFormsModule, FormBuilder, FormGroup, Validators } from '@angular/forms';
import { ApiService } from '../../services/api.service';
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
import { NzInputNumberModule } from 'ng-zorro-antd/input-number';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzStepsModule } from 'ng-zorro-antd/steps';
import { NzPopconfirmModule } from 'ng-zorro-antd/popconfirm';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { LoadingKeys, LoadingService } from '../../services/loading.service';
import { PolarDBXBackupBinlog, CreateBackupBinlogRequest } from '../../models/backup-binlog.model';
import { NamespaceService } from '../../services/namespace.service';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

@Component({
  selector: 'app-backup-binlog-management',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
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
    NzInputNumberModule,
    NzEmptyModule,
    NzStepsModule,
    NzPopconfirmModule,
    NzSpinModule
  ],
  template: `
    <div class="backup-binlog-container">
      <div class="page-header">
        <div class="header-content">
          <h1 class="page-title">
            <i nz-icon nzType="file-done" class="page-icon"></i>
            增量日志备份管理
          </h1>
          <p class="page-description">管理数据库增量日志备份配置，支持二进制日志备份和时间点恢复</p>
        </div>
      </div>

      <div class="page-content">
        <nz-tabset class="main-tabs" [nzTabPosition]="'top'">
          <!-- 日志备份列表 -->
          <nz-tab nzTitle="日志备份列表">
            <ng-template nz-tab>
              <div class="tab-content">
                <nz-card 
                  class="list-card" 
                  nzTitle="增量日志备份配置" 
                  [nzExtra]="listExtra"
                  [nzLoading]="loadingService.isLoading(LoadingKeys.BACKUP_BINLOG_LIST)">
                  <ng-template #listExtra>
                    <button nz-button nzType="default" nzSize="small" (click)="refreshList()">
                      <i nz-icon nzType="reload"></i>
                    刷新
                  </button>
                  </ng-template>
                  
                  <div class="list-content">
                    <nz-table 
                      #basicTable 
                      [nzData]="backupBinlogs" 
                      [nzLoading]="loadingService.isLoading(LoadingKeys.BACKUP_BINLOG_LIST)"
                      [nzPageSize]="10"
                      [nzShowPagination]="backupBinlogs.length > 10">
                      <thead>
                        <tr>
                          <th>名称</th>
                          <th>命名空间</th>
                          <th>集群</th>
                          <th>状态</th>
                          <th>存储配置</th>
                          <th>时间点恢复</th>
                          <th>创建时间</th>
                          <th nzWidth="150px">操作</th>
                        </tr>
                      </thead>
                      <tbody>
                        <tr *ngFor="let item of basicTable.data">
                          <td>
                            <span class="resource-name">{{ item.metadata.name }}</span>
                    </td>
                          <td>
                            <nz-tag nzColor="blue">{{ item.metadata.namespace }}</nz-tag>
                    </td>
                          <td>{{ item.spec.pxcName }}</td>
                          <td>
                            <nz-tag 
                              [nzColor]="getStatusColor(item.status?.phase)"
                              [nz-tooltip]="item.status ? (item.status | json) : ''">
                              {{ getStatusText(item.status?.phase) }}
                            </nz-tag>
                    </td>
                          <td>
                            <ng-container *ngIf="item.spec.storageProvider as sp; else noStorage">
                              {{ sp.storageName }}
                              <i nz-icon nzType="arrow-right" class="storage-arrow"></i>
                              {{ sp.sink }}
                            </ng-container>
                            <ng-template #noStorage>
                              <span class="text-muted">未配置</span>
                            </ng-template>
                    </td>
                          <td>
                            <nz-tag 
                              [nzColor]="item.spec.pointInTimeRecover ? 'green' : 'default'">
                              {{ item.spec.pointInTimeRecover ? '已启用' : '未启用' }}
                            </nz-tag>
                    </td>
                          <td>{{ formatDate(item.metadata.creationTimestamp) }}</td>
                          <td>
                            <div class="action-buttons">
                              <button 
                                nz-button 
                                nzType="text" 
                                nzSize="small"
                                nz-tooltip="查看详情"
                                (click)="viewDetails(item)">
                                <i nz-icon nzType="eye"></i>
                              </button>
                              <button 
                                nz-button 
                                nzType="text" 
                                nzSize="small" 
                                nzDanger
                                nz-tooltip="删除配置"
                                nz-popconfirm
                                nzPopconfirmTitle="确定要删除这个增量日志备份配置吗？"
                                (nzOnConfirm)="deleteBackupBinlog(item.metadata.namespace || 'default', item.metadata.name)">
                                <i nz-icon nzType="delete"></i>
                              </button>
                            </div>
                    </td>
                        </tr>
                      </tbody>
                    </nz-table>
                    
                    <div *ngIf="!loadingService.isLoading(LoadingKeys.BACKUP_BINLOG_LIST) && backupBinlogs.length === 0" class="empty-state">
                      <nz-empty 
                        nzNotFoundImage="simple" 
                        nzNotFoundContent="暂无增量日志备份配置">
                        <div nz-empty-footer>
                          <button nz-button nzType="primary" (click)="switchToCreateTab()">
                            <i nz-icon nzType="plus"></i>
                            创建配置
                          </button>
                        </div>
                      </nz-empty>
                    </div>
                  </div>
                </nz-card>
              </div>
            </ng-template>
          </nz-tab>

          <!-- 创建配置 -->
          <nz-tab nzTitle="创建配置">
            <ng-template nz-tab>
              <div class="tab-content">
                <div class="configuration-wrapper">
                  <!-- 配置步骤指引 -->
                  <nz-card class="steps-card" nzTitle="创建步骤">
                    <nz-steps [nzCurrent]="0" nzSize="small">
                      <nz-step nzTitle="基本配置" nzDescription="名称和集群"></nz-step>
                      <nz-step nzTitle="备份设置" nzDescription="保留期和策略"></nz-step>
                      <nz-step nzTitle="存储配置" nzDescription="存储提供商"></nz-step>
                      <nz-step nzTitle="创建确认" nzDescription="提交配置"></nz-step>
                    </nz-steps>
                  </nz-card>

                  <!-- 配置表单 -->
                  <div class="config-sections">
                    <!-- 基本配置 -->
                    <nz-card class="config-card" nzTitle="基本配置" [nzExtra]="basicExtra">
                      <ng-template #basicExtra>
                        <i nz-icon nzType="setting" class="section-icon"></i>
                      </ng-template>
                      <form [formGroup]="createForm" nz-form nzLayout="vertical">
                        <div nz-row nzGutter="16">
                          <div nz-col [nzSpan]="8">
                            <nz-form-item>
                              <nz-form-label nzRequired>配置名称</nz-form-label>
                              <nz-form-control nzHasFeedback nzErrorTip="请输入有效的配置名称">
                                <input 
                                  nz-input 
                                  formControlName="name" 
                                  placeholder="my-binlog-backup" />
                              </nz-form-control>
                            </nz-form-item>
                          </div>
                          <div nz-col [nzSpan]="8">
                            <nz-form-item>
                              <nz-form-label nzRequired>命名空间</nz-form-label>
                              <nz-form-control nzErrorTip="请输入命名空间">
                                <input 
                                  nz-input 
                                  formControlName="namespace" 
                                  placeholder="default" />
                              </nz-form-control>
                            </nz-form-item>
                          </div>
                          <div nz-col [nzSpan]="8">
                            <nz-form-item>
                              <nz-form-label nzRequired>目标集群</nz-form-label>
                              <nz-form-control nzErrorTip="请输入 PolarDB-X 集群名称">
                                <input 
                                  nz-input 
                                  formControlName="pxcName" 
                                  placeholder="my-polardbx-cluster" />
                              </nz-form-control>
                            </nz-form-item>
                          </div>
                        </div>
                        <div nz-row>
                          <div nz-col [nzSpan]="24">
                            <nz-form-item>
                              <nz-form-label 
                                nz-tooltip 
                                nzTooltipTitle="启用时间点恢复功能，支持恢复到任意时间点">
                                时间点恢复
                              </nz-form-label>
                              <nz-form-control>
                                <nz-switch 
                                  formControlName="pointInTimeRecover"
                                  nzCheckedChildren="开" 
                                  nzUnCheckedChildren="关">
                                </nz-switch>
                                <span class="switch-description">启用后可恢复到任意时间点</span>
                              </nz-form-control>
                            </nz-form-item>
                          </div>
                        </div>
                      </form>
                    </nz-card>

                    <!-- 备份设置 -->
                    <nz-card class="config-card" nzTitle="备份设置" [nzExtra]="backupExtra">
                      <ng-template #backupExtra>
                        <i nz-icon nzType="clock-circle" class="section-icon"></i>
                      </ng-template>
                      <form [formGroup]="createForm" nz-form nzLayout="vertical">
                        <div nz-row nzGutter="16">
                          <div nz-col [nzSpan]="8">
                            <nz-form-item>
                              <nz-form-label 
                                nzRequired
                                nz-tooltip 
                                nzTooltipTitle="远程存储中日志保留时间，单位：小时">
                                远程保留时间
                              </nz-form-label>
                              <nz-form-control nzErrorTip="请输入有效的保留时间">
                                <nz-input-number
                                  formControlName="remoteExpireLogHours"
                                  [nzMin]="1"
                                  [nzMax]="8760"
                                  nzPlaceHolder="168"
                                  nzAddonAfter="小时"
                                  style="width: 100%">
                                </nz-input-number>
                              </nz-form-control>
                            </nz-form-item>
                          </div>
                          <div nz-col [nzSpan]="8">
                            <nz-form-item>
                              <nz-form-label 
                                nzRequired
                                nz-tooltip 
                                nzTooltipTitle="本地存储中日志保留时间，单位：小时">
                                本地保留时间
                              </nz-form-label>
                              <nz-form-control nzErrorTip="请输入有效的保留时间">
                                <nz-input-number
                                  formControlName="localExpireLogHours"
                                  [nzMin]="1"
                                  [nzMax]="168"
                                  nzPlaceHolder="24"
                                  nzAddonAfter="小时"
                                  style="width: 100%">
                                </nz-input-number>
                              </nz-form-control>
                            </nz-form-item>
                          </div>
                          <div nz-col [nzSpan]="8">
                            <nz-form-item>
                              <nz-form-label 
                                nzRequired
                                nz-tooltip 
                                nzTooltipTitle="本地最大二进制日志文件数量">
                                最大本地文件数
                              </nz-form-label>
                              <nz-form-control nzErrorTip="请输入有效的文件数量">
                                <nz-input-number
                                  formControlName="maxLocalBinlogCount"
                                  [nzMin]="10"
                                  [nzMax]="1000"
                                  nzPlaceHolder="60"
                                  nzAddonAfter="个"
                                  style="width: 100%">
                                </nz-input-number>
                              </nz-form-control>
                            </nz-form-item>
                          </div>
                        </div>
                      </form>
                    </nz-card>

                    <!-- 存储配置 -->
                    <nz-card class="config-card" nzTitle="存储配置" [nzExtra]="storageExtra">
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
                            <div class="storage-tips">
                              <p><i nz-icon nzType="info-circle" style="color: #1890ff;"></i> 
                                 存储配置需要预先在 <code>polardbx-hpfs-config</code> ConfigMap 中定义</p>
                              <p><i nz-icon nzType="info-circle" style="color: #1890ff;"></i> 
                                 支持 S3、OSS、SFTP 等多种存储类型</p>
                            </div>
                          </div>
                        </div>
                      </form>
                    </nz-card>
                  </div>

                  <!-- 操作按钮 -->
                  <div class="action-bar">
                    <button nz-button nzSize="large" (click)="resetForm()">
                      <i nz-icon nzType="reload"></i>
                      重置表单
                    </button>
                    <button 
                      nz-button 
                      nzType="primary" 
                      nzSize="large" 
                      [nzLoading]="loadingService.isLoading(LoadingKeys.BACKUP_BINLOG_CREATE)"
                      [disabled]="!createForm.valid"
                      (click)="submitCreate()">
                      <i nz-icon nzType="plus"></i>
                      创建配置
                    </button>
                  </div>
                </div>
              </div>
            </ng-template>
          </nz-tab>
        </nz-tabset>
      </div>
    </div>
  `,
    styles: [`
    .backup-binlog-container {
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
      font-size: 24px;
      font-weight: 600;
      margin: 0 0 8px 0;
      color: #262626;
      display: flex;
      align-items: center;
      gap: 12px;
    }
    
    .page-icon {
      font-size: 28px;
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
      border: none;
      box-shadow: none;
    }
    
    .list-content {
      margin-top: 16px;
    }
    
    .resource-name {
      font-weight: 500;
      color: #1890ff;
    }
    
    .storage-arrow {
      margin: 0 4px;
      color: #ccc;
      font-size: 12px;
    }
    
    .text-muted {
      color: #999;
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
      background: #fff;
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
      background: #fff;
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border: 1px solid #e0e0e0;
      overflow: hidden;
    }
    
    .section-icon {
      font-size: 16px;
      color: #1890ff;
    }
    
    .switch-description {
      margin-left: 8px;
      color: #666;
      font-size: 12px;
    }
    
    .storage-tips {
      background: #f6f8fa;
      padding: 12px;
      border-radius: 6px;
      border: 1px solid #e1e8ed;
    }
    
    .storage-tips p {
      margin: 4px 0;
      font-size: 13px;
      color: #666;
    }
    
    .storage-tips code {
      background: #fff;
      padding: 2px 4px;
      border-radius: 3px;
      font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
      font-size: 12px;
    }
    
    .action-bar {
      display: flex;
      justify-content: center;
      gap: 16px;
      padding: 16px;
      background: #fff;
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border: 1px solid #e0e0e0;
    }
    
    /* 响应式设计 */
    @media (max-width: 768px) {
      .backup-binlog-container {
        padding: 8px;
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
export class BackupBinlogManagementComponent implements OnInit, OnDestroy {
  private destroy$ = new Subject<void>();
  private apiService = inject(ApiService);
  private namespaceService = inject(NamespaceService);
  private fb = inject(FormBuilder);
  private msg = inject(NzMessageService);
  
  loadingService = inject(LoadingService);
  LoadingKeys = LoadingKeys;

  backupBinlogs: PolarDBXBackupBinlog[] = [];
  currentNamespace = 'default';

  createForm: FormGroup = this.fb.group({
    name: ['', [Validators.required, Validators.pattern(/^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/)]],
    namespace: ['default', [Validators.required]],
    pxcName: ['', [Validators.required]],
    pointInTimeRecover: [true],
    remoteExpireLogHours: [168, [Validators.required, Validators.min(1), Validators.max(8760)]],
    localExpireLogHours: [24, [Validators.required, Validators.min(1), Validators.max(168)]],
    maxLocalBinlogCount: [60, [Validators.required, Validators.min(10), Validators.max(1000)]],
    storageName: ['', [Validators.required]],
    sink: ['', [Validators.required]]
  });

  ngOnInit(): void {
    this.namespaceService.activeNamespace$
      .pipe(takeUntil(this.destroy$))
      .subscribe((namespace: string | null) => {
        this.currentNamespace = namespace || 'default';
        this.createForm.patchValue({ namespace: this.currentNamespace });
        this.loadBackupBinlogs();
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  async loadBackupBinlogs(): Promise<void> {
    try {
      this.loadingService.setLoading(LoadingKeys.BACKUP_BINLOG_LIST, true);
      const response = await this.apiService.getBackupBinlogs(this.currentNamespace).toPromise();
      this.backupBinlogs = response || [];
    } catch (error) {
      console.error('加载增量日志备份列表失败:', error);
      this.msg.error('加载增量日志备份列表失败');
      this.backupBinlogs = [];
    } finally {
      this.loadingService.setLoading(LoadingKeys.BACKUP_BINLOG_LIST, false);
    }
  }

  async submitCreate(): Promise<void> {
    if (!this.createForm.valid) {
      this.markFormGroupTouched(this.createForm);
      return;
    }

    try {
      this.loadingService.setLoading(LoadingKeys.BACKUP_BINLOG_CREATE, true);
      const formValue = this.createForm.value;
      
      const request: CreateBackupBinlogRequest = {
        name: formValue.name,
        namespace: formValue.namespace,
        pxcName: formValue.pxcName,
        pointInTimeRecover: formValue.pointInTimeRecover,
        remoteExpireLogHours: formValue.remoteExpireLogHours,
        localExpireLogHours: formValue.localExpireLogHours,
        maxLocalBinlogCount: formValue.maxLocalBinlogCount,
        storageProvider: {
          storageName: formValue.storageName,
          sink: formValue.sink
        }
      };

      await this.apiService.createBackupBinlog(formValue.namespace, request).toPromise();
      this.msg.success('增量日志备份配置创建成功');
      this.resetForm();
      this.loadBackupBinlogs();
      // 切换到列表页签
      setTimeout(() => {
        // 这里可以添加切换到第一个标签页的逻辑
      }, 100);
    } catch (error) {
      console.error('创建增量日志备份配置失败:', error);
      this.msg.error('创建增量日志备份配置失败');
    } finally {
      this.loadingService.setLoading(LoadingKeys.BACKUP_BINLOG_CREATE, false);
    }
  }

  async deleteBackupBinlog(namespace: string, name: string): Promise<void> {
    try {
      await this.apiService.deleteBackupBinlog(namespace, name).toPromise();
      this.msg.success('删除成功');
      this.loadBackupBinlogs();
    } catch (error) {
      console.error('删除增量日志备份配置失败:', error);
      this.msg.error('删除失败');
    }
  }

  refreshList(): void {
    this.loadBackupBinlogs();
  }

  resetForm(): void {
    this.createForm.reset({
      name: '',
      namespace: this.currentNamespace,
      pxcName: '',
      pointInTimeRecover: true,
      remoteExpireLogHours: 168,
      localExpireLogHours: 24,
      maxLocalBinlogCount: 60,
      storageName: '',
      sink: ''
    });
  }

  switchToCreateTab(): void {
    // 这里可以添加切换到创建配置标签页的逻辑
    // 由于使用的是 nz-tabset，可以通过设置 selectedIndex 来实现
  }

  viewDetails(item: PolarDBXBackupBinlog): void {
    // 这里可以添加查看详情的逻辑
    console.log('查看详情:', item);
  }

  getStatusColor(phase?: string): string {
    switch (phase) {
      case 'Running': return 'success';
      case 'Ready': return 'success';
      case 'Failed': return 'error';
      case 'Pending': return 'processing';
      default: return 'default';
    }
  }

  getStatusText(phase?: string): string {
    switch (phase) {
      case 'Running': return '运行中';
      case 'Ready': return '就绪';
      case 'Failed': return '失败';
      case 'Pending': return '等待中';
      default: return '未知';
    }
  }

  formatDate(dateString?: string): string {
    if (!dateString) return '-';
    return new Date(dateString).toLocaleString('zh-CN');
  }

  private markFormGroupTouched(formGroup: FormGroup): void {
    Object.keys(formGroup.controls).forEach(key => {
      const control = formGroup.get(key);
      control?.markAsTouched();
      control?.updateValueAndValidity();
    });
  }
}