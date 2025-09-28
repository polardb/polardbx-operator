import { Component, OnInit, OnDestroy, TemplateRef, ViewChild } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReactiveFormsModule, FormBuilder, FormGroup, Validators, FormsModule } from '@angular/forms';
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
import { NzModalService } from 'ng-zorro-antd/modal';
import { NzModalModule } from 'ng-zorro-antd/modal';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzSwitchModule } from 'ng-zorro-antd/switch';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzCollapseModule } from 'ng-zorro-antd/collapse';
import { NzDropDownModule } from 'ng-zorro-antd/dropdown';
import { NzMenuModule } from 'ng-zorro-antd/menu';
import { NzPageHeaderModule } from 'ng-zorro-antd/page-header';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzProgressModule } from 'ng-zorro-antd/progress';
import { NzInputNumberModule } from 'ng-zorro-antd/input-number';
import { NzDescriptionsModule } from 'ng-zorro-antd/descriptions';
import { Subject } from 'rxjs';
import { EmptyStateComponent } from '../empty-state/empty-state.component';
import { takeUntil, finalize } from 'rxjs/operators';
import { NamespaceService } from '../../services/namespace.service';

import { ApiService } from '../../services/api.service';
import { LoadingService, LoadingKeys } from '../../services/loading.service';
import { 
  SystemTask, 
  SystemTaskList, 
  CreateSystemTaskRequest, 
  UpdateSystemTaskRequest,
  TASK_TYPE_OPTIONS,
  RESOURCE_PRESETS,
  ResourcePreset,
  getTaskDescription,
  getPhaseColor,
  getPhaseLabel,
  formatResourceValue
} from '../../models/system-task.model';

@Component({
  selector: 'app-system-task-management',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    FormsModule,
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
    NzDropDownModule,
    NzMenuModule,
    NzPageHeaderModule,
    NzGridModule,
    NzProgressModule,
    NzInputNumberModule,
    NzDescriptionsModule,
    NzModalModule,
    EmptyStateComponent
  ],
  template: `
    <div class="system-task-management">
      <nz-page-header [nzGhost]="false" nzTitle="系统任务管理" nzSubtitle="管理集群系统资源平衡任务">
        <nz-page-header-extra>
          <i nz-icon nzType="profile" class="page-icon"></i>
        </nz-page-header-extra>
      </nz-page-header>

      <nz-tabset class="main-tabs" [(nzSelectedIndex)]="selectedTab" (nzSelectedIndexChange)="onTabChange($event)">
        <!-- 系统任务列表选项卡 -->
        <nz-tab nzTitle="系统任务">
          <div class="tab-content">
            <div class="actions-toolbar">
              <button nz-button nzType="primary" (click)="refreshTasks()" 
                      [nzLoading]="isLoading('SYSTEM_TASK_LIST')">
                <i nz-icon nzType="reload"></i>
                刷新
              </button>
              <button nz-button nzType="default" (click)="selectedTab = 1">
                <i nz-icon nzType="plus"></i>
                创建任务
              </button>
            </div>

            <nz-card class="table-card">
              <div class="table-container" *ngIf="!isLoading('SYSTEM_TASK_LIST'); else loadingTemplate">
                <nz-table [nzData]="systemTasks" [nzShowPagination]="false" class="system-tasks-table">
                  <thead>
                    <tr>
                      <th>名称</th>
                      <th>命名空间</th>
                      <th>任务类型</th>
                      <th>状态</th>
                      <th>配置</th>
                      <th>进度</th>
                      <th>操作</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr *ngFor="let task of systemTasks">
                      <td>
                        <div class="task-name">
                          <i nz-icon [nzType]="getTaskIcon(task.spec.taskType)" [style.color]="colorForPhase(task.status?.phase)"></i>
                          <span>{{ task.metadata.name }}</span>
                        </div>
                      </td>
                      <td>{{ task.metadata.namespace }}</td>
                      <td>
                        <nz-tag [nzColor]="colorForPhase(task.status?.phase)">
                          {{ getTaskTypeLabel(task.spec.taskType) }}
                        </nz-tag>
                      </td>
                      <td>
                        <nz-tag [nzColor]="colorForPhase(task.status?.phase)">
                          {{ labelForPhase(task.status?.phase) }}
                        </nz-tag>
                      </td>
                      <td>
                        <div class="task-description">
                          {{ getTaskDescription(task) }}
                        </div>
                      </td>
                      <td>
                        <nz-progress [nzPercent]="taskProgress(task)" 
                                     [nzStatus]="taskProgress(task) >= 100 ? 'success' : 'active'"
                                     nzSize="small">
                        </nz-progress>
                      </td>
                      <td>
                        <button nz-button nzType="text" nz-dropdown [nzDropdownMenu]="taskMenu" [nzLoading]="isLoading('SYSTEM_TASK_UPDATE')">
                          <span>操作</span>
                          <i nz-icon nzType="down"></i>
                        </button>
                        <nz-dropdown-menu #taskMenu="nzDropdownMenu">
                          <ul nz-menu>
                            <li nz-menu-item (click)="viewTaskDetails(task)"><i nz-icon nzType="eye"></i> 查看详情</li>
                            <li nz-menu-item (click)="editTask(task)" [class.disabled]="task.status?.phase === 'Success'"><i nz-icon nzType="edit"></i> 编辑任务</li>
                            <li nz-menu-item (click)="deleteTask(task)" class="delete-action"><i nz-icon nzType="delete"></i> 删除任务</li>
                          </ul>
                        </nz-dropdown-menu>
                      </td>
                    </tr>
                  </tbody>
                </nz-table>

                <app-empty-state *ngIf="systemTasks.length === 0"
                                 icon="profile"
                                 title="未找到系统任务"
                                 [hint]="'点击“创建任务”开始使用'"></app-empty-state>
              </div>
            </nz-card>
          </div>
        </nz-tab>

        <!-- 创建/编辑任务选项卡 -->
        <nz-tab nzTitle="创建任务">
          <div class="tab-content">
            <nz-card class="form-card">
              <ng-template #title>
                <span>{{ editingTask ? '编辑系统任务' : '创建新系统任务' }}</span>
              </ng-template>
              
              <form nz-form [formGroup]="taskForm" nzLayout="vertical" class="task-form">
                <!-- 基本配置 -->
                <nz-collapse [nzExpandIconPosition]="'end'">
                  <nz-collapse-panel nzHeader="基本配置" [nzActive]="true">
                    <nz-row [nzGutter]="16">
                      <nz-col [nzSpan]="12">
                        <nz-form-item>
                          <nz-form-label nzRequired>任务名称</nz-form-label>
                          <nz-form-control nzErrorTip="任务名称是必填项">
                            <input nz-input formControlName="name" placeholder="输入任务名称" />
                          </nz-form-control>
                        </nz-form-item>
                      </nz-col>
                      <nz-col [nzSpan]="6">
                        <nz-form-item>
                          <nz-form-label>命名空间</nz-form-label>
                          <nz-form-control>
                            <input nz-input formControlName="namespace" placeholder="default" />
                          </nz-form-control>
                        </nz-form-item>
                      </nz-col>
                      <nz-col [nzSpan]="6">
                        <nz-form-item>
                          <nz-form-label>任务类型</nz-form-label>
                          <nz-form-control>
                            <nz-select formControlName="taskType" nzPlaceHolder="选择任务类型">
                              <nz-option *ngFor="let option of taskTypeOptions" [nzValue]="option.value">
                                <i nz-icon [nzType]="option.icon"></i>
                                {{ option.label }}
                              </nz-option>
                            </nz-select>
                          </nz-form-control>
                        </nz-form-item>
                      </nz-col>
                    </nz-row>
                  </nz-collapse-panel>

                  <!-- 资源配置 -->
                  <nz-collapse-panel nzHeader="资源配置">
                    <nz-row [nzGutter]="16">
                      <nz-col [nzSpan]="12">
                        <nz-form-item>
                          <nz-form-label>最大 CN 副本数</nz-form-label>
                          <nz-form-control nzErrorTip="必须至少为 1">
                            <nz-input-number formControlName="cnReplicas" 
                                           [nzMin]="1" 
                                           nzPlaceHolder="计算节点数量"
                                           style="width: 100%">
                            </nz-input-number>
                          </nz-form-control>
                        </nz-form-item>
                      </nz-col>
                      <nz-col [nzSpan]="12">
                        <nz-form-item>
                          <nz-form-label>资源预设</nz-form-label>
                          <nz-form-control>
                            <nz-select (ngModelChange)="applyResourcePreset($event)" [ngModel]="null" [ngModelOptions]="{ standalone: true }" nzPlaceHolder="选择预设配置">
                              <nz-option [nzValue]="null" nzLabel="自定义配置"></nz-option>
                              <nz-option *ngFor="let preset of resourcePresets" [nzValue]="preset" 
                                         [nzLabel]="preset.label + ' - ' + preset.description">
                              </nz-option>
                            </nz-select>
                          </nz-form-control>
                        </nz-form-item>
                      </nz-col>
                    </nz-row>

                    <!-- CN 资源 -->
                    <div class="resource-group">
                      <h4>CN (计算节点) 资源</h4>
                      <nz-row [nzGutter]="8">
                        <nz-col [nzSpan]="6">
                          <nz-form-item>
                            <nz-form-label>CPU 请求</nz-form-label>
                            <nz-form-control>
                              <input nz-input formControlName="cnCpuRequest" placeholder="100m" />
                            </nz-form-control>
                          </nz-form-item>
                        </nz-col>
                        <nz-col [nzSpan]="6">
                          <nz-form-item>
                            <nz-form-label>CPU 限制</nz-form-label>
                            <nz-form-control>
                              <input nz-input formControlName="cnCpuLimit" placeholder="500m" />
                            </nz-form-control>
                          </nz-form-item>
                        </nz-col>
                        <nz-col [nzSpan]="6">
                          <nz-form-item>
                            <nz-form-label>内存请求</nz-form-label>
                            <nz-form-control>
                              <input nz-input formControlName="cnMemoryRequest" placeholder="256Mi" />
                            </nz-form-control>
                          </nz-form-item>
                        </nz-col>
                        <nz-col [nzSpan]="6">
                          <nz-form-item>
                            <nz-form-label>内存限制</nz-form-label>
                            <nz-form-control>
                              <input nz-input formControlName="cnMemoryLimit" placeholder="512Mi" />
                            </nz-form-control>
                          </nz-form-item>
                        </nz-col>
                      </nz-row>
                    </div>

                    <!-- DN 资源 -->
                    <div class="resource-group">
                      <h4>DN (数据节点) 资源</h4>
                      <nz-row [nzGutter]="8">
                        <nz-col [nzSpan]="6">
                          <nz-form-item>
                            <nz-form-label>CPU 请求</nz-form-label>
                            <nz-form-control>
                              <input nz-input formControlName="dnCpuRequest" placeholder="200m" />
                            </nz-form-control>
                          </nz-form-item>
                        </nz-col>
                        <nz-col [nzSpan]="6">
                          <nz-form-item>
                            <nz-form-label>CPU 限制</nz-form-label>
                            <nz-form-control>
                              <input nz-input formControlName="dnCpuLimit" placeholder="1" />
                            </nz-form-control>
                          </nz-form-item>
                        </nz-col>
                        <nz-col [nzSpan]="6">
                          <nz-form-item>
                            <nz-form-label>内存请求</nz-form-label>
                            <nz-form-control>
                              <input nz-input formControlName="dnMemoryRequest" placeholder="512Mi" />
                            </nz-form-control>
                          </nz-form-item>
                        </nz-col>
                        <nz-col [nzSpan]="6">
                          <nz-form-item>
                            <nz-form-label>内存限制</nz-form-label>
                            <nz-form-control>
                              <input nz-input formControlName="dnMemoryLimit" placeholder="1Gi" />
                            </nz-form-control>
                          </nz-form-item>
                        </nz-col>
                      </nz-row>
                    </div>
                  </nz-collapse-panel>

                  <!-- 节点选择 -->
                  <nz-collapse-panel nzHeader="节点选择 (可选)">
                    <nz-form-item>
                      <nz-form-label>目标节点</nz-form-label>
                      <nz-form-control nzExtra="留空以允许在所有可用节点上调度">
                        <input nz-input formControlName="nodes" 
                               placeholder="node1,node2,node3 (逗号分隔)" />
                      </nz-form-control>
                    </nz-form-item>
                  </nz-collapse-panel>
                </nz-collapse>

                <div class="form-actions">
                  <button nz-button nzType="default" (click)="resetForm()" [nzLoading]="isLoading('SYSTEM_TASK_CREATE')">
                    重置
                  </button>
                  <button nz-button nzType="primary" 
                          (click)="submitTask()" 
                          [nzLoading]="isLoading('SYSTEM_TASK_CREATE')"
                          [disabled]="taskForm.invalid">
                    <i nz-icon [nzType]="editingTask ? 'save' : 'plus'"></i>
                    {{ editingTask ? '更新任务' : '创建任务' }}
                  </button>
                </div>
              </form>
            </nz-card>
          </div>
        </nz-tab>
      </nz-tabset>
    </div>

    <!-- 加载模板 -->
    <ng-template #loadingTemplate>
      <div class="loading-container">
        <nz-spin nzSize="large">
          <p>正在加载系统任务...</p>
        </nz-spin>
      </div>
    </ng-template>

    <!-- 详情对话框模板 -->
    <ng-template #taskDetails let-data>
      <div class="task-details">
        <h3>任务详情</h3>
        <nz-descriptions [nzColumn]="2" nzBordered>
          <nz-descriptions-item nzTitle="名称">{{ data?.metadata?.name }}</nz-descriptions-item>
          <nz-descriptions-item nzTitle="命名空间">{{ data?.metadata?.namespace }}</nz-descriptions-item>
          <nz-descriptions-item nzTitle="类型">{{ getTaskTypeLabel(data?.spec?.taskType) }}</nz-descriptions-item>
          <nz-descriptions-item nzTitle="状态">
            <nz-tag [nzColor]="colorForPhase(data?.status?.phase)">{{ labelForPhase(data?.status?.phase) }}</nz-tag>
          </nz-descriptions-item>
        </nz-descriptions>
        
        <div class="progress-section">
          <h4>执行进度</h4>
          <nz-progress [nzPercent]="taskProgress(data)" [nzStatus]="taskProgress(data) >= 100 ? 'success' : 'active'"></nz-progress>
          
          <div class="steps">
            <div class="step-item" [class.completed]="data?.status?.stBalanceResourceStatus?.rebuildFinish">
              <i nz-icon [nzType]="data?.status?.stBalanceResourceStatus?.rebuildFinish ? 'check-circle' : 'play-circle'" 
                 [style.color]="data?.status?.stBalanceResourceStatus?.rebuildFinish ? '#52c41a' : '#1890ff'"></i>
              <span>Step 1 · Rebuild（重建）</span>
              <small *ngIf="data?.status?.stBalanceResourceStatus?.rebuildTaskName" class="task-name">
                {{ data.status.stBalanceResourceStatus.rebuildTaskName }}
              </small>
            </div>
            <div class="step-item" [class.completed]="data?.status?.stBalanceResourceStatus?.balanceLeaderFinish">
              <i nz-icon [nzType]="data?.status?.stBalanceResourceStatus?.balanceLeaderFinish ? 'check-circle' : 'play-circle'"
                 [style.color]="data?.status?.stBalanceResourceStatus?.balanceLeaderFinish ? '#52c41a' : '#1890ff'"></i>
              <span>Step 2 · Balance Leaders（平衡 Leader）</span>
            </div>
            <div class="step-item" [class.completed]="data?.status?.phase === 'Success'">
              <i nz-icon [nzType]="data?.status?.phase === 'Success' ? 'check-circle' : 'clock-circle'"
                 [style.color]="data?.status?.phase === 'Success' ? '#52c41a' : '#d9d9d9'"></i>
              <span>Step 3 · 完成</span>
            </div>
          </div>
        </div>
      </div>
    </ng-template>
  `,
  styles: [`
    .system-task-management { 
      padding: 16px 24px; 
      background: #f5f5f5; 
      min-height: 100vh; 
    }
    
    .page-icon { 
      font-size: 16px; 
      color: #1890ff; 
    }
    
    .main-tabs {
      margin-top: 16px;
      background: #fff;
      border-radius: 8px;
      padding: 16px;
      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06);
    }
    
    .tab-content {
      padding: 16px 0;
    }
    
    .actions-toolbar { 
      display: flex; 
      gap: 12px; 
      margin-bottom: 16px; 
    }
    
    .table-card, .form-card {
      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06);
      border-radius: 8px;
    }
    
    .system-tasks-table { 
      width: 100%; 
    }
    
    .system-tasks-table th {
      background: #fafafa;
      font-weight: 600;
      color: #262626;
    }
    
    .system-tasks-table td {
      border-bottom: 1px solid #f0f0f0;
    }
    
    .task-name {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    
    .task-description {
      max-width: 200px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    
    .task-form {
      margin-top: 16px;
    }
    
    .resource-group {
      margin: 16px 0;
      padding: 16px;
      background: #fafafa;
      border-radius: 6px;
    }
    
    .resource-group h4 {
      margin: 0 0 16px 0;
      color: #262626;
      font-weight: 600;
    }
    
    .form-actions { 
      display: flex; 
      gap: 12px; 
      justify-content: flex-end;
      margin-top: 24px;
      padding-top: 16px;
      border-top: 1px solid #f0f0f0;
    }
    
    .loading-container {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      padding: 80px 20px;
    }
    
    .loading-container p {
      margin-top: 16px;
      color: #666;
    }
    
    .delete-action {
      color: #ff4d4f !important;
    }
    
    .disabled {
      opacity: 0.5;
      pointer-events: none;
    }
    
    .task-details h3 {
      margin-bottom: 16px;
      color: #262626;
    }
    
    .progress-section {
      margin-top: 24px;
    }
    
    .progress-section h4 {
      margin-bottom: 12px;
      color: #262626;
    }
    
    .steps {
      margin-top: 16px;
    }
    
    .step-item {
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 8px 0;
      border-bottom: 1px solid #f0f0f0;
    }
    
    .step-item:last-child {
      border-bottom: none;
    }
    
    .step-item.completed {
      color: #52c41a;
    }
    
    .step-item .task-name {
      margin-left: 8px;
      font-size: 12px;
      color: #666;
    }
  `]
})
export class SystemTaskManagementComponent implements OnInit, OnDestroy {
  private destroy$ = new Subject<void>();
  
  systemTasks: SystemTask[] = [];
  selectedTab = 0;
  editingTask: SystemTask | null = null;
  
  taskTypeOptions = TASK_TYPE_OPTIONS;
  resourcePresets = RESOURCE_PRESETS;
  
  taskForm: FormGroup;
  @ViewChild('taskDetails') taskDetailsTpl!: TemplateRef<any>;

  constructor(
    private apiService: ApiService,
    private loadingService: LoadingService,
    private fb: FormBuilder,
    private message: NzMessageService,
    private modal: NzModalService,
    private ns: NamespaceService
  ) {
    this.taskForm = this.createTaskForm();
  }

  ngOnInit(): void {
    this.loadSystemTasks();
    this.ns.activeNamespace$.pipe(takeUntil(this.destroy$)).subscribe(() => this.loadSystemTasks());
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  private createTaskForm(): FormGroup {
    return this.fb.group({
      name: ['', [Validators.required, Validators.pattern(/^[a-z0-9-]+$/)]],
      namespace: ['default'],
      taskType: ['BalanceResource'],
      cnReplicas: [1, [Validators.min(1)]],
      cnCpuRequest: [''],
      cnCpuLimit: [''],
      cnMemoryRequest: [''],
      cnMemoryLimit: [''],
      dnCpuRequest: [''],
      dnCpuLimit: [''],
      dnMemoryRequest: [''],
      dnMemoryLimit: [''],
      nodes: ['']
    });
  }

  isLoading(key: keyof typeof LoadingKeys): boolean {
    return this.loadingService.isLoading(LoadingKeys[key]);
  }

  onTabChange(index: number): void {
    this.selectedTab = index;
    if (index === 0) {
      this.editingTask = null;
      this.resetForm();
    }
  }

  loadSystemTasks(): void {
    this.apiService.getSystemTasks()
      .pipe(
        takeUntil(this.destroy$),
        finalize(() => {})
      )
      .subscribe({
        next: (taskList: SystemTaskList) => {
          this.systemTasks = taskList.items || [];
        },
        error: (error: any) => {
          console.error('加载系统任务失败:', error);
          this.message.error('加载系统任务失败');
        }
      });
  }

  refreshTasks(): void {
    this.loadSystemTasks();
  }

  applyResourcePreset(preset: ResourcePreset | null): void {
    if (!preset) return;
    
    this.taskForm.patchValue({
      cnCpuRequest: preset.cnResources.requests?.cpu || '',
      cnCpuLimit: preset.cnResources.limits?.cpu || '',
      cnMemoryRequest: preset.cnResources.requests?.memory || '',
      cnMemoryLimit: preset.cnResources.limits?.memory || '',
      dnCpuRequest: preset.dnResources.requests?.cpu || '',
      dnCpuLimit: preset.dnResources.limits?.cpu || '',
      dnMemoryRequest: preset.dnResources.requests?.memory || '',
      dnMemoryLimit: preset.dnResources.limits?.memory || ''
    });
  }

  submitTask(): void {
    if (this.taskForm.invalid) return;

    const formValue = this.taskForm.value;
    const taskRequest: CreateSystemTaskRequest | UpdateSystemTaskRequest = {
      name: formValue.name,
      namespace: formValue.namespace || 'default',
      taskType: formValue.taskType,
      cnReplicas: formValue.cnReplicas,
      cnResources: {
        requests: {
          cpu: formValue.cnCpuRequest || undefined,
          memory: formValue.cnMemoryRequest || undefined
        },
        limits: {
          cpu: formValue.cnCpuLimit || undefined,
          memory: formValue.cnMemoryLimit || undefined
        }
      },
      dnResources: {
        requests: {
          cpu: formValue.dnCpuRequest || undefined,
          memory: formValue.dnMemoryRequest || undefined
        },
        limits: {
          cpu: formValue.dnCpuLimit || undefined,
          memory: formValue.dnMemoryLimit || undefined
        }
      },
      nodes: formValue.nodes ? formValue.nodes.split(',').map((n: string) => n.trim()).filter((n: string) => n) : undefined
    };

    const operation = this.editingTask
      ? this.apiService.updateSystemTask(this.editingTask.metadata?.['namespace']!, this.editingTask.metadata.name, {
          ...taskRequest,
          resourceVersion: this.editingTask.metadata?.['resourceVersion']
        } as UpdateSystemTaskRequest)
      : this.apiService.createSystemTask((taskRequest as CreateSystemTaskRequest)?.['namespace']!, taskRequest as CreateSystemTaskRequest);

    operation.pipe(
      takeUntil(this.destroy$),
      finalize(() => {})
    ).subscribe({
      next: (task) => {
        const message = this.editingTask ? '系统任务更新成功' : '系统任务创建成功';
        this.message.success(message);
        this.resetForm();
        this.selectedTab = 0;
        this.loadSystemTasks();
      },
      error: (error) => {
        console.error('保存系统任务失败:', error);
        this.message.error('保存系统任务失败');
      }
    });
  }

  editTask(task: SystemTask): void {
    this.editingTask = task;
    this.taskForm.patchValue({
      name: task.metadata.name,
      namespace: task.metadata.namespace,
      taskType: task.spec.taskType,
      cnReplicas: task.spec.cnReplicas,
      cnCpuRequest: task.spec.cnResources?.requests?.cpu || '',
      cnCpuLimit: task.spec.cnResources?.limits?.cpu || '',
      cnMemoryRequest: task.spec.cnResources?.requests?.memory || '',
      cnMemoryLimit: task.spec.cnResources?.limits?.memory || '',
      dnCpuRequest: task.spec.dnResources?.requests?.cpu || '',
      dnCpuLimit: task.spec.dnResources?.limits?.cpu || '',
      dnMemoryRequest: task.spec.dnResources?.requests?.memory || '',
      dnMemoryLimit: task.spec.dnResources?.limits?.memory || '',
      nodes: task.spec.nodes?.join(', ') || ''
    });
    this.selectedTab = 1;
  }

  deleteTask(task: SystemTask): void {
    if (confirm(`确定要删除系统任务 "${task.metadata.name}" 吗？`)) {
      this.apiService.deleteSystemTask(task.metadata.namespace!, task.metadata.name)
        .pipe(
          takeUntil(this.destroy$),
          finalize(() => {})
        )
        .subscribe({
          next: () => {
            this.message.success('系统任务删除成功');
            this.loadSystemTasks();
          },
          error: (error) => {
            console.error('删除系统任务失败:', error);
            this.message.error('删除系统任务失败');
          }
        });
    }
  }

  viewTaskDetails(task: SystemTask): void {
    this.modal.create({
      nzTitle: '任务详情',
      nzContent: this.taskDetailsTpl,
      nzData: task,
      nzFooter: null,
      nzWidth: 600
    });
  }

  resetForm(): void {
    this.editingTask = null;
    this.taskForm.reset({
      name: '',
      namespace: 'default',
      taskType: 'BalanceResource',
      cnReplicas: 1,
      cnCpuRequest: '',
      cnCpuLimit: '',
      cnMemoryRequest: '',
      cnMemoryLimit: '',
      dnCpuRequest: '',
      dnCpuLimit: '',
      dnMemoryRequest: '',
      dnMemoryLimit: '',
      nodes: ''
    });
  }

  getTaskIcon(taskType?: string): string {
    return 'cluster';
  }

  getTaskTypeLabel(taskType?: string): string {
    const option = this.taskTypeOptions.find((opt: any) => opt.value === taskType);
    return option?.label || taskType || 'Unknown';
  }

  getPhaseColor = getPhaseColor;
  getPhaseLabel = getPhaseLabel;
  getTaskDescription = getTaskDescription;
  formatResourceValue = formatResourceValue;

  colorForPhase(phase?: any): string {
    const p = (phase || '') as any;
    try { return getPhaseColor(p as any); } catch { return 'default'; }
  }

  labelForPhase(phase?: any): string {
    const p = (phase || '') as any;
    try { return getPhaseLabel(p as any); } catch { return 'Unknown'; }
  }

  taskProgress(task: SystemTask): number {
    const phase = (task?.status?.phase || '').toString();
    const rebuildDone = !!task?.status?.stBalanceResourceStatus?.rebuildFinish;
    const balanceDone = !!task?.status?.stBalanceResourceStatus?.balanceLeaderFinish;
    if (phase === 'Success') return 100;
    if (balanceDone) return 90;
    if (rebuildDone) return 60;
    if (phase) return 30;
    return 10;
  }
}