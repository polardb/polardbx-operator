import { Component, OnInit, ChangeDetectionStrategy, ChangeDetectorRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReactiveFormsModule, FormBuilder, FormGroup, Validators } from '@angular/forms';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzModalModule, NzModalService } from 'ng-zorro-antd/modal';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzDescriptionsModule } from 'ng-zorro-antd/descriptions';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzCodeEditorModule } from 'ng-zorro-antd/code-editor';

import { ApiService } from '../../services/api.service';

interface LogStrategy {
  id: string;
  name: string;
  description?: string;
  config: any;
  status: 'draft' | 'applied' | 'error';
  appliedAt?: Date;
  updatedAt: Date;
  targetNamespaces?: string[];
  targetNodeTypes?: string[];
}

interface ApplyRecord {
  id: string;
  strategyId: string;
  strategyName: string;
  appliedAt: Date;
  status: 'success' | 'failed';
  message?: string;
  targets?: string[];
}

@Component({
  selector: 'app-log-strategy-management',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    NzCardModule,
    NzButtonModule,
    NzIconModule,
    NzTableModule,
    NzInputModule,
    NzFormModule,
    NzSelectModule,
    NzAlertModule,
    NzSpinModule,
    NzTagModule,
    NzModalModule,
    NzDescriptionsModule,
    NzEmptyModule,
    NzCodeEditorModule
  ],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="log-strategy-management">
      <div class="page-header">
        <div class="header-content">
          <h1 class="page-title">
            <i nz-icon nzType="file-text" class="page-icon"></i>
            日志策略管理
          </h1>
          <p class="page-description">管理 PolarDB-X 集群日志收集策略</p>
        </div>
        <div class="header-actions">
          <button 
            nz-button 
            nzType="default" 
            (click)="refreshStrategies()"
            [nzLoading]="loading">
            <i nz-icon nzType="sync"></i>
            刷新
          </button>
          <button 
            nz-button 
            nzType="primary" 
            (click)="createStrategy()">
            <i nz-icon nzType="plus"></i>
            新建策略
          </button>
        </div>
      </div>

      <div class="page-content">
        <!-- 策略列表 -->
        <nz-card nzTitle="日志策略列表" class="strategies-card">
          <nz-table 
            #strategiesTable 
            [nzData]="strategies" 
            [nzLoading]="loading"
            [nzShowPagination]="strategies.length > 10"
            [nzPageSize]="10"
            nzSize="middle">
            
            <thead>
              <tr>
                <th>策略名称</th>
                <th>状态</th>
                <th>目标命名空间</th>
                <th>更新时间</th>
                <th>应用时间</th>
                <th nzWidth="200px">操作</th>
              </tr>
            </thead>
            <tbody>
              <tr *ngFor="let strategy of strategiesTable.data">
                <td>
                  <div class="strategy-name">
                    <strong>{{ strategy.name }}</strong>
                    <div class="strategy-description" *ngIf="strategy.description">
                      {{ strategy.description }}
                    </div>
                  </div>
                </td>
                <td>
                  <nz-tag [nzColor]="getStatusColor(strategy.status)">
                    {{ getStatusText(strategy.status) }}
                  </nz-tag>
                </td>
                <td>
                  <div class="target-namespaces">
                    <nz-tag 
                      *ngFor="let ns of strategy.targetNamespaces?.slice(0, 2)" 
                      nzColor="blue"
                      class="namespace-tag">
                      {{ ns }}
                    </nz-tag>
                    <span *ngIf="(strategy.targetNamespaces?.length || 0) > 2" class="more-count">
                      +{{ (strategy.targetNamespaces?.length || 0) - 2 }}
                    </span>
                  </div>
                </td>
                <td>{{ strategy.updatedAt | date:'yyyy-MM-dd HH:mm' }}</td>
                <td>
                  <span *ngIf="strategy.appliedAt; else notApplied">
                    {{ strategy.appliedAt | date:'yyyy-MM-dd HH:mm' }}
                  </span>
                  <ng-template #notApplied>
                    <span class="not-applied">未应用</span>
                  </ng-template>
                </td>
                <td>
                  <div class="action-buttons">
                    <button 
                      nz-button 
                      nzType="link" 
                      nzSize="small"
                      (click)="viewStrategy(strategy)">
                      查看
                    </button>
                    <button 
                      nz-button 
                      nzType="link" 
                      nzSize="small"
                      (click)="editStrategy(strategy)">
                      编辑
                    </button>
                    <button 
                      nz-button 
                      nzType="link" 
                      nzSize="small"
                      [nzDanger]="true"
                      (click)="deleteStrategy(strategy)">
                      删除
                    </button>
                  </div>
                </td>
              </tr>
            </tbody>
          </nz-table>

          <div class="empty-state" *ngIf="!loading && strategies.length === 0">
            <nz-empty 
              nzNotFoundImage="simple"
              nzNotFoundContent="暂无日志策略，点击'新建策略'开始创建">
            </nz-empty>
          </div>
        </nz-card>

        <!-- 应用记录 -->
        <nz-card nzTitle="最近应用记录" class="records-card">
          <nz-table 
            #recordsTable 
            [nzData]="applyRecords" 
            [nzLoading]="loadingRecords"
            [nzShowPagination]="applyRecords.length > 5"
            [nzPageSize]="5"
            nzSize="small">
            
            <thead>
              <tr>
                <th>策略名称</th>
                <th>应用状态</th>
                <th>目标</th>
                <th>应用时间</th>
                <th>消息</th>
              </tr>
            </thead>
            <tbody>
              <tr *ngFor="let record of recordsTable.data">
                <td>{{ record.strategyName }}</td>
                <td>
                  <nz-tag [nzColor]="record.status === 'success' ? 'green' : 'red'">
                    {{ record.status === 'success' ? '成功' : '失败' }}
                  </nz-tag>
                </td>
                <td>
                  <div class="apply-targets">
                    <span *ngFor="let target of record.targets?.slice(0, 3); let i = index">
                      {{ target }}<span *ngIf="i < ((record.targets || []).slice(0, 3).length) - 1">, </span>
                    </span>
                    <span *ngIf="(record.targets ? record.targets.length : 0) > 3">
                      ...
                    </span>
                  </div>
                </td>
                <td>{{ record.appliedAt | date:'yyyy-MM-dd HH:mm:ss' }}</td>
                <td>
                  <span class="apply-message">{{ record.message || '-' }}</span>
                </td>
              </tr>
            </tbody>
          </nz-table>

          <div class="empty-records" *ngIf="!loadingRecords && applyRecords.length === 0">
            <nz-empty 
              nzNotFoundImage="simple"
              nzNotFoundContent="暂无应用记录">
            </nz-empty>
          </div>
        </nz-card>
      </div>
    </div>

    <!-- 策略编辑模态框 -->
    <nz-modal
      [(nzVisible)]="editModalVisible"
      [nzTitle]="editingStrategy ? '编辑日志策略' : '新建日志策略'"
      [nzWidth]="800"
      [nzFooter]="null"
      [nzClosable]="true"
      [nzMaskClosable]="false"
      (nzOnCancel)="closeEditModal()">
      
      <ng-container *nzModalContent>
        <form [formGroup]="editForm" class="strategy-form">
          <nz-form-item>
            <nz-form-label [nzSpan]="6" nzRequired>策略名称</nz-form-label>
            <nz-form-control [nzSpan]="18">
              <input 
                nz-input 
                formControlName="name" 
                placeholder="输入策略名称">
            </nz-form-control>
          </nz-form-item>

          <nz-form-item>
            <nz-form-label [nzSpan]="6">描述</nz-form-label>
            <nz-form-control [nzSpan]="18">
              <textarea 
                nz-input 
                formControlName="description" 
                placeholder="输入策略描述（可选）"
                rows="3">
              </textarea>
            </nz-form-control>
          </nz-form-item>

          <nz-form-item>
            <nz-form-label [nzSpan]="6">目标命名空间</nz-form-label>
            <nz-form-control [nzSpan]="18">
              <nz-select 
                formControlName="targetNamespaces" 
                nzMode="multiple"
                nzPlaceholder="选择目标命名空间"
                nzAllowClear
                nzShowSearch>
                <nz-option 
                  *ngFor="let ns of namespaces" 
                  [nzValue]="ns" 
                  [nzLabel]="ns">
                </nz-option>
              </nz-select>
            </nz-form-control>
          </nz-form-item>

          <nz-form-item>
            <nz-form-label [nzSpan]="6">配置格式</nz-form-label>
            <nz-form-control [nzSpan]="18">
              <nz-select 
                formControlName="configFormat" 
                (ngModelChange)="onConfigFormatChange($event)">
                <nz-option nzValue="json" nzLabel="JSON"></nz-option>
                <nz-option nzValue="yaml" nzLabel="YAML"></nz-option>
              </nz-select>
            </nz-form-control>
          </nz-form-item>

          <nz-form-item>
            <nz-form-label [nzSpan]="6" nzRequired>策略配置</nz-form-label>
            <nz-form-control [nzSpan]="18">
              <div class="config-editor">
                <div class="editor-actions">
                  <button 
                    nz-button 
                    nzType="default" 
                    nzSize="small"
                    (click)="precheck()"
                    [nzLoading]="prechecking"
                    [disabled]="!editForm.get('config')?.value?.trim()">
                    <i nz-icon nzType="check-circle"></i>
                    预检
                  </button>
                  <button 
                    nz-button 
                    nzType="dashed" 
                    nzSize="small"
                    (click)="loadTemplate()">
                    <i nz-icon nzType="file-add"></i>
                    加载模板
                  </button>
                  <button 
                    nz-button 
                    nzType="dashed" 
                    nzSize="small"
                    (click)="formatConfig()">
                    <i nz-icon nzType="align-left"></i>
                    格式化
                  </button>
                </div>

                <nz-code-editor
                  formControlName="config"
                  [nzEditorOption]="getEditorOptions()"
                  style="height: 300px;">
                </nz-code-editor>

                <div class="precheck-result" *ngIf="precheckResult">
                  <nz-alert 
                    [nzType]="precheckResult.success ? 'success' : 'error'"
                    [nzMessage]="precheckResult.success ? '预检通过' : '预检失败'"
                    [nzDescription]="precheckResult.message"
                    nzShowIcon>
                  </nz-alert>
                </div>
              </div>
            </nz-form-control>
          </nz-form-item>

          <nz-form-item>
            <nz-form-control [nzSpan]="18" [nzOffset]="6">
              <div class="form-actions">
                <button 
                  nz-button 
                  nzType="default" 
                  (click)="closeEditModal()">
                  取消
                </button>
                <button 
                  nz-button 
                  nzType="primary" 
                  (click)="saveStrategy()"
                  [nzLoading]="saving"
                  [disabled]="!editForm.valid">
                  保存
                </button>
                <button 
                  *ngIf="editingStrategy"
                  nz-button 
                  nzType="primary" 
                  nzDanger
                  (click)="applyStrategy()"
                  [nzLoading]="applying"
                  [disabled]="!editForm.valid">
                  <i nz-icon nzType="rocket"></i>
                  应用策略
                </button>
              </div>
            </nz-form-control>
          </nz-form-item>
        </form>
      </ng-container>
    </nz-modal>

    <!-- 策略查看模态框 -->
    <nz-modal
      [(nzVisible)]="viewModalVisible"
      nzTitle="查看日志策略"
      [nzWidth]="700"
      [nzFooter]="null"
      (nzOnCancel)="closeViewModal()">
      
      <ng-container *nzModalContent>
        <div class="strategy-details" *ngIf="viewingStrategy">
          <nz-descriptions nzBordered nzSize="small">
            <nz-descriptions-item nzTitle="策略名称">{{ viewingStrategy.name }}</nz-descriptions-item>
            <nz-descriptions-item nzTitle="状态">
              <nz-tag [nzColor]="getStatusColor(viewingStrategy.status)">
                {{ getStatusText(viewingStrategy.status) }}
              </nz-tag>
            </nz-descriptions-item>
            <nz-descriptions-item nzTitle="描述" nzSpan="2">
              {{ viewingStrategy.description || '无' }}
            </nz-descriptions-item>
            <nz-descriptions-item nzTitle="目标命名空间" nzSpan="2">
              <nz-tag 
                *ngFor="let ns of viewingStrategy.targetNamespaces" 
                nzColor="blue">
                {{ ns }}
              </nz-tag>
            </nz-descriptions-item>
            <nz-descriptions-item nzTitle="更新时间">
              {{ viewingStrategy.updatedAt | date:'yyyy-MM-dd HH:mm:ss' }}
            </nz-descriptions-item>
            <nz-descriptions-item nzTitle="应用时间">
              <span *ngIf="viewingStrategy.appliedAt; else notAppliedView">
                {{ viewingStrategy.appliedAt | date:'yyyy-MM-dd HH:mm:ss' }}
              </span>
              <ng-template #notAppliedView>
                <span class="not-applied">未应用</span>
              </ng-template>
            </nz-descriptions-item>
          </nz-descriptions>

            <div class="config-section">
              <h4>策略配置</h4>
              <div class="config-viewer">
                <pre>{{ getViewConfigContent() }}</pre>
              </div>
            </div>
        </div>
      </ng-container>
    </nz-modal>
  `,
  styles: [`
    .log-strategy-management {
      padding: 16px;
      background: #f5f5f5;
      min-height: 100vh;
    }

    .page-header {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      margin-bottom: 16px;
      background: #fff;
      padding: 16px 24px;
      border-radius: 8px;
      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06);
      border: 1px solid #e8e8e8;
    }

    .header-content {
      flex: 1;
    }

    .page-title {
      color: rgba(0, 0, 0, 0.87);
      font-size: 20px;
      font-weight: 500;
      margin: 0 0 4px 0;
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .page-icon {
      font-size: 22px;
      color: #1890ff;
    }

    .page-description {
      color: rgba(0, 0, 0, 0.6);
      font-size: 14px;
      margin: 0;
      line-height: 1.5;
    }

    .header-actions {
      display: flex;
      gap: 8px;
      flex-shrink: 0;
    }

    .page-content {
      max-width: 1200px;
      margin: 0 auto;
      display: flex;
      flex-direction: column;
      gap: 16px;
    }

    .strategies-card,
    .records-card {
      border-radius: 8px;
      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06);
      border: 1px solid #e8e8e8;
    }

    .strategy-name {
      display: flex;
      flex-direction: column;
      gap: 4px;
    }

    .strategy-description {
      color: rgba(0, 0, 0, 0.6);
      font-size: 12px;
      line-height: 1.3;
    }

    .target-namespaces {
      display: flex;
      align-items: center;
      gap: 4px;
      flex-wrap: wrap;
    }

    .namespace-tag {
      margin: 0;
    }

    .more-count {
      color: rgba(0, 0, 0, 0.6);
      font-size: 12px;
    }

    .not-applied {
      color: rgba(0, 0, 0, 0.45);
      font-style: italic;
    }

    .action-buttons {
      display: flex;
      gap: 4px;
    }

    .empty-state,
    .empty-records {
      padding: 40px 0;
    }

    .apply-targets {
      font-size: 13px;
    }

    .apply-message {
      font-size: 12px;
      color: rgba(0, 0, 0, 0.65);
    }

    /* 模态框样式 */
    .strategy-form {
      padding: 16px 0;
    }

    .config-editor {
      display: flex;
      flex-direction: column;
      gap: 8px;
    }

    .editor-actions {
      display: flex;
      gap: 8px;
      align-items: center;
    }

    .precheck-result {
      margin-top: 8px;
    }

    .form-actions {
      display: flex;
      gap: 8px;
      justify-content: flex-end;
    }

    .strategy-details {
      padding: 16px 0;
    }

    .config-section {
      margin-top: 24px;
    }

    .config-section h4 {
      margin: 0 0 16px 0;
      color: rgba(0, 0, 0, 0.85);
      font-size: 16px;
      font-weight: 500;
    }

    .config-viewer {
      background: #f6f8fa;
      border: 1px solid #e1e4e8;
      border-radius: 6px;
      padding: 12px;
      max-height: 300px;
      overflow-y: auto;
    }

    .config-viewer pre {
      margin: 0;
      font-family: 'SFMono-Regular', 'Monaco', 'Menlo', 'Courier New', monospace;
      font-size: 13px;
      line-height: 1.4;
      color: #24292e;
      white-space: pre-wrap;
      word-wrap: break-word;
    }

    /* 响应式设计 */
    @media (max-width: 1200px) {
      .page-content {
        max-width: 100%;
      }
    }

    @media (max-width: 768px) {
      .log-strategy-management {
        padding: 8px;
      }

      .page-header {
        flex-direction: column;
        gap: 12px;
        align-items: flex-start;
      }

      .header-actions {
        flex-wrap: wrap;
      }

      .action-buttons {
        flex-direction: column;
        gap: 2px;
      }
    }
  `]
})
export class LogStrategyManagementComponent implements OnInit {
  loading = false;
  loadingRecords = false;
  saving = false;
  applying = false;
  prechecking = false;

  strategies: LogStrategy[] = [];
  applyRecords: ApplyRecord[] = [];
  namespaces: string[] = [];

  editModalVisible = false;
  viewModalVisible = false;
  editingStrategy: LogStrategy | null = null;
  viewingStrategy: LogStrategy | null = null;

  editForm: FormGroup;
  precheckResult: any = null;

  constructor(
    private fb: FormBuilder,
    private api: ApiService,
    private message: NzMessageService,
    private modal: NzModalService,
    private cdr: ChangeDetectorRef
  ) {
    this.editForm = this.fb.group({
      name: ['', Validators.required],
      description: [''],
      targetNamespaces: [[]],
      configFormat: ['json'],
      config: ['', Validators.required]
    });
  }

  ngOnInit(): void {
    this.loadData();
    this.loadNamespaces();
  }

  private loadData(): void {
    this.loadStrategies();
    this.loadApplyRecords();
  }

  private loadStrategies(): void {
    this.loading = true;
    
    this.api.getLogStrategies().subscribe({
      next: (strategies) => {
        this.strategies = strategies || [];
        this.loading = false;
        this.cdr.markForCheck();
      },
      error: (error: any) => {
        console.error('加载日志策略失败:', error);
        this.message.error('加载日志策略失败');
        this.loading = false;
        this.cdr.markForCheck();
      }
    });
  }

  private loadApplyRecords(): void {
    this.loadingRecords = true;
    
    this.api.getLogStrategyApplyRecords().subscribe({
      next: (records: any) => {
        this.applyRecords = records || [];
        this.loadingRecords = false;
        this.cdr.markForCheck();
      },
      error: (error: any) => {
        console.error('加载应用记录失败:', error);
        this.loadingRecords = false;
        this.cdr.markForCheck();
      }
    });
  }

  private loadNamespaces(): void {
    this.api.getNamespaces().subscribe({
      next: (namespaces: any) => {
        this.namespaces = namespaces || [];
        this.cdr.markForCheck();
      },
      error: (error: any) => {
        console.error('加载命名空间失败:', error);
      }
    });
  }

  refreshStrategies(): void {
    this.loadData();
  }

  createStrategy(): void {
    this.editingStrategy = null;
    this.resetEditForm();
    this.editModalVisible = true;
  }

  editStrategy(strategy: LogStrategy): void {
    this.editingStrategy = strategy;
    this.editForm.patchValue({
      name: strategy.name,
      description: strategy.description || '',
      targetNamespaces: strategy.targetNamespaces || [],
      configFormat: this.detectConfigFormat(strategy.config),
      config: this.formatConfigForEdit(strategy.config)
    });
    this.precheckResult = null;
    this.editModalVisible = true;
  }

  viewStrategy(strategy: LogStrategy): void {
    this.viewingStrategy = strategy;
    this.viewModalVisible = true;
  }

  deleteStrategy(strategy: LogStrategy): void {
    this.modal.confirm({
      nzTitle: '确认删除策略？',
      nzContent: `删除策略"${strategy.name}"后不可恢复。`,
      nzOkText: '确认删除',
        nzOkDanger: true,
      nzCancelText: '取消',
      nzOnOk: () => this.doDeleteStrategy(strategy.id)
    });
  }

  private doDeleteStrategy(strategyId: string): void {
    this.api.deleteLogStrategy(strategyId).subscribe({
      next: () => {
        this.message.success('策略删除成功');
        this.loadStrategies();
      },
      error: (error: any) => {
        console.error('删除策略失败:', error);
        this.message.error('删除策略失败');
      }
    });
  }

  saveStrategy(): void {
    if (!this.editForm.valid) {
      this.message.warning('请完善表单信息');
      return;
    }

    this.saving = true;
    const formData = this.editForm.value;
    
    const strategyData = {
      name: formData.name,
      description: formData.description,
      targetNamespaces: formData.targetNamespaces,
      config: this.parseConfig(formData.config, formData.configFormat)
    };

    const request = this.editingStrategy 
      ? this.api.updateLogStrategy(this.editingStrategy.id, strategyData)
      : this.api.createLogStrategy(strategyData);

    request.subscribe({
      next: () => {
        this.message.success(this.editingStrategy ? '策略更新成功' : '策略创建成功');
        this.closeEditModal();
        this.loadStrategies();
        this.saving = false;
        this.cdr.markForCheck();
      },
      error: (error: any) => {
        console.error('保存策略失败:', error);
        this.message.error('保存策略失败: ' + (error.error?.message || error.message));
        this.saving = false;
        this.cdr.markForCheck();
      }
    });
  }

  applyStrategy(): void {
    if (!this.editingStrategy || !this.editForm.valid) {
      return;
    }

    this.modal.confirm({
      nzTitle: '确认应用策略？',
      nzContent: `将应用策略"${this.editForm.value.name}"到目标命名空间。`,
      nzOkText: '确认应用',
      nzOkType: 'primary',
      nzCancelText: '取消',
      nzOnOk: () => this.doApplyStrategy()
    });
  }

  private doApplyStrategy(): void {
    if (!this.editingStrategy) return;

    this.applying = true;
    
    this.api.applyLogStrategy(this.editingStrategy.id).subscribe({
      next: () => {
        this.message.success('策略应用成功');
        this.closeEditModal();
        this.loadData();
        this.applying = false;
        this.cdr.markForCheck();
      },
      error: (error: any) => {
        console.error('应用策略失败:', error);
        this.message.error('应用策略失败: ' + (error.error?.message || error.message));
        this.applying = false;
        this.cdr.markForCheck();
      }
    });
  }

  precheck(): void {
    const config = this.editForm.get('config')?.value;
    if (!config?.trim()) {
      this.message.warning('请输入策略配置');
      return;
    }

    this.prechecking = true;
    this.precheckResult = null;

    const formData = this.editForm.value;
    const strategyData = {
      name: formData.name,
      config: this.parseConfig(formData.config, formData.configFormat),
      targetNamespaces: formData.targetNamespaces
    };

    this.api.precheckLogStrategy(strategyData).subscribe({
      next: (result: any) => {
        this.precheckResult = result;
        this.prechecking = false;
        this.cdr.markForCheck();
      },
      error: (error: any) => {
        console.error('预检失败:', error);
        this.precheckResult = {
          success: false,
          message: error.error?.message || error.message || '预检失败'
        };
        this.prechecking = false;
        this.cdr.markForCheck();
      }
    });
  }

  loadTemplate(): void {
    const format = this.editForm.get('configFormat')?.value || 'json';
    const template = this.getConfigTemplate(format);
    this.editForm.patchValue({ config: template });
    this.precheckResult = null;
  }

  formatConfig(): void {
    const config = this.editForm.get('config')?.value;
    const format = this.editForm.get('configFormat')?.value || 'json';
    
    if (!config?.trim()) {
      this.message.warning('请输入配置内容');
      return;
    }

    try {
      let formatted = '';
      if (format === 'json') {
        const parsed = JSON.parse(config);
        formatted = JSON.stringify(parsed, null, 2);
      } else {
        // YAML 格式化（简单实现）
        formatted = config.trim();
      }
      this.editForm.patchValue({ config: formatted });
    } catch (error) {
      this.message.error('配置格式错误，无法格式化');
    }
  }

  onConfigFormatChange(format: string): void {
    const currentConfig = this.editForm.get('config')?.value;
    if (!currentConfig?.trim()) {
      this.loadTemplate();
    }
  }

  closeEditModal(): void {
    this.editModalVisible = false;
    this.editingStrategy = null;
    this.precheckResult = null;
    this.resetEditForm();
  }

  closeViewModal(): void {
    this.viewModalVisible = false;
    this.viewingStrategy = null;
  }

  private resetEditForm(): void {
    this.editForm.reset({
      name: '',
      description: '',
      targetNamespaces: [],
      configFormat: 'json',
      config: ''
    });
  }

  getStatusColor(status: string): string {
    switch (status) {
      case 'applied': return 'green';
      case 'error': return 'red';
      default: return 'blue';
    }
  }

  getStatusText(status: string): string {
    switch (status) {
      case 'applied': return '已应用';
      case 'error': return '错误';
      default: return '草稿';
    }
  }

  getEditorOptions(): any {
    const format = this.editForm.get('configFormat')?.value || 'json';
    return {
      theme: 'vs',
      language: format,
      readOnly: false,
      minimap: { enabled: false },
      scrollBeyondLastLine: false,
      fontSize: 13,
      lineNumbers: 'on',
      folding: true,
      automaticLayout: true,
      wordWrap: 'on',
      wrappingIndent: 'indent'
    };
  }

  private detectConfigFormat(config: any): string {
    if (typeof config === 'string') {
      try {
        JSON.parse(config);
        return 'json';
      } catch {
        return 'yaml';
      }
    }
    return 'json';
  }

  private formatConfigForEdit(config: any): string {
    if (typeof config === 'string') {
      return config;
    }
    return JSON.stringify(config, null, 2);
  }

  private parseConfig(configStr: string, format: string): any {
    try {
      if (format === 'json') {
        return JSON.parse(configStr);
      } else {
        // 对于 YAML，暂时返回字符串，实际应该使用 YAML 解析器
        return configStr;
      }
    } catch (error) {
      throw new Error('配置格式错误: ' + (error as Error).message);
    }
  }

  getViewConfigContent(): string {
    if (!this.viewingStrategy) return '';
    return this.formatConfigForEdit(this.viewingStrategy.config);
  }

  private getConfigTemplate(format: string): string {
    if (format === 'json') {
      return JSON.stringify({
        "version": "v1",
        "spec": {
          "collectors": [
            {
              "name": "polardbx-logs",
              "type": "filebeat",
              "config": {
                "paths": ["/var/log/polardbx/*.log"],
                "multiline": {
                  "pattern": "^\\d{4}-\\d{2}-\\d{2}",
                  "negate": true,
                  "match": "after"
                }
              }
            }
          ],
          "outputs": [
            {
              "name": "elasticsearch",
              "config": {
                "hosts": ["http://elasticsearch:9200"],
                "index": "polardbx-logs-%{+yyyy.MM.dd}"
              }
            }
          ]
        }
      }, null, 2);
    } else {
      return `version: v1
spec:
  collectors:
    - name: polardbx-logs
      type: filebeat
      config:
        paths:
          - /var/log/polardbx/*.log
        multiline:
          pattern: '^\\d{4}-\\d{2}-\\d{2}'
          negate: true
          match: after
  outputs:
    - name: elasticsearch
      config:
        hosts:
          - http://elasticsearch:9200
        index: polardbx-logs-%{+yyyy.MM.dd}`;
    }
  }
}
