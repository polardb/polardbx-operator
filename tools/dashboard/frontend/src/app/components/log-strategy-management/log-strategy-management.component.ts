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

import { ApiService } from '../../services/api.service';

interface LogStrategy {
  name: string;
  // Optional: backend Strategy supports clusterNamespace, frontend legacy uses default.
  clusterNamespace?: string;
  // Backend expects clusterName; our legacy UI model uses targetCluster for the name part.
  targetCluster: string;
  outputType: 'elasticsearch' | 'stdout';
  status: 'active' | 'error' | 'disabled';
  config: {
    elasticsearch?: {
      hosts: string[];
      username?: string;
      password?: string;
      // optional extensions
      caCrt?: string;
      useTLS?: boolean;
    };
  };
  createdAt?: string;
  updatedAt?: string;
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
    NzEmptyModule
  ],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="page-wrapper log-strategy-management">
      <div class="page-header">
        <div class="title-block">
          <h2>
            <i nz-icon nzType="file-text" class="page-icon"></i>
            日志策略管理
          </h2>
          <p>
            管理 PolarDB-X 集群日志收集策略
            <button nz-button nzType="default" nzSize="small" (click)="refreshStrategies()" [nzLoading]="loading">
              <i nz-icon nzType="reload"></i>
              刷新
            </button>
            <button nz-button nzType="primary" nzSize="small" (click)="createStrategy()">
              <i nz-icon nzType="plus"></i>
              新建策略
            </button>
          </p>
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
                <th>目标集群</th>
                <th>输出</th>
                <th nzWidth="200px">操作</th>
              </tr>
            </thead>
            <tbody>
              <tr *ngFor="let strategy of strategiesTable.data">
                <td>
                  <div class="strategy-name">
                    <strong>{{ strategy.name }}</strong>
                  </div>
                </td>
                <td>
                  <nz-tag nzColor="blue">
                    {{ (strategy.clusterNamespace || 'default') + '/' + (strategy.targetCluster || '-') }}
                  </nz-tag>
                </td>
                <td>
                  <nz-tag [nzColor]="strategy.outputType === 'elasticsearch' ? 'geekblue' : 'default'">
                    {{ strategy.outputType === 'elasticsearch' ? 'Elasticsearch' : 'Stdout' }}
                  </nz-tag>
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
            <nz-form-label [nzSpan]="6" nzRequired>集群命名空间</nz-form-label>
            <nz-form-control [nzSpan]="18">
              <nz-select formControlName="clusterNamespace" nzShowSearch nzPlaceHolder="选择命名空间">
                <nz-option *ngFor="let ns of namespaces" [nzValue]="ns" [nzLabel]="ns"></nz-option>
              </nz-select>
            </nz-form-control>
          </nz-form-item>

          <nz-form-item>
            <nz-form-label [nzSpan]="6" nzRequired>目标集群</nz-form-label>
            <nz-form-control [nzSpan]="18">
              <ng-container *ngIf="clusterOptions.length > 0; else manualCluster">
                <nz-select formControlName="targetCluster" nzShowSearch nzPlaceHolder="选择集群名称">
                  <nz-option *ngFor="let c of clusterOptions" [nzValue]="c" [nzLabel]="c"></nz-option>
                </nz-select>
              </ng-container>
              <ng-template #manualCluster>
                <input nz-input formControlName="targetCluster" placeholder="输入集群名称（例如 demo）" />
              </ng-template>
            </nz-form-control>
          </nz-form-item>

          <nz-form-item>
            <nz-form-label [nzSpan]="6" nzRequired>输出类型</nz-form-label>
            <nz-form-control [nzSpan]="18">
              <nz-select formControlName="outputType">
                <nz-option nzValue="stdout" nzLabel="Stdout（控制台）"></nz-option>
                <nz-option nzValue="elasticsearch" nzLabel="Elasticsearch"></nz-option>
              </nz-select>
            </nz-form-control>
          </nz-form-item>

          <ng-container *ngIf="editForm.get('outputType')?.value === 'elasticsearch'">
            <nz-form-item>
              <nz-form-label [nzSpan]="6" nzRequired>ES Hosts</nz-form-label>
              <nz-form-control [nzSpan]="18" nzExtra="支持逗号/换行分隔；如使用 https:// 将自动启用 TLS">
                <textarea nz-input formControlName="esHosts" rows="3" placeholder="http://elasticsearch:9200, https://es2:9200"></textarea>
              </nz-form-control>
            </nz-form-item>

            <nz-form-item>
              <nz-form-label [nzSpan]="6">用户名</nz-form-label>
              <nz-form-control [nzSpan]="18">
                <input nz-input formControlName="esUsername" placeholder="可选（Basic Auth）" />
              </nz-form-control>
            </nz-form-item>

            <nz-form-item>
              <nz-form-label [nzSpan]="6">密码</nz-form-label>
              <nz-form-control [nzSpan]="18" nzExtra="编辑已有策略时后端不会回显密码；如需更新请重新填写">
                <input nz-input type="password" formControlName="esPassword" placeholder="可选（Basic Auth）" />
              </nz-form-control>
            </nz-form-item>

            <nz-form-item>
              <nz-form-label [nzSpan]="6">CA 证书</nz-form-label>
              <nz-form-control [nzSpan]="18" nzExtra="可选（TLS 自签名场景）">
                <textarea nz-input formControlName="caCrt" rows="4" placeholder="-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----"></textarea>
              </nz-form-control>
            </nz-form-item>
          </ng-container>

          <div style="margin: 8px 0 0 0;">
            <button
              nz-button
              nzType="default"
              nzSize="small"
              (click)="precheck()"
              [nzLoading]="prechecking"
              [disabled]="!editForm.valid">
              <i nz-icon nzType="check-circle"></i>
              预检
            </button>
          </div>

          <div class="precheck-result" *ngIf="precheckResult">
            <nz-alert
              [nzType]="precheckResult.success ? 'success' : 'error'"
              [nzMessage]="precheckResult.success ? '预检通过' : '预检失败'"
              [nzDescription]="precheckResult.message"
              nzShowIcon>
            </nz-alert>
          </div>

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
            <nz-descriptions-item nzTitle="目标集群" nzSpan="2">
              {{ (viewingStrategy.clusterNamespace || 'default') + '/' + (viewingStrategy.targetCluster || '-') }}
            </nz-descriptions-item>
            <nz-descriptions-item nzTitle="输出">
              {{ viewingStrategy.outputType === 'elasticsearch' ? 'Elasticsearch' : 'Stdout' }}
            </nz-descriptions-item>
          </nz-descriptions>

          <div class="config-section" *ngIf="viewingStrategy.outputType === 'elasticsearch'">
            <h4>Elasticsearch 配置</h4>
            <div class="config-viewer">
              <pre>{{ formatEsConfig(viewingStrategy) }}</pre>
            </div>
          </div>
        </div>
      </ng-container>
    </nz-modal>
  `,
  styles: [`
    .page-wrapper {
      display: flex;
      flex-direction: column;
      gap: 16px;
      padding: 24px;
      min-height: 100%;
      background: transparent; /* 外层由 logs-hub 负责背景 */
    }

    .page-header {
      background: #fff;
      padding: 16px;
      border-radius: 10px;
      box-shadow: 0 2px 10px rgba(15, 23, 42, 0.04);
    }

    .title-block {
      h2 {
        margin: 0 0 8px;
        font-size: 22px;
        font-weight: 600;
        color: #1f1f1f;
        display: flex;
        align-items: center;
        gap: 10px;
      }

      p {
        margin: 0;
        color: #595959;
        display: flex;
        align-items: center;
        gap: 12px;
        flex-wrap: wrap;
        line-height: 1.6;
      }
    }

    .page-icon {
      font-size: 22px;
      color: #1890ff;
    }

    .page-content {
      display: flex;
      flex-direction: column;
      gap: 16px;
      width: 100%;
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
        width: 100%;
      }
    }

    @media (max-width: 768px) {
      .page-wrapper {
        padding: 16px;
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
  clusterOptions: string[] = [];

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
      clusterNamespace: ['default', Validators.required],
      targetCluster: ['', Validators.required],
      outputType: ['stdout', Validators.required],
      esHosts: [''],
      esUsername: [''],
      esPassword: [''],
      caCrt: ['']
    });
    // Listen to clusterNamespace to reload clusters (best-effort)
    this.editForm.get('clusterNamespace')?.valueChanges.subscribe((ns) => {
      this.loadClusters(ns);
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
        // Error is already handled by ApiService.handleRequest
        // Just update UI state
        this.strategies = []; // Clear strategies on error
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
        // Error is handled in ApiService, returns empty array
        // Just update UI state
        this.applyRecords = [];
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
        // Auto-load clusters for current or first namespace
        const ns = this.editForm.get('clusterNamespace')?.value || (this.namespaces.length > 0 ? this.namespaces[0] : 'default');
        this.loadClusters(ns);
      },
      error: (error: any) => {
        // Error is handled in ApiService, returns empty array
        // Just update UI state
        this.namespaces = [];
        this.cdr.markForCheck();
      }
    });
  }

  private loadClusters(namespace: string): void {
    this.api.getClusters(namespace || 'default', { silent: true }).subscribe({
      next: (clusters) => {
        this.clusterOptions = (clusters || []).map((c) => c.metadata?.name).filter(Boolean) as string[];
        this.cdr.markForCheck();
      },
      error: () => {
        this.clusterOptions = [];
        this.cdr.markForCheck();
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
    const esCfg: any = strategy.config?.elasticsearch || {};
    this.editForm.patchValue({
      name: strategy.name,
      clusterNamespace: strategy.clusterNamespace || 'default',
      targetCluster: strategy.targetCluster || '',
      outputType: strategy.outputType || 'stdout',
      esHosts: Array.isArray(esCfg.hosts) ? esCfg.hosts.join(',') : (esCfg.hosts || ''),
      esUsername: esCfg.username || '',
      esPassword: '', // password never echoed
      caCrt: esCfg.caCrt || ''
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
      nzOnOk: () => this.doDeleteStrategy(strategy.name)
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
    
    // Build strategy data for backend (aligned with ApiService.mapToBackendLogStrategy)
    const strategyData: any = {
      name: formData.name,
      clusterNamespace: formData.clusterNamespace || 'default',
      targetCluster: formData.targetCluster,
      outputType: formData.outputType
    };
    if (formData.outputType === 'elasticsearch') {
      strategyData.config = {
        elasticsearch: {
          hosts: (formData.esHosts || '').split(/[,\n]/).map((h: string) => h.trim()).filter(Boolean),
          username: formData.esUsername || '',
          password: formData.esPassword || '',
          caCrt: formData.caCrt || ''
        }
      };
    }

    const request = this.editingStrategy 
      ? this.api.updateLogStrategy(this.editingStrategy.name, strategyData)
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
      nzContent: `将应用策略"${this.editForm.value.name}"到目标集群。`,
      nzOkText: '确认应用',
      nzOkType: 'primary',
      nzCancelText: '取消',
      nzOnOk: () => this.doApplyStrategy()
    });
  }

  private doApplyStrategy(): void {
    if (!this.editingStrategy) return;

    this.applying = true;
    
    this.api.applyLogStrategy(this.editingStrategy.name).subscribe({
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
    const fv = this.editForm.value;
    if (!fv.name || !fv.targetCluster) {
      this.message.warning('请填写策略名称和目标集群');
      return;
    }

    this.prechecking = true;
    this.precheckResult = null;

    const strategyData: any = {
      name: fv.name,
      clusterName: fv.targetCluster,
      clusterNamespace: fv.clusterNamespace || 'default',
      output: {
        type: fv.outputType
      }
    };
    if (fv.outputType === 'elasticsearch') {
      strategyData.output.hosts = (fv.esHosts || '').split(/[,\n]/).map((h: string) => h.trim()).filter(Boolean).join(',');
      strategyData.output.username = fv.esUsername || '';
      strategyData.output.password = fv.esPassword || '';
      strategyData.output.useTLS = strategyData.output.hosts.includes('https://');
      strategyData.output.caCrt = fv.caCrt || '';
      strategyData.output.authType = fv.esUsername ? 'basic' : 'none';
    }

    this.api.precheckLogStrategy(strategyData).subscribe({
      next: (result: any) => {
        this.precheckResult = { success: result.valid, message: (result.errors || []).join('; ') || (result.warnings || []).join('; ') || 'OK' };
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
      clusterNamespace: 'default',
      targetCluster: '',
      outputType: 'stdout',
      esHosts: '',
      esUsername: '',
      esPassword: '',
      caCrt: ''
    });
  }

  getStatusColor(status: string): string {
    switch (status) {
      case 'active': return 'green';
      case 'error': return 'red';
      default: return 'blue';
    }
  }

  getStatusText(status: string): string {
    switch (status) {
      case 'active': return '活跃';
      case 'error': return '错误';
      default: return '禁用';
    }
  }

  formatEsConfig(strategy: LogStrategy): string {
    const es = strategy.config?.elasticsearch || {};
    return JSON.stringify(es, null, 2);
  }
}
