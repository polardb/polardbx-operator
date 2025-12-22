import { Component, OnInit, OnDestroy, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReactiveFormsModule, FormBuilder, FormGroup, Validators } from '@angular/forms';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzModalModule } from 'ng-zorro-antd/modal';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzSwitchModule } from 'ng-zorro-antd/switch';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzPopconfirmModule } from 'ng-zorro-antd/popconfirm';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzStatisticModule } from 'ng-zorro-antd/statistic';
import { NzDescriptionsModule } from 'ng-zorro-antd/descriptions';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { Subject } from 'rxjs';
import { takeUntil, finalize } from 'rxjs/operators';

import { ApiService } from '../../services/api.service';
import { LoadingService, LoadingKeys } from '../../services/loading.service';

// Log strategy API types
export interface LogStrategy {
  name: string;
  targetCluster: string;
  outputType: 'elasticsearch' | 'stdout';
  status: 'active' | 'error' | 'disabled';
  config: {
    elasticsearch?: {
      hosts: string[];
      username?: string;
      password?: string;
      index?: string;
    };
  };
  createdAt?: string;
  updatedAt?: string;
}

// Service status API types
export interface LogServiceStatus {
  status: 'running' | 'not_installed' | 'degraded';
  components: {
    filebeat: {
      status: 'running' | 'error' | 'not_found';
      replicas?: { ready: number; total: number };
    };
    logstash: {
      status: 'running' | 'error' | 'not_found';
      replicas?: { ready: number; total: number };
    };
  };
  installCommand?: string;
}

@Component({
  selector: 'app-log-service-dashboard',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    NzCardModule,
    NzButtonModule,
    NzIconModule,
    NzTableModule,
    NzTagModule,
    NzAlertModule,
    NzModalModule,
    NzFormModule,
    NzInputModule,
    NzSelectModule,
    NzSwitchModule,
    NzSpinModule,
    NzPopconfirmModule,
    NzDividerModule,
    NzStatisticModule,
    NzDescriptionsModule,
    NzEmptyModule,
    NzToolTipModule
  ],
  template: `
    <div class="page-wrapper log-service-dashboard">
      <div class="page-header">
        <div class="title-block">
          <h2>
            <i nz-icon nzType="dashboard" class="page-icon"></i>
            日志服务仪表盘
          </h2>
          <p>
            查看 LogCollector 服务状态与日志输出策略
            <button
              nz-button
              nzType="default"
              nzSize="small"
              (click)="reloadAll()"
              [nzLoading]="isLoading('LOG_SERVICE_STATUS') || isLoading('LOG_STRATEGIES_LIST')">
              <i nz-icon nzType="reload"></i>
              刷新
            </button>
          </p>
        </div>
      </div>

      <div class="page-content">
        <!-- 服务状态面板 -->
        <div class="status-section">
        <nz-card nzTitle="LogCollector 服务状态" class="status-card">
          <nz-spin [nzSpinning]="isLoading('LOG_SERVICE_STATUS')">
            <div class="service-status">
              <div class="status-overview">
                <div class="status-indicator" [class]="getStatusClass(serviceStatus?.status)">
                  <i nz-icon [nzType]="getStatusIcon(serviceStatus?.status)"></i>
                  <span class="status-text">{{ getStatusText(serviceStatus?.status) }}</span>
                </div>
                
                <div class="components-status" *ngIf="serviceStatus?.status === 'running' || serviceStatus?.status === 'degraded'">
                  <div class="component-item">
                    <span class="component-name">FileBeat</span>
                    <nz-tag [nzColor]="getComponentStatusColor(serviceStatus?.components?.filebeat?.status)">
                      {{ getComponentStatusText(serviceStatus?.components?.filebeat?.status) }}
                    </nz-tag>
                    <span *ngIf="serviceStatus?.components?.filebeat?.replicas" class="replicas">
                      ({{ serviceStatus?.components?.filebeat?.replicas?.ready }}/{{ serviceStatus?.components?.filebeat?.replicas?.total }})
                    </span>
                  </div>
                  
                  <div class="component-item">
                    <span class="component-name">LogStash</span>
                    <nz-tag [nzColor]="getComponentStatusColor(serviceStatus?.components?.logstash?.status)">
                      {{ getComponentStatusText(serviceStatus?.components?.logstash?.status) }}
                    </nz-tag>
                    <span *ngIf="serviceStatus?.components?.logstash?.replicas" class="replicas">
                      ({{ serviceStatus?.components?.logstash?.replicas?.ready }}/{{ serviceStatus?.components?.logstash?.replicas?.total }})
                    </span>
                  </div>
                </div>
              </div>

              <!-- 安装引导 -->
              <div *ngIf="serviceStatus?.status === 'not_installed'" class="install-guide">
                <nz-alert 
                  nzType="info" 
                  nzShowIcon 
                  nzMessage="日志收集组件尚未安装"
                  class="install-alert">
                  <ng-template #nzDescription>
                    <p>请执行以下命令进行安装：</p>
                    <div class="install-command">
                      <code>{{ serviceStatus?.installCommand }}</code>
                      <button nz-button nzType="link" nzSize="small" (click)="copyInstallCommand()">
                        <i nz-icon nzType="copy"></i>
                        复制
                      </button>
                    </div>
                  </ng-template>
                </nz-alert>
              </div>
            </div>
          </nz-spin>
        </nz-card>
      </div>

      <!-- 日志策略列表 -->
      <div class="strategies-section">
        <nz-card class="strategies-card">
          <div class="card-header">
            <h3>日志输出策略</h3>
            <div class="header-actions">
              <button nz-button nzType="primary" (click)="showCreateModal()">
                <i nz-icon nzType="plus"></i>
                新建策略
              </button>
              <button nz-button nzType="default" (click)="loadStrategies()">
                <i nz-icon nzType="reload"></i>
                刷新
              </button>
            </div>
          </div>

          <nz-spin [nzSpinning]="isLoading('LOG_STRATEGIES_LIST')">
            <nz-table 
              [nzData]="strategies" 
              [nzShowPagination]="false"
              nzSize="middle"
              class="strategies-table">
              <thead>
                <tr>
                  <th>策略名称</th>
                  <th>目标集群</th>
                  <th>输出类型</th>
                  <th>状态</th>
                  <th>创建时间</th>
                  <th width="200px">操作</th>
                </tr>
              </thead>
              <tbody>
                <tr *ngFor="let strategy of strategies">
                  <td>
                    <strong>{{ strategy.name }}</strong>
                  </td>
                  <td>{{ strategy.targetCluster }}</td>
                  <td>
                    <nz-tag [nzColor]="strategy.outputType === 'elasticsearch' ? 'blue' : 'default'">
                      {{ strategy.outputType === 'elasticsearch' ? 'Elasticsearch' : 'Stdout' }}
                    </nz-tag>
                  </td>
                  <td>
                    <nz-tag [nzColor]="getStrategyStatusColor(strategy.status)">
                      {{ getStrategyStatusText(strategy.status) }}
                    </nz-tag>
                  </td>
                  <td>{{ formatDate(strategy.createdAt) }}</td>
                  <td>
                    <button nz-button nzType="link" nzSize="small" (click)="editStrategy(strategy)">
                      <i nz-icon nzType="edit"></i>
                      编辑
                    </button>
                    <nz-divider nzType="vertical"></nz-divider>
                    <button 
                      nz-button 
                      nzType="link" 
                      nzSize="small" 
                      nz-popconfirm
                      nzPopconfirmTitle="确定删除此策略？"
                      (nzOnConfirm)="deleteStrategy(strategy.name)">
                      <i nz-icon nzType="delete"></i>
                      删除
                    </button>
                  </td>
                </tr>
              </tbody>
            </nz-table>

            <div *ngIf="strategies.length === 0" class="empty-state">
              <nz-empty 
                nzNotFoundContent="暂无日志策略"
                nzNotFoundImage="simple">
                <ng-template #nzNotFoundFooter>
                  <button nz-button nzType="primary" (click)="showCreateModal()">
                    创建第一个策略
                  </button>
                </ng-template>
              </nz-empty>
            </div>
          </nz-spin>
        </nz-card>
      </div>
      </div>

      <!-- 创建/编辑策略模态框 -->
      <nz-modal
        [(nzVisible)]="isModalVisible"
        [nzTitle]="editingStrategy ? '编辑日志策略' : '创建日志策略'"
        [nzOkText]="editingStrategy ? '更新' : '创建'"
        nzCancelText="取消"
        [nzOkLoading]="isLoading('LOG_STRATEGY_SAVE')"
        (nzOnOk)="saveStrategy()"
        (nzOnCancel)="closeModal()"
        nzWidth="600px">
        
        <ng-container *nzModalContent>
          <form [formGroup]="strategyForm" nz-form nzLayout="vertical">
          <nz-form-item>
            <nz-form-label nzRequired nzTooltipTitle="策略名称只能包含小写字母、数字和连字符">策略名称</nz-form-label>
            <nz-form-control 
              [nzErrorTip]="strategyForm.get('name')?.hasError('required') ? '请输入策略名称' : '策略名称格式不正确，只能包含小写字母、数字和连字符'">
              <input nz-input formControlName="name" placeholder="例如: my-cluster-logs" />
            </nz-form-control>
          </nz-form-item>

          <nz-form-item>
            <nz-form-label nzRequired>目标 PolarDB-X 集群</nz-form-label>
            <nz-form-control nzErrorTip="请选择目标集群">
              <nz-select formControlName="targetCluster" nzPlaceHolder="选择集群" nzShowSearch>
                <nz-option *ngFor="let cluster of availableClusters" [nzValue]="cluster" [nzLabel]="cluster"></nz-option>
              </nz-select>
            </nz-form-control>
          </nz-form-item>

          <nz-form-item>
            <nz-form-label nzRequired>输出类型</nz-form-label>
            <nz-form-control>
              <nz-select formControlName="outputType" nzPlaceHolder="选择输出类型">
                <nz-option nzValue="stdout" nzLabel="标准输出 (Stdout)">
                  <span>标准输出 (Stdout)</span>
                  <small style="color: #8c8c8c; display: block;">日志输出到 LogStash 控制台，适合测试和调试</small>
                </nz-option>
                <nz-option nzValue="elasticsearch" nzLabel="Elasticsearch">
                  <span>Elasticsearch</span>
                  <small style="color: #8c8c8c; display: block;">日志存储到 Elasticsearch 集群，支持搜索和分析</small>
                </nz-option>
              </nz-select>
            </nz-form-control>
          </nz-form-item>

          <!-- Elasticsearch 配置 -->
          <div *ngIf="strategyForm.get('outputType')?.value === 'elasticsearch'">
            <nz-divider nzText="Elasticsearch 配置" nzOrientation="left"></nz-divider>
            
            <nz-form-item>
              <nz-form-label nzRequired nzTooltipTitle="Elasticsearch 集群的访问地址，支持 HTTP 或 HTTPS">Elasticsearch 主机</nz-form-label>
              <nz-form-control 
                [nzErrorTip]="strategyForm.get('esHosts')?.hasError('required') ? '请输入 Elasticsearch 主机地址' : 'URL 格式不正确，请以 http:// 或 https:// 开头'">
                <input nz-input formControlName="esHosts" placeholder="https://quickstart-es-http.default:9200" />
              </nz-form-control>
            </nz-form-item>

            <nz-form-item>
              <nz-form-label nzRequired>用户名</nz-form-label>
              <nz-form-control nzErrorTip="请输入用户名">
                <input nz-input formControlName="esUsername" placeholder="elastic" />
              </nz-form-control>
            </nz-form-item>

            <nz-form-item>
              <nz-form-label nzRequired>密码</nz-form-label>
              <nz-form-control nzErrorTip="请输入密码">
                <input nz-input nzType="password" formControlName="esPassword" placeholder="输入密码" />
              </nz-form-control>
            </nz-form-item>

            <nz-form-item>
              <nz-form-label nzTooltipTitle="日志索引的前缀名称，将自动添加日志类型后缀">索引前缀</nz-form-label>
              <nz-form-control>
                <input nz-input formControlName="esIndex" placeholder="polardbx-logs" />
                <div style="margin-top: 4px;">
                  <small style="color: #8c8c8c;">将创建如 polardbx-logs-cn-sql、polardbx-logs-dn-audit 等索引</small>
                </div>
              </nz-form-control>
            </nz-form-item>

            <div style="margin-bottom: 16px;">
              <button 
                nz-button 
                nzType="dashed" 
                [nzLoading]="isLoading('ES_CONNECTION_TEST')"
                [disabled]="!canTestConnection()"
                (click)="testElasticsearchConnection()">
                <i nz-icon nzType="link"></i>
                测试连接
              </button>
              <span style="margin-left: 8px; color: #8c8c8c; font-size: 12px;">
                验证 Elasticsearch 配置是否正确
              </span>
            </div>

            <nz-alert 
              nzType="info" 
              nzShowIcon 
              nzMessage="配置提示"
              nzDescription="请确保 Elasticsearch 集群可从 Kubernetes 集群内访问，并且已启用自动创建索引功能。"
              style="margin-bottom: 16px;">
            </nz-alert>
          </div>

          <!-- Stdout 说明 -->
          <div *ngIf="strategyForm.get('outputType')?.value === 'stdout'">
            <nz-alert 
              nzType="info" 
              nzShowIcon 
              nzMessage="标准输出模式"
              nzDescription="日志将输出到 LogStash Pod 的标准输出，您可以通过 kubectl logs 命令查看日志内容。"
              style="margin-top: 16px;">
            </nz-alert>
          </div>
          </form>
        </ng-container>
      </nz-modal>
    </div>
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

    .log-service-dashboard {
      .status-section {
        margin-bottom: 0;

        .status-card {
          .service-status {
            .status-overview {
              display: flex;
              justify-content: space-between;
              align-items: center;
              margin-bottom: 16px;

              .status-indicator {
                display: flex;
                align-items: center;
                gap: 8px;
                font-size: 16px;
                font-weight: 600;

                &.running {
                  color: #52c41a;
                }

                &.not_installed {
                  color: #faad14;
                }

                &.degraded {
                  color: #ff4d4f;
                }

                .anticon {
                  font-size: 20px;
                }
              }

              .components-status {
                display: flex;
                gap: 24px;

                .component-item {
                  display: flex;
                  align-items: center;
                  gap: 8px;

                  .component-name {
                    font-weight: 500;
                    color: #262626;
                  }

                  .replicas {
                    font-size: 12px;
                    color: #8c8c8c;
                  }
                }
              }
            }

            .install-guide {
              .install-alert {
                .install-command {
                  display: flex;
                  align-items: center;
                  gap: 8px;
                  margin-top: 8px;
                  padding: 8px 12px;
                  background: #f6f8fa;
                  border-radius: 4px;

                  code {
                    flex: 1;
                    font-size: 12px;
                    background: transparent;
                    border: none;
                    padding: 0;
                  }
                }
              }
            }
          }
        }
      }

      .strategies-section {
        .strategies-card {
          .card-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 16px;

            h3 {
              margin: 0;
              font-size: 16px;
              font-weight: 600;
              color: #262626;
            }

            .header-actions {
              display: flex;
              gap: 8px;
            }
          }

          .strategies-table {
            ::ng-deep .ant-table-thead > tr > th {
              background: #fafafa;
              font-weight: 600;
            }
          }

          .empty-state {
            padding: 40px;
            text-align: center;
          }
        }
      }
    }

    /* 响应式设计 */
    @media (max-width: 768px) {
      .log-service-dashboard {
        .status-section .status-card .service-status .status-overview {
          flex-direction: column;
          align-items: flex-start;
          gap: 16px;

          .components-status {
            flex-direction: column;
            gap: 12px;
          }
        }

        .strategies-section .strategies-card .card-header {
          flex-direction: column;
          align-items: flex-start;
          gap: 12px;

          .header-actions {
            width: 100%;
            justify-content: flex-end;
          }
        }
      }
    }
  `]
})
export class LogServiceDashboardComponent implements OnInit, OnDestroy {
  private destroy$ = new Subject<void>();
  
  // Service status
  serviceStatus: LogServiceStatus | null = null;
  
  // Log strategies
  strategies: LogStrategy[] = [];
  availableClusters: string[] = [];
  
  // Modal state
  isModalVisible = false;
  editingStrategy: LogStrategy | null = null;
  strategyForm: FormGroup;

  constructor(
    private apiService: ApiService,
    private loadingService: LoadingService,
    private fb: FormBuilder,
    private message: NzMessageService
  ) {
    this.strategyForm = this.createStrategyForm();
  }

  ngOnInit(): void {
    this.loadServiceStatus();
    this.loadStrategies();
    this.loadAvailableClusters();
  }

  reloadAll(): void {
    this.loadServiceStatus();
    this.loadStrategies();
    this.loadAvailableClusters();
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  private createStrategyForm(): FormGroup {
    const form = this.fb.group({
      name: ['', [Validators.required, Validators.pattern(/^[a-z0-9-]+$/)]],
      targetCluster: ['', [Validators.required]],
      outputType: ['stdout', [Validators.required]],
      esHosts: [''],
      esUsername: [''],
      esPassword: [''],
      esIndex: ['polardbx-logs']
    });

    // Watch output type changes and update validators dynamically.
    form.get('outputType')?.valueChanges.subscribe(outputType => {
      const esHostsControl = form.get('esHosts');
      const esUsernameControl = form.get('esUsername');
      const esPasswordControl = form.get('esPassword');

      if (outputType === 'elasticsearch') {
        esHostsControl?.setValidators([Validators.required, Validators.pattern(/^https?:\/\/.+/)]);
        esUsernameControl?.setValidators([Validators.required]);
        esPasswordControl?.setValidators([Validators.required]);
      } else {
        esHostsControl?.clearValidators();
        esUsernameControl?.clearValidators();
        esPasswordControl?.clearValidators();
      }

      esHostsControl?.updateValueAndValidity();
      esUsernameControl?.updateValueAndValidity();
      esPasswordControl?.updateValueAndValidity();
    });

    return form;
  }

  isLoading(key: keyof typeof LoadingKeys): boolean {
    return this.loadingService.isLoading(LoadingKeys[key]);
  }

  // Load service status
  loadServiceStatus(): void {
    // Call backend API to fetch service status.
    // TODO: implement the corresponding backend endpoint.
    this.apiService.getLogServiceStatus()
      .pipe(
        takeUntil(this.destroy$),
        finalize(() => {})
      )
      .subscribe({
        next: (status: LogServiceStatus) => {
          this.serviceStatus = status;
        },
        error: (error) => {
          console.error('加载服务状态失败:', error);
          // Do not inject demo data; keep empty to surface real errors.
          this.serviceStatus = undefined as any;
        }
      });
  }

  // Load log strategy list
  loadStrategies(): void {
    // Call backend API to fetch strategy list.
    // TODO: implement the corresponding backend endpoint.
    this.apiService.getLogStrategies()
      .pipe(
        takeUntil(this.destroy$),
        finalize(() => {})
      )
      .subscribe({
        next: (strategies: LogStrategy[]) => {
          this.strategies = strategies;
        },
        error: (error) => {
          console.error('加载日志策略失败:', error);
          // Do not inject demo data.
          this.strategies = [];
        }
      });
  }

  // Load available clusters
  loadAvailableClusters(): void {
    // Call backend API to fetch cluster list.
    this.apiService.getClusters()
      .pipe(
        takeUntil(this.destroy$),
        finalize(() => {})
      )
      .subscribe({
        next: (clusters: any[]) => {
          this.availableClusters = clusters.map(c => c.metadata.name);
        },
        error: (error) => {
          console.error('加载集群列表失败:', error);
          // Mock data
          this.availableClusters = ['my-polardbx-cluster', 'test-cluster'];
        }
      });
  }

  // Status helpers
  getStatusClass(status?: string): string {
    return status || 'not_installed';
  }

  getStatusIcon(status?: string): string {
    switch (status) {
      case 'running': return 'check-circle';
      case 'degraded': return 'exclamation-circle';
      case 'not_installed': return 'warning';
      default: return 'question-circle';
    }
  }

  getStatusText(status?: string): string {
    switch (status) {
      case 'running': return '运行中';
      case 'degraded': return '异常';
      case 'not_installed': return '未安装';
      default: return '未知';
    }
  }

  getComponentStatusColor(status?: string): string {
    switch (status) {
      case 'running': return 'green';
      case 'crashloop': return 'orange';
      case 'flapping': return 'blue';
      case 'error': return 'red';
      case 'not_found': return 'default';
      default: return 'default';
    }
  }

  getComponentStatusText(status?: string): string {
    switch (status) {
      case 'running': return '运行中';
      case 'crashloop': return '反复重启';
      case 'flapping': return '频繁重启';
      case 'error': return '异常';
      case 'not_found': return '未找到';
      default: return '未知';
    }
  }

  getStrategyStatusColor(status: string): string {
    switch (status) {
      case 'active': return 'green';
      case 'error': return 'red';
      case 'disabled': return 'default';
      default: return 'default';
    }
  }

  getStrategyStatusText(status: string): string {
    switch (status) {
      case 'active': return '生效中';
      case 'error': return '配置错误';
      case 'disabled': return '已禁用';
      default: return '未知';
    }
  }

  // Copy install command
  copyInstallCommand(): void {
    if (this.serviceStatus?.installCommand) {
      navigator.clipboard.writeText(this.serviceStatus.installCommand).then(() => {
        this.message.success('安装命令已复制到剪贴板');
      });
    }
  }

  // Strategy management
  showCreateModal(): void {
    this.editingStrategy = null;
    this.strategyForm.reset({
      outputType: 'stdout'
    });
    // Enable name field (create mode)
    this.strategyForm.get('name')?.enable();
    this.isModalVisible = true;
  }

  editStrategy(strategy: LogStrategy): void {
    this.editingStrategy = strategy;
    const esCfg = strategy.config?.elasticsearch || {} as any;
    this.strategyForm.patchValue({
      name: strategy.name,
      targetCluster: strategy.targetCluster,
      outputType: strategy.outputType,
      esHosts: Array.isArray(esCfg.hosts) ? (esCfg.hosts[0] || '') : (esCfg.hosts || ''),
      esUsername: esCfg.username || '',
      esIndex: esCfg.index || ''
    });
    this.strategyForm.get('name')?.disable();
    this.isModalVisible = true;
  }

  closeModal(): void {
    this.isModalVisible = false;
    this.editingStrategy = null;
  }

  saveStrategy(): void {
    if (this.strategyForm.valid) {
      const formValue = this.strategyForm.value;
      const strategy: LogStrategy = {
        name: formValue.name,
        targetCluster: formValue.targetCluster,
        outputType: formValue.outputType,
        status: 'active',
        config: {}
      };

      if (formValue.outputType === 'elasticsearch') {
        strategy.config.elasticsearch = {
          hosts: [formValue.esHosts],
          username: formValue.esUsername,
          password: formValue.esPassword,
          index: formValue.esIndex
        };
      }

      // Call backend API to persist the strategy
      const apiCall = this.editingStrategy 
        ? this.apiService.updateLogStrategy(strategy.name, strategy)
        : this.apiService.createLogStrategy(strategy);

      apiCall.pipe(
        takeUntil(this.destroy$),
        finalize(() => {})
      ).subscribe({
        next: () => {
          this.message.success(this.editingStrategy ? '策略更新成功' : '策略创建成功');
          this.closeModal();
          this.loadStrategies();
        },
        error: (error) => {
          console.error('保存策略失败:', error);
          this.message.error('保存策略失败');
        }
      });
    }
  }

  deleteStrategy(name: string): void {
    this.apiService.deleteLogStrategy(name)
      .pipe(
        takeUntil(this.destroy$),
        finalize(() => {})
      )
      .subscribe({
        next: () => {
          this.message.success('策略删除成功');
          this.loadStrategies();
        },
        error: (error) => {
          console.error('删除策略失败:', error);
          this.message.error('删除策略失败');
        }
      });
  }

  formatDate(dateString?: string): string {
    if (!dateString) return '-';
    return new Date(dateString).toLocaleString('zh-CN');
  }

  // Test Elasticsearch connectivity
  canTestConnection(): boolean {
    const form = this.strategyForm;
    return !!(
      form.get('esHosts')?.value &&
      form.get('esUsername')?.value &&
      form.get('esPassword')?.value
    );
  }

  testElasticsearchConnection(): void {
    if (!this.canTestConnection()) {
      this.message.warning('请先填写完整的 Elasticsearch 配置');
      return;
    }

    const formValue = this.strategyForm.value;
    const testConfig = {
      hosts: [formValue.esHosts],
      username: formValue.esUsername,
      password: formValue.esPassword
    };

    // Call backend API to test connection
    this.apiService.testElasticsearchConnection(testConfig)
      .pipe(
        takeUntil(this.destroy$),
        finalize(() => {})
      )
      .subscribe({
        next: (result) => {
          this.message.success('Elasticsearch 连接测试成功！');
        },
        error: (error) => {
          console.error('Elasticsearch 连接测试失败:', error);
          this.message.error(`连接测试失败: ${error.error?.message || '无法连接到 Elasticsearch'}`);
        }
      });
  }
}
