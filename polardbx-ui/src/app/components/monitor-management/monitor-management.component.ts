import { Component, OnInit, OnDestroy } from '@angular/core';
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
import { NzMessageModule, NzMessageService } from 'ng-zorro-antd/message';
import { NzModalModule, NzModalService } from 'ng-zorro-antd/modal';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzSwitchModule } from 'ng-zorro-antd/switch';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzCollapseModule } from 'ng-zorro-antd/collapse';
import { NzDropDownModule } from 'ng-zorro-antd/dropdown';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { Subject } from 'rxjs';
import { EmptyStateComponent } from '../empty-state/empty-state.component';
import { takeUntil, finalize } from 'rxjs/operators';

import { ApiService } from '../../services/api.service';
import { LoadingService, LoadingKeys } from '../../services/loading.service';
import { 
  PolarDBXMonitor, 
  CreateMonitorRequest
} from '../../models/monitor.model';
// Removed ConfirmationDialogComponent import - using native confirm() instead

@Component({
  selector: 'app-monitor-management',
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
    NzMessageModule,
    NzModalModule,
    NzToolTipModule,
    NzSwitchModule,
    NzDividerModule,
    NzCollapseModule,
    NzDropDownModule,
    NzGridModule,
    EmptyStateComponent
  ],
  template: `
    <div class="monitor-management">
      <div class="page-header">
        <div class="header-content">
          <h1 class="page-title">
            <i nz-icon nzType="dashboard" class="page-icon"></i>
            监控管理
          </h1>
          <p class="page-description">为 PolarDB-X 集群配置监控和指标采集</p>
        </div>
      </div>

      <nz-tabset class="main-tabs" [(nzSelectedIndex)]="selectedTab" (nzSelectChange)="onTabChange($event)">
        <!-- 监控列表选项卡 -->
        <nz-tab nzTitle="监控配置">
            <div class="tab-content">
              <div class="actions-toolbar">
                <button nz-button nzType="primary" (click)="refreshMonitors()" 
                        [nzLoading]="isLoading('MONITOR_LIST')">
                  <i nz-icon nzType="reload"></i>
                  刷新
                </button>
                <button nz-button nzType="default" (click)="selectedTab = 1">
                  <i nz-icon nzType="plus"></i>
                  创建监控
                </button>
              </div>

              <nz-card class="table-card" nzTitle="监控列表">
                <div class="table-container" *ngIf="!isLoading('MONITOR_LIST'); else loadingTemplate">
                  <nz-table [nzData]="monitors" class="monitors-table" nzBordered>
                    <thead>
                      <tr>
                        <th>名称</th>
                        <th>命名空间</th>
                        <th>目标集群</th>
                        <th>状态</th>
                        <th>配置</th>
                        <th>创建时间</th>
                        <th>操作</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr *ngFor="let monitor of monitors">
                        <td>
                          <div class="monitor-name">
                            <i nz-icon [nzType]="getStatusIcon(monitor.status?.monitorStatus?.phase)" 
                               [style.color]="getStatusColor(monitor.status?.monitorStatus?.phase)"></i>
                            <span>{{ monitor.metadata.name }}</span>
                          </div>
                        </td>
                        <td>{{ monitor.metadata.namespace }}</td>
                        <td>
                          <nz-tag nzColor="blue">{{ monitor.spec.clusterName }}</nz-tag>
                        </td>
                        <td>
                          <nz-tag [nzColor]="getStatusColor(monitor.status?.monitorStatus?.phase)">
                            {{ getStatusLabel(monitor.status?.monitorStatus?.phase) }}
                          </nz-tag>
                        </td>
                        <td>
                          <div class="monitor-config">
                            <div>间隔: {{ monitor.spec.monitorInterval || '30s' }}</div>
                            <div>超时: {{ monitor.spec.scrapeTimeout || '10s' }}</div>
                          </div>
                        </td>
                        <td>{{ formatDate(monitor.metadata.creationTimestamp) }}</td>
                        <td>
                          <a nz-dropdown [nzDropdownMenu]="menu">
                            <i nz-icon nzType="more" nzTheme="outline"></i>
                          </a>
                          <nz-dropdown-menu #menu="nzDropdownMenu">
                            <ul nz-menu>
                              <li nz-menu-item (click)="viewMonitorDetails(monitor)">
                                <i nz-icon nzType="eye"></i>
                                查看详情
                              </li>
                              <li nz-menu-item (click)="editMonitor(monitor)">
                                <i nz-icon nzType="edit"></i>
                                编辑监控
                              </li>
                              <li nz-menu-item (click)="deleteMonitor(monitor)" class="delete-action">
                                <i nz-icon nzType="delete"></i>
                                删除监控
                              </li>
                            </ul>
                          </nz-dropdown-menu>
                        </td>
                      </tr>
                    </tbody>
                  </nz-table>

                  <app-empty-state *ngIf="monitors.length === 0"
                                   icon="dashboard"
                                   title="未找到监控配置"
                                   hint='点击"创建监控"开始使用'></app-empty-state>
                </div>
              </nz-card>
            </div>
        </nz-tab>

        <!-- 创建/编辑监控选项卡 -->
        <nz-tab nzTitle="创建监控">
          <div class="tab-content">
            <nz-card class="form-card" [nzTitle]="createTitle" [nzExtra]="createSubtitle">
              <ng-template #createTitle>
                {{ editingMonitor ? '编辑监控配置' : '创建新监控配置' }}
              </ng-template>
              <ng-template #createSubtitle>
                <span class="subtitle">为 PolarDB-X 集群配置 Prometheus 指标采集</span>
              </ng-template>

              <form [formGroup]="monitorForm" class="monitor-form">
                <nz-collapse [nzBordered]="true" class="form-section" [nzAccordion]="false">
                  <nz-collapse-panel nzHeader="基本配置" [nzActive]="true">
                    <div class="form-row">
                      <nz-form-item class="full-width">
                        <nz-form-label [nzSpan]="5" nzRequired>监控名称</nz-form-label>
                        <nz-form-control [nzSpan]="19" nzHasFeedback>
                          <input nz-input formControlName="name" placeholder="输入监控名称" />
                          <div class="error" *ngIf="monitorForm.get('name')?.hasError('required')">监控名称是必填项</div>
                          <div class="error" *ngIf="monitorForm.get('name')?.hasError('pattern')">名称必须是合法的 Kubernetes 资源名称</div>
                        </nz-form-control>
                      </nz-form-item>
                    </div>

                    <div class="form-row">
                      <nz-form-item class="half-width">
                        <nz-form-label [nzSpan]="8">命名空间</nz-form-label>
                        <nz-form-control [nzSpan]="16">
                          <input nz-input formControlName="namespace" placeholder="default" />
                        </nz-form-control>
                      </nz-form-item>
                      <nz-form-item class="half-width">
                        <nz-form-label [nzSpan]="8" nzRequired>目标集群名称</nz-form-label>
                        <nz-form-control [nzSpan]="16" nzHasFeedback>
                          <input nz-input formControlName="clusterName" placeholder="输入集群名称" />
                          <div class="error" *ngIf="monitorForm.get('clusterName')?.hasError('required')">集群名称是必填项</div>
                        </nz-form-control>
                      </nz-form-item>
                    </div>
                  </nz-collapse-panel>

                  <nz-collapse-panel nzHeader="监控设置" [nzActive]="true">
                    <div class="form-row">
                      <nz-form-item class="half-width">
                        <nz-form-label [nzSpan]="8">监控间隔</nz-form-label>
                        <nz-form-control [nzSpan]="16">
                          <nz-select formControlName="monitorInterval" nzPlaceHolder="选择采集间隔">
                            <nz-option nzValue="15s" nzLabel="15 秒"></nz-option>
                            <nz-option nzValue="30s" nzLabel="30 秒 (默认)"></nz-option>
                            <nz-option nzValue="1m" nzLabel="1 分钟"></nz-option>
                            <nz-option nzValue="2m" nzLabel="2 分钟"></nz-option>
                            <nz-option nzValue="5m" nzLabel="5 分钟"></nz-option>
                          </nz-select>
                          <div class="hint">指标采集的频率</div>
                        </nz-form-control>
                      </nz-form-item>

                      <nz-form-item class="half-width">
                        <nz-form-label [nzSpan]="8">采集超时</nz-form-label>
                        <nz-form-control [nzSpan]="16">
                          <nz-select formControlName="scrapeTimeout" nzPlaceHolder="选择超时时间">
                            <nz-option nzValue="5s" nzLabel="5 秒"></nz-option>
                            <nz-option nzValue="10s" nzLabel="10 秒 (默认)"></nz-option>
                            <nz-option nzValue="15s" nzLabel="15 秒"></nz-option>
                            <nz-option nzValue="30s" nzLabel="30 秒"></nz-option>
                          </nz-select>
                          <div class="hint">等待指标响应的最长时间</div>
                        </nz-form-control>
                      </nz-form-item>
                    </div>

                    <div class="info-section">
                      <i nz-icon nzType="info-circle"></i>
                      <div class="info-content">
                        <h4>监控配置指南</h4>
                        <ul>
                          <li><strong>监控间隔:</strong> 较短的间隔提供更精细的数据，但会增加资源消耗</li>
                          <li><strong>采集超时:</strong> 应短于监控间隔以避免请求重叠</li>
                          <li><strong>Prometheus 集成:</strong> 此监控将通过 ServiceMonitor 被 Prometheus 自动发现</li>
                          <li><strong>可用指标:</strong> 数据库连接、查询性能、资源使用、PolarDB-X 指标</li>
                        </ul>
                      </div>
                    </div>
                  </nz-collapse-panel>

                  <nz-collapse-panel nzHeader="高级设置">
                    <div class="form-row">
                      <label class="toggle-label">
                        <span>启用自定义指标采集</span>
                        <nz-switch formControlName="enableCustomMetrics"></nz-switch>
                      </label>
                    </div>

                    <div class="form-row" *ngIf="monitorForm.get('enableCustomMetrics')?.value">
                      <nz-form-item class="full-width">
                        <nz-form-label [nzSpan]="6">自定义指标路径</nz-form-label>
                        <nz-form-control [nzSpan]="18">
                          <input nz-input formControlName="customMetricsPath" placeholder="/metrics/custom" />
                          <div class="hint">额外的指标端点路径</div>
                        </nz-form-control>
                      </nz-form-item>
                    </div>

                    <div class="form-row">
                      <label class="toggle-label">
                        <span>启用默认告警规则</span>
                        <nz-switch formControlName="enableAlerts"></nz-switch>
                      </label>
                    </div>
                  </nz-collapse-panel>
                </nz-collapse>
              </form>

              <div class="card-actions">
                <button nz-button (click)="resetForm()" [disabled]="isLoading('MONITOR_CREATE')">重置</button>
                <button nz-button nzType="primary" 
                        (click)="submitMonitor()" 
                        [disabled]="monitorForm.invalid || isLoading('MONITOR_CREATE')">
                  <i nz-icon [nzType]="editingMonitor ? 'save' : 'plus'"></i>
                  {{ editingMonitor ? '更新监控' : '创建监控' }}
                </button>
              </div>
            </nz-card>
          </div>
        </nz-tab>

        <!-- 告警列表 -->
        <nz-tab nzTitle="告警">
          <div class="tab-content">
            <div class="actions-toolbar">
              <button nz-button nzType="primary" (click)="loadAlerts()">
                <i nz-icon nzType="reload"></i>
                刷新
              </button>
            </div>

            <nz-card class="table-card">
              <div class="table-container">
                <nz-table [nzData]="alerts" class="monitors-table" nzBordered *ngIf="alerts?.length; else alertsEmpty">
                  <thead>
                    <tr>
                      <th>来源</th>
                      <th>级别</th>
                      <th>对象</th>
                      <th>消息</th>
                      <th>时间</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr *ngFor="let a of alerts">
                      <td>{{ a.source }}</td>
                      <td><nz-tag [nzColor]="getAlertColor(a.severity)">{{ a.severity || 'info' }}</nz-tag></td>
                      <td>{{ a.object || a.labels?.cluster || '-' }}</td>
                      <td>{{ a.message || a.labels?.alertname }}</td>
                      <td>{{ a.time || '-' }}</td>
                    </tr>
                  </tbody>
                </nz-table>
                <ng-template #alertsEmpty>
                  <app-empty-state icon="warning" title="暂无告警" hint="系统当前没有活动告警"></app-empty-state>
                </ng-template>
              </div>
            </nz-card>
          </div>
        </nz-tab>
      </nz-tabset>
    </div>

    <!-- 加载模板 -->
    <ng-template #loadingTemplate>
      <div class="loading-container">
        <nz-spin nzSize="large">
          <div class="loading-tip">正在加载监控配置...</div>
        </nz-spin>
      </div>
    </ng-template>
  `,
  styleUrl: './monitor-management.component.scss'
})
export class MonitorManagementComponent implements OnInit, OnDestroy {
  private destroy$ = new Subject<void>();
  
  monitors: PolarDBXMonitor[] = [];
  displayedColumns: string[] = ['name', 'namespace', 'clusterName', 'status', 'configuration', 'createdTime', 'actions'];
  alerts: any[] = [];
  alertsColumns: string[] = ['source', 'severity', 'object', 'message', 'time'];
  selectedTab = 0;
  editingMonitor: PolarDBXMonitor | null = null;
  
  monitorForm: FormGroup;

  constructor(
    private apiService: ApiService,
    private loadingService: LoadingService,
    private fb: FormBuilder,
    private message: NzMessageService,
    private modal: NzModalService
  ) {
    this.monitorForm = this.createMonitorForm();
  }

  ngOnInit(): void {
    this.loadMonitors();
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  private createMonitorForm(): FormGroup {
    return this.fb.group({
      name: ['', [Validators.required, Validators.pattern(/^[a-z0-9-]+$/)]],
      namespace: ['default'],
      clusterName: ['', Validators.required],
      monitorInterval: ['30s'],
      scrapeTimeout: ['10s'],
      enableCustomMetrics: [false],
      customMetricsPath: ['/metrics/custom'],
      enableAlerts: [true]
    });
  }

  isLoading(key: keyof typeof LoadingKeys): boolean {
    return this.loadingService.isLoading(LoadingKeys[key]);
  }

  onTabChange(event: any): void {
    this.selectedTab = event.index;
    if (event.index === 0) {
      this.editingMonitor = null;
      this.resetForm();
    } else if (event.index === 2) {
      this.loadAlerts();
    }
  }

  loadMonitors(): void {
    this.apiService.getMonitors()
      .pipe(
        takeUntil(this.destroy$),
        finalize(() => {})
      )
      .subscribe({
        next: (monitors: PolarDBXMonitor[]) => {
          this.monitors = monitors || [];
        },
        error: (error) => {
          console.error('加载监控失败:', error);
          this.message.error('加载监控失败');
        }
      });
  }

  refreshMonitors(): void {
    this.loadMonitors();
  }

  submitMonitor(): void {
    if (this.monitorForm.invalid) return;

    const formValue = this.monitorForm.value;
    const monitorRequest: CreateMonitorRequest = {
      name: formValue.name,
      namespace: formValue.namespace || 'default',
      clusterName: formValue.clusterName,
      monitorInterval: formValue.monitorInterval,
      scrapeTimeout: formValue.scrapeTimeout
    };

    const operation = this.editingMonitor
      ? this.apiService.updateMonitor(this.editingMonitor.metadata?.['namespace'] as string, this.editingMonitor)
      : this.apiService.createMonitor(monitorRequest.namespace!, monitorRequest);

    operation.pipe(
      takeUntil(this.destroy$),
      finalize(() => {})
    ).subscribe({
      next: (monitor) => {
        const message = this.editingMonitor ? '监控更新成功' : '监控创建成功';
        this.message.success(message);
        this.resetForm();
        this.selectedTab = 0;
        this.loadMonitors();
      },
      error: (error) => {
        console.error('保存监控失败:', error);
        this.message.error('保存监控失败');
      }
    });
  }

  editMonitor(monitor: PolarDBXMonitor): void {
    this.editingMonitor = monitor;
    this.monitorForm.patchValue({
      name: monitor.metadata.name,
      namespace: monitor.metadata.namespace,
      clusterName: monitor.spec.clusterName,
      monitorInterval: monitor.spec.monitorInterval || '30s',
      scrapeTimeout: monitor.spec.scrapeTimeout || '10s',
      enableCustomMetrics: false,
      customMetricsPath: '/metrics/custom',
      enableAlerts: true
    });
    this.selectedTab = 1;
  }

  deleteMonitor(monitor: PolarDBXMonitor): void {
    if (confirm(`删除监控配置\n\n您确定要删除监控配置 "${monitor.metadata.name}" 吗？`)) {
      this.apiService.deleteMonitor(monitor.metadata?.['namespace'] as string, monitor.metadata.name)
        .pipe(
          takeUntil(this.destroy$),
          finalize(() => {})
        )
        .subscribe({
          next: () => {
            this.message.success('监控配置删除成功');
            this.loadMonitors();
          },
          error: (error) => {
            console.error('删除监控失败:', error);
            this.message.error('删除监控配置失败');
          }
        });
    }
  }

  viewMonitorDetails(monitor: PolarDBXMonitor): void {
    // TODO: 实现监控详情对话框
    console.log('查看监控详情:', monitor);
  }

  resetForm(): void {
    this.editingMonitor = null;
    this.monitorForm.reset({
      name: '',
      namespace: 'default',
      clusterName: '',
      monitorInterval: '30s',
      scrapeTimeout: '10s',
      enableCustomMetrics: false,
      customMetricsPath: '/metrics/custom',
      enableAlerts: true
    });
  }

  getStatusColor(phase?: string): string {
    switch (phase) {
      case 'Running': return '#52c41a';
      case 'Creating': return '#1890ff';
      case 'Failed': return '#f5222d';
      case 'Deleting': return '#faad14';
      default: return '#bfbfbf';
    }
  }

  getStatusIcon(phase?: string): string {
    switch (phase) {
      case 'Running': return 'check_circle';
      case 'Creating': return 'hourglass_empty';
      case 'Failed': return 'error';
      case 'Deleting': return 'delete';
      default: return 'help';
    }
  }

  getStatusLabel(phase?: string): string {
    switch (phase) {
      case 'Running': return '运行中';
      case 'Creating': return '创建中';
      case 'Failed': return '失败';
      case 'Deleting': return '删除中';
      default: return '未知';
    }
  }

  formatDate(dateString?: string): string {
    if (!dateString) return 'N/A';
    return new Date(dateString).toLocaleString();
  }

  loadAlerts(): void {
    this.apiService.listAlerts({}).subscribe({
      next: (res) => { this.alerts = res?.items || []; },
      error: () => { this.alerts = []; }
    });
  }

  getAlertColor(sev?: string): string {
    const s = (sev || '').toLowerCase();
    if (s === 'critical') return 'red';
    if (s === 'warning') return 'orange';
    return 'blue';
  }
}