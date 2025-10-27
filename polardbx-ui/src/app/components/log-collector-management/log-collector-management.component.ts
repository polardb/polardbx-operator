import { Component, OnInit, OnDestroy, ViewChild, TemplateRef } from '@angular/core';
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
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzSwitchModule } from 'ng-zorro-antd/switch';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzCollapseModule } from 'ng-zorro-antd/collapse';
import { NzDropDownModule } from 'ng-zorro-antd/dropdown';
import { NzProgressModule } from 'ng-zorro-antd/progress';
import { NzStepsModule } from 'ng-zorro-antd/steps';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzModalModule, NzModalService } from 'ng-zorro-antd/modal';
import { Subject } from 'rxjs';
import { takeUntil, finalize } from 'rxjs/operators';

import { ApiService } from '../../services/api.service';
import { LoadingService, LoadingKeys } from '../../services/loading.service';
import { 
  PolarDBXLogCollector, 
  PolarDBXLogCollectorList,
  CreateLogCollectorRequest,
  UpdateLogCollectorRequest,
  COMPONENT_PRESETS,
  NAMING_PATTERNS,
  ComponentPreset,
  ComponentNamingPattern,
  validateComponentName,
  getCollectorDescription,
  getReadinessPercentage,
  getCollectorStatusColor,
  getCollectorStatusText,
  generateComponentName
} from '../../models/log-collector.model';
// Removed ConfirmationDialogComponent import - using native confirm() instead

@Component({
  selector: 'app-log-collector-management',
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
    NzDropDownModule,
    NzProgressModule,
    NzStepsModule,
    NzAlertModule,
    NzEmptyModule,
    NzModalModule
  ],
  template: `
    <div class="log-collector-management">
      <nz-tabset class="main-tabs" [(nzSelectedIndex)]="selectedTab" (nzSelectedIndexChange)="onTabChange($event)">
        <!-- 日志采集器列表选项卡 -->
        <nz-tab nzTitle="日志采集器">
            <div class="tab-content">
              <div class="actions-toolbar">
                <button nz-button nzType="default" (click)="refreshCollectors()" [disabled]="isLoading('LOG_COLLECTOR_LIST')">
                  <i nz-icon nzType="reload"></i>
                  <span>刷新</span>
                </button>
                <button nz-button nzType="default" (click)="selectedTab = 1">
                  <i nz-icon nzType="plus"></i>
                  <span>创建采集器</span>
                </button>
              </div>

              <nz-card class="table-card">
                <div class="table-container" *ngIf="!isLoading('LOG_COLLECTOR_LIST'); else loadingTemplate">
                  <nz-table [nzData]="logCollectors" [nzShowPagination]="false" class="collectors-table">
                    <thead>
                      <tr>
                        <th>采集器名称</th>
                        <th>命名空间</th>
                        <th>组件</th>
                        <th>状态</th>
                        <th>配置</th>
                        <th>创建时间</th>
                        <th>操作</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr *ngFor="let collector of logCollectors">
                        <td>
                          <div class="collector-name">
                            <i nz-icon nzType="book"></i>
                            <span>{{ collector.metadata.name }}</span>
                          </div>
                        </td>
                        <td>{{ collector.metadata.namespace }}</td>
                        <td>
                          <div class="components-info">
                            <div *ngIf="collector.spec.fileBeatName" class="component-item">
                              <i nz-icon nzType="file-text" class="filebeat-icon"></i>
                              <span>{{ collector.spec.fileBeatName }}</span>
                            </div>
                            <div *ngIf="collector.spec.logStashName" class="component-item">
                              <i nz-icon nzType="deployment-unit" class="logstash-icon"></i>
                              <span>{{ collector.spec.logStashName }}</span>
                            </div>
                            <div *ngIf="!collector.spec.fileBeatName && !collector.spec.logStashName" class="no-components">
                              <i nz-icon nzType="warning"></i>
                              <span>无组件</span>
                            </div>
                          </div>
                        </td>
                        <td>
                          <div class="status-info">
                            <div class="status-summary">
                              <nz-tag [nzColor]="getStatusColor(collector)">{{ getStatusText(collector) }}</nz-tag>
                            </div>
                            <div class="readiness-bar" *ngIf="hasStatus(collector)">
                              <nz-progress [nzPercent]="getReadiness(collector)" [nzStatus]="getReadiness(collector) >= 100 ? 'success' : 'active'"></nz-progress>
                              <span class="readiness-text">{{ getReadiness(collector) }}% 就绪</span>
                            </div>
                          </div>
                        </td>
                        <td>
                          <div class="config-info">
                            <div *ngIf="collector.status?.configStatus?.fileBeatConfigId as fbConfigId" class="config-item">
                              <i nz-icon nzType="setting"></i>
                              <span>FB: {{ fbConfigId.substring(0, 8) }}...</span>
                            </div>
                            <div *ngIf="collector.status?.configStatus?.logStashConfigId as lsConfigId" class="config-item">
                              <i nz-icon nzType="sliders"></i>
                              <span>LS: {{ lsConfigId.substring(0, 8) }}...</span>
                            </div>
                          </div>
                        </td>
                        <td>{{ formatDate(collector.metadata.creationTimestamp) }}</td>
                        <td>
                          <button nz-button nzType="default" nz-dropdown [nzDropdownMenu]="collectorMenu" [disabled]="isLoading('LOG_COLLECTOR_UPDATE')">
                            <span>操作</span>
                            <i nz-icon nzType="down"></i>
                          </button>
                          <nz-dropdown-menu #collectorMenu="nzDropdownMenu">
                            <ul nz-menu>
                              <li nz-menu-item (click)="viewCollectorDetails(collector)"><i nz-icon nzType="eye"></i> 查看详情</li>
                              <li nz-menu-item (click)="editCollector(collector)"><i nz-icon nzType="edit"></i> 编辑采集器</li>
                              <li nz-menu-item (click)="viewConfiguration(collector)" [nzDisabled]="!hasConfiguration(collector)"><i nz-icon nzType="code"></i> 查看配置</li>
                              <li nz-menu-item (click)="deleteCollector(collector)" class="delete-action"><i nz-icon nzType="delete"></i> 删除采集器</li>
                            </ul>
                          </nz-dropdown-menu>
                        </td>
                      </tr>
                    </tbody>
                  </nz-table>

                  <div class="no-data" *ngIf="logCollectors.length === 0">
                    <i nz-icon nzType="book"></i>
                    <p>未找到日志采集器。创建您的第一个采集器开始使用。</p>
                  </div>
                </div>
              </nz-card>
            </div>
        </nz-tab>

        <!-- 创建/编辑采集器选项卡 -->
        <nz-tab nzTitle="创建采集器">
          <div class="tab-content">
            <!-- 步骤导航 -->
            <div class="wizard-header">
              <nz-steps [nzCurrent]="currentStep" nzSize="small" class="creation-steps">
                <nz-step nzTitle="基本信息" nzDescription="配置采集器名称和命名空间"></nz-step>
                <nz-step nzTitle="组件选择" nzDescription="选择 FileBeat 和 LogStash 组件"></nz-step>
                <nz-step nzTitle="配置确认" nzDescription="确认配置并创建采集器"></nz-step>
              </nz-steps>
            </div>

            <!-- 步骤内容 -->
            <nz-card class="wizard-card">
              <form [formGroup]="collectorForm" class="collector-wizard">
                
                <!-- 步骤 1: 基本信息 -->
                <div class="step-content" *ngIf="currentStep === 0">
                  <div class="step-header">
                    <h3>
                      <i nz-icon nzType="info-circle"></i>
                      基本信息配置
                    </h3>
                    <p>设置日志采集器的基本信息，包括名称、命名空间和预设模板</p>
                  </div>

                  <div class="form-grid">
                    <nz-form-item class="form-item-large">
                      <nz-form-label nzRequired>采集器名称</nz-form-label>
                      <nz-form-control nzHasFeedback [nzErrorTip]="nameErrorTpl">
                        <input nz-input formControlName="name" placeholder="例如: my-log-collector" />
                        <ng-template #nameErrorTpl let-control>
                          <ng-container *ngIf="control.hasError('required')">采集器名称是必填项</ng-container>
                          <ng-container *ngIf="control.hasError('pattern')">名称必须是合法的 Kubernetes 资源名称</ng-container>
                        </ng-template>
                      </nz-form-control>
                    </nz-form-item>

                    <nz-form-item class="form-item-medium">
                      <nz-form-label>命名空间</nz-form-label>
                      <nz-form-control nzExtra="采集器将部署在此命名空间中">
                        <input nz-input formControlName="namespace" placeholder="default" />
                      </nz-form-control>
                    </nz-form-item>

                    <nz-form-item class="form-item-large">
                      <nz-form-label>组件预设</nz-form-label>
                      <nz-form-control nzExtra="选择预设模板可快速配置常用组件组合">
                        <nz-select formControlName="preset" nzPlaceHolder="选择预设模板或自定义配置">
                          <nz-option [nzValue]="null" nzLabel="自定义配置"></nz-option>
                          <nz-option *ngFor="let preset of componentPresets" 
                                     [nzValue]="preset.name" 
                                     [nzLabel]="preset.label">
                            <span>{{ preset.label }}</span>
                            <nz-tag nzColor="blue" style="margin-left: 8px;">{{ preset.description }}</nz-tag>
                          </nz-option>
                        </nz-select>
                      </nz-form-control>
                    </nz-form-item>
                  </div>

                  <!-- 预设说明 -->
                  <div class="preset-info" *ngIf="selectedPreset">
                    <nz-alert nzType="info" nzShowIcon>
                      <ng-template #nzMessage>
                        <strong>{{ selectedPreset.label }}</strong>
                      </ng-template>
                      <ng-template #nzDescription>
                        {{ selectedPreset.description }}
                        <br>
                        <span *ngIf="selectedPreset.fileBeatName">✓ FileBeat: {{ selectedPreset.fileBeatName }}</span>
                        <span *ngIf="selectedPreset.logStashName" style="margin-left: 16px;">✓ LogStash: {{ selectedPreset.logStashName }}</span>
                      </ng-template>
                    </nz-alert>
                  </div>
                </div>

                <!-- 步骤 2: 组件选择 -->
                <div class="step-content" *ngIf="currentStep === 1">
                  <div class="step-header">
                    <h3>
                      <i nz-icon nzType="setting"></i>
                      组件配置
                    </h3>
                    <p>选择和配置 FileBeat 日志采集器和 LogStash 日志处理器</p>
                  </div>

                  <div class="components-grid">
                    <!-- FileBeat 组件卡片 -->
                    <div class="component-card" [class.active]="collectorForm.get('enableFileBeat')!.value">
                      <div class="component-header">
                        <div class="component-info">
                          <i nz-icon nzType="file-text" class="component-icon filebeat"></i>
                          <div>
                            <h4>FileBeat</h4>
                            <p>轻量级日志采集器</p>
                          </div>
                        </div>
                        <nz-switch formControlName="enableFileBeat" 
                                   (ngModelChange)="onComponentToggle('fileBeat', $event)">
                        </nz-switch>
                      </div>

                      <div class="component-details" *ngIf="collectorForm.get('enableFileBeat')!.value">
                        <nz-form-item>
                          <nz-form-label>组件名称</nz-form-label>
                          <nz-form-control>
                            <input nz-input formControlName="fileBeatName" placeholder="filebeat-main" />
                          </nz-form-control>
                        </nz-form-item>

                        <div class="component-features">
                          <h5>功能特性</h5>
                          <ul>
                            <li><i nz-icon nzType="check-circle" class="feature-icon"></i>轻量级日志采集</li>
                            <li><i nz-icon nzType="check-circle" class="feature-icon"></i>实时文件监控</li>
                            <li><i nz-icon nzType="check-circle" class="feature-icon"></i>自动故障恢复</li>
                            <li><i nz-icon nzType="check-circle" class="feature-icon"></i>低资源占用</li>
                          </ul>
                        </div>
                      </div>
                    </div>

                    <!-- LogStash 组件卡片 -->
                    <div class="component-card" [class.active]="collectorForm.get('enableLogStash')!.value">
                      <div class="component-header">
                        <div class="component-info">
                          <i nz-icon nzType="deployment-unit" class="component-icon logstash"></i>
                          <div>
                            <h4>LogStash</h4>
                            <p>日志处理和转换引擎</p>
                          </div>
                        </div>
                        <nz-switch formControlName="enableLogStash" 
                                   (ngModelChange)="onComponentToggle('logStash', $event)">
                        </nz-switch>
                      </div>

                      <div class="component-details" *ngIf="collectorForm.get('enableLogStash')!.value">
                        <nz-form-item>
                          <nz-form-label>组件名称</nz-form-label>
                          <nz-form-control>
                            <input nz-input formControlName="logStashName" placeholder="logstash-main" />
                          </nz-form-control>
                        </nz-form-item>

                        <div class="component-features">
                          <h5>功能特性</h5>
                          <ul>
                            <li><i nz-icon nzType="check-circle" class="feature-icon"></i>数据解析和转换</li>
                            <li><i nz-icon nzType="check-circle" class="feature-icon"></i>灵活的过滤器</li>
                            <li><i nz-icon nzType="check-circle" class="feature-icon"></i>多种输出格式</li>
                            <li><i nz-icon nzType="check-circle" class="feature-icon"></i>水平扩展支持</li>
                          </ul>
                        </div>
                      </div>
                    </div>
                  </div>

                  <!-- 命名模式配置 -->
                  <div class="naming-section" *ngIf="!selectedPreset">
                    <nz-divider nzText="高级选项" nzOrientation="left"></nz-divider>
                    
                    <nz-form-item>
                      <nz-form-label>命名模式</nz-form-label>
                      <nz-form-control nzExtra="使用命名模式可以自动生成规范的组件名称">
                        <nz-select formControlName="pattern" nzPlaceHolder="选择命名模式">
                          <nz-option [nzValue]="null" nzLabel="手动命名"></nz-option>
                          <nz-option *ngFor="let pattern of namingPatterns" 
                                     [nzValue]="pattern" 
                                     [nzLabel]="pattern.label">
                          </nz-option>
                        </nz-select>
                      </nz-form-control>
                    </nz-form-item>

                    <div class="pattern-config" *ngIf="selectedNamingPattern">
                      <div class="pattern-fields">
                        <nz-form-item>
                          <nz-form-label>环境</nz-form-label>
                          <nz-form-control>
                            <input nz-input formControlName="patternEnv" placeholder="prod" />
                          </nz-form-control>
                        </nz-form-item>
                        <nz-form-item>
                          <nz-form-label>集群 ID</nz-form-label>
                          <nz-form-control>
                            <input nz-input formControlName="patternCluster" placeholder="main" />
                          </nz-form-control>
                        </nz-form-item>
                        <nz-form-item>
                          <nz-form-label>功能用途</nz-form-label>
                          <nz-form-control>
                            <input nz-input formControlName="patternFunction" placeholder="database-logs" />
                          </nz-form-control>
                        </nz-form-item>
                      </div>
                      <button nz-button nzType="dashed" (click)="generateNames()" class="generate-btn">
                        <i nz-icon nzType="highlight"></i>
                        生成组件名称
                      </button>
                    </div>
                  </div>
                </div>

                <!-- 步骤 3: 配置确认 -->
                <div class="step-content" *ngIf="currentStep === 2">
                  <div class="step-header">
                    <h3>
                      <i nz-icon nzType="check-circle"></i>
                      配置确认
                    </h3>
                    <p>请确认以下配置信息，确认无误后点击创建按钮</p>
                  </div>

                  <div class="config-summary">
                    <!-- 基本信息摘要 -->
                    <nz-card nzTitle="基本信息" nzSize="small" class="summary-card">
                      <div class="summary-item">
                        <span class="label">采集器名称:</span>
                        <span class="value">{{ collectorForm.get('name')!.value || '未设置' }}</span>
                      </div>
                      <div class="summary-item">
                        <span class="label">命名空间:</span>
                        <span class="value">{{ collectorForm.get('namespace')!.value || 'default' }}</span>
                      </div>
                      <div class="summary-item" *ngIf="selectedPreset">
                        <span class="label">使用预设:</span>
                        <nz-tag nzColor="blue">{{ selectedPreset.label }}</nz-tag>
                      </div>
                    </nz-card>

                    <!-- 组件配置摘要 -->
                    <nz-card nzTitle="组件配置" nzSize="small" class="summary-card">
                      <div class="component-summary">
                        <div class="component-item" *ngIf="collectorForm.get('enableFileBeat')!.value">
                          <i nz-icon nzType="file-text" class="component-icon filebeat"></i>
                          <div class="component-details">
                            <strong>FileBeat</strong>
                            <span>{{ collectorForm.get('fileBeatName')!.value }}</span>
                          </div>
                          <nz-tag nzColor="green">已启用</nz-tag>
                        </div>
                        <div class="component-item" *ngIf="collectorForm.get('enableLogStash')!.value">
                          <i nz-icon nzType="deployment-unit" class="component-icon logstash"></i>
                          <div class="component-details">
                            <strong>LogStash</strong>
                            <span>{{ collectorForm.get('logStashName')!.value }}</span>
                          </div>
                          <nz-tag nzColor="green">已启用</nz-tag>
                        </div>
                        <div *ngIf="!collectorForm.get('enableFileBeat')!.value && !collectorForm.get('enableLogStash')!.value" class="no-components">
                          <nz-empty nzNotFoundContent="未选择任何组件" nzNotFoundImage="simple"></nz-empty>
                        </div>
                      </div>
                    </nz-card>

                    <!-- 部署预览 -->
                    <nz-card nzTitle="部署预览" nzSize="small" class="summary-card">
                      <nz-alert nzType="info" nzShowIcon nzMessage="即将创建的资源">
                        <ng-template #nzDescription>
                          <ul class="resource-list">
                            <li *ngIf="collectorForm.get('enableFileBeat')!.value">
                              <i nz-icon nzType="container"></i>
                              FileBeat DaemonSet: {{ collectorForm.get('fileBeatName')!.value }}
                            </li>
                            <li *ngIf="collectorForm.get('enableLogStash')!.value">
                              <i nz-icon nzType="deployment-unit"></i>
                              LogStash Deployment: {{ collectorForm.get('logStashName')!.value }}
                            </li>
                            <li>
                              <i nz-icon nzType="setting"></i>
                              ConfigMap: {{ collectorForm.get('name')!.value }}-config
                            </li>
                          </ul>
                        </ng-template>
                      </nz-alert>
                    </nz-card>
                  </div>
                </div>

                <!-- 步骤导航按钮 -->
                <div class="wizard-actions">
                  <button nz-button nzType="default" 
                          (click)="previousStep()" 
                          [disabled]="currentStep === 0">
                    <i nz-icon nzType="left"></i>
                    上一步
                  </button>
                  
                  <div class="action-group">
                    <button nz-button nzType="default" (click)="resetForm()">
                      <i nz-icon nzType="reload"></i>
                      重置
                    </button>
                    
                    <button nz-button nzType="primary" 
                            (click)="nextStep()" 
                            *ngIf="currentStep < 2"
                            [disabled]="!canProceedToNextStep()">
                      下一步
                      <i nz-icon nzType="right"></i>
                    </button>
                    
                    <button nz-button nzType="primary" 
                            (click)="submitCollector()" 
                            *ngIf="currentStep === 2"
                            [disabled]="collectorForm.invalid || isLoading('LOG_COLLECTOR_CREATE')"
                            [nzLoading]="isLoading('LOG_COLLECTOR_CREATE')">
                      <i nz-icon nzType="plus"></i>
                      {{ editingCollector ? '更新采集器' : '创建采集器' }}
                    </button>
                  </div>
                </div>
              </form>
            </nz-card>
          </div>
        </nz-tab>
      </nz-tabset>
    </div>

    <!-- 加载模板 -->
    <ng-template #loadingTemplate>
      <nz-spin [nzSpinning]="true">
        <div class="loading-tip">正在加载日志采集器...</div>
      </nz-spin>
    </ng-template>

    <!-- 详情对话框模板 -->
    <ng-template #detailsDialog let-data>
      <h3 style="margin-top:0">采集器详情</h3>
      <div class="detail-row"><strong>名称:</strong> {{ data?.metadata?.name }}</div>
      <div class="detail-row"><strong>命名空间:</strong> {{ data?.metadata?.namespace }}</div>
      <div class="detail-row"><strong>FileBeat:</strong> {{ data?.spec?.fileBeatName || '-' }}</div>
      <div class="detail-row"><strong>LogStash:</strong> {{ data?.spec?.logStashName || '-' }}</div>
      <div class="detail-row" *ngIf="data?.status?.configStatus as cs">
        <strong>配置状态:</strong>
        <pre style="white-space:pre-wrap;background:#f6f8fa;padding:8px;border-radius:4px">{{ cs | json }}</pre>
      </div>
      <div class="detail-row" *ngIf="!data?.status?.configStatus">
        <nz-alert nzType="info" nzMessage="暂无配置状态" nzShowIcon></nz-alert>
      </div>
    </ng-template>

    <!-- 配置查看对话框模板 -->
    <ng-template #configDialog let-data>
      <h3 style="margin-top:0">配置查看</h3>
      <div *ngIf="data?.status?.configStatus as cs">
        <nz-card nzTitle="ConfigStatus" nzSize="small" style="margin-bottom:12px">
          <pre style="white-space:pre-wrap;background:#f6f8fa;padding:8px;border-radius:4px">{{ cs | json }}</pre>
        </nz-card>
        <nz-card nzTitle="Spec Snapshot" nzSize="small" *ngIf="data?.status?.specSnapshot as snap">
          <pre style="white-space:pre-wrap;background:#f6f8fa;padding:8px;border-radius:4px">{{ snap | json }}</pre>
        </nz-card>
      </div>
      <div *ngIf="!data?.status?.configStatus">
        <nz-alert nzType="info" nzMessage="该采集器暂无配置可显示" nzShowIcon></nz-alert>
      </div>
    </ng-template>
  `,
  styleUrl: './log-collector-management.component.scss'
})
export class LogCollectorManagementComponent implements OnInit, OnDestroy {
  private destroy$ = new Subject<void>();
  
  logCollectors: PolarDBXLogCollector[] = [];
  displayedColumns: string[] = ['name', 'namespace', 'components', 'status', 'configuration', 'createdTime', 'actions'];
  selectedTab = 0;
  editingCollector: PolarDBXLogCollector | null = null;
  selectedNamingPattern: ComponentNamingPattern | null = null;
  
  // 步骤向导相关属性
  currentStep = 0;
  selectedPreset: ComponentPreset | null = null;
  
  componentPresets = COMPONENT_PRESETS;
  namingPatterns = NAMING_PATTERNS;
  
  collectorForm: FormGroup;
  // 模板引用 (用于 NzModalService 动态渲染)
  @ViewChild('detailsDialog', { static: true }) detailsDialog!: TemplateRef<any>;
  @ViewChild('configDialog', { static: true }) configDialog!: TemplateRef<any>;

  constructor(
    private apiService: ApiService,
    private loadingService: LoadingService,
    private fb: FormBuilder,
    private message: NzMessageService,
    private modal: NzModalService
  ) {
    this.collectorForm = this.createCollectorForm();
  }

  ngOnInit(): void {
    this.loadLogCollectors();
    // Wire preset change
    this.collectorForm.get('preset')?.valueChanges
      .pipe(takeUntil(this.destroy$))
      .subscribe((name: string | null) => this.applyComponentPreset(name));
    // Wire naming pattern change
    this.collectorForm.get('pattern')?.valueChanges
      .pipe(takeUntil(this.destroy$))
      .subscribe((pattern: ComponentNamingPattern | null) => this.applyNamingPattern(pattern));
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  private createCollectorForm(): FormGroup {
    return this.fb.group({
      name: ['', [Validators.required, Validators.pattern(/^[a-z0-9-]+$/)]],
      namespace: ['default'],
      preset: [null],
      enableFileBeat: [false],
      fileBeatName: [''],
      enableLogStash: [false],
      logStashName: [''],
      pattern: [null],
      patternEnv: [''],
      patternCluster: [''],
      patternFunction: ['']
    });
  }

  isLoading(key: keyof typeof LoadingKeys): boolean {
    return this.loadingService.isLoading(LoadingKeys[key]);
  }

  onTabChange(index: number): void {
    this.selectedTab = index;
    if (index === 0) {
      this.editingCollector = null;
      this.resetForm();
    }
  }

  loadLogCollectors(): void {
    const ns = this.collectorForm?.get('namespace')?.value || 'default';
    this.apiService.getLogCollectors(ns)
      .pipe(
        takeUntil(this.destroy$),
        finalize(() => {})
      )
      .subscribe({
        next: (collectors: PolarDBXLogCollector[]) => {
          this.logCollectors = collectors || [];
          // 兜底：如果某些采集器没有 status，从平台状态接口融合信息
          this.apiService.getLogServiceStatus().pipe(takeUntil(this.destroy$)).subscribe({
            next: (s: any) => {
              const comps = s?.components || {};
              const fb = comps?.filebeat || {};
              const ls = comps?.logstash || {};
              this.logCollectors = (this.logCollectors || []).map(c => {
                const cc: any = c as any;
                if (!cc.status) { cc.status = {}; }
                if (!cc.status.configStatus) {
                  const fbReady = fb?.replicas?.ready ?? 0;
                  const fbTotal = fb?.replicas?.total ?? 0;
                  const lsReady = ls?.replicas?.ready ?? 0;
                  const lsTotal = ls?.replicas?.total ?? 0;
                  cc.status.configStatus = {
                    fileBeatReadyCount: fbReady,
                    fileBeatCount: fbTotal,
                    logStashReadyCount: lsReady,
                    logStashCount: lsTotal
                  } as any;
                }
                return cc as PolarDBXLogCollector;
              });
            },
            error: () => {}
          });
        },
        error: (error) => {
          console.error('加载日志采集器失败:', error);
          this.message.error('加载日志采集器失败');
        }
      });
  }

  refreshCollectors(): void {
    this.loadLogCollectors();
  }

  applyComponentPreset(presetName: string | null): void {
    if (!presetName) {
      this.selectedPreset = null;
      return;
    }
    const preset = this.componentPresets.find(p => p.name === presetName);
    if (!preset) return;
    
    this.selectedPreset = preset;
    this.collectorForm.patchValue({
      enableFileBeat: !!preset.fileBeatName,
      fileBeatName: preset.fileBeatName || '',
      enableLogStash: !!preset.logStashName,
      logStashName: preset.logStashName || ''
    });
  }

  applyNamingPattern(pattern: ComponentNamingPattern | null): void {
    this.selectedNamingPattern = pattern;
    if (pattern) {
      this.collectorForm.patchValue({
        patternEnv: 'prod',
        patternCluster: 'main',
        patternFunction: 'database-logs'
      });
    }
  }

  generateNames(): void {
    if (!this.selectedNamingPattern) return;

    const variables = {
      env: this.collectorForm.get('patternEnv')!.value || 'prod',
      cluster: this.collectorForm.get('patternCluster')!.value || 'main',
      function: this.collectorForm.get('patternFunction')!.value || 'logs'
    };

    const fileBeatName = generateComponentName(this.selectedNamingPattern.fileBeatPattern, variables);
    const logStashName = generateComponentName(this.selectedNamingPattern.logStashPattern, variables);

    this.collectorForm.patchValue({
      fileBeatName: fileBeatName,
      logStashName: logStashName,
      enableFileBeat: true,
      enableLogStash: true
    });
  }

  submitCollector(): void {
    if (this.collectorForm.invalid) return;

    const formValue = this.collectorForm.value;
    const collectorRequest: CreateLogCollectorRequest = {
      name: formValue.name,
      namespace: formValue.namespace || 'default',
      fileBeatName: formValue.enableFileBeat ? formValue.fileBeatName : undefined,
      logStashName: formValue.enableLogStash ? formValue.logStashName : undefined
    };

    const operation = this.editingCollector
      ? this.apiService.updateLogCollector(this.editingCollector.metadata.namespace!, {
          ...this.editingCollector,
          spec: {
            fileBeatName: collectorRequest.fileBeatName,
            logStashName: collectorRequest.logStashName
          }
        })
      : this.apiService.createLogCollector(collectorRequest.namespace!, collectorRequest);

    operation.pipe(
      takeUntil(this.destroy$),
      finalize(() => {})
    ).subscribe({
      next: (collector) => {
            const messageText = this.editingCollector ? '日志采集器更新成功' : '日志采集器创建成功';
    this.message.success(messageText);
        this.resetForm();
        this.selectedTab = 0;
        this.loadLogCollectors();
      },
      error: (error) => {
        console.error('保存日志采集器失败:', error);
        this.message.error('保存日志采集器失败');
      }
    });
  }

  editCollector(collector: PolarDBXLogCollector): void {
    this.editingCollector = collector;
    this.collectorForm.patchValue({
      name: collector.metadata.name,
      namespace: collector.metadata.namespace,
      enableFileBeat: !!collector.spec.fileBeatName,
      fileBeatName: collector.spec.fileBeatName || '',
      enableLogStash: !!collector.spec.logStashName,
      logStashName: collector.spec.logStashName || ''
    });
    this.selectedTab = 1;
  }

  deleteCollector(collector: PolarDBXLogCollector): void {
    if (confirm(`删除日志采集器\n\n确定要删除日志采集器 "${collector.metadata.name}" 吗？`)) {
      this.apiService.deleteLogCollector(collector.metadata.namespace!, collector.metadata.name)
        .pipe(
          takeUntil(this.destroy$),
          finalize(() => {})
        )
        .subscribe({
          next: () => {
            this.message.success('日志采集器删除成功');
            this.loadLogCollectors();
          },
          error: (error) => {
            console.error('删除日志采集器失败:', error);
            this.message.error('删除日志采集器失败');
          }
        });
    }
  }

  viewCollectorDetails(collector: PolarDBXLogCollector): void {
    this.modal.create({
      nzTitle: '采集器详情',
      nzContent: (this as any).detailsDialog,
      nzData: collector,
      nzFooter: null,
      nzWidth: 720
    });
  }

  viewConfiguration(collector: PolarDBXLogCollector): void {
    if (!this.hasConfiguration(collector)) {
      this.message.info('该采集器暂无配置可显示');
      return;
    }
    this.modal.create({
      nzTitle: '查看配置',
      nzContent: (this as any).configDialog,
      nzData: collector,
      nzFooter: null,
      nzWidth: 900
    });
  }


  formatDate(dateString?: string): string {
    if (!dateString) return 'N/A';
    return new Date(dateString).toLocaleString();
  }

  getCollectorDescription(collector: PolarDBXLogCollector): string {
    return getCollectorDescription(collector);
  }

  getReadiness(collector: PolarDBXLogCollector): number {
    return getReadinessPercentage(collector.status?.configStatus);
  }

  getStatusColor(collector: PolarDBXLogCollector): string {
    return getCollectorStatusColor(collector);
  }

  getStatusText(collector: PolarDBXLogCollector): string {
    return getCollectorStatusText(collector);
  }

  hasStatus(collector: PolarDBXLogCollector): boolean {
    return !!collector.status?.configStatus;
  }

  hasConfiguration(collector: PolarDBXLogCollector): boolean {
    return !!(collector.status?.configStatus?.fileBeatConfigId || collector.status?.configStatus?.logStashConfigId);
  }

  // 步骤向导方法
  nextStep(): void {
    if (this.currentStep < 2) {
      this.currentStep++;
    }
  }

  previousStep(): void {
    if (this.currentStep > 0) {
      this.currentStep--;
    }
  }

  canProceedToNextStep(): boolean {
    switch (this.currentStep) {
      case 0:
        // 第一步：检查名称是否填写
        return !!this.collectorForm.get('name')!.value && (this.collectorForm.get('name')!.valid ?? false);
      case 1:
        // 第二步：检查是否至少选择了一个组件
        return this.collectorForm.get('enableFileBeat')!.value || this.collectorForm.get('enableLogStash')!.value;
      default:
        return true;
    }
  }

  onComponentToggle(component: 'fileBeat' | 'logStash', enabled: boolean): void {
    // 当组件被启用时，如果名称为空，则设置默认名称
    if (enabled) {
      const nameField = component === 'fileBeat' ? 'fileBeatName' : 'logStashName';
      const currentName = this.collectorForm.get(nameField)!.value;
      if (!currentName) {
        const defaultName = component === 'fileBeat' ? 'filebeat-main' : 'logstash-main';
        this.collectorForm.patchValue({ [nameField]: defaultName });
      }
    }
  }

  resetForm(): void {
    this.collectorForm.reset({
      namespace: 'default',
      enableFileBeat: false,
      enableLogStash: false
    });
    this.currentStep = 0;
    this.selectedPreset = null;
    this.selectedNamingPattern = null;
    this.editingCollector = null;
  }
}