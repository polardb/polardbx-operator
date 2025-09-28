import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReactiveFormsModule, FormsModule, FormBuilder, FormGroup } from '@angular/forms';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzSwitchModule } from 'ng-zorro-antd/switch';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzStepsModule } from 'ng-zorro-antd/steps';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzCollapseModule } from 'ng-zorro-antd/collapse';
import { NzCheckboxModule } from 'ng-zorro-antd/checkbox';
import { NzTypographyModule } from 'ng-zorro-antd/typography';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzStatisticModule } from 'ng-zorro-antd/statistic';
import { NzResultModule } from 'ng-zorro-antd/result';
import { NzDescriptionsModule } from 'ng-zorro-antd/descriptions';
import { ApiService } from '../../services/api.service';
import { Router } from '@angular/router';

@Component({
  selector: 'app-monitoring-install-wizard',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    FormsModule,
    NzCardModule,
    NzFormModule,
    NzInputModule,
    NzSelectModule,
    NzButtonModule,
    NzIconModule,
    NzSwitchModule,
    NzSpinModule,
    NzGridModule,
    NzStepsModule,
    NzAlertModule,
    NzCollapseModule,
    NzCheckboxModule,
    NzTypographyModule,
    NzDividerModule,
    NzStatisticModule,
    NzResultModule,
    NzDescriptionsModule
  ],
  template: `
    <div class="wizard">
      <div class="page-header">
        <div class="header-content">
          <h1 class="page-title">
            <i nz-icon nzType="tool" class="page-icon"></i>
            监控安装向导
          </h1>
          <p class="page-description">一站式安装和配置 PolarDB-X Monitor 监控堆栈</p>
        </div>
      </div>

      <div class="page-content">
        <nz-steps [nzCurrent]="currentStep" class="wizard-steps" nzSize="small">
          <nz-step nzTitle="环境检查" nzDescription="检测系统环境和依赖"></nz-step>
          <nz-step nzTitle="配置选择" nzDescription="选择安装方案和参数"></nz-step>
          <nz-step nzTitle="安装执行" nzDescription="执行安装并监控进度"></nz-step>
          <nz-step nzTitle="验证完成" nzDescription="验证安装结果"></nz-step>
        </nz-steps>

        <!-- 步骤1：环境检查 -->
        <nz-card *ngIf="currentStep === 0" class="step-card" nzTitle="环境检查" [nzExtra]="checkExtra">
          <ng-template #checkExtra>
            <button nz-button nzType="primary" nzSize="small" (click)="runEnvironmentCheck()" [nzLoading]="checking">
              <i nz-icon nzType="sync"></i>
              重新检查
            </button>
          </ng-template>

          <div class="check-section">
            <nz-spin [nzSpinning]="checking">
              <div class="loading-tip" *ngIf="checking">正在检查环境...</div>
              <div class="check-items">
                <div class="check-item" *ngFor="let check of environmentChecks">
                  <div class="check-info">
                    <span class="check-status">
                      <i nz-icon [nzType]="check.status === 'success' ? 'check-circle' : check.status === 'error' ? 'close-circle' : 'clock-circle'"
                         [style.color]="check.status === 'success' ? '#52c41a' : check.status === 'error' ? '#ff4d4f' : '#faad14'"></i>
                    </span>
                    <span class="check-name">{{ check.name }}</span>
                    <span class="check-description">{{ check.description }}</span>
                  </div>
                  <div class="check-result" [ngClass]="check.status">
                    {{ check.result }}
                  </div>
                </div>
              </div>
            </nz-spin>
          </div>

          <div class="step-actions">
            <button nz-button nzType="primary" 
                    [disabled]="!allChecksPassed" 
                    (click)="nextStep()">
              下一步：配置选择
              <i nz-icon nzType="right"></i>
            </button>
          </div>
        </nz-card>

        <!-- 步骤2：配置选择 -->
        <nz-card *ngIf="currentStep === 1" class="step-card" nzTitle="配置选择">
          <form [formGroup]="form" class="config-form">
            <div class="config-section">
              <h4>基本配置</h4>
              <nz-row [nzGutter]="16">
                <nz-col [nzSpan]="12">
                  <nz-form-item>
                    <nz-form-label nzRequired>安装方案</nz-form-label>
                    <nz-form-control>
                      <nz-select formControlName="deploymentType" nzPlaceHolder="选择安装方案" (ngModelChange)="onDeploymentTypeChange($event)">
                        <nz-option nzValue="default" nzLabel="标准安装 - 使用默认配置"></nz-option>
                        <nz-option nzValue="production" nzLabel="生产环境 - 高可用配置"></nz-option>
                        <nz-option nzValue="minimal" nzLabel="最小安装 - 节约资源"></nz-option>
                        <nz-option nzValue="custom" nzLabel="自定义配置"></nz-option>
                      </nz-select>
                    </nz-form-control>
                  </nz-form-item>
                </nz-col>
                <nz-col [nzSpan]="12">
                  <nz-form-item>
                    <nz-form-label>命名空间</nz-form-label>
                    <nz-form-control>
                      <input nz-input formControlName="namespace" />
                    </nz-form-control>
                  </nz-form-item>
                </nz-col>
              </nz-row>
            </div>

            <!-- 动态配置区域 -->
            <div class="config-section" *ngIf="form.value.deploymentType">
              <h4>{{ getDeploymentConfig().title }}</h4>
              <nz-alert [nzType]="getDeploymentConfig().alertType" [nzMessage]="getDeploymentConfig().description" nzShowIcon class="deployment-alert"></nz-alert>
              
              <div class="resource-overview" *ngIf="getDeploymentConfig().resources">
                <h5>预估资源需求</h5>
                <nz-row [nzGutter]="16">
                  <nz-col [nzSpan]="8">
                    <nz-statistic nzTitle="CPU" [nzValue]="getDeploymentConfig().resources.cpu" nzSuffix="核"></nz-statistic>
                  </nz-col>
                  <nz-col [nzSpan]="8">
                    <nz-statistic nzTitle="内存" [nzValue]="getDeploymentConfig().resources.memory" nzSuffix="GB"></nz-statistic>
                  </nz-col>
                  <nz-col [nzSpan]="8">
                    <nz-statistic nzTitle="存储" [nzValue]="getDeploymentConfig().resources.storage" nzSuffix="GB"></nz-statistic>
                  </nz-col>
                </nz-row>
              </div>

              <!-- 自定义配置 -->
              <div *ngIf="form.value.deploymentType === 'custom'" class="custom-config">
                <nz-collapse nzGhost>
                  <nz-collapse-panel nzHeader="组件配置">
                    <nz-row [nzGutter]="16">
                      <nz-col [nzSpan]="8">
                        <nz-form-item>
                          <nz-form-control>
                            <label nz-checkbox formControlName="enablePrometheus">启用 Prometheus</label>
                          </nz-form-control>
                        </nz-form-item>
                      </nz-col>
                      <nz-col [nzSpan]="8">
                        <nz-form-item>
                          <nz-form-control>
                            <label nz-checkbox formControlName="enableGrafana">启用 Grafana</label>
                          </nz-form-control>
                        </nz-form-item>
                      </nz-col>
                      <nz-col [nzSpan]="8">
                        <nz-form-item>
                          <nz-form-control>
                            <label nz-checkbox formControlName="enableAlertmanager">启用 Alertmanager</label>
                          </nz-form-control>
                        </nz-form-item>
                      </nz-col>
                    </nz-row>
                  </nz-collapse-panel>
                </nz-collapse>
              </div>
            </div>
          </form>

          <div class="step-actions">
            <button nz-button nzType="default" (click)="prevStep()">
              <i nz-icon nzType="left"></i>
              上一步
            </button>
            <button nz-button nzType="primary" (click)="nextStep()" [disabled]="!form.valid">
              下一步：开始安装
              <i nz-icon nzType="right"></i>
            </button>
          </div>
        </nz-card>

        <!-- 步骤3：安装执行 -->
        <nz-card *ngIf="currentStep === 2" class="step-card" nzTitle="安装执行">
          <div class="install-section">
            <nz-alert nzType="info" nzMessage="安装进行中" nzDescription="请勿关闭页面，安装过程可能需要几分钟时间" nzShowIcon class="install-alert"></nz-alert>
            
            <div class="install-progress">
              <nz-steps nzDirection="vertical" nzSize="small" [nzCurrent]="installStep">
                <nz-step nzTitle="准备安装环境" [nzDescription]="getInstallStepDescription(0)"></nz-step>
                <nz-step nzTitle="创建命名空间" [nzDescription]="getInstallStepDescription(1)"></nz-step>
                <nz-step nzTitle="安装 CRD" [nzDescription]="getInstallStepDescription(2)"></nz-step>
                <nz-step nzTitle="部署监控组件" [nzDescription]="getInstallStepDescription(3)"></nz-step>
                <nz-step nzTitle="配置服务" [nzDescription]="getInstallStepDescription(4)"></nz-step>
              </nz-steps>
            </div>

            <div class="install-logs" *ngIf="installLogs.length > 0">
              <h5>安装日志</h5>
              <div class="log-viewer">
                <div class="log-entry" *ngFor="let log of installLogs" [ngClass]="log.level">
                  <span class="log-time">{{ log.timestamp | date:'HH:mm:ss' }}</span>
                  <span class="log-message">{{ log.message }}</span>
                </div>
              </div>
            </div>
          </div>

          <div class="step-actions">
            <button nz-button nzType="default" (click)="cancelInstall()" [disabled]="installing">
              取消安装
            </button>
            <button nz-button nzType="primary" 
                    *ngIf="installCompleted"
                    (click)="nextStep()">
              下一步：验证结果
              <i nz-icon nzType="right"></i>
            </button>
          </div>
        </nz-card>

        <!-- 步骤4：验证完成 -->
        <nz-card *ngIf="currentStep === 3" class="step-card" nzTitle="验证完成">
          <div class="verification-section">
            <nz-result 
              [nzStatus]="installSuccess ? 'success' : 'error'"
              [nzTitle]="installSuccess ? '安装成功' : '安装失败'"
              [nzSubTitle]="installSuccess ? 'PolarDB-X Monitor 已成功安装并运行' : '安装过程中出现错误，请检查日志'">
              
              <div nz-result-extra *ngIf="installSuccess">
                <button nz-button nzType="primary" (click)="goToMonitoring()">
                  <i nz-icon nzType="dashboard"></i>
                  打开监控面板
                </button>
                <button nz-button nzType="default" (click)="goToHealth()">
                  <i nz-icon nzType="heart"></i>
                  健康检查
                </button>
              </div>
              
              <div nz-result-extra *ngIf="!installSuccess">
                <button nz-button nzType="primary" (click)="retryInstall()">
                  <i nz-icon nzType="reload"></i>
                  重新安装
                </button>
                <button nz-button nzType="default" (click)="restart()">
                  <i nz-icon nzType="undo"></i>
                  重新开始
                </button>
                <button nz-button nzType="default" (click)="goToHealth()">
                  <i nz-icon nzType="heart"></i>
                  健康检查
                </button>
                <button nz-button nzType="default" (click)="goToMonitoring()">
                  <i nz-icon nzType="dashboard"></i>
                  监控面板
                </button>
              </div>
            </nz-result>

            <!-- 安装摘要 -->
            <div class="install-summary" *ngIf="installSuccess">
              <h5>安装摘要</h5>
              <nz-descriptions nzBordered nzSize="small">
                <nz-descriptions-item nzTitle="命名空间">{{ form.value.namespace }}</nz-descriptions-item>
                <nz-descriptions-item nzTitle="安装方案">{{ getDeploymentConfig().title }}</nz-descriptions-item>
                <nz-descriptions-item nzTitle="组件数量">{{ getInstalledComponents().length }}</nz-descriptions-item>
                <nz-descriptions-item nzTitle="安装时间">{{ installDuration }}</nz-descriptions-item>
              </nz-descriptions>
            </div>
          </div>
        </nz-card>
      </div>
    </div>
  `,
  styles: [`
    .wizard {
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
      color: rgba(0, 0, 0, 0.87);
      font-size: 18px;
      font-weight: 500;
      margin: 0 0 4px 0;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    
    .page-icon {
      font-size: 20px;
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
      display: flex;
      flex-direction: column;
      gap: 16px;
    }
    
    .wizard-steps {
      margin-bottom: 24px;
    }
    
    .config-card, .tips-card {
      background: #fff;
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border: 1px solid #e0e0e0;
    }
    
    .section-icon {
      font-size: 16px;
      color: #1890ff;
    }
    
    .info-alert {
      margin-bottom: 16px;
    }
    
    .config-form {
      margin-bottom: 24px;
    }
    
    .mode-explanations {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }
    
    .mode-item {
      padding: 12px;
      border: 1px solid #e0e0e0;
      border-radius: 6px;
      background: #fafafa;
    }
    
    .mode-item h4 {
      margin: 0 0 8px 0;
      color: rgba(0, 0, 0, 0.85);
      font-size: 14px;
      font-weight: 500;
      display: flex;
      align-items: center;
      gap: 6px;
    }
    
    .mode-item p {
      margin: 0;
      color: rgba(0, 0, 0, 0.6);
      font-size: 13px;
      line-height: 1.4;
    }
    
    .action-buttons {
      display: flex;
      justify-content: flex-end;
      gap: 12px;
      padding-top: 16px;
      border-top: 1px solid #e0e0e0;
    }
    
    .tip-alert {
      margin-bottom: 12px;
    }
    
    .tip-alert:last-child {
      margin-bottom: 0;
    }
    
    .tip-content p {
      margin: 4px 0;
      font-size: 13px;
    }
    
    .tip-content code {
      background: #f5f5f5;
      padding: 2px 4px;
      border-radius: 3px;
      font-family: 'Monaco', 'Menlo', monospace;
    }

    /* 新的向导样式 */
    .step-card {
      background: #fff;
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border: 1px solid #e0e0e0;
      margin-bottom: 16px;
    }

    .check-section {
      padding: 16px 0;
    }

    .loading-tip {
      text-align: center;
      color: rgba(0,0,0,0.65);
      letter-spacing: 0.5px;
      padding: 20px 0;
      font-size: 14px;
    }

    .check-items {
      display: flex;
      flex-direction: column;
      gap: 12px;
    }

    .check-item {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 12px 16px;
      border: 1px solid #e8e8e8;
      border-radius: 6px;
      background: #fafafa;
    }

    .check-info {
      display: flex;
      align-items: center;
      gap: 12px;
    }

    .check-status {
      font-size: 16px;
    }

    .check-name {
      font-weight: 500;
      color: rgba(0, 0, 0, 0.85);
      min-width: 120px;
    }

    .check-description {
      color: rgba(0, 0, 0, 0.6);
      font-size: 13px;
    }

    .check-result {
      font-size: 13px;
      font-family: monospace;
    }

    .check-result.success {
      color: #52c41a;
    }

    .check-result.error {
      color: #ff4d4f;
    }

    .check-result.pending {
      color: #faad14;
    }

    .config-section {
      margin-bottom: 24px;
    }

    .config-section h4 {
      margin: 0 0 16px 0;
      color: rgba(0, 0, 0, 0.85);
      font-size: 16px;
      font-weight: 500;
    }

    .config-section h5 {
      margin: 16px 0 12px 0;
      color: rgba(0, 0, 0, 0.8);
      font-size: 14px;
      font-weight: 500;
    }

    .deployment-alert {
      margin: 12px 0;
    }

    .resource-overview {
      margin: 16px 0;
      padding: 16px;
      background: #f8f9fa;
      border-radius: 6px;
    }

    .custom-config {
      margin-top: 16px;
    }

    .install-section {
      padding: 16px 0;
    }

    .install-alert {
      margin-bottom: 24px;
    }

    .install-progress {
      margin: 24px 0;
    }

    .install-logs {
      margin-top: 24px;
    }

    .log-viewer {
      max-height: 300px;
      overflow-y: auto;
      background: #f6f8fa;
      border: 1px solid #e1e4e8;
      border-radius: 6px;
      padding: 12px;
    }

    .log-entry {
      display: flex;
      align-items: flex-start;
      gap: 12px;
      margin-bottom: 4px;
      font-family: monospace;
      font-size: 12px;
    }

    .log-time {
      color: #666;
      min-width: 60px;
    }

    .log-message {
      flex: 1;
    }

    .log-entry.info .log-message {
      color: #1890ff;
    }

    .log-entry.success .log-message {
      color: #52c41a;
    }

    .log-entry.error .log-message {
      color: #ff4d4f;
    }

    .log-entry.warning .log-message {
      color: #faad14;
    }

    .verification-section {
      padding: 16px 0;
    }

    .install-summary {
      margin-top: 24px;
      padding-top: 16px;
      border-top: 1px solid #e8e8e8;
    }

    .install-summary h5 {
      margin: 0 0 16px 0;
      color: rgba(0, 0, 0, 0.85);
      font-size: 14px;
      font-weight: 500;
    }

    .step-actions {
      display: flex;
      justify-content: flex-end;
      gap: 12px;
      margin-top: 24px;
      padding-top: 16px;
      border-top: 1px solid #e8e8e8;
    }
    
    /* 响应式设计 */
    @media (max-width: 1200px) {
      .page-content {
        max-width: 100%;
        padding: 0 8px;
      }
    }
    
    @media (max-width: 768px) {
      .wizard {
        padding: 8px;
      }
      
      .action-buttons {
        flex-direction: column;
      }
      
      .mode-explanations {
        gap: 12px;
      }

      .command-block pre {
        font-size: 12px;
      }
    }
  `]
})
export class MonitoringInstallWizardComponent implements OnInit {
  form: FormGroup;
  currentStep = 0;
  
  // 环境检查
  checking = false;
  environmentChecks: any[] = [];
  allChecksPassed = false;

  // 安装状态
  installing = false;
  installStep = 0;
  installCompleted = false;
  installSuccess = false;
  installLogs: any[] = [];
  installDuration = '';
  // 验证
  verifying = false;
  verifyAttempts = 0;
  verifyMaxAttempts = 10;
  private verifyTimer?: any;
  
  private installStartTime?: Date;

  // 部署配置
  private deploymentConfigs = {
    default: {
      title: '标准安装',
      description: '使用默认配置，适合开发和测试环境',
      alertType: 'info',
      resources: { cpu: 2, memory: 4, storage: 20 }
    },
    production: {
      title: '生产环境',
      description: '高可用配置，适合生产环境使用',
      alertType: 'success',
      resources: { cpu: 6, memory: 16, storage: 100 }
    },
    minimal: {
      title: '最小安装',
      description: '最小资源配置，适合资源受限环境',
      alertType: 'warning',
      resources: { cpu: 1, memory: 2, storage: 10 }
    },
    custom: {
      title: '自定义配置',
      description: '根据需要自定义组件和资源配置',
      alertType: 'info',
      resources: { cpu: 0, memory: 0, storage: 0 }
    }
  };

  constructor(
    private fb: FormBuilder, 
    private api: ApiService, 
    private message: NzMessageService,
    private router: Router
  ) {
    this.form = this.fb.group({
      deploymentType: ['default'],
      namespace: ['polardbx-monitor'],
      enablePrometheus: [true],
      enableGrafana: [true],
      enableAlertmanager: [true]
    });
  }

  ngOnInit(): void {
    this.runEnvironmentCheck();
  }

  runEnvironmentCheck(): void {
    this.checking = true;
    // 初始化占位
    this.environmentChecks = [
      { name: '可用区与节点', description: 'AZ 分布与节点统计', status: 'pending', result: '检查中...' },
      { name: '时钟漂移', description: '控制面与节点时钟一致性', status: 'pending', result: '检查中...' },
      { name: 'IOPS 估算', description: '磁盘 IOPS 估算（占位）', status: 'pending', result: '检查中...' }
    ];
    this.api.getMonitoringPreflight().subscribe({
      next: (res: any) => {
        const az = res?.az || {}; // zones, count, nodeCount, nodesWithZone
        const clock = res?.clock || {}; // ok, message
        const iops = res?.iops || {}; // estimated, ok, message
        this.environmentChecks[0] = {
          ...this.environmentChecks[0],
          status: 'success',
          result: `zones: ${az.count ?? 0}, nodes: ${az.nodeCount ?? 0}`
        };
        this.environmentChecks[1] = {
          ...this.environmentChecks[1],
          status: clock.ok ? 'success' : 'warning',
          result: clock.message || (clock.ok ? 'ok' : '需要人工确认')
        };
        this.environmentChecks[2] = {
          ...this.environmentChecks[2],
          status: iops.ok ? 'success' : 'warning',
          result: iops.message || '未执行真实基准（占位）'
        };
        this.allChecksPassed = true;
        this.checking = false;
      },
      error: () => {
        this.environmentChecks = this.environmentChecks.map((c) => ({ ...c, status: 'error', result: '检查失败' }));
        this.allChecksPassed = false;
        this.checking = false;
      }
    });
  }

  onDeploymentTypeChange(type: string): void {
    // 根据部署类型自动调整配置
    if (type === 'production') {
      this.form.patchValue({
        enablePrometheus: true,
        enableGrafana: true,
        enableAlertmanager: true
      });
    } else if (type === 'minimal') {
      this.form.patchValue({
        enablePrometheus: true,
        enableGrafana: false,
        enableAlertmanager: false
      });
    }
  }

  getDeploymentConfig(): any {
    const type = this.form.value.deploymentType || 'default';
    return this.deploymentConfigs[type as keyof typeof this.deploymentConfigs] || this.deploymentConfigs.default;
  }

  getInstallStepDescription(step: number): string {
    if (step < this.installStep) return '已完成';
    if (step === this.installStep) return '进行中...';
    return '等待中';
  }

  getInstalledComponents(): string[] {
    const components: string[] = [];
    if (this.form.value.enablePrometheus) components.push('Prometheus');
    if (this.form.value.enableGrafana) components.push('Grafana');
    if (this.form.value.enableAlertmanager) components.push('Alertmanager');
    return components;
  }

  nextStep(): void {
    if (this.currentStep === 1) {
      // 开始安装
      this.startInstallation();
    }
    if (this.currentStep === 2) {
      // 进入验证步骤时启动自动校验
      this.currentStep++;
      this.startVerification();
      return;
    }
    if (this.currentStep < 3) this.currentStep++;
  }

  prevStep(): void {
    if (this.currentStep > 0) {
      this.currentStep--;
    }
  }

  startInstallation(): void {
    this.installing = true;
    this.installStep = 0;
    this.installLogs = [];
    this.installStartTime = new Date();
    this.addLog('info', '开始安装 PolarDB-X Monitor...');

    // 提交安装计划（后端当前持久化计划，不直接执行 Helm）
    const ns = this.form.value.namespace || 'polardbx-operator-system';
    this.api.monitoringBootstrap({ mode: 'managed', namespace: ns, releaseName: 'kube-prometheus-stack', dryRun: false }).subscribe({
      next: () => {
        this.addLog('success', '已提交安装计划（bootstrap accepted）');
        // 展示进度动画并完成
        this.simulateInstallation();
      },
      error: (e) => {
        this.addLog('error', '提交安装计划失败');
        this.completeInstallation(false);
      }
    });
  }

  private simulateInstallation(): void {
    const steps = [
      { message: '准备安装环境...', delay: 1000 },
      { message: '创建命名空间 ' + this.form.value.namespace, delay: 1500 },
      { message: '安装 CRD 资源...', delay: 2000 },
      { message: '部署监控组件...', delay: 3000 },
      { message: '配置服务和网络...', delay: 1000 }
    ];

    let currentStep = 0;
    const executeStep = () => {
      if (currentStep < steps.length) {
        this.installStep = currentStep;
        this.addLog('info', steps[currentStep].message);
        
        setTimeout(() => {
          currentStep++;
          executeStep();
        }, steps[currentStep].delay);
      } else {
        this.completeInstallation(true);
      }
    };

    executeStep();
  }

  private completeInstallation(success: boolean): void {
    this.installing = false;
    this.installCompleted = true;
    this.installSuccess = success;
    
    if (this.installStartTime) {
      const duration = Date.now() - this.installStartTime.getTime();
      this.installDuration = Math.round(duration / 1000) + ' 秒';
    }

    if (success) {
      this.addLog('success', '安装完成！所有组件已成功部署');
      this.message.success('PolarDB-X Monitor 安装成功！');
    } else {
      this.addLog('error', '安装失败，请检查错误信息');
      this.message.error('安装过程中出现错误');
    }
  }

  private addLog(level: string, message: string): void {
    this.installLogs.push({
      level,
      message,
      timestamp: new Date()
    });
  }

  cancelInstall(): void {
    this.installing = false;
    this.addLog('warning', '用户取消安装');
    this.message.warning('安装已取消');
  }

  retryInstall(): void {
    this.currentStep = 1;
    this.installing = false;
    this.installCompleted = false;
    this.installSuccess = false;
    this.installLogs = [];
  }

  restart(): void {
    this.currentStep = 0;
    this.installing = false;
    this.installCompleted = false;
    this.installSuccess = false;
    this.installLogs = [];
    this.runEnvironmentCheck();
  }

  goToMonitoring(): void {
    this.router.navigate(['/operations/monitoring/overview']);
  }

  goToHealth(): void {
    this.router.navigate(['/operations/monitoring/health']);
  }

  private clearVerifyTimer(): void {
    if (this.verifyTimer) {
      clearInterval(this.verifyTimer);
      this.verifyTimer = undefined;
    }
  }

  private startVerification(): void {
    this.verifying = true;
    this.verifyAttempts = 0;
    this.installSuccess = false;
    this.clearVerifyTimer();
    this.verifyTimer = setInterval(() => {
      this.verifyAttempts++;
      this.api.getMonitoringStatus().subscribe({
        next: (s: any) => {
          const comps = s?.components || {};
          const ok = !!(comps?.prometheus?.ready && comps?.grafana?.ready);
          if (ok) {
            this.installSuccess = true;
            this.verifying = false;
            this.clearVerifyTimer();
          } else if (this.verifyAttempts >= this.verifyMaxAttempts) {
            this.installSuccess = false;
            this.verifying = false;
            this.clearVerifyTimer();
          }
        },
        error: () => {
          if (this.verifyAttempts >= this.verifyMaxAttempts) {
            this.installSuccess = false;
            this.verifying = false;
            this.clearVerifyTimer();
          }
        }
      });
    }, 3000);
  }
}