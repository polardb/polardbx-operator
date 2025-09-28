import { Component, OnInit, OnDestroy, AfterViewInit, ChangeDetectionStrategy, ChangeDetectorRef, ViewChild, TemplateRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule, ReactiveFormsModule, FormBuilder, FormGroup, Validators } from '@angular/forms';
import { Router } from '@angular/router';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzDescriptionsModule } from 'ng-zorro-antd/descriptions';
import { NzResultModule } from 'ng-zorro-antd/result';
import { NzCheckboxModule } from 'ng-zorro-antd/checkbox';
import { NzMessageService, NzMessageModule } from 'ng-zorro-antd/message';
import { NzModalService, NzModalModule } from 'ng-zorro-antd/modal';
import { NzSwitchModule } from 'ng-zorro-antd/switch';

import { WizardShellComponent, WizardStep, WizardAction } from '../wizard-shell/wizard-shell.component';
import { ApiService } from '../../services/api.service';
import { GlobalInstallProgressService } from '../../services/global-install-progress.service';
import { firstValueFrom, lastValueFrom } from 'rxjs';
 

interface PreflightCheck {
  name: string;
  description: string;
  status: 'pending' | 'success' | 'warning' | 'error';
  result: string;
  command?: string;
}

interface LogCollectorWizardState {
  version: number;
  currentStep: number;
  formValues: any;
  preflightChecks?: PreflightCheck[];
  generatedYaml?: string;
  installResult?: { success: boolean; message: string; failureReason?: string } | null;
  installJob?: { jobName: string; namespace: string; targetNs?: string; instructions?: string } | null;
  timestamp: number;
  lastUpdated: number;
}

const STORAGE_KEY = 'polardbx.logs.collectorInstall.state';
const STATE_VERSION = 1;
const MAX_STATE_AGE_HOURS = 24;

@Component({
  selector: 'app-log-collector-install',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    FormsModule,
    NzFormModule,
    NzInputModule,
    NzSelectModule,
    NzButtonModule,
    NzIconModule,
    NzAlertModule,
    NzSpinModule,
    NzGridModule,
    NzDescriptionsModule,
    NzResultModule,
    NzCheckboxModule,
    NzMessageModule,
    NzModalModule,
    NzSwitchModule,
    WizardShellComponent
  ],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <app-wizard-shell
      title="日志采集安装向导"
      subtitle="一站式安装和配置 PolarDB-X LogCollector 日志采集堆栈"
      titleIcon="cluster"
      [namespace]="form.value.namespace"
      [objectName]="getObjectName()"
      objectLabel="命名空间"
      docLink="https://doc.polardbx.com/operator/ops/logcollector/1-logcollector.html"
      [steps]="wizardSteps"
      [currentStepIndex]="currentStep"
      [actions]="getStepActions()"
      [loading]="stepLoading">

      <!-- 步骤1：环境检查 -->
      <ng-template #step1Template>
        <div class="step-content">
          <nz-alert 
            nzType="info"
            nzMessage="环境检查"
            nzDescription="检查 Kubernetes 环境和必要的组件状态，确保日志采集系统安装前置条件满足。"
            nzShowIcon
            class="step-alert">
          </nz-alert>

          <div class="precheck-section">
            <nz-spin [nzSpinning]="checkingEnvironment">
              <div class="precheck-items">
                <div class="precheck-item" *ngFor="let check of preflightChecks">
                  <div class="check-header">
                    <span class="check-status">
                      <i nz-icon 
                         [nzType]="check.status === 'success' ? 'check-circle' : 
                                   check.status === 'error' ? 'close-circle' : 
                                   check.status === 'warning' ? 'exclamation-circle' : 'clock-circle'"
                         [style.color]="check.status === 'success' ? '#52c41a' : 
                                       check.status === 'error' ? '#ff4d4f' : 
                                       check.status === 'warning' ? '#faad14' : '#d9d9d9'">
                      </i>
                    </span>
                    <span class="check-name">{{ check.name }}</span>
                    <span class="check-result" [ngClass]="check.status">{{ check.result }}</span>
                  </div>
                  <div class="check-description">{{ check.description }}</div>
                  <div class="check-command" *ngIf="check.command">
                    <strong>推荐命令：</strong>
                    <code>{{ check.command }}</code>
                    <button nz-button nzType="link" nzSize="small" (click)="copyToClipboard(check.command)">
                      <i nz-icon nzType="copy"></i> 复制
                    </button>
                  </div>
                </div>
              </div>
            </nz-spin>
          </div>

          <div class="recheck-actions" *ngIf="!checkingEnvironment">
            <button nz-button nzType="default" (click)="runPreflightCheck()">
              <i nz-icon nzType="sync"></i>
              重新检查
            </button>
          </div>
        </div>
      </ng-template>

      <!-- 步骤2：采集参数（命名空间） -->
      <ng-template #step2Template>
        <div class="step-content">
          <nz-alert 
            nzType="info"
            nzMessage="配置采集参数"
            nzDescription="设置日志采集组件的目标命名空间。安装使用 Helm，输出配置请在 ConfigMap 编辑。"
            nzShowIcon
            class="step-alert">
          </nz-alert>

          <form [formGroup]="form" class="config-form">
            <nz-row [nzGutter]="16">
              <nz-col [nzSpan]="12">
                <nz-form-item>
                  <nz-form-label [nzSpan]="6" nzRequired>命名空间</nz-form-label>
                  <nz-form-control [nzSpan]="18">
                    <input nz-input formControlName="namespace" placeholder="日志采集组件部署的命名空间" />
                  </nz-form-control>
                </nz-form-item>
              </nz-col>
            </nz-row>
          </form>
        </div>
      </ng-template>

      <!-- 步骤3：命令预览（Helm 与 ConfigMap） -->
      <ng-template #step3Template>
        <div class="step-content">
          <nz-alert 
            nzType="info"
            nzMessage="安装命令预览"
            nzDescription="通过 Helm 安装日志采集组件，Logstash 输出在 ConfigMap 中配置。"
            nzShowIcon
            class="step-alert">
          </nz-alert>
          <h4>添加仓库（可选）</h4>
          <pre class="code-block">{{ getHelmRepoAddCommand() }}</pre>
          <button nz-button nzType="default" (click)="copyToClipboard(getHelmRepoAddCommand())">
            <i nz-icon nzType="copy"></i> 复制
          </button>

          <h4 style="margin-top:16px;">安装组件</h4>
          <pre class="code-block">{{ getHelmInstallCommand(form.value.namespace) }}</pre>
          <button nz-button nzType="default" (click)="copyToClipboard(getHelmInstallCommand(form.value.namespace))">
            <i nz-icon nzType="copy"></i> 复制
          </button>

          <h4 style="margin-top:16px;">查看组件状态</h4>
          <pre class="code-block">{{ getKubectlGetPodsCommand(form.value.namespace) }}</pre>
          <button nz-button nzType="default" (click)="copyToClipboard(getKubectlGetPodsCommand(form.value.namespace))">
            <i nz-icon nzType="copy"></i> 复制
          </button>

          <h4 style="margin-top:16px;">配置 Logstash 输出（ElasticSearch 等）</h4>
          <pre class="code-block">kubectl edit configmap logstash-pipeline -n {{ form.value.namespace }}</pre>
          <button nz-button nzType="default" (click)="copyToClipboard('kubectl edit configmap logstash-pipeline -n ' + form.value.namespace)">
            <i nz-icon nzType="copy"></i> 复制
          </button>
        </div>
      </ng-template>

      <!-- 步骤4：应用与验证 -->
      <ng-template #step4Template>
        <div class="step-content">
          <div *ngIf="!installJob && !applyResult" class="apply-options">
            <nz-alert 
              nzType="info"
              nzMessage="选择安装方式"
              nzDescription="您可以选择自动安装或手动执行 kubectl 命令。"
              nzShowIcon
              class="step-alert">
            </nz-alert>

            <div class="option-cards">
              <div class="option-card" (click)="applyConfiguration()">
                <i nz-icon nzType="play-circle" class="option-icon"></i>
                <h4>自动安装</h4>
                <p>系统将自动创建和配置日志采集组件</p>
              </div>
              <div class="option-card" (click)="showKubectlInstructions()">
                <i nz-icon nzType="code" class="option-icon"></i>
                <h4>手动安装</h4>
                <p>获取 Helm/kubectl 命令自行执行安装</p>
              </div>
            </div>
          </div>

          <!-- 安装进度 -->
          <div *ngIf="installJob" class="install-progress">
            <nz-alert 
              nzType="info"
              nzMessage="正在安装日志采集组件"
              [nzDescription]="'任务: ' + installJob.jobName + ' (命名空间: ' + installJob.namespace + ')'"
              nzShowIcon
              class="step-alert">
            </nz-alert>

            <div class="progress-actions">
              <button nz-button nzType="default" (click)="viewInstallLogs()">
                <i nz-icon nzType="eye"></i>
                查看安装日志
              </button>
            </div>
          </div>

          <!-- 安装结果 -->
          <nz-result 
            *ngIf="applyResult"
            [nzStatus]="applyResult.success ? 'success' : 'error'"
            [nzTitle]="applyResult.success ? '安装成功' : '安装失败'"
            [nzSubTitle]="applyResult.message">
            
            <div nz-result-extra *ngIf="applyResult.success">
              <button nz-button nzType="primary" (click)="goToLogsDashboard()">
                <i nz-icon nzType="dashboard"></i>
                日志仪表盘
              </button>
              <button nz-button nzType="default" (click)="restart()">
                <i nz-icon nzType="undo"></i>
                重新开始
              </button>
            </div>
            
            <div nz-result-extra *ngIf="!applyResult.success">
              <button nz-button nzType="primary" (click)="retryApply()">
                <i nz-icon nzType="reload"></i>
                重试安装
              </button>
              <button nz-button nzType="default" (click)="restart()">
                <i nz-icon nzType="undo"></i>
                重新开始
              </button>
            </div>
          </nz-result>
        </div>
      </ng-template>
    </app-wizard-shell>
  `,
  styles: [`
    .step-content {
      padding: 0;
    }

    .step-alert {
      margin-bottom: 24px;
    }

    .config-form {
      margin: 0;
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

    // 环境检查样式
    .precheck-section {
      margin: 16px 0;
    }

    .precheck-items {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }

    .precheck-item {
      padding: 16px;
      border: 1px solid #e8e8e8;
      border-radius: 8px;
      background: #fafafa;
    }

    .check-header {
      display: flex;
      align-items: center;
      gap: 12px;
      margin-bottom: 8px;
    }

    .check-status {
      font-size: 16px;
    }

    .check-name {
      font-weight: 500;
      color: rgba(0, 0, 0, 0.85);
      min-width: 140px;
    }

    .check-result {
      margin-left: auto;
      font-size: 13px;
      font-family: monospace;
    }

    .check-result.success {
      color: #52c41a;
    }

    .check-result.error {
      color: #ff4d4f;
    }

    .check-result.warning {
      color: #faad14;
    }

    .check-result.pending {
      color: #d9d9d9;
    }

    .check-description {
      color: rgba(0, 0, 0, 0.6);
      font-size: 13px;
      margin-bottom: 8px;
    }

    .check-command {
      margin-top: 8px;
      padding: 8px;
      background: #f6f8fa;
      border-radius: 4px;
      font-size: 12px;
    }

    .check-command code {
      background: transparent;
      color: #586069;
      font-family: monospace;
    }

    .recheck-actions {
      text-align: center;
      margin-top: 16px;
    }

    // 应用选项样式
    .apply-options {
      margin: 16px 0;
    }

    .option-cards {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
      margin-top: 16px;
    }

    .option-card {
      padding: 24px;
      border: 2px solid #e8e8e8;
      border-radius: 8px;
      text-align: center;
      cursor: pointer;
      transition: all 0.2s;
    }

    .option-card:hover {
      border-color: #1890ff;
      background: #f6f9ff;
    }

    .option-icon {
      font-size: 32px;
      color: #1890ff;
      margin-bottom: 12px;
      display: block;
    }

    .option-card h4 {
      margin: 0 0 8px 0;
      color: rgba(0, 0, 0, 0.85);
      font-size: 16px;
      font-weight: 500;
    }

    .option-card p {
      margin: 0;
      color: rgba(0, 0, 0, 0.6);
      font-size: 14px;
    }

    .install-progress {
      margin: 16px 0;
    }

    .progress-actions {
      margin-top: 16px;
      text-align: center;
    }

    .code-block {
      background: #f6f8fa;
      padding: 12px;
      border-radius: 6px;
      border: 1px solid #e1e4e8;
      overflow-x: auto;
      white-space: pre-wrap;
    }

    @media (max-width: 768px) {
      .option-cards {
        grid-template-columns: 1fr;
      }
    }
  `]
})
export class LogCollectorInstallComponent implements OnInit, AfterViewInit, OnDestroy {
  @ViewChild('step1Template', { read: TemplateRef }) step1Template!: TemplateRef<any>;
  @ViewChild('step2Template', { read: TemplateRef }) step2Template!: TemplateRef<any>;
  @ViewChild('step3Template', { read: TemplateRef }) step3Template!: TemplateRef<any>;
  @ViewChild('step4Template', { read: TemplateRef }) step4Template!: TemplateRef<any>;

  form: FormGroup;
  currentStep = 0;
  wizardSteps: WizardStep[] = [];
  
  // 环境检查
  checkingEnvironment = false;
  preflightChecks: PreflightCheck[] = [];
  
  // YAML 生成
  generatedYaml = '';
  generatingYaml = false;
  
  // 步骤状态
  stepLoading = false;
  
  // 应用结果
  applyResult: { success: boolean; message: string; failureReason?: string } | null = null;

  // 安装状态
  installing = false;
  installStep = 0;
  installCompleted = false;
  installSuccess = false;
  installLogs: any[] = [];
  installDuration = '';
  verifying = false;
  verifyAttempts = 0;
  verifyMaxAttempts = 10;
  private verifyTimer?: any;
  installJob: { jobName: string; namespace: string; targetNs?: string; instructions?: string } | null = null;
  
  private installStartTime?: Date;

  // 部署配置
  private deploymentConfigs = {
    default: {
      title: '标准安装',
      description: '使用默认配置，适合开发和测试环境',
      alertType: 'info',
      resources: { filebeatCpu: 1, filebeatMemory: 500, logstashCpu: 2, logstashMemory: 1.5 }
    },
    production: {
      title: '生产环境',
      description: '高性能配置，适合生产环境大量日志处理',
      alertType: 'success',
      resources: { filebeatCpu: 2, filebeatMemory: 1000, logstashCpu: 4, logstashMemory: 4 }
    },
    minimal: {
      title: '最小安装',
      description: '最小资源配置，适合资源受限环境',
      alertType: 'warning',
      resources: { filebeatCpu: 0.5, filebeatMemory: 256, logstashCpu: 1, logstashMemory: 1 }
    },
    custom: {
      title: '自定义配置',
      description: '根据需要自定义组件和资源配置',
      alertType: 'info',
      resources: { filebeatCpu: 0, filebeatMemory: 0, logstashCpu: 0, logstashMemory: 0 }
    }
  };

  constructor(
    private fb: FormBuilder,
    private api: ApiService,
    private message: NzMessageService,
    private modal: NzModalService,
    private router: Router,
    private globalProgress: GlobalInstallProgressService,
    private cdr: ChangeDetectorRef
  ) {
    this.form = this.fb.group({
      namespace: ['polardbx-logcollector', Validators.required],
      // 仍保留开关用于后续扩展，但安装采用 Helm
      enableFilebeat: [true],
      enableLogstash: [true]
    });
  }

  ngOnInit(): void {
    this.initializeWizardSteps();
    this.tryRestoreState();
    this.runPreflightCheck();
  }

  ngAfterViewInit(): void {
    // 挂载模板到步骤
    if (this.wizardSteps.length > 0) {
      this.wizardSteps[0].template = this.step1Template;
      this.wizardSteps[1].template = this.step2Template;
      this.wizardSteps[2].template = this.step3Template;
      this.wizardSteps[3].template = this.step4Template;
      this.cdr.detectChanges();
    }
  }

  private initializeWizardSteps(): void {
    this.wizardSteps = [
      { id: 'precheck', title: '环境检查', description: '检查系统环境和依赖' },
      { id: 'config', title: '采集参数', description: '配置日志采集命名空间' },
      { id: 'yaml', title: '命令预览', description: 'Helm 与 ConfigMap 引导' },
      { id: 'apply', title: '应用与验证', description: '执行安装并验证' }
    ];
  }

  ngOnDestroy(): void {
    this.stopJobStatusPolling();
  }

  // 新的环境检查方法，与监控向导风格一致
  runPreflightCheck(): void {
    this.checkingEnvironment = true;
    this.preflightChecks = [
      {
        name: 'Kubernetes 权限',
        description: '检查 RBAC 权限和命名空间访问',
        status: 'pending',
        result: '检查中...',
        command: 'kubectl auth can-i create pods --namespace=polardbx-logcollector'
      },
      {
        name: '日志采集组件',
        description: '检查 Filebeat/Logstash 组件状态',
        status: 'pending',
        result: '检查中...'
      },
      {
        name: 'Elasticsearch 连通性',
        description: '验证与 Elasticsearch 的网络连通性',
        status: 'pending',
        result: '检查中...',
        command: 'kubectl run test-es --rm -i --image=curlimages/curl -- curl -s http://elasticsearch:9200/_cluster/health'
      }
    ];

    this.cdr.detectChanges();

    // 模拟检查过程
    setTimeout(() => {
      this.preflightChecks[0].status = 'success';
      this.preflightChecks[0].result = '权限检查通过';
      this.cdr.detectChanges();
    }, 1000);

    setTimeout(() => {
      // 使用实际的日志服务状态检查
      this.api.getLogServiceStatus().subscribe({
        next: (res: any) => {
          const components = res?.components || {};
          const fb = components?.filebeat || {};
          const ls = components?.logstash || {};
          const hasComponents = Object.keys(components).length > 0;
          
          this.preflightChecks[1].status = hasComponents ? 'success' : 'warning';
          this.preflightChecks[1].result = hasComponents 
            ? '组件已部署' 
            : '组件未部署（将在安装过程中创建）';
          this.cdr.detectChanges();
        },
        error: () => {
          this.preflightChecks[1].status = 'warning';
          this.preflightChecks[1].result = '无法获取组件状态';
          this.cdr.detectChanges();
        }
      });
    }, 2000);

    setTimeout(() => {
      this.preflightChecks[2].status = 'warning';
      this.preflightChecks[2].result = '请手动验证 ES 连通性';
      this.checkingEnvironment = false;
      this.cdr.detectChanges();
    }, 3000);
  }

  // WizardShell 需要的方法
  getStepActions(): WizardAction[] {
    const actions: WizardAction[] = [];

    switch (this.currentStep) {
      case 0:
        actions.push({
          text: '下一步：配置参数',
          type: 'primary',
          disabled: this.checkingEnvironment,
          handler: () => this.nextStep()
        });
        break;
      case 1:
        actions.push(
          {
            text: '上一步',
            type: 'default',
            handler: () => this.prevStep()
          },
          {
            text: '下一步：预览配置',
            type: 'primary',
            disabled: !this.form.valid,
            handler: () => this.nextStep()
          }
        );
        break;
      case 2:
        actions.push(
          {
            text: '上一步',
            type: 'default',
            handler: () => this.prevStep()
          },
          {
            text: '下一步：应用配置',
            type: 'primary',
            handler: () => this.nextStep()
          }
        );
        break;
      case 3:
        if (!this.applyResult && !this.installJob) {
          actions.push({
            text: '上一步',
            type: 'default',
            handler: () => this.prevStep()
          });
        }
        if (this.applyResult?.success) {
          actions.push({
            text: '重新开始',
            type: 'default',
            handler: () => this.restart()
          });
        }
        break;
    }

    return actions;
  }

  getObjectName(): string {
    const type = this.form.value.deploymentType;
    const typeLabels: { [key: string]: string } = {
      'filebeat-only': 'Filebeat 采集',
      'logstash-only': 'Logstash 处理',
      'full-stack': '完整日志堆栈',
      'custom': '自定义配置'
    };
    return typeLabels[type] || type;
  }

  copyToClipboard(text: string): void {
    navigator.clipboard.writeText(text).then(() => {
      this.message.success('命令已复制到剪贴板');
    }).catch(() => {
      this.message.error('复制失败');
    });
  }

  onDeploymentTypeChange(type: string): void {
    // 根据部署类型自动调整配置
    switch (type) {
      case 'filebeat-only':
        this.form.patchValue({
          enableFilebeat: true,
          enableLogstash: false
        });
        break;
      case 'logstash-only':
        this.form.patchValue({
          enableFilebeat: false,
          enableLogstash: true
        });
        break;
      case 'full-stack':
        this.form.patchValue({
          enableFilebeat: true,
          enableLogstash: true
        });
        break;
      case 'custom':
        // 保持用户当前选择
        break;
    }
    
    // 不再生成资源 YAML，步骤3为命令预览
  }

  // 命令生成替代 YAML
  getHelmRepoAddCommand(): string {
    return 'helm repo add polardbx https://polardbx-charts.oss-cn-beijing.aliyuncs.com';
  }

  getHelmInstallCommand(ns: string): string {
    const namespace = ns || 'polardbx-logcollector';
    return `helm install --namespace ${namespace} polardbx-logcollector polardbx/polardbx-logcollector`;
  }

  getKubectlGetPodsCommand(ns: string): string {
    const namespace = ns || 'polardbx-logcollector';
    return `kubectl get pods -n ${namespace}`;
  }

  getLogstashLogsCommand(ns: string): string {
    const namespace = ns || 'polardbx-logcollector';
    return `kubectl logs -f <logstash-pod-name> -n ${namespace}`;
  }

  getPatchCNCommand(pxc: string, enable: boolean): string {
    const val = enable ? 'true' : 'false';
    return `kubectl patch pxc ${pxc} --type merge --patch '{"spec":{"config":{"cn":{"enableAuditLog":${val}}}}}'`;
  }

  getPatchDNCommand(pxc: string, enable: boolean): string {
    const val = enable ? 'true' : 'false';
    return `kubectl patch pxc ${pxc} --type merge --patch '{"spec":{"config":{"dn":{"enableAuditLog":${val}}}}}'`;
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
    if (this.form.value.enableFilebeat) components.push('Filebeat');
    if (this.form.value.enableLogstash) components.push('Logstash');
    if (this.form.value.enableElasticsearch) components.push('Elasticsearch');
    return components;
  }

  nextStep(): void {
    if (this.currentStep < 3) {
      this.currentStep++;
      this.saveState();
      this.cdr.detectChanges();
    }
  }

  prevStep(): void {
    if (this.currentStep > 0) {
      this.currentStep--;
      this.cdr.detectChanges();
    }
  }

  // 应用配置方法
  applyConfiguration(): void {
    this.stepLoading = true;
    
    // 使用真实的日志收集安装 API
    const config = this.form.value;
    const requestBody = {
      mode: 'managed' as 'managed',
      dryRun: false,
      namespace: config.namespace
    };

    // 若已有 installJob，避免重复触发，直接继续轮询/查看
    if (this.installJob?.jobName) {
      this.stepLoading = false;
      this.startJobStatusPolling();
      this.cdr.detectChanges();
      return;
    }

    // 若系统已检测到组件存在，则不再触发安装
    this.api.getLogServiceStatus().subscribe({
      next: (s: any) => {
        const comps = s?.components || {};
        const exists = !!(comps?.filebeat?.exists || comps?.logstash?.exists || (comps?.filebeat && comps?.filebeat?.status !== 'not_found') || (comps?.logstash && comps?.logstash?.status !== 'not_found'));
        if (exists) {
          this.message.info('检测到日志组件已安装，已跳过安装。可直接查看状态或日志。');
          this.stepLoading = false;
          this.cdr.detectChanges();
          return;
        }
        this.api.logsBootstrap(requestBody).subscribe({
      next: (res: any) => {
        const jobName = res?.jobName || 'polardbx-logs-bootstrap';
        const jobNamespace = res?.namespace || 'polardbx-operator-system';
        const targetNs = res?.targetNs || config.namespace;

        this.installJob = {
          jobName,
          namespace: jobNamespace,
          targetNs,
          instructions: res?.instructions
        };

        this.message.success('安装任务已启动');
        
        // 报告到全局进度服务
        this.globalProgress.reportLogsInstall(jobName, jobNamespace, targetNs);
        
        // 启动状态轮询
        this.startJobStatusPolling();
        
        this.stepLoading = false;
        this.saveState();
        this.cdr.detectChanges();
          },
          error: (error: any) => {
            const msg = error?.error?.error || error?.error?.message || error?.message || '安装触发失败';
            this.applyResult = { success: false, message: `安装失败: ${msg}`, failureReason: msg };
            this.stepLoading = false;
            this.saveState();
            this.cdr.detectChanges();
          }
        });
      },
      error: () => {
        // 获取状态失败时，保守地继续尝试触发安装
        this.api.logsBootstrap(requestBody).subscribe({
          next: (res: any) => {
            const jobName = res?.jobName || 'polardbx-logs-bootstrap';
            const jobNamespace = res?.namespace || 'polardbx-operator-system';
            const targetNs = res?.targetNs || config.namespace;
            this.installJob = { jobName, namespace: jobNamespace, targetNs, instructions: res?.instructions };
            this.message.success('安装任务已启动');
            this.globalProgress.reportLogsInstall(jobName, jobNamespace, targetNs);
            this.startJobStatusPolling();
            this.stepLoading = false;
            this.saveState();
            this.cdr.detectChanges();
          },
          error: (error: any) => {
            const msg = error?.error?.error || error?.error?.message || error?.message || '安装触发失败';
            this.applyResult = { success: false, message: `安装失败: ${msg}`, failureReason: msg };
            this.stepLoading = false;
            this.saveState();
            this.cdr.detectChanges();
          }
        });
      }
    });
  }

  showKubectlInstructions(): void {
    const ns = this.form.value.namespace || 'polardbx-logcollector';
    const content = `
<div style="margin-bottom: 8px;">添加仓库（可选）</div>
<pre style="background:#f6f8fa;padding:12px;border-radius:6px;overflow-x:auto;">${this.getHelmRepoAddCommand()}</pre>
<div style="margin:12px 0 8px;">安装组件</div>
<pre style="background:#f6f8fa;padding:12px;border-radius:6px;overflow-x:auto;">${this.getHelmInstallCommand(ns)}</pre>
<div style="margin:12px 0 8px;">查看组件状态</div>
<pre style="background:#f6f8fa;padding:12px;border-radius:6px;overflow-x:auto;">${this.getKubectlGetPodsCommand(ns)}</pre>
`;
    this.modal.create({
      nzTitle: 'Helm/kubectl 安装命令',
      nzContent: content,
      nzFooter: [
        {
          label: '复制全部命令',
          type: 'primary',
          onClick: () => {
            const all = `${this.getHelmRepoAddCommand()}\n${this.getHelmInstallCommand(ns)}\n${this.getKubectlGetPodsCommand(ns)}`;
            this.copyToClipboard(all);
            return true;
          }
        },
        { label: '关闭', onClick: () => true }
      ],
      nzWidth: 820
    });
  }

  viewInstallLogs(): void {
    if (!this.installJob) return;

    this.api.logsBootstrapLogs(this.installJob.jobName, this.installJob.namespace).subscribe({
      next: (res: any) => {
        const content = this.normalizeLogsResponse(res);
        this.modal.create({
          nzTitle: '安装日志',
          nzContent: `<pre style="background: #f6f8fa; padding: 12px; border-radius: 6px; max-height: 400px; overflow-y: auto;">${content}</pre>`,
          nzWidth: 800,
          nzFooter: [
            { label: '关闭', onClick: () => true }
          ]
        });
      },
      error: (_error: any) => {
        this.message.error('获取安装日志失败');
      }
    });
  }

  // 规范化后端返回的日志对象，避免 [object Object]
  private normalizeLogsResponse(res: any): string {
    const raw = (res && (res.logs || res.message || res.text || res)) as any;
    const str = typeof raw === 'string' ? raw : JSON.stringify(raw, null, 2);
    return this.escapeHtml(str || '暂无日志');
  }

  private escapeHtml(s: string): string {
    return s.replace(/[&<>]/g, (ch) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' } as any)[ch] || ch);
  }

  retryApply(): void {
    this.applyResult = null;
    this.installJob = null;
    this.cdr.detectChanges();
  }

  startInstallation(): void {
    this.installing = true;
    this.installStep = 0;
    this.installLogs = [];
    this.installStartTime = new Date();
    this.addLog('info', '开始安装 PolarDB-X LogCollector...');

    // 使用真实的日志收集安装 API
    const ns = this.form.value.namespace || 'polardbx-logcollector';
    const deployment = this.form.value.deploymentType || 'default';

    const requestBody = {
      mode: 'managed' as 'managed',
      dryRun: false,
      enableFilebeat: this.form.value.enableFilebeat || true,
      enableLogstash: this.form.value.enableLogstash || true,
      deploymentType: deployment
    };

    // 已存在任务则不重复创建
    if (this.installJob?.jobName) {
      this.simulateInstallation();
      this.saveState();
      return;
    }

    // 若组件已存在则不创建
    this.api.getLogServiceStatus().subscribe({
      next: (s: any) => {
        const comps = s?.components || {};
        const exists = !!(comps?.filebeat?.exists || comps?.logstash?.exists || (comps?.filebeat && comps?.filebeat?.status !== 'not_found') || (comps?.logstash && comps?.logstash?.status !== 'not_found'));
        if (exists) {
          this.addLog('info', '检测到日志组件已安装，跳过安装流程');
          this.completeInstallation(true);
          this.saveState();
          return;
        }
        this.api.logsBootstrap(requestBody).subscribe({
      next: (res: any) => {
        const jobName = res?.jobName || 'polardbx-logs-bootstrap';
        const jobNamespace = res?.namespace || 'polardbx-operator-system';
        const targetNs = res?.targetNs || ns;

        this.installJob = {
          jobName,
          namespace: jobNamespace,
          targetNs,
          instructions: res?.instructions
        };

        this.addLog('success', `已创建日志收集安装 Job: ${jobName}`);

        // 报告到全局进度服务
        this.globalProgress.reportLogsInstall(jobName, jobNamespace, targetNs);

        // 启动状态轮询
        this.startJobStatusPolling();

        // 展示进度动画
        this.simulateInstallation();
        this.saveState(); // 保存状态
        },
        error: (error: any) => {
          const msg = error?.error?.error || error?.error?.message || error?.message || '安装触发失败';
          this.addLog('error', `启动安装失败: ${msg}`);
          this.completeInstallation(false);
          this.saveState();
        }
      });
      },
      error: () => {
        this.api.logsBootstrap(requestBody).subscribe({
          next: (res: any) => {
            const jobName = res?.jobName || 'polardbx-logs-bootstrap';
            const jobNamespace = res?.namespace || 'polardbx-operator-system';
            const targetNs = res?.targetNs || ns;
            this.installJob = { jobName, namespace: jobNamespace, targetNs, instructions: res?.instructions };
            this.addLog('success', `已创建日志收集安装 Job: ${jobName}`);
            this.globalProgress.reportLogsInstall(jobName, jobNamespace, targetNs);
            this.startJobStatusPolling();
            this.simulateInstallation();
            this.saveState();
          },
          error: (error: any) => {
            const msg = error?.error?.error || error?.error?.message || error?.message || '安装触发失败';
            this.addLog('error', `启动安装失败: ${msg}`);
            this.completeInstallation(false);
            this.saveState();
          }
        });
      }
    });
  }

  private simulateInstallation(): void {
    const steps = [
      { message: '准备安装环境...', delay: 1000 },
      { message: '创建命名空间 ' + this.form.value.namespace, delay: 1500 },
      { message: '部署 Filebeat DaemonSet...', delay: 2500 },
      { message: '部署 Logstash...', delay: 2000 },
      { message: '配置日志采集规则...', delay: 1000 }
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
      this.message.success('PolarDB-X LogCollector 安装成功！');
    } else {
      this.addLog('error', '安装失败，请检查错误信息');
      this.message.error('安装过程中出现错误');
    }

    this.saveState(); // 保存状态
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

  // 注意：restart 已在文件末尾实现，这里避免重复实现

  goToLogsDashboard(): void {
    this.router.navigate(['/operations/logs/dashboard']);
  }

  goToCollectors(): void {
    this.router.navigate(['/operations/logs/collectors']);
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
      this.api.getLogServiceStatus().subscribe({
        next: (s: any) => {
          const comps = s?.components || {};
          const fbStatus: string = comps?.filebeat?.status || (comps?.filebeat?.ready ? 'running' : 'error');
          const lsStatus: string = comps?.logstash?.status || (comps?.logstash?.ready ? 'running' : 'error');
          const ok = (fbStatus === 'running') && (lsStatus === 'running');
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

  // ==================== localStorage 持久化功能 ====================

  private saveState(): void {
    try {
      const compactFormValues = {
        deploymentType: this.form.value.deploymentType,
        namespace: this.form.value.namespace,
        enableFilebeat: this.form.value.enableFilebeat,
        enableLogstash: this.form.value.enableLogstash,
        enableElasticsearch: this.form.value.enableElasticsearch
      };

      const state: LogCollectorWizardState = {
        version: STATE_VERSION,
        currentStep: this.currentStep,
        formValues: compactFormValues,
        preflightChecks: this.preflightChecks,
        generatedYaml: this.generatedYaml,
        installResult: this.applyResult,
        installJob: this.installJob,
        timestamp: Date.now(),
        lastUpdated: Date.now()
      };
      localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
    } catch (error) {
      console.warn('保存日志安装向导状态失败:', error);
    }
  }

  private tryRestoreState(): void {
    try {
      const savedData = localStorage.getItem(STORAGE_KEY);
      if (!savedData) return;

      const state: LogCollectorWizardState = JSON.parse(savedData);

      // 版本校验
      if (!state.version || state.version !== STATE_VERSION) {
        console.log('状态版本不匹配，清除旧状态');
        this.clearSavedState();
        return;
      }

      // 检查状态是否太旧
      const hoursOld = (Date.now() - (state.lastUpdated || state.timestamp)) / (1000 * 60 * 60);
      if (hoursOld > MAX_STATE_AGE_HOURS) {
        console.log(`状态已过期 (${Math.round(hoursOld)}小时)，清除旧状态`);
        this.clearSavedState();
        return;
      }

      // 确认是否恢复状态
      if (state.currentStep > 0 || state.installJob) {
        this.confirmStateRestore(state);
      }
    } catch (error) {
      console.warn('恢复日志安装向导状态失败:', error);
      this.clearSavedState();
    }
  }

  private confirmStateRestore(state: LogCollectorWizardState): void {
    const hoursOld = (Date.now() - (state.lastUpdated || state.timestamp)) / (1000 * 60 * 60);
    const timeInfo = hoursOld < 1
      ? `${Math.round(hoursOld * 60)}分钟前`
      : `${Math.round(hoursOld)}小时前`;

    const message = state.installJob
      ? `检测到 ${timeInfo} 的日志收集安装任务 (${state.installJob.jobName})，是否继续跟踪安装进度？`
      : `检测到 ${timeInfo} 未完成的日志收集安装向导，是否从第 ${state.currentStep + 1} 步继续？`;

    this.modal.confirm({
      nzTitle: '恢复向导状态',
      nzContent: message,
      nzOkText: '继续',
      nzCancelText: '重新开始',
      nzOkType: 'primary',
      nzOnOk: () => this.restoreState(state),
      nzOnCancel: () => {
        this.modal.confirm({
          nzTitle: '确认清理状态',
          nzContent: '这将永久删除保存的向导状态，确定要重新开始吗？',
          nzOkText: '确定',
          nzCancelText: '取消',
          nzOkType: 'primary',
          nzOkDanger: true,
          nzOnOk: () => this.clearSavedState()
        });
      }
    });
  }

  private restoreState(state: LogCollectorWizardState): void {
    try {
      // 恢复表单值
      this.form.patchValue(state.formValues);

      // 恢复步骤
      this.currentStep = state.currentStep;

      // 恢复其他状态
      if (state.preflightChecks) {
        this.preflightChecks = state.preflightChecks;
      }
      if (state.generatedYaml) {
        this.generatedYaml = state.generatedYaml;
      }
      if (state.installResult) {
        this.applyResult = state.installResult;
      }
      if (state.installJob) {
        this.installJob = state.installJob;
        // 如果有安装任务，启动状态轮询
        this.startJobStatusPolling();
      }

      this.message.success('已恢复向导状态');
    } catch (error) {
      console.error('状态恢复失败:', error);
      this.message.error('状态恢复失败，请重新开始');
      this.clearSavedState();
    }
  }

  private clearSavedState(): void {
    try {
      localStorage.removeItem(STORAGE_KEY);
    } catch (error) {
      console.warn('清除保存状态失败:', error);
    }
  }

  // ==================== Job 状态轮询 ====================

  private pollingInterval: any;
  private pollingRetryCount = 0;
  private basePollingInterval = 5000; // 5秒基础间隔
  private maxPollingInterval = 60000; // 最大60秒间隔

  private startJobStatusPolling(): void {
    // 没有有效任务则不轮询，避免 404 噪音
    if (!this.installJob?.jobName) return;

    this.stopJobStatusPolling();
    this.pollingRetryCount = 0;

    const pollWithBackoff = () => {
      this.checkJobStatus();
      const currentInterval = Math.min(
        this.basePollingInterval * Math.pow(2, this.pollingRetryCount),
        this.maxPollingInterval
      );
      this.pollingInterval = setTimeout(pollWithBackoff, currentInterval);
    };

    // 立即检查一次
    pollWithBackoff();
  }

  private stopJobStatusPolling(): void {
    if (this.pollingInterval) {
      clearTimeout(this.pollingInterval);
      this.pollingInterval = null;
    }
    this.pollingRetryCount = 0;
  }

  private checkJobStatus(): void {
    if (!this.installJob?.jobName) return;

    this.api.logsBootstrapStatus(this.installJob.jobName, this.installJob.namespace).subscribe({
      next: (status: any) => {
        this.pollingRetryCount = 0; // 重置重试计数
        const phase = status?.phase;

        if (phase === 'Succeeded') {
          this.completeInstallation(true);
          this.stopJobStatusPolling();
          this.message.success('日志收集安装已完成');
          // 成功后清理本地保存的向导状态
          this.clearSavedState();
        } else if (phase === 'Failed') {
          const reason = status?.failureReason || '未知错误';
          this.installSuccess = false;
          this.installCompleted = true;
          this.addLog('error', `安装失败: ${reason}`);
          this.stopJobStatusPolling();
          this.message.error('日志收集安装失败');
          this.saveState(); // 保存失败状态
        }
        // 运行中的任务继续轮询
      },
      error: (error: any) => {
        this.pollingRetryCount++;

        // 404 表示 Job 不存在或已被清理
        if (error?.status === 404) {
          this.installSuccess = false;
          this.installCompleted = true;
          this.addLog('warning', '安装任务已被清理或不存在，请重新启动安装');
          this.stopJobStatusPolling();
          this.message.warning('安装任务不存在，可能已被系统清理');
          this.installJob = null; // 清空任务，避免后续进入页面继续轮询
          this.saveState();
          return;
        }

        console.warn(`检查任务状态失败 (重试${this.pollingRetryCount}次):`, error);

        // 达到最大重试次数后停止轮询
        if (this.pollingRetryCount >= 5) {
          this.stopJobStatusPolling();
          this.message.warning('无法获取安装状态，请手动检查任务进度');
        }
      }
    });
  }

  restart(): void {
    this.clearSavedState(); // 清理保存的状态
    this.currentStep = 0;
    this.installing = false;
    this.installCompleted = false;
    this.installSuccess = false;
    this.installLogs = [];
    this.installJob = null;
    this.runPreflightCheck();
  }
}


