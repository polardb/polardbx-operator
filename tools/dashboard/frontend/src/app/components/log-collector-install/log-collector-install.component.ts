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
import { firstValueFrom } from 'rxjs';
 

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
    <div class="page-wrapper log-collector-wizard">
      <div class="page-header">
        <div class="title-block">
          <h2>
            <i nz-icon nzType="file-text" class="page-icon"></i>
            日志采集安装向导
          </h2>
          <p>一站式安装日志采集堆栈，统一收集 PolarDB-X 集群日志</p>
        </div>
      </div>
      
      <app-wizard-shell
        title="日志采集向导"
        subtitle="配置日志采集组件"
        [showHeader]="false"
        [layout]="'horizontal'"
        [namespace]="form.value.namespace"
        [objectName]="getObjectName()"
        objectLabel="采集器"
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
            nzDescription="检查 Kubernetes 环境和日志采集组件依赖，确保安装前置条件满足。"
            nzShowIcon
            class="step-alert">
          </nz-alert>

          <div class="config-section">
            <h4>
              <i nz-icon nzType="safety-certificate"></i>
              环境预检
            </h4>
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

      <!-- 步骤2：采集参数 -->
      <ng-template #step2Template>
        <div class="step-content">
          <nz-alert 
            nzType="info"
            nzMessage="配置采集参数"
            nzDescription="设置日志采集堆栈的部署命名空间和基本参数。"
            nzShowIcon
            class="step-alert">
          </nz-alert>

          <div class="config-section">
            <h4>
              <i nz-icon nzType="setting"></i>
              基础配置
            </h4>
            <form [formGroup]="form" class="config-form">
              <nz-row [nzGutter]="16">
                <nz-col [nzSpan]="12">
                  <nz-form-item>
                    <nz-form-label [nzSpan]="6" nzRequired>命名空间</nz-form-label>
                    <nz-form-control [nzSpan]="18">
                      <input nz-input formControlName="namespace" placeholder="例如: polardbx-log" />
                    </nz-form-control>
                  </nz-form-item>
                </nz-col>
              </nz-row>
            </form>
          </div>
        </div>
      </ng-template>

      <!-- 步骤3：命令预览 -->
      <ng-template #step3Template>
        <div class="step-content">
          <nz-alert 
            nzType="info"
            nzMessage="安装命令预览"
            nzDescription="查看并复制 Helm 安装命令，支持手动或自动执行。"
            nzShowIcon
            class="step-alert">
          </nz-alert>
          
          <div class="config-section">
            <h4>
              <i nz-icon nzType="code"></i>
              添加 Helm 仓库（可选）
            </h4>
            <div class="command-block">
              <pre>{{ getHelmRepoAddCommand() }}</pre>
              <button 
                nz-button 
                nzType="dashed" 
                nzSize="small"
                (click)="copyToClipboard(getHelmRepoAddCommand())">
                <i nz-icon nzType="copy"></i>
                复制命令
              </button>
            </div>
          </div>

          <div class="config-section">
            <h4>
              <i nz-icon nzType="rocket"></i>
              安装日志采集堆栈
            </h4>
            <div class="command-block">
              <pre>{{ getHelmInstallCommand(form.value.namespace) }}</pre>
              <button 
                nz-button 
                nzType="dashed" 
                nzSize="small"
                (click)="copyToClipboard(getHelmInstallCommand(form.value.namespace))">
                <i nz-icon nzType="copy"></i>
                复制命令
              </button>
            </div>
          </div>

          <div class="config-section">
            <h4>
              <i nz-icon nzType="eye"></i>
              查看组件状态
            </h4>
            <div class="command-block">
              <pre>{{ getKubectlGetPodsCommand(form.value.namespace) }}</pre>
              <button 
                nz-button 
                nzType="dashed" 
                nzSize="small"
                (click)="copyToClipboard(getKubectlGetPodsCommand(form.value.namespace))">
                <i nz-icon nzType="copy"></i>
                复制命令
              </button>
            </div>
          </div>

          <div class="config-section">
            <h4>
              <i nz-icon nzType="setting"></i>
              配置 Logstash 输出
            </h4>
            <p style="color: rgba(0, 0, 0, 0.65); font-size: 13px; margin-bottom: 12px;">
              安装完成后，编辑 ConfigMap 配置 Elasticsearch 等输出目标。
            </p>
            <div class="command-block">
              <pre>kubectl edit configmap logstash-pipeline -n {{ form.value.namespace }}</pre>
              <button 
                nz-button 
                nzType="dashed" 
                nzSize="small"
                (click)="copyToClipboard('kubectl edit configmap logstash-pipeline -n ' + form.value.namespace)">
                <i nz-icon nzType="copy"></i>
                复制命令
              </button>
            </div>
          </div>
        </div>
      </ng-template>

      <!-- 步骤4：测试与应用 -->
      <ng-template #step4Template>
        <div class="step-content">
          <nz-result 
            [nzStatus]="applyResult?.success ? 'success' : (applyResult ? 'error' : 'info')"
            [nzTitle]="getResultTitle()"
            [nzSubTitle]="getResultSubtitle()">
            
            <div nz-result-content *ngIf="!applyResult && !installJob && !verifying">
              <div class="test-section">
                <h4>安装方式选择</h4>
                <nz-alert 
                  nzType="info"
                  nzMessage="选择安装方式"
                  nzDescription="推荐使用自动安装，系统将自动配置日志采集堆栈。也可以手动执行命令。"
                  nzShowIcon
                  class="test-alert">
                </nz-alert>

                <div class="test-actions">
                  <button 
                    nz-button 
                    nzType="primary"
                    (click)="applyConfiguration()"
                    [nzLoading]="applying">
                    <i nz-icon nzType="play-circle"></i>
                    自动安装
                  </button>
                  <button 
                    nz-button 
                    nzType="default"
                    (click)="showKubectlInstructions()">
                    <i nz-icon nzType="code"></i>
                    查看手动命令
                  </button>
                </div>
              </div>
            </div>

            <!-- 安装进度 & 日志 -->
            <div nz-result-content *ngIf="(!applyResult && installJob) || (verifying && !applyResult)">
              <div class="install-progress-grid">
                <div class="progress-card">
                  <h4>
                    <i nz-icon nzType="rocket"></i>
                    安装任务状态
                  </h4>

                  <ng-container *ngIf="installJob && !verifying; else verifyingBlock">
                    <nz-spin [nzSpinning]="true" nzTip="正在安装日志采集组件...">
                      <div class="progress-summary">
                        <p>任务名称：{{ installJob.jobName }}</p>
                        <p>命名空间：{{ installJob.namespace }}</p>
                        <p class="job-phase">当前状态：{{ jobPhase || '运行中' }}</p>
                        <p *ngIf="jobMessage" class="job-message">{{ jobMessage }}</p>
                      </div>
                    </nz-spin>
                  </ng-container>

                  <ng-template #verifyingBlock>
                    <nz-alert
                      nzType="info"
                      nzShowIcon
                      nzMessage="正在验证组件状态"
                      [nzDescription]="latestComponentStatus ? 'Filebeat：' + latestComponentStatus.filebeat + ' · Logstash：' + latestComponentStatus.logstash : '正在获取组件状态…'">
                    </nz-alert>
                  </ng-template>

                  <div class="progress-actions">
                    <button nz-button nzType="default" (click)="viewInstallLogs()" [disabled]="!installJob">
                      <i nz-icon nzType="file-text"></i>
                      查看安装日志
                    </button>
                  </div>
                </div>

                <div class="log-card">
                  <div class="log-card-header">
                    <h4>
                      <i nz-icon nzType="file-text"></i>
                      实时安装日志
                    </h4>
                    <button nz-button nzType="link" nzSize="small" (click)="startLogStreaming()" *ngIf="installJob">
                      <i nz-icon nzType="sync"></i>
                      刷新
                    </button>
                  </div>
                  <nz-spin [nzSpinning]="installLogLoading" nzTip="加载日志...">
                    <pre class="live-log" *ngIf="installLogLines.length; else emptyLog">{{ installLogLines.join('\n') }}</pre>
                    <ng-template #emptyLog>
                      <div class="empty-log">暂未获取到日志内容</div>
                    </ng-template>
                  </nz-spin>
                </div>
              </div>
            </div>

            <div nz-result-extra *ngIf="applyResult?.success">
              <button nz-button nzType="primary" (click)="goToLogsDashboard()">
                <i nz-icon nzType="dashboard"></i>
                查看日志
              </button>
              <button nz-button nzType="default" (click)="restart()">
                <i nz-icon nzType="plus"></i>
                重新配置
              </button>
            </div>
            
            <div nz-result-extra *ngIf="applyResult && !applyResult.success">
              <button nz-button nzType="primary" (click)="retryApply()">
                <i nz-icon nzType="reload"></i>
                重试安装
              </button>
              <button nz-button nzType="default" (click)="goToPrevStep()">
                <i nz-icon nzType="left"></i>
                上一步
              </button>
              <button nz-button nzType="default" (click)="viewInstallLogs()">
                <i nz-icon nzType="file-text"></i>
                查看日志
              </button>
            </div>
          </nz-result>
        </div>
      </ng-template>
      </app-wizard-shell>
    </div>
  `,
  styles: [`
    .page-wrapper {
      display: flex;
      flex-direction: column;
      gap: 16px;
      padding: 24px;
      min-height: 100%;
      background: transparent;
    }

    /* Override wizard-shell dark background */
    :deep(.wizard-shell) {
      background: transparent !important;
    }
    
    :deep(.wizard-body) {
      background: transparent !important;
    }
    
    :deep(.wizard-header) {
      background: white !important;
    }
    
    :deep(.wizard-footer) {
      background: white !important;
    }

    .page-header {
      background: #fff;
      padding: 16px;
      border-radius: 10px;
      box-shadow: 0 2px 10px rgba(15, 23, 42, 0.04);
    }

    .title-block h2 {
      margin: 0 0 8px;
      font-size: 22px;
      font-weight: 600;
      color: #1f1f1f;
      display: flex;
      align-items: center;
      gap: 10px;
    }

    .title-block p {
      margin: 0;
      color: #595959;
      line-height: 1.6;
    }

    .page-icon {
      color: var(--primary-color, #4a7c9b) !important;
    }
  `,
  `
    .step-content {
      padding: 0;
    }

    .step-alert {
      margin-bottom: 24px;
    }

    .config-form {
      margin-bottom: 24px;
    }

    .config-section {
      margin-bottom: 32px;
      padding: 20px;
      background: #fafafa;
      border: 1px solid #e8e8e8;
      border-radius: 8px;
    }

    .config-section h4 {
      margin: 0 0 20px 0;
      color: rgba(0, 0, 0, 0.85);
      font-size: 16px;
      font-weight: 500;
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .command-block {
      position: relative;
      background: #f6f8fa;
      border: 1px solid #e1e4e8;
      border-radius: 6px;
      padding: 12px;
      margin-bottom: 16px;
    }

    .command-block pre {
      margin: 0 0 12px 0;
      font-family: 'SFMono-Regular', 'Monaco', 'Menlo', 'Courier New', monospace;
      font-size: 13px;
      line-height: 1.4;
      color: #24292e;
      word-wrap: break-word;
      white-space: pre-wrap;
    }

    .command-block button {
      margin-top: 8px;
    }

    // Environment check styles
    .precheck-items {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }

    .precheck-item {
      padding: 16px;
      border: 1px solid #e8e8e8;
      border-radius: 8px;
      background: white;
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

    // Apply option styles
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

    .install-progress-grid {
      display: flex;
      flex-direction: column;
      gap: 20px;
    }

    .progress-card,
    .log-card {
      background: #fff;
      border: 1px solid #e8e8e8;
      border-radius: 8px;
      padding: 20px;
      width: 100%;
    }

    .progress-card {
      order: 1;
    }

    .log-card {
      order: 2;
    }

    .progress-card h4,
    .log-card h4 {
      margin: 0 0 16px 0;
      color: rgba(0, 0, 0, 0.85);
      font-size: 16px;
      font-weight: 500;
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .progress-summary p {
      margin: 0 0 8px 0;
      color: rgba(0, 0, 0, 0.65);
      font-size: 14px;
      line-height: 1.6;
    }

    .progress-actions {
      margin-top: 16px;
      padding-top: 16px;
      border-top: 1px solid #f0f0f0;
    }

    .job-phase {
      color: rgba(0, 0, 0, 0.85);
      font-weight: 500;
    }

    .job-message {
      color: rgba(0, 0, 0, 0.6);
      font-size: 12px;
      margin-top: 4px;
    }

    .log-card-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 12px;
    }

    .live-log {
      background: #0b1a33;
      color: #d6f4ff;
      font-family: 'SFMono-Regular', 'Monaco', 'Menlo', 'Courier New', monospace;
      font-size: 12px;
      line-height: 1.5;
      border-radius: 6px;
      padding: 12px;
      max-height: 450px;
      min-height: 300px;
      overflow-y: auto;
      white-space: pre-wrap;
    }

    .empty-log {
      text-align: center;
      color: rgba(0, 0, 0, 0.45);
      padding: 24px 0;
      font-size: 13px;
    }

    .test-section {
      padding: 20px;
    }

    .test-section h4 {
      margin: 0 0 16px 0;
      color: rgba(0, 0, 0, 0.85);
      font-size: 16px;
      font-weight: 500;
    }

    .test-alert {
      margin-bottom: 20px;
    }

    .test-actions {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
    }

    .test-actions button {
      min-width: 140px;
    }

    @media (max-width: 768px) {
      .option-cards {
        grid-template-columns: 1fr;
      }

      .test-actions {
        flex-direction: column;
      }

      .test-actions button {
        width: 100%;
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
  
  // Environment check
  checkingEnvironment = false;
  preflightChecks: PreflightCheck[] = [];
  
  // YAML generation
  generatedYaml = '';
  generatingYaml = false;
  
  // Step status
  stepLoading = false;
  
  // Apply result
  applyResult: { success: boolean; message: string; failureReason?: string } | null = null;

  // Installation status
  installSuccess = false;
  verifying = false;
  verifyAttempts = 0;
  verifyMaxAttempts = 10;
  private verifyTimer?: any;
  installLogLines: string[] = [];
  installLogLoading = false;
  private logPollingTimer?: any;
  private lastLogSnapshot = '';
  applying = false;
  jobPhase: string | null = null;
  jobMessage: string | null = null;
  latestComponentStatus: { filebeat: string; logstash: string; updatedAt: number } | null = null;
  installJob: { jobName: string; namespace: string; targetNs?: string; instructions?: string } | null = null;
  
  // Deployment configuration
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
      // Keep switches for future extension, but installation uses Helm
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
    // Mount templates to steps
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
    this.stopLogStreaming();
    this.clearVerifyTimer();
  }

  // New environment check method, consistent with monitoring wizard style
  async runPreflightCheck(): Promise<void> {
    const namespace = this.form.value.namespace || 'polardbx-logcollector';

    this.checkingEnvironment = true;
    this.preflightChecks = [
      {
        name: 'Kubernetes 权限',
        description: '检查 RBAC 权限和命名空间访问',
        status: 'pending',
        result: '检查中...',
        command: `kubectl auth can-i create pods --namespace=${namespace}`
      },
      {
        name: '日志采集组件',
        description: '检查 Filebeat/Logstash 组件状态',
        status: 'pending',
        result: '检查中...'
      },
      {
        name: '输出配置',
        description: '检查 Logstash 输出与管道配置',
        status: 'pending',
        result: '检查中...',
        command: `kubectl get configmap logstash-pipeline -n ${namespace}`
      }
    ];

    this.cdr.detectChanges();

    // 1. Dry-run to validate permissions
    try {
      await firstValueFrom(this.api.logsBootstrap({ mode: 'managed', namespace, dryRun: true }));
      this.updatePreflightCheck(0, 'success', 'Dry-run 成功，具备安装所需权限');
    } catch (error) {
      this.updatePreflightCheck(0, 'error', `权限检查失败：${this.extractErrorMessage(error)}`);
    }

    // 2. Component status
    let logServiceStatus: any | null = null;
    try {
      logServiceStatus = await firstValueFrom(this.api.getLogServiceStatus(namespace));
      const state = (logServiceStatus?.state || logServiceStatus?.status || '').toLowerCase();
      const summary = this.summarizeComponentStatus(logServiceStatus?.components);
      const status: PreflightCheck['status'] = state === 'running'
        ? 'success'
        : state === 'not_installed'
          ? 'warning'
          : 'warning';
      const result = summary || '未检测到日志采集组件（安装后将自动创建）';
      this.updatePreflightCheck(1, status, result);
    } catch (error) {
      this.updatePreflightCheck(1, 'warning', `无法获取组件状态：${this.extractErrorMessage(error)}`);
    }

    // 3. Output/pipeline configuration
    if (logServiceStatus) {
      const pipelineExists = !!logServiceStatus.pipelineConfigMapExists;
      const result = pipelineExists
        ? '检测到 Logstash pipeline 配置，请确认输出目标连通性'
        : '未发现 Logstash 输出配置，安装后可在命令预览步骤查看配置示例';
      const status: PreflightCheck['status'] = pipelineExists ? 'success' : 'warning';
      this.updatePreflightCheck(2, status, result);
    } else {
      this.updatePreflightCheck(2, 'warning', '输出配置检查依赖组件状态，请先完成前置检查');
    }

    this.checkingEnvironment = false;
    this.saveState();
    this.cdr.detectChanges();
  }

  private updatePreflightCheck(index: number, status: PreflightCheck['status'], result: string): void {
    if (!this.preflightChecks[index]) {
      return;
    }
    this.preflightChecks[index] = {
      ...this.preflightChecks[index],
      status,
      result
    };
    this.cdr.markForCheck();
  }

  private extractErrorMessage(error: any): string {
    const raw = error?.error || error;
    if (typeof raw === 'string') {
      return raw;
    }
    return raw?.error || raw?.message || error?.message || '未知错误';
  }

  /**
   * 检查环境预检是否有阻塞性错误 (error 状态)
   * 只有 error 状态会阻止继续，warning 状态可以继续（用户确认后）
   */
  hasBlockingPreflightError(): boolean {
    return this.preflightChecks.some(check => check.status === 'error');
  }

  /**
   * 检查环境预检是否已完成（所有检查都不是 pending 状态）
   */
  isPreflightComplete(): boolean {
    return this.preflightChecks.length > 0 && 
           this.preflightChecks.every(check => check.status !== 'pending');
  }

  private summarizeComponentStatus(components: any): string {
    if (!components) {
      return '';
    }
    const parts: string[] = [];
    if (components.filebeat) {
      parts.push(this.formatComponentStatus('Filebeat', components.filebeat));
    }
    if (components.logstash) {
      parts.push(this.formatComponentStatus('Logstash', components.logstash));
    }
    return parts.filter(Boolean).join(' · ');
  }

  private formatComponentStatus(name: string, info: any): string {
    if (!info) {
      return '';
    }
    const status = info.status || '未知';
    const ready = info?.replicas?.ready;
    const total = info?.replicas?.total;
    if (typeof ready === 'number' && typeof total === 'number' && total > 0) {
      return `${name}: ${status} (${ready}/${total})`;
    }
    return `${name}: ${status}`;
  }

  // Methods required by WizardShell
  getStepActions(): WizardAction[] {
    const actions: WizardAction[] = [];

    switch (this.currentStep) {
      case 0:
        // Environment check step: must complete check with no blocking errors to proceed
        const canProceedFromPrecheck = this.isPreflightComplete() && !this.hasBlockingPreflightError();
        actions.push({
          text: '下一步：配置参数',
          type: 'primary',
          disabled: this.checkingEnvironment || !canProceedFromPrecheck,
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

  getResultTitle(): string {
    if (this.applyResult) {
      return this.applyResult.success ? '安装成功' : '安装失败';
    }
    if (this.verifying) {
      return '正在验证组件状态';
    }
    if (this.installJob) {
      return '安装进行中';
    }
    return '准备安装';
  }

  getResultSubtitle(): string {
    if (this.applyResult) {
      return this.applyResult.message;
    }
    if (this.verifying) {
      return '已完成安装任务，正在检测 Filebeat / Logstash 组件运行状态';
    }
    if (this.installJob) {
      return '正在安装日志采集堆栈，请稍候...';
    }
    return '选择安装方式，开始部署日志采集组件';
  }

  copyToClipboard(text: string): void {
    navigator.clipboard.writeText(text).then(() => {
      this.message.success('命令已复制到剪贴板');
    }).catch(() => {
      this.message.error('复制失败');
    });
  }

  onDeploymentTypeChange(type: string): void {
    // Automatically adjust configuration based on deployment type
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
        // Keep user's current selection
        break;
    }
    
    // No longer generate resource YAML, step 3 is command preview
  }

  // Command generation replaces YAML
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

  // Apply configuration method
  applyConfiguration(): void {
    this.stepLoading = true;
    this.applying = true;
    this.applyResult = null;
    this.verifying = false;
    this.installLogLines = [];
    this.installLogLoading = false;
  this.jobPhase = null;
  this.jobMessage = null;
  this.latestComponentStatus = null;
    
    // Use real log collection installation API
    const config = this.form.value;
    const requestBody = {
      mode: 'managed' as 'managed',
      dryRun: false,
      namespace: config.namespace
    };

    // If installJob already exists, avoid duplicate triggering, directly continue polling/viewing
    if (this.installJob?.jobName) {
      this.stepLoading = false;
      this.applying = false;
      this.startJobStatusPolling();
      this.cdr.detectChanges();
      return;
    }

    // If system has detected component exists, do not trigger installation again
    this.api.getLogServiceStatus(config.namespace).subscribe({
      next: (s: any) => {
        const comps = s?.components || {};
        const exists = !!(comps?.filebeat?.exists || comps?.logstash?.exists || (comps?.filebeat && comps?.filebeat?.status !== 'not_found') || (comps?.logstash && comps?.logstash?.status !== 'not_found'));
        if (exists) {
          this.message.success('检测到日志采集组件已安装，无需重复部署');
          this.applyResult = {
            success: true,
            message: '日志采集组件已存在，可直接前往日志总览查看数据。'
          };
          this.stepLoading = false;
          this.applying = false;
          this.saveState();
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

        this.lastLogSnapshot = '';
        this.installLogLines = [];
        this.installLogLoading = true;

        this.message.success('安装任务已启动');
        
        // Report to global progress service
        this.globalProgress.reportLogsInstall(jobName, jobNamespace, targetNs);
        
        // Start status polling
        this.startJobStatusPolling();
        this.startLogStreaming();
        
        this.stepLoading = false;
        this.applying = false;
        this.saveState();
        this.cdr.detectChanges();
          },
          error: (error: any) => {
            const msg = error?.error?.error || error?.error?.message || error?.message || '安装触发失败';
            this.applyResult = { success: false, message: `安装失败: ${msg}`, failureReason: msg };
            this.stepLoading = false;
            this.applying = false;
            this.saveState();
            this.cdr.detectChanges();
          }
        });
      },
      error: () => {
        // When getting status fails, conservatively continue trying to trigger installation
        this.api.logsBootstrap(requestBody).subscribe({
          next: (res: any) => {
            const jobName = res?.jobName || 'polardbx-logs-bootstrap';
            const jobNamespace = res?.namespace || 'polardbx-operator-system';
            const targetNs = res?.targetNs || config.namespace;
            this.installJob = { jobName, namespace: jobNamespace, targetNs, instructions: res?.instructions };
                this.lastLogSnapshot = '';
                this.installLogLines = [];
                this.installLogLoading = true;
            this.message.success('安装任务已启动');
            this.globalProgress.reportLogsInstall(jobName, jobNamespace, targetNs);
            this.startJobStatusPolling();
            this.startLogStreaming();
            this.stepLoading = false;
                this.applying = false;
            this.saveState();
            this.cdr.detectChanges();
          },
          error: (error: any) => {
            const msg = error?.error?.error || error?.error?.message || error?.message || '安装触发失败';
            this.applyResult = { success: false, message: `安装失败: ${msg}`, failureReason: msg };
            this.stepLoading = false;
                this.applying = false;
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
    if (!this.installJob) {
      if (this.installLogLines.length) {
        const cached = this.installLogLines.join('\n');
        this.modal.create({
          nzTitle: '安装日志（缓存）',
          nzContent: `<pre style="background: #f6f8fa; padding: 12px; border-radius: 6px; max-height: 400px; overflow-y: auto;">${cached}</pre>`,
          nzWidth: 800,
          nzFooter: [
            { label: '复制日志', onClick: () => { this.copyToClipboard(cached); return true; } },
            { label: '关闭', onClick: () => true }
          ]
        });
      }
      return;
    }

    this.api.logsBootstrapLogs(this.installJob.jobName, this.installJob.namespace).subscribe({
      next: (res: any) => {
        const content = this.normalizeLogsResponse(res);
        this.modal.create({
          nzTitle: '安装日志',
          nzContent: `<pre style="background: #f6f8fa; padding: 12px; border-radius: 6px; max-height: 400px; overflow-y: auto;">${content}</pre>`,
          nzWidth: 800,
          nzFooter: [
            { label: '复制日志', onClick: () => { this.copyToClipboard(content); return true; } },
            { label: '关闭', onClick: () => true }
          ]
        });
      },
      error: (_error: any) => {
        this.message.error('获取安装日志失败');
      }
    });
  }

  startLogStreaming(): void {
    if (!this.installJob?.jobName?.trim()) {
      console.warn('[LogCollector] startLogStreaming called but installJob.jobName is empty/missing', this.installJob);
      this.installLogLoading = false;
      return;
    }

    this.stopLogStreaming();
    this.installLogLoading = true;

    const pollLogs = (initial = false) => {
      if (!this.installJob?.jobName?.trim()) {
        console.warn('[LogCollector] pollLogs guard: installJob.jobName is empty', this.installJob);
        this.installLogLoading = false;
        return;
      }

      console.debug('[LogCollector] Polling logs:', { jobName: this.installJob.jobName, namespace: this.installJob.namespace });
      this.api.logsBootstrapLogs(this.installJob.jobName, this.installJob.namespace).subscribe({
        next: (res: any) => {
          const normalized = this.normalizeLogsResponse(res);
          if (normalized !== this.lastLogSnapshot) {
            this.lastLogSnapshot = normalized;
          }
          this.installLogLines = this.lastLogSnapshot
            ? this.lastLogSnapshot.split(/\r?\n/)
            : [];
          this.installLogLoading = false;
          this.cdr.markForCheck();
        },
        error: () => {
          if (initial) {
            this.installLogLoading = false;
            this.cdr.markForCheck();
          }
        }
      });
    };

    pollLogs(true);
    this.logPollingTimer = setInterval(() => pollLogs(), 4000);
  }

  private stopLogStreaming(): void {
    if (this.logPollingTimer) {
      clearInterval(this.logPollingTimer);
      this.logPollingTimer = undefined;
    }
    this.installLogLoading = false;
  }

  // Normalize log objects returned by backend to avoid [object Object]
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
    this.verifying = false;
    this.stopLogStreaming();
    this.clearVerifyTimer();
    this.installLogLines = [];
    this.lastLogSnapshot = '';
    this.jobPhase = null;
    this.jobMessage = null;
    this.latestComponentStatus = null;
    this.applying = false;
    this.cdr.detectChanges();
  }

  goToPrevStep(): void {
    if (this.currentStep > 0) {
      this.currentStep--;
      this.applyResult = null;
      this.cdr.markForCheck();
    }
  }

  // Note: restart is already implemented at the end of the file, avoid duplicate implementation here

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

    const evaluateStatus = (res: any) => {
      const comps = res?.components || {};
      const fbStatus: string = comps?.filebeat?.status || (comps?.filebeat?.ready ? 'running' : comps?.filebeat?.phase);
      const lsStatus: string = comps?.logstash?.status || (comps?.logstash?.ready ? 'running' : comps?.logstash?.phase);
      const filebeatReady = fbStatus === 'running';
      const logstashReady = lsStatus === 'running';
      const ok = filebeatReady && logstashReady;

      this.latestComponentStatus = {
        filebeat: fbStatus || '未知',
        logstash: lsStatus || '未知',
        updatedAt: Date.now()
      };

      if (ok) {
        this.installSuccess = true;
        this.verifying = false;
        this.applyResult = {
          success: true,
          message: '日志采集组件已成功部署并处于运行状态'
        };
        this.stopLogStreaming();
        this.installJob = null;
        this.clearVerifyTimer();
        this.clearSavedState();
        this.message.success('日志采集组件已成功部署');
        this.cdr.detectChanges();
        return true;
      }

      const statusDescription = `Filebeat 状态: ${fbStatus || '未知'}, Logstash 状态: ${lsStatus || '未知'}`;

      if (this.verifyAttempts >= this.verifyMaxAttempts) {
        this.installSuccess = false;
        this.verifying = false;
        this.applyResult = {
          success: false,
          message: '组件未在预期时间内就绪，请检查运行状态',
          failureReason: statusDescription
        };
        this.stopLogStreaming();
        this.clearVerifyTimer();
        this.message.error('组件未在预期时间内就绪，请检查运行状态');
        this.saveState();
        this.cdr.detectChanges();
        return true;
      }

      this.applyResult = null;
      this.cdr.detectChanges();
      return false;
    };

    const poll = () => {
      this.verifyAttempts++;
      this.api.getLogServiceStatus(this.form.value.namespace).subscribe({
        next: (res: any) => {
          if (evaluateStatus(res)) {
            this.clearVerifyTimer();
          }
        },
        error: () => {
          if (this.verifyAttempts >= this.verifyMaxAttempts) {
            this.installSuccess = false;
            this.verifying = false;
            this.applyResult = {
              success: false,
              message: '无法获取组件状态，请手动确认安装结果'
            };
            this.stopLogStreaming();
            this.clearVerifyTimer();
            this.message.error('无法获取组件状态，请手动确认安装结果');
            this.saveState();
            this.cdr.detectChanges();
          }
        }
      });
    };

    poll();
    this.verifyTimer = setInterval(poll, 4000);
  }

  // ==================== localStorage persistence functionality ====================

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

      // Version validation
      if (!state.version || state.version !== STATE_VERSION) {
        console.log('状态版本不匹配，清除旧状态');
        this.clearSavedState();
        return;
      }

      // Check if state is too old
      const hoursOld = (Date.now() - (state.lastUpdated || state.timestamp)) / (1000 * 60 * 60);
      if (hoursOld > MAX_STATE_AGE_HOURS) {
        console.log(`状态已过期 (${Math.round(hoursOld)}小时)，清除旧状态`);
        this.clearSavedState();
        return;
      }

      // Confirm whether to restore state
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
      // Restore form values
      this.form.patchValue(state.formValues);

      // Restore step
      this.currentStep = state.currentStep;

      // Restore other state
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
        // If there is an installation task, start status polling
        this.startJobStatusPolling();
        this.startLogStreaming();
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

  // ==================== Job status polling ====================

  private pollingInterval: any;
  private pollingRetryCount = 0;
  private basePollingInterval = 5000; // 5 second base interval
  private maxPollingInterval = 60000; // Maximum 60 second interval

  private startJobStatusPolling(): void {
    // Don't poll if there's no valid task, avoid 404 noise
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

    // Check immediately once
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
        this.pollingRetryCount = 0; // Reset retry count
        const phase = status?.phase;
        this.jobPhase = phase || null;
        this.jobMessage = status?.message || status?.details?.message || status?.conditions?.[0]?.message || null;

        if (phase === 'Succeeded') {
          this.stopJobStatusPolling();
          this.stopLogStreaming(); // Stop log polling to avoid 404
          this.message.success('安装任务已完成，正在验证组件状态');
          this.verifying = true;
          this.saveState();
          this.startVerification();
        } else if (phase === 'Failed') {
          const reason = status?.failureReason || '未知错误';
          this.installSuccess = false;
          this.verifying = false;
          this.stopLogStreaming();
          this.latestComponentStatus = null;
          this.applyResult = {
            success: false,
            message: `安装失败: ${reason}`,
            failureReason: reason
          };
          this.stopJobStatusPolling();
          this.message.error('日志收集安装失败');
          this.saveState(); // Save failed state
        }
        // Running tasks continue polling
      },
      error: (error: any) => {
        this.pollingRetryCount++;

        // 404 means Job doesn't exist or has been cleaned up
        if (error?.status === 404) {
          this.installSuccess = false;
          this.verifying = false;
          this.stopLogStreaming();
          this.stopJobStatusPolling();
          this.message.warning('安装任务不存在，可能已被系统清理');
          this.installJob = null; // Clear task to avoid continuing polling when entering page later
          this.jobPhase = null;
          this.jobMessage = null;
          this.latestComponentStatus = null;
          this.saveState();
          return;
        }

        console.warn(`检查任务状态失败 (重试${this.pollingRetryCount}次):`, error);

        // Stop polling after reaching maximum retry count
        if (this.pollingRetryCount >= 5) {
          this.stopJobStatusPolling();
          this.message.warning('无法获取安装状态，请手动检查任务进度');
        }
      }
    });
  }

  restart(): void {
    this.clearSavedState(); // Clear saved state
    this.currentStep = 0;
    this.installSuccess = false;
    this.installJob = null;
    this.applyResult = null;
    this.verifying = false;
    this.stopLogStreaming();
    this.clearVerifyTimer();
    this.installLogLines = [];
    this.lastLogSnapshot = '';
    this.jobPhase = null;
    this.jobMessage = null;
    this.latestComponentStatus = null;
    this.applying = false;
    this.runPreflightCheck();
  }
}

