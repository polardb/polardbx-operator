import { Component, OnInit, ChangeDetectionStrategy, ChangeDetectorRef, ViewChild, TemplateRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReactiveFormsModule, FormBuilder, FormGroup, Validators, FormArray } from '@angular/forms';
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
import { NzStepsModule } from 'ng-zorro-antd/steps';
import { NzSwitchModule } from 'ng-zorro-antd/switch';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzModalService, NzModalModule } from 'ng-zorro-antd/modal';
import { NzMessageService, NzMessageModule } from 'ng-zorro-antd/message';

import { WizardShellComponent, WizardStep, WizardAction } from '../wizard-shell/wizard-shell.component';
import { YamlPreviewComponent } from '../yaml-preview/yaml-preview.component';
import { ApiService } from '../../services/api.service';

interface AlertChannel {
  type: 'email' | 'dingtalk';
  name: string;
  config: EmailConfig | DingTalkConfig;
}

interface EmailConfig {
  smtp_smarthost: string;
  smtp_from: string;
  smtp_auth_username: string;
  smtp_auth_password: string;
  smtp_require_tls: boolean;
  to: string[];
  subject?: string;
  body?: string;
}

interface DingTalkConfig {
  webhook_url: string;
  secret?: string;
  title?: string;
  text?: string;
}

@Component({
  selector: 'app-alert-receiver-wizard',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
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
    NzStepsModule,
    NzSwitchModule,
    NzDividerModule,
    NzModalModule,
    NzMessageModule,
    WizardShellComponent,
    YamlPreviewComponent
  ],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="alert-receiver-wizard">
      <div class="page-header">
        <div class="header-content">
          <h1 class="page-title">
            <i nz-icon nzType="mail" class="page-icon"></i>
            告警接收器配置向导
          </h1>
          <p class="subtitle">配置邮件和钉钉告警通知，生成 Alertmanager Secret 配置</p>
        </div>
      </div>
      
      <app-wizard-shell
        title="告警接收器向导"
        subtitle="配置告警通知渠道"
        [namespace]="form.value.namespace"
        [objectName]="getObjectName()"
        objectLabel="接收器"
        docLink="https://docs.polardbx.com/alerting"
        [steps]="wizardSteps"
        [currentStepIndex]="currentStep"
        [actions]="getStepActions()"
        [loading]="stepLoading">

      <!-- 步骤1：选择渠道 -->
      <ng-template #step1Template>
        <div class="step-content">
          <nz-alert 
            nzType="info"
            nzMessage="告警通知渠道"
            nzDescription="选择要配置的告警通知渠道。可以配置邮件或钉钉通知，也可以同时配置多种渠道。"
            nzShowIcon
            class="step-alert">
          </nz-alert>

          <form [formGroup]="form" class="config-form">
            <nz-row [nzGutter]="16">
              <nz-col [nzSpan]="12">
                <nz-form-item>
                  <nz-form-label [nzSpan]="6" nzRequired>通知渠道</nz-form-label>
                  <nz-form-control [nzSpan]="18">
                    <nz-select 
                      formControlName="channelType" 
                      nzPlaceholder="选择通知渠道">
                      <nz-option nzValue="email" nzLabel="邮件通知"></nz-option>
                      <nz-option nzValue="dingtalk" nzLabel="钉钉通知"></nz-option>
                      <nz-option nzValue="both" nzLabel="邮件 + 钉钉"></nz-option>
                    </nz-select>
                  </nz-form-control>
                </nz-form-item>
              </nz-col>
              <nz-col [nzSpan]="12">
                <nz-form-item>
                  <nz-form-label [nzSpan]="6" nzRequired>命名空间</nz-form-label>
                  <nz-form-control [nzSpan]="18">
                    <nz-select 
                      formControlName="namespace" 
                      nzPlaceholder="选择命名空间"
                      nzShowSearch>
                      <nz-option 
                        *ngFor="let ns of namespaces" 
                        [nzValue]="ns" 
                        [nzLabel]="ns">
                      </nz-option>
                    </nz-select>
                  </nz-form-control>
                </nz-form-item>
              </nz-col>
            </nz-row>

            <nz-row [nzGutter]="16">
              <nz-col [nzSpan]="24">
                <nz-form-item>
                  <nz-form-label [nzSpan]="3" nzRequired>接收器名称</nz-form-label>
                  <nz-form-control [nzSpan]="21">
                    <input 
                      nz-input 
                      formControlName="receiverName"
                      placeholder="例如: polardbx-alerts">
                  </nz-form-control>
                </nz-form-item>
              </nz-col>
            </nz-row>

            <div class="channel-description" *ngIf="form.value.channelType">
              <h4>{{ getChannelDescription().title }}</h4>
              <p>{{ getChannelDescription().description }}</p>
              
              <div class="features-list">
                <h5>配置内容：</h5>
                <ul>
                  <li *ngFor="let feature of getChannelDescription().features">{{ feature }}</li>
                </ul>
              </div>
            </div>
          </form>
        </div>
      </ng-template>

      <!-- 步骤2：配置参数 -->
      <ng-template #step2Template>
        <div class="step-content">
          <nz-alert 
            nzType="info"
            nzMessage="通知参数配置"
            nzDescription="配置具体的通知参数。所有敏感信息将安全存储在 Kubernetes Secret 中。"
            nzShowIcon
            class="step-alert">
          </nz-alert>

          <form [formGroup]="form" class="config-form">
            <!-- 邮件配置 -->
            <div *ngIf="needsEmailConfig()" class="config-section">
              <h4>
                <i nz-icon nzType="mail"></i>
                邮件通知配置
              </h4>
              
              <nz-row [nzGutter]="16">
                <nz-col [nzSpan]="12">
                  <nz-form-item>
                    <nz-form-label [nzSpan]="8" nzRequired>SMTP 服务器</nz-form-label>
                    <nz-form-control [nzSpan]="16">
                      <input 
                        nz-input 
                        formControlName="smtpHost"
                        placeholder="例如: smtp.gmail.com:587">
                    </nz-form-control>
                  </nz-form-item>
                </nz-col>
                <nz-col [nzSpan]="12">
                  <nz-form-item>
                    <nz-form-label [nzSpan]="8" nzRequired>发件人邮箱</nz-form-label>
                    <nz-form-control [nzSpan]="16">
                      <input 
                        nz-input 
                        formControlName="smtpFrom"
                        placeholder="例如: alerts@company.com">
                    </nz-form-control>
                  </nz-form-item>
                </nz-col>
              </nz-row>

              <nz-row [nzGutter]="16">
                <nz-col [nzSpan]="12">
                  <nz-form-item>
                    <nz-form-label [nzSpan]="8" nzRequired>SMTP 用户名</nz-form-label>
                    <nz-form-control [nzSpan]="16">
                      <input 
                        nz-input 
                        formControlName="smtpUsername"
                        placeholder="SMTP 认证用户名">
                    </nz-form-control>
                  </nz-form-item>
                </nz-col>
                <nz-col [nzSpan]="12">
                  <nz-form-item>
                    <nz-form-label [nzSpan]="8" nzRequired>SMTP 密码</nz-form-label>
                    <nz-form-control [nzSpan]="16">
                      <input 
                        nz-input 
                        type="password"
                        formControlName="smtpPassword"
                        placeholder="SMTP 认证密码">
                    </nz-form-control>
                  </nz-form-item>
                </nz-col>
              </nz-row>

              <nz-row [nzGutter]="16">
                <nz-col [nzSpan]="24">
                  <nz-form-item>
                    <nz-form-label [nzSpan]="3" nzRequired>收件人列表</nz-form-label>
                    <nz-form-control [nzSpan]="21">
                      <textarea 
                        nz-input 
                        formControlName="emailRecipients"
                        placeholder="每行一个邮箱地址，例如：&#10;admin@company.com&#10;ops@company.com"
                        rows="3">
                      </textarea>
                    </nz-form-control>
                  </nz-form-item>
                </nz-col>
              </nz-row>

              <nz-row [nzGutter]="16">
                <nz-col [nzSpan]="12">
                  <nz-form-item>
                    <nz-form-label [nzSpan]="8">邮件主题</nz-form-label>
                    <nz-form-control [nzSpan]="16">
                      <input 
                        nz-input 
                        formControlName="emailSubject"
                        [attr.placeholder]="'默认: [{{ .Status }}] {{ .GroupLabels.alertname }}'">
                    </nz-form-control>
                  </nz-form-item>
                </nz-col>
                <nz-col [nzSpan]="12">
                  <nz-form-item>
                    <nz-form-label [nzSpan]="8">启用 TLS</nz-form-label>
                    <nz-form-control [nzSpan]="16">
                      <nz-switch formControlName="smtpTLS"></nz-switch>
                    </nz-form-control>
                  </nz-form-item>
                </nz-col>
              </nz-row>
            </div>

            <!-- 钉钉配置 -->
            <div *ngIf="needsDingTalkConfig()" class="config-section">
              <h4>
                <i nz-icon nzType="dingding"></i>
                钉钉通知配置
              </h4>
              
              <nz-row [nzGutter]="16">
                <nz-col [nzSpan]="24">
                  <nz-form-item>
                    <nz-form-label [nzSpan]="3" nzRequired>Webhook URL</nz-form-label>
                    <nz-form-control [nzSpan]="21">
                      <input 
                        nz-input 
                        formControlName="dingTalkWebhook"
                        placeholder="https://oapi.dingtalk.com/robot/send?access_token=...">
                    </nz-form-control>
                  </nz-form-item>
                </nz-col>
              </nz-row>

              <nz-row [nzGutter]="16">
                <nz-col [nzSpan]="12">
                  <nz-form-item>
                    <nz-form-label [nzSpan]="8">签名密钥</nz-form-label>
                    <nz-form-control [nzSpan]="16">
                      <input 
                        nz-input 
                        type="password"
                        formControlName="dingTalkSecret"
                        placeholder="可选，用于签名验证">
                    </nz-form-control>
                  </nz-form-item>
                </nz-col>
                <nz-col [nzSpan]="12">
                  <nz-form-item>
                    <nz-form-label [nzSpan]="8">消息标题</nz-form-label>
                    <nz-form-control [nzSpan]="16">
                      <input 
                        nz-input 
                        formControlName="dingTalkTitle"
                        placeholder="默认: PolarDB-X 告警">
                    </nz-form-control>
                  </nz-form-item>
                </nz-col>
              </nz-row>
            </div>
          </form>
        </div>
      </ng-template>

      <!-- 步骤3：YAML 预览 -->
      <ng-template #step3Template>
        <div class="step-content">
          <app-yaml-preview
            [yamlContent]="generatedYaml"
            [filename]="getYamlFilename()"
            [loading]="generatingYaml"
            [readonly]="true">
          </app-yaml-preview>
        </div>
      </ng-template>

      <!-- 步骤4：测试与应用 -->
      <ng-template #step4Template>
        <div class="step-content">
          <nz-result 
            [nzStatus]="applyResult?.success ? 'success' : (applyResult ? 'error' : 'info')"
            [nzTitle]="getResultTitle()"
            [nzSubTitle]="getResultSubtitle()">
            
            <div nz-result-content *ngIf="!applyResult">
              <div class="test-section">
                <h4>测试告警通知</h4>
                <nz-alert 
                  nzType="info"
                  nzMessage="测试建议"
                  nzDescription="建议先测试告警发送，确认配置正确后再应用到 Alertmanager。"
                  nzShowIcon
                  class="test-alert">
                </nz-alert>

                <div class="test-actions">
                  <button 
                    nz-button 
                    nzType="default"
                    (click)="sendTestAlert()"
                    [nzLoading]="testingSend">
                    <i nz-icon nzType="experiment"></i>
                    发送测试告警
                  </button>
                  <button 
                    nz-button 
                    nzType="primary"
                    (click)="applyConfiguration()"
                    [nzLoading]="applying">
                    <i nz-icon nzType="check"></i>
                    应用配置
                  </button>
                </div>

                <div class="test-result" *ngIf="testResult">
                  <nz-alert 
                    [nzType]="testResult.success ? 'success' : 'error'"
                    [nzMessage]="testResult.success ? '测试发送成功' : '测试发送失败'"
                    [nzDescription]="testResult.message"
                    nzShowIcon>
                  </nz-alert>
                </div>
              </div>

              <nz-divider></nz-divider>

              <div class="manual-apply">
                <h4>手动应用</h4>
                <p>或者您可以复制以下命令手动应用配置：</p>
                
                <div class="command-block">
                  <pre>{{ getKubectlCommand() }}</pre>
                  <button 
                    nz-button 
                    nzType="dashed" 
                    nzSize="small"
                    (click)="copyKubectlCommand()">
                    <i nz-icon nzType="copy"></i>
                    复制命令
                  </button>
                </div>
              </div>
            </div>

            <div nz-result-extra *ngIf="applyResult?.success">
              <button nz-button nzType="primary" (click)="goToAlertManager()">
                <i nz-icon nzType="alert"></i>
                查看 Alertmanager
              </button>
              <button nz-button nzType="default" (click)="goToAlertsManagement()">
                <i nz-icon nzType="setting"></i>
                告警管理
              </button>
            </div>

            <div nz-result-extra *ngIf="applyResult && !applyResult.success">
              <button nz-button nzType="primary" (click)="retryApply()">
                <i nz-icon nzType="reload"></i>
                重试
              </button>
              <button nz-button nzType="default" (click)="goToPrevStep()">
                <i nz-icon nzType="left"></i>
                上一步
              </button>
            </div>
          </nz-result>
        </div>
      </ng-template>
      </app-wizard-shell>
    </div>
  `,
  styles: [`
    .alert-receiver-wizard {
      padding: 16px;
      background: #f5f5f5;
      min-height: 100vh;
    }

    /* 覆盖wizard-shell的深色背景 */
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
      margin-bottom: 16px;
    }

    .header-content {
      max-width: 1120px;
      margin: 0 auto;
    }

    .page-title {
      font-size: 20px !important;
      font-weight: 600 !important;
      color: rgba(0, 0, 0, 0.88) !important;
      margin: 0 !important;
      display: flex;
      align-items: center;
      gap: 12px;
    }

    .page-icon {
      font-size: 24px !important;
      color: #1890ff !important;
    }

    .subtitle {
      color: rgba(0, 0, 0, 0.65);
      font-size: 14px;
      margin: 4px 0 0 36px;
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

    .channel-description {
      margin-top: 24px;
      padding: 16px;
      background: #f8f9fa;
      border-radius: 6px;
      border: 1px solid #e8e8e8;
    }

    .channel-description h4 {
      margin: 0 0 8px 0;
      color: rgba(0, 0, 0, 0.85);
      font-size: 14px;
      font-weight: 500;
    }

    .channel-description p {
      margin: 0 0 12px 0;
      color: rgba(0, 0, 0, 0.65);
      font-size: 13px;
      line-height: 1.5;
    }

    .features-list h5 {
      margin: 0 0 8px 0;
      color: rgba(0, 0, 0, 0.8);
      font-size: 13px;
      font-weight: 500;
    }

    .features-list ul {
      margin: 0;
      padding-left: 16px;
    }

    .features-list li {
      color: rgba(0, 0, 0, 0.65);
      font-size: 12px;
      line-height: 1.4;
      margin-bottom: 4px;
    }

    .test-section {
      text-align: left;
      max-width: 600px;
      margin: 0 auto;
    }

    .test-section h4 {
      margin: 0 0 16px 0;
      color: rgba(0, 0, 0, 0.85);
      font-size: 16px;
      font-weight: 500;
    }

    .test-alert {
      margin-bottom: 24px;
    }

    .test-actions {
      display: flex;
      gap: 12px;
      margin-bottom: 24px;
    }

    .test-result {
      margin-bottom: 24px;
    }

    .manual-apply {
      text-align: left;
    }

    .manual-apply h4 {
      margin: 0 0 12px 0;
      color: rgba(0, 0, 0, 0.85);
      font-size: 16px;
      font-weight: 500;
    }

    .manual-apply p {
      color: rgba(0, 0, 0, 0.65);
      font-size: 14px;
      margin-bottom: 16px;
    }

    .command-block {
      position: relative;
      background: #f6f8fa;
      border: 1px solid #e1e4e8;
      border-radius: 6px;
      padding: 12px;
    }

    .command-block pre {
      margin: 0;
      font-family: 'SFMono-Regular', 'Monaco', 'Menlo', 'Courier New', monospace;
      font-size: 13px;
      line-height: 1.4;
      color: #24292e;
      word-wrap: break-word;
      white-space: pre-wrap;
    }

    .command-block button {
      position: absolute;
      top: 8px;
      right: 8px;
    }

    /* 响应式设计 */
    @media (max-width: 768px) {
      .test-actions {
        flex-direction: column;
      }

      .test-section,
      .manual-apply {
        max-width: 100%;
      }
    }
  `]
})
export class AlertReceiverWizardComponent implements OnInit {
  @ViewChild('step1Template', { read: TemplateRef }) step1Template!: TemplateRef<any>;
  @ViewChild('step2Template', { read: TemplateRef }) step2Template!: TemplateRef<any>;
  @ViewChild('step3Template', { read: TemplateRef }) step3Template!: TemplateRef<any>;
  @ViewChild('step4Template', { read: TemplateRef }) step4Template!: TemplateRef<any>;

  form: FormGroup;
  currentStep = 0;
  stepLoading = false;

  // 数据源
  namespaces: string[] = [];

  // YAML 生成
  generatingYaml = false;
  generatedYaml = '';

  // 测试和应用
  testingSend = false;
  applying = false;
  testResult: { success: boolean; message: string } | null = null;
  applyResult: { success: boolean; message: string } | null = null;

  wizardSteps: WizardStep[] = [];

  constructor(
    private fb: FormBuilder,
    private api: ApiService,
    private message: NzMessageService,
    private modal: NzModalService,
    private router: Router,
    private cdr: ChangeDetectorRef
  ) {
    this.form = this.fb.group({
      channelType: ['email', Validators.required],
      namespace: ['polardbx-monitor', Validators.required],
      receiverName: ['', Validators.required],
      // 邮件配置
      smtpHost: [''],
      smtpFrom: [''],
      smtpUsername: [''],
      smtpPassword: [''],
      smtpTLS: [true],
      emailRecipients: [''],
      emailSubject: [''],
      // 钉钉配置
      dingTalkWebhook: [''],
      dingTalkSecret: [''],
      dingTalkTitle: ['']
    });
  }

  ngOnInit(): void {
    this.initializeWizardSteps();
    this.loadNamespaces();
    this.setupFormValidators();
  }

  private initializeWizardSteps(): void {
    this.wizardSteps = [
      { id: 'channel', title: '选择渠道', description: '通知渠道类型' },
      { id: 'config', title: '配置参数', description: '通知参数设置' },
      { id: 'yaml', title: 'YAML 预览', description: '配置预览' },
      { id: 'test', title: '测试应用', description: '测试与应用' }
    ];
  }

  ngAfterViewInit(): void {
    // 设置步骤模板
    this.wizardSteps[0].template = this.step1Template;
    this.wizardSteps[1].template = this.step2Template;
    this.wizardSteps[2].template = this.step3Template;
    this.wizardSteps[3].template = this.step4Template;
    this.cdr.detectChanges();
  }

  private setupFormValidators(): void {
    // 监听渠道类型变化，动态设置验证器
    this.form.get('channelType')?.valueChanges.subscribe((channelType) => {
      this.updateValidators(channelType);
    });

    // 初始设置验证器
    this.updateValidators(this.form.value.channelType);
  }

  private updateValidators(channelType: string): void {
    // 清除所有验证器
    const emailFields = ['smtpHost', 'smtpFrom', 'smtpUsername', 'smtpPassword', 'emailRecipients'];
    const dingTalkFields = ['dingTalkWebhook'];

    emailFields.forEach(field => {
      this.form.get(field)?.clearValidators();
      this.form.get(field)?.updateValueAndValidity();
    });

    dingTalkFields.forEach(field => {
      this.form.get(field)?.clearValidators();
      this.form.get(field)?.updateValueAndValidity();
    });

    // 根据渠道类型设置验证器
    if (channelType === 'email' || channelType === 'both') {
      emailFields.forEach(field => {
        this.form.get(field)?.setValidators([Validators.required]);
        this.form.get(field)?.updateValueAndValidity();
      });
    }

    if (channelType === 'dingtalk' || channelType === 'both') {
      dingTalkFields.forEach(field => {
        this.form.get(field)?.setValidators([Validators.required]);
        this.form.get(field)?.updateValueAndValidity();
      });
    }
  }

  private loadNamespaces(): void {
    this.api.getNamespaces().subscribe({
      next: (namespaces: any) => {
        this.namespaces = namespaces || [];
        this.cdr.markForCheck();
      },
      error: (error: any) => {
        console.error('加载命名空间失败:', error);
        this.message.error('加载命名空间失败');
      }
    });
  }

  getObjectName(): string {
    const receiverName = this.form.value.receiverName;
    const channelType = this.form.value.channelType;
    if (!receiverName) return '';
    return `${receiverName} (${this.getChannelTypeText(channelType)})`;
  }

  private getChannelTypeText(type: string): string {
    switch (type) {
      case 'email': return '邮件';
      case 'dingtalk': return '钉钉';
      case 'both': return '邮件+钉钉';
      default: return '';
    }
  }

  getChannelDescription(): { title: string; description: string; features: string[] } {
    const type = this.form.value.channelType;
    switch (type) {
      case 'email':
        return {
          title: '邮件通知',
          description: '通过 SMTP 发送邮件告警通知，支持多个收件人。',
          features: [
            'SMTP 服务器配置（主机、用户名、密码）',
            'TLS 加密连接支持',
            '多收件人支持',
            '自定义邮件主题和内容模板'
          ]
        };
      case 'dingtalk':
        return {
          title: '钉钉通知',
          description: '通过钉钉机器人发送告警消息到群聊。',
          features: [
            'Webhook URL 配置',
            '可选签名密钥验证',
            '自定义消息标题',
            'Markdown 格式消息支持'
          ]
        };
      case 'both':
        return {
          title: '混合通知',
          description: '同时配置邮件和钉钉通知，确保告警信息及时送达。',
          features: [
            '邮件和钉钉双重保障',
            '独立的配置参数',
            '分别测试发送',
            '统一的接收器配置'
          ]
        };
      default:
        return { title: '', description: '', features: [] };
    }
  }

  needsEmailConfig(): boolean {
    const type = this.form.value.channelType;
    return type === 'email' || type === 'both';
  }

  needsDingTalkConfig(): boolean {
    const type = this.form.value.channelType;
    return type === 'dingtalk' || type === 'both';
  }

  getStepActions(): WizardAction[] {
    const actions: WizardAction[] = [];
    
    // 上一步按钮
    if (this.currentStep > 0) {
      actions.push({
        text: '上一步',
        icon: 'left',
        handler: () => this.prevStep()
      });
    }

    // 根据当前步骤添加特定按钮
    switch (this.currentStep) {
      case 0: // 选择渠道
        actions.push({
          text: '下一步：配置参数',
          type: 'primary',
          icon: 'right',
          disabled: !this.form.get('channelType')?.valid || !this.form.get('receiverName')?.valid,
          handler: () => this.nextStep()
        });
        break;
      
      case 1: // 配置参数
        actions.push({
          text: '下一步：YAML 预览',
          type: 'primary',
          icon: 'right',
          disabled: !this.isConfigValid(),
          handler: () => this.nextStep()
        });
        break;
      
      case 2: // YAML 预览
        actions.push({
          text: '重新生成',
          icon: 'sync',
          loading: this.generatingYaml,
          handler: () => this.generateYaml()
        });
        actions.push({
          text: '下一步：测试应用',
          type: 'primary',
          icon: 'right',
          disabled: !this.generatedYaml,
          handler: () => this.nextStep()
        });
        break;
      
      case 3: // 测试应用
        actions.push({
          text: '完成',
          type: 'default',
          icon: 'check-circle',
          handler: () => this.finish()
        });
        break;
    }

    return actions;
  }

  private isConfigValid(): boolean {
    const channelType = this.form.value.channelType;
    
    if (channelType === 'email' || channelType === 'both') {
      const emailValid = this.form.get('smtpHost')?.valid &&
                         this.form.get('smtpFrom')?.valid &&
                         this.form.get('smtpUsername')?.valid &&
                         this.form.get('smtpPassword')?.valid &&
                         this.form.get('emailRecipients')?.valid;
      if (!emailValid) return false;
    }

    if (channelType === 'dingtalk' || channelType === 'both') {
      const dingTalkValid = this.form.get('dingTalkWebhook')?.valid;
      if (!dingTalkValid) return false;
    }

    return true;
  }

  nextStep(): void {
    if (this.currentStep < this.wizardSteps.length - 1) {
      this.currentStep++;
      
      // 进入 YAML 预览步骤时自动生成
      if (this.currentStep === 2) {
        this.generateYaml();
      }
      
      this.cdr.markForCheck();
    }
  }

  prevStep(): void {
    if (this.currentStep > 0) {
      this.currentStep--;
      this.cdr.markForCheck();
    }
  }

  goToPrevStep(): void {
    this.prevStep();
  }

  generateYaml(): void {
    this.generatingYaml = true;
    
    const config = this.form.value;
    const channels: AlertChannel[] = [];

    // 生成邮件配置
    if (this.needsEmailConfig()) {
      const emailRecipients = config.emailRecipients
        .split('\n')
        .map((email: string) => email.trim())
        .filter((email: string) => email);

      channels.push({
        type: 'email',
        name: `${config.receiverName}-email`,
        config: {
          smtp_smarthost: config.smtpHost,
          smtp_from: config.smtpFrom,
          smtp_auth_username: config.smtpUsername,
          smtp_auth_password: config.smtpPassword,
          smtp_require_tls: config.smtpTLS,
          to: emailRecipients,
          subject: config.emailSubject || '[{{ .Status }}] {{ .GroupLabels.alertname }}',
          body: 'PolarDB-X 告警通知\\n\\n状态: {{ .Status }}\\n告警: {{ .GroupLabels.alertname }}\\n详情: {{ range .Alerts }}{{ .Annotations.summary }}{{ end }}'
        }
      });
    }

    // 生成钉钉配置
    if (this.needsDingTalkConfig()) {
      channels.push({
        type: 'dingtalk',
        name: `${config.receiverName}-dingtalk`,
        config: {
          webhook_url: config.dingTalkWebhook,
          secret: config.dingTalkSecret || undefined,
          title: config.dingTalkTitle || 'PolarDB-X 告警',
          text: '**状态:** {{ .Status }}\\n**告警:** {{ .GroupLabels.alertname }}\\n{{ range .Alerts }}**详情:** {{ .Annotations.summary }}{{ end }}'
        }
      });
    }

    // 生成 Secret YAML
    const secretData = this.generateSecretYaml(config.receiverName, channels);
    
    // 模拟生成过程
    setTimeout(() => {
      this.generatedYaml = secretData;
      this.generatingYaml = false;
      this.cdr.markForCheck();
    }, 1000);
  }

  private generateSecretYaml(receiverName: string, channels: AlertChannel[]): string {
    const alertmanagerConfig = {
      route: {
        receiver: receiverName,
        group_by: ['alertname'],
        group_wait: '10s',
        group_interval: '5m',
        repeat_interval: '12h'
      },
      receivers: [
        {
          name: receiverName,
          email_configs: channels
            .filter(c => c.type === 'email')
            .map(c => ({
              to: (c.config as EmailConfig).to,
              from: (c.config as EmailConfig).smtp_from,
              smarthost: (c.config as EmailConfig).smtp_smarthost,
              auth_username: (c.config as EmailConfig).smtp_auth_username,
              auth_password: (c.config as EmailConfig).smtp_auth_password,
              require_tls: (c.config as EmailConfig).smtp_require_tls,
              subject: (c.config as EmailConfig).subject,
              body: (c.config as EmailConfig).body
            })),
          webhook_configs: channels
            .filter(c => c.type === 'dingtalk')
            .map(c => ({
              url: (c.config as DingTalkConfig).webhook_url,
              title: (c.config as DingTalkConfig).title,
              text: (c.config as DingTalkConfig).text
            }))
        }
      ]
    };

    const configYaml = JSON.stringify(alertmanagerConfig, null, 2);
    const configB64 = btoa(configYaml);

    return `apiVersion: v1
kind: Secret
metadata:
  name: alertmanager-${receiverName}
  namespace: ${this.form.value.namespace}
  labels:
    app.kubernetes.io/name: alertmanager
    app.kubernetes.io/component: configuration
type: Opaque
data:
  alertmanager.yml: ${configB64}

---
# 可选：用于钉钉 Webhook 适配的 ConfigMap
apiVersion: v1
kind: ConfigMap
metadata:
  name: ${receiverName}-webhook-config
  namespace: ${this.form.value.namespace}
data:
  config.yaml: |
    dingtalk:
      webhook_url: "${channels.find(c => c.type === 'dingtalk')?.config ? (channels.find(c => c.type === 'dingtalk')!.config as DingTalkConfig).webhook_url : ''}"
      secret: "${channels.find(c => c.type === 'dingtalk')?.config ? (channels.find(c => c.type === 'dingtalk')!.config as DingTalkConfig).secret || '' : ''}"`;
  }

  getYamlFilename(): string {
    return `alertmanager-${this.form.value.receiverName}-config.yaml`;
  }

  sendTestAlert(): void {
    this.testingSend = true;
    this.testResult = null;

    // 调用测试接口
    const testData = {
      receiverName: this.form.value.receiverName,
      channels: this.getChannelsForTest()
    };

    this.api.testAlert(testData).subscribe({
      next: (result: any) => {
        this.testResult = {
          success: true,
          message: result.message || '测试告警发送成功，请检查邮箱或钉钉群'
        };
        this.testingSend = false;
        this.cdr.markForCheck();
      },
      error: (error: any) => {
        this.testResult = {
          success: false,
          message: error.error?.message || error.message || '测试发送失败'
        };
        this.testingSend = false;
        this.cdr.markForCheck();
      }
    });
  }

  private getChannelsForTest(): any[] {
    const config = this.form.value;
    const channels = [];

    if (this.needsEmailConfig()) {
      channels.push({
        type: 'email',
        to: config.emailRecipients.split('\n').map((e: string) => e.trim()).filter(Boolean),
        smtp_smarthost: config.smtpHost,
        smtp_from: config.smtpFrom
      });
    }

    if (this.needsDingTalkConfig()) {
      channels.push({
        type: 'dingtalk',
        webhook_url: config.dingTalkWebhook
      });
    }

    return channels;
  }

  applyConfiguration(): void {
    this.modal.confirm({
      nzTitle: '确认应用配置？',
      nzContent: `将应用告警接收器配置"${this.form.value.receiverName}"`,
      nzOkText: '确认应用',
      nzOkType: 'primary',
      nzCancelText: '取消',
      nzOnOk: () => this.doApplyConfiguration()
    });
  }

  private doApplyConfiguration(): void {
    this.applying = true;
    
    // 模拟应用过程
    setTimeout(() => {
      this.applyResult = {
        success: true,
        message: '告警接收器配置已成功应用'
      };
      this.applying = false;
      this.message.success('告警接收器配置应用成功！');
      this.cdr.markForCheck();
    }, 2000);
  }

  retryApply(): void {
    this.applyResult = null;
    this.applyConfiguration();
  }

  getKubectlCommand(): string {
    const filename = this.getYamlFilename();
    return `# 保存 YAML 内容到文件\nkubectl apply -f ${filename}\n\n# 或者直接应用\ncat <<EOF | kubectl apply -f -\n${this.generatedYaml}\nEOF`;
  }

  copyKubectlCommand(): void {
    const command = this.getKubectlCommand();
    navigator.clipboard.writeText(command).then(() => {
      this.message.success('命令已复制到剪贴板');
    }).catch(() => {
      this.message.error('复制失败');
    });
  }

  getResultTitle(): string {
    if (!this.applyResult) {
      return '准备测试和应用';
    }
    return this.applyResult.success ? '配置应用成功' : '配置应用失败';
  }

  getResultSubtitle(): string {
    if (!this.applyResult) {
      return '测试告警发送并应用接收器配置';
    }
    return this.applyResult.message;
  }

  goToAlertManager(): void {
    window.open('http://alertmanager.example.com', '_blank');
  }

  goToAlertsManagement(): void {
    this.router.navigate(['/operations/alerts']);
  }

  finish(): void {
    this.router.navigate(['/operations/alerts']);
  }
}
