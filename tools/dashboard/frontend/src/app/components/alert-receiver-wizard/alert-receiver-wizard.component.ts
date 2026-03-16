import { Component, OnInit, ChangeDetectionStrategy, ChangeDetectorRef, ViewChild, TemplateRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReactiveFormsModule, FormBuilder, FormGroup, Validators } from '@angular/forms';
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
    <div class="page-wrapper alert-receiver-wizard">
      <div class="page-header">
        <div class="title-block">
          <h2>
            <i nz-icon nzType="mail" class="page-icon"></i>
            告警接收器配置向导
          </h2>
          <p>配置邮件和钉钉告警通知，生成 Alertmanager Secret 配置</p>
        </div>
      </div>
      
      <app-wizard-shell
        title="告警接收器向导"
        subtitle="配置告警通知渠道"
        [showHeader]="false"
        [layout]="'horizontal'"
        [namespace]="form.value.namespace"
        [objectName]="getObjectName()"
        objectLabel="接收器"
        docLink="https://doc.polardbx.com/operator/ops/monitor/5-alert-config.html"
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
              <nz-col [nzSpan]="12">
                <nz-form-item>
                  <nz-form-label [nzSpan]="6" nzRequired>Alertmanager 命名空间</nz-form-label>
                  <nz-form-control [nzSpan]="18">
                    <nz-select
                      formControlName="alertmanagerNamespace"
                      nzPlaceholder="Alertmanager 所在命名空间"
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
              <nz-col [nzSpan]="12">
                <nz-form-item>
                  <nz-form-label [nzSpan]="6" nzRequired>Alertmanager 名称</nz-form-label>
                  <nz-form-control [nzSpan]="18">
                    <input
                      nz-input
                      formControlName="alertmanagerName"
                      placeholder="默认: main">
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
                    <nz-form-label [nzSpan]="3" nzRequired>适配器 URL</nz-form-label>
                    <nz-form-control [nzSpan]="21">
                      <input 
                        nz-input 
                        formControlName="dingTalkAdapterUrl"
                        placeholder="例如: http://webhook-dingtalk.polardbx-monitor:8060/dingtalk/webhook1/send">
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

                <form [formGroup]="form">
                  <nz-form-item style="margin-bottom: 16px;">
                    <nz-form-label>Alertmanager URL</nz-form-label>
                    <nz-form-control>
                      <input
                        nz-input
                        formControlName="alertmanagerUrl"
                        placeholder="例如: http://alertmanager:9093（留空则尝试使用已保存的默认地址）">
                    </nz-form-control>
                  </nz-form-item>
                </form>

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
                  <button 
                    nz-button 
                    nzType="dashed"
                    (click)="copyKubectlCommand()">
                    <i nz-icon nzType="copy"></i>
                    复制 kubectl 命令
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
                <i nz-icon nzType="alert" nzTheme="outline"></i>
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

    /* Responsive design */
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

  // Data source
  namespaces: string[] = [];

  // YAML generation
  generatingYaml = false;
  generatedYaml = '';

  // Test and apply
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
      receiverName: ['', [Validators.required, Validators.pattern(/^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/)]],
      alertmanagerUrl: [''],
      alertmanagerNamespace: ['polardbx-monitor', Validators.required],
      alertmanagerName: ['main', Validators.required],
      // Email configuration
      smtpHost: [''],
      smtpFrom: [''],
      smtpUsername: [''],
      smtpPassword: [''],
      smtpTLS: [true],
      emailRecipients: [''],
      emailSubject: [''],
      // DingTalk configuration
      dingTalkAdapterUrl: ['']
    });
  }

  ngOnInit(): void {
    this.initializeWizardSteps();
    this.loadNamespaces();
    this.loadDefaultAlertmanagerUrl();
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
    // Set step templates
    this.wizardSteps[0].template = this.step1Template;
    this.wizardSteps[1].template = this.step2Template;
    this.wizardSteps[2].template = this.step3Template;
    this.wizardSteps[3].template = this.step4Template;
    this.cdr.detectChanges();
  }

  private setupFormValidators(): void {
    // Listen to channel type changes, dynamically set validators
    this.form.get('channelType')?.valueChanges.subscribe((channelType) => {
      this.updateValidators(channelType);
    });

    // Initial validator setup
    this.updateValidators(this.form.value.channelType);
  }

  private updateValidators(channelType: string): void {
    // Clear all validators
    const emailFields = ['smtpHost', 'smtpFrom', 'smtpUsername', 'smtpPassword', 'emailRecipients'];
    const dingTalkFields = ['dingTalkAdapterUrl'];

    emailFields.forEach(field => {
      this.form.get(field)?.clearValidators();
      this.form.get(field)?.updateValueAndValidity();
    });

    dingTalkFields.forEach(field => {
      this.form.get(field)?.clearValidators();
      this.form.get(field)?.updateValueAndValidity();
    });

    // Set validators based on channel type
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

  private loadDefaultAlertmanagerUrl(): void {
    this.api.getAlertRoutes().subscribe({
      next: (resp: any) => {
        const url = (resp?.alertmanagerUrl || '').trim();
        if (url && !String(this.form.value.alertmanagerUrl || '').trim()) {
          this.form.patchValue({ alertmanagerUrl: url }, { emitEvent: false });
          this.cdr.markForCheck();
        }
      },
      error: () => {
        // ignore
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
          description: 'Alertmanager 不直接支持钉钉；需要部署钉钉 Webhook 适配器，将 Alertmanager webhook payload 转换为钉钉格式。',
          features: [
            '填写钉钉适配器 URL（例如 prometheus-webhook-dingtalk）',
            '适配器负责与钉钉机器人交互（token/secret 等）',
            'Alertmanager 通过 webhookConfigs 将告警推送到适配器'
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
    
    // Previous step button
    if (this.currentStep > 0) {
      actions.push({
        text: 'Previous',
        icon: 'left',
        handler: () => this.prevStep()
      });
    }

    // Add specific buttons based on current step
    switch (this.currentStep) {
      case 0: // Select channel
        actions.push({
          text: 'Next: Configure Parameters',
          type: 'primary',
          icon: 'right',
          disabled: !this.form.get('channelType')?.valid ||
            !this.form.get('namespace')?.valid ||
            !this.form.get('alertmanagerNamespace')?.valid ||
            !this.form.get('alertmanagerName')?.valid ||
            !this.form.get('receiverName')?.valid,
          handler: () => this.nextStep()
        });
        break;
      
      case 1: // Configure parameters
        actions.push({
          text: 'Next: YAML Preview',
          type: 'primary',
          icon: 'right',
          disabled: !this.isConfigValid(),
          handler: () => this.nextStep()
        });
        break;
      
      case 2: // YAML preview
        actions.push({
          text: 'Regenerate',
          icon: 'sync',
          loading: this.generatingYaml,
          handler: () => this.generateYaml()
        });
        actions.push({
          text: 'Next: Test & Apply',
          type: 'primary',
          icon: 'right',
          disabled: !this.generatedYaml,
          handler: () => this.nextStep()
        });
        break;
      
      case 3: // Test & apply
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
      const dingTalkValid = this.form.get('dingTalkAdapterUrl')?.valid;
      if (!dingTalkValid) return false;
    }

    return true;
  }

  nextStep(): void {
    if (this.currentStep < this.wizardSteps.length - 1) {
      this.currentStep++;
      
      // Auto-generate when entering YAML preview step
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
    try {
      this.generatedYaml = this.generateKubernetesYaml();
    } finally {
      this.generatingYaml = false;
      this.cdr.markForCheck();
    }
  }

  private generateKubernetesYaml(): string {
    const receiverName = String(this.form.value.receiverName || '').trim();
    const targetNamespace = String(this.form.value.namespace || '').trim();
    const alertmanagerName = String(this.form.value.alertmanagerName || 'main').trim() || 'main';
    if (!receiverName || !targetNamespace) return '';

    const docs: string[] = [];

    let smtpSecretName = '';
    if (this.needsEmailConfig()) {
      smtpSecretName = `polardbx-alert-receiver-${receiverName}-smtp`;
      const smtpPassword = String(this.form.value.smtpPassword || '');
      docs.push(`apiVersion: v1
kind: Secret
metadata:
  name: ${smtpSecretName}
  namespace: ${targetNamespace}
  labels:
    app.kubernetes.io/managed-by: polardbx-dashboard
type: Opaque
stringData:
  smtpPassword: ${this.yamlQuote(smtpPassword)}`);
    }

    const emailConfigs: string[] = [];
    if (this.needsEmailConfig()) {
      const toList = this.parseEmailRecipients(String(this.form.value.emailRecipients || ''));
      const to = toList.join(',');
      emailConfigs.push(`    emailConfigs:
    - to: ${this.yamlQuote(to)}
      from: ${this.yamlQuote(String(this.form.value.smtpFrom || '').trim())}
      smarthost: ${this.yamlQuote(String(this.form.value.smtpHost || '').trim())}
      authUsername: ${this.yamlQuote(String(this.form.value.smtpUsername || '').trim())}
      authIdentity: ${this.yamlQuote(String(this.form.value.smtpUsername || '').trim())}
      authPassword:
        name: ${smtpSecretName}
        key: smtpPassword
      requireTLS: ${this.form.value.smtpTLS ? 'true' : 'false'}
      sendResolved: true
      text: |-
        {{ range .Alerts -}}
        [{{ .Status }}] {{ .Labels.alertname }} ({{ .Labels.severity }})
        namespace: {{ .Labels.namespace }}
        instance: {{ .Labels.instance }}
        summary: {{ .Annotations.summary }}
        description: {{ .Annotations.description }}
        {{ end }}`);
    }

    const webhookConfigs: string[] = [];
    if (this.needsDingTalkConfig()) {
      const adapterUrl = String(this.form.value.dingTalkAdapterUrl || '').trim();
      webhookConfigs.push(`    webhookConfigs:
    - url: ${this.yamlQuote(adapterUrl)}
      sendResolved: true`);
    }

    const receiverBlocks = [emailConfigs.join('\n'), webhookConfigs.join('\n')].filter(Boolean).join('\n');

    docs.push(`apiVersion: monitoring.coreos.com/v1alpha1
kind: AlertmanagerConfig
metadata:
  name: polardbx-alert-receiver-${receiverName}
  namespace: ${targetNamespace}
  labels:
    polardbx.com/alertmanager: ${alertmanagerName}
    app.kubernetes.io/managed-by: polardbx-dashboard
spec:
  route:
    receiver: ${receiverName}
    groupBy: ["namespace", "alertname"]
    groupWait: "30s"
    groupInterval: "5m"
    repeatInterval: "12h"
  receivers:
  - name: ${receiverName}
${receiverBlocks}`);

    return docs.join('\n---\n');
  }

  private yamlQuote(value: string): string {
    const raw = value ?? '';
    return `'${raw.replace(/'/g, `''`)}'`;
  }

  private parseEmailRecipients(raw: string): string[] {
    return raw
      .split('\n')
      .map(line => line.trim())
      .filter(Boolean);
  }

  getYamlFilename(): string {
    return `alertmanager-${this.form.value.receiverName}-config.yaml`;
  }

  sendTestAlert(): void {
    this.testingSend = true;
    this.testResult = null;

    const labels: Record<string, string> = {
      alertname: 'polardbx_test_alert',
      severity: 'warning',
      receiver: String(this.form.value.receiverName || '').trim() || 'unknown',
      namespace: String(this.form.value.namespace || '').trim() || 'unknown',
      channel: String(this.form.value.channelType || '').trim() || 'unknown'
    };

    this.api.testAlert({
      alertmanagerUrl: String(this.form.value.alertmanagerUrl || '').trim() || undefined,
      labels
    }).subscribe({
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

    this.api.applyAlertReceiverConfig(this.buildApplyPayload()).subscribe({
      next: (resp: any) => {
        const resources = Array.isArray(resp?.resources) ? resp.resources.join(', ') : '';
        this.applyResult = {
          success: true,
          message: resources ? `已应用: ${resources}` : '告警接收器配置已成功应用'
        };
        this.message.success('告警接收器配置应用成功！');
        this.applying = false;
        this.cdr.markForCheck();
      },
      error: (error: any) => {
        this.applyResult = {
          success: false,
          message: error?.error?.message || error?.message || '配置应用失败'
        };
        this.message.error(this.applyResult.message);
        this.applying = false;
        this.cdr.markForCheck();
      }
    });
  }

  private buildApplyPayload(): any {
    const channelType = String(this.form.value.channelType || '').trim();
    const targetNamespace = String(this.form.value.namespace || '').trim();
    const receiverName = String(this.form.value.receiverName || '').trim();
    const alertmanagerNamespace = String(this.form.value.alertmanagerNamespace || '').trim() || 'polardbx-monitor';
    const alertmanagerName = String(this.form.value.alertmanagerName || '').trim() || 'main';
    const alertmanagerUrl = String(this.form.value.alertmanagerUrl || '').trim() || undefined;

    const payload: any = {
      channelType,
      targetNamespace,
      receiverName,
      alertmanagerNamespace,
      alertmanagerName,
      alertmanagerUrl,
      autoEnable: true
    };

    if (this.needsEmailConfig()) {
      payload.email = {
        smarthost: String(this.form.value.smtpHost || '').trim(),
        from: String(this.form.value.smtpFrom || '').trim(),
        username: String(this.form.value.smtpUsername || '').trim(),
        password: String(this.form.value.smtpPassword || ''),
        requireTLS: !!this.form.value.smtpTLS,
        to: this.parseEmailRecipients(String(this.form.value.emailRecipients || ''))
      };
    }

    if (this.needsDingTalkConfig()) {
      payload.dingtalk = {
        adapterUrl: String(this.form.value.dingTalkAdapterUrl || '').trim()
      };
    }

    return payload;
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
    const url = String(this.form.value.alertmanagerUrl || '').trim();
    if (!url) {
      this.message.info('未配置 Alertmanager URL');
      return;
    }
    window.open(url, '_blank');
  }

  goToAlertsManagement(): void {
    this.router.navigate(['/operations/alerts']);
  }

  finish(): void {
    this.router.navigate(['/operations/alerts']);
  }
}
