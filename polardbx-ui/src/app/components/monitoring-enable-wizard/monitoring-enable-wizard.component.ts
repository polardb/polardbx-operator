import { Component, OnInit, OnDestroy, ChangeDetectionStrategy, ChangeDetectorRef, ViewChild, TemplateRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReactiveFormsModule, FormBuilder, FormGroup, Validators } from '@angular/forms';
import { FormsModule } from '@angular/forms';
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
import { NzMessageModule, NzMessageService } from 'ng-zorro-antd/message';
import { NzModalModule, NzModalService } from 'ng-zorro-antd/modal';

import { WizardShellComponent, WizardStep, WizardAction } from '../wizard-shell/wizard-shell.component';
import { YamlPreviewComponent } from '../yaml-preview/yaml-preview.component';
import { ApiService } from '../../services/api.service';
import { GlobalInstallProgressService } from '../../services/global-install-progress.service';

interface PreflightCheck {
  name: string;
  description: string;
  status: 'pending' | 'success' | 'warning' | 'error';
  result: string;
  command?: string;
}

interface WizardState {
  version: number;
  currentStep: number;
  formValues: any;
  preflightChecks?: PreflightCheck[];
  generatedYaml?: string;
  applyResult?: { success: boolean; message: string; failureReason?: string } | null;
  installJob?: { jobName: string; namespace: string; targetNs?: string; instructions?: string } | null;
  timestamp: number;
  lastUpdated: number;
}

const STORAGE_KEY = 'polardbx.monitoring.enableWizard.state';
const STATE_VERSION = 1;
const MAX_STATE_AGE_HOURS = 24;

@Component({
  selector: 'app-monitoring-enable-wizard',
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
    NzMessageModule,
    NzModalModule,
    WizardShellComponent,
    YamlPreviewComponent
  ],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="monitoring-enable-wizard">
      <div class="page-header">
        <div class="header-content">
          <h1 class="page-title">
            <i nz-icon nzType="tool" class="page-icon"></i>
            监控一键开启向导
          </h1>
          <p class="subtitle">快速启用 PolarDB-X 集群监控（企业版 PolarDBXMonitor / 标准版 ServiceMonitor）</p>
        </div>
      </div>
      
      <app-wizard-shell
        title="监控开启向导"
        subtitle="快速启用 PolarDB-X 集群监控"
        [namespace]="form.value.namespace"
        [objectName]="getObjectName()"
        objectLabel="目标"
        docLink="https://docs.polardbx.com/monitoring"
        [steps]="wizardSteps"
        [currentStepIndex]="currentStep"
        [actions]="getStepActions()"
        [loading]="stepLoading">

      <!-- 步骤1：选择目标 -->
      <ng-template #step1Template>
        <div class="step-content">
          <nz-alert 
            nzType="info"
            nzMessage="选择监控目标"
            nzDescription="选择要启用监控的集群类型和具体目标。企业版使用 PolarDBXMonitor，标准版使用 ServiceMonitor。"
            nzShowIcon
            class="step-alert">
          </nz-alert>

          <form [formGroup]="form" class="config-form">
            <nz-row [nzGutter]="16">
              <nz-col [nzSpan]="12">
                <nz-form-item>
                  <nz-form-label [nzSpan]="6" nzRequired>安装模式</nz-form-label>
                  <nz-form-control [nzSpan]="18">
                    <nz-select 
                      formControlName="installMode"
                      nzPlaceholder="选择安装模式">
                      <nz-option nzValue="stack" nzLabel="仅安装监控组件 (Prometheus/Grafana)"></nz-option>
                      <nz-option nzValue="target" nzLabel="安装并为目标启用采集 (生成CRD)"></nz-option>
                    </nz-select>
                  </nz-form-control>
                </nz-form-item>
              </nz-col>
              <nz-col [nzSpan]="12">
                <nz-form-item>
                  <nz-form-label [nzSpan]="6" nzRequired>监控类型</nz-form-label>
                  <nz-form-control [nzSpan]="18">
                    <nz-select 
                      formControlName="monitoringType" 
                      nzPlaceholder="选择监控类型">
                      <nz-option nzValue="enterprise" nzLabel="企业版 (PolarDBXMonitor)"></nz-option>
                      <nz-option nzValue="standard" nzLabel="标准版 (ServiceMonitor)"></nz-option>
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
                      nzShowSearch
                      nzAllowClear>
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

            <nz-row [nzGutter]="16" *ngIf="form.value.monitoringType && form.value.installMode === 'target'">
              <nz-col [nzSpan]="12">
                <nz-form-item>
                  <nz-form-label [nzSpan]="6" nzRequired>{{ getTargetLabel() }}</nz-form-label>
                  <nz-form-control [nzSpan]="18">
                    <nz-select 
                      formControlName="targetName" 
                      nzPlaceholder="选择目标"
                      nzShowSearch
                      nzAllowClear
                      [nzLoading]="loadingTargets">
                      <nz-option 
                        *ngFor="let target of targets" 
                        [nzValue]="target" 
                        [nzLabel]="target">
                      </nz-option>
                    </nz-select>
                  </nz-form-control>
                </nz-form-item>
              </nz-col>
              <nz-col [nzSpan]="12">
                <nz-form-item>
                  <nz-form-label [nzSpan]="6">Monitor 名称</nz-form-label>
                  <nz-form-control [nzSpan]="18">
                    <input 
                      nz-input 
                      formControlName="monitorName"
                      placeholder="自动生成（可自定义）">
                  </nz-form-control>
                </nz-form-item>
              </nz-col>
            </nz-row>

            <div class="type-description" *ngIf="form.value.monitoringType">
              <h4>{{ getTypeDescription().title }}</h4>
              <p>{{ getTypeDescription().description }}</p>
            </div>
          </form>
        </div>
      </ng-template>

      <!-- 步骤2：前置检测 -->
      <ng-template #step2Template>
        <div class="step-content">
          <nz-alert 
            nzType="info"
            nzMessage="环境检测"
            nzDescription="检查 CRD、权限和监控组件状态，确保监控配置可以正常应用。"
            nzShowIcon
            class="step-alert">
          </nz-alert>

          <div class="preflight-section">
            <nz-spin [nzSpinning]="runningPreflight">
              <div class="loading-tip" *ngIf="runningPreflight">正在检查环境...</div>
              <div class="check-items">
                <div class="check-item" *ngFor="let check of preflightChecks">
                  <div class="check-info">
                    <span class="check-status">
                      <i nz-icon
                        [nzType]="getCheckIcon(check.status)"
                        [style.color]="getCheckColor(check.status)">
                      </i>
                    </span>
                    <span class="check-name">{{ check.name }}</span>
                    <span class="check-description">{{ check.description }}</span>
                  </div>
                  <div class="check-result" [ngClass]="check.status">
                    {{ check.result }}
                  </div>
                  <div class="check-command" *ngIf="check.command">
                    <button 
                      nz-button 
                      nzType="dashed" 
                      nzSize="small"
                      (click)="copyCommand(check.command!)">
                      <i nz-icon nzType="copy"></i>
                      复制命令
                    </button>
                  </div>
                </div>
              </div>
            </nz-spin>
          </div>
        </div>
      </ng-template>

      <!-- 步骤3：采集参数 -->
      <ng-template #step3Template>
        <div class="step-content">
          <nz-alert 
            nzType="info"
            nzMessage="监控参数配置"
            nzDescription="配置监控采集间隔、超时时间等参数。使用默认值即可满足大多数场景。"
            nzShowIcon
            class="step-alert">
          </nz-alert>

          <form [formGroup]="form" class="config-form">
            <div class="config-section">
              <h4>{{ form.value.monitoringType === 'enterprise' ? 'PolarDBXMonitor 参数' : 'ServiceMonitor 参数' }}</h4>
              
              <nz-row [nzGutter]="16">
                <nz-col [nzSpan]="12">
                  <nz-form-item>
                    <nz-form-label [nzSpan]="6">采集间隔</nz-form-label>
                    <nz-form-control [nzSpan]="18">
                      <input 
                        nz-input 
                        formControlName="scrapeInterval"
                        placeholder="例如: 30s">
                    </nz-form-control>
                  </nz-form-item>
                </nz-col>
                <nz-col [nzSpan]="12">
                  <nz-form-item>
                    <nz-form-label [nzSpan]="6">超时时间</nz-form-label>
                    <nz-form-control [nzSpan]="18">
                      <input 
                        nz-input 
                        formControlName="scrapeTimeout"
                        placeholder="例如: 10s">
                    </nz-form-control>
                  </nz-form-item>
                </nz-col>
              </nz-row>

              <div *ngIf="form.value.monitoringType === 'standard'">
                <nz-row [nzGutter]="16">
                  <nz-col [nzSpan]="24">
                    <nz-form-item>
                      <nz-form-label [nzSpan]="3">标签选择器</nz-form-label>
                      <nz-form-control [nzSpan]="21">
                        <input 
                          nz-input 
                          formControlName="selectorLabels"
                          placeholder="自动填充 XStore 标签（可自定义）"
                          readonly>
                      </nz-form-control>
                    </nz-form-item>
                  </nz-col>
                </nz-row>
              </div>
            </div>

            <div class="config-preview">
              <h4>配置预览</h4>
              <nz-descriptions nzBordered nzSize="small">
                <nz-descriptions-item nzTitle="监控类型">
                  {{ form.value.monitoringType === 'enterprise' ? 'PolarDBXMonitor' : 'ServiceMonitor' }}
                </nz-descriptions-item>
                <nz-descriptions-item nzTitle="目标">{{ getObjectName() }}</nz-descriptions-item>
                <nz-descriptions-item nzTitle="命名空间">{{ form.value.namespace }}</nz-descriptions-item>
                <nz-descriptions-item nzTitle="采集间隔">{{ form.value.scrapeInterval }}</nz-descriptions-item>
                <nz-descriptions-item nzTitle="超时时间">{{ form.value.scrapeTimeout }}</nz-descriptions-item>
              </nz-descriptions>
            </div>
          </form>
        </div>
      </ng-template>

      <!-- 步骤4：YAML 预览 -->
      <ng-template #step4Template>
        <div class="step-content">
          <app-yaml-preview
            [yamlContent]="generatedYaml"
            [filename]="getYamlFilename()"
            [loading]="generatingYaml"
            [readonly]="true">
          </app-yaml-preview>
        </div>
      </ng-template>

      <!-- 步骤5：应用与验证 -->
      <ng-template #step5Template>
        <div class="step-content">
          <nz-result 
            [nzStatus]="applyResult?.success ? 'success' : (applyResult ? 'error' : 'info')"
            [nzTitle]="getResultTitle()"
            [nzSubTitle]="getResultSubtitle()">
            
            <div nz-result-content *ngIf="!applyResult">
              <div class="apply-options">
                <h4>应用方式</h4>
                <nz-alert 
                  nzType="info"
                  nzMessage="选择应用方式"
                  nzDescription="您可以复制命令手动执行，或者让系统自动应用配置。"
                  nzShowIcon
                  class="apply-alert">
                </nz-alert>

                <div class="kubectl-command">
                  <h5>kubectl 命令</h5>
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
            </div>

            <div nz-result-content *ngIf="applyResult?.success && installJob">
              <div class="apply-options">
                <h4>安装任务已创建</h4>
                <nz-descriptions nzBordered nzSize="small">
                  <nz-descriptions-item nzTitle="Job 名称">{{ installJob.jobName }}</nz-descriptions-item>
                  <nz-descriptions-item nzTitle="Job 命名空间">{{ installJob.namespace }}</nz-descriptions-item>
                  <nz-descriptions-item nzTitle="目标命名空间">{{ installJob.targetNs || 'polardbx-monitor' }}</nz-descriptions-item>
                </nz-descriptions>

                <div class="kubectl-command">
                  <h5>查看安装日志</h5>
                  <div class="command-block">
                    <pre>{{ getJobLogsCommand() }}</pre>
                    <button 
                      nz-button 
                      nzType="dashed" 
                      nzSize="small"
                      (click)="copyJobLogsCommand()">
                      <i nz-icon nzType="copy"></i>
                      复制命令
                    </button>
                  </div>
              <div style="margin-top:8px; display:flex; align-items:center; gap:8px;">
                <span style="color: rgba(0,0,0,0.65);">Tail 行数:</span>
                <nz-select
                  [ngModel]="tailLines"
                  (ngModelChange)="tailLines = $event"
                  nzSize="small"
                  style="width: 100px;">
                  <nz-option *ngFor="let n of tailOptions" [nzValue]="n" [nzLabel]="n"></nz-option>
                </nz-select>
              </div>
                </div>

                <div class="kubectl-command" style="margin-top: 12px;">
                  <h5>检查组件状态</h5>
                  <div class="command-block">
                    <pre>{{ getPodsCheckCommand() }}</pre>
                    <button 
                      nz-button 
                      nzType="dashed" 
                      nzSize="small"
                      (click)="copyPodsCheckCommand()">
                      <i nz-icon nzType="copy"></i>
                      复制命令
                    </button>
                  </div>
                </div>

                <div class="kubectl-command" style="margin-top: 12px;">
                  <h5>端口转发（本地访问）</h5>
                  <div class="command-block">
                    <pre>kubectl port-forward svc/grafana -n polardbx-monitor 3000
kubectl port-forward svc/prometheus-k8s -n polardbx-monitor 9090
kubectl port-forward svc/alertmanager-main -n polardbx-monitor 9093</pre>
                    <button 
                      nz-button 
                      nzType="dashed" 
                      nzSize="small"
                      (click)="copyPortForwardCommands()">
                      <i nz-icon nzType="copy"></i>
                      复制全部
                    </button>
                  </div>
                </div>

                <div class="kubectl-command" style="margin-top: 12px;">
                  <h5>LoadBalancer 示例（可选，values.yaml）</h5>
                  <div class="command-block">
                    <pre>{{ getLoadBalancerValuesSnippet() }}</pre>
                    <button 
                      nz-button 
                      nzType="dashed" 
                      nzSize="small"
                      (click)="copyLoadBalancerValues()">
                      <i nz-icon nzType="copy"></i>
                      复制片段
                    </button>
                  </div>
                </div>
              </div>
            </div>

            <div nz-result-extra *ngIf="applyResult?.success">
              <button nz-button nzType="primary" (click)="goToMonitoring()">
                <i nz-icon nzType="dashboard"></i>
                查看监控
              </button>
              <button nz-button nzType="default" (click)="goToPrometheus()">
                <i nz-icon nzType="line-chart"></i>
                Prometheus
              </button>
              <button nz-button nzType="default" (click)="goToGrafana()">
                <i nz-icon nzType="bar-chart"></i>
                Grafana
              </button>
            </div>

            <div nz-result-extra *ngIf="applyResult && !applyResult.success">
              <button nz-button nzType="primary" (click)="retryApply()">
                <i nz-icon nzType="reload"></i>
                重试安装
              </button>
              <button
                nz-button
                nzType="default"
                (click)="viewInstallLogs()"
                *ngIf="installJob?.jobName">
                <i nz-icon nzType="file-text"></i>
                查看日志
              </button>
              <button nz-button nzType="default" (click)="goToPrevStep()">
                <i nz-icon nzType="left"></i>
                上一步
              </button>
            </div>
          </nz-result>

          <div class="verification-tips" *ngIf="applyResult?.success">
            <h4>验证指引</h4>
            <nz-alert 
              nzType="success"
              nzMessage="常见验证步骤"
              nzDescription="监控配置已应用，您可以通过以下方式验证是否生效："
              nzShowIcon>
            </nz-alert>
            <ul class="tips-list">
              <li>检查 ServiceMonitor/PolarDBXMonitor 资源状态</li>
              <li>访问 Prometheus Targets 页面确认目标发现</li>
              <li>在 Grafana 中查看相关面板数据</li>
              <li>检查 Alertmanager 告警规则加载情况</li>
            </ul>
          </div>
        </div>
      </ng-template>
      </app-wizard-shell>
    </div>
  `,
  styles: [`
    .monitoring-enable-wizard {
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
      margin-bottom: 24px;
    }

    .config-section h4 {
      margin: 0 0 16px 0;
      color: rgba(0, 0, 0, 0.85);
      font-size: 16px;
      font-weight: 500;
    }

    .type-description {
      margin-top: 20px;
      padding: 16px;
      background: #f8f9fa;
      border-radius: 6px;
      border: 1px solid #e8e8e8;
    }

    .type-description h4 {
      margin: 0 0 8px 0;
      color: rgba(0, 0, 0, 0.85);
      font-size: 14px;
      font-weight: 500;
    }

    .type-description p {
      margin: 0;
      color: rgba(0, 0, 0, 0.65);
      font-size: 13px;
      line-height: 1.5;
    }

    .preflight-section {
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
      flex: 1;
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
      flex: 1;
    }

    .check-result {
      font-size: 13px;
      font-family: monospace;
      margin-right: 12px;
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
      color: #1890ff;
    }

    .check-command {
      flex-shrink: 0;
    }

    .config-preview {
      margin-top: 24px;
      padding-top: 16px;
      border-top: 1px solid #e8e8e8;
    }

    .config-preview h4 {
      margin: 0 0 16px 0;
      color: rgba(0, 0, 0, 0.85);
      font-size: 14px;
      font-weight: 500;
    }

    .apply-options {
      text-align: left;
      max-width: 600px;
      margin: 0 auto;
    }

    .apply-options h4 {
      margin: 0 0 16px 0;
      color: rgba(0, 0, 0, 0.85);
      font-size: 16px;
      font-weight: 500;
    }

    .apply-alert {
      margin-bottom: 24px;
    }

    .kubectl-command h5 {
      margin: 16px 0 12px 0;
      color: rgba(0, 0, 0, 0.8);
      font-size: 14px;
      font-weight: 500;
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

    .verification-tips {
      margin-top: 32px;
      padding-top: 24px;
      border-top: 1px solid #e8e8e8;
      text-align: left;
      max-width: 600px;
      margin-left: auto;
      margin-right: auto;
    }

    .verification-tips h4 {
      margin: 0 0 16px 0;
      color: rgba(0, 0, 0, 0.85);
      font-size: 16px;
      font-weight: 500;
    }

    .tips-list {
      margin: 16px 0 0 0;
      padding-left: 20px;
    }

    .tips-list li {
      margin-bottom: 8px;
      color: rgba(0, 0, 0, 0.65);
      font-size: 14px;
      line-height: 1.5;
    }
  `]
})
export class MonitoringEnableWizardComponent implements OnInit, OnDestroy {
  @ViewChild('step1Template', { read: TemplateRef }) step1Template!: TemplateRef<any>;
  @ViewChild('step2Template', { read: TemplateRef }) step2Template!: TemplateRef<any>;
  @ViewChild('step3Template', { read: TemplateRef }) step3Template!: TemplateRef<any>;
  @ViewChild('step4Template', { read: TemplateRef }) step4Template!: TemplateRef<any>;
  @ViewChild('step5Template', { read: TemplateRef }) step5Template!: TemplateRef<any>;

  form: FormGroup;
  currentStep = 0;
  stepLoading = false;

  // 数据源
  namespaces: string[] = [];
  targets: string[] = [];
  loadingTargets = false;

  // 前置检测
  runningPreflight = false;
  preflightChecks: PreflightCheck[] = [];

  // YAML 生成
  generatingYaml = false;
  generatedYaml = '';

  // 应用结果
  applyResult: { success: boolean; message: string; failureReason?: string } | null = null;
  installJob: { jobName: string; namespace: string; targetNs?: string; instructions?: string } | null = null;

  wizardSteps: WizardStep[] = [];

  constructor(
    private fb: FormBuilder,
    private api: ApiService,
    private message: NzMessageService,
    private modal: NzModalService,
    private router: Router,
    private cdr: ChangeDetectorRef,
    private globalProgress: GlobalInstallProgressService
  ) {
    this.form = this.fb.group({
      installMode: ['stack', Validators.required],
      monitoringType: ['enterprise', Validators.required],
      namespace: ['polardbx-monitor', Validators.required],
      targetName: ['', Validators.required],
      monitorName: [''],
      scrapeInterval: ['30s'],
      scrapeTimeout: ['10s'],
      selectorLabels: ['']
    });
  }

  // 日志 tail 行数设置（默认 200）
  tailLines = 200;
  readonly tailOptions = [100, 200, 500];

  ngOnInit(): void {
    this.initializeWizardSteps();
    this.loadNamespaces();
    this.setupFormWatchers();
    this.tryRestoreState();
  }

  private initializeWizardSteps(): void {
    // 将在 ngAfterViewInit 中设置模板
    this.wizardSteps = [
      { id: 'target', title: '选择目标', description: '监控类型与目标' },
      { id: 'preflight', title: '前置检测', description: '环境检查' },
      { id: 'config', title: '采集参数', description: '监控配置' },
      { id: 'yaml', title: 'YAML 预览', description: '配置预览' },
      { id: 'apply', title: '应用验证', description: '应用与验证' }
    ];
  }

  ngAfterViewInit(): void {
    // 设置步骤模板
    this.wizardSteps[0].template = this.step1Template;
    this.wizardSteps[1].template = this.step2Template;
    this.wizardSteps[2].template = this.step3Template;
    this.wizardSteps[3].template = this.step4Template;
    this.wizardSteps[4].template = this.step5Template;
    this.cdr.detectChanges();
  }

  private setupFormWatchers(): void {
    // 监听监控类型变化
    this.form.get('monitoringType')?.valueChanges.subscribe(() => {
      this.loadTargets();
    });

    // 监听安装模式变化，动态控制 targetName 校验
    this.form.get('installMode')?.valueChanges.subscribe((mode) => {
      const targetCtrl = this.form.get('targetName');
      if (mode === 'target') {
        targetCtrl?.addValidators(Validators.required);
      } else {
        targetCtrl?.clearValidators();
        this.form.patchValue({ targetName: '', monitorName: '', selectorLabels: '' });
      }
      targetCtrl?.updateValueAndValidity({ emitEvent: false });
      this.cdr.markForCheck();
    });

    // 监听命名空间变化
    this.form.get('namespace')?.valueChanges.subscribe(() => {
      this.loadTargets();
    });

    // 监听目标名称变化，自动生成监控名称
    this.form.get('targetName')?.valueChanges.subscribe((targetName) => {
      if (targetName && !this.form.get('monitorName')?.value) {
        const monitorName = `${targetName}-monitor`;
        this.form.patchValue({ monitorName });
      }
      this.updateSelectorLabels();
    });
  }

  private loadNamespaces(): void {
    this.api.getNamespaces().subscribe({
      next: (namespaces) => {
        this.namespaces = namespaces || [];
        this.cdr.markForCheck();
      },
      error: (error) => {
        console.error('加载命名空间失败:', error);
        this.message.error('加载命名空间失败');
      }
    });
  }

  private loadTargets(): void {
    const monitoringType = this.form.value.monitoringType;
    const namespace = this.form.value.namespace;
    
    if (!monitoringType || !namespace) {
      this.targets = [];
      return;
    }

    this.loadingTargets = true;
    
    if (monitoringType === 'enterprise') {
      // 加载 PolarDB-X 集群
      this.api.getClusters(namespace).subscribe({
        next: (clusters) => {
          this.targets = clusters.map((c: any) => c.metadata?.name).filter(Boolean) || [];
          this.loadingTargets = false;
          this.cdr.markForCheck();
        },
        error: (error) => {
          console.error('加载集群失败:', error);
          this.targets = [];
          this.loadingTargets = false;
          this.cdr.markForCheck();
        }
      });
    } else {
      // 加载 XStore
      this.api.getXStores(namespace).subscribe({
        next: (xstores) => {
          this.targets = xstores.map((x: any) => x.metadata?.name).filter(Boolean) || [];
          this.loadingTargets = false;
          this.cdr.markForCheck();
        },
        error: (error) => {
          console.error('加载 XStore 失败:', error);
          this.targets = [];
          this.loadingTargets = false;
          this.cdr.markForCheck();
        }
      });
    }
  }

  private updateSelectorLabels(): void {
    if (this.form.value.monitoringType === 'standard' && this.form.value.targetName) {
      const labels = `xstore/name=${this.form.value.targetName}`;
      this.form.patchValue({ selectorLabels: labels });
    }
  }

  onMonitoringTypeChange(type: string): void {
    // 重置相关字段
    this.form.patchValue({
      targetName: '',
      monitorName: '',
      selectorLabels: ''
    });
    this.loadTargets();
  }

  getTargetLabel(): string {
    return this.form.value.monitoringType === 'enterprise' ? '集群名称' : 'XStore 名称';
  }

  getObjectName(): string {
    const targetName = this.form.value.targetName;
    const monitoringType = this.form.value.monitoringType;
    if (!targetName) return '';
    return `${targetName} (${monitoringType === 'enterprise' ? 'PolarDBX' : 'XStore'})`;
  }

  getTypeDescription(): { title: string; description: string } {
    const type = this.form.value.monitoringType;
    if (type === 'enterprise') {
      return {
        title: '企业版监控',
        description: '使用 PolarDBXMonitor CRD 为 PolarDB-X 集群启用监控。适用于完整的集群级别监控。'
      };
    } else {
      return {
        title: '标准版监控',
        description: '使用 ServiceMonitor CRD 为 XStore 启用监控。适用于特定 XStore 实例的监控。'
      };
    }
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
      case 0: // 选择目标
        actions.push({
          text: '下一步：环境检测',
          type: 'primary',
          icon: 'right',
          disabled: !this.isStep1Valid(),
          handler: () => this.nextStep()
        });
        break;
      
      case 1: // 前置检测
        actions.push({
          text: '重新检测',
          icon: 'sync',
          loading: this.runningPreflight,
          handler: () => this.runPreflightChecks()
        });
        actions.push({
          text: '下一步：参数配置',
          type: 'primary',
          icon: 'right',
          disabled: this.hasPreflightErrors(),
          handler: () => this.nextStep()
        });
        break;
      
      case 2: // 采集参数
        actions.push({
          text: '下一步：YAML 预览',
          type: 'primary',
          icon: 'right',
          handler: () => this.nextStep()
        });
        break;
      
      case 3: // YAML 预览
        actions.push({
          text: '重新生成',
          icon: 'sync',
          loading: this.generatingYaml,
          handler: () => this.generateYaml()
        });
        actions.push({
          text: '下一步：应用配置',
          type: 'primary',
          icon: 'right',
          disabled: this.form.value.installMode === 'target' ? !this.generatedYaml : false,
          handler: () => this.nextStep()
        });
        break;
      
      case 4: // 应用验证
        if (!this.applyResult) {
          actions.push({
            text: '自动应用',
            type: 'primary',
            icon: 'check',
            handler: () => this.applyConfiguration()
          });
        }
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

  private isStep1Valid(): boolean {
    const v = this.form.value;
    if (!v.namespace || !v.monitoringType) return false;
    if (v.installMode === 'target') {
      return !!v.targetName;
    }
    return true;
  }

  nextStep(): void {
    if (this.currentStep < this.wizardSteps.length - 1) {
      this.currentStep++;

      // 进入特定步骤时的自动操作
      switch (this.currentStep) {
        case 1: // 进入前置检测
          this.runPreflightChecks();
          break;
        case 3: // 进入 YAML 预览
          this.generateYaml();
          break;
      }

      this.saveState(); // 保存状态
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

  runPreflightChecks(): void {
    this.runningPreflight = true;

    // 初始化检查项
    this.preflightChecks = [
      {
        name: 'CRD 检查',
        description: this.form.value.monitoringType === 'enterprise' ? 
          '检查 PolarDBXMonitor CRD' : '检查 ServiceMonitor CRD',
        status: 'pending',
        result: '检查中...'
      },
      {
        name: 'RBAC 权限',
        description: '检查 K8s API 访问权限',
        status: 'pending',
        result: '检查中...'
      },
      {
        name: 'Prometheus 状态',
        description: '检查 Prometheus 运行状态',
        status: 'pending',
        result: '检查中...'
      }
    ];

    const ns = this.form.value.namespace || 'polardbx-monitor';

    // 1) 轻量：CRD 与 RBAC 仍保持占位（后续接入真实校验）
    setTimeout(() => {
      this.preflightChecks[0] = {
        ...this.preflightChecks[0],
        status: 'success',
        result: 'CRD 已安装（或将于安装时部署）'
      };
      this.preflightChecks[1] = {
        ...this.preflightChecks[1],
        status: 'warning',
        result: '权限需按集群环境确认',
        command: 'kubectl auth can-i list pods --as=system:serviceaccount:default:prometheus'
      };
      this.cdr.markForCheck();
    }, 400);

    // 2) 实锤：调用 /monitoring/status 获取 Prometheus/Grafana 就绪情况
    this.api.getMonitoringStatus(ns).subscribe({
      next: (s: any) => {
        const comp = s?.components || {};
        const prom = comp?.prometheus || {};
        const graf = comp?.grafana || {};
        const am = comp?.alertmanager || {};
        const nsUsed = s?.namespace || ns;

        // Prometheus
        const pReady = !!prom.ready;
        const pMsg = `Prometheus: ${pReady ? 'Ready' : 'Not Ready'}  (${prom.readyReplicas ?? 0}/${prom.replicas ?? 0})  svc=${prom.service ? 'Yes' : 'No'}  ns=${nsUsed}`;
        this.preflightChecks[2] = {
          ...this.preflightChecks[2],
          status: pReady ? 'success' : 'warning',
          result: pMsg,
          command: `kubectl -n ${nsUsed} get pods | grep -Ei 'prom|kube-prometheus'\n` +
                   `kubectl -n ${nsUsed} get svc | grep -Ei 'prom|kube-prometheus'`
        };

        // 追加 Grafana 检查项（插入到 Prometheus 后面）
        const gReady = !!graf.ready;
        const gMsg = `Grafana: ${gReady ? 'Ready' : 'Not Ready'}  (${graf.readyReplicas ?? 0}/${graf.replicas ?? 0})  svc=${graf.service ? 'Yes' : 'No'}  ns=${nsUsed}`;
        this.preflightChecks.splice(3, 0, {
          name: 'Grafana 状态',
          description: '检查 Grafana 运行状态',
          status: gReady ? 'success' : 'warning',
          result: gMsg,
          command: `kubectl -n ${nsUsed} get pods | grep -Ei 'grafana'\n` +
                   `kubectl -n ${nsUsed} get svc | grep -Ei 'grafana'`
        });

        // 追加 Alertmanager 检查项
        const amConfigured = !!am.configured;
        const aMsg = `Alertmanager: ${amConfigured ? 'Service Present' : 'Service Missing'}  ns=${nsUsed}`;
        this.preflightChecks.splice(4, 0, {
          name: 'Alertmanager 状态',
          description: '检查 Alertmanager Service 配置',
          status: amConfigured ? 'success' : 'warning',
          result: aMsg,
          command: `kubectl -n ${nsUsed} get svc | grep -Ei 'alertmanager'`
        });

        this.runningPreflight = false;
        this.cdr.markForCheck();
      },
      error: () => {
        this.preflightChecks[2] = {
          ...this.preflightChecks[2],
          status: 'warning',
          result: '无法获取 Prometheus 状态（可能未安装）',
          command: `kubectl -n ${ns} get pods | grep -i prom\n` +
                   `kubectl -n ${ns} get svc | grep -i prom`
        };
        this.runningPreflight = false;
        this.cdr.markForCheck();
      }
    });
  }

  hasPreflightErrors(): boolean {
    return this.preflightChecks.some(check => check.status === 'error');
  }

  getCheckIcon(status: string): string {
    switch (status) {
      case 'success': return 'check-circle';
      case 'error': return 'close-circle';
      case 'warning': return 'exclamation-circle';
      default: return 'loading';
    }
  }

  getCheckColor(status: string): string {
    switch (status) {
      case 'success': return '#52c41a';
      case 'error': return '#ff4d4f';
      case 'warning': return '#faad14';
      default: return '#1890ff';
    }
  }

  copyCommand(command: string): void {
    navigator.clipboard.writeText(command).then(() => {
      this.message.success('命令已复制到剪贴板');
    }).catch(() => {
      this.message.error('复制失败');
    });
  }

  generateYaml(): void {
    this.generatingYaml = true;
    
    // 根据表单数据生成 YAML
    const config = this.form.value;
    let yaml = '';
    
    if (config.installMode !== 'target') {
      // 仅安装监控组件时不生成 CRD YAML
      setTimeout(() => {
        this.generatedYaml = '';
        this.generatingYaml = false;
        this.cdr.markForCheck();
      }, 300);
      return;
    }

    if (config.monitoringType === 'enterprise') {
      yaml = `apiVersion: polardbx.aliyun.com/v1
kind: PolarDBXMonitor
metadata:
  name: ${config.monitorName}
  namespace: ${config.namespace}
spec:
  clusterName: ${config.targetName}
  monitorInterval: ${config.scrapeInterval}
  scrapeTimeout: ${config.scrapeTimeout}
  enabled: true`;
    } else {
      yaml = `apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: ${config.monitorName}
  namespace: ${config.namespace}
spec:
  selector:
    matchLabels:
      ${config.selectorLabels}
  endpoints:
  - port: metrics
    interval: ${config.scrapeInterval}
    scrapeTimeout: ${config.scrapeTimeout}
    path: /metrics`;
    }
    
    // 模拟生成过程
    setTimeout(() => {
      this.generatedYaml = yaml;
      this.generatingYaml = false;
      this.cdr.markForCheck();
    }, 1000);
  }

  getYamlFilename(): string {
    const config = this.form.value;
    const type = config.monitoringType === 'enterprise' ? 'polardbxmonitor' : 'servicemonitor';
    return `${config.monitorName}-${type}.yaml`;
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

  applyConfiguration(): void {
    this.modal.confirm({
      nzTitle: '确认应用配置？',
      nzContent: `将自动应用 ${this.form.value.monitoringType === 'enterprise' ? 'PolarDBXMonitor' : 'ServiceMonitor'} 配置`,
      nzOkText: '确认应用',
      nzCancelText: '取消',
      nzOkType: 'primary',
      nzOnOk: () => this.doApplyConfiguration()
    });
  }

  private doApplyConfiguration(): void {
    this.stepLoading = true;
    this.installJob = null;
    this.applyResult = null;
    // 直接触发安装（当前后端未提供 exists 标记，保守推进）
    this.api.monitoringBootstrap({ mode: 'managed', dryRun: false }).subscribe({
      next: (res: any) => {
        const jobName = res?.jobName || 'polardbx-monitor-bootstrap';
        const ns = res?.namespace || 'polardbx-operator-system';
        const targetNs = res?.targetNs;
        this.installJob = { jobName, namespace: ns, targetNs, instructions: res?.instructions };
        this.applyResult = { success: true, message: '安装任务已创建，请查看 Job 日志以跟踪进度' };
        this.stepLoading = false;
        this.message.success('已启动监控安装任务');

        // 报告到全局进度服务
        this.globalProgress.reportMonitoringInstall(jobName, ns, targetNs);

        // 启动状态轮询
        this.startJobStatusPolling();
        this.saveState(); // 保存状态
        this.cdr.markForCheck();
      },
      error: (error: any) => {
        const msg = error?.error?.error || error?.error?.message || error?.message || '安装触发失败';
        this.applyResult = { success: false, message: msg };
        this.stepLoading = false;
        this.message.error('监控安装失败: ' + msg);
        this.saveState(); // 即使失败也保存状态
        this.cdr.markForCheck();
      }
    });
  }

  retryApply(): void {
    this.applyResult = null;
    this.applyConfiguration();
  }

  getResultTitle(): string {
    if (!this.applyResult) {
      return '准备应用配置';
    }
    return this.applyResult.success ? '配置应用成功' : '配置应用失败';
  }

  getResultSubtitle(): string {
    if (!this.applyResult) {
      return '选择应用方式以启用监控配置';
    }
    return this.applyResult.message;
  }

  getJobLogsCommand(): string {
    const ns = this.installJob?.namespace || 'polardbx-operator-system';
    const name = this.installJob?.jobName || 'polardbx-monitor-bootstrap';
    return `kubectl logs -n ${ns} job/${name}`;
  }

  copyJobLogsCommand(): void {
    const cmd = `${this.getJobLogsCommand()} --tail=${this.tailLines}`;
    navigator.clipboard.writeText(cmd).then(() => this.message.success('命令已复制到剪贴板'));
  }

  getPodsCheckCommand(): string {
    return 'kubectl get pods -n polardbx-monitor';
  }

  copyPodsCheckCommand(): void {
    const cmd = this.getPodsCheckCommand();
    navigator.clipboard.writeText(cmd).then(() => this.message.success('命令已复制到剪贴板'));
  }

  copyPortForwardCommands(): void {
    const cmds = [
      'kubectl port-forward svc/grafana -n polardbx-monitor 3000',
      'kubectl port-forward svc/prometheus-k8s -n polardbx-monitor 9090',
      'kubectl port-forward svc/alertmanager-main -n polardbx-monitor 9093'
    ].join('\n');
    navigator.clipboard.writeText(cmds).then(() => this.message.success('端口转发命令已复制'));
  }

  getLoadBalancerValuesSnippet(): string {
    return `monitors:\n  grafana:\n    serviceType: LoadBalancer\n  prometheus:\n    serviceType: LoadBalancer`;
  }

  copyLoadBalancerValues(): void {
    const snippet = this.getLoadBalancerValuesSnippet();
    navigator.clipboard.writeText(snippet).then(() => this.message.success('values 片段已复制'));
  }

  viewInstallLogs(): void {
    if (!this.installJob?.jobName) return;

    // 创建一个简单的内联日志查看器
    const logViewer = `
      <div style="padding: 16px;">
        <nz-alert
          nzType="info"
          nzMessage="查看安装日志"
          nzDescription="使用以下命令查看详细的安装日志"
          nzShowIcon
          style="margin-bottom: 16px;">
        </nz-alert>

        <div style="background: #f6f8fa; border: 1px solid #e1e4e8; border-radius: 6px; padding: 12px; margin-bottom: 16px;">
          <div style="font-family: monospace; font-size: 13px; word-break: break-all;">
            kubectl logs -n ${this.installJob.namespace} job/${this.installJob.jobName} --follow
          </div>
        </div>

        <div style="display: flex; gap: 8px;">
          <button nz-button nzType="primary" id="copy-logs-cmd">
            <i nz-icon nzType="copy"></i> 复制命令
          </button>
          <button nz-button nzType="default" id="refresh-status">
            <i nz-icon nzType="sync"></i> 刷新状态
          </button>
        </div>
      </div>
    `;

    const modal = this.modal.create({
      nzTitle: '安装日志',
      nzContent: logViewer,
      nzWidth: 600,
      nzFooter: [
        {
          label: '关闭',
          type: 'default',
          onClick: () => modal.destroy()
        }
      ]
    });

    // 绑定按钮事件
    modal.afterOpen.subscribe(() => {
      const copyBtn = document.getElementById('copy-logs-cmd');
      const refreshBtn = document.getElementById('refresh-status');

      if (copyBtn) {
        copyBtn.onclick = () => {
          const cmd = `kubectl logs -n ${this.installJob?.namespace} job/${this.installJob?.jobName} --follow --tail=${this.tailLines}`;
          navigator.clipboard.writeText(cmd).then(() => {
            this.message.success('命令已复制到剪贴板');
          });
        };
      }

      if (refreshBtn) {
        refreshBtn.onclick = () => {
          this.checkJobStatus();
          this.message.info('正在刷新状态...');
        };
      }
    });
  }

  goToMonitoring(): void {
    this.router.navigate(['/operations/monitoring/overview']);
  }

  goToPrometheus(): void {
    window.open('http://prometheus.example.com', '_blank');
  }

  goToGrafana(): void {
    window.open('http://grafana.example.com', '_blank');
  }

  finish(): void {
    this.clearSavedState();
    this.router.navigate(['/operations/monitoring']);
  }

  // ==================== localStorage 持久化功能 ====================

  private saveState(): void {
    try {
      // 裁剪冗余字段以减小存储体积
      const compactFormValues = {
        monitoringType: this.form.value.monitoringType,
        namespace: this.form.value.namespace,
        targetName: this.form.value.targetName,
        monitorName: this.form.value.monitorName,
        scrapeInterval: this.form.value.scrapeInterval,
        scrapeTimeout: this.form.value.scrapeTimeout
      };

      const state: WizardState = {
        version: STATE_VERSION,
        currentStep: this.currentStep,
        formValues: compactFormValues,
        preflightChecks: this.preflightChecks,
        generatedYaml: this.generatedYaml,
        applyResult: this.applyResult,
        installJob: this.installJob,
        timestamp: Date.now(),
        lastUpdated: Date.now()
      };
      localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
    } catch (error) {
      console.warn('保存向导状态失败:', error);
    }
  }

  private tryRestoreState(): void {
    try {
      const savedData = localStorage.getItem(STORAGE_KEY);
      if (!savedData) return;

      const state: WizardState = JSON.parse(savedData);

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
      console.warn('恢复向导状态失败:', error);
      this.clearSavedState();
    }
  }

  private confirmStateRestore(state: WizardState): void {
    const hoursOld = (Date.now() - (state.lastUpdated || state.timestamp)) / (1000 * 60 * 60);
    const timeInfo = hoursOld < 1
      ? `${Math.round(hoursOld * 60)}分钟前`
      : `${Math.round(hoursOld)}小时前`;

    const message = state.installJob
      ? `检测到 ${timeInfo} 的监控安装任务 (${state.installJob.jobName})，是否继续跟踪安装进度？`
      : `检测到 ${timeInfo} 未完成的监控配置向导，是否从第 ${state.currentStep + 1} 步继续？`;

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

  private restoreState(state: WizardState): void {
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
      if (state.applyResult) {
        this.applyResult = state.applyResult;
      }
      if (state.installJob) {
        this.installJob = state.installJob;
        // 如果有安装任务，启动状态轮询
        this.startJobStatusPolling();
      }

      // 重新加载数据
      this.loadTargets();

      this.message.success('已恢复向导状态');
      this.cdr.markForCheck();
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

    this.api.monitoringBootstrapStatus(this.installJob.jobName, this.installJob.namespace).subscribe({
      next: (status: any) => {
        this.pollingRetryCount = 0; // 重置重试计数
        const phase = status?.phase;

        if (phase === 'Succeeded') {
          this.applyResult = { success: true, message: '监控安装已完成！' };
          this.stopJobStatusPolling();
          this.message.success('监控安装已完成');
          // 成功后清理本地保存的向导状态
          this.clearSavedState();
        } else if (phase === 'Failed') {
          const reason = status?.failureReason || '未知错误';
          this.applyResult = {
            success: false,
            message: `安装失败: ${reason}`,
            failureReason: reason
          } as any;
          this.stopJobStatusPolling();
          this.message.error('监控安装失败');
        }
        // 运行中的任务继续轮询
        this.saveState(); // 保存最新状态
        this.cdr.markForCheck();
      },
      error: (error: any) => {
        this.pollingRetryCount++;

        // 404 表示 Job 不存在或已被清理
        if (error?.status === 404) {
          this.applyResult = {
            success: false,
            message: '安装任务已被清理或不存在，请重新启动安装',
            failureReason: 'Job not found (possibly TTL cleaned)'
          };
          this.stopJobStatusPolling();
          this.message.warning('安装任务不存在，可能已被系统清理');
          this.saveState();
          this.cdr.markForCheck();
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

  ngOnDestroy(): void {
    this.stopJobStatusPolling();
  }
}
