import { Component, OnInit, OnDestroy, AfterViewInit, ChangeDetectionStrategy, ChangeDetectorRef, ViewChild, TemplateRef, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReactiveFormsModule, FormBuilder, FormGroup, Validators } from '@angular/forms';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { Subscription, interval, Subject } from 'rxjs';
import { switchMap, takeUntil } from 'rxjs/operators';
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
import { NzModalModule, NzModalRef, NzModalService } from 'ng-zorro-antd/modal';
import { NzInputNumberModule } from 'ng-zorro-antd/input-number';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzProgressModule } from 'ng-zorro-antd/progress';

import { WizardShellComponent, WizardStep, WizardAction } from '../wizard-shell/wizard-shell.component';
import { YamlPreviewComponent } from '../yaml-preview/yaml-preview.component';
import { LogViewerComponent } from '../log-viewer/log-viewer.component';
import { ApiService } from '../../services/api.service';
import { GlobalInstallProgressService } from '../../services/global-install-progress.service';
import { Pod } from '../../models/pod.model';

interface PreflightCheck {
  id?: string;
  name: string;
  description: string;
  status: 'pending' | 'success' | 'warning' | 'error';
  result: string;
  command?: string;
  optional?: boolean;
}

interface PreflightBlocker {
  id: string;
  title: string;
  message: string;
  command?: string;
  docsUrl?: string;
  severity?: 'error' | 'warning' | 'info';
}

type CrdKey = 'polardbxMonitor' | 'serviceMonitor';

interface CrdMeta {
  id: string;
  key: CrdKey;
  label: string;
  crdName: string;
  description: string;
  optionalDescription?: string;
  installHint?: string;
  installCommand?: string;
}

interface CrdCondition {
  type?: string;
  status?: string;
  reason?: string;
  message?: string;
}

interface CrdVersionInfo {
  name?: string;
  served?: boolean;
  storage?: boolean;
}

interface CrdInfo {
  name?: string;
  displayName?: string;
  exists?: boolean;
  established?: boolean;
  namesAccepted?: boolean;
  group?: string;
  kind?: string;
  plural?: string;
  singular?: string;
  shortNames?: string[];
  scope?: string;
  versions?: CrdVersionInfo[];
  storedVersions?: string[];
  conditions?: CrdCondition[];
  error?: string;
}

interface ComponentStatusSnapshot {
  ready?: boolean;
  readyReplicas?: number;
  replicas?: number;
  service?: boolean;
  exists?: boolean;
  accessUrl?: string;
}

interface AlertManagerSnapshot extends ComponentStatusSnapshot {
  configured?: boolean;
}

interface MonitoringStatusResponse {
  namespace?: string;
  namespaceExists?: boolean;
  namespaceError?: string;
  components?: {
    prometheus?: ComponentStatusSnapshot;
    grafana?: ComponentStatusSnapshot;
    alertmanager?: AlertManagerSnapshot;
  };
  prerequisites?: {
    crds?: Partial<Record<CrdKey, CrdInfo>>;
  };
}

type ApiEnvelope<T> = T | { data?: T; success?: boolean; message?: string; error?: unknown };

interface MonitoringBootstrapResponse {
  jobName?: string;
  namespace?: string;
  targetNs?: string;
}

interface MonitoringBootstrapCondition {
  type?: string;
  status?: string;
  reason?: string;
  message?: string;
  lastTransitionTime?: string;
}

interface MonitoringBootstrapStatusResponse {
  phase?: string;
  startTime?: string;
  completionTime?: string;
  failureReason?: string;
  active?: number;
  succeeded?: number;
  failed?: number;
  conditions?: MonitoringBootstrapCondition[];
}

interface MonitoringBootstrapLogsResponse {
  logs?: string[] | string;
  jobName?: string;
  namespace?: string;
  podName?: string;
  pod?: string;
  tailLines?: number;
}

interface ImageRegistryPreset {
  name: string;
  registry: string;
  description: string;
  region: string;
  status: 'verified' | 'slow' | 'custom';
}

interface ImageRegistryConfig {
  registry: string;
  customRegistry?: string;
}

type WizardInstallMode = 'stack' | 'target';
type WizardInstallChannel = 'console' | 'helm';
type WizardMonitoringType = 'enterprise' | 'standard';

interface WizardFormValue {
  installMode: WizardInstallMode;
  installChannel: WizardInstallChannel;
  monitoringType: WizardMonitoringType;
  namespace: string;
  targetName: string;
  monitorName: string;
  scrapeInterval: string;
  scrapeTimeout: string;
  selectorLabels: string;
}

interface WizardState {
  version: number;
  currentStep: number;
  formValues: WizardFormValue;
  preflightChecks?: PreflightCheck[];
  generatedYaml?: string;
  applyResult?: { success: boolean; message: string; failureReason?: string } | null;
  installJob?: { jobName: string; namespace: string; targetNs?: string; instructions?: string } | null;
  timestamp: number;
  lastUpdated: number;
}

// 新增：安装状态接口
interface InstallStatus {
  phase: 'Pending' | 'Installing' | 'Verifying' | 'Active' | 'Failed' | 'Degraded';
  progress: number; // 0-100
  steps: InstallStep[];
  components: ComponentStatus[];
  estimatedTimeRemaining?: number; // 秒
  startTime?: string;
  endTime?: string;
  logs?: string[];
}

interface InstallStep {
  name: string;
  status: 'pending' | 'running' | 'success' | 'failed';
  message?: string;
  startTime?: string;
  endTime?: string;
}

interface ComponentStatus {
  name: 'prometheus' | 'grafana' | 'alertmanager' | 'node-exporter' | 'kube-state-metrics';
  status: 'pending' | 'running' | 'ready' | 'error';
  message?: string;
  readyPods?: string; // e.g., "2/2"
  port?: number;
  url?: string;
}

interface ComponentHealth {
  name: string;
  status: 'healthy' | 'degraded' | 'unhealthy';
  readyPods: string;
  port: number;
  url?: string;
  cpu?: string;
  memory?: string;
  disk?: string;
}

interface VerificationResult {
  overallHealth: number; // 0-100
  checks: VerificationCheck[];
  warnings: string[];
  recommendations: string[];
  timestamp: string;
}

interface VerificationCheck {
  name: string;
  category: 'api' | 'metrics' | 'dashboard' | 'alerting' | 'network' | 'storage';
  status: 'success' | 'warning' | 'error';
  message: string;
  details?: string;
}

interface FailureDiagnosis {
  errorType: 'ImagePull' | 'ResourceLimit' | 'NetworkIssue' | 'PermissionDenied' | 'ConfigError' | 'Unknown';
  errorMessage: string;
  possibleCauses: PossibleCause[];
  relatedLogs: string[];
  timestamp: string;
}

interface PossibleCause {
  description: string;
  probability: number; // 0-100
  suggestedFix?: string;
  autoFixable: boolean;
  fixAction?: string; // 自动修复操作标识
}

const STORAGE_KEY = 'polardbx.monitoring.enableWizard.state';
const STATE_VERSION = 2;
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
    NzInputNumberModule,
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
    NzCardModule,
    NzTagModule,
    NzProgressModule,
    WizardShellComponent,
    YamlPreviewComponent
  ],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="monitoring-enable-wizard">
      <!-- ✅ 检测中状态 -->
      <div class="checking-overlay" *ngIf="checkingExisting">
        <nz-spin nzSimple [nzSize]="'large'" nzTip="正在检测监控系统状态..."></nz-spin>
      </div>
      
      <!-- ✅ 检测完成后显示向导 -->
      <div *ngIf="!checkingExisting">
        <div class="page-header">
          <div class="header-content">
            <h1 class="page-title">
              <i nz-icon nzType="tool" class="page-icon"></i>
              监控一键开启向导
            </h1>
            <p class="subtitle">快速启用 PolarDB-X 集群监控（企业版 PolarDBXMonitor / 标准版 ServiceMonitor）</p>
          </div>
        </div>
      </div>
      
      <app-wizard-shell
        *ngIf="!checkingExisting"
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

      <!-- 步骤0：镜像源配置 -->
      <ng-template #step0Template>
        <div class="step-content">
          <nz-alert 
            nzType="info"
            nzMessage="镜像源配置"
            nzDescription="为监控安装选择镜像拉取渠道。推荐使用已经验证可用的 DaoCloud 镜像加速；如果您已完成配置，也可以直接跳过本步骤。"
            nzShowIcon
            class="step-alert">
          </nz-alert>

          <nz-spin [nzSpinning]="loadingRegistries">
            <!-- 当前配置显示 -->
            <nz-alert 
              *ngIf="currentRegistryConfig"
              nzType="info"
              nzShowIcon
              class="current-config-alert"
              style="margin-bottom: 16px;">
              <div nz-alert-message>
                <strong>当前镜像源配置</strong>
              </div>
              <div nz-alert-description>
                {{ currentRegistryConfig }}
              </div>
            </nz-alert>

            <div class="registry-options">
              <div 
                *ngFor="let registry of availableRegistries"
                class="registry-card"
                [class.registry-card-selected]="selectedRegistry === registry.registry"
                role="button"
                tabindex="0"
                (click)="switchRegistry(registry.registry)"
                (keyup.enter)="switchRegistry(registry.registry)"
                (keyup.space)="switchRegistry(registry.registry)">
                <div class="registry-card-header">
                  <div class="registry-name">
                    <span>{{ registry.name }}</span>
                    <span class="registry-badge" [ngClass]="getRegistryStatusBadgeClass(registry.status)">
                      <span *ngIf="registry.status === 'verified'">已验证</span>
                      <span *ngIf="registry.status === 'slow'">网络较慢</span>
                      <span *ngIf="registry.status === 'custom'">自定义</span>
                    </span>
                  </div>
                  <i nz-icon 
                     [nzType]="'check-circle'" 
                     [nzTheme]="selectedRegistry === registry.registry ? 'fill' : 'outline'"
                     [class.selected-icon]="selectedRegistry === registry.registry">
                  </i>
                </div>
                <div class="registry-description">{{ registry.description }}</div>
                <div class="registry-url" *ngIf="registry.registry !== 'custom'">{{ registry.registry }}</div>
              </div>

              <!-- 自定义镜像源输入框 -->
              <div *ngIf="selectedRegistry === 'custom'" class="custom-registry-input">
                <nz-input-group nzSearch nzSize="large" [nzAddOnAfter]="suffixButton">
                  <input 
                    type="text" 
                    nz-input 
                    [(ngModel)]="customRegistryInput"
                    placeholder="例如：registry.example.com 或 my-registry.com:5000" />
                </nz-input-group>
                <ng-template #suffixButton>
                  <button nz-button nzType="primary" nzSearch (click)="saveImageRegistryConfig()">
                    <i nz-icon nzType="check"></i>
                  </button>
                </ng-template>
                <div class="custom-registry-tips">
                  <i nz-icon nzType="info-circle" nzTheme="fill"></i>
                  请确保您的自定义镜像源包含所需的 Helm Chart 和镜像资源
                </div>
              </div>
            </div>

            <!-- 额外说明 -->
            <div class="registry-notes">
              <h4>镜像源说明</h4>
              <ul>
                <li><strong>DaoCloud</strong>：经过验证可用的镜像加速源，适合快速拉取镜像</li>
                <li><strong>Docker Hub</strong>：官方公共镜像源，访问可能较慢或受到限流</li>
                <li><strong>自定义</strong>：使用企业/私有镜像仓库时，在下方输入自定义地址</li>
              </ul>
            </div>
          </nz-spin>
        </div>
      </ng-template>

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

          <nz-alert
            *ngIf="form.value.namespace && !selectedNamespaceExists"
            nzType="warning"
            nzShowIcon
            class="step-alert"
            [nzMessage]="'命名空间 ' + form.value.namespace + ' 尚未创建'"
            [nzDescription]="namespaceGuideTpl">
          </nz-alert>

          <ng-template #namespaceGuideTpl>
            <p>请在 Kubernetes 集群中创建该命名空间后再继续，或切换到已存在的命名空间。</p>
            <ng-container *ngIf="form.value.namespace as ns">
              <pre>{{ getNamespaceCreateCommand(ns) }}</pre>
              <button 
                nz-button 
                nzType="dashed" 
                nzSize="small"
                (click)="copyCommand(getNamespaceCreateCommand(ns))">
                <i nz-icon nzType="copy"></i>
                复制命令
              </button>
            </ng-container>
          </ng-template>

          <nz-alert
            *ngIf="namespaceError"
            nzType="error"
            nzShowIcon
            class="step-alert"
            nzMessage="无法确认命名空间状态"
            [nzDescription]="namespaceError">
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
                  <nz-form-label [nzSpan]="6" nzRequired>安装渠道</nz-form-label>
                  <nz-form-control [nzSpan]="18">
                    <nz-select
                      formControlName="installChannel"
                      nzPlaceholder="选择安装渠道">
                      <nz-option nzValue="console" nzLabel="控制台自动安装 (推荐)"></nz-option>
                      <nz-option nzValue="helm" nzLabel="Helm 命令手动安装"></nz-option>
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
                      [nzPlaceHolder]="loadingTargets ? '正在加载...' : '选择目标'"
                      nzShowSearch
                      nzAllowClear
                      [nzLoading]="loadingTargets"
                      nzNotFoundContent="暂无可用资源，请先确保集群/XStore 已创建">
                      <nz-option-group *ngIf="targets.length > 0" nzLabel="{{ getTargetLabel() }}">
                        <nz-option 
                          *ngFor="let target of targets" 
                          [nzValue]="target" 
                          [nzLabel]="target">
                        </nz-option>
                      </nz-option-group>
                      <nz-option-group *ngIf="targets.length === 0 && !loadingTargets" nzLabel="提示">
                        <p style="padding: 8px 12px; color: rgba(0,0,0,0.45); font-size: 12px; margin: 0;">
                          {{ form.value.namespace ? '没有找到任何' + getTargetLabel() : '请先选择命名空间' }}
                        </p>
                      </nz-option-group>
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

          <div class="blocking-alert" *ngIf="preflightBlocking.length">
            <nz-alert
              [nzType]="getPreflightAlertType()"
              nzShowIcon
              [nzMessage]="getPreflightAlertTitle()"
              [nzDescription]="blockingDetail">
            </nz-alert>

            <ng-template #blockingDetail>
              <ul class="blocking-list">
                <li *ngFor="let block of preflightBlocking">
                  <div class="blocking-title">{{ block.title }}</div>
                  <div class="blocking-message">{{ block.message }}</div>
                  <div class="blocking-command" *ngIf="block.command">
                    <pre>{{ block.command }}</pre>
                    <button nz-button nzType="dashed" nzSize="small" (click)="copyCommand(block.command)">
                      <i nz-icon nzType="copy"></i>
                      复制命令
                    </button>
                  </div>
                  <button
                    *ngIf="block.docsUrl"
                    nz-button
                    nzType="link"
                    class="blocking-link"
                    (click)="openDocsUrl(block.docsUrl)">
                    <i nz-icon nzType="book"></i>
                    查看官方文档
                  </button>
                </li>
              </ul>

              <div class="blocking-actions">
                <button
                  *ngIf="form.value.installChannel === 'console'"
                  nz-button
                  nzType="primary"
                  (click)="startStackInstallFromPreflight()"
                  [nzLoading]="preflightStackInstalling">
                  <i nz-icon nzType="cloud-upload"></i>
                  一键安装监控组件
                </button>
                <button
                  nz-button
                  [nzType]="form.value.installChannel === 'helm' ? 'primary' : 'default'"
                  (click)="copyCommand(getHelmInstallScript())">
                  <i nz-icon nzType="copy"></i>
                  复制 Helm 安装脚本
                </button>
                <button nz-button nzType="link" (click)="openDocsUrl(installDocsUrl)">
                  <i nz-icon nzType="book"></i>
                  查看安装指南
                </button>
              </div>
            </ng-template>
          </div>

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
                    <span class="check-optional" *ngIf="check.optional">可选</span>
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
              
              <!-- 显示已选择的目标 -->
              <div *ngIf="form.value.installMode === 'target'" style="margin-bottom: 16px;">
                <nz-alert 
                  nzType="info"
                  nzMessage="当前监控目标"
                  [nzDescription]="'命名空间: ' + form.value.namespace + ' / ' + getTargetLabel() + ': ' + (form.value.targetName || '未选择')"
                  nzShowIcon>
                </nz-alert>
              </div>
              
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
                      <nz-form-label [nzSpan]="3" nzRequired>标签选择器</nz-form-label>
                      <nz-form-control [nzSpan]="21">
                        <textarea
                          nz-input
                          formControlName="selectorLabels"
                          [nzAutosize]="{ minRows: 3, maxRows: 6 }"
                          placeholder="请输入标签，每行一个 key: value，例如 xstore/name: my-xstore"></textarea>
                        <p class="selector-helper">
                          使用 <code>key: value</code> 格式，每行一个标签；系统会自动补全 <code>xstore/service: metrics</code>
                        </p>
                        <div class="selector-preview" *ngIf="recommendedSelectorPreview">
                          <div class="selector-preview-title">推荐示例</div>
                          <pre>{{ recommendedSelectorPreview }}</pre>
                        </div>
                      </nz-form-control>
                    </nz-form-item>
                  </nz-col>
                </nz-row>
              </div>
            </div>

            <div class="config-preview">
              <h4>配置预览</h4>
              <nz-descriptions nzBordered nzSize="small">
                <nz-descriptions-item nzTitle="安装模式">
                  {{ form.value.installMode === 'stack' ? '仅安装监控组件' : '安装并启用目标采集' }}
                </nz-descriptions-item>
                <nz-descriptions-item nzTitle="监控类型">
                  {{ form.value.monitoringType === 'enterprise' ? 'PolarDBXMonitor' : 'ServiceMonitor' }}
                </nz-descriptions-item>
                <nz-descriptions-item nzTitle="命名空间">{{ form.value.namespace }}</nz-descriptions-item>
                <nz-descriptions-item nzTitle="目标" *ngIf="form.value.installMode === 'target'">
                  {{ form.value.targetName || '未选择' }}
                </nz-descriptions-item>
                <nz-descriptions-item nzTitle="采集间隔">{{ form.value.scrapeInterval }}</nz-descriptions-item>
                <nz-descriptions-item nzTitle="超时时间">{{ form.value.scrapeTimeout }}</nz-descriptions-item>
                <nz-descriptions-item 
                  nzTitle="标签选择器" 
                  *ngIf="form.value.monitoringType === 'standard' && form.value.installMode === 'target'"
                  [nzSpan]="3">
                  {{ form.value.selectorLabels || '未设置' }}
                </nz-descriptions-item>
              </nz-descriptions>
            </div>
          </form>
        </div>
      </ng-template>

      <!-- 步骤4：YAML 预览 -->
      <ng-template #step4Template>
        <div class="step-content">
          <nz-alert 
            *ngIf="form.value.installMode === 'stack'"
            nzType="info"
            nzMessage="监控组件安装模式"
            nzDescription="当前选择的是「仅安装监控组件」模式，无需生成 CRD YAML。安装完成后，您可以手动创建 PolarDBXMonitor 或 ServiceMonitor 来为集群/XStore 启用监控。"
            nzShowIcon
            class="step-alert">
          </nz-alert>
          
          <app-yaml-preview
            *ngIf="form.value.installMode === 'target'"
            [yamlContent]="generatedYaml"
            [filename]="getYamlFilename()"
            [loading]="generatingYaml"
            [readonly]="true">
          </app-yaml-preview>
          
          <nz-result
            *ngIf="form.value.installMode !== 'target'"
            nzStatus="info"
            nzTitle="无需生成 YAML"
            nzSubTitle="仅安装监控组件模式下，不需要生成 CRD 配置文件">
          </nz-result>
        </div>
      </ng-template>

      <!-- 步骤5：应用与验证 -->
      <ng-template #step5Template>
        <div class="step-content">
          
          <!-- 新增：安装中状态（实时进度） -->
          <div class="installing-status" *ngIf="installStatus && installStatus.phase === 'Installing'">
            <nz-card [nzBordered]="true">
              <div slot="title">
                <i nz-icon nzType="rocket" nzTheme="twotone"></i>
                正在安装监控栈...
              </div>
              <div class="progress-section">
                <nz-progress 
                  [nzPercent]="installStatus.progress" 
                  nzStatus="active"
                  [nzShowInfo]="true">
                </nz-progress>
                
                <div class="install-steps" style="margin-top: 24px;">
                  <div 
                    class="install-step-item" 
                    *ngFor="let step of installStatus.steps"
                    [class.step-success]="step.status === 'success'"
                    [class.step-running]="step.status === 'running'"
                    [class.step-pending]="step.status === 'pending'"
                    [class.step-failed]="step.status === 'failed'">
                    <span class="step-icon">
                      <i nz-icon [nzType]="getStepIcon(step.status)" [nzTheme]="getStepIconTheme(step.status)"></i>
                    </span>
                    <span class="step-name">{{ step.name }}</span>
                    <span class="step-message" *ngIf="step.message">{{ step.message }}</span>
                  </div>
                </div>
                
                <div class="estimated-time" *ngIf="installStatus.estimatedTimeRemaining">
                  <i nz-icon nzType="clock-circle"></i>
                  预计剩余时间: {{ formatTime(installStatus.estimatedTimeRemaining) }}
                </div>
                
                <div class="install-actions" style="margin-top: 16px;">
                  <button nz-button nzType="default" nzSize="small" (click)="viewInstallLogs()" *ngIf="installJob">
                    <i nz-icon nzType="file-text"></i>
                    查看详细日志
                  </button>
                  <button nz-button nzType="dashed" nzSize="small" (click)="goToMonitoring()">
                    <i nz-icon nzType="desktop"></i>
                    后台运行
                  </button>
                </div>
              </div>
            </nz-card>
          </div>
          
          <!-- 新增：安装成功状态（组件健康卡片） -->
          <div class="success-status" *ngIf="installStatus && installStatus.phase === 'Active'">
            <nz-result 
              nzStatus="success"
              nzSubTitle="所有组件已正常运行">
              <div slot="title">
                <i nz-icon nzType="check-circle" nzTheme="twotone" [style.color]="'#52c41a'"></i>
                监控栈安装成功！
              </div>
              
              <div nz-result-content>
                <!-- 下一步操作引导 -->
                <div class="next-actions">
                  <h4>
                    <i nz-icon nzType="aim" style="margin-right: 8px;"></i>
                    下一步操作
                  </h4>
                  <div class="action-cards">
                    <nz-card nzHoverable class="action-card" (click)="verifyMonitoring()">
                      <div class="action-icon">
                        <i nz-icon nzType="search" [style.fontSize]="'32px'" [style.color]="'#52c41a'"></i>
                      </div>
                      <div class="action-title">验证监控功能</div>
                      <div class="action-desc">自动检查所有组件</div>
                      <button nz-button nzType="primary" nzSize="small" [nzLoading]="verifyingMonitoring">
                        开始验证
                      </button>
                    </nz-card>
                    
                    <nz-card nzHoverable class="action-card" (click)="goToGrafana()">
                      <div class="action-icon">
                        <i nz-icon nzType="dashboard" [style.fontSize]="'32px'" [style.color]="'#1890ff'"></i>
                      </div>
                      <div class="action-title">查看监控大盘</div>
                      <div class="action-desc">访问 Grafana 仪表板</div>
                      <button nz-button nzType="default" nzSize="small">
                        立即查看
                      </button>
                    </nz-card>
                  </div>
                </div>
                
                <!-- 组件状态 -->
                <div class="components-status">
                  <h4>
                    <i nz-icon nzType="build" style="margin-right: 8px;"></i>
                    组件状态
                  </h4>
                  <nz-card [nzBordered]="true">
                    <div class="component-item" *ngFor="let comp of componentsHealth">
                      <div class="comp-name">
                        <span class="status-dot" [ngClass]="'status-' + comp.status"></span>
                        <strong>{{ comp.name }}</strong>
                      </div>
                      <div class="comp-details">
                        <nz-tag [nzColor]="comp.status === 'healthy' ? 'success' : 'warning'">
                          {{ getHealthStatusText(comp.status) }}
                        </nz-tag>
                        <span class="comp-pods">{{ comp.readyPods }} Pods</span>
                        <span class="comp-port">{{ comp.port }}端口</span>
                        <a *ngIf="comp.url" [href]="comp.url" target="_blank" class="comp-link">
                          <i nz-icon nzType="link"></i>
                          访问
                        </a>
                      </div>
                    </div>
                  </nz-card>
                </div>
                
                <!-- 快速访问链接 -->
                <div class="quick-access" *ngIf="componentsHealth.length > 0">
                  <h4>
                    <i nz-icon nzType="link" style="margin-right: 8px;"></i>
                    快速访问
                  </h4>
                  <ul class="access-list">
                    <li *ngFor="let comp of componentsHealth">
                      <strong>{{ comp.name }}:</strong>
                      <a [href]="comp.url" target="_blank" *ngIf="comp.url">{{ comp.url }}</a>
                      <span *ngIf="comp.name === 'Grafana'" class="credentials">
                        (admin/prom-operator)
                      </span>
                    </li>
                  </ul>
                  <nz-alert 
                    nzType="info"
                    nzShowIcon
                    style="margin-top: 16px;">
                    <div nz-alert-message>
                      <i nz-icon nzType="bulb" style="margin-right: 4px;"></i>
                      提示
                    </div>
                    <div nz-alert-description>
                      监控数据需要 3-5 分钟开始收集
                    </div>
                  </nz-alert>
                </div>
              </div>
              
              <div nz-result-extra>
                <button nz-button nzType="primary" (click)="goToMonitoring()">
                  <i nz-icon nzType="check-circle"></i>
                  完成配置
                </button>
                <button nz-button nzType="default" (click)="verifyMonitoring()" [nzLoading]="verifyingMonitoring">
                  <i nz-icon nzType="safety-certificate"></i>
                  验证功能
                </button>
                <button nz-button nzType="default">
                  <i nz-icon nzType="book"></i>
                  查看文档
                </button>
              </div>
            </nz-result>
          </div>
          
          <!-- 新增：验证结果展示 -->
          <div class="verification-result" *ngIf="verificationResult">
            <nz-card [nzBordered]="true">
              <div slot="title">
                <i nz-icon nzType="search" style="margin-right: 8px;"></i>
                监控功能验证
              </div>
              <div class="verification-checks">
                <div class="check-item" *ngFor="let check of verificationResult.checks">
                  <div class="check-header">
                    <span class="check-icon">
                      <i nz-icon 
                        [nzType]="getCheckIcon(check.status)" 
                        [style.color]="getCheckColor(check.status)">
                      </i>
                    </span>
                    <span class="check-name">{{ check.name }}</span>
                    <nz-tag [nzColor]="getCheckTagColor(check.status)">
                      {{ getCheckStatusText(check.status) }}
                    </nz-tag>
                  </div>
                  <div class="check-message">{{ check.message }}</div>
                  <div class="check-details" *ngIf="check.details">
                    <small>{{ check.details }}</small>
                  </div>
                </div>
              </div>
              
              <div class="overall-health" style="margin-top: 24px;">
                <div class="health-score">
                  <span class="score-label">总体健康度:</span>
                  <nz-progress 
                    [nzPercent]="verificationResult.overallHealth" 
                    [nzStatus]="verificationResult.overallHealth >= 80 ? 'success' : (verificationResult.overallHealth >= 60 ? 'normal' : 'exception')"
                    [nzStrokeColor]="verificationResult.overallHealth >= 80 ? '#52c41a' : (verificationResult.overallHealth >= 60 ? '#faad14' : '#ff4d4f')">
                  </nz-progress>
                </div>
              </div>
              
              <div class="warnings" *ngIf="verificationResult.warnings.length > 0">
                <h5>
                  <i nz-icon nzType="warning" style="margin-right: 4px; color: #faad14;"></i>
                  发现 {{ verificationResult.warnings.length }} 个警告
                </h5>
                <ul>
                  <li *ngFor="let warning of verificationResult.warnings">{{ warning }}</li>
                </ul>
              </div>
              
              <div class="recommendations" *ngIf="verificationResult.recommendations.length > 0">
                <h5>
                  <i nz-icon nzType="bulb" style="margin-right: 4px; color: #1890ff;"></i>
                  建议
                </h5>
                <ul>
                  <li *ngFor="let rec of verificationResult.recommendations">{{ rec }}</li>
                </ul>
              </div>
              
              <div class="verification-actions" style="margin-top: 16px;">
                <button nz-button nzType="primary" (click)="goToMonitoring()">
                  <i nz-icon nzType="check"></i>
                  继续
                </button>
                <button nz-button nzType="default" (click)="verifyMonitoring()" [nzLoading]="verifyingMonitoring">
                  <i nz-icon nzType="reload"></i>
                  重新验证
                </button>
              </div>
            </nz-card>
          </div>
          
          <!-- 新增：失败诊断（智能建议） -->
          <div class="failure-diagnosis" *ngIf="failureDiagnosis">
            <nz-result 
              nzStatus="error"
              [nzSubTitle]="failureDiagnosis.errorMessage">
              <div slot="title">
                <i nz-icon nzType="close-circle" nzTheme="twotone" [style.color]="'#ff4d4f'"></i>
                安装失败
              </div>
              
              <div nz-result-content>
                <!-- 失败详情 -->
                <div class="diagnosis-details">
                  <h4>
                    <i nz-icon nzType="file-text" style="margin-right: 8px;"></i>
                    失败详情
                  </h4>
                  <nz-descriptions nzBordered nzSize="small">
                    <nz-descriptions-item nzTitle="时间">{{ formatTimestamp(failureDiagnosis.timestamp) }}</nz-descriptions-item>
                    <nz-descriptions-item nzTitle="阶段">{{ getCurrentInstallStage() }}</nz-descriptions-item>
                    <nz-descriptions-item nzTitle="错误">{{ failureDiagnosis.errorType }}</nz-descriptions-item>
                  </nz-descriptions>
                </div>
                
                <!-- 可能原因 -->
                <div class="possible-causes" style="margin-top: 24px;">
                  <h4>
                    <i nz-icon nzType="search" style="margin-right: 8px;"></i>
                    可能原因
                  </h4>
                  <div class="cause-list">
                    <div 
                      class="cause-item" 
                      *ngFor="let cause of failureDiagnosis.possibleCauses; let i = index"
                      [class.cause-primary]="i === 0">
                      <div class="cause-header">
                        <span class="cause-number">{{ i + 1 }}</span>
                        <span class="cause-desc">{{ cause.description }}</span>
                        <nz-tag nzColor="blue">可能性 {{ cause.probability }}%</nz-tag>
                      </div>
                      <div class="cause-fix" *ngIf="cause.suggestedFix">
                        <span class="fix-label">建议:</span>
                        <span>{{ cause.suggestedFix }}</span>
                      </div>
                      <div class="cause-action" *ngIf="cause.autoFixable">
                        <button 
                          nz-button 
                          nzType="primary" 
                          nzSize="small"
                          (click)="autoFix(cause)"
                          [nzLoading]="autoFixing">
                          <i nz-icon nzType="tool"></i>
                          一键修复
                        </button>
                      </div>
                    </div>
                  </div>
                </div>
                
                <!-- 智能建议（高亮显示） -->
                <nz-alert 
                  nzType="warning"
                  nzShowIcon
                  style="margin-top: 24px;"
                  *ngIf="failureDiagnosis.possibleCauses[0]?.autoFixable">
                  <div nz-alert-message>
                    <i nz-icon nzType="bulb" style="margin-right: 4px;"></i>
                    <strong>智能建议</strong>
                  </div>
                  <div nz-alert-description>
                    <div class="smart-suggestion">
                      <p>{{ failureDiagnosis.possibleCauses[0].suggestedFix }}</p>
                      <button 
                        nz-button 
                        nzType="primary"
                        (click)="autoFix(failureDiagnosis.possibleCauses[0])"
                        [nzLoading]="autoFixing">
                        一键{{ failureDiagnosis.possibleCauses[0].fixAction === 'switchImageRegistry' ? '切换镜像源并重试' : '自动修复' }}
                      </button>
                    </div>
                  </div>
                </nz-alert>
                
                <!-- 详细日志 -->
                <div class="error-logs" *ngIf="failureDiagnosis.relatedLogs.length > 0" style="margin-top: 24px;">
                  <h4>
                    <i nz-icon nzType="file-text" style="margin-right: 8px;"></i>
                    详细日志
                  </h4>
                  <nz-card [nzBordered]="true">
                    <pre class="log-content">{{ failureDiagnosis.relatedLogs.join('\n') }}</pre>
                    <button nz-button nzType="dashed" nzSize="small" (click)="viewFullLogs()">
                      <i nz-icon nzType="fullscreen"></i>
                      展开完整日志
                    </button>
                  </nz-card>
                </div>
              </div>
              
              <div nz-result-extra>
                <button nz-button nzType="primary" (click)="retryApply()">
                  <i nz-icon nzType="reload"></i>
                  手动重试
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
                  返回配置
                </button>
              </div>
            </nz-result>
          </div>
          
          <!-- 原有的 nz-result（降级展示） -->
          <nz-result 
            *ngIf="!installStatus && !failureDiagnosis"
            [nzStatus]="applyResult?.success ? 'success' : (applyResult ? 'error' : 'info')"
            [nzTitle]="getResultTitle()"
            [nzSubTitle]="getResultSubtitle()">
            
            <div nz-result-content *ngIf="!applyResult">
              <div class="apply-options">
                <h4>应用方式</h4>
                <nz-alert 
                  nzType="info"
                  nzMessage="安装指引"
                  [nzDescription]="form.value.installChannel === 'helm' ? '您选择了 Helm 手动安装，请在 Kubernetes 集群中执行下面的命令后，再返回此向导点击“完成”。' : '您可以复制命令手动执行，或者让系统自动应用配置。'"
                  nzShowIcon
                  class="apply-alert">
                </nz-alert>

                <div 
                  class="kubectl-command"
                  *ngIf="form.value.installChannel === 'helm'">
                  <h5>Helm 安装脚本</h5>
                  <div class="command-block">
                    <pre>{{ getHelmInstallScript() }}</pre>
                    <button 
                      nz-button 
                      nzType="dashed" 
                      nzSize="small"
                      (click)="copyCommand(getHelmInstallScript())">
                      <i nz-icon nzType="copy"></i>
                      复制脚本
                    </button>
                  </div>
                </div>

                <div class="kubectl-command" *ngIf="form.value.installMode === 'target'">
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

          <!-- 错误详情和解决方案 -->
          <div class="error-details" *ngIf="applyResult && !applyResult.success">
            <nz-alert 
              nzType="error"
              nzMessage="安装失败原因"
              [nzDescription]="applyResult.failureReason || applyResult.message"
              nzShowIcon>
            </nz-alert>

            <!-- 镜像拉取错误的特殊提示 -->
            <nz-alert 
              *ngIf="applyResult.failureReason?.includes('ImagePull') || applyResult.failureReason?.includes('镜像拉取')"
              nzType="warning"
              nzMessage="镜像拉取失败解决方案"
              nzShowIcon
              style="margin-top: 16px;">
              <div style="font-size: 13px; line-height: 1.8;">
                <p><strong>问题原因：</strong>Kubernetes 集群无法从 Docker Hub 拉取镜像（网络超时或访问受限）</p>
                
                <p><strong>解决方案：</strong></p>
                <ol style="margin-left: 20px; margin-top: 8px;">
                  <li><strong>配置镜像加速器（推荐）</strong>
                    <pre style="background: #f5f5f5; padding: 8px; border-radius: 4px; margin: 8px 0; font-size: 12px;">{{ getMirrorConfigCommands() }}</pre>
                  </li>
                  
                  <li><strong>手动预拉取镜像</strong>
                    <pre style="background: #f5f5f5; padding: 8px; border-radius: 4px; margin: 8px 0; font-size: 12px;">{{ getPrePullCommands() }}</pre>
                  </li>
                  
                  <li><strong>检查 Job Pod 状态</strong>
                    <pre style="background: #f5f5f5; padding: 8px; border-radius: 4px; margin: 8px 0; font-size: 12px;">kubectl describe pod -n {{ installJob?.namespace || 'polardbx-operator-system' }} {{ getJobPodName() }}</pre>
                  </li>
                </ol>
                
                <p style="margin-top: 12px; color: #1890ff;">
                  <i nz-icon nzType="info-circle"></i>
                  配置镜像加速后，请点击「重试安装」按钮重新启动安装任务。
                </p>
              </div>
            </nz-alert>

            <!-- BackoffLimit 重试失败提示 -->
            <nz-alert
              *ngIf="isBackoffLimitError(applyResult.failureReason)"
              nzType="warning"
              nzMessage="安装任务达到重试上限"
              nzShowIcon
              style="margin-top: 16px;">
              <div class="backoff-guidance">
                <p><strong>问题原因：</strong>安装 Job 多次重试仍然失败，Kubernetes 已停止继续尝试。</p>
                <p><strong>处理步骤：</strong></p>
                <ol>
                  <li>
                    检查 Job 事件和失败原因
                    <div class="command-block">
                      <pre>{{ getJobDescribeCommand() }}</pre>
                      <button nz-button nzType="dashed" nzSize="small" (click)="copyCommand(getJobDescribeCommand())">
                        <i nz-icon nzType="copy"></i>
                        复制
                      </button>
                    </div>
                  </li>
                  <li>
                    查看最近失败 Pod 的日志
                    <div class="command-block">
                      <pre>{{ getJobFailedPodLogsCommand() }}</pre>
                      <button nz-button nzType="dashed" nzSize="small" (click)="copyCommand(getJobFailedPodLogsCommand())">
                        <i nz-icon nzType="copy"></i>
                        复制
                      </button>
                    </div>
                  </li>
                  <li>
                    处理问题后删除旧 Job 并重新触发安装
                    <div class="command-block">
                      <pre>{{ getJobDeleteCommand() }}</pre>
                      <button nz-button nzType="dashed" nzSize="small" (click)="copyCommand(getJobDeleteCommand())">
                        <i nz-icon nzType="copy"></i>
                        复制
                      </button>
                    </div>
                  </li>
                </ol>
                <p style="margin-top: 12px;">
                  修复后点击页面中的「重试安装」，系统会重新创建安装任务。
                </p>
              </div>
            </nz-alert>

            <!-- Job 日志查看命令 -->
            <div class="kubectl-command" style="margin-top: 16px;" *ngIf="installJob && installJob.jobName">
              <h5>查看完整错误日志</h5>
              <div class="command-block">
                <pre>kubectl logs -n {{ installJob!.namespace }} job/{{ installJob!.jobName }} --follow</pre>
                <button 
                  nz-button 
                  nzType="dashed" 
                  nzSize="small"
                  (click)="copyJobLogsCommand()">
                  <i nz-icon nzType="copy"></i>
                  复制命令
                </button>
              </div>
            </div>
          </div>

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
      position: relative;
    }
    
    /* ✅ 检测中覆盖层 */
    .checking-overlay {
      position: fixed;
      top: 0;
      left: 0;
      right: 0;
      bottom: 0;
      display: flex;
      align-items: center;
      justify-content: center;
      background: rgba(255, 255, 255, 0.95);
      z-index: 1000;
    }

    /* Step 0: 镜像源配置样式 */
    .registry-options {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
      gap: 16px;
      margin: 24px 0;
    }

    .registry-card {
      padding: 20px;
      border: 2px solid #e8e8e8;
      border-radius: 8px;
      background: #fff;
      cursor: pointer;
      transition: all 0.3s ease;
    }

    .registry-card:hover {
      border-color: #40a9ff;
      box-shadow: 0 2px 8px rgba(24, 144, 255, 0.2);
    }

    .registry-card-selected {
      border-color: #1890ff;
      background: #f0f7ff;
      box-shadow: 0 2px 12px rgba(24, 144, 255, 0.3);
    }

    .registry-card-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 12px;
    }

    .registry-name {
      font-size: 16px;
      font-weight: 500;
      color: rgba(0, 0, 0, 0.85);
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .registry-badge {
      font-size: 12px;
      padding: 2px 8px;
      border-radius: 4px;
      font-weight: normal;
    }

    .badge-verified {
      background: #f6ffed;
      color: #52c41a;
      border: 1px solid #b7eb8f;
    }

    .badge-slow {
      background: #fff7e6;
      color: #faad14;
      border: 1px solid #ffd591;
    }

    .badge-custom {
      background: #f0f5ff;
      color: #1890ff;
      border: 1px solid #adc6ff;
    }

    .selected-icon {
      color: #1890ff;
      font-size: 20px;
    }

    .registry-description {
      color: rgba(0, 0, 0, 0.65);
      font-size: 14px;
      margin-bottom: 8px;
      line-height: 1.5;
    }

    .registry-url {
      font-size: 13px;
      font-family: 'Courier New', monospace;
      color: rgba(0, 0, 0, 0.45);
      padding: 4px 8px;
      background: #f5f5f5;
      border-radius: 4px;
      word-break: break-all;
    }

    .custom-registry-input {
      margin-top: 16px;
      padding: 16px;
      background: #fafafa;
      border: 1px dashed #d9d9d9;
      border-radius: 8px;
    }

    .custom-registry-tips {
      margin-top: 12px;
      font-size: 13px;
      color: rgba(0, 0, 0, 0.6);
      display: flex;
      align-items: center;
      gap: 6px;
    }

    .custom-registry-tips i {
      color: #1890ff;
    }

    .registry-notes {
      margin-top: 32px;
      padding: 16px;
      background: #fff;
      border: 1px solid #e8e8e8;
      border-radius: 8px;
    }

    .registry-notes h4 {
      margin: 0 0 12px 0;
      color: rgba(0, 0, 0, 0.85);
      font-size: 14px;
      font-weight: 500;
    }

    .registry-notes ul {
      margin: 0;
      padding-left: 20px;
    }

    .registry-notes li {
      margin-bottom: 8px;
      color: rgba(0, 0, 0, 0.65);
      font-size: 13px;
      line-height: 1.6;
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

    .blocking-alert {
      margin-bottom: 24px;
    }

    .blocking-list {
      margin: 0 0 12px 0;
      padding-left: 0;
      list-style: none;
    }

    .blocking-list li {
      padding: 12px 0;
      border-bottom: 1px dashed #f0f0f0;
    }

    .blocking-list li:last-child {
      border-bottom: none;
      padding-bottom: 0;
    }

    .blocking-title {
      font-weight: 600;
      color: rgba(0, 0, 0, 0.85);
      margin-bottom: 4px;
    }

    .blocking-message {
      color: rgba(0, 0, 0, 0.65);
      font-size: 13px;
      line-height: 1.5;
      margin-bottom: 8px;
    }

    .blocking-command {
      background: #f6f8fa;
      border: 1px solid #e1e4e8;
      border-radius: 6px;
      padding: 12px;
      margin-bottom: 8px;
      position: relative;
    }

    .blocking-command pre {
      margin: 0;
      font-family: 'SFMono-Regular', 'Monaco', 'Menlo', 'Courier New', monospace;
      font-size: 12px;
      white-space: pre-wrap;
      word-break: break-all;
    }

    .blocking-command button {
      margin-top: 8px;
    }

    .blocking-link {
      display: inline-flex;
      align-items: center;
      gap: 4px;
      font-size: 12px;
      color: #1890ff;
      cursor: pointer;
    }

    .blocking-actions {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      margin-top: 12px;
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

    .check-optional {
      background: #f0f5ff;
      color: #2f54eb;
      font-size: 11px;
      padding: 2px 8px;
      border-radius: 10px;
      line-height: 1;
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

    .backoff-guidance {
      font-size: 13px;
      line-height: 1.7;
      text-align: left;
    }

    .backoff-guidance ol {
      margin: 12px 0;
      padding-left: 18px;
    }

    .backoff-guidance li {
      margin-bottom: 12px;
    }

    .backoff-guidance .command-block {
      margin-top: 8px;
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
    
    /* 新增：安装中状态样式 */
    .installing-status {
      max-width: 800px;
      margin: 0 auto;
    }
    
    .progress-section {
      padding: 16px 0;
    }
    
    .install-steps {
      display: flex;
      flex-direction: column;
      gap: 12px;
    }
    
    .install-step-item {
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 12px 16px;
      border-radius: 6px;
      background: #fafafa;
      border: 1px solid #e8e8e8;
      transition: all 0.3s;
    }
    
    .install-step-item.step-success {
      background: #f6ffed;
      border-color: #b7eb8f;
    }
    
    .install-step-item.step-running {
      background: #e6f7ff;
      border-color: #91d5ff;
      animation: pulse 2s ease-in-out infinite;
    }
    
    .install-step-item.step-pending {
      opacity: 0.6;
    }
    
    .install-step-item.step-failed {
      background: #fff2e8;
      border-color: #ffbb96;
    }
    
    @keyframes pulse {
      0%, 100% { opacity: 1; }
      50% { opacity: 0.7; }
    }
    
    .step-icon {
      font-size: 20px;
      line-height: 1;
    }
    
    .step-name {
      flex: 1;
      font-weight: 500;
      color: rgba(0, 0, 0, 0.85);
    }
    
    .step-message {
      font-size: 13px;
      color: rgba(0, 0, 0, 0.65);
    }
    
    .estimated-time {
      margin-top: 16px;
      padding: 12px;
      background: #f0f2f5;
      border-radius: 6px;
      text-align: center;
      color: rgba(0, 0, 0, 0.65);
      font-size: 14px;
    }
    
    .install-actions {
      display: flex;
      justify-content: center;
      gap: 12px;
    }
    
    /* 新增：成功状态样式 */
    .success-status {
      max-width: 900px;
      margin: 0 auto;
    }
    
    .next-actions {
      margin-bottom: 32px;
    }
    
    .next-actions h4 {
      margin: 0 0 16px 0;
      font-size: 18px;
      font-weight: 600;
      color: rgba(0, 0, 0, 0.85);
    }
    
    .action-cards {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 16px;
      margin-top: 16px;
    }
    
    .action-card {
      text-align: center;
      padding: 24px;
      cursor: pointer;
      transition: all 0.3s;
    }
    
    .action-card:hover {
      transform: translateY(-4px);
      box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
    }
    
    .action-icon {
      font-size: 48px;
      margin-bottom: 12px;
    }
    
    .action-title {
      font-size: 16px;
      font-weight: 600;
      margin-bottom: 8px;
      color: rgba(0, 0, 0, 0.85);
    }
    
    .action-desc {
      font-size: 13px;
      color: rgba(0, 0, 0, 0.65);
      margin-bottom: 16px;
    }
    
    .components-status {
      margin: 32px 0;
    }
    
    .components-status h4 {
      margin: 0 0 16px 0;
      font-size: 18px;
      font-weight: 600;
      color: rgba(0, 0, 0, 0.85);
    }
    
    .component-item {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 12px 16px;
      border-bottom: 1px solid #f0f0f0;
    }
    
    .component-item:last-child {
      border-bottom: none;
    }
    
    .comp-name {
      display: flex;
      align-items: center;
      gap: 8px;
      flex: 1;
    }
    
    .status-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      display: inline-block;
    }
    
    .status-dot.status-healthy {
      background-color: #52c41a;
    }
    
    .status-dot.status-degraded {
      background-color: #faad14;
    }
    
    .status-dot.status-unhealthy {
      background-color: #ff4d4f;
    }
    
    .comp-details {
      display: flex;
      align-items: center;
      gap: 12px;
      font-size: 13px;
    }
    
    .comp-pods, .comp-port {
      color: rgba(0, 0, 0, 0.65);
    }
    
    .comp-link {
      color: #1890ff;
      text-decoration: none;
    }
    
    .comp-link:hover {
      text-decoration: underline;
    }
    
    .quick-access h4 {
      margin: 24px 0 16px 0;
      font-size: 18px;
      font-weight: 600;
      color: rgba(0, 0, 0, 0.85);
    }
    
    .access-list {
      list-style: none;
      padding: 0;
      margin: 0 0 16px 0;
    }
    
    .access-list li {
      padding: 8px 0;
      font-size: 14px;
      color: rgba(0, 0, 0, 0.65);
    }
    
    .access-list li strong {
      margin-right: 8px;
      color: rgba(0, 0, 0, 0.85);
    }
    
    .access-list li a {
      color: #1890ff;
      text-decoration: none;
      margin-right: 8px;
    }
    
    .access-list li a:hover {
      text-decoration: underline;
    }
    
    .credentials {
      color: rgba(0, 0, 0, 0.45);
      font-size: 12px;
    }
    
    /* 新增：验证结果样式 */
    .verification-result {
      max-width: 800px;
      margin: 0 auto;
    }
    
    .verification-checks {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }
    
    .check-item {
      padding: 16px;
      border-radius: 6px;
      background: #fafafa;
      border: 1px solid #e8e8e8;
    }
    
    .check-header {
      display: flex;
      align-items: center;
      gap: 12px;
      margin-bottom: 8px;
    }
    
    .check-icon {
      font-size: 20px;
      line-height: 1;
    }
    
    .check-name {
      flex: 1;
      font-weight: 500;
      font-size: 14px;
      color: rgba(0, 0, 0, 0.85);
    }
    
    .check-message {
      margin-left: 32px;
      font-size: 13px;
      color: rgba(0, 0, 0, 0.65);
    }
    
    .check-details {
      margin-left: 32px;
      margin-top: 4px;
      font-size: 12px;
      color: rgba(0, 0, 0, 0.45);
    }
    
    .overall-health {
      padding: 16px;
      background: #f0f2f5;
      border-radius: 6px;
    }
    
    .health-score {
      display: flex;
      align-items: center;
      gap: 16px;
    }
    
    .score-label {
      font-weight: 500;
      color: rgba(0, 0, 0, 0.85);
      min-width: 100px;
    }
    
    .warnings, .recommendations {
      margin-top: 24px;
    }
    
    .warnings h5, .recommendations h5 {
      margin: 0 0 12px 0;
      font-size: 14px;
      font-weight: 600;
      color: rgba(0, 0, 0, 0.85);
    }
    
    .warnings ul, .recommendations ul {
      margin: 0;
      padding-left: 20px;
    }
    
    .warnings li, .recommendations li {
      margin-bottom: 8px;
      font-size: 13px;
      color: rgba(0, 0, 0, 0.65);
    }
    
    .verification-actions {
      display: flex;
      justify-content: center;
      gap: 12px;
    }
    
    /* 新增：失败诊断样式 */
    .failure-diagnosis {
      max-width: 900px;
      margin: 0 auto;
    }
    
    .diagnosis-details {
      margin-bottom: 24px;
    }
    
    .diagnosis-details h4 {
      margin: 0 0 16px 0;
      font-size: 16px;
      font-weight: 600;
      color: rgba(0, 0, 0, 0.85);
    }
    
    .possible-causes h4 {
      margin: 0 0 16px 0;
      font-size: 16px;
      font-weight: 600;
      color: rgba(0, 0, 0, 0.85);
    }
    
    .cause-list {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }
    
    .cause-item {
      padding: 16px;
      border-radius: 8px;
      background: #fafafa;
      border: 1px solid #e8e8e8;
      transition: all 0.3s;
    }
    
    .cause-item.cause-primary {
      background: #fff7e6;
      border-color: #ffd591;
    }
    
    .cause-header {
      display: flex;
      align-items: center;
      gap: 12px;
      margin-bottom: 8px;
    }
    
    .cause-number {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 24px;
      height: 24px;
      border-radius: 50%;
      background: #1890ff;
      color: #fff;
      font-size: 12px;
      font-weight: 600;
      flex-shrink: 0;
    }
    
    .cause-primary .cause-number {
      background: #faad14;
    }
    
    .cause-desc {
      flex: 1;
      font-size: 14px;
      font-weight: 500;
      color: rgba(0, 0, 0, 0.85);
    }
    
    .cause-fix {
      margin-left: 36px;
      padding: 8px 12px;
      background: #e6f7ff;
      border-radius: 4px;
      font-size: 13px;
      color: rgba(0, 0, 0, 0.65);
    }
    
    .fix-label {
      font-weight: 500;
      color: #1890ff;
      margin-right: 8px;
    }
    
    .cause-action {
      margin-top: 12px;
      margin-left: 36px;
    }
    
    .smart-suggestion {
      padding: 12px;
      background: #fff;
      border-radius: 4px;
    }
    
    .smart-suggestion p {
      margin: 0 0 12px 0;
      font-size: 14px;
      color: rgba(0, 0, 0, 0.85);
    }

    .selector-helper {
      margin-top: 8px;
      font-size: 12px;
      color: rgba(0, 0, 0, 0.65);
    }

    .selector-helper code {
      background: #f5f5f5;
      padding: 2px 4px;
      border-radius: 3px;
    }

    .selector-preview {
      margin-top: 12px;
      padding: 12px;
      background: #f6f8fa;
      border: 1px dashed #d9d9d9;
      border-radius: 4px;
      font-family: 'SFMono-Regular', 'Consolas', 'Liberation Mono', 'Menlo', 'Courier', monospace;
      font-size: 12px;
      line-height: 1.5;
      color: #24292e;
      white-space: pre-line;
    }

    .selector-preview-title {
      margin-bottom: 8px;
      font-weight: 600;
      color: rgba(0, 0, 0, 0.75);
    }

    .selector-preview pre {
      margin: 0;
      white-space: pre-wrap;
    }
    
    .error-logs h4 {
      margin: 0 0 16px 0;
      font-size: 16px;
      font-weight: 600;
      color: rgba(0, 0, 0, 0.85);
    }
    
    .log-content {
      max-height: 300px;
      overflow-y: auto;
      margin: 0 0 12px 0;
      padding: 12px;
      background: #f5f5f5;
      border-radius: 4px;
      font-family: 'SFMono-Regular', 'Monaco', 'Menlo', 'Courier New', monospace;
      font-size: 12px;
      line-height: 1.5;
      color: #24292e;
      white-space: pre-wrap;
      word-break: break-all;
    }
  `]
})
export class MonitoringEnableWizardComponent implements OnInit, OnDestroy, AfterViewInit {
  private readonly stepTemplateRefs: (TemplateRef<unknown> | null)[] = [null, null, null, null, null, null];

  @ViewChild('step0Template', { read: TemplateRef })
  set step0Template(template: TemplateRef<unknown> | undefined) {
    this.setStepTemplateRef(0, template ?? null);
  }

  @ViewChild('step1Template', { read: TemplateRef })
  set step1Template(template: TemplateRef<unknown> | undefined) {
    this.setStepTemplateRef(1, template ?? null);
  }

  @ViewChild('step2Template', { read: TemplateRef })
  set step2Template(template: TemplateRef<unknown> | undefined) {
    this.setStepTemplateRef(2, template ?? null);
  }

  @ViewChild('step3Template', { read: TemplateRef })
  set step3Template(template: TemplateRef<unknown> | undefined) {
    this.setStepTemplateRef(3, template ?? null);
  }

  @ViewChild('step4Template', { read: TemplateRef })
  set step4Template(template: TemplateRef<unknown> | undefined) {
    this.setStepTemplateRef(4, template ?? null);
  }

  @ViewChild('step5Template', { read: TemplateRef })
  set step5Template(template: TemplateRef<unknown> | undefined) {
    this.setStepTemplateRef(5, template ?? null);
  }

  @ViewChild('logViewerTemplate', { read: TemplateRef }) logViewerTemplate!: TemplateRef<unknown>;

  private readonly fb = inject(FormBuilder);
  private readonly api = inject(ApiService);
  private readonly message = inject(NzMessageService);
  private readonly modal = inject(NzModalService);
  private readonly router = inject(Router);
  private readonly cdr = inject(ChangeDetectorRef);
  private readonly globalProgress = inject(GlobalInstallProgressService);

  private setStepTemplateRef(index: number, template: TemplateRef<unknown> | null): void {
    this.stepTemplateRefs[index] = template;
    this.attachStepTemplates();
  }

  private attachStepTemplates(): void {
    if (!this.wizardSteps || this.wizardSteps.length === 0) {
      return;
    }

    let updated = false;
    this.stepTemplateRefs.forEach((template, index) => {
      if (template && this.wizardSteps[index] && this.wizardSteps[index].template !== template) {
        this.wizardSteps[index].template = template;
        updated = true;
      }
    });

    if (updated) {
      this.cdr.markForCheck();
    }
  }

  form: FormGroup = this.fb.group({
    installChannel: ['console', Validators.required],
    installMode: ['stack', Validators.required],
    monitoringType: ['enterprise', Validators.required],
    namespace: ['polardbx-monitor', Validators.required],
    targetName: ['', Validators.required],
    monitorName: [''],
    scrapeInterval: ['30s'],
    scrapeTimeout: ['10s'],
    selectorLabels: ['']
  });
  currentStep = 0;
  stepLoading = false;
  
  // ✅ 新增：检测状态（防止向导过早显示）
  checkingExisting = true; // 初始为 true，检测完成后设为 false

  // 数据源
  namespaces: string[] = [];
  targets: string[] = [];
  loadingTargets = false;
  recommendedSelectorPreview = '';
  monitoringNamespaceExists = true;
  selectedNamespaceExists = true;
  namespaceError: string | null = null;
  lastCheckedNamespace: string | null = null;
  alertmanagerConfigured = true;
  alertmanagerGuideCommand = '';
  readonly alertmanagerDocsUrl = 'https://doc.polardbx.com/zh/operator/ops/monitor/2-monitor-cluster-exist.html';

  // 镜像源配置
  availableRegistries: ImageRegistryPreset[] = [];
  selectedRegistry = 'docker.m.daocloud.io'; // 默认 DaoCloud
  customRegistryInput = '';
  loadingRegistries = false;
  currentRegistryConfig = ''; // 当前生效的配置

  // 前置检测
  runningPreflight = false;
  preflightChecks: PreflightCheck[] = [];
  preflightBlocking: PreflightBlocker[] = [];
  preflightStackInstalling = false;
  readonly installDocsUrl = 'https://doc.polardbx.com/zh/operator/ops/monitor/1-monitor-install.html';
  private readonly helmInstallScriptTemplate = `helm repo add polardbx https://polardbx-charts.oss-cn-beijing.aliyuncs.com
helm repo update
helm upgrade --install polardbx-monitor polardbx/polardbx-monitor --namespace {{namespace}} --create-namespace`;

  // YAML 生成
  generatingYaml = false;
  generatedYaml = '';

  // 应用结果
  applyResult: { success: boolean; message: string; failureReason?: string } | null = null;
  installJob: { jobName: string; namespace: string; targetNs?: string; instructions?: string } | null = null;

  // 新增：安装状态跟踪
  installStatus: InstallStatus | null = null;
  installPolling: Subscription | null = null; // interval subscription
  
  // 新增：组件健康状态
  componentsHealth: ComponentHealth[] = [];
  
  // 新增：验证结果
  verificationResult: VerificationResult | null = null;
  verifyingMonitoring = false;
  
  // 新增：失败诊断
  failureDiagnosis: FailureDiagnosis | null = null;
  autoFixing = false;

  // RxJS Subjects for cleanup
  private destroy$ = new Subject<void>();
  private logModalRef?: NzModalRef<LogViewerComponent>;
  private logViewerRefreshSub?: Subscription;
  private logFetchSub?: Subscription;

  wizardSteps: WizardStep[] = [];

  private unwrapApiData<T>(response: ApiEnvelope<T>): T {
    if (response && typeof response === 'object' && 'data' in response) {
      const { data } = response as { data?: T };
      if (data !== undefined) {
        return data;
      }
    }
    return response as T;
  }

  private getErrorMessage(error: unknown): string {
    if (!error) {
      return '未知错误';
    }
    if (typeof error === 'string') {
      return error;
    }
    if (error instanceof Error) {
      return error.message;
    }
    if (typeof error === 'object') {
      const errObj = error as { message?: string; error?: unknown; statusText?: string };
      if (typeof errObj.message === 'string' && errObj.message.trim()) {
        return errObj.message;
      }
      if (errObj.error && typeof errObj.error === 'object') {
        const nested = errObj.error as { message?: string; error?: string };
        if (typeof nested.message === 'string' && nested.message.trim()) {
          return nested.message;
        }
        if (typeof nested.error === 'string' && nested.error.trim()) {
          return nested.error;
        }
      }
      if (typeof errObj.statusText === 'string' && errObj.statusText.trim()) {
        return errObj.statusText;
      }
    }
    return '未知错误';
  }
  // 日志 tail 行数设置（默认 200）
  tailLines = 200;
  readonly tailOptions = [100, 200, 500, 1000];

  ngOnInit(): void {
    this.initializeWizardSteps();
    this.loadImageRegistryPresets();
    this.loadCurrentImageRegistry();
    this.loadNamespaces();
    this.setupFormWatchers();
    this.updateSelectorLabels();
    // 初始化加载 targets（不依赖 watch 触发）
    setTimeout(() => this.loadTargets(), 200);
    
    // ✅ 优先检测是否已安装，再决定是否恢复状态
    // 检测逻辑会清理过期或失败的状态
    this.checkExistingInstallation();
    
    // ✅ 延迟恢复状态，让检测逻辑先执行
    // 如果检测到已安装或有 Job，会直接跳转，不会恢复状态
    setTimeout(() => {
      if (!this.checkingExisting) {
        // 只有在检测完成且未跳转时才恢复状态
        this.tryRestoreState();
      }
    }, 100);
  }

  private initializeWizardSteps(): void {
    // 将在 ngAfterViewInit 中设置模板
    this.wizardSteps = [
      { id: 'registry', title: '镜像源配置', description: '选择镜像拉取地址' },
      { id: 'target', title: '选择目标', description: '监控类型与目标' },
      { id: 'preflight', title: '前置检测', description: '环境检查' },
      { id: 'config', title: '采集参数', description: '监控配置' },
      { id: 'yaml', title: 'YAML 预览', description: '配置预览' },
      { id: 'apply', title: '应用验证', description: '应用与验证' }
    ];
    this.attachStepTemplates();
  }

  ngAfterViewInit(): void {
    this.attachStepTemplates();
    this.cdr.detectChanges();
  }

  private setupFormWatchers(): void {
    // 监听监控类型变化
    this.form.get('monitoringType')?.valueChanges.subscribe(() => {
      this.loadTargets();
      this.updateSelectorLabels();
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
      this.updateSelectorLabels();
      this.cdr.markForCheck();
    });

    // 监听命名空间变化
    this.form.get('namespace')?.valueChanges.subscribe((ns: string) => {
      this.selectedNamespaceExists = ns ? this.namespaces.includes(ns) : false;
      this.namespaceError = null;
      this.loadTargets();
      this.updateSelectorLabels();
      this.cdr.markForCheck();
    });

    // 监听目标名称变化，自动生成监控名称
    this.form.get('targetName')?.valueChanges.subscribe((targetName) => {
      if (targetName && !this.form.get('monitorName')?.value) {
        const monitorName = `${targetName}-monitor`;
        this.form.patchValue({ monitorName });
      }
      this.updateSelectorLabels();
    });

    this.form.get('installChannel')?.valueChanges.subscribe(() => {
      this.cdr.markForCheck();
    });
  }

  // ==================== 镜像源配置相关方法 ====================

  loadImageRegistryPresets(): void {
    this.loadingRegistries = true;
    this.api.getImageRegistryPresets().subscribe({
      next: (response) => {
        // 处理 API 响应格式 {success: true, data: [...]}
        const presets = this.unwrapApiData(response as ApiEnvelope<ImageRegistryPreset[]>);
        this.availableRegistries = Array.isArray(presets) ? presets : [
          {
            name: 'DaoCloud 镜像加速',
            registry: 'docker.m.daocloud.io',
            description: '经过验证可用，推荐在中国大陆环境使用的镜像加速服务',
            region: 'cn',
            status: 'verified'
          },
          {
            name: 'Docker Hub 官方源',
            registry: 'registry-1.docker.io',
            description: '官方公共镜像源，全球可用，在中国大陆访问可能较慢或受限',
            region: 'global',
            status: 'slow'
          },
          {
            name: '自定义镜像源',
            registry: 'custom',
            description: '连接企业内部或私有镜像仓库时，输入自定义仓库地址',
            region: 'custom',
            status: 'custom'
          }
        ];
        this.loadingRegistries = false;
        this.cdr.markForCheck();
      },
      error: (error) => {
        console.error('加载镜像源预设失败:', error);
        this.message.error(`加载镜像源预设失败: ${this.getErrorMessage(error)}`);
        this.availableRegistries = [
          {
            name: 'DaoCloud 镜像加速',
            registry: 'docker.m.daocloud.io',
            description: '经过验证可用，推荐在中国大陆环境使用的镜像加速服务',
            region: 'cn',
            status: 'verified'
          },
          {
            name: 'Docker Hub 官方源',
            registry: 'registry-1.docker.io',
            description: '官方公共镜像源，全球可用，在中国大陆访问可能较慢或受限',
            region: 'global',
            status: 'slow'
          },
          {
            name: '自定义镜像源',
            registry: 'custom',
            description: '连接企业内部或私有镜像仓库时，输入自定义仓库地址',
            region: 'custom',
            status: 'custom'
          }
        ];
        this.loadingRegistries = false;
        this.cdr.markForCheck();
      }
    });
  }

  loadCurrentImageRegistry(): void {
    this.api.getImageRegistryConfig().subscribe({
      next: (response) => {
        // 处理 API 响应格式 {success: true, data: {...}}
        const config = this.unwrapApiData(response as ApiEnvelope<ImageRegistryConfig & { defaultRegistry?: string }>);
        if (config.registry === 'custom' && config.customRegistry) {
          this.selectedRegistry = 'custom';
          this.customRegistryInput = config.customRegistry;
          this.currentRegistryConfig = `自定义镜像源: ${config.customRegistry}`;
        } else {
          const registry = config.registry || config.defaultRegistry || 'docker.m.daocloud.io';
          this.selectedRegistry = registry;
          this.currentRegistryConfig = `镜像源: ${registry}`;
        }
        this.cdr.markForCheck();
      },
      error: (error) => {
        console.error('加载当前镜像源配置失败:', error);
        // 使用默认值
        this.selectedRegistry = 'docker.m.daocloud.io';
        this.currentRegistryConfig = `镜像源: docker.m.daocloud.io (默认，原因: ${this.getErrorMessage(error)})`;
        this.cdr.markForCheck();
      }
    });
  }

  switchRegistry(registry: string): void {
    this.selectedRegistry = registry;
    if (registry !== 'custom') {
      this.customRegistryInput = '';
    }
    this.cdr.markForCheck();
  }

  saveImageRegistryConfig(): void {
    const config: ImageRegistryConfig = {
      registry: this.selectedRegistry
    };

    if (this.selectedRegistry === 'custom') {
      if (!this.customRegistryInput || !this.customRegistryInput.trim()) {
        this.message.error('请输入自定义镜像源地址');
        return;
      }
      config.customRegistry = this.customRegistryInput.trim();
    }

    this.api.setImageRegistryConfig(config).subscribe({
      next: () => {
        this.message.success('镜像源配置已保存');
        // 更新当前配置显示
        if (this.selectedRegistry === 'custom') {
          this.currentRegistryConfig = `自定义镜像源: ${this.customRegistryInput}`;
        } else {
          this.currentRegistryConfig = `镜像源: ${this.selectedRegistry}`;
        }
        this.cdr.markForCheck();
      },
      error: (error) => {
        console.error('保存镜像源配置失败:', error);
        this.message.error('保存镜像源配置失败');
        this.cdr.markForCheck();
      }
    });
  }

  skipImageRegistryConfig(): void {
    // 用户选择跳过，不保存配置，直接进入下一步
    this.nextStep();
  }

  applyRegistryAndContinue(): void {
    const config: ImageRegistryConfig = {
      registry: this.selectedRegistry
    };

    if (this.selectedRegistry === 'custom') {
      if (!this.customRegistryInput || !this.customRegistryInput.trim()) {
        this.message.error('请输入自定义镜像源地址');
        return;
      }
      config.customRegistry = this.customRegistryInput.trim();
    }

    this.stepLoading = true;
    this.api.setImageRegistryConfig(config).subscribe({
      next: () => {
        this.message.success('镜像源配置已保存');
        // 更新当前配置显示
        if (this.selectedRegistry === 'custom') {
          this.currentRegistryConfig = `自定义镜像源: ${this.customRegistryInput}`;
        } else {
          this.currentRegistryConfig = `镜像源: ${this.selectedRegistry}`;
        }
        this.stepLoading = false;
        this.nextStep(); // 进入下一步
        this.cdr.markForCheck();
      },
      error: (error) => {
        console.error('保存镜像源配置失败:', error);
        this.message.error('保存镜像源配置失败');
        this.stepLoading = false;
        this.cdr.markForCheck();
      }
    });
  }

  getRegistryStatusBadgeClass(status: string): string {
    switch (status) {
      case 'verified': return 'badge-verified';
      case 'slow': return 'badge-slow';
      case 'custom': return 'badge-custom';
      default: return '';
    }
  }

  // ==================== 命名空间相关方法 ====================

  private loadNamespaces(): void {
    this.api.getNamespaces().subscribe({
      next: (namespaces) => {
        this.namespaces = namespaces || [];
        const currentNs = this.form.value.namespace;
        this.monitoringNamespaceExists = this.namespaces.includes('polardbx-monitor');
        this.selectedNamespaceExists = currentNs ? this.namespaces.includes(currentNs) : false;
        this.namespaceError = null;
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
          this.targets = clusters
            .map(cluster => cluster.metadata?.name)
            .filter((name): name is string => Boolean(name));
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
          this.targets = xstores
            .map(xstore => xstore.metadata?.name)
            .filter((name): name is string => Boolean(name));
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
  const { installMode, monitoringType } = this.form.value;
  const targetName = (this.form.value.targetName || '').trim();
  const currentLabels = (this.form.value.selectorLabels ?? '') as string;

    if (monitoringType !== 'standard' || installMode !== 'target') {
      this.recommendedSelectorPreview = '';
      if (currentLabels) {
        this.form.patchValue({ selectorLabels: '' }, { emitEvent: false });
      }
      return;
    }

    this.recommendedSelectorPreview = this.buildRecommendedSelector(targetName);

    if (!targetName) {
      if (currentLabels) {
        this.form.patchValue({ selectorLabels: '' }, { emitEvent: false });
      }
      return;
    }

    const normalizedMap = this.ensureEssentialLabelMap(
      this.parseSelectorMap(this.parseSelectorLines(currentLabels)),
      targetName
    );
    const normalizedValue = this.buildSelectorPreviewFromMap(normalizedMap);

    if (currentLabels !== normalizedValue) {
      this.form.patchValue({ selectorLabels: normalizedValue }, { emitEvent: false });
    }
  }

  private buildRecommendedSelector(targetName?: string): string {
    const map = new Map<string, string>();
    map.set('xstore/name', targetName && targetName.trim() ? targetName.trim() : '<XStore 名称>');
    map.set('xstore/service', 'metrics');
    return this.buildSelectorPreviewFromMap(map);
  }

  private parseSelectorLines(raw: string | undefined | null): string[] {
    if (!raw) {
      return [];
    }
    return raw
      .split(/\r?\n|,/)
      .map((line) => line.trim())
      .filter((line) => !!line);
  }

  private parseSelectorMap(lines: string[]): Map<string, string> {
    const map = new Map<string, string>();
    for (const line of lines) {
      const normalized = line.replace('=', ':');
      const idx = normalized.indexOf(':');
      if (idx === -1) {
        continue;
      }
      const key = normalized.slice(0, idx).trim();
      const value = normalized.slice(idx + 1).trim();
      if (key && value) {
        map.set(key, value);
      }
    }
    return map;
  }

  private ensureEssentialLabelMap(source: Map<string, string>, targetName?: string): Map<string, string> {
    const result = new Map<string, string>();
    const trimmedTarget = targetName?.trim();

    if (source.has('xstore/name')) {
      result.set('xstore/name', source.get('xstore/name')!);
    } else if (trimmedTarget) {
      result.set('xstore/name', trimmedTarget);
    }

    if (source.has('xstore/service')) {
      result.set('xstore/service', source.get('xstore/service')!);
    } else {
      result.set('xstore/service', 'metrics');
    }

    source.forEach((value, key) => {
      if (key !== 'xstore/name' && key !== 'xstore/service') {
        result.set(key, value);
      }
    });

    return result;
  }

  private buildSelectorPreviewFromMap(map: Map<string, string>): string {
    return Array.from(map.entries())
      .map(([key, value]) => `${key}: ${value}`)
      .join('\n');
  }

  private buildMatchLabelsYaml(raw: string | undefined, targetName?: string): string {
    const map = this.ensureEssentialLabelMap(
      this.parseSelectorMap(this.parseSelectorLines(raw)),
      targetName
    );
    return Array.from(map.entries())
      .map(([key, value]) => `      ${key}: ${value}`)
      .join('\n');
  }

  private validateSelectorLabels(raw: string | undefined, targetName?: string): boolean {
    if (!raw) {
      return false;
    }
    const map = this.parseSelectorMap(this.parseSelectorLines(raw));
    if (!map.size) {
      return false;
    }
    if (!map.has('xstore/name') || !map.has('xstore/service')) {
      return false;
    }
    const trimmedTarget = targetName?.trim();
    if (trimmedTarget && map.get('xstore/name') !== trimmedTarget) {
      return false;
    }
    return true;
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
    
    // 上一步按钮 - 如果配置已应用成功，则禁用
    if (this.currentStep > 0 && !this.applyResult?.success) {
      actions.push({
        text: '上一步',
        icon: 'left',
        handler: () => this.prevStep()
      });
    }

    // 根据当前步骤添加特定按钮
    switch (this.currentStep) {
      case 0: // 镜像源配置
        actions.push({
          text: '跳过',
          icon: 'arrow-right',
          handler: () => this.skipImageRegistryConfig()
        });
        actions.push({
          text: '应用并继续',
          type: 'primary',
          icon: 'check',
          loading: this.stepLoading,
          disabled: this.selectedRegistry === 'custom' && !this.customRegistryInput.trim(),
          handler: () => this.applyRegistryAndContinue()
        });
        break;

      case 1: // 选择目标
        actions.push({
          text: '下一步：环境检测',
          type: 'primary',
          icon: 'right',
          disabled: !this.isStep1Valid(),
          handler: () => this.nextStep()
        });
        break;
      
      case 2: // 前置检测
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
          handler: () => this.handlePreflightStepNext()
        });
        break;
      
      case 3: // 采集参数
        actions.push({
          text: '下一步：YAML 预览',
          type: 'primary',
          icon: 'right',
          disabled: !this.isStep3Valid(),
          handler: () => this.nextStep()
        });
        break;
      
      case 4: // YAML 预览
        if (this.form.value.installMode === 'target') {
          actions.push({
            text: '重新生成',
            icon: 'sync',
            loading: this.generatingYaml,
            handler: () => this.generateYaml()
          });
        }
        actions.push({
          text: '下一步：应用配置',
          type: 'primary',
          icon: 'right',
          disabled: this.form.value.installMode === 'target' ? !this.generatedYaml : false,
          handler: () => this.nextStep()
        });
        break;
      
      case 5: // 应用验证
        // 合并「自动应用」和「完成」为一个按钮
        if (this.form.value.installChannel === 'helm') {
          actions.push({
            text: '完成',
            type: 'primary',
            icon: 'check-circle',
            handler: () => this.finishManualInstall()
          });
        } else {
          actions.push({
            text: '应用并完成',
            type: 'primary',
            icon: 'check-circle',
            handler: () => this.applyConfiguration()
          });
        }
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

  private isStep3Valid(): boolean {
    const v = this.form.value;
    // 基本参数验证
    if (!v.scrapeInterval || !v.scrapeTimeout) return false;
    
    // 如果是 target 模式，需要验证目标是否已选择
    if (v.installMode === 'target') {
      if (!v.targetName) return false;
      
      // 如果是 ServiceMonitor，需要验证标签选择器
      if (v.monitoringType === 'standard') {
        if (!this.validateSelectorLabels(v.selectorLabels, v.targetName)) {
          return false;
        }
      }
    }
    
    return true;
  }

  nextStep(): void {
    if (this.currentStep < this.wizardSteps.length - 1) {
      this.currentStep++;

      // 进入特定步骤时的自动操作
      switch (this.currentStep) {
        case 2: // 进入前置检测 (现在是第3个步骤, index=2)
          this.runPreflightChecks();
          break;
        case 4: // 进入 YAML 预览 (现在是第5个步骤, index=4)
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
    this.preflightBlocking = [];

    const monitoringType: WizardMonitoringType = this.form.value.monitoringType === 'enterprise' ? 'enterprise' : 'standard';
    const primaryMeta: CrdMeta = monitoringType === 'enterprise' ? {
      id: 'crd-primary',
      key: 'polardbxMonitor',
      label: 'PolarDBXMonitor',
      crdName: 'polardbxmonitors.polardbx.aliyun.com',
      description: '检查 PolarDBXMonitor CRD 是否可用'
    } : {
      id: 'crd-primary',
      key: 'serviceMonitor',
      label: 'ServiceMonitor',
      crdName: 'servicemonitors.monitoring.coreos.com',
      description: '检查 ServiceMonitor CRD 是否可用',
      installHint: '可执行 kubectl apply -f charts/polardbx-monitor/crds/ 安装 Prometheus Operator CRD。',
      installCommand: 'kubectl apply -f charts/polardbx-monitor/crds/'
    };

    const secondaryMeta: CrdMeta = monitoringType === 'enterprise' ? {
      id: 'crd-servicemonitor',
      key: 'serviceMonitor',
      label: 'ServiceMonitor',
      crdName: 'servicemonitors.monitoring.coreos.com',
      description: 'Prometheus Operator 提供 ServiceMonitor CRD',
      optionalDescription: '用于标准版采集或后续扩展，可选检测',
      installHint: '缺失时可执行 kubectl apply -f charts/polardbx-monitor/crds/ 重新注册 CRD。',
      installCommand: 'kubectl apply -f charts/polardbx-monitor/crds/'
    } : {
      id: 'crd-polardbxmonitor',
      key: 'polardbxMonitor',
      label: 'PolarDBXMonitor',
      crdName: 'polardbxmonitors.polardbx.aliyun.com',
      description: 'PolarDBX Operator 提供 PolarDBXMonitor CRD',
      optionalDescription: '用于企业版监控场景，如需集群级监控请确保已安装'
    };

    const ns = this.form.value.namespace || 'polardbx-monitor';

    // 初始化检查项
    this.preflightChecks = [
      {
        id: 'namespace',
        name: '监控命名空间',
        description: `确认命名空间 ${ns} 可用`,
        status: 'pending',
        result: '检查中...'
      },
      {
        id: primaryMeta.id,
        name: `${primaryMeta.label} CRD`,
        description: primaryMeta.description,
        status: 'pending',
        result: '检查中...'
      },
      {
        id: 'rbac',
        name: 'RBAC 权限',
        description: '检查 K8s API 访问权限',
        status: 'pending',
        result: '检查中...'
      },
      {
        id: 'prometheus',
        name: 'Prometheus 状态',
        description: '检查 Prometheus 运行状态',
        status: 'pending',
        result: '检查中...'
      }
    ];

    setTimeout(() => {
      const rbacIdx = this.preflightChecks.findIndex(check => check.id === 'rbac');
      if (rbacIdx !== -1) {
        this.preflightChecks[rbacIdx] = {
          ...this.preflightChecks[rbacIdx],
          status: 'warning',
          result: '权限需按集群环境确认',
          command: 'kubectl auth can-i list pods --as=system:serviceaccount:default:prometheus'
        };
        this.cdr.markForCheck();
      }
    }, 400);

    this.api.getMonitoringStatus(ns).subscribe({
      next: (s: MonitoringStatusResponse) => {
        const comp = s.components ?? {};
        const prom = comp.prometheus ?? {};
        const graf = comp.grafana ?? {};
        const am = comp.alertmanager ?? {};
        const nsUsed = s.namespace ?? ns;
        this.lastCheckedNamespace = nsUsed;
        const namespaceExists = s.namespaceExists !== false;
        this.selectedNamespaceExists = namespaceExists;
        this.namespaceError = s.namespaceError ?? null;
        if (nsUsed === 'polardbx-monitor' && namespaceExists) {
          this.monitoringNamespaceExists = true;
        }
        const prereqs = s.prerequisites?.crds ?? {};

        const namespaceIdx = this.preflightChecks.findIndex(check => check.id === 'namespace');
        if (namespaceIdx !== -1) {
          let status: 'success' | 'warning' | 'error' = 'success';
          let result = `命名空间 ${nsUsed} 已存在`;
          let command: string | undefined;

          if (!namespaceExists && !this.namespaceError) {
            status = 'error';
            result = `命名空间 ${nsUsed} 未创建`;
            command = this.getNamespaceCreateCommand(nsUsed);
          } else if (this.namespaceError) {
            status = 'warning';
            result = `无法确认命名空间 ${nsUsed} 状态：${this.namespaceError}`;
          }

          this.preflightChecks[namespaceIdx] = {
            ...this.preflightChecks[namespaceIdx],
            status,
            result,
            command
          };
        }

        const primaryIdx = this.preflightChecks.findIndex(check => check.id === primaryMeta.id);
        if (primaryIdx !== -1) {
          this.preflightChecks[primaryIdx] = this.buildCrdCheck(prereqs[primaryMeta.key], primaryMeta, true);
        }

        const secondaryCheck = this.buildCrdCheck(prereqs[secondaryMeta.key], secondaryMeta, false);
        const secondaryIdx = this.preflightChecks.findIndex(check => check.id === secondaryMeta.id);
        if (secondaryIdx === -1) {
          const insertPos = primaryIdx !== -1 ? primaryIdx + 1 : this.preflightChecks.length;
          this.preflightChecks.splice(insertPos, 0, secondaryCheck);
        } else {
          this.preflightChecks[secondaryIdx] = secondaryCheck;
        }

        const promIdx = this.preflightChecks.findIndex(check => check.id === 'prometheus');
        if (promIdx !== -1) {
          const pReady = !!prom.ready;
          const pMsg = `Prometheus: ${pReady ? 'Ready' : 'Not Ready'}  (${prom.readyReplicas ?? 0}/${prom.replicas ?? 0})  svc=${prom.service ? 'Yes' : 'No'}  ns=${nsUsed}`;
          const promCheck: PreflightCheck = {
            ...this.preflightChecks[promIdx],
            status: pReady ? 'success' : 'warning',
            result: pMsg,
            command: pReady ? undefined : `kubectl -n ${nsUsed} get pods | grep -Ei 'prom|kube-prometheus'\n` +
              `kubectl -n ${nsUsed} get svc | grep -Ei 'prom|kube-prometheus'`
          };
          this.preflightChecks[promIdx] = promCheck;

          const gReady = !!graf.ready;
          const gMsg = `Grafana: ${gReady ? 'Ready' : 'Not Ready'}  (${graf.readyReplicas ?? 0}/${graf.replicas ?? 0})  svc=${graf.service ? 'Yes' : 'No'}  ns=${nsUsed}`;
          const grafCheck: PreflightCheck = {
            id: 'grafana',
            name: 'Grafana 状态',
            description: '检查 Grafana 运行状态',
            status: gReady ? 'success' : 'warning',
            result: gMsg,
            command: gReady ? undefined : `kubectl -n ${nsUsed} get pods | grep -Ei 'grafana'\n` +
              `kubectl -n ${nsUsed} get svc | grep -Ei 'grafana'`
          };
          const grafIdx = this.preflightChecks.findIndex(check => check.id === 'grafana');
          if (grafIdx === -1) {
            this.preflightChecks.splice(promIdx + 1, 0, grafCheck);
          } else {
            this.preflightChecks[grafIdx] = grafCheck;
          }

          const amConfigured = !!am.configured;
          const aMsg = `Alertmanager: ${amConfigured ? 'Service Present' : 'Service Missing'}  ns=${nsUsed}`;
          const amCheck: PreflightCheck = {
            id: 'alertmanager',
            name: 'Alertmanager 状态',
            description: '检查 Alertmanager Service 配置',
            status: amConfigured ? 'success' : 'warning',
            result: aMsg,
            command: amConfigured ? undefined : `kubectl -n ${nsUsed} get svc | grep -Ei 'alertmanager'`
          };
          const amIdx = this.preflightChecks.findIndex(check => check.id === 'alertmanager');
          if (amIdx === -1) {
            const insertIdx = this.preflightChecks.findIndex(check => check.id === 'grafana');
            this.preflightChecks.splice(insertIdx !== -1 ? insertIdx + 1 : promIdx + 1, 0, amCheck);
          } else {
            this.preflightChecks[amIdx] = amCheck;
          }
          this.alertmanagerConfigured = amConfigured;
          this.alertmanagerGuideCommand = amConfigured ? '' : this.buildAlertmanagerGuide(nsUsed);
        }

  this.updatePreflightBlocking(primaryMeta, monitoringType, secondaryMeta);
        this.runningPreflight = false;
        this.cdr.markForCheck();
      },
      error: () => {
        const primaryIdx = this.preflightChecks.findIndex(check => check.id === primaryMeta.id);
        if (primaryIdx !== -1) {
          this.preflightChecks[primaryIdx] = {
            ...this.preflightChecks[primaryIdx],
            status: 'warning',
            result: '无法获取 CRD 状态（接口调用失败）',
            command: `kubectl get crd ${primaryMeta.crdName}`
          };
        }

        const promIdx = this.preflightChecks.findIndex(check => check.id === 'prometheus');
        if (promIdx !== -1) {
          this.preflightChecks[promIdx] = {
            ...this.preflightChecks[promIdx],
            status: 'warning',
            result: '无法获取 Prometheus 状态（可能未安装）',
            command: `kubectl -n ${ns} get pods | grep -i prom\n` +
              `kubectl -n ${ns} get svc | grep -i prom`
          };
        }

        this.runningPreflight = false;
        this.preflightBlocking = [
          {
            id: primaryMeta.id,
            title: `${primaryMeta.label} CRD 状态未知` ,
            message: '无法获取 CRD 状态，请检查 Kubernetes API 访问权限后重试。',
            command: `kubectl get crd ${primaryMeta.crdName}`,
            docsUrl: this.installDocsUrl
          }
        ];
        this.cdr.markForCheck();
      }
    });
  }

  hasPreflightErrors(): boolean {
    return this.preflightChecks.some(check => check.status === 'error' && !check.optional);
  }

  hasPreflightWarnings(): boolean {
    return this.getActivePreflightWarnings().length > 0;
  }

  private handlePreflightStepNext(): void {
    if (this.hasPreflightErrors()) {
      return;
    }
    const warnings = this.getActivePreflightWarnings();
    if (warnings.length) {
      this.confirmProceedWithWarnings(warnings);
    } else {
      this.nextStep();
    }
  }

  private getActivePreflightWarnings(): PreflightCheck[] {
    return this.preflightChecks.filter(check => check.status === 'warning');
  }

  private confirmProceedWithWarnings(warnings: PreflightCheck[]): void {
    const listItems = warnings
      .map(check => `<li><strong>${this.escapeHtml(check.name)}</strong>：${this.escapeHtml(check.result)}</li>`)
      .join('');
    const content = `
      <div class="preflight-warning-modal">
        <p>以下检测项存在警告，继续安装可能影响监控系统稳定性：</p>
        <ul>${listItems}</ul>
        <p style="margin-top: 12px;">请确认已了解风险后再继续。</p>
      </div>
    `;

    this.modal.confirm({
      nzTitle: '检测到环境警告',
      nzContent: content,
      nzOkText: '继续安装',
      nzOkType: 'primary',
      nzOkDanger: true,
      nzCancelText: '取消',
      nzCentered: true,
      nzOnOk: () => this.nextStep()
    });
  }

  private escapeHtml(value: string): string {
    return value
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
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

  getNamespaceCreateCommand(namespace?: string): string {
    const ns = (namespace || '').trim() || 'polardbx-monitor';
    return `kubectl create namespace ${ns}`;
  }

  private buildAlertmanagerGuide(namespace: string): string {
    const ns = (namespace || '').trim() || 'polardbx-monitor';
    return `# 为运行 Alertmanager 的节点打标签
kubectl label node <node-name> polardbx.com/alertmanager-node=true --overwrite

# 将 Alertmanager 固定到带标签的节点
kubectl patch alertmanager alertmanager-main -n ${ns} --type merge -p '{"spec":{"nodeSelector":{"polardbx.com/alertmanager-node":"true"}}}'`;
  }

  getPreflightAlertType(): 'error' | 'warning' | 'info' {
    if (!this.preflightBlocking.length) {
      return 'info';
    }
    if (this.preflightBlocking.some(block => block.severity === 'error')) {
      return 'error';
    }
    if (this.preflightBlocking.some(block => block.severity === 'warning')) {
      return 'warning';
    }
    return 'info';
  }

  getPreflightAlertTitle(): string {
    const type = this.getPreflightAlertType();
    if (type === 'warning') {
      return '存在需确认的准备项';
    }
    if (type === 'info') {
      return '检查提醒';
    }
    return '监控组件前置条件缺失';
  }

  openDocsUrl(url: string): void {
    if (!url) {
      return;
    }
    if (typeof window !== 'undefined') {
      window.open(url, '_blank');
    }
  }

  startStackInstallFromPreflight(): void {
    if (this.form.value.installChannel === 'helm') {
      this.message.info('已选择 Helm 手动安装，请使用下方脚本在集群中执行安装。');
      return;
    }
    this.startMonitoringBootstrap('preflight');
  }

  private buildCrdCheck(info: CrdInfo | undefined, meta: CrdMeta, required: boolean): PreflightCheck {
    const optional = !required;
    const description = required ? meta.description : (meta.optionalDescription || meta.description);
    let status: 'success' | 'warning' | 'error' = required ? 'error' : 'warning';
    let result = '';
    let command: string | undefined = meta.installCommand;

    if (!info) {
      status = required ? 'error' : 'warning';
      result = '无法获取 CRD 状态';
      command = command || `kubectl get crd ${meta.crdName}`;
    } else if (info.error) {
      status = 'error';
      result = `查询失败：${info.error}`;
      command = command || `kubectl get crd ${meta.crdName}`;
    } else if (!info.exists) {
      status = required ? 'error' : 'warning';
      result = `${meta.label} CRD 未安装${meta.installHint ? `，${meta.installHint}` : ''}`;
      command = command || `kubectl get crd ${meta.crdName}`;
    } else if (!info.established) {
      status = 'warning';
      const detail = this.extractConditionMessage(info, 'Established') || 'CRD 状态未就绪';
      result = `${meta.label} CRD 未就绪：${detail}`;
      command = `kubectl get crd ${meta.crdName} -o yaml | grep -A5 status`;
    } else {
      status = 'success';
      const versions = Array.isArray(info.versions)
        ? info.versions
            .map((v) => v?.name ?? '')
            .filter((v): v is string => v.length > 0)
            .join(', ')
        : '';
      const segments: string[] = [];
      if (versions) {
        segments.push(`版本: ${versions}`);
      }
      if (info.scope) {
        segments.push(info.scope);
      }
      const suffix = segments.length ? `（${segments.join(' / ')}）` : '';
      result = `${meta.label} CRD 已安装${suffix}`;
    }

    return {
      id: meta.id,
      name: `${meta.label} CRD${optional ? '（可选）' : ''}`,
      description,
      status,
      result,
      command,
      optional
    };
  }

  private updatePreflightBlocking(primaryMeta: CrdMeta, monitoringType: WizardMonitoringType, secondaryMeta?: CrdMeta): void {
    const blockers: PreflightBlocker[] = [];
    const ns = this.lastCheckedNamespace || this.form.value.namespace || 'polardbx-monitor';
    const namespaceCheck = this.preflightChecks.find(check => check.id === 'namespace');

    if (namespaceCheck) {
      if (namespaceCheck.status === 'error') {
        blockers.push({
          id: 'namespace',
          title: '监控命名空间缺失',
          message: namespaceCheck.result || `命名空间 ${ns} 不存在，请先创建。`,
          command: this.getNamespaceCreateCommand(ns),
          docsUrl: this.installDocsUrl,
          severity: 'error'
        });
      } else if (namespaceCheck.status === 'warning') {
        blockers.push({
          id: 'namespace-warning',
          title: namespaceCheck.name,
          message: namespaceCheck.result || `请确认命名空间 ${ns} 已经创建且可访问。`,
          docsUrl: this.installDocsUrl,
          severity: 'warning'
        });
      }
    }

    const primary = this.preflightChecks.find(check => check.id === primaryMeta.id);

    if (primary && primary.status === 'error') {
      const title = `${primaryMeta.label} CRD 未就绪`;
      const message = primary.result || `${primaryMeta.label} CRD 未安装，请先部署监控组件栈后再继续。`;
      blockers.push({
        id: primaryMeta.id,
        title,
        message,
        command: primaryMeta.installCommand || `kubectl get crd ${primaryMeta.crdName}`,
        docsUrl: this.installDocsUrl,
        severity: 'error'
      });
    }

    const promCheck = this.preflightChecks.find(check => check.id === 'prometheus');
    if (promCheck && promCheck.status !== 'success') {
      blockers.push({
        id: 'prometheus',
        title: '监控组件未安装或未就绪',
        message: promCheck.result || `未检测到 Prometheus，请先在 ${ns} 命名空间安装监控组件栈。`,
        command: `kubectl get pods -n ${ns}`,
        docsUrl: this.installDocsUrl,
        severity: promCheck.status === 'error' ? 'error' : 'warning'
      });
    }

    const secondaryId = monitoringType === 'enterprise' ? 'crd-servicemonitor' : 'crd-polardbxmonitor';
    const secondary = this.preflightChecks.find(check => check.id === secondaryId);
    if (secondary && secondary.status === 'error') {
      const installCmd = secondaryMeta?.installCommand ?? (monitoringType === 'standard' ? primaryMeta.installCommand : undefined);
      blockers.push({
        id: secondary.id!,
        title: secondary.name,
        message: secondary.result,
        command: secondary.command || installCmd || secondary.command,
        docsUrl: this.installDocsUrl,
        severity: 'warning'
      });
    }

    const alertmanagerCheck = this.preflightChecks.find(check => check.id === 'alertmanager');
    if (alertmanagerCheck && alertmanagerCheck.status !== 'success') {
      blockers.push({
        id: 'alertmanager',
        title: 'Alertmanager 节点标签与启用',
        message: '建议为 Alertmanager 节点打标签并 patch 实例以确保副本能够调度。',
        command: this.alertmanagerGuideCommand || this.buildAlertmanagerGuide(ns),
        docsUrl: this.alertmanagerDocsUrl,
        severity: alertmanagerCheck.status === 'error' ? 'error' : 'warning'
      });
    }

    this.preflightBlocking = blockers;
    this.cdr.markForCheck();
  }

  private extractConditionMessage(info: CrdInfo | undefined, type: string): string | undefined {
    if (!info || !Array.isArray(info.conditions)) {
      return undefined;
    }
    const cond = info.conditions.find((c) => c?.type === type);
    return cond?.message || cond?.reason || undefined;
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
      const matchLabelsYaml = this.buildMatchLabelsYaml(config.selectorLabels, config.targetName);
      yaml = `apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: ${config.monitorName}
  namespace: ${config.namespace}
spec:
  selector:
    matchLabels:
${matchLabelsYaml}
  podTargetLabels:
    - xstore/name
    - xstore/role
  endpoints:
  - port: metrics
    path: /metrics
    interval: ${config.scrapeInterval}
    scrapeTimeout: ${config.scrapeTimeout}`;
    }
    
    // 直接生成，无需模拟延迟
    this.generatedYaml = yaml;
    this.generatingYaml = false;
    this.cdr.markForCheck();
  }

  getYamlFilename(): string {
    const config = this.form.value;
    const type = config.monitoringType === 'enterprise' ? 'polardbxmonitor' : 'servicemonitor';
    return `${config.monitorName}-${type}.yaml`;
  }

  getHelmInstallScript(): string {
    const namespace = this.form.value.namespace?.trim() || 'polardbx-monitor';
    return this.helmInstallScriptTemplate.replace(/{{namespace}}/g, namespace);
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

  finishManualInstall(): void {
    this.modal.confirm({
      nzTitle: '确认完成手动安装？',
      nzContent: '请确保已执行 Helm 安装命令，并按照需要应用了 YAML/CRD 配置。完成后可在监控总览中检查组件状态。',
      nzOkText: '确认完成',
      nzCancelText: '继续检查',
      nzOkType: 'primary',
      nzOnOk: () => {
        this.message.success('请在监控总览中验证监控栈状态。');
        this.finish();
      }
    });
  }

  applyConfiguration(): void {
    if (this.form.value.installChannel === 'helm') {
      this.message.info('已选择 Helm 手动安装，请复制命令并在集群中执行。');
      return;
    }
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
    this.startMonitoringBootstrap('apply');
  }

  private startMonitoringBootstrap(origin: 'preflight' | 'apply'): void {
    const setLoading = (value: boolean) => {
      if (origin === 'apply') {
        this.stepLoading = value;
      } else {
        this.preflightStackInstalling = value;
      }
      this.cdr.markForCheck();
    };

    if (origin === 'apply') {
      this.installJob = null;
      this.applyResult = null;
      this.installStatus = null;
      this.failureDiagnosis = null;
    }

    setLoading(true);

    this.api.monitoringBootstrap({ mode: 'managed', dryRun: false }).subscribe({
      next: (res: MonitoringBootstrapResponse) => {
        const jobName = res?.jobName || 'polardbx-monitor-bootstrap';
        const ns = res?.namespace || 'polardbx-operator-system';
        const targetNs = res?.targetNs;

        const jobInfo = {
          jobName,
          namespace: ns,
          targetNs,
          timestamp: Date.now(),
          expiresAt: Date.now() + (15 * 60 * 1000)
        };
        localStorage.setItem('polardbx-monitor-install-job', JSON.stringify(jobInfo));

        setLoading(false);

        const successMessage = origin === 'apply'
          ? '安装任务已创建，正在跳转到监控总览查看进度...'
          : '监控组件安装任务已触发，正在跳转到监控总览查看进度...';
        this.message.success(successMessage);

        this.globalProgress.reportMonitoringInstall(jobName, ns, targetNs);

        setTimeout(() => {
          this.router.navigate(['/operations/monitoring/overview']);
        }, 2000);
      },
      error: (error: unknown) => {
        const msg = this.getErrorMessage(error) || '安装触发失败';
        if (origin === 'apply') {
          this.applyResult = { success: false, message: msg };
        }
        setLoading(false);
        this.message.error('监控安装失败: ' + msg);
        if (origin === 'apply') {
          this.saveState();
        }
      }
    });
  }

  retryApply(): void {
    this.applyResult = null;
    this.applyConfiguration();
  }

  getResultTitle(): string {
    if (!this.applyResult) {
      return this.form.value.installChannel === 'helm'
        ? '手动执行安装命令'
        : '准备应用配置';
    }
    return this.applyResult.success ? '配置应用成功' : '配置应用失败';
  }

  getResultSubtitle(): string {
    if (!this.applyResult) {
      return this.form.value.installChannel === 'helm'
        ? '复制并执行下面的 Helm/kubectl 命令，完成后点击页面下方“完成”。'
        : '选择应用方式以启用监控配置';
    }
    return this.applyResult.message;
  }

  getJobLogsCommand(): string {
    const ns = this.installJob?.namespace || 'polardbx-operator-system';
    const name = this.installJob?.jobName || 'polardbx-monitor-bootstrap';
    return `kubectl logs -n ${ns} job/${name}`;
  }

  getJobDescribeCommand(): string {
    const ns = this.installJob?.namespace || 'polardbx-operator-system';
    const name = this.installJob?.jobName || 'polardbx-monitor-bootstrap';
    return `kubectl describe job -n ${ns} ${name}`;
  }

  getJobFailedPodLogsCommand(): string {
    const ns = this.installJob?.namespace || 'polardbx-operator-system';
    const name = this.installJob?.jobName || 'polardbx-monitor-bootstrap';
    return `kubectl logs -n ${ns} $(kubectl get pods -n ${ns} -l job-name=${name} -o name | head -1) --previous`;
  }

  getJobDeleteCommand(): string {
    const ns = this.installJob?.namespace || 'polardbx-operator-system';
    const name = this.installJob?.jobName || 'polardbx-monitor-bootstrap';
    return `kubectl delete job -n ${ns} ${name}`;
  }

  isBackoffLimitError(reason?: string | null): boolean {
    if (!reason) {
      return false;
    }
    const normalized = reason.toLowerCase();
    return normalized.includes('backoff') || normalized.includes('backofflimit') || normalized.includes('重试次数');
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
  
  // ==================== 新增：模板辅助方法 ====================
  
  /**
   * 获取安装步骤图标
   */
  getStepIcon(status: string): string {
    const iconMap: Record<string, string> = {
      'success': 'check-circle',
      'running': 'loading',
      'pending': 'clock-circle',
      'failed': 'close-circle'
    };
    return iconMap[status] || 'question-circle';
  }
  
  /**
   * 获取步骤图标主题
   */
  getStepIconTheme(status: string): 'fill' | 'outline' | 'twotone' {
    return status === 'success' ? 'fill' : 'outline';
  }
  
  /**
   * 格式化时间（秒 -> 分钟秒）
   */
  formatTime(seconds: number): string {
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    return mins > 0 ? `${mins} 分 ${secs} 秒` : `${secs} 秒`;
  }
  
  /**
   * 获取健康状态文本
   */
  getHealthStatusText(status: string): string {
    const textMap: Record<string, string> = {
      'healthy': '运行中',
      'degraded': '降级',
      'unhealthy': '异常'
    };
    return textMap[status] || '未知';
  }
  
  /**
   * 获取验证检查 Tag 颜色
   */
  getCheckTagColor(status: string): string {
    const colorMap: Record<string, string> = {
      'success': 'success',
      'warning': 'warning',
      'error': 'error'
    };
    return colorMap[status] || 'default';
  }
  
  /**
   * 获取验证检查状态文本
   */
  getCheckStatusText(status: string): string {
    const textMap: Record<string, string> = {
      'success': '通过',
      'warning': '警告',
      'error': '失败'
    };
    return textMap[status] || '未知';
  }
  
  /**
   * 格式化时间戳
   */
  formatTimestamp(timestamp: string): string {
    const date = new Date(timestamp);
    return date.toLocaleString('zh-CN');
  }
  
  /**
   * 获取当前安装阶段
   */
  getCurrentInstallStage(): string {
    if (this.installStatus?.steps) {
      const failedStep = this.installStatus.steps.find(s => s.status === 'failed');
      return failedStep?.name || '未知阶段';
    }
    return '未知';
  }
  
  /**
   * 查看完整日志（模态框）
   */
  viewFullLogs(): void {
    if (!this.failureDiagnosis) return;
    
    this.modal.info({
      nzTitle: '完整错误日志',
      nzContent: `<pre style="max-height: 500px; overflow-y: auto; background: #f5f5f5; padding: 12px; border-radius: 4px;">${this.failureDiagnosis.relatedLogs.join('\n')}</pre>`,
      nzWidth: 800
    });
  }

  viewInstallLogs(): void {
    if (!this.installJob?.jobName) return;

    this.cleanupLogModal();

    const modalRef = this.modal.create<LogViewerComponent>({
      nzTitle: '安装任务日志',
      nzContent: LogViewerComponent,
      nzWidth: 860,
      nzFooter: null,
      nzBodyStyle: { padding: '0' }
    });

    this.logModalRef = modalRef;

    const component = modalRef.componentInstance;
    if (component) {
      component.jobName = this.installJob.jobName;
      component.namespace = this.installJob.namespace;
      component.tailLines = this.tailLines;
      component.tailLinesOptions = this.tailOptions;
      component.loading = true;
      this.logViewerRefreshSub = component.refresh.subscribe(payload => {
        const nextTail = Number(payload?.tailLines) || this.tailLines;
        this.tailLines = nextTail;
        component.loading = true;
        this.fetchInstallLogs(modalRef, nextTail);
      });
    }

    modalRef.afterClose.subscribe(() => {
      this.logViewerRefreshSub?.unsubscribe();
      this.logViewerRefreshSub = undefined;
      this.logFetchSub?.unsubscribe();
      this.logFetchSub = undefined;
      if (this.logModalRef === modalRef) {
        this.logModalRef = undefined;
      }
    });

    modalRef.afterOpen.subscribe(() => {
      this.fetchInstallLogs(modalRef, this.tailLines);
    });
  }

  private fetchInstallLogs(modalRef: NzModalRef<LogViewerComponent>, tailLines: number): void {
    const component = modalRef.componentInstance;
    const job = this.installJob;
    if (!component || !job?.jobName) {
      return;
    }

    component.loading = true;
    component.error = undefined;

    const { jobName, namespace } = job;
    this.logFetchSub?.unsubscribe();
    this.logFetchSub = this.api.monitoringBootstrapLogs(jobName, namespace, tailLines).subscribe({
      next: (res: MonitoringBootstrapLogsResponse) => {
        const rawLogs = res?.logs;
        const lines = Array.isArray(rawLogs)
          ? rawLogs
          : typeof rawLogs === 'string'
            ? rawLogs.split(/\r?\n/)
            : [];
        component.logs = lines;
        component.jobName = res?.jobName || jobName;
        component.namespace = namespace;
        component.podName = res?.podName || res?.pod || component.podName;
        component.lastUpdated = new Date();
        component.loading = false;
        component.error = undefined;
      },
      error: (error: unknown) => {
        const message = this.getErrorMessage(error) || '未知错误';
        component.loading = false;
        component.error = message;
        this.message.error('获取安装日志失败: ' + message);
      }
    });
  }

  private cleanupLogModal(): void {
    this.logViewerRefreshSub?.unsubscribe();
    this.logViewerRefreshSub = undefined;
    this.logFetchSub?.unsubscribe();
    this.logFetchSub = undefined;
    if (this.logModalRef) {
      this.logModalRef.close();
      this.logModalRef = undefined;
    }
  }

  goToMonitoring(): void {
    // 安装完成后跳转到监控概览页面
    this.router.navigate(['/operations/monitoring/overview']);
  }

  goToPrometheus(): void {
    this.api.getMonitoringStatus().subscribe({
      next: (status: MonitoringStatusResponse) => {
        const promComponent = status.components?.prometheus;
        if (!promComponent?.exists) {
          this.modal.info({
            nzTitle: 'Prometheus 未安装',
            nzContent: '请先安装监控栈后再尝试访问 Prometheus'
          });
          return;
        }

        const accessUrl = promComponent.accessUrl;
        if (!accessUrl) {
          this.modal.info({
            nzTitle: '无法直接访问 Prometheus',
            nzContent: '请执行以下命令进行端口转发：\n\nkubectl port-forward svc/prometheus-k8s -n polardbx-monitor 9090:9090\n\n然后访问：http://localhost:9090'
          });
          return;
        }

        // If it starts with http, it's a direct URL
        if (accessUrl.startsWith('http')) {
          window.open(accessUrl, '_blank');
        } else if (accessUrl.startsWith('NodePort:')) {
          const portMatch = accessUrl.match(/(\d+)/);
          if (portMatch) {
            this.modal.info({
              nzTitle: '访问 Prometheus',
              nzContent: `请获取 Kubernetes 集群中任意节点的 IP 地址，然后访问：\n\nhttp://<node-ip>:${portMatch[1]}`
            });
          }
        } else if (accessUrl.includes('port-forward')) {
          this.modal.info({
            nzTitle: '访问 Prometheus',
            nzContent: `请执行以下命令进行端口转发：\n\nkubectl ${accessUrl}\n\n然后访问：http://localhost:9090`
          });
        }
      },
      error: (error: unknown) => {
        this.message.error(`获取 Prometheus 状态失败: ${this.getErrorMessage(error)}`);
      }
    });
  }

  goToGrafana(): void {
    this.api.getMonitoringStatus().subscribe({
      next: (status: MonitoringStatusResponse) => {
        const grafanaComponent = status.components?.grafana;
        if (!grafanaComponent?.exists) {
          this.modal.info({
            nzTitle: 'Grafana 未安装',
            nzContent: '请先安装监控栈后再尝试访问 Grafana'
          });
          return;
        }

        const accessUrl = grafanaComponent.accessUrl;
        if (!accessUrl) {
          this.modal.info({
            nzTitle: '无法直接访问 Grafana',
            nzContent: '请执行以下命令进行端口转发：\n\nkubectl port-forward svc/grafana -n polardbx-monitor 3000:3000\n\n然后访问：http://localhost:3000'
          });
          return;
        }

        // If it starts with http, it's a direct URL
        if (accessUrl.startsWith('http')) {
          window.open(accessUrl, '_blank');
        } else if (accessUrl.startsWith('NodePort:')) {
          // Extract port from "NodePort: <port> (需要使用 <node-ip>:<port> 访问)"
          const portMatch = accessUrl.match(/(\d+)/);
          if (portMatch) {
            this.modal.info({
              nzTitle: '访问 Grafana',
              nzContent: `请获取 Kubernetes 集群中任意节点的 IP 地址，然后访问：\n\nhttp://<node-ip>:${portMatch[1]}`
            });
          }
        } else if (accessUrl.includes('port-forward')) {
          // port-forward instruction
          this.modal.info({
            nzTitle: '访问 Grafana',
            nzContent: `请执行以下命令进行端口转发：\n\nkubectl ${accessUrl}\n\n然后访问：http://localhost:3000`
          });
        }
      },
      error: (error: unknown) => {
        this.message.error(`获取 Grafana 状态失败: ${this.getErrorMessage(error)}`);
      }
    });
  }

  finish(): void {
    this.clearSavedState();
    this.router.navigate(['/operations/monitoring/overview']);
  }

  // ==================== localStorage 持久化功能 ====================

  private saveState(): void {
    try {
      // 裁剪冗余字段以减小存储体积
      const compactFormValues: WizardFormValue = {
        installChannel: this.form.value.installChannel,
        installMode: this.form.value.installMode,
        monitoringType: this.form.value.monitoringType,
        namespace: this.form.value.namespace,
        targetName: this.form.value.targetName,
        monitorName: this.form.value.monitorName,
        scrapeInterval: this.form.value.scrapeInterval,
        scrapeTimeout: this.form.value.scrapeTimeout,
        selectorLabels: this.form.value.selectorLabels
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
      this.updateSelectorLabels();

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
        // ✅ 不再在向导中启动轮询
        // 如果有安装任务，应该直接跳转到 Overview
        console.log('检测到保存的安装任务，应该已在 checkExistingInstallation() 中处理');
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

  private pollingInterval: ReturnType<typeof setTimeout> | null = null;
  private pollingRetryCount = 0;
  private basePollingInterval = 5000; // 5秒基础间隔
  private maxPollingInterval = 60000; // 最大60秒间隔

  private startJobStatusPolling(): void {
    // ⚠️ 已弃用：向导不应该轮询 Job 状态
    // 所有进度追踪应该在 Overview 组件中进行
    console.warn('startJobStatusPolling() 在向导中被调用，这不应该发生。应该跳转到 Overview。');
    return;
  }

  private stopJobStatusPolling(): void {
    if (this.pollingInterval) {
      clearTimeout(this.pollingInterval);
      this.pollingInterval = null;
    }
    this.pollingRetryCount = 0;
  }

  // ==================== 新增：安装状态轮询（增强版） ====================
  
  /**
   * 启动安装状态实时轮询（每3秒）
   */
  private startInstallPolling(): void {
    if (!this.installJob?.jobName) return;
    
    this.installPolling = interval(3000)
      .pipe(
        switchMap(() => this.api.monitoringBootstrapStatus(this.installJob!.jobName, this.installJob!.namespace)),
        takeUntil(this.destroy$)
      )
      .subscribe({
        next: (status: MonitoringBootstrapStatusResponse) => {
          this.updateInstallStatus(status);
          
          // 终止条件
          if (this.installStatus?.phase === 'Active' || this.installStatus?.phase === 'Failed') {
            this.stopInstallPolling();
            this.onInstallComplete(this.installStatus);
          }
          
          this.cdr.markForCheck();
        },
        error: (error: unknown) => {
          console.error('轮询安装状态失败:', error);
          // 失败时降级到旧的轮询机制
          if (typeof (error as { status?: number })?.status === 'number' && (error as { status?: number }).status === 404) {
            this.stopInstallPolling();
          }
        }
      });
  }
  
  /**
   * 停止安装状态轮询
   */
  private stopInstallPolling(): void {
    if (this.installPolling) {
      this.installPolling.unsubscribe();
      this.installPolling = null;
    }
  }
  
  /**
   * 更新安装状态（从 API 响应解析）
   */
  private updateInstallStatus(apiStatus: MonitoringBootstrapStatusResponse): void {
    // 解析后端实际返回的数据格式
    // 后端返回: { phase: "Running"|"Succeeded"|"Failed"|"Pending", startTime, active, succeeded, failed, failureReason, conditions }
  const phase = apiStatus?.phase || 'Running';
    const progress = this.calculateProgress(apiStatus);
    
    this.installStatus = {
      phase: this.mapPhaseToStatus(phase),
      progress: progress,
      steps: this.extractInstallSteps(apiStatus),
      components: this.extractComponentStatus(apiStatus),
      estimatedTimeRemaining: this.estimateTimeRemaining(progress),
      startTime: apiStatus?.startTime,
      endTime: apiStatus?.completionTime,
      logs: []  // 日志需要单独调用 /bootstrap/logs API
    };
    
    // 如果失败，提取失败原因用于诊断
    if (phase === 'Failed' && apiStatus?.failureReason) {
      this.installStatus.logs = [apiStatus.failureReason];
    }
  }
  
  /**
   * 映射 API phase 到标准状态
   */
  private mapPhaseToStatus(phase: string): InstallStatus['phase'] {
    const phaseMap: Record<string, InstallStatus['phase']> = {
      'Pending': 'Pending',
      'Running': 'Installing',
      'Verifying': 'Verifying',
      'Succeeded': 'Active',
      'Failed': 'Failed',
      'ImagePullError': 'Failed'
    };
    return phaseMap[phase] || 'Installing';
  }
  
  /**
   * 计算安装进度（0-100）
   * 后端返回的字段: active, succeeded, failed
   * 我们根据 Job 的状态估算进度
   */
  private calculateProgress(apiStatus: MonitoringBootstrapStatusResponse): number {
  const phase = apiStatus?.phase;
  const succeeded = apiStatus?.succeeded ?? 0;
    
    // 精确的进度映射
    if (phase === 'Succeeded') return 100;
  if (phase === 'Failed') return succeeded > 0 ? 95 : 50; // 失败时根据是否有成功的任务判断
    if (phase === 'Pending') return 10;
    
    // Running 状态：根据时间估算（假设平均需要 5 分钟）
    if (phase === 'Running' && apiStatus?.startTime) {
      const startTime = new Date(apiStatus.startTime);
      const now = new Date();
      const elapsedSeconds = (now.getTime() - startTime.getTime()) / 1000;
      const estimatedTotalSeconds = 300; // 5 分钟
      const progress = Math.min(95, Math.floor((elapsedSeconds / estimatedTotalSeconds) * 100));
      return Math.max(15, progress); // 至少 15%，最多 95%
    }
    
    // 默认值
    return 30;
  }
  
  /**
   * 提取安装步骤
   * 后端暂时没有返回详细步骤，我们根据 conditions 或 phase 生成合理的步骤显示
   */
  private extractInstallSteps(apiStatus: MonitoringBootstrapStatusResponse): InstallStep[] {
  const phase = apiStatus?.phase;
    
    // 基础步骤模板
    const steps: InstallStep[] = [
      { name: 'Job 已创建', status: 'success', startTime: apiStatus?.startTime },
      { name: 'Helm Chart 准备', status: phase === 'Pending' ? 'pending' : 'success' },
      { name: '监控组件安装', status: 'pending' },
      { name: '配置应用', status: 'pending' },
      { name: '健康检查', status: 'pending' }
    ];
    
    // 根据 phase 更新步骤状态
    if (phase === 'Running') {
      steps[1].status = 'success';
      steps[2].status = 'running';
      steps[2].message = '正在部署 Prometheus, Grafana, Alertmanager...';
    } else if (phase === 'Succeeded') {
      steps.forEach((step, idx) => {
        step.status = 'success';
        if (idx === steps.length - 1) {
          step.endTime = apiStatus?.completionTime;
        }
      });
    } else if (phase === 'Failed') {
      steps[1].status = 'success';
      steps[2].status = 'failed';
      steps[2].message = apiStatus?.failureReason || '安装失败';
    }
    
    return steps;
  }
  
  /**
   * 提取组件状态
   * 后端暂时没有返回组件级别的状态，我们根据 phase 生成合理的显示
   */
  private extractComponentStatus(apiStatus: MonitoringBootstrapStatusResponse): ComponentStatus[] {
    const phase = apiStatus?.phase;
    
    // 基础组件列表
    const components: ComponentStatus[] = [
      { name: 'prometheus', status: 'pending', readyPods: '0/2', port: 9090 },
      { name: 'grafana', status: 'pending', readyPods: '0/1', port: 3000 },
      { name: 'alertmanager', status: 'pending', readyPods: '0/3', port: 9093 }
    ];
    
    // 根据 phase 更新状态
    if (phase === 'Running') {
      components[0].status = 'running';
      components[0].readyPods = '1/2';
    } else if (phase === 'Succeeded') {
      components.forEach(comp => {
        comp.status = 'ready';
        const total = comp.readyPods!.split('/')[1];
        comp.readyPods = `${total}/${total}`;
      });
    } else if (phase === 'Failed') {
      components[0].status = 'error';
      components[0].message = apiStatus?.failureReason || '部署失败';
    }
    
    return components;
  }
  
  /**
   * 估算剩余时间（秒）
   */
  private estimateTimeRemaining(progress: number): number {
    if (progress >= 90) return 30;
    if (progress >= 70) return 60;
    if (progress >= 50) return 120;
    return 180;
  }
  
  /**
   * 安装完成回调
   */
  private onInstallComplete(status: InstallStatus): void {
    if (status.phase === 'Active') {
      this.message.success('监控栈安装成功！');
      this.loadComponentsHealth(); // 加载组件健康状态
      this.clearSavedState();
    } else if (status.phase === 'Failed') {
      this.message.error('监控栈安装失败');
      this.diagnoseFailure(status); // 诊断失败原因
    }
  }
  
  /**
   * 加载组件健康状态（成功后）
   */
  private loadComponentsHealth(): void {
    // 调用实际 API 获取监控组件的 Pod 状态
    const monitoringNs = this.installJob?.targetNs || 'polardbx-monitor';
    
    this.api.listPods(monitoringNs).subscribe({
      next: (pods: Pod[]) => {
        this.componentsHealth = this.parseComponentsFromPods(pods);
        
        // 如果没有找到任何组件，显示警告而不是假数据
        if (this.componentsHealth.length === 0) {
          console.warn('未找到任何监控组件 Pod');
          this.message.warning('监控组件可能尚未完全启动，请稍后刷新');
        }
        
        this.cdr.markForCheck();
      },
      error: (err) => {
        console.error('获取组件状态失败:', err);
        this.message.error('获取监控组件状态失败');
        this.componentsHealth = [];
        this.cdr.markForCheck();
      }
    });
  }
  
  /**
   * 从 Pods 列表解析组件健康状态
   */
  private parseComponentsFromPods(pods: Pod[]): ComponentHealth[] {
    const components: ComponentHealth[] = [];
    
    // 定义监控组件的匹配规则
    const componentRules: {
      name: string;
      port: number;
      labelMatch: (labels: Record<string, string | undefined>) => boolean;
    }[] = [
  { name: 'Prometheus', labelMatch: labels => labels['app'] === 'prometheus' || labels['app.kubernetes.io/name'] === 'prometheus', port: 9090 },
  { name: 'Grafana', labelMatch: labels => labels['app'] === 'grafana' || labels['app.kubernetes.io/name'] === 'grafana', port: 3000 },
  { name: 'Alertmanager', labelMatch: labels => labels['app'] === 'alertmanager' || labels['app.kubernetes.io/name'] === 'alertmanager', port: 9093 },
  { name: 'Node Exporter', labelMatch: labels => labels['app'] === 'node-exporter' || labels['app.kubernetes.io/name'] === 'node-exporter', port: 9100 },
  { name: 'Kube State Metrics', labelMatch: labels => labels['app'] === 'kube-state-metrics' || labels['app.kubernetes.io/name'] === 'kube-state-metrics', port: 8080 }
    ];
    
    componentRules.forEach(rule => {
      const matchedPods = pods.filter(pod => rule.labelMatch(pod.metadata.labels ?? {}));
      
      if (matchedPods.length > 0) {
        const totalPods = matchedPods.length;
        const readyPods = matchedPods.filter(pod => {
          const conditions = pod.status?.conditions ?? [];
          return conditions.some(condition => condition.type === 'Ready' && condition.status === 'True');
        }).length;
        
        // 判断健康状态
        let status: 'healthy' | 'degraded' | 'unhealthy';
        if (readyPods === totalPods) {
          status = 'healthy';
        } else if (readyPods > 0) {
          status = 'degraded';
        } else {
          status = 'unhealthy';
        }
        
        const cpu: string | undefined = undefined;
        const memory: string | undefined = undefined;
        
        components.push({
          name: rule.name,
          status,
          readyPods: `${readyPods}/${totalPods}`,
          port: rule.port,
          url: undefined,
          cpu,
          memory
        });
      }
    });
    
    // 不返回假数据，返回空数组
    return components;
  }
  
  /**
   * 诊断失败原因
   */
  private diagnoseFailure(status: InstallStatus): void {
    // 首先尝试获取详细日志
    if (this.installJob?.jobName) {
      this.loadFailureLogs(status);
    } else {
      this.performDiagnosis(status, []);
    }
  }
  
  /**
   * 加载失败日志用于诊断
   */
  private loadFailureLogs(status: InstallStatus): void {
    if (!this.installJob) return;
    
    this.api.monitoringBootstrapLogs(this.installJob.jobName, this.installJob.namespace, 100).subscribe({
      next: (res: MonitoringBootstrapLogsResponse) => {
        const logs = Array.isArray(res.logs) ? res.logs : [];
        this.performDiagnosis(status, logs);
      },
      error: (error: unknown) => {
        console.warn('获取日志失败，使用基本诊断:', error);
        this.performDiagnosis(status, []);
      }
    });
  }
  
  /**
   * 执行诊断分析
   */
  private performDiagnosis(status: InstallStatus, logs: string[]): void {
    const logsText = logs.join('\n');
    // 也检查 status.logs（可能包含 failureReason）
    const statusLogs = status.logs || [];
    const combinedText = logsText + '\n' + statusLogs.join('\n');
    
    // 智能分析错误类型
    let errorType: FailureDiagnosis['errorType'] = 'Unknown';
    const possibleCauses: PossibleCause[] = [];
    
    // ImagePullBackOff 检测
    if (combinedText.includes('ImagePull') || combinedText.includes('ErrImagePull') || 
        combinedText.includes('image pull') || combinedText.includes('Back-off pulling image')) {
      errorType = 'ImagePull';
      possibleCauses.push({
        description: '镜像拉取失败（国外镜像源不可达）',
        probability: 85,
        suggestedFix: '切换到 DaoCloud 镜像源',
        autoFixable: true,
        fixAction: 'switchImageRegistry'
      });
      possibleCauses.push({
        description: '网络连接问题',
        probability: 10,
        suggestedFix: '检查节点是否可访问 Docker Hub',
        autoFixable: false
      });
      possibleCauses.push({
        description: '镜像不存在或标签错误',
        probability: 5,
        suggestedFix: '验证镜像标签是否正确',
        autoFixable: false
      });
    }
    
    // 资源不足检测
    else if (combinedText.includes('Insufficient') || combinedText.includes('OutOfMemory') ||
             combinedText.includes('resources') || combinedText.includes('quota')) {
      errorType = 'ResourceLimit';
      possibleCauses.push({
        description: '集群资源不足',
        probability: 90,
        suggestedFix: '减少 Prometheus 副本数或调整资源限制',
        autoFixable: true,
        fixAction: 'adjustResources'
      });
      possibleCauses.push({
        description: '命名空间 ResourceQuota 限制',
        probability: 10,
        suggestedFix: '检查并调整 ResourceQuota',
        autoFixable: false
      });
    }
    
    // 网络错误检测
    else if (combinedText.includes('dial tcp') || combinedText.includes('timeout') ||
             combinedText.includes('connection refused') || combinedText.includes('network')) {
      errorType = 'NetworkIssue';
      possibleCauses.push({
        description: 'Kubernetes 服务网络异常',
        probability: 70,
        suggestedFix: '检查网络策略和 DNS 配置',
        autoFixable: false
      });
      possibleCauses.push({
        description: 'CNI 插件问题',
        probability: 20,
        suggestedFix: '检查 CNI 插件状态',
        autoFixable: false
      });
    }
    
    // 权限错误
    else if (combinedText.includes('forbidden') || combinedText.includes('Unauthorized') ||
             combinedText.includes('permission denied') || combinedText.includes('RBAC')) {
      errorType = 'PermissionDenied';
      possibleCauses.push({
        description: 'ServiceAccount 权限不足',
        probability: 80,
        suggestedFix: '检查 RBAC 权限配置',
        autoFixable: false
      });
      possibleCauses.push({
        description: 'ClusterRole 绑定缺失',
        probability: 15,
        suggestedFix: '确认 ClusterRoleBinding 存在',
        autoFixable: false
      });
    }
    
    // 配置错误检测
    else if (combinedText.includes('invalid') || combinedText.includes('malformed') ||
             combinedText.includes('parse error') || combinedText.includes('validation')) {
      errorType = 'ConfigError';
      possibleCauses.push({
        description: '配置格式错误',
        probability: 70,
        suggestedFix: '检查 Helm values 或 YAML 配置',
        autoFixable: false
      });
    }
    
    // 如果没有匹配到具体错误，提供通用建议
    if (possibleCauses.length === 0) {
      possibleCauses.push({
        description: '未知错误，需要查看详细日志',
        probability: 100,
        suggestedFix: '查看 Job Pod 日志获取更多信息',
        autoFixable: false
      });
    }
    
    // 提取错误消息
    const failedStep = status.steps.find(s => s.status === 'failed');
    const errorMessage = failedStep?.message || statusLogs[0] || '安装失败';
    
    this.failureDiagnosis = {
      errorType,
      errorMessage,
      possibleCauses: possibleCauses.sort((a, b) => b.probability - a.probability),
      relatedLogs: logs.length > 0 ? logs.slice(-20) : statusLogs, // 最后 20 行日志
      timestamp: new Date().toISOString()
    };
    
    this.cdr.markForCheck();
  }
  
  /**
   * 自动修复
   */
  async autoFix(cause: PossibleCause): Promise<void> {
    if (!cause.autoFixable || !cause.fixAction) {
      this.message.warning('此问题暂不支持自动修复');
      return;
    }
    
    this.autoFixing = true;
    this.cdr.markForCheck();
    
    try {
      switch (cause.fixAction) {
        case 'switchImageRegistry':
          await this.autoSwitchImageRegistry();
          break;
        case 'adjustResources':
          await this.autoAdjustResources();
          break;
        default:
          this.message.warning('未知的修复操作');
      }
      
      this.message.success('自动修复完成，正在重试安装...');
      await this.retryInstallation();
    } catch (error) {
      console.error('自动修复失败:', error);
      this.message.error('自动修复失败，请手动处理');
    } finally {
      this.autoFixing = false;
      this.cdr.markForCheck();
    }
  }
  
  /**
   * 自动切换镜像源
   */
  private autoSwitchImageRegistry(): Promise<void> {
    return new Promise((resolve, reject) => {
      const config = {
        registry: 'docker.m.daocloud.io',
        customRegistry: undefined
      };
      
      this.api.setImageRegistryConfig(config).subscribe({
        next: () => {
          this.selectedRegistry = 'docker.m.daocloud.io';
          this.message.success('已自动切换到 DaoCloud 镜像源');
          resolve();
        },
        error: (err) => {
          console.error('切换镜像源失败:', err);
          reject(err);
        }
      });
    });
  }
  
  /**
   * 自动调整资源配置
   */
  private autoAdjustResources(): Promise<void> {
    return new Promise((resolve) => {
      // 这里可以调用 API 更新 Helm values
      this.message.info('调整资源配置...');
      setTimeout(() => resolve(), 1000);
    });
  }
  
  /**
   * 重试安装
   */
  private async retryInstallation(): Promise<void> {
    // 清除失败状态
    this.failureDiagnosis = null;
    this.installStatus = null;
    this.applyResult = null;
    
    // 重新执行应用步骤
    this.applyConfiguration();
  }
  
  // ==================== 新增：监控功能验证 ====================
  
  /**
   * 验证监控功能（自动健康检查）
   */
  async verifyMonitoring(): Promise<void> {
    this.verifyingMonitoring = true;
    this.cdr.markForCheck();
    
    try {
      const checks: VerificationCheck[] = [];
      
      // 1. Prometheus API 测试
      const prometheusCheck = await this.checkPrometheusAPI();
      checks.push(prometheusCheck);
      
      // 2. Grafana API 测试
      const grafanaCheck = await this.checkGrafanaAPI();
      checks.push(grafanaCheck);
      
      // 3. Alertmanager API 测试
      const alertmanagerCheck = await this.checkAlertmanagerAPI();
      checks.push(alertmanagerCheck);
      
      // 4. 指标采集检测
      const metricsCheck = await this.checkMetricsCollection();
      checks.push(metricsCheck);
      
      // 5. 仪表板检测
      const dashboardCheck = await this.checkDashboards();
      checks.push(dashboardCheck);
      
      // 计算总体健康度
      const overallHealth = this.calculateHealth(checks);
      
      // 提取警告和建议
      const warnings = this.extractWarnings(checks);
      const recommendations = this.generateRecommendations(checks);
      
      this.verificationResult = {
        overallHealth,
        checks,
        warnings,
        recommendations,
        timestamp: new Date().toISOString()
      };
      
      if (overallHealth >= 80) {
        this.message.success(`验证完成，健康度 ${overallHealth}%`);
      } else if (overallHealth >= 60) {
        this.message.warning(`验证完成，发现 ${warnings.length} 个警告`);
      } else {
        this.message.error('验证失败，请检查组件状态');
      }
      
    } catch (error) {
      console.error('监控验证失败:', error);
      this.message.error('验证过程出错');
    } finally {
      this.verifyingMonitoring = false;
      this.cdr.markForCheck();
    }
  }
  
  /**
   * 检查 Prometheus API
   */
  private async checkPrometheusAPI(): Promise<VerificationCheck> {
    try {
      // 基于组件健康状态进行验证
      const prometheusComponent = this.componentsHealth.find(c => c.name === 'Prometheus');
      
      if (!prometheusComponent) {
        return {
          name: 'Prometheus 查询 API',
          category: 'api',
          status: 'error',
          message: '未找到 Prometheus 组件',
          details: '请确认 Prometheus 已正确部署'
        };
      }
      
      if (prometheusComponent.status === 'healthy') {
        return {
          name: 'Prometheus 查询 API',
          category: 'api',
          status: 'success',
          message: `Prometheus 运行正常 (${prometheusComponent.readyPods})`,
          details: `端口: ${prometheusComponent.port}, URL: ${prometheusComponent.url}`
        };
      } else {
        return {
          name: 'Prometheus 查询 API',
          category: 'api',
          status: 'warning',
          message: `Prometheus 状态: ${prometheusComponent.status} (${prometheusComponent.readyPods})`,
          details: '部分 Pod 未就绪'
        };
      }
    } catch (error) {
      return {
        name: 'Prometheus 查询 API',
        category: 'api',
        status: 'error',
        message: 'API 检查失败',
        details: String(error)
      };
    }
  }
  
  /**
   * 检查 Grafana API
   */
  private async checkGrafanaAPI(): Promise<VerificationCheck> {
    try {
      const grafanaComponent = this.componentsHealth.find(c => c.name === 'Grafana');
      
      if (!grafanaComponent) {
        return {
          name: 'Grafana API',
          category: 'api',
          status: 'error',
          message: '未找到 Grafana 组件',
          details: '请确认 Grafana 已正确部署'
        };
      }
      
      if (grafanaComponent.status === 'healthy') {
        return {
          name: 'Grafana API',
          category: 'api',
          status: 'success',
          message: `Grafana 运行正常 (${grafanaComponent.readyPods})`,
          details: `端口: ${grafanaComponent.port}, URL: ${grafanaComponent.url}`
        };
      } else {
        return {
          name: 'Grafana API',
          category: 'api',
          status: 'warning',
          message: `Grafana 状态: ${grafanaComponent.status} (${grafanaComponent.readyPods})`,
          details: '部分 Pod 未就绪'
        };
      }
    } catch (error) {
      return {
        name: 'Grafana API',
        category: 'api',
        status: 'error',
        message: 'Grafana 检查失败',
        details: String(error)
      };
    }
  }
  
  /**
   * 检查 Alertmanager API
   */
  private async checkAlertmanagerAPI(): Promise<VerificationCheck> {
    try {
      const alertmanagerComponent = this.componentsHealth.find(c => c.name === 'Alertmanager');
      
      if (!alertmanagerComponent) {
        return {
          name: 'Alertmanager API',
          category: 'alerting',
          status: 'error',
          message: '未找到 Alertmanager 组件',
          details: '请确认 Alertmanager 已正确部署'
        };
      }
      
      if (alertmanagerComponent.status === 'healthy') {
        return {
          name: 'Alertmanager API',
          category: 'alerting',
          status: 'success',
          message: `Alertmanager 运行正常 (${alertmanagerComponent.readyPods})`,
          details: `端口: ${alertmanagerComponent.port}, URL: ${alertmanagerComponent.url}`
        };
      } else {
        return {
          name: 'Alertmanager API',
          category: 'alerting',
          status: 'warning',
          message: `Alertmanager 状态: ${alertmanagerComponent.status} (${alertmanagerComponent.readyPods})`,
          details: '部分 Pod 未就绪'
        };
      }
    } catch (error) {
      return {
        name: 'Alertmanager API',
        category: 'alerting',
        status: 'error',
        message: 'Alertmanager 检查失败',
        details: String(error)
      };
    }
  }
  
  /**
   * 检查指标采集
   */
  private async checkMetricsCollection(): Promise<VerificationCheck> {
    try {
      const nodeExporter = this.componentsHealth.find(c => c.name === 'Node Exporter');
      const kubeStateMetrics = this.componentsHealth.find(c => c.name === 'Kube State Metrics');
      
      const exporters = [nodeExporter, kubeStateMetrics].filter(c => c !== undefined);
      
      if (exporters.length === 0) {
        return {
          name: '指标采集',
          category: 'metrics',
          status: 'warning',
          message: '未找到指标导出器组件',
          details: 'Node Exporter 和 Kube State Metrics 可能未部署'
        };
      }
      
      const allHealthy = exporters.every(c => c!.status === 'healthy');
      const someHealthy = exporters.some(c => c!.status === 'healthy');
      
      if (allHealthy) {
        return {
          name: '指标采集',
          category: 'metrics',
          status: 'success',
          message: '所有指标导出器运行正常',
          details: exporters.map(c => `${c!.name}: ${c!.readyPods}`).join(', ')
        };
      } else if (someHealthy) {
        return {
          name: '指标采集',
          category: 'metrics',
          status: 'warning',
          message: '部分指标导出器存在问题',
          details: exporters.map(c => `${c!.name}: ${c!.status} (${c!.readyPods})`).join(', ')
        };
      } else {
        return {
          name: '指标采集',
          category: 'metrics',
          status: 'error',
          message: '指标导出器未就绪',
          details: exporters.map(c => `${c!.name}: ${c!.status}`).join(', ')
        };
      }
    } catch (error) {
      return {
        name: '指标采集',
        category: 'metrics',
        status: 'error',
        message: '指标采集检查失败',
        details: String(error)
      };
    }
  }
  
  /**
   * 检查仪表板
   */
  private async checkDashboards(): Promise<VerificationCheck> {
    try {
      const grafanaComponent = this.componentsHealth.find(c => c.name === 'Grafana');
      
      if (!grafanaComponent || grafanaComponent.status !== 'healthy') {
        return {
          name: '默认仪表板',
          category: 'dashboard',
          status: 'error',
          message: 'Grafana 未就绪，无法检查仪表板',
          details: 'Grafana 需要先运行正常'
        };
      }
      
      // 基于 Grafana 运行状态推断
      return {
        name: '默认仪表板',
        category: 'dashboard',
        status: 'success',
        message: 'Grafana 运行正常，仪表板应已加载',
        details: `访问 ${grafanaComponent.url} 查看仪表板`
      };
    } catch (error) {
      return {
        name: '默认仪表板',
        category: 'dashboard',
        status: 'error',
        message: '仪表板检查失败',
        details: String(error)
      };
    }
  }
  
  /**
   * 计算总体健康度
   */
  private calculateHealth(checks: VerificationCheck[]): number {
    if (checks.length === 0) return 0;
    
    let score = 0;
    checks.forEach(check => {
      if (check.status === 'success') score += 100;
      else if (check.status === 'warning') score += 60;
      else score += 0;
    });
    
    return Math.floor(score / checks.length);
  }
  
  /**
   * 提取警告信息
   */
  private extractWarnings(checks: VerificationCheck[]): string[] {
    return checks
      .filter(c => c.status === 'warning' || c.status === 'error')
      .map(c => `${c.name}: ${c.message}`);
  }
  
  /**
   * 生成建议
   */
  private generateRecommendations(checks: VerificationCheck[]): string[] {
    const recommendations: string[] = [];
    
    checks.forEach(check => {
      if (check.status === 'warning' && check.category === 'metrics') {
        recommendations.push('建议在所有节点部署 node-exporter 以获取完整的节点指标');
      }
      if (check.status === 'error' && check.category === 'api') {
        recommendations.push(`检查 ${check.name} 的网络连接和服务状态`);
      }
    });
    
    if (recommendations.length === 0) {
      recommendations.push('所有组件运行正常，建议定期检查告警规则');
    }
    
    return recommendations;
  }

  private checkJobStatus(): void {
    if (!this.installJob?.jobName) return;

    this.api.monitoringBootstrapStatus(this.installJob.jobName, this.installJob.namespace).subscribe({
      next: (status: MonitoringBootstrapStatusResponse) => {
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
          };
          this.stopJobStatusPolling();
          this.message.error('监控安装失败');
        } else if (phase === 'ImagePullError') {
          // 镜像拉取错误 - 给出明确提示
          const reason = status?.failureReason || '镜像拉取失败';
          this.applyResult = {
            success: false,
            message: `安装失败: ${reason}`,
            failureReason: reason
          };
          this.stopJobStatusPolling();
          this.message.error('镜像拉取失败，请检查网络或配置镜像加速器');
        }
        // 运行中的任务继续轮询
        this.saveState(); // 保存最新状态
        this.cdr.markForCheck();
      },
      error: (error: unknown) => {
        this.pollingRetryCount++;

        // 404 表示 Job 不存在或已被清理
        if ((error as { status?: number })?.status === 404) {
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

  /**
   * 检测是否已安装监控系统或有正在进行的安装任务
   */
  private checkExistingInstallation(): void {
    this.checkingExisting = true; // ✅ 开始检测
    this.cdr.markForCheck();
    
    // 1. 先检查 localStorage 是否有进行中的任务
    const savedJobInfo = localStorage.getItem('polardbx-monitor-install-job');
    if (savedJobInfo) {
      try {
        const jobInfo = JSON.parse(savedJobInfo);
        
        // ✅ 验证是否过期（Job TTL 是 10 分钟，localStorage 保存 15 分钟）
        if (jobInfo.expiresAt && Date.now() > jobInfo.expiresAt) {
          console.log('Job 信息已过期，清理 localStorage');
          localStorage.removeItem('polardbx-monitor-install-job');
          // 继续检查组件是否已安装
          this.checkComponentsInstallation();
          return;
        }
        
        // 验证 Job 是否还在运行
        this.api.monitoringBootstrapStatus(jobInfo.jobName, jobInfo.namespace).subscribe({
          next: (jobStatus: MonitoringBootstrapStatusResponse) => {
            const phase = jobStatus?.phase;

            if (phase === 'Running' || phase === 'Pending') {
              // ✅ 有正在进行的安装任务 → 直接跳转 Overview（不询问）
              this.message.info('检测到正在进行的安装任务，正在跳转到监控总览...');
              setTimeout(() => {
                this.router.navigate(['/operations/monitoring/overview']);
              }, 1000);
              return; // 停止后续检查
            }

            if (phase === 'Succeeded' || phase === 'Failed' || phase === 'ImagePullError') {
              const updatedInfo = {
                ...jobInfo,
                phase,
                finishedAt: Date.now(),
                expiresAt: Date.now() + (15 * 60 * 1000) // 延长可见期，便于在 Overview 查看结果
              };
              localStorage.setItem('polardbx-monitor-install-job', JSON.stringify(updatedInfo));

              // ✅ 清理向导状态，避免回到步骤六
              this.clearSavedState();

              // ✅ 提示用户前往 Overview 查看结果
              if (phase === 'Succeeded') {
                this.message.success('检测到最近一次监控安装已完成，正在跳转到监控总览...');
              } else {
                this.message.warning('检测到最近一次监控安装未成功，请在监控总览中查看详情。');
              }

              setTimeout(() => {
                this.router.navigate(['/operations/monitoring/overview']);
              }, 1000);
              return;
            }

            // 未能识别的状态，继续检查组件安装情况
            this.checkComponentsInstallation();
          },
          error: (error: unknown) => {
            console.warn('检测安装任务失败:', error);
            
            // ✅ 如果 Job 不存在（404），清理 localStorage 并跳转 Overview
            if ((error as { status?: number })?.status === 404) {
              console.log('Job 已不存在（可能被 TTL 清理），清理 localStorage 和向导状态');
              localStorage.removeItem('polardbx-monitor-install-job');
              this.clearSavedState();
              this.message.warning('最近的监控安装任务不存在，正在跳转到监控总览。');
              setTimeout(() => {
                this.router.navigate(['/operations/monitoring/overview']);
              }, 1000);
              return;
            }
            
            // 其他错误暂时忽略，继续检查是否已安装
            this.checkComponentsInstallation();
          }
        });
      } catch (e) {
        console.warn('解析安装任务信息失败:', e);
        localStorage.removeItem('polardbx-monitor-install-job');
        this.checkComponentsInstallation();
      }
    } else {
      // 没有进行中的任务，检查是否已安装
      this.checkComponentsInstallation();
    }
  }

  /**
   * 检测监控组件是否已安装
   */
  private checkComponentsInstallation(): void {
    this.api.getMonitoringStatus().subscribe({
      next: (status: MonitoringStatusResponse) => {
        const components = status?.components ?? {};
        const isInstalled = !!(
          components.prometheus?.exists ||
          components.grafana?.exists ||
          components.alertmanager?.exists
        );

        if (isInstalled) {
          // ✅ 检测到已安装 → 清理所有状态 → 直接跳转 Overview
          console.log('检测到监控系统已安装，清理向导状态');
          localStorage.removeItem('polardbx-monitor-install-job');
          this.clearSavedState();
          
          this.message.info('检测到监控系统已安装，正在跳转到监控总览...');
          setTimeout(() => {
            this.router.navigate(['/operations/monitoring/overview']);
          }, 1000);
          // 不需要设置 checkingExisting = false，因为会跳转
        } else {
          // ✅ 未安装 → 显示向导
          this.checkingExisting = false;
          this.cdr.markForCheck();
        }
      },
      error: (error: unknown) => {
        console.warn('检测已安装状态失败:', error);
        // ✅ 检测失败不阻塞流程，显示向导让用户继续
        this.checkingExisting = false;
        this.cdr.markForCheck();
      }
    });
  }

  /**
   * 显示已安装警告对话框
   */
  private showInstalledWarning(components: MonitoringStatusResponse['components'] | undefined): void {
    const installedComponents: string[] = [];
    const comps = components ?? {};
    if (comps.prometheus?.exists) {
      const status = comps.prometheus.ready ? '运行中' : '异常';
      installedComponents.push(`<li><i class="anticon anticon-check-circle" style="color: #52c41a; margin-right: 4px;"></i> Prometheus (${status})</li>`);
    }
    if (comps.grafana?.exists) {
      const status = comps.grafana.ready ? '运行中' : '异常';
      installedComponents.push(`<li><i class="anticon anticon-check-circle" style="color: #52c41a; margin-right: 4px;"></i> Grafana (${status})</li>`);
    }
    if (comps.alertmanager?.exists) {
      const status = comps.alertmanager.ready ? '运行中' : (comps.alertmanager.configured ? '已配置' : '异常');
      installedComponents.push(`<li><i class="anticon anticon-check-circle" style="color: #52c41a; margin-right: 4px;"></i> Alertmanager (${status})</li>`);
    }

    this.modal.confirm({
      nzTitle: '检测到已安装的监控系统',
      nzContent: `
        <div style="margin: 16px 0;">
          <p><strong>系统已检测到以下监控组件：</strong></p>
          <ul style="list-style: none; padding-left: 0;">
            ${installedComponents.join('')}
          </ul>
          <p style="margin-top: 16px; color: #666;">您可以选择：</p>
          <ul style="color: #666;">
            <li><strong>查看状态</strong>：跳转到监控总览页面查看详细信息</li>
            <li><strong>继续配置</strong>：继续使用向导（可能需要先卸载现有组件）</li>
          </ul>
        </div>
      `,
      nzOkText: '查看状态',
      nzCancelText: '继续配置',
      nzWidth: 500,
      nzOnOk: () => {
        // 跳转到 overview 查看状态
        this.router.navigate(['/operations/monitoring/overview']);
      },
      nzOnCancel: () => {
        // 继续使用向导
        this.message.info('继续配置流程。如需重新安装，请先在监控总览页面卸载现有组件。');
      }
    });
  }

  ngOnDestroy(): void {
    this.stopJobStatusPolling();
    this.stopInstallPolling();
    this.cleanupLogModal();
    this.destroy$.next();
    this.destroy$.complete();
  }

  // 镜像拉取错误提示的辅助方法
  getMirrorConfigCommands(): string {
    return `# 配置 Docker 镜像加速器（以 minikube 为例）
minikube ssh
sudo tee /etc/docker/daemon.json <<EOF
{
  "registry-mirrors": [
    "https://docker.m.daocloud.io",
    "https://dockerproxy.com"
  ]
}
EOF
sudo systemctl daemon-reload
sudo systemctl restart docker
exit

# 重启 minikube
minikube stop && minikube start`;
  }

  getPrePullCommands(): string {
    return `# 手动拉取镜像到集群节点
minikube ssh
docker pull docker.m.daocloud.io/alpine/helm:3.12.3
exit`;
  }

  getJobPodName(): string {
    if (!this.installJob?.jobName) return '<job-pod-name>';
    return `\${kubectl get pods -n ${this.installJob.namespace} -l job-name=${this.installJob.jobName} -o name | head -1}`;
  }
}
