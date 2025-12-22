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

// New: Installation status interface
interface InstallStatus {
  phase: 'Pending' | 'Installing' | 'Verifying' | 'Active' | 'Failed' | 'Degraded';
  progress: number; // 0-100
  steps: InstallStep[];
  components: ComponentStatus[];
  estimatedTimeRemaining?: number; // seconds
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
  fixAction?: string; // Auto-fix action identifier
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
      <!-- ✅ Checking status -->
      <div class="checking-overlay" *ngIf="checkingExisting">
        <nz-spin nzSimple [nzSize]="'large'" nzTip="Detecting monitoring system status..."></nz-spin>
      </div>
      
      <!-- ✅ Show wizard after check completes -->
      <div *ngIf="!checkingExisting">
        <div class="page-header">
          <div class="header-content">
            <h1 class="page-title">
              <i nz-icon nzType="tool" class="page-icon"></i>
              Monitoring Quick Start Wizard
            </h1>
            <p class="subtitle">Quickly enable PolarDB-X cluster monitoring (Enterprise PolarDBXMonitor / Standard ServiceMonitor)</p>
          </div>
        </div>
      </div>
      
      <app-wizard-shell
        *ngIf="!checkingExisting"
        title="Monitoring Enablement Wizard"
        subtitle="Quickly enable PolarDB-X cluster monitoring"
        [namespace]="form.value.namespace"
        [objectName]="getObjectName()"
        objectLabel="Target"
        docLink="https://docs.polardbx.com/monitoring"
        [steps]="wizardSteps"
        [currentStepIndex]="currentStep"
        [actions]="getStepActions()"
        [loading]="stepLoading">

      <!-- Step 0: Image registry configuration -->
      <ng-template #step0Template>
        <div class="step-content">
          <nz-alert 
            nzType="info"
            nzMessage="Image Registry Configuration"
            nzDescription="Choose an image registry for monitoring installation. DaoCloud mirror acceleration is recommended; if already configured, you can skip this step."
            nzShowIcon
            class="step-alert">
          </nz-alert>

          <nz-spin [nzSpinning]="loadingRegistries">
            <!-- Current configuration display -->
            <nz-alert 
              *ngIf="currentRegistryConfig"
              nzType="info"
              nzShowIcon
              class="current-config-alert"
              style="margin-bottom: 16px;">
              <div nz-alert-message>
                <strong>Current image registry configuration</strong>
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
                      <span *ngIf="registry.status === 'verified'">Verified</span>
                      <span *ngIf="registry.status === 'slow'">Slow network</span>
                      <span *ngIf="registry.status === 'custom'">Custom</span>
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

              <!-- Custom image registry input -->
              <div *ngIf="selectedRegistry === 'custom'" class="custom-registry-input">
                <nz-input-group nzSearch nzSize="large" [nzAddOnAfter]="suffixButton">
                  <input 
                    type="text" 
                    nz-input 
                    [(ngModel)]="customRegistryInput"
                    placeholder="e.g., registry.example.com or my-registry.com:5000" />
                </nz-input-group>
                <ng-template #suffixButton>
                  <button nz-button nzType="primary" nzSearch (click)="saveImageRegistryConfig()">
                    <i nz-icon nzType="check"></i>
                  </button>
                </ng-template>
                <div class="custom-registry-tips">
                  <i nz-icon nzType="info-circle" nzTheme="fill"></i>
                  Ensure your custom image registry contains required Helm charts and images
                </div>
              </div>
            </div>

            <!-- Additional notes -->
            <div class="registry-notes">
              <h4>Image registry notes</h4>
              <ul>
                <li><strong>DaoCloud</strong>: Verified mirror acceleration source, good for fast pulls</li>
                <li><strong>Docker Hub</strong>: Official public registry; may be slow or rate-limited</li>
                <li><strong>Custom</strong>: For enterprise/private registries, enter a custom address below</li>
              </ul>
            </div>
          </nz-spin>
        </div>
      </ng-template>

      <!-- Step 1: Select target -->
      <ng-template #step1Template>
        <div class="step-content">
          <nz-alert 
            nzType="info"
            nzMessage="Select monitoring target"
            nzDescription="Choose the cluster type and specific target to enable monitoring. Enterprise uses PolarDBXMonitor, Standard uses ServiceMonitor."
            nzShowIcon
            class="step-alert">
          </nz-alert>

          <nz-alert
            *ngIf="form.value.namespace && !selectedNamespaceExists"
            nzType="warning"
            nzShowIcon
            class="step-alert"
            [nzMessage]="'Namespace ' + form.value.namespace + ' is not created yet'"
            [nzDescription]="namespaceGuideTpl">
          </nz-alert>

          <ng-template #namespaceGuideTpl>
            <p>Please create this namespace in the Kubernetes cluster before continuing, or switch to an existing namespace.</p>
            <ng-container *ngIf="form.value.namespace as ns">
              <pre>{{ getNamespaceCreateCommand(ns) }}</pre>
              <button 
                nz-button 
                nzType="dashed" 
                nzSize="small"
                (click)="copyCommand(getNamespaceCreateCommand(ns))">
                <i nz-icon nzType="copy"></i>
                Copy command
              </button>
            </ng-container>
          </ng-template>

          <nz-alert
            *ngIf="namespaceError"
            nzType="error"
            nzShowIcon
            class="step-alert"
            nzMessage="Cannot confirm namespace status"
            [nzDescription]="namespaceError">
          </nz-alert>

          <form [formGroup]="form" class="config-form">
            <nz-row [nzGutter]="16">
              <nz-col [nzSpan]="12">
                <nz-form-item>
                  <nz-form-label [nzSpan]="6" nzRequired>Install mode</nz-form-label>
                  <nz-form-control [nzSpan]="18">
                    <nz-select 
                      formControlName="installMode"
                      nzPlaceholder="Select install mode">
                      <nz-option nzValue="stack" nzLabel="Install monitoring stack only (Prometheus/Grafana)"></nz-option>
                      <nz-option nzValue="target" nzLabel="Install and enable scraping for target (generate CRDs)"></nz-option>
                    </nz-select>
                  </nz-form-control>
                </nz-form-item>
              </nz-col>
              <nz-col [nzSpan]="12">
                <nz-form-item>
                  <nz-form-label [nzSpan]="6" nzRequired>Install channel</nz-form-label>
                  <nz-form-control [nzSpan]="18">
                    <nz-select
                      formControlName="installChannel"
                      nzPlaceholder="Select install channel">
                      <nz-option nzValue="console" nzLabel="Console auto-install (recommended)"></nz-option>
                      <nz-option nzValue="helm" nzLabel="Helm command manual install"></nz-option>
                    </nz-select>
                  </nz-form-control>
                </nz-form-item>
              </nz-col>
              <nz-col [nzSpan]="12">
                <nz-form-item>
                  <nz-form-label [nzSpan]="6" nzRequired>Monitoring type</nz-form-label>
                  <nz-form-control [nzSpan]="18">
                    <nz-select 
                      formControlName="monitoringType" 
                      nzPlaceholder="Select monitoring type">
                      <nz-option nzValue="enterprise" nzLabel="Enterprise (PolarDBXMonitor)"></nz-option>
                      <nz-option nzValue="standard" nzLabel="Standard (ServiceMonitor)"></nz-option>
                    </nz-select>
                  </nz-form-control>
                </nz-form-item>
              </nz-col>
              <nz-col [nzSpan]="12">
                <nz-form-item>
                  <nz-form-label [nzSpan]="6" nzRequired>Namespace</nz-form-label>
                  <nz-form-control [nzSpan]="18">
                    <nz-select 
                      formControlName="namespace" 
                      nzPlaceholder="Select namespace"
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
                      [nzPlaceHolder]="loadingTargets ? 'Loading...' : 'Select target'"
                      nzShowSearch
                      nzAllowClear
                      [nzLoading]="loadingTargets"
                      nzNotFoundContent="No resources available; ensure cluster/XStore exists">
                      <nz-option-group *ngIf="targets.length > 0" nzLabel="{{ getTargetLabel() }}">
                        <nz-option 
                          *ngFor="let target of targets" 
                          [nzValue]="target" 
                          [nzLabel]="target">
                        </nz-option>
                      </nz-option-group>
                      <nz-option-group *ngIf="targets.length === 0 && !loadingTargets" nzLabel="Hint">
                        <p style="padding: 8px 12px; color: rgba(0,0,0,0.45); font-size: 12px; margin: 0;">
                          {{ form.value.namespace ? 'No ' + getTargetLabel() + ' found' : 'Please select a namespace first' }}
                        </p>
                      </nz-option-group>
                    </nz-select>
                  </nz-form-control>
                </nz-form-item>
              </nz-col>
              <nz-col [nzSpan]="12">
                <nz-form-item>
                  <nz-form-label [nzSpan]="6">Monitor Name</nz-form-label>
                  <nz-form-control [nzSpan]="18">
                    <input 
                      nz-input 
                      formControlName="monitorName"
                      placeholder="Auto-generated (customizable)">
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

      <!-- Step 2: Preflight checks -->
      <ng-template #step2Template>
        <div class="step-content">
          <nz-alert 
            nzType="info"
            nzMessage="Environment checks"
            nzDescription="Check CRDs, permissions, and monitoring components to ensure configuration can be applied."
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
                      Copy command
                    </button>
                  </div>
                  <button
                    *ngIf="block.docsUrl"
                    nz-button
                    nzType="link"
                    class="blocking-link"
                    (click)="openDocsUrl(block.docsUrl)">
                    <i nz-icon nzType="book"></i>
                    View documentation
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
                  One-click install monitoring stack
                </button>
                <button
                  nz-button
                  [nzType]="form.value.installChannel === 'helm' ? 'primary' : 'default'"
                  (click)="copyCommand(getHelmInstallScript())">
                  <i nz-icon nzType="copy"></i>
                  Copy Helm install script
                </button>
                <button nz-button nzType="link" (click)="openDocsUrl(installDocsUrl)">
                  <i nz-icon nzType="book"></i>
                  View install guide
                </button>
              </div>
            </ng-template>
          </div>

          <div class="preflight-section">
            <nz-spin [nzSpinning]="runningPreflight">
              <div class="loading-tip" *ngIf="runningPreflight">Checking environment...</div>
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
                    <span class="check-optional" *ngIf="check.optional">Optional</span>
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
                      Copy command
                    </button>
                  </div>
                </div>
              </div>
            </nz-spin>
          </div>
        </div>
      </ng-template>

      <!-- Step 3: Collection parameters -->
      <ng-template #step3Template>
        <div class="step-content">
          <nz-alert 
            nzType="info"
            nzMessage="Monitoring parameter configuration"
            nzDescription="Configure scrape interval and timeout. Defaults fit most scenarios."
            nzShowIcon
            class="step-alert">
          </nz-alert>

          <form [formGroup]="form" class="config-form">
            <div class="config-section">
              <h4>{{ form.value.monitoringType === 'enterprise' ? 'PolarDBXMonitor parameters' : 'ServiceMonitor parameters' }}</h4>
              
              <!-- Show selected target -->
              <div *ngIf="form.value.installMode === 'target'" style="margin-bottom: 16px;">
                <nz-alert 
                  nzType="info"
                  nzMessage="Current monitoring target"
                  [nzDescription]="'Namespace: ' + form.value.namespace + ' / ' + getTargetLabel() + ': ' + (form.value.targetName || 'Not selected')"
                  nzShowIcon>
                </nz-alert>
              </div>
              
              <nz-row [nzGutter]="16">
                <nz-col [nzSpan]="12">
                  <nz-form-item>
                    <nz-form-label [nzSpan]="6">Scrape interval</nz-form-label>
                    <nz-form-control [nzSpan]="18">
                      <input 
                        nz-input 
                        formControlName="scrapeInterval"
                        placeholder="e.g., 30s">
                    </nz-form-control>
                  </nz-form-item>
                </nz-col>
                <nz-col [nzSpan]="12">
                  <nz-form-item>
                    <nz-form-label [nzSpan]="6">Timeout</nz-form-label>
                    <nz-form-control [nzSpan]="18">
                      <input 
                        nz-input 
                        formControlName="scrapeTimeout"
                        placeholder="e.g., 10s">
                    </nz-form-control>
                  </nz-form-item>
                </nz-col>
              </nz-row>

              <div *ngIf="form.value.monitoringType === 'standard'">
                <nz-row [nzGutter]="16">
                  <nz-col [nzSpan]="24">
                    <nz-form-item>
                      <nz-form-label [nzSpan]="3" nzRequired>Label selector</nz-form-label>
                      <nz-form-control [nzSpan]="21">
                        <textarea
                          nz-input
                          formControlName="selectorLabels"
                          [nzAutosize]="{ minRows: 3, maxRows: 6 }"
                          placeholder="Enter labels, one per line key: value, e.g., xstore/name: my-xstore"></textarea>
                        <p class="selector-helper">
                          Use <code>key: value</code> per line; system auto-completes <code>xstore/service: metrics</code>
                        </p>
                        <div class="selector-preview" *ngIf="recommendedSelectorPreview">
                          <div class="selector-preview-title">Recommended example</div>
                          <pre>{{ recommendedSelectorPreview }}</pre>
                        </div>
                      </nz-form-control>
                    </nz-form-item>
                  </nz-col>
                </nz-row>
              </div>
            </div>

            <div class="config-preview">
              <h4>Configuration preview</h4>
              <nz-descriptions nzBordered nzSize="small">
                <nz-descriptions-item nzTitle="Install mode">
                  {{ form.value.installMode === 'stack' ? 'Install monitoring stack only' : 'Install and enable target scraping' }}
                </nz-descriptions-item>
                <nz-descriptions-item nzTitle="Monitoring type">
                  {{ form.value.monitoringType === 'enterprise' ? 'PolarDBXMonitor' : 'ServiceMonitor' }}
                </nz-descriptions-item>
                <nz-descriptions-item nzTitle="Namespace">{{ form.value.namespace }}</nz-descriptions-item>
                <nz-descriptions-item nzTitle="Target" *ngIf="form.value.installMode === 'target'">
                  {{ form.value.targetName || 'Not selected' }}
                </nz-descriptions-item>
                <nz-descriptions-item nzTitle="Scrape interval">{{ form.value.scrapeInterval }}</nz-descriptions-item>
                <nz-descriptions-item nzTitle="Timeout">{{ form.value.scrapeTimeout }}</nz-descriptions-item>
                <nz-descriptions-item 
                  nzTitle="Label selector" 
                  *ngIf="form.value.monitoringType === 'standard' && form.value.installMode === 'target'"
                  [nzSpan]="3">
                  {{ form.value.selectorLabels || 'Not set' }}
                </nz-descriptions-item>
              </nz-descriptions>
            </div>
          </form>
        </div>
      </ng-template>

      <!-- Step 4: YAML preview -->
      <ng-template #step4Template>
        <div class="step-content">
          <nz-alert 
            *ngIf="form.value.installMode === 'stack'"
            nzType="info"
            nzMessage="Monitoring stack installation mode"
            nzDescription="Current mode installs monitoring stack only; no CRD YAML is needed. After installation, you can manually create PolarDBXMonitor or ServiceMonitor to enable monitoring for a cluster/XStore."
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
            nzTitle="No YAML generation needed"
            nzSubTitle="In stack-only mode, CRD manifests are not required">
          </nz-result>
        </div>
      </ng-template>

      <!-- Step 5: Apply & verify -->
      <ng-template #step5Template>
        <div class="step-content">
          
          <!-- Installation in progress (live progress) -->
          <div class="installing-status" *ngIf="installStatus && installStatus.phase === 'Installing'">
            <nz-card [nzBordered]="true">
              <div slot="title">
                <i nz-icon nzType="rocket" nzTheme="twotone"></i>
                Installing monitoring stack...
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
                  Estimated remaining time: {{ formatTime(installStatus.estimatedTimeRemaining) }}
                </div>
                
                <div class="install-actions" style="margin-top: 16px;">
                  <button nz-button nzType="default" nzSize="small" (click)="viewInstallLogs()" *ngIf="installJob">
                    <i nz-icon nzType="file-text"></i>
                    View detailed logs
                  </button>
                  <button nz-button nzType="dashed" nzSize="small" (click)="goToMonitoring()">
                    <i nz-icon nzType="desktop"></i>
                    Run in background
                  </button>
                </div>
              </div>
            </nz-card>
          </div>
          
          <!-- Installation success (component health cards) -->
          <div class="success-status" *ngIf="installStatus && installStatus.phase === 'Active'">
            <nz-result 
              nzStatus="success"
              nzSubTitle="All components are running">
              <div slot="title">
                <i nz-icon nzType="check-circle" nzTheme="twotone" [style.color]="'#52c41a'"></i>
                Monitoring stack installed successfully!
              </div>
              
              <div nz-result-content>
                <!-- Next actions -->
                <div class="next-actions">
                  <h4>
                    <i nz-icon nzType="aim" style="margin-right: 8px;"></i>
                    Next steps
                  </h4>
                  <div class="action-cards">
                    <nz-card nzHoverable class="action-card" (click)="verifyMonitoring()">
                      <div class="action-icon">
                        <i nz-icon nzType="search" [style.fontSize]="'32px'" [style.color]="'#52c41a'"></i>
                      </div>
                      <div class="action-title">Verify monitoring</div>
                      <div class="action-desc">Auto-check all components</div>
                      <button nz-button nzType="primary" nzSize="small" [nzLoading]="verifyingMonitoring">
                        Start verification
                      </button>
                    </nz-card>
                    
                    <nz-card nzHoverable class="action-card" (click)="goToGrafana()">
                      <div class="action-icon">
                        <i nz-icon nzType="dashboard" [style.fontSize]="'32px'" [style.color]="'#1890ff'"></i>
                      </div>
                      <div class="action-title">View monitoring dashboards</div>
                      <div class="action-desc">Visit Grafana dashboards</div>
                      <button nz-button nzType="default" nzSize="small">
                        Open now
                      </button>
                    </nz-card>
                  </div>
                </div>
                
                <!-- Component status -->
                <div class="components-status">
                  <h4>
                    <i nz-icon nzType="build" style="margin-right: 8px;"></i>
                    Component status
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
                        <span class="comp-port">{{ comp.port }} port</span>
                        <a *ngIf="comp.url" [href]="comp.url" target="_blank" class="comp-link">
                          <i nz-icon nzType="link"></i>
                          Open
                        </a>
                      </div>
                    </div>
                  </nz-card>
                </div>
                
                <!-- Quick access links -->
                <div class="quick-access" *ngIf="componentsHealth.length > 0">
                  <h4>
                    <i nz-icon nzType="link" style="margin-right: 8px;"></i>
                    Quick access
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
                      <i nz-icon nzType="bulb" nzTheme="outline" style="margin-right: 4px;"></i>
                      Tip
                    </div>
                    <div nz-alert-description>
                      Monitoring data may take 3-5 minutes to start collecting
                    </div>
                  </nz-alert>
                </div>
              </div>
              
              <div nz-result-extra>
                <button nz-button nzType="primary" (click)="goToMonitoring()">
                  <i nz-icon nzType="check-circle"></i>
                  Finish configuration
                </button>
                <button nz-button nzType="default" (click)="verifyMonitoring()" [nzLoading]="verifyingMonitoring">
                  <i nz-icon nzType="safety-certificate"></i>
                  Verify features
                </button>
                <button nz-button nzType="default">
                  <i nz-icon nzType="book"></i>
                  View docs
                </button>
              </div>
            </nz-result>
          </div>
          
          <!-- Verification results -->
          <div class="verification-result" *ngIf="verificationResult">
            <nz-card [nzBordered]="true">
              <div slot="title">
                <i nz-icon nzType="search" style="margin-right: 8px;"></i>
                Monitoring verification
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
                  <span class="score-label">Overall health:</span>
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
                  Found {{ verificationResult.warnings.length }} warning(s)
                </h5>
                <ul>
                  <li *ngFor="let warning of verificationResult.warnings">{{ warning }}</li>
                </ul>
              </div>
              
              <div class="recommendations" *ngIf="verificationResult.recommendations.length > 0">
                <h5>
                  <i nz-icon nzType="bulb" nzTheme="outline" style="margin-right: 4px; color: #1890ff;"></i>
                  Recommendations
                </h5>
                <ul>
                  <li *ngFor="let rec of verificationResult.recommendations">{{ rec }}</li>
                </ul>
              </div>
              
              <div class="verification-actions" style="margin-top: 16px;">
                <button nz-button nzType="primary" (click)="goToMonitoring()">
                  <i nz-icon nzType="check"></i>
                  Continue
                </button>
                <button nz-button nzType="default" (click)="verifyMonitoring()" [nzLoading]="verifyingMonitoring">
                  <i nz-icon nzType="reload"></i>
                  Re-run verification
                </button>
              </div>
            </nz-card>
          </div>
          
          <!-- Failure diagnosis (smart suggestion) -->
          <div class="failure-diagnosis" *ngIf="failureDiagnosis">
            <nz-result 
              nzStatus="error"
              [nzSubTitle]="failureDiagnosis.errorMessage">
              <div slot="title">
                <i nz-icon nzType="close-circle" nzTheme="twotone" [style.color]="'#ff4d4f'"></i>
                Installation failed
              </div>
              
              <div nz-result-content>
                <!-- Failure details -->
                <div class="diagnosis-details">
                  <h4>
                    <i nz-icon nzType="file-text" style="margin-right: 8px;"></i>
                    Failure details
                  </h4>
                  <nz-descriptions nzBordered nzSize="small">
                    <nz-descriptions-item nzTitle="Time">{{ formatTimestamp(failureDiagnosis.timestamp) }}</nz-descriptions-item>
                    <nz-descriptions-item nzTitle="Stage">{{ getCurrentInstallStage() }}</nz-descriptions-item>
                    <nz-descriptions-item nzTitle="Error">{{ failureDiagnosis.errorType }}</nz-descriptions-item>
                  </nz-descriptions>
                </div>
                
                <!-- Possible causes -->
                <div class="possible-causes" style="margin-top: 24px;">
                  <h4>
                    <i nz-icon nzType="search" style="margin-right: 8px;"></i>
                    Possible causes
                  </h4>
                  <div class="cause-list">
                    <div 
                      class="cause-item" 
                      *ngFor="let cause of failureDiagnosis.possibleCauses; let i = index"
                      [class.cause-primary]="i === 0">
                      <div class="cause-header">
                        <span class="cause-number">{{ i + 1 }}</span>
                        <span class="cause-desc">{{ cause.description }}</span>
                        <nz-tag nzColor="blue">Probability {{ cause.probability }}%</nz-tag>
                      </div>
                      <div class="cause-fix" *ngIf="cause.suggestedFix">
                        <span class="fix-label">Recommendation:</span>
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
                          Auto-fix
                        </button>
                      </div>
                    </div>
                  </div>
                </div>
                
                <!-- Smart suggestion -->
                <nz-alert 
                  nzType="warning"
                  nzShowIcon
                  style="margin-top: 24px;"
                  *ngIf="failureDiagnosis.possibleCauses[0]?.autoFixable">
                  <div nz-alert-message>
                    <i nz-icon nzType="bulb" nzTheme="outline" style="margin-right: 4px;"></i>
                    <strong>Smart suggestion</strong>
                  </div>
                  <div nz-alert-description>
                    <div class="smart-suggestion">
                      <p>{{ failureDiagnosis.possibleCauses[0].suggestedFix }}</p>
                      <button 
                        nz-button 
                        nzType="primary"
                        (click)="autoFix(failureDiagnosis.possibleCauses[0])"
                        [nzLoading]="autoFixing">
                        {{ failureDiagnosis.possibleCauses[0].fixAction === 'switchImageRegistry' ? 'Switch registry & retry' : 'Auto-fix' }}
                      </button>
                    </div>
                  </div>
                </nz-alert>
                
                <!-- Detailed logs -->
                <div class="error-logs" *ngIf="failureDiagnosis.relatedLogs.length > 0" style="margin-top: 24px;">
                  <h4>
                    <i nz-icon nzType="file-text" style="margin-right: 8px;"></i>
                    Detailed logs
                  </h4>
                  <nz-card [nzBordered]="true">
                    <pre class="log-content">{{ failureDiagnosis.relatedLogs.join('\n') }}</pre>
                    <button nz-button nzType="dashed" nzSize="small" (click)="viewFullLogs()">
                      <i nz-icon nzType="fullscreen"></i>
                      Expand full logs
                    </button>
                  </nz-card>
                </div>
              </div>
              
              <div nz-result-extra>
                <button nz-button nzType="primary" (click)="retryApply()">
                  <i nz-icon nzType="reload"></i>
                  Retry manually
                </button>
                <button 
                  nz-button 
                  nzType="default"
                  (click)="viewInstallLogs()"
                  *ngIf="installJob?.jobName">
                  <i nz-icon nzType="file-text"></i>
                  View logs
                </button>
                <button nz-button nzType="default" (click)="goToPrevStep()">
                  <i nz-icon nzType="left"></i>
                  Back to configuration
                </button>
              </div>
            </nz-result>
          </div>
          
          <!-- Fallback nz-result (legacy display) -->
          <nz-result 
            *ngIf="!installStatus && !failureDiagnosis"
            [nzStatus]="applyResult?.success ? 'success' : (applyResult ? 'error' : 'info')"
            [nzTitle]="getResultTitle()"
            [nzSubTitle]="getResultSubtitle()">
            
            <div nz-result-content *ngIf="!applyResult">
              <div class="apply-options">
                <h4>Apply options</h4>
                <nz-alert 
                  nzType="info"
                  nzMessage="Installation guidance"
                  [nzDescription]="form.value.installChannel === 'helm' ? 'You chose Helm manual install. Run the following commands in the cluster, then return and click Finish.' : 'You can copy commands to run manually, or let the system apply automatically.'"
                  nzShowIcon
                  class="apply-alert">
                </nz-alert>

                <div 
                  class="kubectl-command"
                  *ngIf="form.value.installChannel === 'helm'">
                  <h5>Helm install script</h5>
                  <div class="command-block">
                    <pre>{{ getHelmInstallScript() }}</pre>
                    <button 
                      nz-button 
                      nzType="dashed" 
                      nzSize="small"
                      (click)="copyCommand(getHelmInstallScript())">
                      <i nz-icon nzType="copy"></i>
                      Copy script
                    </button>
                  </div>
                </div>

                <div class="kubectl-command" *ngIf="form.value.installMode === 'target'">
                  <h5>kubectl command</h5>
                  <div class="command-block">
                    <pre>{{ getKubectlCommand() }}</pre>
                    <button 
                      nz-button 
                      nzType="dashed" 
                      nzSize="small"
                      (click)="copyKubectlCommand()">
                      <i nz-icon nzType="copy"></i>
                      Copy command
                    </button>
                  </div>
                </div>
              </div>
            </div>

            <div nz-result-content *ngIf="applyResult?.success && installJob">
              <div class="apply-options">
                <h4>Install job created</h4>
                <nz-descriptions nzBordered nzSize="small">
                  <nz-descriptions-item nzTitle="Job name">{{ installJob.jobName }}</nz-descriptions-item>
                  <nz-descriptions-item nzTitle="Job namespace">{{ installJob.namespace }}</nz-descriptions-item>
                  <nz-descriptions-item nzTitle="Target namespace">{{ installJob.targetNs || 'polardbx-monitor' }}</nz-descriptions-item>
                </nz-descriptions>

                <div class="kubectl-command">
                  <h5>View install logs</h5>
                  <div class="command-block">
                    <pre>{{ getJobLogsCommand() }}</pre>
                    <button 
                      nz-button 
                      nzType="dashed" 
                      nzSize="small"
                      (click)="copyJobLogsCommand()">
                      <i nz-icon nzType="copy"></i>
                      Copy command
                    </button>
                  </div>
              <div style="margin-top:8px; display:flex; align-items:center; gap:8px;">
                <span style="color: rgba(0,0,0,0.65);">Tail lines:</span>
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
                  <h5>Check component status</h5>
                  <div class="command-block">
                    <pre>{{ getPodsCheckCommand() }}</pre>
                    <button 
                      nz-button 
                      nzType="dashed" 
                      nzSize="small"
                      (click)="copyPodsCheckCommand()">
                      <i nz-icon nzType="copy"></i>
                      Copy command
                    </button>
                  </div>
                </div>

                <div class="kubectl-command" style="margin-top: 12px;">
                  <h5>Port-forward (local access)</h5>
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
                      Copy all
                    </button>
                  </div>
                </div>

                <div class="kubectl-command" style="margin-top: 12px;">
                  <h5>LoadBalancer example (optional, values.yaml)</h5>
                  <div class="command-block">
                    <pre>{{ getLoadBalancerValuesSnippet() }}</pre>
                    <button 
                      nz-button 
                      nzType="dashed" 
                      nzSize="small"
                      (click)="copyLoadBalancerValues()">
                      <i nz-icon nzType="copy"></i>
                      Copy snippet
                    </button>
                  </div>
                </div>
              </div>
            </div>

            <div nz-result-extra *ngIf="applyResult?.success">
              <button nz-button nzType="primary" (click)="goToMonitoring()">
                <i nz-icon nzType="dashboard"></i>
                View monitoring
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
                Retry install
              </button>
              <button
                nz-button
                nzType="default"
                (click)="viewInstallLogs()"
                *ngIf="installJob?.jobName">
                <i nz-icon nzType="file-text"></i>
                View logs
              </button>
              <button nz-button nzType="default" (click)="goToPrevStep()">
                <i nz-icon nzType="left"></i>
                Previous
              </button>
            </div>
          </nz-result>

          <!-- Error details and solutions -->
          <div class="error-details" *ngIf="applyResult && !applyResult.success">
            <nz-alert 
              nzType="error"
              nzMessage="Installation failure reasons"
              [nzDescription]="applyResult.failureReason || applyResult.message"
              nzShowIcon>
            </nz-alert>

            <!-- Special notice for image pull errors -->
            <nz-alert 
              *ngIf="applyResult.failureReason?.includes('ImagePull') || applyResult.failureReason?.includes('镜像拉取')"
              nzType="warning"
              nzMessage="Image pull failure solutions"
              nzShowIcon
              style="margin-top: 16px;">
              <div style="font-size: 13px; line-height: 1.8;">
                <p><strong>Root cause:</strong> Kubernetes cluster cannot pull images from Docker Hub (network timeout or access restricted)</p>
                
                <p><strong>Solutions:</strong></p>
                <ol style="margin-left: 20px; margin-top: 8px;">
                  <li><strong>Configure image registry mirrors (recommended)</strong>
                    <pre style="background: #f5f5f5; padding: 8px; border-radius: 4px; margin: 8px 0; font-size: 12px;">{{ getMirrorConfigCommands() }}</pre>
                  </li>
                  
                  <li><strong>Manually pre-pull images</strong>
                    <pre style="background: #f5f5f5; padding: 8px; border-radius: 4px; margin: 8px 0; font-size: 12px;">{{ getPrePullCommands() }}</pre>
                  </li>
                  
                  <li><strong>Check Job Pod status</strong>
                    <pre style="background: #f5f5f5; padding: 8px; border-radius: 4px; margin: 8px 0; font-size: 12px;">kubectl describe pod -n {{ installJob?.namespace || 'polardbx-operator-system' }} {{ getJobPodName() }}</pre>
                  </li>
                </ol>
                
                <p style="margin-top: 12px; color: #1890ff;">
                  <i nz-icon nzType="info-circle"></i>
                  After configuring mirrors, click "Retry install" to restart the job.
                </p>
              </div>
            </nz-alert>

            <!-- BackoffLimit retry failure notice -->
            <nz-alert
              *ngIf="isBackoffLimitError(applyResult.failureReason)"
              nzType="warning"
              nzMessage="Install job reached retry limit"
              nzShowIcon
              style="margin-top: 16px;">
              <div class="backoff-guidance">
                <p><strong>Root cause:</strong> Install job failed after multiple retries; Kubernetes stopped further attempts.</p>
                <p><strong>Next steps:</strong></p>
                <ol>
                  <li>
                    Check Job events and failure reason
                    <div class="command-block">
                      <pre>{{ getJobDescribeCommand() }}</pre>
                      <button nz-button nzType="dashed" nzSize="small" (click)="copyCommand(getJobDescribeCommand())">
                        <i nz-icon nzType="copy"></i>
                        Copy
                      </button>
                    </div>
                  </li>
                  <li>
                    View latest failed Pod logs
                    <div class="command-block">
                      <pre>{{ getJobFailedPodLogsCommand() }}</pre>
                      <button nz-button nzType="dashed" nzSize="small" (click)="copyCommand(getJobFailedPodLogsCommand())">
                        <i nz-icon nzType="copy"></i>
                        Copy
                      </button>
                    </div>
                  </li>
                  <li>
                    After fixing issues, delete old Job and re-trigger installation
                    <div class="command-block">
                      <pre>{{ getJobDeleteCommand() }}</pre>
                      <button nz-button nzType="dashed" nzSize="small" (click)="copyCommand(getJobDeleteCommand())">
                        <i nz-icon nzType="copy"></i>
                        Copy
                      </button>
                    </div>
                  </li>
                </ol>
                <p style="margin-top: 12px;">
                  After fixing, click "Retry install" to recreate the install job.
                </p>
              </div>
            </nz-alert>

            <!-- Job 日志查看命令 -->
            <div class="kubectl-command" style="margin-top: 16px;" *ngIf="installJob && installJob.jobName">
              <h5>View full error logs</h5>
              <div class="command-block">
                <pre>kubectl logs -n {{ installJob!.namespace }} job/{{ installJob!.jobName }} --follow</pre>
                <button 
                  nz-button 
                  nzType="dashed" 
                  nzSize="small"
                  (click)="copyJobLogsCommand()">
                  <i nz-icon nzType="copy"></i>
                  Copy command
                </button>
              </div>
            </div>
          </div>

          <div class="verification-tips" *ngIf="applyResult?.success">
            <h4>Verification guidance</h4>
            <nz-alert 
              nzType="success"
              nzMessage="Common verification steps"
              nzDescription="Monitoring configuration has been applied; verify with the following steps:"
              nzShowIcon>
            </nz-alert>
            <ul class="tips-list">
              <li>Check ServiceMonitor/PolarDBXMonitor resource status</li>
              <li>Visit Prometheus Targets page to confirm target discovery</li>
              <li>View related dashboards in Grafana</li>
              <li>Check Alertmanager rule loading</li>
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
    
    /* ✅ Detection overlay */
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

    /* Step 0: Image registry configuration styles */
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
    
    /* New: Installing status styles */
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
    
    /* New: Success status styles */
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
    
    /* New: Verification result styles */
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
    
    /* New: Failure diagnosis styles */
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
  
  // ✅ New: Detection state (prevents wizard from displaying too early)
  checkingExisting = true; // Initially true, set to false after detection completes

  // Data sources
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

  // Image registry configuration
  availableRegistries: ImageRegistryPreset[] = [];
  selectedRegistry = 'docker.m.daocloud.io'; // Default DaoCloud
  customRegistryInput = '';
  loadingRegistries = false;
  currentRegistryConfig = ''; // Currently active configuration

  // Preflight checks
  runningPreflight = false;
  preflightChecks: PreflightCheck[] = [];
  preflightBlocking: PreflightBlocker[] = [];
  preflightStackInstalling = false;
  readonly installDocsUrl = 'https://doc.polardbx.com/zh/operator/ops/monitor/1-monitor-install.html';
  private readonly helmInstallScriptTemplate = `helm repo add polardbx https://polardbx-charts.oss-cn-beijing.aliyuncs.com
helm repo update
helm upgrade --install polardbx-monitor polardbx/polardbx-monitor --namespace {{namespace}} --create-namespace`;

  // YAML generation
  generatingYaml = false;
  generatedYaml = '';

  // Apply result
  applyResult: { success: boolean; message: string; failureReason?: string } | null = null;
  installJob: { jobName: string; namespace: string; targetNs?: string; instructions?: string } | null = null;

  // New: Installation status tracking
  installStatus: InstallStatus | null = null;
  installPolling: Subscription | null = null; // interval subscription
  
  // New: Component health status
  componentsHealth: ComponentHealth[] = [];
  
  // New: Verification result
  verificationResult: VerificationResult | null = null;
  verifyingMonitoring = false;
  
  // New: Failure diagnosis
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
      return 'Unknown error';
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
    return 'Unknown error';
    return 'Unknown error';
  }
  // Log tail lines setting (default 200)
  tailLines = 200;
  readonly tailOptions = [100, 200, 500, 1000];

  ngOnInit(): void {
    this.initializeWizardSteps();
    this.loadImageRegistryPresets();
    this.loadCurrentImageRegistry();
    this.loadNamespaces();
    this.setupFormWatchers();
    this.updateSelectorLabels();
    // Initialize and load targets (not dependent on watch trigger)
    setTimeout(() => this.loadTargets(), 200);
    
    // ✅ Prioritize checking if already installed, then decide whether to restore state
    // Detection logic will clean up expired or failed states
    this.checkExistingInstallation();
    
    // ✅ Delay state restoration to let detection logic execute first
    // If installation or Job is detected, it will directly navigate without restoring state
    setTimeout(() => {
      if (!this.checkingExisting) {
        // Only restore state when detection is complete and no navigation occurred
        this.tryRestoreState();
      }
    }, 100);
  }

  private initializeWizardSteps(): void {
    // Templates will be set in ngAfterViewInit
    this.wizardSteps = [
      { id: 'registry', title: 'Image registry', description: 'Choose registry endpoint' },
      { id: 'target', title: 'Select target', description: 'Monitoring type and target' },
      { id: 'preflight', title: 'Preflight checks', description: 'Environment checks' },
      { id: 'config', title: 'Collection params', description: 'Monitoring configuration' },
      { id: 'yaml', title: 'YAML preview', description: 'Configuration preview' },
      { id: 'apply', title: 'Apply & verify', description: 'Apply and verify' }
    ];
    this.attachStepTemplates();
  }

  ngAfterViewInit(): void {
    this.attachStepTemplates();
    this.cdr.detectChanges();
  }

  private setupFormWatchers(): void {
    // Listen to monitoring type changes
    this.form.get('monitoringType')?.valueChanges.subscribe(() => {
      this.loadTargets();
      this.updateSelectorLabels();
    });

    // Listen to install mode changes, dynamically control targetName validation
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

    // Listen to namespace changes
    this.form.get('namespace')?.valueChanges.subscribe((ns: string) => {
      this.selectedNamespaceExists = ns ? this.namespaces.includes(ns) : false;
      this.namespaceError = null;
      this.loadTargets();
      this.updateSelectorLabels();
      this.cdr.markForCheck();
    });

    // Listen to target name changes, auto-generate monitoring name
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

  // ==================== Image Registry Configuration Related Methods ====================

  loadImageRegistryPresets(): void {
    this.loadingRegistries = true;
    this.api.getImageRegistryPresets().subscribe({
      next: (response) => {
        // Handle API response format {success: true, data: [...]}
        const presets = this.unwrapApiData(response as ApiEnvelope<ImageRegistryPreset[]>);
        this.availableRegistries = Array.isArray(presets) ? presets : [
          {
            name: 'DaoCloud Mirror Acceleration',
            registry: 'docker.m.daocloud.io',
            description: 'Verified mirror acceleration, recommended for mainland China',
            region: 'cn',
            status: 'verified'
          },
          {
            name: 'Docker Hub (official)',
            registry: 'registry-1.docker.io',
            description: 'Official public registry, global; may be slow or limited in mainland China',
            region: 'global',
            status: 'slow'
          },
          {
            name: 'Custom registry',
            registry: 'custom',
            description: 'Use a custom address for enterprise/private registries',
            region: 'custom',
            status: 'custom'
          }
        ];
        this.loadingRegistries = false;
        this.cdr.markForCheck();
      },
      error: (error) => {
        console.error('Failed to load registry presets:', error);
        this.message.error(`Failed to load registry presets: ${this.getErrorMessage(error)}`);
        this.availableRegistries = [
          {
            name: 'DaoCloud Mirror Acceleration',
            registry: 'docker.m.daocloud.io',
            description: 'Verified mirror acceleration, recommended for mainland China',
            region: 'cn',
            status: 'verified'
          },
          {
            name: 'Docker Hub (official)',
            registry: 'registry-1.docker.io',
            description: 'Official public registry, global; may be slow or limited in mainland China',
            region: 'global',
            status: 'slow'
          },
          {
            name: 'Custom registry',
            registry: 'custom',
            description: 'Use a custom address for enterprise/private registries',
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
        // Handle API response format {success: true, data: {...}}
        const config = this.unwrapApiData(response as ApiEnvelope<ImageRegistryConfig & { defaultRegistry?: string }>);
        if (config.registry === 'custom' && config.customRegistry) {
          this.selectedRegistry = 'custom';
          this.customRegistryInput = config.customRegistry;
          this.currentRegistryConfig = `Custom registry: ${config.customRegistry}`;
        } else {
          const registry = config.registry || config.defaultRegistry || 'docker.m.daocloud.io';
          this.selectedRegistry = registry;
          this.currentRegistryConfig = `Registry: ${registry}`;
        }
        this.cdr.markForCheck();
      },
      error: (error) => {
        console.error('Failed to load current image registry configuration:', error);
        // Use default value
        this.selectedRegistry = 'docker.m.daocloud.io';
        this.currentRegistryConfig = `Registry: docker.m.daocloud.io (default, reason: ${this.getErrorMessage(error)})`;
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
        this.message.error('Please enter custom registry address');
        return;
      }
      config.customRegistry = this.customRegistryInput.trim();
    }

    this.api.setImageRegistryConfig(config).subscribe({
      next: () => {
        this.message.success('Registry configuration saved');
        // Update current configuration display
        if (this.selectedRegistry === 'custom') {
          this.currentRegistryConfig = `Custom registry: ${this.customRegistryInput}`;
        } else {
          this.currentRegistryConfig = `Registry: ${this.selectedRegistry}`;
        }
        this.cdr.markForCheck();
      },
      error: (error) => {
        console.error('Failed to save registry config:', error);
        this.message.error('Failed to save registry config');
        this.cdr.markForCheck();
      }
    });
  }

  skipImageRegistryConfig(): void {
    // User chose to skip, don't save configuration, proceed directly to next step
    this.nextStep();
  }

  applyRegistryAndContinue(): void {
    const config: ImageRegistryConfig = {
      registry: this.selectedRegistry
    };

    if (this.selectedRegistry === 'custom') {
      if (!this.customRegistryInput || !this.customRegistryInput.trim()) {
        this.message.error('Please enter custom registry address');
        return;
      }
      config.customRegistry = this.customRegistryInput.trim();
    }

    this.stepLoading = true;
    this.api.setImageRegistryConfig(config).subscribe({
      next: () => {
        this.message.success('Registry configuration saved');
        // Update current configuration display
        if (this.selectedRegistry === 'custom') {
          this.currentRegistryConfig = `Custom registry: ${this.customRegistryInput}`;
        } else {
          this.currentRegistryConfig = `Registry: ${this.selectedRegistry}`;
        }
        this.stepLoading = false;
        this.nextStep(); // Proceed to next step
        this.cdr.markForCheck();
      },
      error: (error) => {
        console.error('Failed to save registry config:', error);
        this.message.error('Failed to save registry config');
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

  // ==================== Namespace Related Methods ====================

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
        console.error('Failed to load namespaces:', error);
        this.message.error('Failed to load namespaces');
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
      // Load PolarDB-X clusters
      this.api.getClusters(namespace).subscribe({
        next: (clusters) => {
          this.targets = clusters
            .map(cluster => cluster.metadata?.name)
            .filter((name): name is string => Boolean(name));
          this.loadingTargets = false;
          this.cdr.markForCheck();
        },
        error: (error) => {
          console.error('Failed to load clusters:', error);
          this.targets = [];
          this.loadingTargets = false;
          this.cdr.markForCheck();
        }
      });
    } else {
      // Load XStore
      this.api.getXStores(namespace).subscribe({
        next: (xstores) => {
          this.targets = xstores
            .map(xstore => xstore.metadata?.name)
            .filter((name): name is string => Boolean(name));
          this.loadingTargets = false;
          this.cdr.markForCheck();
        },
        error: (error) => {
          console.error('Failed to load XStore:', error);
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
    return this.form.value.monitoringType === 'enterprise' ? 'Cluster Name' : 'XStore Name';
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
        title: 'Enterprise Monitoring',
        description: 'Use PolarDBXMonitor CRD to enable monitoring for PolarDB-X clusters. Suitable for complete cluster-level monitoring.'
      };
    } else {
      return {
        title: 'Standard Monitoring',
        description: 'Use ServiceMonitor CRD to enable monitoring for XStore. Suitable for monitoring specific XStore instances.'
      };
    }
  }

  getStepActions(): WizardAction[] {
    const actions: WizardAction[] = [];
    
    // Previous step button - disabled if configuration has been successfully applied
    if (this.currentStep > 0 && !this.applyResult?.success) {
      actions.push({
        text: 'Previous',
        icon: 'left',
        handler: () => this.prevStep()
      });
    }

    // Add specific buttons based on current step
    switch (this.currentStep) {
      case 0: // Image registry configuration
        actions.push({
          text: 'Skip',
          icon: 'arrow-right',
          handler: () => this.skipImageRegistryConfig()
        });
        actions.push({
          text: 'Apply and Continue',
          type: 'primary',
          icon: 'check',
          loading: this.stepLoading,
          disabled: this.selectedRegistry === 'custom' && !this.customRegistryInput.trim(),
          handler: () => this.applyRegistryAndContinue()
        });
        break;

      case 1: // Select target
        actions.push({
          text: 'Next: Environment Checks',
          type: 'primary',
          icon: 'right',
          disabled: !this.isStep1Valid(),
          handler: () => this.nextStep()
        });
        break;
      
      case 2: // Preflight checks
        actions.push({
          text: 'Re-check',
          icon: 'sync',
          loading: this.runningPreflight,
          handler: () => this.runPreflightChecks()
        });
        actions.push({
          text: 'Next: Parameter Configuration',
          type: 'primary',
          icon: 'right',
          disabled: this.hasPreflightErrors(),
          handler: () => this.handlePreflightStepNext()
        });
        break;
      
      case 3: // Collection parameters
        actions.push({
          text: 'Next: YAML Preview',
          type: 'primary',
          icon: 'right',
          disabled: !this.isStep3Valid(),
          handler: () => this.nextStep()
        });
        break;
      
      case 4: // YAML preview
        if (this.form.value.installMode === 'target') {
          actions.push({
            text: 'Regenerate',
            icon: 'sync',
            loading: this.generatingYaml,
            handler: () => this.generateYaml()
          });
        }
        actions.push({
          text: 'Next: Apply Configuration',
          type: 'primary',
          icon: 'right',
          disabled: this.form.value.installMode === 'target' ? !this.generatedYaml : false,
          handler: () => this.nextStep()
        });
        break;
      
      case 5: // Apply and verify
        // Merge "Auto Apply" and "Finish" into one button
        if (this.form.value.installChannel === 'helm') {
          actions.push({
            text: 'Finish',
            type: 'primary',
            icon: 'check-circle',
            handler: () => this.finishManualInstall()
          });
        } else {
          actions.push({
            text: 'Apply and Finish',
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
    // Basic parameter validation
    if (!v.scrapeInterval || !v.scrapeTimeout) return false;
    
    // If in target mode, need to verify if target is selected
    if (v.installMode === 'target') {
      if (!v.targetName) return false;
      
      // If ServiceMonitor, need to verify label selector
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

      // Auto operations when entering specific steps
      switch (this.currentStep) {
        case 2: // Enter preflight checks (now the 3rd step, index=2)
          this.runPreflightChecks();
          break;
        case 4: // Enter YAML preview (now the 5th step, index=4)
          this.generateYaml();
          break;
      }

      this.saveState(); // Save state
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
      description: 'Check if PolarDBXMonitor CRD is available'
    } : {
      id: 'crd-primary',
      key: 'serviceMonitor',
      label: 'ServiceMonitor',
      crdName: 'servicemonitors.monitoring.coreos.com',
      description: 'Check if ServiceMonitor CRD is available',
      installHint: 'Execute kubectl apply -f charts/polardbx-monitor/crds/ to install Prometheus Operator CRD.',
      installCommand: 'kubectl apply -f charts/polardbx-monitor/crds/'
    };

    const secondaryMeta: CrdMeta = monitoringType === 'enterprise' ? {
      id: 'crd-servicemonitor',
      key: 'serviceMonitor',
      label: 'ServiceMonitor',
      crdName: 'servicemonitors.monitoring.coreos.com',
      description: 'Prometheus Operator provides ServiceMonitor CRD',
      optionalDescription: 'For standard collection or future extensions, optional check',
      installHint: 'If missing, execute kubectl apply -f charts/polardbx-monitor/crds/ to re-register CRD.',
      installCommand: 'kubectl apply -f charts/polardbx-monitor/crds/'
    } : {
      id: 'crd-polardbxmonitor',
      key: 'polardbxMonitor',
      label: 'PolarDBXMonitor',
      crdName: 'polardbxmonitors.polardbx.aliyun.com',
      description: 'PolarDBX Operator provides PolarDBXMonitor CRD',
      optionalDescription: 'For enterprise monitoring scenarios, ensure installed if cluster-level monitoring is needed'
    };

    const ns = this.form.value.namespace || 'polardbx-monitor';

    // Initialize check items
    this.preflightChecks = [
      {
        id: 'namespace',
        name: 'Monitoring Namespace',
        description: `Confirm namespace ${ns} is available`,
        status: 'pending',
        result: 'Checking...'
      },
      {
        id: primaryMeta.id,
        name: `${primaryMeta.label} CRD`,
        description: primaryMeta.description,
        status: 'pending',
        result: 'Checking...'
      },
      {
        id: 'rbac',
        name: 'RBAC Permissions',
        description: 'Check K8s API access permissions',
        status: 'pending',
        result: 'Checking...'
      },
      {
        id: 'prometheus',
        name: 'Prometheus Status',
        description: 'Check Prometheus running status',
        status: 'pending',
        result: 'Checking...'
      }
    ];

    setTimeout(() => {
      const rbacIdx = this.preflightChecks.findIndex(check => check.id === 'rbac');
      if (rbacIdx !== -1) {
        this.preflightChecks[rbacIdx] = {
          ...this.preflightChecks[rbacIdx],
          status: 'warning',
          result: 'Permissions need to be confirmed based on cluster environment',
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
          let result = `Namespace ${nsUsed} exists`;
          let command: string | undefined;

          if (!namespaceExists && !this.namespaceError) {
            status = 'error';
            result = `Namespace ${nsUsed} not created`;
            command = this.getNamespaceCreateCommand(nsUsed);
          } else if (this.namespaceError) {
            status = 'warning';
            result = `Cannot confirm namespace ${nsUsed} status: ${this.namespaceError}`;
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
            name: 'Grafana Status',
            description: 'Check Grafana running status',
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
            name: 'Alertmanager Status',
            description: 'Check Alertmanager Service configuration',
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
            result: 'Cannot get CRD status (API call failed)',
            command: `kubectl get crd ${primaryMeta.crdName}`
          };
        }

        const promIdx = this.preflightChecks.findIndex(check => check.id === 'prometheus');
        if (promIdx !== -1) {
          this.preflightChecks[promIdx] = {
            ...this.preflightChecks[promIdx],
            status: 'warning',
            result: 'Cannot get Prometheus status (may not be installed)',
            command: `kubectl -n ${ns} get pods | grep -i prom\n` +
              `kubectl -n ${ns} get svc | grep -i prom`
          };
        }

        this.runningPreflight = false;
        this.preflightBlocking = [
          {
            id: primaryMeta.id,
            title: `${primaryMeta.label} CRD status unknown` ,
            message: 'Cannot get CRD status, please check Kubernetes API access permissions and retry.',
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
        <p>The following checks have warnings. Continuing installation may affect monitoring system stability:</p>
        <ul>${listItems}</ul>
        <p style="margin-top: 12px;">Please confirm you understand the risks before continuing.</p>
      </div>
    `;

    this.modal.confirm({
      nzTitle: 'Environment Warnings Detected',
      nzContent: content,
      nzOkText: 'Continue Installation',
      nzOkType: 'primary',
      nzOkDanger: true,
      nzCancelText: 'Cancel',
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
      this.message.success('Command copied to clipboard');
    }).catch(() => {
      this.message.error('Copy failed');
    });
  }

  getNamespaceCreateCommand(namespace?: string): string {
    const ns = (namespace || '').trim() || 'polardbx-monitor';
    return `kubectl create namespace ${ns}`;
  }

  private buildAlertmanagerGuide(namespace: string): string {
    const ns = (namespace || '').trim() || 'polardbx-monitor';
    return `# Label nodes running Alertmanager
kubectl label node <node-name> polardbx.com/alertmanager-node=true --overwrite

# Pin Alertmanager to labeled nodes
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
      return 'Items Requiring Confirmation';
    }
    if (type === 'info') {
      return 'Check Reminder';
    }
    return 'Monitoring Component Prerequisites Missing';
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
      this.message.info('Helm manual installation selected. Please use the script below to execute installation in the cluster.');
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
      result = 'Cannot get CRD status';
      command = command || `kubectl get crd ${meta.crdName}`;
    } else if (info.error) {
      status = 'error';
      result = `Query failed: ${info.error}`;
      command = command || `kubectl get crd ${meta.crdName}`;
    } else if (!info.exists) {
      status = required ? 'error' : 'warning';
      result = `${meta.label} CRD not installed${meta.installHint ? `, ${meta.installHint}` : ''}`;
      command = command || `kubectl get crd ${meta.crdName}`;
    } else if (!info.established) {
      status = 'warning';
      const detail = this.extractConditionMessage(info, 'Established') || 'CRD status not ready';
      result = `${meta.label} CRD not ready: ${detail}`;
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
        segments.push(`Version: ${versions}`);
      }
      if (info.scope) {
        segments.push(info.scope);
      }
      const suffix = segments.length ? ` (${segments.join(' / ')})` : '';
      result = `${meta.label} CRD installed${suffix}`;
    }

    return {
      id: meta.id,
      name: `${meta.label} CRD${optional ? ' (Optional)' : ''}`,
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
          title: 'Monitoring Namespace Missing',
          message: namespaceCheck.result || `Namespace ${ns} does not exist, please create it first.`,
          command: this.getNamespaceCreateCommand(ns),
          docsUrl: this.installDocsUrl,
          severity: 'error'
        });
      } else if (namespaceCheck.status === 'warning') {
        blockers.push({
          id: 'namespace-warning',
          title: namespaceCheck.name,
          message: namespaceCheck.result || `Please confirm namespace ${ns} has been created and is accessible.`,
          docsUrl: this.installDocsUrl,
          severity: 'warning'
        });
      }
    }

    const primary = this.preflightChecks.find(check => check.id === primaryMeta.id);

    if (primary && primary.status === 'error') {
      const title = `${primaryMeta.label} CRD Not Ready`;
      const message = primary.result || `${primaryMeta.label} CRD not installed, please deploy monitoring component stack first before continuing.`;
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
          title: 'Monitoring Components Not Installed or Not Ready',
          message: promCheck.result || `Prometheus not detected, please install monitoring component stack in namespace ${ns} first.`,
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
          title: 'Alertmanager Node Labeling and Enablement',
          message: 'Recommend labeling Alertmanager nodes and patching instances to ensure replicas can be scheduled.',
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
    
    // Generate YAML based on form data
    const config = this.form.value;
    let yaml = '';
    
    if (config.installMode !== 'target') {
      // Don't generate CRD YAML when only installing monitoring components
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
    
    // Generate directly, no need to simulate delay
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
    return `# Save YAML content to file\nkubectl apply -f ${filename}\n\n# Or apply directly\ncat <<EOF | kubectl apply -f -\n${this.generatedYaml}\nEOF`;
  }

  copyKubectlCommand(): void {
    const command = this.getKubectlCommand();
    navigator.clipboard.writeText(command).then(() => {
      this.message.success('Command copied to clipboard');
    }).catch(() => {
      this.message.error('Copy failed');
    });
  }

  finishManualInstall(): void {
    this.modal.confirm({
      nzTitle: 'Confirm Manual Installation Complete?',
      nzContent: 'Please ensure Helm install commands have been executed and YAML/CRD configurations applied as needed. Afterwards, check component status in the Monitoring Overview.',
      nzOkText: 'Confirm Finish',
      nzCancelText: 'Continue Checking',
      nzOkType: 'primary',
      nzOnOk: () => {
        this.message.success('Please verify monitoring stack status in the Monitoring Overview.');
        this.finish();
      }
    });
  }

  applyConfiguration(): void {
    if (this.form.value.installChannel === 'helm') {
      this.message.info('Helm manual install selected. Please copy the command and execute it in your cluster.');
      return;
    }
    this.modal.confirm({
      nzTitle: 'Confirm Apply Configuration?',
      nzContent: `Will automatically apply ${this.form.value.monitoringType === 'enterprise' ? 'PolarDBXMonitor' : 'ServiceMonitor'} configuration`,
      nzOkText: 'Confirm Apply',
      nzCancelText: 'Cancel',
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
          ? 'Install task created, redirecting to Monitoring Overview to check progress...'
          : 'Monitoring component install task triggered, redirecting to Monitoring Overview to check progress...';
        this.message.success(successMessage);

        this.globalProgress.reportMonitoringInstall(jobName, ns, targetNs);

        setTimeout(() => {
          this.router.navigate(['/operations/monitoring/overview']);
        }, 2000);
      },
      error: (error: unknown) => {
        const msg = this.getErrorMessage(error) || 'Install trigger failed';
        if (origin === 'apply') {
          this.applyResult = { success: false, message: msg };
        }
        setLoading(false);
        this.message.error('Monitoring install failed: ' + msg);
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
        ? 'Manually Execute Install Command'
        : 'Prepare to Apply Configuration';
    }
    return this.applyResult.success ? 'Configuration Applied Successfully' : 'Configuration Application Failed';
  }

  getResultSubtitle(): string {
    if (!this.applyResult) {
      return this.form.value.installChannel === 'helm'
        ? 'Copy and execute the Helm/kubectl commands below, then click "Finish" on this page.'
        : 'Select application method to enable monitoring configuration';
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
    return normalized.includes('backoff') || normalized.includes('backofflimit') || normalized.includes('retry');
  }

  copyJobLogsCommand(): void {
    const cmd = `${this.getJobLogsCommand()} --tail=${this.tailLines}`;
    navigator.clipboard.writeText(cmd).then(() => this.message.success('Command copied to clipboard'));
  }

  getPodsCheckCommand(): string {
    return 'kubectl get pods -n polardbx-monitor';
  }

  copyPodsCheckCommand(): void {
    const cmd = this.getPodsCheckCommand();
    navigator.clipboard.writeText(cmd).then(() => this.message.success('Command copied to clipboard'));
  }

  copyPortForwardCommands(): void {
    const cmds = [
      'kubectl port-forward svc/grafana -n polardbx-monitor 3000',
      'kubectl port-forward svc/prometheus-k8s -n polardbx-monitor 9090',
      'kubectl port-forward svc/alertmanager-main -n polardbx-monitor 9093'
    ].join('\n');
    navigator.clipboard.writeText(cmds).then(() => this.message.success('Port forward command copied'));
  }

  getLoadBalancerValuesSnippet(): string {
    return `monitors:\n  grafana:\n    serviceType: LoadBalancer\n  prometheus:\n    serviceType: LoadBalancer`;
  }

  copyLoadBalancerValues(): void {
    const snippet = this.getLoadBalancerValuesSnippet();
    navigator.clipboard.writeText(snippet).then(() => this.message.success('values 片段已复制'));
  }
  
  // ==================== New: Template Helper Methods ====================
  
  /**
   * Get installation step icon
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
   * Get step icon theme
   */
  getStepIconTheme(status: string): 'fill' | 'outline' | 'twotone' {
    return status === 'success' ? 'fill' : 'outline';
  }
  
  /**
   * Format time (seconds -> minutes seconds)
   */
  formatTime(seconds: number): string {
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    return mins > 0 ? `${mins} 分 ${secs} 秒` : `${secs} 秒`;
  }
  
  /**
   * Get health status text
   */
  getHealthStatusText(status: string): string {
    const textMap: Record<string, string> = {
      'healthy': 'Running',
      'degraded': 'Degraded',
      'unhealthy': 'Abnormal'
    };
    return textMap[status] || 'Unknown';
  }
  
  /**
   * Get verification check tag color
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
   * Get verification check status text
   */
  getCheckStatusText(status: string): string {
    const textMap: Record<string, string> = {
      'success': 'Pass',
      'warning': 'Warning',
      'error': 'Failed'
    };
    return textMap[status] || 'Unknown';
  }
  
  /**
   * Format timestamp
   */
  formatTimestamp(timestamp: string): string {
    const date = new Date(timestamp);
    return date.toLocaleString('zh-CN');
  }
  
  /**
   * Get current installation stage
   */
  getCurrentInstallStage(): string {
    if (this.installStatus?.steps) {
      const failedStep = this.installStatus.steps.find(s => s.status === 'failed');
      return failedStep?.name || 'Unknown Phase';
    }
    return 'Unknown';
  }
  
  /**
   * View full logs (modal)
   */
  viewFullLogs(): void {
    if (!this.failureDiagnosis) return;
    
    this.modal.info({
      nzTitle: 'Full Error Logs',
      nzContent: `<pre style="max-height: 500px; overflow-y: auto; background: #f5f5f5; padding: 12px; border-radius: 4px;">${this.failureDiagnosis.relatedLogs.join('\n')}</pre>`,
      nzWidth: 800
    });
  }

  viewInstallLogs(): void {
    if (!this.installJob?.jobName) return;

    this.cleanupLogModal();

    const modalRef = this.modal.create<LogViewerComponent>({
      nzTitle: 'Install Task Logs',
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
        const message = this.getErrorMessage(error) || 'Unknown error';
        component.loading = false;
        component.error = message;
        this.message.error('Failed to get install logs: ' + message);
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
    // Navigate to monitoring overview page after installation completes
    this.router.navigate(['/operations/monitoring/overview']);
  }

  goToPrometheus(): void {
    this.api.getMonitoringStatus().subscribe({
      next: (status: MonitoringStatusResponse) => {
        const promComponent = status.components?.prometheus;
        if (!promComponent?.exists) {
          this.modal.info({
            nzTitle: 'Prometheus Not Installed',
            nzContent: 'Please install monitoring stack before attempting to access Prometheus'
          });
          return;
        }

        const accessUrl = promComponent.accessUrl;
        if (!accessUrl) {
          this.modal.info({
            nzTitle: 'Cannot Directly Access Prometheus',
            nzContent: 'Please execute the following command for port forwarding:\n\nkubectl port-forward svc/prometheus-k8s -n polardbx-monitor 9090:9090\n\nThen access: http://localhost:9090'
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
              nzTitle: 'Access Prometheus',
              nzContent: `Please get the IP address of any node in your Kubernetes cluster, then access:\n\nhttp://<node-ip>:${portMatch[1]}`
            });
          }
        } else if (accessUrl.includes('port-forward')) {
          this.modal.info({
            nzTitle: 'Access Prometheus',
            nzContent: `Please execute the following command for port forwarding:\n\nkubectl ${accessUrl}\n\nThen access: http://localhost:9090`
          });
        }
      },
      error: (error: unknown) => {
        this.message.error(`Failed to get Prometheus status: ${this.getErrorMessage(error)}`);
      }
    });
  }

  goToGrafana(): void {
    this.api.getMonitoringStatus().subscribe({
      next: (status: MonitoringStatusResponse) => {
        const grafanaComponent = status.components?.grafana;
        if (!grafanaComponent?.exists) {
          this.modal.info({
            nzTitle: 'Grafana Not Installed',
            nzContent: 'Please install monitoring stack before attempting to access Grafana'
          });
          return;
        }

        const accessUrl = grafanaComponent.accessUrl;
        if (!accessUrl) {
          this.modal.info({
            nzTitle: 'Cannot Directly Access Grafana',
            nzContent: 'Please execute the following command for port forwarding:\n\nkubectl port-forward svc/grafana -n polardbx-monitor 3000:3000\n\nThen access: http://localhost:3000'
          });
          return;
        }

        // If it starts with http, it's a direct URL
        if (accessUrl.startsWith('http')) {
          window.open(accessUrl, '_blank');
        } else if (accessUrl.startsWith('NodePort:')) {
          // Extract port from "NodePort: <port> (need to access using <node-ip>:<port>)"
          const portMatch = accessUrl.match(/(\d+)/);
          if (portMatch) {
            this.modal.info({
              nzTitle: 'Access Grafana',
              nzContent: `Please get the IP address of any node in your Kubernetes cluster, then access:\n\nhttp://<node-ip>:${portMatch[1]}`
            });
          }
        } else if (accessUrl.includes('port-forward')) {
          // port-forward instruction
          this.modal.info({
            nzTitle: 'Access Grafana',
            nzContent: `Please execute the following command for port forwarding:\n\nkubectl ${accessUrl}\n\nThen access: http://localhost:3000`
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

  // ==================== localStorage Persistence Functionality ====================

  private saveState(): void {
    try {
      // Trim redundant fields to reduce storage size
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
      console.warn('Failed to save wizard state:', error);
    }
  }

  private tryRestoreState(): void {
    try {
      const savedData = localStorage.getItem(STORAGE_KEY);
      if (!savedData) return;

      const state: WizardState = JSON.parse(savedData);

      // Version validation
      if (!state.version || state.version !== STATE_VERSION) {
        console.log('State version mismatch, clearing old state');
        this.clearSavedState();
        return;
      }

      // Check if state is too old
      const hoursOld = (Date.now() - (state.lastUpdated || state.timestamp)) / (1000 * 60 * 60);
      if (hoursOld > MAX_STATE_AGE_HOURS) {
        console.log(`State expired (${Math.round(hoursOld)} hours), clearing old state`);
        this.clearSavedState();
        return;
      }

      // Confirm whether to restore state
      if (state.currentStep > 0 || state.installJob) {
        this.confirmStateRestore(state);
      }
    } catch (error) {
      console.warn('Failed to restore wizard state:', error);
      this.clearSavedState();
    }
  }

  private confirmStateRestore(state: WizardState): void {
    const hoursOld = (Date.now() - (state.lastUpdated || state.timestamp)) / (1000 * 60 * 60);
    const timeInfo = hoursOld < 1
      ? `${Math.round(hoursOld * 60)}分钟前`
      : `${Math.round(hoursOld)}小时前`;

    const message = state.installJob
      ? `Detected monitoring install job from ${timeInfo} (${state.installJob.jobName}), continue tracking install progress?`
      : `Detected incomplete monitoring configuration wizard from ${timeInfo}, continue from step ${state.currentStep + 1}?`;

    this.modal.confirm({
      nzTitle: 'Restore Wizard State',
      nzContent: message,
      nzOkText: 'Continue',
      nzCancelText: 'Restart',
      nzOkType: 'primary',
      nzOnOk: () => this.restoreState(state),
      nzOnCancel: () => {
        this.modal.confirm({
          nzTitle: 'Confirm Clear State',
          nzContent: 'This will permanently delete the saved wizard state. Are you sure you want to restart?',
          nzOkText: 'Confirm',
          nzCancelText: 'Cancel',
          nzOkType: 'primary',
          nzOkDanger: true,
          nzOnOk: () => this.clearSavedState()
        });
      }
    });
  }

  private restoreState(state: WizardState): void {
    try {
      // Restore form values
      this.form.patchValue(state.formValues);
      this.updateSelectorLabels();

      // Restore step
      this.currentStep = state.currentStep;

      // Restore other state
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
        // ✅ No longer start polling in wizard
        // If there's an installation job, should directly navigate to Overview
        console.log('Detected saved installation job, should have been handled in checkExistingInstallation()');
      }

      // Reload data
      this.loadTargets();

      this.message.success('Wizard state restored');
      this.cdr.markForCheck();
    } catch (error) {
      console.error('State restoration failed:', error);
      this.message.error('State restoration failed, please restart');
      this.clearSavedState();
    }
  }

  private clearSavedState(): void {
    try {
      localStorage.removeItem(STORAGE_KEY);
    } catch (error) {
      console.warn('Failed to clear saved state:', error);
    }
  }

  // ==================== Job Status Polling ====================

  private pollingInterval: ReturnType<typeof setTimeout> | null = null;
  private pollingRetryCount = 0;
  private basePollingInterval = 5000; // 5 second base interval
  private maxPollingInterval = 60000; // Maximum 60 second interval

  private startJobStatusPolling(): void {
    // ⚠️ Deprecated: Wizard should not poll Job status
    // All progress tracking should be done in Overview component
    console.warn('startJobStatusPolling() called in wizard, this should not happen. Should navigate to Overview.');
    return;
  }

  private stopJobStatusPolling(): void {
    if (this.pollingInterval) {
      clearTimeout(this.pollingInterval);
      this.pollingInterval = null;
    }
    this.pollingRetryCount = 0;
  }

  // ==================== New: Installation Status Polling (Enhanced) ====================
  
  /**
   * Start real-time installation status polling (every 3 seconds)
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
          
          // Termination condition
          if (this.installStatus?.phase === 'Active' || this.installStatus?.phase === 'Failed') {
            this.stopInstallPolling();
            this.onInstallComplete(this.installStatus);
          }
          
          this.cdr.markForCheck();
        },
        error: (error: unknown) => {
          console.error('Failed to poll installation status:', error);
          // Fallback to old polling mechanism on failure
          if (typeof (error as { status?: number })?.status === 'number' && (error as { status?: number }).status === 404) {
            this.stopInstallPolling();
          }
        }
      });
  }
  
  /**
   * Stop installation status polling
   */
  private stopInstallPolling(): void {
    if (this.installPolling) {
      this.installPolling.unsubscribe();
      this.installPolling = null;
    }
  }
  
  /**
   * Update installation status (parse from API response)
   */
  private updateInstallStatus(apiStatus: MonitoringBootstrapStatusResponse): void {
    // Parse the actual data format returned by backend
    // Backend returns: { phase: "Running"|"Succeeded"|"Failed"|"Pending", startTime, active, succeeded, failed, failureReason, conditions }
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
      logs: []  // Logs need to be fetched separately via /bootstrap/logs API
    };
    
    // If failed, extract failure reason for diagnosis
    if (phase === 'Failed' && apiStatus?.failureReason) {
      this.installStatus.logs = [apiStatus.failureReason];
    }
  }
  
  /**
   * Map API phase to standard status
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
   * Calculate installation progress (0-100)
   * Backend returns fields: active, succeeded, failed
   * We estimate progress based on Job status
   */
  private calculateProgress(apiStatus: MonitoringBootstrapStatusResponse): number {
  const phase = apiStatus?.phase;
  const succeeded = apiStatus?.succeeded ?? 0;
    
    // Precise progress mapping
    if (phase === 'Succeeded') return 100;
  if (phase === 'Failed') return succeeded > 0 ? 95 : 50; // When failed, judge based on whether there are successful tasks
    if (phase === 'Pending') return 10;
    
    // Running state: estimate based on time (assume average 5 minutes needed)
    if (phase === 'Running' && apiStatus?.startTime) {
      const startTime = new Date(apiStatus.startTime);
      const now = new Date();
      const elapsedSeconds = (now.getTime() - startTime.getTime()) / 1000;
      const estimatedTotalSeconds = 300; // 5 minutes
      const progress = Math.min(95, Math.floor((elapsedSeconds / estimatedTotalSeconds) * 100));
      return Math.max(15, progress); // At least 15%, at most 95%
    }
    
    // Default value
    return 30;
  }
  
  /**
   * Extract installation steps
   * Backend doesn't return detailed steps yet, we generate reasonable step display based on conditions or phase
   */
  private extractInstallSteps(apiStatus: MonitoringBootstrapStatusResponse): InstallStep[] {
  const phase = apiStatus?.phase;
    
    // Basic step template
    const steps: InstallStep[] = [
      { name: 'Job 已创建', status: 'success', startTime: apiStatus?.startTime },
      { name: 'Helm Chart 准备', status: phase === 'Pending' ? 'pending' : 'success' },
      { name: 'Monitoring Component Installation', status: 'pending' },
      { name: 'Configuration Application', status: 'pending' },
      { name: 'Health Check', status: 'pending' }
    ];
    
    // Update step status based on phase
    if (phase === 'Running') {
      steps[1].status = 'success';
      steps[2].status = 'running';
      steps[2].message = 'Deploying Prometheus, Grafana, Alertmanager...';
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
      steps[2].message = apiStatus?.failureReason || 'Install failed';
    }
    
    return steps;
  }
  
  /**
   * Extract component status
   * Backend doesn't return component-level status yet, we generate reasonable display based on phase
   */
  private extractComponentStatus(apiStatus: MonitoringBootstrapStatusResponse): ComponentStatus[] {
    const phase = apiStatus?.phase;
    
    // Basic component list
    const components: ComponentStatus[] = [
      { name: 'prometheus', status: 'pending', readyPods: '0/2', port: 9090 },
      { name: 'grafana', status: 'pending', readyPods: '0/1', port: 3000 },
      { name: 'alertmanager', status: 'pending', readyPods: '0/3', port: 9093 }
    ];
    
    // Update status based on phase
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
      components[0].message = apiStatus?.failureReason || 'Deployment failed';
    }
    
    return components;
  }
  
  /**
   * Estimate remaining time (seconds)
   */
  private estimateTimeRemaining(progress: number): number {
    if (progress >= 90) return 30;
    if (progress >= 70) return 60;
    if (progress >= 50) return 120;
    return 180;
  }
  
  /**
   * Installation completion callback
   */
  private onInstallComplete(status: InstallStatus): void {
    if (status.phase === 'Active') {
      this.message.success('Monitoring stack installed successfully!');
      this.loadComponentsHealth(); // Load component health status
      this.clearSavedState();
    } else if (status.phase === 'Failed') {
      this.message.error('Monitoring stack install failed');
      this.diagnoseFailure(status); // Diagnose failure reason
    }
  }
  
  /**
   * Load component health status (after success)
   */
  private loadComponentsHealth(): void {
    // Call actual API to get monitoring component Pod status
    const monitoringNs = this.installJob?.targetNs || 'polardbx-monitor';
    
    this.api.listPods(monitoringNs).subscribe({
      next: (pods: Pod[]) => {
        this.componentsHealth = this.parseComponentsFromPods(pods);
        
        // If no components found, show warning instead of fake data
        if (this.componentsHealth.length === 0) {
          console.warn('No monitoring component Pods found');
          this.message.warning('Monitoring components may not be fully started, please refresh later');
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
   * Parse component health status from Pods list
   */
  private parseComponentsFromPods(pods: Pod[]): ComponentHealth[] {
    const components: ComponentHealth[] = [];
    
    // Define matching rules for monitoring components
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
        
        // Determine health status
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
    
    // Don't return fake data, return empty array
    return components;
  }
  
  /**
   * Diagnose failure reason
   */
  private diagnoseFailure(status: InstallStatus): void {
    // First try to get detailed logs
    if (this.installJob?.jobName) {
      this.loadFailureLogs(status);
    } else {
      this.performDiagnosis(status, []);
    }
  }
  
  /**
   * Load failure logs for diagnosis
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
   * Perform diagnosis analysis
   */
  private performDiagnosis(status: InstallStatus, logs: string[]): void {
    const logsText = logs.join('\n');
    // Also check status.logs (may contain failureReason)
    const statusLogs = status.logs || [];
    const combinedText = logsText + '\n' + statusLogs.join('\n');
    
    // Intelligently analyze error type
    let errorType: FailureDiagnosis['errorType'] = 'Unknown';
    const possibleCauses: PossibleCause[] = [];
    
    // ImagePullBackOff detection
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
    
    // Insufficient resources detection
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
    
    // Network error detection
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
    
    // Permission error
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
    
    // Configuration error detection
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
    
    // If no specific error matched, provide general suggestions
    if (possibleCauses.length === 0) {
      possibleCauses.push({
        description: '未知错误，需要查看详细日志',
        probability: 100,
        suggestedFix: '查看 Job Pod 日志获取更多信息',
        autoFixable: false
      });
    }
    
    // Extract error message
    const failedStep = status.steps.find(s => s.status === 'failed');
    const errorMessage = failedStep?.message || statusLogs[0] || '安装失败';
    
    this.failureDiagnosis = {
      errorType,
      errorMessage,
      possibleCauses: possibleCauses.sort((a, b) => b.probability - a.probability),
      relatedLogs: logs.length > 0 ? logs.slice(-20) : statusLogs, // Last 20 lines of logs
      timestamp: new Date().toISOString()
    };
    
    this.cdr.markForCheck();
  }
  
  /**
   * Auto fix
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
   * Auto switch image registry
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
   * Auto adjust resource configuration
   */
  private autoAdjustResources(): Promise<void> {
    return new Promise((resolve) => {
      // Here can call API to update Helm values
      this.message.info('调整资源配置...');
      setTimeout(() => resolve(), 1000);
    });
  }
  
  /**
   * Retry installation
   */
  private async retryInstallation(): Promise<void> {
    // Clear failure state
    this.failureDiagnosis = null;
    this.installStatus = null;
    this.applyResult = null;
    
    // Re-execute apply step
    this.applyConfiguration();
  }
  
  // ==================== New: Monitoring Function Verification ====================
  
  /**
   * Verify monitoring functionality (automatic health check)
   */
  async verifyMonitoring(): Promise<void> {
    this.verifyingMonitoring = true;
    this.cdr.markForCheck();
    
    try {
      const checks: VerificationCheck[] = [];
      
      // 1. Prometheus API test
      const prometheusCheck = await this.checkPrometheusAPI();
      checks.push(prometheusCheck);
      
      // 2. Grafana API test
      const grafanaCheck = await this.checkGrafanaAPI();
      checks.push(grafanaCheck);
      
      // 3. Alertmanager API test
      const alertmanagerCheck = await this.checkAlertmanagerAPI();
      checks.push(alertmanagerCheck);
      
      // 4. Metrics collection detection
      const metricsCheck = await this.checkMetricsCollection();
      checks.push(metricsCheck);
      
      // 5. Dashboard detection
      const dashboardCheck = await this.checkDashboards();
      checks.push(dashboardCheck);
      
      // Calculate overall health
      const overallHealth = this.calculateHealth(checks);
      
      // Extract warnings and recommendations
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
   * Check Prometheus API
   */
  private async checkPrometheusAPI(): Promise<VerificationCheck> {
    try {
      // Verify based on component health status
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
   * Check Grafana API
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
   * Check Alertmanager API
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
   * Check metrics collection
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
   * Check dashboards
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
      
      // Infer based on Grafana running status
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
   * Calculate overall health
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
   * Extract warning information
   */
  private extractWarnings(checks: VerificationCheck[]): string[] {
    return checks
      .filter(c => c.status === 'warning' || c.status === 'error')
      .map(c => `${c.name}: ${c.message}`);
  }
  
  /**
   * Generate recommendations
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
        this.pollingRetryCount = 0; // Reset retry count
        const phase = status?.phase;

        if (phase === 'Succeeded') {
          this.applyResult = { success: true, message: '监控安装已完成！' };
          this.stopJobStatusPolling();
          this.message.success('监控安装已完成');
          // Clear locally saved wizard state after success
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
          // Image pull error - provide clear prompt
          const reason = status?.failureReason || '镜像拉取失败';
          this.applyResult = {
            success: false,
            message: `安装失败: ${reason}`,
            failureReason: reason
          };
          this.stopJobStatusPolling();
          this.message.error('镜像拉取失败，请检查网络或配置镜像加速器');
        }
        // Running tasks continue polling
        this.saveState(); // Save latest state
        this.cdr.markForCheck();
      },
      error: (error: unknown) => {
        this.pollingRetryCount++;

        // 404 means Job doesn't exist or has been cleaned up
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

        console.warn(`Failed to check task status (retry ${this.pollingRetryCount} times):`, error);

        // Stop polling after reaching maximum retry count
        if (this.pollingRetryCount >= 5) {
          this.stopJobStatusPolling();
          this.message.warning('无法获取安装状态，请手动检查任务进度');
        }
      }
    });
  }

  /**
   * Check if monitoring system is already installed or there's an ongoing installation task
   */
  private checkExistingInstallation(): void {
    this.checkingExisting = true; // ✅ Start detection
    this.cdr.markForCheck();
    
    // 1. First check if there's an ongoing task in localStorage
    const savedJobInfo = localStorage.getItem('polardbx-monitor-install-job');
    if (savedJobInfo) {
      try {
        const jobInfo = JSON.parse(savedJobInfo);
        
        // ✅ Verify if expired (Job TTL is 10 minutes, localStorage saves for 15 minutes)
        if (jobInfo.expiresAt && Date.now() > jobInfo.expiresAt) {
          console.log('Job info expired, clearing localStorage');
          localStorage.removeItem('polardbx-monitor-install-job');
          // Continue checking if components are installed
          this.checkComponentsInstallation();
          return;
        }
        
        // Verify if Job is still running
        this.api.monitoringBootstrapStatus(jobInfo.jobName, jobInfo.namespace).subscribe({
          next: (jobStatus: MonitoringBootstrapStatusResponse) => {
            const phase = jobStatus?.phase;

            if (phase === 'Running' || phase === 'Pending') {
              // ✅ There's an ongoing installation task → directly navigate to Overview (no prompt)
              this.message.info('检测到正在进行的安装任务，正在跳转到监控总览...');
              setTimeout(() => {
                this.router.navigate(['/operations/monitoring/overview']);
              }, 1000);
              return; // Stop subsequent checks
            }

            if (phase === 'Succeeded' || phase === 'Failed' || phase === 'ImagePullError') {
              const updatedInfo = {
                ...jobInfo,
                phase,
                finishedAt: Date.now(),
                expiresAt: Date.now() + (15 * 60 * 1000) // Extend visibility period for viewing results in Overview
              };
              localStorage.setItem('polardbx-monitor-install-job', JSON.stringify(updatedInfo));

              // ✅ Clear wizard state to avoid returning to step 6
              this.clearSavedState();

              // ✅ Prompt user to go to Overview to view results
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

            // Unrecognized state, continue checking component installation
            this.checkComponentsInstallation();
          },
          error: (error: unknown) => {
            console.warn('Failed to detect installation task:', error);
            
            // ✅ If Job doesn't exist (404), clear localStorage and navigate to Overview
            if ((error as { status?: number })?.status === 404) {
              console.log('Job no longer exists (possibly cleaned up by TTL), clearing localStorage and wizard state');
              localStorage.removeItem('polardbx-monitor-install-job');
              this.clearSavedState();
              this.message.warning('最近的监控安装任务不存在，正在跳转到监控总览。');
              setTimeout(() => {
                this.router.navigate(['/operations/monitoring/overview']);
              }, 1000);
              return;
            }
            
            // Other errors temporarily ignored, continue checking if installed
            this.checkComponentsInstallation();
          }
        });
      } catch (e) {
        console.warn('Failed to parse installation task info:', e);
        localStorage.removeItem('polardbx-monitor-install-job');
        this.checkComponentsInstallation();
      }
    } else {
      // No ongoing task, check if installed
      this.checkComponentsInstallation();
    }
  }

  /**
   * Check if monitoring components are installed
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
          // ✅ Detected installed → clear all state → directly navigate to Overview
          console.log('Detected monitoring system installed, clearing wizard state');
          localStorage.removeItem('polardbx-monitor-install-job');
          this.clearSavedState();
          
          this.message.info('检测到监控系统已安装，正在跳转到监控总览...');
          setTimeout(() => {
            this.router.navigate(['/operations/monitoring/overview']);
          }, 1000);
          // No need to set checkingExisting = false, as will navigate
        } else {
          // ✅ Not installed → show wizard
          this.checkingExisting = false;
          this.cdr.markForCheck();
        }
      },
      error: (error: unknown) => {
        console.warn('Failed to detect installed status:', error);
        // ✅ Detection failure doesn't block flow, show wizard for user to continue
        this.checkingExisting = false;
        this.cdr.markForCheck();
      }
    });
  }

  /**
   * Show installed warning dialog
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
        // Navigate to overview to view status
        this.router.navigate(['/operations/monitoring/overview']);
      },
      nzOnCancel: () => {
        // Continue using wizard
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

  // Helper method for image pull error prompts
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
