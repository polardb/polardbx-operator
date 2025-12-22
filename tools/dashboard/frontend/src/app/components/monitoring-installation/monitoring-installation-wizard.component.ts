import { DatePipe, DecimalPipe, JsonPipe, KeyValuePipe, NgFor, NgIf, NgSwitch, NgSwitchCase, NgSwitchDefault, SlicePipe } from '@angular/common';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, DestroyRef, LOCALE_ID, OnInit, TemplateRef, ViewChild, inject } from '@angular/core';
import { Router } from '@angular/router';
import { FormBuilder, FormControl, ReactiveFormsModule, Validators } from '@angular/forms';
import { firstValueFrom } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { NzStepsModule } from 'ng-zorro-antd/steps';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzSkeletonModule } from 'ng-zorro-antd/skeleton';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzResultModule } from 'ng-zorro-antd/result';
import { NzStatisticModule } from 'ng-zorro-antd/statistic';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzProgressModule } from 'ng-zorro-antd/progress';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzModalModule, NzModalRef, NzModalService } from 'ng-zorro-antd/modal';
import { NzDrawerModule } from 'ng-zorro-antd/drawer';
import { NzTabsModule } from 'ng-zorro-antd/tabs';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzListModule } from 'ng-zorro-antd/list';
import { NzDescriptionsModule } from 'ng-zorro-antd/descriptions';
import { NzMessageModule, NzMessageService } from 'ng-zorro-antd/message';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { MonitoringInstallationApiService, EnvironmentSnapshot, CreatePlanRequest, CreatePlanResponse, InstallStatusResponse, StartInstallRequest, StartInstallResponse, DiagnoseResponse, AutoFixResponse, RetryStatus, RetryRequest } from '../../services/monitoring-installation-api.service';
import { MonitoringInstallationStateService, InstallPhase, InstallationState, CheckpointHistoryEntry } from '../../services/monitoring-installation-state.service';
import type { components } from '../../models/generated/monitoring-installation';
import { NamespaceService } from '../../services/namespace.service';
import { getWizardLocale, translateWizard } from './monitoring-installation-wizard.locale';

interface ComponentCard {
  name: string;
  displayName: string;
  exists?: boolean;
  healthy?: boolean;
  version?: string;
  action: components['schemas']['ComponentAction']['action'];
  actionReason?: string;
  status: 'healthy' | 'warning' | 'error' | 'missing';
  detail: string;
}

type InstallIntent = components['schemas']['CreatePlanRequest']['intent'];
type Checkpoint = components['schemas']['Checkpoint'];
type ComponentActionName = components['schemas']['ComponentAction']['action'];
type PlanActionName = components['schemas']['PlanStep']['action'];
type ExecutionActionName = components['schemas']['ExecutionStep']['action'];
type AnyActionName = ComponentActionName | PlanActionName | ExecutionActionName | 'skip';

@Component({
  selector: 'app-monitoring-installation-wizard',
  standalone: true,
  imports: [
    DatePipe,
    DecimalPipe,
  JsonPipe,
    KeyValuePipe,
    NgFor,
    NgIf,
    NgSwitch,
    NgSwitchCase,
  NgSwitchDefault,
    SlicePipe,
    ReactiveFormsModule,
    NzStepsModule,
    NzFormModule,
    NzSelectModule,
    NzInputModule,
    NzButtonModule,
    NzAlertModule,
    NzSkeletonModule,
    NzGridModule,
    NzCardModule,
    NzTagModule,
    NzResultModule,
    NzStatisticModule,
    NzSpinModule,
    NzProgressModule,
    NzTableModule,
    NzModalModule,
    NzDrawerModule,
    NzTabsModule,
    NzDividerModule,
    NzListModule,
    NzDescriptionsModule,
    NzMessageModule,
    NzIconModule
  ],
  templateUrl: './monitoring-installation-wizard.component.html',
  styleUrl: './monitoring-installation-wizard.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class MonitoringInstallationWizardComponent implements OnInit {
  private readonly api = inject(MonitoringInstallationApiService);
  private readonly state = inject(MonitoringInstallationStateService);
  private readonly namespaceService = inject(NamespaceService);
  private readonly destroyRef = inject(DestroyRef);
  private readonly cdr = inject(ChangeDetectorRef);
  private readonly modal = inject(NzModalService);
  private readonly messages = inject(NzMessageService);
  private readonly fb = inject(FormBuilder);
  private readonly router = inject(Router);
  private readonly localeId = inject<string>(LOCALE_ID);
  private readonly wizardLocale = getWizardLocale(this.localeId);
  @ViewChild('resumeDialogTpl', { static: true }) resumeDialogTpl?: TemplateRef<void>;
  private resumeModal?: NzModalRef;
  private resumeDialogOpened = false;
  private resumeBypassOnce = false;
  resumeSessionId?: string;
  resumeHistory: CheckpointHistoryEntry[] = [];
  private pendingCheckpoint: Checkpoint | null = null;
  private pollTimer: number | null = null;
  resumeInitialPhaseLabel?: string;

  t(key: string, params?: Record<string, string | number>): string {
    return translateWizard(this.wizardLocale, key, params);
  }

  planIntent: InstallIntent = 'install';
  private readonly componentDisplayNames: Record<string, string> = {
    prometheus: 'Prometheus',
    grafana: 'Grafana',
    alertmanager: 'Alertmanager',
    'node-exporter': 'Node Exporter',
    'kube-state-metrics': 'Kube State Metrics',
    thanos: 'Thanos',
    'blackbox-exporter': 'Blackbox Exporter'
  };

  readonly steps = [
    { title: this.t('steps.detect.title'), description: this.t('steps.detect.description') },
    { title: this.t('steps.plan.title'), description: this.t('steps.plan.description') },
    { title: this.t('steps.progress.title'), description: this.t('steps.progress.description') }
  ];

  currentStep = 0;
  namespaceOptions: string[] = ['polardbx-monitor'];
  readonly detectForm = this.fb.group({
    namespace: new FormControl('polardbx-monitor', { nonNullable: true, validators: [Validators.required] }),
    installMode: new FormControl<'managed' | 'assisted' | 'byo'>('managed', { nonNullable: true, validators: [Validators.required] }),
    valuesYaml: new FormControl('', { nonNullable: true })
  });

  detectionLoading = false;
  detectionError?: string;
  detectionSnapshot: EnvironmentSnapshot | null = null;
  detectionComponents: ComponentCard[] = [];
  detectionRecommendations: string[] = [];
  lastDetectedAt?: string;

  planLoading = false;
  planResponse: CreatePlanResponse | null = null;
  planWarnings: string[] = [];

  installStatus: InstallStatusResponse | null = null;
  installPolling = false;
  installLoading = false;
  retryActionLoading = false;
  retryCountdownSeconds: number | null = null;

  diagnosticsVisible = false;
  diagnosticsLoading = false;
  diagnosticsError?: string;
  diagnosisResult: DiagnoseResponse | null = null;
  selectedErrorIndex = 0;
  autoFixStates: Record<string, { status: 'idle' | 'running' | 'success' | 'error'; message?: string }> = {};
  private retryCountdownTimer: number | null = null;

  ngOnInit(): void {
    this.namespaceService.namespaces$
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe(list => {
        const merged = new Set<string>(['polardbx-monitor']);
        (list || []).forEach(ns => merged.add(ns));
        this.namespaceOptions = Array.from(merged).sort();
        this.cdr.markForCheck();
      });

    this.detectEnvironment();
    this.destroyRef.onDestroy(() => this.clearStatusPolling());
  }

  async detectEnvironment(): Promise<void> {
    if (this.detectionLoading) {
      return;
    }
    this.resumeInitialPhaseLabel = undefined;
    const namespace = (this.detectForm.value.namespace ?? 'polardbx-monitor').trim() || 'polardbx-monitor';
    this.detectionLoading = true;
    this.detectionError = undefined;
    this.clearStatusPolling();
    this.cdr.markForCheck();

    try {
      await this.state.hydrate({ namespace });
      const currentState = this.state.getSnapshot();
      if (this.maybeOpenResumeDialog(currentState)) {
        this.detectionLoading = false;
        this.cdr.markForCheck();
        return;
      }
      if (this.planIntent !== 'repair') {
        this.planIntent = 'install';
      }
      this.state.updatePhase('Detecting');
      const snapshot = await firstValueFrom(this.api.detectEnvironment(namespace));
      this.detectionSnapshot = snapshot;
      this.lastDetectedAt = snapshot.detectedAt;
      this.detectionComponents = this.normalizeComponents(snapshot);
      this.detectionRecommendations = snapshot.recommendations ?? [];
      this.planResponse = null;
      this.planWarnings = [];
      this.state.updatePhase('Planning');
      this.state.updateProgress(0);
      this.currentStep = 0;
      this.messages.success(this.t('toasts.detectSuccess'));
    } catch (error) {
      console.error('[monitoring-installation] detect failed', error);
      this.detectionError = this.extractErrorMessage(error);
      this.state.updatePhase('NotStarted');
      this.messages.error(this.t('toasts.detectFailure'));
    } finally {
      this.detectionLoading = false;
      this.cdr.markForCheck();
    }
  }

  private maybeOpenResumeDialog(state: InstallationState): boolean {
    if (this.resumeBypassOnce) {
      this.resumeBypassOnce = false;
      this.pendingCheckpoint = null;
      this.resumeSessionId = undefined;
      this.resumeHistory = [];
      return false;
    }

    const checkpoint = state.checkpoint ?? null;
    const sessionId = state.sessionId ?? checkpoint?.sessionId ?? undefined;

    if (!checkpoint && !sessionId) {
      this.pendingCheckpoint = null;
      this.resumeSessionId = undefined;
      this.resumeDialogOpened = false;
      this.resumeHistory = [];
      return false;
    }

    this.pendingCheckpoint = checkpoint;
    this.resumeSessionId = sessionId;
    this.resumeHistory = this.state.getCheckpointHistory();

    if (this.resumeDialogOpened) {
      return true;
    }

    if (!this.resumeDialogTpl) {
      return false;
    }

    this.resumeDialogOpened = true;
    this.resumeModal = this.modal.create({
      nzTitle: this.t('resume.title'),
      nzContent: this.resumeDialogTpl,
      nzFooter: null,
      nzMaskClosable: false,
      nzClosable: false,
      nzCentered: true,
      nzWidth: 520,
      nzClassName: 'monitoring-resume-modal'
    });
    this.cdr.markForCheck();
    return true;
  }

  private closeResumeModal(): void {
    if (this.resumeModal) {
      this.resumeModal.close();
      this.resumeModal = undefined;
    }
    this.resumeDialogOpened = false;
    this.resumeHistory = [];
  }

  async onResumeContinue(): Promise<void> {
    this.closeResumeModal();
    if (!this.resumeSessionId) {
      this.messages.warning(this.t('toasts.resumeMissing'));
      await this.state.clearCheckpoint();
      this.pendingCheckpoint = null;
      this.resumeSessionId = undefined;
      this.resumeDialogOpened = false;
      this.resumeBypassOnce = true;
      await this.detectEnvironment();
      return;
    }
    await this.resumeExistingSession(this.resumeSessionId);
  }

  async onResumeRestart(): Promise<void> {
    this.closeResumeModal();
    await this.state.clearCheckpoint();
    this.pendingCheckpoint = null;
    this.resumeSessionId = undefined;
    this.planIntent = 'install';
    this.resumeDialogOpened = false;
    this.resumeBypassOnce = true;
    this.messages.success(this.t('toasts.resumeCleared'));
    await this.detectEnvironment();
  }

  async onResumeRepair(): Promise<void> {
    this.closeResumeModal();
    await this.state.clearCheckpoint();
    this.pendingCheckpoint = null;
    this.resumeSessionId = undefined;
    this.planIntent = 'repair';
    this.resumeDialogOpened = false;
    this.resumeBypassOnce = true;
    this.messages.info(this.t('toasts.resumeRepairMode'));
    await this.detectEnvironment();
  }

  private async resumeExistingSession(sessionId: string): Promise<void> {
    try {
      const status = await firstValueFrom(this.api.getInstallStatus(sessionId));
      await this.handleStatusUpdate(status);
      this.currentStep = 2;
      this.installPolling = true;
      this.startStatusPolling(status.sessionId);
      this.resumeInitialPhaseLabel = this.getInstallPhaseLabel(status.phase);
      this.resumeSessionId = status.sessionId;
      this.messages.success(this.t('toasts.installResumed'));
    } catch (error) {
      console.error('[monitoring-installation] resume session failed', error);
      this.messages.warning(this.t('toasts.resumeFailed'));
      await this.state.clearCheckpoint();
      this.pendingCheckpoint = null;
      this.resumeSessionId = undefined;
      this.resumeDialogOpened = false;
      this.resumeBypassOnce = true;
      await this.detectEnvironment();
    }
    this.cdr.markForCheck();
  }

  async generatePlan(): Promise<void> {
    if (this.planLoading) {
      return;
    }
    if (!this.detectionSnapshot) {
      this.messages.warning(this.t('toasts.detectRequired'));
      return;
    }

    this.planLoading = true;
    this.planResponse = null;
    this.planWarnings = [];
    this.clearStatusPolling();
    this.installStatus = null;
    this.installPolling = false;
    this.cdr.markForCheck();

    const request: CreatePlanRequest = {
      intent: this.planIntent ?? 'install',
      detected: this.detectionSnapshot,
      config: {
        targetNamespace: this.detectionSnapshot.namespace,
        installMode: this.detectForm.value.installMode ?? 'managed',
        valuesYaml: (this.detectForm.value.valuesYaml ?? '').trim() || undefined,
        autoRepair: true
      }
    };

    try {
      const response = await firstValueFrom(this.api.createPlan(request));
      this.planResponse = response;
      this.planWarnings = response.warnings ?? [];
      this.currentStep = 1;
      this.state.updatePhase('Planning');
      this.messages.success(this.t('toasts.planGenerated'));
    } catch (error) {
      console.error('[monitoring-installation] create plan failed', error);
      this.messages.error(this.t('toasts.planFailed'));
    } finally {
      this.planLoading = false;
      this.cdr.markForCheck();
    }
  }

  private normalizeComponents(snapshot: EnvironmentSnapshot): ComponentCard[] {
    return (snapshot.components ?? []).map<ComponentCard>(component => {
      const exists = component.exists ?? false;
      const healthy = component.healthy ?? false;
      const action = component.actionRecommendation?.action ?? 'install';
      const reason = component.actionRecommendation?.reason;
      const detailSegments: string[] = [];
      const details = component.details ?? {};
      Object.entries(details).forEach(([key, value]) => {
        if (value) {
          detailSegments.push(`${key}: ${value}`);
        }
      });
      if (!details['version'] && component.version) {
        detailSegments.push(`version: ${component.version}`);
      }

      let status: ComponentCard['status'];
      if (!exists) {
        status = 'missing';
      } else if (healthy) {
        status = 'healthy';
      } else if (action === 'repair' || action === 'upgrade') {
        status = 'warning';
      } else {
        status = 'error';
      }

      if (reason) {
        detailSegments.push(reason);
      }

      return {
        name: component.name,
        displayName: this.getComponentDisplayName(component.name),
        exists,
        healthy,
        version: component.version,
        action,
        actionReason: reason,
        status,
        detail: detailSegments.join(' · ')
      };
    });
  }

  getComponentDisplayName(name: components['schemas']['ComponentName'] | string): string {
    return this.componentDisplayNames[name as string] ?? String(name);
  }

  getActionTagColor(action: AnyActionName): string {
    switch (action) {
      case 'install':
        return 'processing';
      case 'upgrade':
        return 'warning';
      case 'repair':
        return 'magenta';
      case 'reinstall':
        return 'volcano';
      case 'uninstall':
        return 'volcano';
      case 'verify':
        return 'blue';
      case 'cleanup':
        return 'geekblue';
      case 'skip':
      default:
        return 'default';
    }
  }

  getActionLabel(action: AnyActionName): string {
    switch (action) {
      case 'install':
        return this.t('actions.install');
      case 'upgrade':
        return this.t('actions.upgrade');
      case 'repair':
        return this.t('actions.repair');
      case 'reinstall':
        return this.t('actions.reinstall');
      case 'uninstall':
        return this.t('actions.uninstall');
      case 'verify':
        return this.t('actions.verify');
      case 'cleanup':
        return this.t('actions.cleanup');
      case 'skip':
      default:
        return this.t('actions.skip');
    }
  }

  getRiskTagColor(level?: components['schemas']['InstallationPlan']['riskLevel'] | null): string {
    switch (level) {
      case 'low':
        return 'success';
      case 'medium':
        return 'warning';
      case 'high':
        return 'error';
      default:
        return 'default';
    }
  }

  getRiskLabel(level?: components['schemas']['InstallationPlan']['riskLevel'] | null): string {
    switch (level) {
      case 'low':
        return this.t('plan.risk.low');
      case 'medium':
        return this.t('plan.risk.medium');
      case 'high':
        return this.t('plan.risk.high');
      default:
        return this.t('common.unknown');
    }
  }

  formatDuration(seconds?: number | null): string {
    if (!seconds || seconds <= 0) {
      return this.t('common.none');
    }
    if (seconds < 90) {
      return this.t('duration.seconds', { n: Math.max(1, Math.round(seconds)) });
    }
    const minutes = Math.round(seconds / 60);
    if (minutes < 90) {
      return this.t('duration.minutes', { n: minutes });
    }
    const hours = minutes / 60;
    return this.t('duration.hours', { n: hours.toFixed(1) });
  }

  formatDependencies(dep?: number[] | null): string {
    if (!dep || dep.length === 0) {
      return this.t('common.none');
    }
    return dep.sort((a, b) => a - b).join(' → ');
  }

  async startInstallation(): Promise<void> {
    if (this.installLoading) {
      return;
    }
    if (!this.planResponse) {
      this.messages.warning(this.t('toasts.planRequired'));
      return;
    }

    this.resumeInitialPhaseLabel = undefined;
    this.installLoading = true;
    this.cdr.markForCheck();

    const plan = this.planResponse.plan;
    const cfg: any = {
      targetNamespace: plan.sessionTemplate.namespace,
      installMode: this.detectForm.value.installMode ?? 'managed',
      autoRepair: true
    };
    const values = (this.detectForm.value.valuesYaml ?? '').trim();
    if (values) cfg.valuesYaml = values;

    const request: StartInstallRequest = { plan };

    const snapshot = this.state.getSnapshot();
    const checkpoint = this.pendingCheckpoint ?? snapshot.checkpoint ?? undefined;
    if (checkpoint) {
      request.checkpoint = checkpoint;
      request.checkpoint.config = checkpoint.config ?? cfg;
    } else {
      request.checkpoint = { config: cfg } as any;
    }

    try {
      const response: StartInstallResponse = await firstValueFrom(this.api.startInstallation(request));
      const createdAt = response.createdAt ?? new Date().toISOString();
      const initialStatus: InstallStatusResponse = {
        sessionId: response.sessionId,
        phase: 'Installing',
        progress: 0,
        components: [],
        updatedAt: createdAt,
        startedAt: createdAt,
        errors: []
      };
      this.installStatus = initialStatus;
      this.pendingCheckpoint = null;
      this.state.clearErrors();
      this.state.updatePhase('Installing');
      this.state.updateProgress(0);
      this.state.updateFromStatus(initialStatus);
      this.currentStep = 2;
  this.resumeSessionId = response.sessionId;
      const resumed = response.resumed === true;
  this.messages.success(this.t(resumed ? 'toasts.installResumed' : 'toasts.installStarted'));
      this.installPolling = true;
      this.startStatusPolling(response.sessionId);
    } catch (error) {
      console.error('[monitoring-installation] start installation failed', error);
  this.messages.error(this.t('toasts.installStartFailed'));
    } finally {
      this.installLoading = false;
      this.cdr.markForCheck();
    }
  }

  private clearStatusPolling(): void {
    if (this.pollTimer !== null) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }
    this.installPolling = false;
    this.clearRetryCountdown();
  }

  private startStatusPolling(sessionId: string): void {
    if (this.pollTimer !== null) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }
    this.installPolling = true;
    const poll = async (): Promise<void> => {
      if (!this.installPolling) {
        return;
      }
      try {
        const status = await firstValueFrom(this.api.getInstallStatus(sessionId));
        await this.handleStatusUpdate(status);
        if (!this.isTerminalStatus(status) && this.installPolling) {
          const delay = this.computeNextPollDelay(status);
          this.pollTimer = window.setTimeout(() => {
            void poll();
          }, delay);
        }
      } catch (error) {
        console.error('[monitoring-installation] poll status failed', error);
  this.messages.warning(this.t('toasts.refreshFailurePaused'));
        this.installPolling = false;
        this.cdr.markForCheck();
      }
    };

    void poll();
  }

  private async handleStatusUpdate(status: InstallStatusResponse): Promise<void> {
    this.installStatus = status;
    this.pendingCheckpoint = status.checkpoint ?? null;
    this.resumeSessionId = status.sessionId;
    if (this.resumeInitialPhaseLabel !== undefined) {
      this.resumeInitialPhaseLabel = this.getInstallPhaseLabel(status.phase);
    }
    this.state.updateFromStatus(status);
    this.updateRetryCountdown(status);
    if (status.checkpoint) {
      try {
        await this.state.persistCheckpoint(status.checkpoint);
      } catch (error) {
        console.error('[monitoring-installation] persist checkpoint failed', error);
      }
    }

    const errors = status.errors ?? [];
    if (!errors.length) {
      this.diagnosticsVisible = false;
      this.selectedErrorIndex = 0;
      this.diagnosisResult = null;
      this.diagnosticsError = undefined;
      this.autoFixStates = {};
    } else if (this.selectedErrorIndex >= errors.length) {
      this.selectedErrorIndex = 0;
    }
    if (this.diagnosticsVisible && !this.diagnosticsLoading) {
      void this.loadDiagnostics(this.selectedErrorIndex);
    }

    if (this.isTerminalStatus(status)) {
      this.clearStatusPolling();
      if (status.phase === 'Active') {
        this.messages.success(this.t('toasts.installSuccess'));
      } else if (status.phase === 'Failed') {
        this.messages.error(this.t('toasts.installFailure'));
      } else if (status.phase === 'Degraded') {
        this.messages.warning(this.t('toasts.installDegraded'));
      }
    }

    this.cdr.markForCheck();
  }

  private isTerminalStatus(status: InstallStatusResponse): boolean {
    if (this.hasActiveRetry(status)) {
      return false;
    }
    const phase = status.phase;
    return phase === 'Active' || phase === 'Failed' || phase === 'Degraded';
  }

  private hasActiveRetry(status: InstallStatusResponse): boolean {
    const mode = status.retry?.mode ?? null;
    return mode === 'scheduled' || mode === 'running';
  }

  private computeNextPollDelay(status: InstallStatusResponse): number {
    const minDelay = 3000;
    const maxDelay = 15000;
    const retry = status.retry;
    if (retry?.mode === 'scheduled') {
      if (retry.nextRetryAt) {
        const target = Date.parse(retry.nextRetryAt);
        if (!Number.isNaN(target)) {
          const diff = target - Date.now();
          if (diff > 0) {
            return Math.min(Math.max(diff, minDelay), maxDelay);
          }
        }
      }
      if (retry.backoffSeconds && retry.backoffSeconds > 0) {
        const derived = retry.backoffSeconds * 1000;
        return Math.min(Math.max(Math.floor(derived / 2), minDelay), maxDelay);
      }
      return maxDelay;
    }
    if (retry?.mode === 'running') {
      return minDelay;
    }
    if (retry?.mode === 'exhausted') {
      return 10000;
    }
    return minDelay;
  }

  private updateRetryCountdown(status: InstallStatusResponse): void {
    const retry = status.retry;
    if (!retry || retry.mode !== 'scheduled' || !retry.nextRetryAt) {
      this.retryCountdownSeconds = null;
      this.clearRetryCountdown();
      return;
    }

    const target = Date.parse(retry.nextRetryAt);
    if (Number.isNaN(target)) {
      this.retryCountdownSeconds = null;
      this.clearRetryCountdown();
      return;
    }

    this.clearRetryCountdown();
    const tick = (): void => {
      const diffMs = target - Date.now();
      if (diffMs <= 0) {
        this.retryCountdownSeconds = 0;
        this.clearRetryCountdown();
        this.cdr.markForCheck();
        return;
      }
      this.retryCountdownSeconds = Math.ceil(diffMs / 1000);
      this.retryCountdownTimer = window.setTimeout(() => tick(), 1000);
      this.cdr.markForCheck();
    };

    tick();
  }

  private clearRetryCountdown(): void {
    if (this.retryCountdownTimer !== null) {
      clearTimeout(this.retryCountdownTimer);
      this.retryCountdownTimer = null;
    }
    if (this.retryCountdownSeconds !== null) {
      this.retryCountdownSeconds = null;
    }
  }

  private async requestRetry(options: { mode?: RetryRequest['mode']; force?: boolean; reason?: string } = {}): Promise<void> {
    const sessionId = this.installStatus?.sessionId;
    if (!sessionId) {
      this.messages.warning(this.t('toasts.retryUnavailable'));
      return;
    }

    const payload: RetryRequest = {};
    if (options.mode) {
      payload.mode = options.mode;
    }
    if (options.force === true) {
      payload.force = true;
    }
    if (options.reason) {
      payload.reason = options.reason;
    }

    this.retryActionLoading = true;
    this.cdr.markForCheck();

    try {
      const response = await firstValueFrom(this.api.triggerRetry(sessionId, Object.keys(payload).length ? payload : undefined));
      await this.handleStatusUpdate(response);
      this.startStatusPolling(sessionId);
      this.messages.success(this.t(options.force ? 'toasts.retryForced' : 'toasts.retryTriggered'));
    } catch (error) {
      console.error('[monitoring-installation] trigger retry failed', error);
      this.messages.error(this.t('toasts.retryFailure', { error: this.extractErrorMessage(error) }));
    } finally {
      this.retryActionLoading = false;
      this.cdr.markForCheck();
    }
  }

  getProgressPercent(progress?: number | null): number {
    if (progress == null) {
      return 0;
    }
    const percent = Math.round(progress * 100);
    return Math.max(0, Math.min(100, percent));
  }

  getProgressStatus(phase?: InstallPhase | null): 'active' | 'success' | 'exception' {
    if (phase === 'Active') {
      return 'success';
    }
    if (phase === 'Failed') {
      return 'exception';
    }
    return 'active';
  }

  getInstallPhaseLabel(phase?: InstallPhase | null): string {
    switch (phase) {
      case 'Detecting':
        return this.t('phases.detecting');
      case 'Planning':
        return this.t('phases.planning');
      case 'Installing':
        return this.t('phases.installing');
      case 'Verifying':
        return this.t('phases.verifying');
      case 'Active':
        return this.t('phases.active');
      case 'Failed':
        return this.t('phases.failed');
      case 'Degraded':
        return this.t('phases.degraded');
      case 'Upgrading':
        return this.t('phases.upgrading');
      case 'Uninstalling':
        return this.t('phases.uninstalling');
      default:
        return this.t('phases.pending');
    }
  }

  getInstallPhaseTagColor(phase?: InstallPhase | null): string {
    switch (phase) {
      case 'Active':
        return 'green';
      case 'Failed':
        return 'red';
      case 'Degraded':
        return 'orange';
      case 'Installing':
      case 'Verifying':
        return 'blue';
      default:
        return 'default';
    }
  }

  getComponentPhaseLabel(phase?: components['schemas']['ComponentPhase'] | null): string {
    switch (phase) {
      case 'NotInstalled':
        return this.t('componentPhase.notInstalled');
      case 'Planned':
        return this.t('componentPhase.planned');
      case 'Installing':
        return this.t('componentPhase.installing');
      case 'VerifyingHealth':
        return this.t('componentPhase.verifying');
      case 'Healthy':
        return this.t('componentPhase.healthy');
      case 'Unhealthy':
        return this.t('componentPhase.unhealthy');
      case 'Failed':
        return this.t('componentPhase.failed');
      case 'UpgradeAvailable':
        return this.t('componentPhase.upgradeAvailable');
      default:
        return this.t('componentPhase.unknown');
    }
  }

  getComponentPhaseColor(phase?: components['schemas']['ComponentPhase'] | null): string {
    switch (phase) {
      case 'Healthy':
        return 'success';
      case 'Installing':
      case 'VerifyingHealth':
        return 'processing';
      case 'Unhealthy':
      case 'Failed':
        return 'error';
      case 'UpgradeAvailable':
        return 'warning';
      default:
        return 'default';
    }
  }

  getExecutionStatusLabel(status?: components['schemas']['ExecutionStep']['status'] | null): string {
    switch (status) {
      case 'pending':
        return this.t('executionStatus.pending');
      case 'running':
        return this.t('executionStatus.running');
      case 'succeeded':
        return this.t('executionStatus.succeeded');
      case 'failed':
        return this.t('executionStatus.failed');
      case 'skipped':
        return this.t('executionStatus.skipped');
      default:
        return this.t('executionStatus.unknown');
    }
  }

  async refreshStatus(): Promise<void> {
    if (!this.installStatus?.sessionId) {
      this.messages.warning(this.t('messages.noActiveSession'));
      return;
    }
    try {
      const status = await firstValueFrom(this.api.getInstallStatus(this.installStatus.sessionId));
      await this.handleStatusUpdate(status);
      this.messages.success(this.t('toasts.refreshSuccess'));
    } catch (error) {
      console.error('[monitoring-installation] refresh status failed', error);
      this.messages.error(this.t('toasts.refreshFailure'));
    }
  }

  backgroundRun(): void {
    this.messages.info(this.t('messages.backgroundRun'));
    this.currentStep = 0;
  }

  getStatusTagColor(status: ComponentCard['status']): string {
    switch (status) {
      case 'healthy':
        return 'success';
      case 'warning':
        return 'warning';
      case 'missing':
        return 'default';
      case 'error':
      default:
        return 'error';
    }
  }

  getStatusLabel(status: ComponentCard['status']): string {
    switch (status) {
      case 'healthy':
        return this.t('componentStatus.healthy');
      case 'warning':
        return this.t('componentStatus.warning');
      case 'missing':
        return this.t('componentStatus.missing');
      case 'error':
      default:
        return this.t('componentStatus.error');
    }
  }

  getRetryModeTagColor(mode?: RetryStatus['mode'] | null): string {
    switch (mode) {
      case 'scheduled':
        return 'blue';
      case 'running':
        return 'gold';
      case 'exhausted':
        return 'red';
      default:
        return 'default';
    }
  }

  getRetryModeLabel(mode?: RetryStatus['mode'] | null): string {
    switch (mode) {
      case 'scheduled':
        return this.t('retry.mode.scheduled');
      case 'running':
        return this.t('retry.mode.running');
      case 'exhausted':
        return this.t('retry.mode.exhausted');
      default:
        return this.t('retry.mode.idle');
    }
  }

  getRetryProgressPercent(retry: RetryStatus | null): number {
    if (!retry || !retry.maxRetries || retry.maxRetries <= 0) {
      return 0;
    }
    const attempts = retry.retries ?? 0;
    const percent = (attempts / retry.maxRetries) * 100;
    return Math.max(0, Math.min(100, percent));
  }

  formatCountdown(seconds: number | null): string {
    if (seconds == null) {
      return this.t('common.none');
    }
    const clamped = Math.max(0, seconds);
    const minutes = Math.floor(clamped / 60);
    const remaining = clamped % 60;
    if (minutes > 0) {
      const formattedSeconds = remaining.toString().padStart(2, '0');
      return this.t('countdown.format', { minutes, seconds: formattedSeconds });
    }
    return this.t('countdown.seconds', { seconds: clamped });
  }

  formatBackoff(seconds?: number | null): string {
    if (seconds == null || seconds < 0) {
      return this.t('common.none');
    }
    const clamped = Math.floor(seconds);
    if (clamped >= 60) {
      const minutes = Math.floor(clamped / 60);
      const remaining = clamped % 60;
      if (remaining > 0) {
        return this.t('countdown.format', { minutes, seconds: remaining.toString().padStart(2, '0') });
      }
      return this.t('duration.minutes', { n: minutes });
    }
    return this.t('duration.seconds', { n: clamped });
  }

  canManualRetry(retry: RetryStatus | null): boolean {
    if (!this.installStatus?.sessionId) {
      return false;
    }
    if (!retry) {
      return true;
    }
    return retry.mode !== 'running';
  }

  showForceRetry(retry: RetryStatus | null): boolean {
    if (!retry) {
      return false;
    }
    const attempts = retry.retries ?? 0;
    const max = retry.maxRetries ?? 0;
    return retry.mode === 'exhausted' || (max > 0 && attempts >= max);
  }

  async triggerManualRetry(force = false): Promise<void> {
    await this.requestRetry({ mode: 'manual', force });
  }

  get selectedInstallError(): components['schemas']['InstallError'] | null {
    const errors = this.installStatus?.errors ?? [];
    if (!errors.length) {
      return null;
    }
    const index = Math.min(Math.max(this.selectedErrorIndex, 0), errors.length - 1);
    return errors[index];
  }

  openDiagnostics(index = 0): void {
    const previousIndex = this.selectedErrorIndex;
    this.selectedErrorIndex = index;
    if (previousIndex !== index) {
      this.autoFixStates = {};
    }
    this.diagnosticsVisible = true;
    void this.loadDiagnostics(index);
  }

  closeDiagnostics(): void {
    this.diagnosticsVisible = false;
  }

  selectDiagnosticError(index: number): void {
    if (index === this.selectedErrorIndex) {
      return;
    }
    this.selectedErrorIndex = index;
    this.autoFixStates = {};
    void this.loadDiagnostics(index);
  }

  getAutoFixState(fixId: string): { status: 'idle' | 'running' | 'success' | 'error'; message?: string } {
    return this.autoFixStates[fixId] ?? { status: 'idle' };
  }

  getDiagnosisSeverityColor(severity: components['schemas']['Diagnosis']['severity']): string {
    switch (severity) {
      case 'critical':
        return 'red';
      case 'major':
        return 'orange';
      case 'minor':
      default:
        return 'blue';
    }
  }

  getDiagnosisSeverityLabel(severity: components['schemas']['Diagnosis']['severity']): string {
    switch (severity) {
      case 'critical':
        return this.t('diagnostics.severity.critical');
      case 'major':
        return this.t('diagnostics.severity.major');
      case 'minor':
      default:
        return this.t('diagnostics.severity.minor');
    }
  }

  async triggerAutoFix(fixId: string): Promise<void> {
    const sessionId = this.installStatus?.sessionId;
    if (!sessionId) {
      this.messages.warning(this.t('toasts.autofixNoSession'));
      return;
    }
    this.autoFixStates = {
      ...this.autoFixStates,
      [fixId]: { status: 'running' }
    };
    this.cdr.markForCheck();

    const context = this.buildDiagnosticContext(this.selectedInstallError);

    try {
      const response: AutoFixResponse = await firstValueFrom(this.api.applyAutoFix({
        fixId,
        sessionId,
        context: Object.keys(context).length ? context : undefined
      }));
      const status: 'success' | 'error' = response.success ? 'success' : 'error';
      this.autoFixStates = {
        ...this.autoFixStates,
        [fixId]: { status, message: response.message ?? undefined }
      };
      if (response.success) {
        this.messages.success(response.message ?? this.t('toasts.autofixSuccess'));
        await this.reloadStatusSilently(sessionId);
        if (this.diagnosticsVisible && !this.diagnosticsLoading) {
          await this.loadDiagnostics(this.selectedErrorIndex);
        }
      } else {
        this.messages.warning(response.message ?? this.t('toasts.autofixWarning'));
      }
    } catch (error) {
      console.error('[monitoring-installation] auto-fix failed', error);
      this.autoFixStates = {
        ...this.autoFixStates,
        [fixId]: { status: 'error', message: this.extractErrorMessage(error) }
      };
      this.messages.error(this.t('toasts.autofixFailure'));
    } finally {
      this.cdr.markForCheck();
    }
  }

  private async loadDiagnostics(index: number): Promise<void> {
    const errors = this.installStatus?.errors ?? [];
    if (!errors.length) {
      this.diagnosisResult = null;
      this.diagnosticsError = this.t('diagnostics.noneAvailable');
      this.diagnosticsLoading = false;
      this.autoFixStates = {};
      this.cdr.markForCheck();
      return;
    }

    const clampedIndex = Math.min(Math.max(index, 0), errors.length - 1);
    this.selectedErrorIndex = clampedIndex;

    const targetError = this.cloneInstallError(errors[clampedIndex]);
    this.diagnosticsLoading = true;
    this.diagnosticsError = undefined;
    this.diagnosisResult = null;
    this.cdr.markForCheck();

    const context = this.buildDiagnosticContext(targetError);

    try {
      const response: DiagnoseResponse = await firstValueFrom(this.api.diagnoseFailure({
        error: targetError,
        context: Object.keys(context).length ? context : undefined
      }));
      this.diagnosisResult = response;
    } catch (error) {
      console.error('[monitoring-installation] diagnose failure', error);
      this.diagnosticsError = this.extractErrorMessage(error);
    } finally {
      this.diagnosticsLoading = false;
      this.cdr.markForCheck();
    }
  }

  private async reloadStatusSilently(sessionId: string): Promise<void> {
    try {
      const status = await firstValueFrom(this.api.getInstallStatus(sessionId));
      await this.handleStatusUpdate(status);
    } catch (error) {
      console.error('[monitoring-installation] silent status refresh failed', error);
    }
  }

  private cloneInstallError(error: components['schemas']['InstallError']): components['schemas']['InstallError'] {
    return {
      category: error.category,
      message: error.message,
      component: error.component,
      stepOrder: error.stepOrder,
      occurredAt: error.occurredAt,
      context: error.context ? { ...error.context } : undefined
    };
  }

  private buildDiagnosticContext(error: components['schemas']['InstallError'] | null): Record<string, string> {
    const context: Record<string, string> = {};
    const namespace = this.detectionSnapshot?.namespace ?? (this.detectForm.value.namespace ?? '').trim();
    if (namespace) {
      context['namespace'] = namespace;
    }
    if (this.installStatus?.sessionId) {
      context['sessionId'] = this.installStatus.sessionId;
    }
    if (error?.component) {
      context['component'] = String(error.component);
    }
    if (error?.stepOrder != null) {
      context['stepOrder'] = String(error.stepOrder);
    }
    if (error?.context) {
      Object.entries(error.context).forEach(([key, value]) => {
        if (value != null) {
          context[key] = String(value);
        }
      });
    }
    return context;
  }

  private extractErrorMessage(error: unknown): string {
    if (!error) {
      return this.t('errors.detectionUnavailable');
    }
    if (typeof error === 'string') {
      return error;
    }
    if ('message' in (error as Record<string, unknown>)) {
      const message = String((error as Record<string, unknown>)['message'] ?? this.t('errors.detectionFailed'));
      return message;
    }
    return this.t('errors.detectionUnavailable');
  }

  protected readonly InstallPhaseDetecting: InstallPhase = 'Detecting';
}
