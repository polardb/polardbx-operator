import { AfterViewInit, Component, ElementRef, NgZone, OnDestroy, OnInit, TemplateRef, ViewChild, inject } from '@angular/core';
import { CommonModule, DatePipe } from '@angular/common';
import { FormsModule, ReactiveFormsModule, FormBuilder, FormGroup, Validators } from '@angular/forms';
import { Router, ActivatedRoute, RouterModule } from '@angular/router';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzIconModule, NZ_ICONS } from 'ng-zorro-antd/icon';
import { IconDefinition } from '@ant-design/icons-angular';
import {
  DashboardOutline,
  SafetyOutline,
  SafetyCertificateOutline,
  ClusterOutline,
  SettingOutline,
  ReloadOutline,
  DatabaseOutline,
  ControlOutline,
  DesktopOutline,
  SyncOutline,
  RocketOutline,
  CopyOutline,
  InfoCircleOutline,
  EyeOutline,
  FileTextOutline,
  CodeOutline,
  DownloadOutline,
  RedoOutline,
  DeleteOutline,
  ArrowRightOutline,
  TagOutline,
  ClockCircleOutline,
  BellOutline,
  CheckOutline
} from '@ant-design/icons-angular/icons';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzModalModule, NzModalService } from 'ng-zorro-antd/modal';
import { NzTabsModule } from 'ng-zorro-antd/tabs';
import { NzCheckboxModule } from 'ng-zorro-antd/checkbox';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzPageHeaderModule } from 'ng-zorro-antd/page-header';
import { NzSpaceModule } from 'ng-zorro-antd/space';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzStatisticModule } from 'ng-zorro-antd/statistic';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzCollapseModule } from 'ng-zorro-antd/collapse';
import { NzStepsModule } from 'ng-zorro-antd/steps';
import { NzProgressModule } from 'ng-zorro-antd/progress';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzBadgeModule } from 'ng-zorro-antd/badge';
import { NzInputNumberModule } from 'ng-zorro-antd/input-number';
import { ClusterChangeWizardComponent } from '../../components/cluster-change-wizard/cluster-change-wizard.component';
import { PrechangeCheckComponent } from '../../components/prechange-check/prechange-check.component';
import { Chart, registerables, ChartConfiguration, ChartType } from 'chart.js';
import * as dagre from 'dagre';
import { Subject, of, timer } from 'rxjs';
import { catchError, debounceTime, distinctUntilChanged, map, switchMap, takeUntil } from 'rxjs/operators';

import { ApiService } from '../../services/api.service';
import { LoadingService, LoadingKeys } from '../../services/loading.service';
import { PolarDBXCluster } from '../../models/cluster.model';
import { Pod } from '../../models/pod.model';
import { PolarDBXBackup, BackupInfo, CreateBackupRequest } from '../../models/backup.model';
import { ClusterResourceUsage } from '../../models/cluster-resource-usage.model';

Chart.register(...registerables);

interface NodeInfo {
  name: string;
  type: string;
  status: string;
  cpu: string;
  memory: string;
  ip: string;
  resources?: string; // Displayed resource summary
  readyContainers?: number; // Number of ready containers
  totalContainers?: number; // Total container count
  totalRestarts?: number; // Total restart count
  hasUnhealthyContainers?: boolean; // Whether any container is unhealthy
  containerStatuses?: ContainerStatus[]; // Container status details
}

interface ContainerStatus {
  name: string;
  ready: boolean;
  restartCount: number;
  state?: string; // Running, Waiting, Terminated
  reason?: string; // Reason
  message?: string; // Details
}



@Component({
  selector: 'app-cluster-detail',
  templateUrl: './cluster-detail.component.html',
  styleUrls: ['./cluster-detail.component.scss'],
  standalone: true,
  imports: [
    CommonModule,
    DatePipe,
    FormsModule,
    ReactiveFormsModule,
    RouterModule,
    NzButtonModule,
    NzCardModule,
    NzIconModule,
    NzSelectModule,
    NzTabsModule,
    NzTableModule,
    NzInputModule,
    NzTagModule,
    NzToolTipModule,
    NzFormModule,
    NzPageHeaderModule,
    NzSpaceModule,
    NzDividerModule,
    NzGridModule,
    NzStatisticModule,
    NzSpinModule,
    NzCollapseModule,
    NzStepsModule,
    NzProgressModule,
    NzAlertModule,
    NzBadgeModule,
    NzCheckboxModule,
    NzInputNumberModule,
    NzModalModule,
    ClusterChangeWizardComponent,
    PrechangeCheckComponent
  ],
  providers: [
    {
      provide: NZ_ICONS,
      useValue: [
        DashboardOutline,
        SafetyOutline,
        SafetyCertificateOutline,
        ClusterOutline,
        SettingOutline,
        ReloadOutline,
        DatabaseOutline,
        ControlOutline,
        DesktopOutline,
        SyncOutline,
        RocketOutline,
        CopyOutline,
        InfoCircleOutline,
        EyeOutline,
        FileTextOutline,
        CodeOutline,
        DownloadOutline,
        RedoOutline,
        DeleteOutline,
        ArrowRightOutline,
        TagOutline,
        ClockCircleOutline,
        BellOutline,
        CheckOutline
      ]
    }
  ]
})
export class ClusterDetailComponent implements OnInit, AfterViewInit, OnDestroy {
  readonly LoadingKeys = LoadingKeys;
  private route = inject(ActivatedRoute);
  public router = inject(Router);
  private zone = inject(NgZone);
  private fb = inject(FormBuilder);
  private messageService = inject(NzMessageService);
  private modalService = inject(NzModalService);
  private apiService = inject(ApiService);
  public loadingService = inject(LoadingService);
  private destroy$ = new Subject<void>();

  @ViewChild('cpuChart') cpuChartRef!: ElementRef<HTMLCanvasElement>;
  @ViewChild('memChart') memChartRef!: ElementRef<HTMLCanvasElement>;
  @ViewChild('topologySvg') topologySvgRef!: ElementRef<SVGSVGElement>;
  @ViewChild('backupDownloadInfoTpl') backupDownloadInfoTpl?: TemplateRef<any>;

  // Cluster basics
  clusterName = '';
  clusterNamespace = '';
  clusterStatus = 'Running';
  clusterVersion = '5.4.18';

  // Topology
  cnReplicas = 2;
  dnReplicas = 2;
  gmsReplicas = 1;
  cdcReplicas = 0;

  // Tab index
  selectedTabIndex = 0;

  // Node table data
  nodeDataSource: NodeInfo[] = [];
  nodeColumns: string[] = ['name', 'type', 'status', 'ready', 'restarts', 'resources', 'ip'];
  // Add action column
  constructorColumns() {
    if (!this.nodeColumns.includes('actions')) this.nodeColumns.push('actions');
  }

  // Node table page size (supports "view all")
  nodesPageSize = 10;
  openNodesManagement(role?: 'CN'|'DN'|'GMS'|'CDC'): void {
    const ns = this.clusterNamespace || 'default';
    const cluster = this.clusterName || '';
    this.router.navigate(['/operations', 'nodes'], {
      queryParams: {
        namespace: ns,
        keyword: cluster,
        role: role || undefined
      }
    });
  }

  // Backup data
  backupDataSource: BackupInfo[] = [];
  backupColumns: string[] = ['id', 'completedTime', 'type', 'status', 'actions'];
  backupDownloadInfoText = '';

  // Forms
  configForm: FormGroup;
  upgradeForm: FormGroup;

  // Logs
  selectedLogNode = 'all';
  logContent = '请选择一个节点和容器，然后点击“查看日志”来加载日志。';
  isLoadingLogs = false;

  // Chart instances
  private cpuChart?: Chart;
  private memChart?: Chart;

  // Resource usage (metrics-server)
  resourceUsageAvailable = true;
  resourceUsageSource = '';
  resourceUsageMessage = '';
  resourceUsageLastUpdated = '';
  private readonly resourceUsageMaxPoints = 30;
  private readonly resourceUsagePollIntervalMs = 60_000;
  private cpuSeries: Array<number | null> = [];
  private memSeries: Array<number | null> = [];
  private timeSeries: string[] = [];

  cluster: PolarDBXCluster | null = null;
  cnCount = 0;
  dnCount = 0;
  gmsCount = 0;
  cdcCount = 0;
  allPods: Pod[] = [];
  selectedPod = '';
  selectedContainer = '';
  selectedPodContainers: { name: string }[] = [];
  rowContainer: Record<string, string> = {};
  alerts = { critical: 0, warning: 0, info: 0, total: 0, source: 'none' };
  // Explicitly declare keys to avoid TS4111 caused by index signatures.
  roleSummary: {
    cn: { total: number; ready: number; pods: Pod[] };
    dn: { total: number; ready: number; pods: Pod[] };
    gms: { total: number; ready: number; pods: Pod[] };
    cdc: { total: number; ready: number; pods: Pod[] };
  } = {
    cn: { total: 0, ready: 0, pods: [] },
    dn: { total: 0, ready: 0, pods: [] },
    gms: { total: 0, ready: 0, pods: [] },
    cdc: { total: 0, ready: 0, pods: [] }
  };

  @ViewChild('resizeHandle', { static: false }) resizeHandle!: ElementRef;
  @ViewChild('leftPanel', { static: false }) leftPanel!: ElementRef;

  // Precheck status
  precheck: {
    loading: boolean;
    pass: boolean;
    plan: Array<{ id: string; state: 'ok' | 'warn' | 'error'; message: string }>;
    checks: any;
    generatedAt: string;
    hasWarn: boolean;
    hasError: boolean;
  } = {
    loading: false,
    pass: true,
    plan: [],
    checks: {},
    generatedAt: '',
    hasWarn: false,
    hasError: false
  };
  warnAck = false;

  constructor() {
    // Initialize forms
    this.configForm = this.fb.group({
      cnReplicas: [this.cnReplicas, [Validators.required, Validators.min(1)]],
      dnReplicas: [this.dnReplicas, [Validators.required, Validators.min(1)]],
      cdcReplicas: [this.cdcReplicas, [Validators.min(0)]]
    });

    this.upgradeForm = this.fb.group({
      targetVersion: ['5.4.18', Validators.required]
    });
  }

  ngOnInit(): void {
    // Record last known version (used for candidate fallback).
    try { localStorage.setItem('lastClusterVersion', this.clusterVersion || ''); } catch {}
    // Force-clear any leftover loading states.
    this.loadingService.clearAll();
    this.constructorColumns();
    
    // Clear again after a short delay to ensure state is fully reset.
    setTimeout(() => {
      this.loadingService.clearAll();
    }, 100);
    
    // Load cluster identity from route params (compatible with legacy queryParam namespace).
    this.route.paramMap.pipe(takeUntil(this.destroy$)).subscribe(params => {
      this.clusterName = params.get('name') || this.clusterName || 'demo-cluster';
      this.clusterNamespace = params.get('namespace') || this.route.snapshot.queryParamMap.get('namespace') || this.clusterNamespace || 'default';
      this.loadClusterData();

      // Fetch upgrade candidates; quick-upgrade defaults to the recommended one.
      if (this.clusterName && this.clusterNamespace) {
        this.apiService.getClusterUpgradePlan(this.clusterNamespace, this.clusterName).subscribe({
          next: (plan: any) => {
            const cands: Array<{ version: string; recommended?: boolean }> = Array.isArray(plan?.candidates) ? plan.candidates : [];
            if (cands.length > 0) {
              const recommended = cands.find(c => c.recommended) || cands[0];
              const v = recommended?.version || cands[0]?.version;
              if (v) this.upgradeForm.patchValue({ targetVersion: v }, { emitEvent: false });
            }
          },
          error: () => {}
        });
      }
    });

    // Trigger precheck on form changes (throttled).
    this.configForm.valueChanges.pipe(debounceTime(400), takeUntil(this.destroy$)).subscribe(() => this.runPrecheck());
    this.upgradeForm.valueChanges.pipe(debounceTime(400), takeUntil(this.destroy$)).subscribe(() => this.runPrecheck());

    // Poll resource usage (CPU% of requests; Memory% of limits) for engine/server containers.
    this.route.paramMap.pipe(
      takeUntil(this.destroy$),
      map(params => ({
        name: params.get('name') || this.clusterName || 'demo-cluster',
        namespace: params.get('namespace') || this.route.snapshot.queryParamMap.get('namespace') || this.clusterNamespace || 'default'
      })),
      distinctUntilChanged((a, b) => a.name === b.name && a.namespace === b.namespace),
      switchMap(({ namespace, name }) => {
        this.resetResourceUsageSeries();
        return timer(0, this.resourceUsagePollIntervalMs).pipe(
          switchMap(() =>
            this.apiService.getClusterResourceUsage(namespace, name, true).pipe(
              catchError((err) => {
                this.resourceUsageAvailable = false;
                this.resourceUsageSource = '';
                this.resourceUsageMessage = err?.error?.message || '无法获取指标（可能未安装 metrics-server 或权限不足）';
                this.resourceUsageLastUpdated = '';
                return of(null as any);
              })
            )
          )
        );
      })
    ).subscribe((usage: ClusterResourceUsage | null) => {
      if (!usage) return;
      this.applyResourceUsage(usage);
    });
  }

  ngAfterViewInit(): void {
    // Delay chart initialization to ensure the DOM is rendered.
    setTimeout(() => {
      this.initCharts();
    }, 100);
  }

  private toInt(val: any, fallback = 0): number {
    if (typeof val === 'number' && Number.isFinite(val)) return val;
    if (typeof val === 'string') {
      const n = parseInt(val, 10);
      return Number.isFinite(n) ? n : fallback;
    }
    return fallback;
  }

  private refreshReplicasFromCluster(cluster: any): void {
    const cn = this.toInt(cluster?.spec?.topology?.nodes?.cn?.replicas, this.cnReplicas);
    const dn = this.toInt(cluster?.spec?.topology?.nodes?.dn?.replicas, this.dnReplicas);
    const cdcFromSpec = cluster?.spec?.topology?.nodes?.cdc?.replicas;
    const cdcFromStatus = cluster?.status?.replicaStatus?.cdc?.total;
    const cdc = this.toInt(cdcFromSpec, this.toInt(cdcFromStatus, 0));
    const gmsFromStatus = this.toInt(cluster?.status?.replicaStatus?.gms?.total, this.gmsReplicas);

    this.cnReplicas = cn;
    this.dnReplicas = dn;
    this.cdcReplicas = cdc;
    this.gmsReplicas = gmsFromStatus || this.gmsReplicas;

    this.configForm.patchValue(
      { cnReplicas: this.cnReplicas, dnReplicas: this.dnReplicas, cdcReplicas: this.cdcReplicas },
      { emitEvent: false }
    );
  }

  private refreshVersionFromCluster(cluster: any): void {
    const v = (cluster?.spec?.topology?.version || cluster?.status?.version || cluster?.status?.polardbxVersion || cluster?.spec?.version || '').toString();
    if (v) this.clusterVersion = v;
  }

  private loadClusterData(): void {
    // Set a timeout to clear loading state in case the API hangs.
    setTimeout(() => {
      this.loadingService.clearAll();
    }, 10000); // Force clear after 10 seconds.
    
    this.apiService.getCluster(this.clusterNamespace, this.clusterName).subscribe({
      next: (cluster) => {
        this.cluster = cluster;
        this.clusterStatus = (cluster as any)?.status?.phase || this.clusterStatus;
        this.refreshReplicasFromCluster(cluster as any);
        this.refreshVersionFromCluster(cluster as any);
      },
      error: (err) => {
        this.messageService.error(`加载集群详情失败：${err.message}`);
      }
    });
    
    this.apiService.getPodsForCluster(this.clusterNamespace, this.clusterName).subscribe({
      next: (pods) => {
        this.cnCount = 0;
        this.dnCount = 0;
        this.gmsCount = 0;
        this.cdcCount = 0;
        const nodes: NodeInfo[] = pods.map(pod => {
          const nodeInfo = this.transformPodToNodeInfo(pod);
          switch (nodeInfo.type) {
            case 'CN':
              this.cnCount++;
              break;
            case 'DN':
              this.dnCount++;
              break;
            case 'GMS':
              this.gmsCount++;
              break;
            case 'CDC':
              this.cdcCount++;
              break;
          }
          return nodeInfo;
        });
        this.nodeDataSource = nodes;
        this.allPods = pods;
        // Compute topology summary:
        // - CN: count by Pod (replicas)
        // - DN/GMS: count by XStore (group by label xstore/name), because each XStore may have cand/log pods.
        // "Available" means: engine container is Running (avoids "always 0" when sidecars are unready).
        this.roleSummary = { cn: { total: 0, ready: 0, pods: [] }, dn: { total: 0, ready: 0, pods: [] }, gms: { total: 0, ready: 0, pods: [] }, cdc: { total: 0, ready: 0, pods: [] } };
        type RoleKey = 'cn' | 'dn' | 'gms' | 'cdc';
        const roles: RoleKey[] = ['cn', 'dn', 'gms', 'cdc'];

        const desiredTotals: Partial<Record<RoleKey, number>> = {
          cn: this.toInt((this.cluster as any)?.status?.replicaStatus?.cn?.total, 0) || this.cnReplicas || 0,
          dn: this.toInt((this.cluster as any)?.status?.replicaStatus?.dn?.total, 0) || this.dnReplicas || 0,
          gms: this.toInt((this.cluster as any)?.status?.replicaStatus?.gms?.total, 0) || this.gmsReplicas || 0,
          cdc: this.toInt((this.cluster as any)?.status?.replicaStatus?.cdc?.total, 0) || 0
        };

        const isEngineRunning = (pp: Pod): boolean => {
          const css = pp.status?.containerStatuses || [];
          return css.some((cs: any) => cs?.name === 'engine' && !!cs?.state?.running);
        };

        const groupKeyForRole = (r: RoleKey, pp: Pod): string => {
          if (r === 'dn' || r === 'gms') {
            return pp.metadata?.labels?.['xstore/name'] || pp.metadata?.name || '';
          }
          // CN/CDC: per pod
          return pp.metadata?.name || '';
        };

        const groups: Record<RoleKey, Map<string, { engineRunning: boolean }>> = {
          cn: new Map(),
          dn: new Map(),
          gms: new Map(),
          cdc: new Map()
        };

        for (const p of pods) {
          const role = this.detectPodRole(p).role.toLowerCase();
          if (roles.includes(role as RoleKey)) {
            const r = role as RoleKey;
            this.roleSummary[r].pods.push(p);
            const key = groupKeyForRole(r, p);
            if (!key) continue;
            if (!groups[r].has(key)) groups[r].set(key, { engineRunning: false });
            if (isEngineRunning(p)) groups[r].get(key)!.engineRunning = true;
          }
        }

        for (const r of roles) {
          const totalGroups = groups[r].size;
          const readyGroups = Array.from(groups[r].values()).filter(v => v.engineRunning).length;
          const desired = desiredTotals[r] || 0;
          // Prefer desired replica total when available; otherwise fall back to observed groups.
          const total = desired > 0 ? desired : totalGroups;
          const ready = Math.min(readyGroups, total);
          this.roleSummary[r].total = total;
          this.roleSummary[r].ready = ready;
        }
        // Re-render topology to ensure DN → CDC edges and counts are updated.
        this.renderTopology();
        if (this.allPods.length > 0) {
          this.selectedPod = this.allPods[0].metadata.name;
          this.onPodSelectionChange();
        }
        // Run precheck once after loading.
        this.runPrecheck();
      },
      error: (err) => {
        this.messageService.error(`加载 Pod 列表失败：${err.message}`);
      }
    });

    this.loadBackupData();
    this.loadLogData();

    // Load alert summary (can override endpoint via localStorage.alertmanager).
    const am = localStorage.getItem('alertmanager') || '';
    this.apiService.getClusterAlertsSummary(this.clusterNamespace, this.clusterName, am || undefined).subscribe({
      next: (s) => this.alerts = s,
      error: () => this.alerts = { critical: 0, warning: 0, info: 0, total: 0, source: 'none' }
    });
  }

  // Precheck: show before execution and on config changes.
  runPrecheck(): void {
    if (!this.clusterName || !this.clusterNamespace) return;
    this.precheck.loading = true;
    this.warnAck = false;
    this.loadingService.wrapObservable(LoadingKeys.PRECHECK_RUN, this.apiService.runPrecheck(this.clusterNamespace, this.clusterName, 'config')).subscribe({
      next: (res: any) => {
        this.precheck.plan = Array.isArray(res?.plan) ? res.plan : [];
        this.precheck.checks = res?.checks || {};
        this.precheck.generatedAt = res?.generatedAt || '';
        this.precheck.hasWarn = this.precheck.plan.some(p => p.state === 'warn');
        this.precheck.hasError = this.precheck.plan.some(p => p.state === 'error');
        this.precheck.pass = !this.precheck.hasError;
        this.precheck.loading = false;
      },
      error: () => {
        this.precheck.loading = false;
        this.precheck.hasError = true;
        this.precheck.pass = false;
      }
    });
  }

  getPlanIcon(state: 'ok'|'warn'|'error'): string {
    switch (state) {
      case 'ok': return 'check_circle';
      case 'warn': return 'warning';
      default: return 'error';
    }
  }

  getPlanColor(state: 'ok'|'warn'|'error'): 'primary'|'accent'|'warn' {
    switch (state) {
      case 'ok': return 'primary';
      case 'warn': return 'accent';
      default: return 'warn';
    }
  }

  canApplyConfig(): boolean {
    return this.configForm.valid && this.hasConfigChanges() && !this.precheck.hasError && (!this.precheck.hasWarn || this.warnAck);
  }

  canStartUpgrade(): boolean {
    return this.upgradeForm.valid && !this.precheck.hasError && (!this.precheck.hasWarn || this.warnAck);
  }

  getPrecheckStatusChip(): { text: string; color: 'primary'|'accent'|'warn' } {
    if (this.precheck.loading) return { text: '检查中', color: 'accent' };
    if (this.precheck.hasError) return { text: '未通过', color: 'warn' };
    if (this.precheck.hasWarn) return { text: '存在警告', color: 'accent' };
    return { text: '检查通过', color: 'primary' };
  }

  copyPrecheckDetails(): void {
    try {
      const payload = {
        generatedAt: this.precheck.generatedAt,
        plan: this.precheck.plan,
        checks: this.precheck.checks
      };
      const text = JSON.stringify(payload, null, 2);
      navigator.clipboard.writeText(text).then(() => {
        this.messageService.success('预检详情已复制到剪贴板');
      }).catch(() => {
        // Fallback
        const ta = document.createElement('textarea');
        ta.value = text;
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
        this.messageService.success('预检详情已复制');
      });
    } catch {
      this.messageService.error('复制失败');
    }
  }

  // Quick actions
  goCreateBackupQuick(): void {
    this.selectedTabIndex = 3; // Backups & restore tab
  }
  goHpfsConfigQuick(): void {
    this.router.navigate(['/operations', 'logs', 'dashboard']);
  }

  getUptime(): string {
    const start = (this.cluster as any)?.status?.startTime || this.cluster?.metadata?.creationTimestamp;
    if (!start) return '未知';
    const startTime = new Date(start).getTime();
    const now = Date.now();
    let diff = Math.max(0, now - startTime);
    const days = Math.floor(diff / (24 * 3600 * 1000)); diff -= days * 24 * 3600 * 1000;
    const hours = Math.floor(diff / (3600 * 1000)); diff -= hours * 3600 * 1000;
    const mins = Math.floor(diff / (60 * 1000));
    if (days > 0) return `${days}天${hours}小时`;
    if (hours > 0) return `${hours}小时${mins}分钟`;
    return `${mins}分钟`;
  }

  copyConnectionInfo(): void {
    const addr = `svc.${this.clusterName}:3306`;
    const text = [
      `服务地址: ${addr}`,
      `用户名: root`,
      `协议: MySQL 5.7 兼容`
    ].join('\n');
    navigator.clipboard.writeText(text).then(() => {
      this.messageService.success('已复制连接信息');
    }).catch(() => {
      this.messageService.error('复制失败，请手动复制');
    });
  }

  goToBackupsTab(): void {
    // Switch to the "Backups & restore" tab (index may be 3 depending on template layout).
    this.selectedTabIndex = 3;
  }

  getAlertCount(): number {
    return this.alerts.total || 0;
  }

  getAvailabilityPercent(): number {
    // Estimated availability: ready pods / total pods.
    const total = this.allPods?.length || 0;
    if (total === 0) return 0;
    const ready = this.allPods.filter(p => (p.status?.phase || '').toLowerCase() === 'running').length;
    return Math.round((ready / total) * 100);
  }

  getAvailabilityColor(): 'primary' | 'accent' | 'warn' {
    const v = this.getAvailabilityPercent();
    if (v > 99) return 'primary';
    if (v >= 95) return 'accent';
    return 'warn';
  }

  copyExecCommand(node: NodeInfo): void {
    const pod = node.name;
    const container = (this.selectedPodContainers?.[0]?.name) || '';
    const ns = this.clusterNamespace || 'default';
    const command = `kubectl exec -it ${pod} -n ${ns} ${container ? '-c ' + container + ' ' : ''}-- /bin/bash`;
    navigator.clipboard.writeText(command).then(() => {
      this.messageService.success('已复制 kubectl exec 命令');
    }).catch(() => {
      this.messageService.error('复制失败，请手动复制');
    });
  }

  openExecDialog(node: NodeInfo): void {
    import('../../components/exec-command-dialog/exec-command-dialog.component').then(m => {
      const containers = this.allPods.find(p => p.metadata.name === node.name)?.spec?.containers?.map(c => c.name) || [];
      const ref = this.modalService.create({
        nzTitle: '执行命令',
        nzContent: m.ExecCommandDialogComponent,
        nzWidth: '700px',
        nzData: {
          namespace: this.clusterNamespace || 'default',
          pod: node.name,
          containers,
          defaultContainer: containers[0] || ''
        }
      });
      ref.afterClose.subscribe((result: any) => {
        if (result && result.runOnce) {
          this.messageService.success('命令已执行');
        }
      });
    });
  }

  openWebShell(node: NodeInfo): void {
    import('../../components/webshell-dialog/webshell-dialog.component').then(m => {
      const containers = this.getContainersOf(node);
      const prefer = this.getPreferredContainers(containers)[0] || '';
      const chosen = this.rowContainer[node.name] || prefer;
      this.modalService.create({
        nzTitle: 'Web Shell',
        nzContent: m.WebShellDialogComponent,
        nzWidth: '900px',
        nzStyle: { height: '600px' },
        nzData: {
          namespace: this.clusterNamespace || 'default',
          pod: node.name,
          container: chosen,
          containers: containers
        }
      });
    });
  }

  openPodDetail(node: NodeInfo, tab?: 'overview'|'logs'|'json'|'kubectl'|'terminal'): void {
    const podName = node.name;
    const ns = this.clusterNamespace || 'default';
    this.router.navigate(['/operations','nodes', ns, podName], {
      queryParams: tab ? { tab } : undefined
    });
  }

  getContainersOf(node: NodeInfo): string[] {
    return this.allPods.find(p => p.metadata.name === node.name)?.spec?.containers?.map(c => c.name) || [];
  }
  getPreferredContainers(containers: string[]): string[] {
    const items = (containers || []).slice();
    const lower = (s: string) => (s||'').toLowerCase();
    const negatives = ['prober','probe','exporter','agent','sidecar','pause','proxy','reloader','metrics','prom','istio','linkerd','kube-rbac-proxy','configmap-reload','reloader'];
    const positives = ['engine','mysql','xstore','server','main','app','dn','cn','gms','cdc'];
    const preferred = items.filter(c => positives.some(p => lower(c) === p || lower(c).includes(p)));
    const others = items.filter(c => !preferred.includes(c) && !negatives.some(n => lower(c).includes(n)));
    const sidecars = items.filter(c => negatives.some(n => lower(c).includes(n)));
    const dedup = (arr: string[]) => Array.from(new Set(arr));
    return dedup([...preferred, ...others, ...sidecars]);
  }
  setRowContainer(podName: string, container: string): void {
    this.rowContainer[podName] = container;
  }
  openWebShellWith(node: NodeInfo, container: string): void {
    this.rowContainer[node.name] = container;
    this.openWebShell(node);
  }

  private getBackupPreferences(): { storageName: string; sink: string; retentionTime: string } {
    // Allow overriding via local storage to switch between environments (e.g. lyfz vs default minio).
    const storageName = (localStorage.getItem('backupStorageName') || 's3').trim();
    const sink = (localStorage.getItem('backupSink') || 'default').trim();
    // Default 240 hours (10 days), in Go time.Duration format.
    const retentionTime = (localStorage.getItem('backupRetentionTime') || '240h').trim();
    return { storageName, sink, retentionTime };
  }

  private transformPodToNodeInfo(pod: Pod): NodeInfo {
    // Use role detection.
    const roleInfo = this.detectPodRole(pod);
    
    // Placeholder for resource requests
    const cpu = pod.spec?.containers?.[0]?.resources?.requests?.cpu || 'N/A';
    const memory = pod.spec?.containers?.[0]?.resources?.requests?.memory || 'N/A';

    // Parse container statuses.
    const containerStatuses: ContainerStatus[] = [];
    let readyContainers = 0;
    let totalRestarts = 0;
    let hasUnhealthyContainers = false;

    if (pod.status?.containerStatuses) {
      for (const cs of pod.status.containerStatuses) {
        const containerStatus: ContainerStatus = {
          name: cs.name,
          ready: cs.ready || false,
          restartCount: cs.restartCount || 0,
          state: this.getContainerState(cs),
          reason: this.getContainerReason(cs),
          message: this.getContainerMessage(cs)
        };
        
        containerStatuses.push(containerStatus);
        
        if (cs.ready) readyContainers++;
        totalRestarts += cs.restartCount || 0;
        
        // Mark unhealthy if not ready or restarts exceed threshold.
        if (!cs.ready || (cs.restartCount && cs.restartCount > 3)) {
          hasUnhealthyContainers = true;
        }
      }
    }

    const totalContainers = pod.spec?.containers?.length || 0;

    return {
      name: pod.metadata.name,
      type: roleInfo.role,
      status: pod.status?.phase || '未知',
      cpu: cpu,
      memory: memory,
      ip: pod.status?.podIP || '未知',
      resources: `${cpu}/${memory}`,
      readyContainers,
      totalContainers,
      totalRestarts,
      hasUnhealthyContainers,
      containerStatuses
    };
  }

  // Get container state.
  private getContainerState(containerStatus: any): string {
    if (containerStatus.state?.running) return 'Running';
    if (containerStatus.state?.waiting) return 'Waiting';
    if (containerStatus.state?.terminated) return 'Terminated';
    return 'Unknown';
  }

  // Get container state reason.
  private getContainerReason(containerStatus: any): string {
    if (containerStatus.state?.waiting?.reason) return containerStatus.state.waiting.reason;
    if (containerStatus.state?.terminated?.reason) return containerStatus.state.terminated.reason;
    return '';
  }

  // Get container state message.
  private getContainerMessage(containerStatus: any): string {
    if (containerStatus.state?.waiting?.message) return containerStatus.state.waiting.message;
    if (containerStatus.state?.terminated?.message) return containerStatus.state.terminated.message;
    return '';
  }

  // Lightweight role detection (simplified).
  private detectPodRole(pod: Pod): { role: string; category: string } {
    if (!pod || !pod.metadata) {
      return { role: 'Unknown', category: 'unknown' };
    }

    const name = pod.metadata.name || '';
    const labels = pod.metadata.labels || {};

    // Prefer role from labels.
    if (labels['polardbx/role']) {
      return { role: labels['polardbx/role'].toUpperCase(), category: 'compute' };
    }

    // Infer from name patterns.
    if (name.includes('-cn-')) return { role: 'CN', category: 'compute' };
    if (name.includes('-dn-')) return { role: 'DN', category: 'storage' };
    if (name.includes('-gms-')) return { role: 'GMS', category: 'service' };
    if (name.includes('-cdc-')) return { role: 'CDC', category: 'service' };
    if (name.includes('-columnar-')) return { role: 'Columnar', category: 'storage' };
    if (name.toLowerCase().includes('minio')) return { role: 'MinIO', category: 'storage' };
    if (name.toLowerCase().includes('sftp')) return { role: 'SFTP', category: 'service' };
    if (name.toLowerCase().includes('hpfs')) return { role: 'HPFS', category: 'service' };

    return { role: 'Unknown', category: 'unknown' };
  }

  private loadBackupData(): void {
    // If cluster info isn't loaded yet, fall back to route params.
    const namespace = this.cluster?.metadata.namespace || this.clusterNamespace;
    const clusterName = this.cluster?.metadata.name || this.clusterName;

    if (!namespace || !clusterName) {
      console.warn('无法加载备份数据：缺少集群信息');
      return;
    }

    this.apiService.getBackups(namespace, clusterName)
      .subscribe({
        next: (backups: PolarDBXBackup[]) => {
          console.log('获取到备份列表:', backups);
          const backupInfos: BackupInfo[] = backups.map(backup => ({
            id: backup.metadata.name,
            name: backup.metadata.name,
            namespace: backup.metadata.namespace,
            completedTime: backup.status?.completionTime || backup.metadata.creationTimestamp,
            type: backup.spec.backupType || 'Snapshot',
            status: backup.status?.phase || '未知',
            phase: backup.status?.phase,
            message: backup.status?.message,
            backupObject: backup
          }));
          this.backupDataSource = backupInfos;
          console.log('备份数据已设置到表格:', backupInfos);
        },
        error: (error) => {
          console.error('获取备份列表失败:', error);
          this.messageService.error('获取备份列表失败');
        }
      });
  }

  private loadLogData(): void {
    // Mock log data
    this.logContent = `[2024-12-01 15:30:01] INFO: PolarDB-X cluster ${this.clusterName} is running normally
[2024-12-01 15:29:58] INFO: All nodes are healthy
[2024-12-01 15:29:55] INFO: Connection pool status: 50/100 active connections
[2024-12-01 15:29:52] INFO: Memory usage: 65% of allocated memory
[2024-12-01 15:29:49] INFO: CPU usage: 45% average across all nodes
[2024-12-01 15:29:46] INFO: Storage usage: 2.5GB / 100GB (2.5%)
[2024-12-01 15:29:43] INFO: Backup job completed successfully
[2024-12-01 15:29:40] INFO: Replication lag: 0.2ms average`;
  }

  private initCharts(): void {
    this.initCpuChart();
    this.initMemChart();
    this.renderTopology();
    this.updateResourceCharts();
  }

  private initCpuChart(): void {
    if (!this.cpuChartRef?.nativeElement) return;

    const ctx = this.cpuChartRef.nativeElement.getContext('2d');
    if (!ctx) return;

    const config: ChartConfiguration = {
      type: 'line' as ChartType,
      data: {
        labels: [],
        datasets: [{
          label: 'CPU 使用率 (%)',
          data: [],
          borderColor: '#1976d2',
          backgroundColor: 'rgba(25, 118, 210, 0.1)',
          fill: true,
          tension: 0.4
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          y: {
            beginAtZero: true,
            max: 100,
            ticks: {
              callback: function(value: any) {
                return value + '%';
              }
            }
          }
        },
        plugins: {
          legend: {
            display: false
          }
        }
      }
    };

    this.cpuChart = new Chart(ctx, config);
  }

  private initMemChart(): void {
    if (!this.memChartRef?.nativeElement) return;

    const ctx = this.memChartRef.nativeElement.getContext('2d');
    if (!ctx) return;

    const config: ChartConfiguration = {
      type: 'line' as ChartType,
      data: {
        labels: [],
        datasets: [{
          label: '内存使用率 (%)',
          data: [],
          borderColor: '#4caf50',
          backgroundColor: 'rgba(76, 175, 80, 0.1)',
          fill: true,
          tension: 0.4
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          y: {
            beginAtZero: true,
            max: 100,
            ticks: {
              callback: function(value: any) {
                return value + '%';
              }
            }
          }
        },
        plugins: {
          legend: {
            display: false
          }
        }
      }
    };

    this.memChart = new Chart(ctx, config);
  }

  private resetResourceUsageSeries(): void {
    this.resourceUsageAvailable = true;
    this.resourceUsageSource = '';
    this.resourceUsageMessage = '';
    this.resourceUsageLastUpdated = '';
    this.cpuSeries = [];
    this.memSeries = [];
    this.timeSeries = [];

    if (this.cpuChart) {
      this.cpuChart.data.labels = [];
      this.cpuChart.data.datasets[0].data = [];
      this.cpuChart.update();
    }
    if (this.memChart) {
      this.memChart.data.labels = [];
      this.memChart.data.datasets[0].data = [];
      this.memChart.update();
    }
  }

  private applyResourceUsage(usage: ClusterResourceUsage): void {
    this.resourceUsageAvailable = !!usage.available;
    this.resourceUsageSource = usage.source || (usage.available ? 'metrics-server' : '');
    this.resourceUsageMessage = usage.message || '';

    const ts = new Date(usage.timestampMs || Date.now());
    const label = ts.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
    this.resourceUsageLastUpdated = label;

    const cpuPct = (typeof usage.cpu?.pctOfRequests === 'number' && Number.isFinite(usage.cpu.pctOfRequests)) ? usage.cpu.pctOfRequests : null;
    const memPct = (typeof usage.memory?.pctOfLimits === 'number' && Number.isFinite(usage.memory.pctOfLimits)) ? usage.memory.pctOfLimits : null;

    this.timeSeries.push(label);
    this.cpuSeries.push(cpuPct);
    this.memSeries.push(memPct);

    while (this.timeSeries.length > this.resourceUsageMaxPoints) {
      this.timeSeries.shift();
      this.cpuSeries.shift();
      this.memSeries.shift();
    }

    this.updateResourceCharts();
  }

  private updateResourceCharts(): void {
    if (this.cpuChart) {
      this.cpuChart.data.labels = [...this.timeSeries];
      this.cpuChart.data.datasets[0].data = [...this.cpuSeries] as any;
      this.cpuChart.update('none');
    }
    if (this.memChart) {
      this.memChart.data.labels = [...this.timeSeries];
      this.memChart.data.datasets[0].data = [...this.memSeries] as any;
      this.memChart.update('none');
    }
  }

  private renderTopology(): void {
    if (!this.topologySvgRef) return;
    const svg = this.topologySvgRef.nativeElement;
    // Clear
    while (svg.firstChild) svg.removeChild(svg.firstChild);
    // Container size
    const containerWidth = (svg.clientWidth || svg.getBoundingClientRect().width || 300);
    const containerHeight = (svg.clientHeight || svg.getBoundingClientRect().height || 220);
    // Define arrow marker
    const defs = document.createElementNS('http://www.w3.org/2000/svg', 'defs');
    const marker = document.createElementNS('http://www.w3.org/2000/svg', 'marker');
    marker.setAttribute('id', 'arrow');
    marker.setAttribute('markerWidth', '10');
    marker.setAttribute('markerHeight', '7');
    marker.setAttribute('refX', '10');
    marker.setAttribute('refY', '3.5');
    marker.setAttribute('orient', 'auto');
    const arrowPath = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    arrowPath.setAttribute('d', 'M0,0 L10,3.5 L0,7 Z');
    arrowPath.setAttribute('fill', '#9aa6b2');
    marker.appendChild(arrowPath);
    defs.appendChild(marker);
    svg.appendChild(defs);
    const g = new dagre.graphlib.Graph();
    g.setGraph({ rankdir: 'LR', nodesep: 20, ranksep: 40 });
    g.setDefaultEdgeLabel(() => ({}));

    // Group content for overall scaling/panning.
    const contentGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    svg.appendChild(contentGroup);

    // Define nodes (only show roles with actual pods / desired replicas).
    const buildRoleLabel = (id: 'cn'|'dn'|'gms'|'cdc'): string => {
      const s = this.roleSummary[id];
      const podsCount = (s?.pods || []).length;
      const base = `${id.toUpperCase()} (${s.ready}/${s.total})`;
      // DN/GMS 可能因为 cand/log 等 Pod 导致 podsCount > total（total 是逻辑副本口径）。
      return podsCount > s.total ? `${base} · Pods:${podsCount}` : base;
    };
    const roles: Array<{ id: 'cn'|'dn'|'gms'|'cdc'; label: string; count: number }> = [
      { id: 'cn', label: buildRoleLabel('cn'), count: this.roleSummary['cn'].total },
      { id: 'dn', label: buildRoleLabel('dn'), count: this.roleSummary['dn'].total },
      { id: 'gms', label: buildRoleLabel('gms'), count: this.roleSummary['gms'].total }
    ];
    
    // Only show CDC when it has actual pods.
    if (this.roleSummary['cdc'].total > 0) {
      roles.push({ id: 'cdc', label: buildRoleLabel('cdc'), count: this.roleSummary['cdc'].total });
    }
    
    // Limit node width based on container width.
    const maxNodeWidth = Math.max(120, Math.min(220, Math.floor(containerWidth * 0.35)));
    for (const r of roles) {
      const calc = 90 + r.label.length * 6;
      const width = Math.min(calc, maxNodeWidth);
      const height = 36;
      g.setNode(r.id, { label: r.label, width, height });
    }
    // Simple edges: CN->DN->GMS, plus DN->CDC (if any).
    g.setEdge('cn', 'dn');
    g.setEdge('dn', 'gms');
    if (this.roleSummary['cdc'].total > 0) {
      g.setEdge('dn', 'cdc');
    }

    dagre.layout(g);

    // Draw nodes (health-based colors + hover tooltip listing pods).
    for (const v of g.nodes()) {
      const n = g.node(v) as any;
      const rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
      rect.setAttribute('x', String(n.x - n.width / 2));
      rect.setAttribute('y', String(n.y - n.height / 2));
      rect.setAttribute('width', String(n.width));
      rect.setAttribute('height', String(n.height));
      rect.setAttribute('rx', '8');
      const health = this.getRoleHealth(v as 'cn'|'dn'|'gms'|'cdc');
      const fill = health >= 100 ? '#eef7ff' : (health >= 95 ? '#fff8e1' : '#ffeaea');
      const stroke = health >= 100 ? '#2f5d8a' : (health >= 95 ? '#e6a23c' : '#f56c6c');
      rect.setAttribute('fill', fill);
      rect.setAttribute('stroke', stroke);
      rect.setAttribute('stroke-width', '1');
      rect.style.cursor = 'pointer';
      rect.addEventListener('click', () => { this.onTopologyNodeClick(v as 'cn'|'dn'|'gms'|'cdc'); });
      contentGroup.appendChild(rect);

      const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      text.setAttribute('x', String(n.x));
      text.setAttribute('y', String(n.y + 4));
      text.setAttribute('text-anchor', 'middle');
      text.setAttribute('font-size', '12');
      text.textContent = n.label;
      text.style.cursor = 'pointer';
      text.addEventListener('click', () => { this.onTopologyNodeClick(v as 'cn'|'dn'|'gms'|'cdc'); });
      contentGroup.appendChild(text);

      // Built-in tooltip (native <title>) listing pods.
      const title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
      const pods = this.roleSummary[v as 'cn'|'dn'|'gms'|'cdc'].pods
        .map(p => `${p.metadata?.name} [${p.status?.phase}]`).join('\n');
      title.textContent = pods || '无 Pod';
      rect.appendChild(title);
      text.appendChild(title.cloneNode(true));
    }
    // Draw edges.
    for (const e of g.edges()) {
      const edge = g.edge(e) as any;
      const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      const points = edge.points as Array<{ x: number; y: number }>;
      const d = points.map((p, i) => (i === 0 ? `M ${p.x} ${p.y}` : `L ${p.x} ${p.y}`)).join(' ');
      path.setAttribute('d', d);
      path.setAttribute('fill', 'none');
      // Edge color: derived from downstream role health.
      const target = (e.w as 'cn'|'dn'|'gms');
      const health = this.getRoleHealth(target);
      const stroke = health >= 100 ? '#2f5d8a' : (health >= 95 ? '#e6a23c' : '#f56c6c');
      path.setAttribute('stroke', stroke);
      path.setAttribute('stroke-width', '1.5');
      path.setAttribute('marker-end', 'url(#arrow)');
      // Edge tooltip (placeholder; can later include traffic/latency).
      const t = document.createElementNS('http://www.w3.org/2000/svg', 'title');
      t.textContent = `链路 ${e.v} → ${e.w}`;
      path.appendChild(t);
      contentGroup.appendChild(path);
    }

    // Compute scaling based on layout and container size to keep content fully visible.
    const nodeList = g.nodes().map((v: string) => g.node(v) as any);
    if (nodeList.length > 0) {
      const minX = Math.min(...nodeList.map((n: any) => n.x - n.width / 2));
      const maxX = Math.max(...nodeList.map((n: any) => n.x + n.width / 2));
      const minY = Math.min(...nodeList.map((n: any) => n.y - n.height / 2));
      const maxY = Math.max(...nodeList.map((n: any) => n.y + n.height / 2));

      const layoutWidth = Math.max(1, maxX - minX);
      const layoutHeight = Math.max(1, maxY - minY);
      const padding = 12;
      const availableWidth = Math.max(1, containerWidth - padding * 2);
      const availableHeight = Math.max(1, containerHeight - padding * 2);
      const scaleW = availableWidth / layoutWidth;
      const scaleH = availableHeight / layoutHeight;
      const scale = Math.min(1, scaleW, scaleH);

      const translateX = padding + (availableWidth - layoutWidth * scale) / 2 - minX * scale;
      const translateY = padding + (availableHeight - layoutHeight * scale) / 2 - minY * scale;
      contentGroup.setAttribute('transform', `translate(${translateX}, ${translateY}) scale(${scale})`);
    }
  }

  private onTopologyNodeClick(role: 'cn'|'dn'|'gms'|'cdc'): void {
    const ns = this.clusterNamespace || 'default';
    const cluster = this.clusterName || '';
    const roleUpper = role.toUpperCase();

    // If the role maps to a single pod (common for CN), jump directly to node detail.
    const pods = (this.roleSummary?.[role]?.pods || []) as any[];
    const onlyPodName = pods.length === 1 ? (pods[0]?.metadata?.name as string) : '';
    if (onlyPodName && role !== 'dn' && role !== 'gms') {
      this.zone.run(() => this.router.navigate(['/operations', 'nodes', ns, onlyPodName]));
      return;
    }

    // Otherwise go to the nodes list and pre-filter.
    this.zone.run(() => this.router.navigate(['/operations', 'nodes'], {
      queryParams: {
        namespace: ns,
        role: roleUpper,
        keyword: cluster
      }
    }));
  }

  private getRoleHealth(role: 'cn'|'dn'|'gms'|'cdc'): number {
    const s = this.roleSummary[role];
    if (!s || s.total === 0) return 0;
    return Math.round((s.ready / s.total) * 100);
  }

  // Event handlers
  deleteCluster(): void {
    if (confirm(`确定要删除集群 ${this.clusterName} 吗？此操作不可撤销。`)) {
      // Precheck
      this.apiService.runPrecheck(this.clusterNamespace, this.clusterName, 'config', {}).subscribe({
        next: () => {
          // Delete only after precheck passes.
          this.apiService.deleteCluster(this.clusterNamespace, this.clusterName).subscribe({
            next: () => {
              this.messageService.success('集群删除成功');
              this.router.navigate(['/clusters']);
            },
            error: (err) => {
              this.messageService.error(`删除集群失败：${err.message}`);
            }
          });
        },
        error: (err) => {
          const checks = err?.error?.checks || {};
          const msg = `前置检查未通过：\n` +
            `- 最近全备：${checks.hasRecentBackup ? '是' : '否'}\n` +
            `- 存储连通：${checks.storageConnectivity || '未知'}\n` +
            `- RPO滞后(秒)：${typeof checks.rpoLagSeconds==='number'?checks.rpoLagSeconds:'未知'}`;
          this.messageService.error(msg);
        }
      });
    }
  }

  // Quick scaling - skip precheck
  quickApplyConfig(): void {
    if (!this.configForm.valid || !this.cluster) {
      this.messageService.error('请检查配置信息');
      return;
    }

    this.messageService.warning('快速模式将跳过前置检查，仅建议在开发环境使用');
    
    // Build scaling request
    const config = this.configForm.value;
    const scalingRequest: any = {};

    // Detect replica changes
    if (config.cnReplicas !== this.cnReplicas) {
      scalingRequest.cnReplicas = config.cnReplicas;
    }
    if (config.dnReplicas !== this.dnReplicas) {
      scalingRequest.dnReplicas = config.dnReplicas;
    }
    if (config.cdcReplicas !== this.cdcReplicas) {
      scalingRequest.cdcReplicas = config.cdcReplicas;
    }
    if (config.cdcReplicas !== this.cdcReplicas) {
      scalingRequest.cdcReplicas = config.cdcReplicas;
    }

    // No-op when there are no changes.
    if (Object.keys(scalingRequest).length === 0) {
      this.messageService.info('配置没有变化');
      return;
    }

    // Confirmation dialog
    const changeList = [];
    if (scalingRequest.cnReplicas !== undefined) {
      changeList.push(`CN 节点: ${this.cnReplicas} → ${scalingRequest.cnReplicas}`);
    }
    if (scalingRequest.dnReplicas !== undefined) {
      changeList.push(`DN 节点: ${this.dnReplicas} → ${scalingRequest.dnReplicas}`);
    }
    if (scalingRequest.cdcReplicas !== undefined) {
      changeList.push(`CDC 节点: ${this.cdcReplicas} → ${scalingRequest.cdcReplicas}`);
    }
    if (scalingRequest.cdcReplicas !== undefined) {
      changeList.push(`CDC 节点: ${this.cdcReplicas} → ${scalingRequest.cdcReplicas}`);
    }

    if (confirm(`确定要快速应用以下配置变更吗？\n${changeList.join('\n')}\n\n⚠️ 快速模式将跳过前置检查`)) {
      // Call scale API directly and skip precheck.
      this.apiService.scaleCluster(this.clusterNamespace, this.clusterName, scalingRequest)
        .subscribe({
          next: (response: any) => {
            this.messageService.success('快速变配请求已提交');
            // Update local state
            if (scalingRequest.cnReplicas !== undefined) {
              this.cnReplicas = scalingRequest.cnReplicas;
            }
            if (scalingRequest.dnReplicas !== undefined) {
              this.dnReplicas = scalingRequest.dnReplicas;
            }
            if (scalingRequest.cdcReplicas !== undefined) {
              this.cdcReplicas = scalingRequest.cdcReplicas;
            }
            // Refresh cluster data
            setTimeout(() => {
              this.loadClusterData();
            }, 2000);
          },
          error: (error) => {
            console.error('快速变配失败:', error);
            this.messageService.error(`快速变配失败: ${error.message || '未知错误'}`);
          }
        });
    }
  }

  // Full scaling flow (kept for wizard mode)
  applyConfig(): void {
    if (!this.configForm.valid || !this.cluster) {
      this.messageService.error('请检查配置信息');
      return;
    }

    const config = this.configForm.value;
    const scalingRequest: any = {};

    // Detect replica changes
    if (config.cnReplicas !== this.cnReplicas) {
      scalingRequest.cnReplicas = config.cnReplicas;
    }
    if (config.dnReplicas !== this.dnReplicas) {
      scalingRequest.dnReplicas = config.dnReplicas;
    }

    // No-op when there are no changes.
    if (Object.keys(scalingRequest).length === 0) {
      this.messageService.info('配置没有变化');
      return;
    }

    // Confirmation dialog
    const changeList = [];
    if (scalingRequest.cnReplicas !== undefined) {
      changeList.push(`CN 节点: ${this.cnReplicas} → ${scalingRequest.cnReplicas}`);
    }
    if (scalingRequest.dnReplicas !== undefined) {
      changeList.push(`DN 节点: ${this.dnReplicas} → ${scalingRequest.dnReplicas}`);
    }

    if (confirm(`确定要应用以下配置变更吗？\n${changeList.join('\n')}`)) {
      // Precheck (scale)
      this.apiService.runPrecheck(this.clusterNamespace, this.clusterName, 'scale', scalingRequest)
        .subscribe({
          next: (res: any) => {
            const token = res?.token || '';
            const tokenSig = res?.tokenSig || '';
            this.apiService.scaleCluster(this.clusterNamespace, this.clusterName, scalingRequest, token, tokenSig)
        .subscribe({
          next: (response) => {
            this.messageService.success('集群扩缩容任务已启动');
            // Update local state
            if (scalingRequest.cnReplicas !== undefined) {
              this.cnReplicas = scalingRequest.cnReplicas;
            }
            if (scalingRequest.dnReplicas !== undefined) {
              this.dnReplicas = scalingRequest.dnReplicas;
            }
            if (scalingRequest.cdcReplicas !== undefined) {
              this.cdcReplicas = scalingRequest.cdcReplicas;
            }
            // Refresh cluster data
            setTimeout(() => {
              this.loadClusterData();
            }, 2000);
          },
          error: (error) => {
            console.error('扩缩容失败:', error);
            this.messageService.error(`扩缩容失败: ${error.message || '未知错误'}`);
          }
        });
          },
          error: (err) => {
            const checks = err?.error?.checks || {};
            const msg = `前置检查未通过：\n` +
              `- 最近全备：${checks.hasRecentBackup ? '是' : '否'}\n` +
              `- 存储连通：${checks.storageConnectivity || '未知'}\n` +
              `- RPO滞后(秒)：${typeof checks.rpoLagSeconds==='number'?checks.rpoLagSeconds:'未知'}`;
            this.messageService.error(msg);
          }
        });
    }
  }

  resetConfig(): void {
    // Reset form values to current cluster state.
    this.configForm.patchValue({
      cnReplicas: this.cnReplicas,
      dnReplicas: this.dnReplicas,
      cdcReplicas: this.cdcReplicas
    });
    this.messageService.success('配置已重置');
  }

  resetUpgrade(): void {
    // Reset upgrade form.
    this.upgradeForm.patchValue({
      targetVersion: this.clusterVersion
    });
    this.messageService.success('升级配置已重置');
  }

  // Check whether there are config changes.
  hasConfigChanges(): boolean {
    const config = this.configForm.value;
    return config.cnReplicas !== this.cnReplicas || 
           config.dnReplicas !== this.dnReplicas ||
           config.cdcReplicas !== this.cdcReplicas;
  }

  // Estimate cost changes.
  previewCost(): void {
    const config = this.configForm.value;
    const currentTotal = this.cnReplicas + this.dnReplicas + 1; // +1 for GMS
    const newTotal = config.cnReplicas + config.dnReplicas + 1;
    const costChange = ((newTotal - currentTotal) / currentTotal * 100).toFixed(1);
    
    const message = newTotal > currentTotal 
      ? `扩容后成本预计增加 ${costChange}%`
      : newTotal < currentTotal 
        ? `缩容后成本预计减少 ${Math.abs(parseFloat(costChange))}%`
        : '配置无变化，成本不变';
    
    this.messageService.info(message);
  }

  // Node management
  restartNode(node: NodeInfo): void {
    if (confirm(`确定要重启节点 ${node.name} 吗？这会短暂中断该节点的服务。`)) {
      this.apiService.deletePod(this.clusterNamespace, node.name).subscribe({
        next: () => {
          this.messageService.info(`节点 ${node.name} 重启中...`);
          setTimeout(() => this.loadClusterData(), 2000);
        },
        error: (error) => {
          this.messageService.error(`重启节点失败: ${error.message}`);
        }
      });
    }
  }

  deleteAndRecreateNode(node: NodeInfo): void {
    const warningMessage = node.type === 'GMS' 
      ? `⚠️ 警告：GMS节点是集群的管理核心，删除重建可能导致集群短时间不可用。\n\n确定要删除重建 ${node.name} 吗？`
      : `确定要删除重建节点 ${node.name} 吗？该节点将被重新创建。`;
      
    if (confirm(warningMessage)) {
      this.messageService.info(`正在删除重建节点 ${node.name}...`);
      
      this.apiService.deletePod(this.clusterNamespace, node.name).subscribe({
        next: () => {
          this.messageService.info(`节点 ${node.name} 已删除，正在重建...`);
          // Delay refresh to show rebuild progress.
          setTimeout(() => this.loadClusterData(), 3000);
          setTimeout(() => this.loadClusterData(), 10000);
        },
        error: (error) => {
          this.messageService.error(`删除节点失败: ${error.message}`);
        }
      });
    }
  }

  isolateNode(node: NodeInfo): void {
    if (node.type === 'GMS') {
      this.messageService.warning('GMS节点不能被隔离');
      return;
    }
    
    if (confirm(`确定要隔离节点 ${node.name} 吗？隔离后该节点将不再处理新的请求。`)) {
      // TODO: call the corresponding API to isolate the node.
      // Implementation depends on PolarDB-X isolation mechanism.
      this.messageService.info(`节点隔离功能正在开发中...`);
    }
  }

  // Open alert details dialog
  openAlertsDialog(): void {
    if (this.getAlertCount() === 0) {
      this.messageService.info('当前无告警信息');
      return;
    }

    import('../../components/alerts-detail-dialog/alerts-detail-dialog.component').then(m => {
      const ref = this.modalService.create({
        nzTitle: '告警详情',
        nzContent: m.AlertsDetailDialogComponent,
        nzWidth: '80vw',
        nzStyle: { maxWidth: '1200px', height: '70vh' },
        nzData: {
          cluster: this.cluster,
          alerts: this.alerts,
          namespace: this.clusterNamespace,
          clusterName: this.clusterName
        }
      });

      ref.afterClose.subscribe((result: any) => {
        if (result?.refreshAlerts) {
          // Reload alert data
          const am = localStorage.getItem('alertmanager') || '';
          this.apiService.getClusterAlertsSummary(this.clusterNamespace, this.clusterName, am || undefined).subscribe({
            next: (s) => this.alerts = s,
            error: () => this.alerts = { critical: 0, warning: 0, info: 0, total: 0, source: 'none' }
          });
        }
      });
    }).catch(error => {
      console.error('Failed to load alerts dialog:', error);
      this.messageService.info('告警详情功能正在开发中...');
    });
  }

  // Quick upgrade - skip precheck
  quickStartUpgrade(): void {
    if (!this.upgradeForm.valid || !this.cluster) {
      this.messageService.error('请检查升级配置');
      return;
    }

    this.messageService.warning('快速升级将跳过安全检查，仅建议在开发环境使用');
    
    const targetVersion = this.upgradeForm.value.targetVersion;
    const currentVersion = this.clusterVersion;

    const upgradeRequest = {
      targetVersion: targetVersion,
      strategy: 'rolling', // Default to rolling upgrade
      maxUnavailable: 1
    };

    const confirmMessage = `确定要快速升级集群吗？\n\n` +
                          `当前版本: ${currentVersion}\n` +
                          `目标版本: ${targetVersion}\n` +
                          `升级策略: 滚动升级\n\n` +
                          `⚠️ 快速模式将跳过安全检查，升级过程中可能会有短暂的服务中断`;

    if (confirm(confirmMessage)) {
      // Call upgrade API directly and skip precheck.
      this.apiService.upgradeCluster(this.clusterNamespace, this.clusterName, upgradeRequest)
        .subscribe({
          next: (response: any) => {
            this.messageService.success('快速升级请求已提交');
            // Refresh cluster data
            setTimeout(() => {
              this.loadClusterData();
            }, 3000);
          },
          error: (error) => {
            console.error('快速升级失败:', error);
            this.messageService.error(`快速升级失败: ${error.message || '未知错误'}`);
          }
        });
    }
  }

  // Full upgrade flow (kept for wizard mode)
  startUpgrade(): void {
    if (!this.upgradeForm.valid || !this.cluster) {
      this.messageService.error('请检查升级配置');
      return;
    }

    const targetVersion = this.upgradeForm.value.targetVersion;
    const currentVersion = this.clusterVersion;

    // Version comparison
    if (targetVersion === currentVersion) {
      this.messageService.info('目标版本与当前版本相同');
      return;
    }

    const upgradeRequest = {
      targetVersion: targetVersion,
      strategy: 'rolling', // Default to rolling upgrade
      maxUnavailable: 1
    };

    const confirmMessage = `确定要将集群升级吗？\n\n` +
                          `当前版本: ${currentVersion}\n` +
                          `目标版本: ${targetVersion}\n` +
                          `升级策略: 滚动升级\n\n` +
                          `⚠️ 升级过程中可能会有短暂的服务中断`;

    if (confirm(confirmMessage)) {
      this.apiService.runPrecheck(this.clusterNamespace, this.clusterName, 'upgrade', this.upgradeForm.value)
        .subscribe({
          next: (res: any) => {
            const token = res?.token || '';
            const tokenSig = res?.tokenSig || '';
            this.apiService.upgradeCluster(this.clusterNamespace, this.clusterName, upgradeRequest, token, tokenSig)
        .subscribe({
          next: (response) => {
            this.messageService.success('集群升级任务已启动，请关注升级进度');
            
            // Update local displayed version
            this.clusterVersion = targetVersion;
            
            // Refresh cluster status
            setTimeout(() => {
              this.loadClusterData();
            }, 2000);
          },
          error: (error) => {
            console.error('升级失败:', error);
            this.messageService.error(`升级失败: ${error.message || '未知错误'}`);
          }
        });
          },
          error: (err) => {
            const checks = err?.error?.checks || {};
            const msg = `前置检查未通过：\n` +
              `- 最近全备：${checks.hasRecentBackup ? '是' : '否'}\n` +
              `- 存储连通：${checks.storageConnectivity || '未知'}\n` +
              `- RPO滞后(秒)：${typeof checks.rpoLagSeconds==='number'?checks.rpoLagSeconds:'未知'}`;
            this.messageService.error(msg);
          }
        });
    }
  }

  createBackup(): void {
    if (!this.cluster) return;

    // Generate a Kubernetes-compliant backup name (max 63 chars, lowercase alnum + hyphen).
    const now = new Date();
    const shortTimestamp = now.getFullYear().toString().slice(-2) + 
                          (now.getMonth() + 1).toString().padStart(2, '0') +
                          now.getDate().toString().padStart(2, '0') +
                          now.getHours().toString().padStart(2, '0') +
                          now.getMinutes().toString().padStart(2, '0');
    
    // Ensure name <= 63 chars and conforms to DNS label rules.
    const clusterName = this.cluster.metadata.name;
    const baseName = `${clusterName}-bak-${shortTimestamp}`;
    
    // If the name is too long, truncate the cluster-name part.
    const backupName = baseName.length > 63 ? 
                       `${clusterName.slice(0, 63 - 16)}-bak-${shortTimestamp}` : 
                       baseName;

    // Preferences: storage type / sink / retention
    const prefs = this.getBackupPreferences();

    // Build a full PolarDBXBackup object (Kubernetes resource format).
    const backupObject: any = {
      apiVersion: "polardbx.aliyun.com/v1",
      kind: "PolarDBXBackup",
      metadata: {
        name: backupName,
        namespace: this.cluster.metadata.namespace
      },
      spec: {
        cluster: {
          name: this.cluster.metadata.name
        },
        retentionTime: prefs.retentionTime,
        storageProvider: {
          storageName: prefs.storageName,
          sink: prefs.sink
        },
        preferredBackupRole: "follower"
      }
    };

    this.apiService.createBackup(this.cluster.metadata.namespace, this.cluster.metadata.name, backupObject)
      .subscribe({
        next: (backup: PolarDBXBackup) => {
          console.log('备份创建成功:', backup);
          this.messageService.success(`备份 ${backup.metadata.name} 创建成功`);
          this.loadBackupData(); // Reload backup data
        },
        error: (error) => {
          console.error('创建备份失败:', error);
          this.messageService.error('创建备份失败');
        }
      });
  }

  downloadBackup(backup: BackupInfo): void {
    const namespace = (backup?.namespace || this.clusterNamespace || 'default').trim() || 'default';
    const name = (backup?.name || backup?.id || '').trim();
    if (!name) {
      this.messageService.warning('备份名称为空，无法下载');
      return;
    }

    const phase = (backup?.phase || backup?.status || '').toLowerCase().trim();
    const downloadable = ['finished', 'completed', 'succeeded'].includes(phase);
    if (!downloadable) {
      this.showBackupDownloadInfo(namespace, name, `备份尚未完成（当前状态：${backup?.status || backup?.phase || '未知'}）`);
      return;
    }

    this.apiService.downloadBackupFile(namespace, name).subscribe({
      next: (blob: Blob) => {
        const stamp = new Date().toISOString().replace(/[:.]/g, '-');
        const filename = `${name}-${stamp}.tar.gz`;

        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);

        this.messageService.success('下载已开始');
      },
      error: (err: any) => {
        const status = err?.status;
        if (status === 409) {
          this.showBackupDownloadInfo(namespace, name, '备份未完成，暂不能下载');
          return;
        }
        if (status === 404) {
          this.showBackupDownloadInfo(namespace, name, '未找到备份或存储配置（HPFS sink）');
          return;
        }
        this.showBackupDownloadInfo(namespace, name, err?.error?.message || err?.message || '下载失败');
      }
    });
  }

  private showBackupDownloadInfo(namespace: string, name: string, hint?: string): void {
    this.apiService.getBackupDownloadInfo(namespace, name).subscribe({
      next: (info: any) => {
        const rootPath = info?.backupRootPath || '—';
        const storage = info?.storage || '—';
        const sink = info?.sink || '—';
        const command = info?.command || '（无）';

        this.backupDownloadInfoText = [
          hint ? String(hint) : '',
          `备份: ${namespace}/${name}`,
          `存储: ${storage} / sink=${sink}`,
          `路径: ${rootPath}`,
          '',
          '建议命令:',
          String(command),
          '',
          '提示：在线下载依赖后端能直连存储；大文件更推荐用上面的 CLI 命令。'
        ].filter(Boolean).join('\n');

        this.modalService.info({
          nzTitle: '备份下载',
          nzWidth: 720,
          nzContent: this.backupDownloadInfoTpl || this.backupDownloadInfoText
        });
      },
      error: () => {
        this.messageService.warning(hint || '无法获取下载信息');
      }
    });
  }

  restoreBackup(backup: BackupInfo): void {
    if (confirm(`确定要从备份 ${backup.id} 恢复集群吗？此操作将覆盖当前数据。`)) {
      // Mock restore backup
      this.messageService.info('恢复任务已启动');
    }
  }

  deleteBackup(backup: BackupInfo): void {
    if (confirm(`确定要删除备份 ${backup.id} 吗？`)) {
      this.apiService.deleteBackup(backup.namespace || this.clusterNamespace, backup.name)
        .subscribe({
          next: () => {
            console.log('备份删除成功:', backup.name);
            this.messageService.success(`备份 ${backup.name} 删除成功`);
            this.loadBackupData(); // Reload backup data
          },
          error: (error) => {
            console.error('删除备份失败:', error);
            this.messageService.error('删除备份失败');
          }
        });
    }
  }

  forceDeleteBackup(backup: BackupInfo): void {
    if (confirm(`强制删除将直接移除 finalizers 并清理对象。确定对备份 ${backup.name} 执行吗？`)) {
      this.apiService.forceDeleteBackup(backup.namespace || this.clusterNamespace, backup.name)
        .subscribe({
          next: () => {
            this.messageService.success(`备份 ${backup.name} 强制删除成功`);
            this.loadBackupData();
          },
          error: (error) => {
            console.error('强制删除备份失败:', error);
            this.messageService.error('强制删除备份失败');
          }
        });
    }
  }

  onPodSelectionChange(): void {
    const selectedPod = this.allPods.find(p => p.metadata.name === this.selectedPod);
    const containers = selectedPod?.spec?.containers || [];
    this.selectedPodContainers = containers;
    const list = containers.map(c => c.name);
    const ordered = this.getPreferredContainers(list);
    this.selectedContainer = ordered[0] || (list[0] || '');
  }

  fetchLogs(): void {
    if (!this.selectedPod || !this.selectedContainer) {
      this.messageService.warning('请先选择一个 Pod 和 Container');
      return;
    }
    this.isLoadingLogs = true;
    this.logContent = '正在加载日志...';
    this.apiService.getPodLogs(this.clusterNamespace, this.selectedPod, this.selectedContainer).subscribe({
      next: (logs) => {
        this.logContent = logs || '该 Pod 没有可显示的日志。';
        this.isLoadingLogs = false;
      },
      error: (err) => {
        this.logContent = `加载日志失败: ${err.error?.error || err.message}`;
        this.isLoadingLogs = false;
        this.messageService.error(`加载日志失败: ${err.message}`);
      }
    });
  }

  // Helper methods (moved to the end of the file)

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
    // Clear all loading states to avoid leaks.
    this.loadingService.clearAll();
    
    // Dispose chart instances.
    if (this.cpuChart) {
      this.cpuChart.destroy();
    }
    if (this.memChart) {
      this.memChart.destroy();
    }
  }

  refreshData() {
    // Reload cluster data
    this.loadClusterData();
    this.loadBackupData();
    this.loadLogData();
  }

  // Handle tab changes
  onTabChange(index: number): void {
    this.selectedTabIndex = index;
    if (index === 2) { // Backups & restore tab index (0-based)
      // Ensure backup data is loaded.
      setTimeout(() => {
        this.loadBackupData();
      }, 100);
    }
  }

  // Node type color (moved to new location)

  // Status color
  getStatusColor(status: string): string {
    switch (status) {
      case 'Running':
      case 'Ready':
      case 'Completed':
        return 'primary';
      case 'Pending':
      case 'Creating':
        return 'accent';
      case 'Failed':
      case 'Error':
        return 'warn';
      default:
        return 'basic';
    }
  }

  // Ready ratio color
  getReadyColor(ready: number = 0, total: number = 0): string {
    if (total === 0) return 'basic';
    const ratio = ready / total;
    if (ratio === 1) return 'primary';  // All ready - green
    if (ratio >= 0.5) return 'accent';  // Partially ready - blue
    return 'warn';                      // Mostly not ready - red
  }

  // Ready ratio tooltip
  getReadyTooltip(ready: number = 0, total: number = 0, containers: ContainerStatus[] = []): string {
    if (total === 0) return '无容器';
    
    const notReadyContainers = containers.filter(c => !c.ready);
    if (notReadyContainers.length === 0) {
      return '所有容器都已就绪';
    }
    
    const notReadyInfo = notReadyContainers.map(c => 
      `${c.name}: ${c.state || '未知'} ${c.reason ? '(' + c.reason + ')' : ''}`
    ).join('\n');
    
    return `未就绪容器:\n${notReadyInfo}`;
  }

  // Unhealthy container info
  getUnhealthyContainersInfo(containers: ContainerStatus[] = []): string {
    const unhealthy = containers.filter(c => !c.ready || (c.restartCount > 3));
    if (unhealthy.length === 0) return '';
    
    return '不健康容器:\n' + unhealthy.map(c => 
      `${c.name}: 重启${c.restartCount}次 ${c.reason ? '(' + c.reason + ')' : ''}`
    ).join('\n');
  }

  // Restart count tooltip
  getRestartsTooltip(containers: ContainerStatus[] = []): string {
    if (containers.length === 0) return '无容器信息';
    
    return '各容器重启次数:\n' + containers.map(c => 
      `${c.name}: ${c.restartCount}次`
    ).join('\n');
  }

  // ng-zorro adaptation helpers - redefined to avoid duplicates
  getNodeTypeIcon(type: string): string {
    switch (type.toLowerCase()) {
      case 'cn': return 'desktop';
      case 'dn': return 'database';
      case 'gms': return 'control';
      case 'cdc': return 'sync';
      default: return 'question-circle';
    }
  }

  getNodeTypeColor(type: string): string {
    switch (type.toLowerCase()) {
      case 'cn': return '#1890ff';
      case 'dn': return '#52c41a';
      case 'gms': return '#fa8c16';
      case 'cdc': return '#722ed1';
      default: return '#8c8c8c';
    }
  }

  getNodeTypeTagColor(type: string): string {
    switch (type.toLowerCase()) {
      case 'cn': return 'blue';
      case 'dn': return 'green';
      case 'gms': return 'orange';
      case 'cdc': return 'purple';
      default: return 'default';
    }
  }

  getNodeStatusColor(status: string): string {
    switch (status.toLowerCase()) {
      case 'running': return 'green';
      case 'pending': return 'orange';
      case 'failed': return 'red';
      case 'unknown': return 'default';
      default: return 'default';
    }
  }

  getNodeStatusIcon(status: string): string {
    switch (status.toLowerCase()) {
      case 'running': return 'check-circle';
      case 'pending': return 'clock-circle';
      case 'failed': return 'close-circle';
      case 'unknown': return 'question-circle';
      default: return 'question-circle';
    }
  }

  getBackupStatusColor(status: string): string {
    switch (status.toLowerCase()) {
      case 'completed': return 'green';
      case 'running': return 'blue';
      case 'failed': return 'red';
      case 'pending': return 'orange';
      default: return 'default';
    }
  }

  // Check description
  getCheckDescription(): string {
    if (!this.precheck) return '';
    
    if (this.precheck.pass) {
      return '所有检查项目都已通过，集群状态良好，可以安全执行操作。';
    }
    
    const issues = [];
    if (this.precheck.hasWarn) {
      issues.push('警告');
    }
    if (this.precheck.hasError) {
      issues.push('错误');
    }
    
    return issues.length > 0 ? `发现 ${issues.join(' 和 ')}，请查看详细信息并处理后再执行操作。` : '';
  }

}
