import { Component, OnInit, OnDestroy, inject, ChangeDetectionStrategy, ChangeDetectorRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import { NzCardModule } from 'ng-zorro-antd/card';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzModalModule, NzModalService } from 'ng-zorro-antd/modal';
import { NzMessageModule, NzMessageService } from 'ng-zorro-antd/message';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzStatisticModule } from 'ng-zorro-antd/statistic';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzBadgeModule } from 'ng-zorro-antd/badge';

import { ApiService } from '../../services/api.service';
import { NamespaceService } from '../../services/namespace.service';
import { Pod } from '../../models/pod.model';
import { PodRoleDetector, PodRoleInfo } from '../../utils/pod-role-detector';
import { Subject, takeUntil, interval } from 'rxjs';

interface NodeItem {
  name: string;
  role: string;
  roleInfo: PodRoleInfo;
  status: string;
  ip: string;
  nodeName: string;
  restarts: number;
  age: string;
  pod: Pod;
}

interface RoleSummary {
  role: string;
  ready: number;
  total: number;
  category: string;
  color: string;
}

@Component({
  selector: 'app-nodes',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    NzCardModule,
    NzButtonModule,
    NzIconModule,
    NzTableModule,
    NzInputModule,
    NzSelectModule,
    NzTagModule,
    NzToolTipModule,
    NzModalModule,
    NzMessageModule,
    NzSpinModule,
    NzStatisticModule,
    NzGridModule,
    NzEmptyModule,
    NzDividerModule,
    NzBadgeModule
  ],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="page-wrapper nodes-page">
      <div class="page-header">
        <div class="title-block">
          <h2>
            <i nz-icon nzType="apartment" class="page-icon"></i>
            节点管理
          </h2>
          <p>
            查看和管理 PolarDB-X 集群中的所有 Pod 节点
            <button nz-button nzType="default" nzSize="small" (click)="load()" [nzLoading]="loading">
              <i nz-icon nzType="reload"></i>
              刷新
            </button>
          </p>
        </div>
      </div>

      <div class="page-content">
        <nz-card class="stats-card" [nzBordered]="false">
          <div nz-row [nzGutter]="16">
            <div nz-col [nzSpan]="6" *ngFor="let s of summary | slice:0:4">
              <nz-card class="stat-item" [nzBordered]="true" [nzBodyStyle]="{ padding: '16px' }">
                <nz-statistic [nzValue]="s.ready" [nzTitle]="s.role" [nzSuffix]="'/ ' + s.total" [nzValueStyle]="{ color: getStatColor(s) }"></nz-statistic>
              </nz-card>
            </div>
          </div>
          <div nz-row [nzGutter]="16" style="margin-top: 16px;" *ngIf="summary.length > 4">
            <div nz-col [nzSpan]="6" *ngFor="let s of summary | slice:4:8">
              <nz-card class="stat-item" [nzBordered]="true" [nzBodyStyle]="{ padding: '16px' }">
                <nz-statistic [nzValue]="s.ready" [nzTitle]="s.role" [nzSuffix]="'/ ' + s.total" [nzValueStyle]="{ color: getStatColor(s) }"></nz-statistic>
              </nz-card>
            </div>
          </div>
        </nz-card>

        <nz-card class="filter-card" [nzBordered]="false">
          <div class="filter-row">
            <div class="filter-item">
              <label>命名空间</label>
              <nz-select [(ngModel)]="namespace" (ngModelChange)="onNamespaceChange($event)" nzShowSearch nzAllowClear [nzPlaceHolder]="'选择命名空间'" style="width: 200px;">
                <nz-option *ngFor="let ns of namespaceOptions" [nzValue]="ns" [nzLabel]="ns"></nz-option>
              </nz-select>
            </div>
            <div class="filter-item">
              <label>角色筛选</label>
              <nz-select [(ngModel)]="roleFilter" (ngModelChange)="applyFilter()" nzAllowClear [nzPlaceHolder]="'全部角色'" style="width: 180px;">
                <nz-option nzValue="" nzLabel="全部角色"></nz-option>
                <nz-option nzValue="CN" nzLabel="CN (计算节点)"></nz-option>
                <nz-option nzValue="DN" nzLabel="DN (数据节点)"></nz-option>
                <nz-option nzValue="GMS" nzLabel="GMS (元服务)"></nz-option>
                <nz-option nzValue="CDC" nzLabel="CDC (数据捕获)"></nz-option>
                <nz-option nzValue="MinIO" nzLabel="MinIO (存储)"></nz-option>
                <nz-option nzValue="Unknown" nzLabel="未知"></nz-option>
              </nz-select>
            </div>
            <div class="filter-item">
              <label>搜索</label>
              <nz-input-group [nzPrefix]="prefixIcon" style="width: 280px;">
                <input nz-input [(ngModel)]="keyword" (ngModelChange)="applyFilter()" placeholder="搜索名称或 IP..." />
              </nz-input-group>
              <ng-template #prefixIcon><i nz-icon nzType="search"></i></ng-template>
            </div>
          </div>
        </nz-card>

        <nz-card class="table-card" [nzBordered]="false">
          <nz-spin [nzSpinning]="loading">
            <nz-table #nodeTable [nzData]="filtered" [nzPageSize]="20" [nzShowSizeChanger]="true" [nzPageSizeOptions]="[10, 20, 50, 100]" nzSize="middle" [nzScroll]="{ x: '1200px' }">
              <thead>
                <tr>
                  <th nzWidth="280px">名称</th>
                  <th nzWidth="100px">角色</th>
                  <th nzWidth="100px">状态</th>
                  <th nzWidth="140px">IP</th>
                  <th nzWidth="180px">节点</th>
                  <th nzWidth="80px">重启</th>
                  <th nzWidth="100px">运行时间</th>
                  <th nzWidth="160px" nzRight>操作</th>
                </tr>
              </thead>
              <tbody>
                <tr *ngFor="let node of nodeTable.data">
                  <td><a (click)="openDetail(node)" class="node-name">{{ node.name }}</a></td>
                  <td><nz-tag [nzColor]="getRoleColor(node.roleInfo)">{{ node.role }}</nz-tag></td>
                  <td><nz-badge [nzStatus]="getStatusBadge(node.status)" [nzText]="node.status"></nz-badge></td>
                  <td><code class="ip-code">{{ node.ip || '-' }}</code></td>
                  <td><span class="node-host" nz-tooltip [nzTooltipTitle]="node.nodeName">{{ node.nodeName | slice:0:24 }}{{ node.nodeName.length > 24 ? '...' : '' }}</span></td>
                  <td><span [class.restart-warning]="node.restarts > 0">{{ node.restarts }}</span></td>
                  <td>{{ node.age }}</td>
                  <td nzRight>
                    <button nz-button nzType="link" nzSize="small" (click)="openDetail(node)" nz-tooltip nzTooltipTitle="查看详情"><i nz-icon nzType="eye"></i></button>
                    <nz-divider nzType="vertical"></nz-divider>
                    <button nz-button nzType="link" nzSize="small" (click)="openExec(node)" nz-tooltip nzTooltipTitle="执行命令"><i nz-icon nzType="play-circle"></i></button>
                  </td>
                </tr>
              </tbody>
            </nz-table>
            <nz-empty *ngIf="!loading && filtered.length === 0" nzNotFoundContent="暂无节点数据"></nz-empty>
          </nz-spin>
        </nz-card>
      </div>
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
      display: flex;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
      line-height: 1.6;
      button { margin-left: 8px; }
    }
    .page-content { display: flex; flex-direction: column; gap: 16px; }
    .stats-card { border-radius: 10px; box-shadow: 0 2px 10px rgba(15, 23, 42, 0.04); }
    .stat-item { text-align: center; border-radius: 8px; transition: box-shadow 0.3s; }
    .stat-item:hover { box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1); }
    .filter-card { border-radius: 10px; box-shadow: 0 2px 10px rgba(15, 23, 42, 0.04); }
    .filter-row { display: flex; gap: 24px; flex-wrap: wrap; align-items: flex-end; }
    .filter-item { display: flex; flex-direction: column; gap: 8px; }
    .filter-item label { font-size: 13px; color: rgba(0, 0, 0, 0.65); font-weight: 500; }
    .table-card { border-radius: 10px; box-shadow: 0 2px 10px rgba(15, 23, 42, 0.04); }
    .node-name { color: var(--primary-color, #ff6a00); cursor: pointer; font-weight: 500; }
    .node-name:hover { text-decoration: underline; }
    .ip-code { font-family: monospace; font-size: 12px; background: #f5f5f5; padding: 2px 6px; border-radius: 4px; }
    .node-host { font-size: 12px; color: rgba(0, 0, 0, 0.65); }
    .restart-warning { color: #faad14; font-weight: 600; }
    ::ng-deep .ant-table-thead > tr > th { background: #fafafa; font-weight: 600; }
    ::ng-deep .ant-statistic-content { font-size: 24px; }
    ::ng-deep .ant-statistic-content-suffix { font-size: 14px; color: rgba(0, 0, 0, 0.45); }

    @media (max-width: 768px) { .page-wrapper { padding: 16px; } }
  `]
})
export class NodesComponent implements OnInit, OnDestroy {
  private readonly api = inject(ApiService);
  private readonly nsService = inject(NamespaceService);
  private readonly modal = inject(NzModalService);
  private readonly message = inject(NzMessageService);
  private readonly router = inject(Router);
  private readonly route = inject(ActivatedRoute);
  private readonly cdr = inject(ChangeDetectorRef);
  private readonly destroy$ = new Subject<void>();

  namespace = 'default';
  namespaceOptions: string[] = [];
  keyword = '';
  roleFilter = '';
  loading = false;
  private pendingOpenPodName?: string;

  all: NodeItem[] = [];
  filtered: NodeItem[] = [];
  summary: RoleSummary[] = [];

  ngOnInit(): void {
    // Support deep link / navigation from topology:
    // /operations/nodes?namespace=default&role=CN&keyword=<clusterName>&pod=<podName>
    this.route.queryParamMap.pipe(takeUntil(this.destroy$)).subscribe(pm => {
      const ns = (pm.get('namespace') || pm.get('ns') || '').trim();
      const role = (pm.get('role') || '').trim();
      const keyword = (pm.get('keyword') || pm.get('q') || pm.get('cluster') || '').trim();
      const pod = (pm.get('pod') || '').trim();

      let needReload = false;
      if (ns && ns !== this.namespace) {
        this.namespace = ns;
        this.nsService.setActive(this.namespace);
        needReload = true;
      }
      if (role) this.roleFilter = role;
      if (keyword) this.keyword = keyword;
      if (pod) this.pendingOpenPodName = pod;

      if (needReload) {
        this.load();
      } else {
        // If data already loaded, apply filter immediately.
        this.applyFilter();
        this.tryOpenPendingPod();
      }
      this.cdr.markForCheck();
    });

    this.nsService.namespaces$.pipe(takeUntil(this.destroy$)).subscribe(list => {
      this.namespaceOptions = list || [];
      this.cdr.markForCheck();
    });
    this.nsService.activeNamespace$.pipe(takeUntil(this.destroy$)).subscribe(ns => {
      if (ns && ns !== this.namespace) {
        this.namespace = ns;
        this.load();
      }
    });
    this.load();
    interval(30000).pipe(takeUntil(this.destroy$)).subscribe(() => this.load());
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  onNamespaceChange(ns: string): void {
    this.namespace = ns || 'default';
    this.nsService.setActive(this.namespace);
    this.load();
  }

  load(): void {
    this.loading = true;
    this.cdr.markForCheck();
    this.api.listPods(this.namespace).subscribe({
      next: (pods: Pod[]) => {
        const items: NodeItem[] = [];
        const roleStats = new Map<string, { ready: number; total: number; category: string }>();
        for (const p of pods) {
          const name = p.metadata?.name || '';
          const roleInfo = PodRoleDetector.detectRole(p);
          const phase = p.status?.phase || '未知';
          const ip = p.status?.podIP || '';
          const nodeName = p.spec?.nodeName || '';
          const restarts = this.getRestartCount(p);
          const age = this.calculateAge(p.metadata?.creationTimestamp);
          items.push({ name, role: roleInfo.role, roleInfo, status: phase, ip, nodeName, restarts, age, pod: p });
          const key = roleInfo.role;
          if (!roleStats.has(key)) roleStats.set(key, { ready: 0, total: 0, category: roleInfo.category });
          const stats = roleStats.get(key)!;
          stats.total++;
          if (phase?.toLowerCase() === 'running') stats.ready++;
        }
        this.all = items;
        this.applyFilter();
        this.summary = Array.from(roleStats.entries())
          .map(([role, stats]) => ({ role, ready: stats.ready, total: stats.total, category: stats.category, color: this.getCategoryColor(stats.category) }))
          .sort((a, b) => {
            const priorityOrder = ['compute', 'storage', 'service', 'monitor', 'unknown'];
            return priorityOrder.indexOf(a.category) - priorityOrder.indexOf(b.category) || a.role.localeCompare(b.role);
          });
        this.loading = false;
        this.tryOpenPendingPod();
        this.cdr.markForCheck();
      },
      error: () => {
        this.all = []; this.filtered = []; this.summary = [];
        this.loading = false;
        this.message.error('加载节点列表失败');
        this.cdr.markForCheck();
      }
    });
  }

  private tryOpenPendingPod(): void {
    const target = (this.pendingOpenPodName || '').trim();
    if (!target) return;
    // Navigate to node detail page directly (pods detail view).
    this.pendingOpenPodName = undefined;
    this.router.navigate(['/operations/nodes', this.namespace, target]);
  }

  applyFilter(): void {
    const kw = (this.keyword || '').toLowerCase();
    this.filtered = this.all.filter(n => (!this.roleFilter || n.role === this.roleFilter) && (!kw || n.name.toLowerCase().includes(kw) || n.ip.toLowerCase().includes(kw)));
    this.cdr.markForCheck();
  }

  getRestartCount(pod: Pod): number {
    return (pod.status?.containerStatuses || []).reduce((sum, cs) => sum + (cs.restartCount || 0), 0);
  }

  calculateAge(timestamp: string | undefined): string {
    if (!timestamp) return '-';
    const diffMs = Date.now() - new Date(timestamp).getTime();
    const diffDays = Math.floor(diffMs / 86400000);
    const diffHours = Math.floor((diffMs % 86400000) / 3600000);
    if (diffDays > 0) return diffDays + 'd ' + diffHours + 'h';
    const diffMinutes = Math.floor((diffMs % 3600000) / 60000);
    if (diffHours > 0) return diffHours + 'h ' + diffMinutes + 'm';
    return diffMinutes + 'm';
  }

  getRoleColor(roleInfo: PodRoleInfo): string {
    switch (roleInfo.category) {
      case 'compute': return 'blue';
      case 'storage': return 'green';
      case 'service': return 'orange';
      case 'monitor': return 'purple';
      default: return 'default';
    }
  }

  getCategoryColor(category: string): string {
    switch (category) {
      case 'compute': return '#1890ff';
      case 'storage': return '#52c41a';
      case 'service': return '#fa8c16';
      case 'monitor': return '#722ed1';
      default: return '#8c8c8c';
    }
  }

  getStatusBadge(status: string): 'success' | 'processing' | 'error' | 'default' | 'warning' {
    switch (status?.toLowerCase()) {
      case 'running': return 'success';
      case 'pending': return 'processing';
      case 'failed': return 'error';
      case 'succeeded': return 'default';
      default: return 'warning';
    }
  }

  getStatColor(s: RoleSummary): string {
    if (s.ready === s.total && s.total > 0) return '#52c41a';
    if (s.ready === 0) return '#ff4d4f';
    return '#faad14';
  }

  openDetail(node: NodeItem): void {
    this.router.navigate(['/operations/nodes', this.namespace, node.name]);
  }

  openTerminal(node: NodeItem): void {
    import('../../components/webshell-dialog/webshell-dialog.component').then(m => {
      const containers = (node.pod.spec?.containers || []).map(c => c.name);
      this.modal.create({
        nzTitle: '终端 - ' + node.name,
        nzContent: m.WebShellDialogComponent,
        nzWidth: 900,
        nzData: { namespace: this.namespace, pod: node.name, container: containers[0] || '', containers },
        nzFooter: null,
        nzBodyStyle: { padding: '0', height: '500px' }
      });
    });
  }

  openExec(node: NodeItem): void {
    import('../../components/exec-command-dialog/exec-command-dialog.component').then(m => {
      const containers = (node.pod.spec?.containers || []).map(c => c.name);
      this.modal.create({
        nzTitle: '执行命令 - ' + node.name,
        nzContent: m.ExecCommandDialogComponent,
        nzWidth: 700,
        nzData: { namespace: this.namespace, pod: node.name, containers, defaultContainer: containers[0] || '' },
        nzFooter: null
      });
    });
  }
}
