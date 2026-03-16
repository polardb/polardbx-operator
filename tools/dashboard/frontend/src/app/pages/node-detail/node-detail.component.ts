import { Component, OnInit, OnDestroy, inject, ChangeDetectionStrategy, ChangeDetectorRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ActivatedRoute, Router, RouterModule } from '@angular/router';
import { FormsModule } from '@angular/forms';

import { NzTabsModule } from 'ng-zorro-antd/tabs';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzInputNumberModule } from 'ng-zorro-antd/input-number';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzMessageModule, NzMessageService } from 'ng-zorro-antd/message';
import { NzModalModule, NzModalService } from 'ng-zorro-antd/modal';
import { NzDescriptionsModule } from 'ng-zorro-antd/descriptions';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzBadgeModule } from 'ng-zorro-antd/badge';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzAlertModule } from 'ng-zorro-antd/alert';

import { ApiService } from '../../services/api.service';
import { Pod } from '../../models/pod.model';
import { Subject, takeUntil } from 'rxjs';
import { WebShellDialogComponent } from '../../components/webshell-dialog/webshell-dialog.component';

@Component({
  selector: 'app-node-detail-page',
  standalone: true,
  imports: [
    CommonModule, RouterModule, FormsModule,
    NzTabsModule, NzButtonModule, NzIconModule, NzSelectModule, NzInputModule, NzInputNumberModule,
    NzToolTipModule, NzCardModule, NzMessageModule, NzModalModule, NzDescriptionsModule,
    NzTagModule, NzSpinModule, NzDividerModule, NzBadgeModule, NzGridModule, NzAlertModule,
    WebShellDialogComponent,
  ],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="node-detail-page">
      <div class="page-header">
        <div class="header-left">
          <button nz-button nzType="text" (click)="goBack()" nz-tooltip nzTooltipTitle="返回列表">
            <i nz-icon nzType="arrow-left"></i>
          </button>
          <div class="header-title">
            <h1>节点详情</h1>
            <p class="breadcrumb">{{ namespace }} / {{ podName }}</p>
          </div>
        </div>
        <div class="header-actions">
          <button nz-button nzType="primary" (click)="showTerminalTab()"><i nz-icon nzType="code"></i> 终端</button>
          <button nz-button nzType="default" (click)="openExec()"><i nz-icon nzType="play-circle"></i> 执行命令</button>
        </div>
      </div>

      <nz-spin [nzSpinning]="loading">
        <nz-tabset [nzAnimated]="true" [nzTabPosition]="'top'" [(nzSelectedIndex)]="selectedTabIndex">
          <nz-tab nzTitle="概览">
            <nz-card class="detail-card" *ngIf="pod as p">
              <nz-descriptions nzTitle="基本信息" nzBordered [nzColumn]="2">
                <nz-descriptions-item nzTitle="命名空间">{{ namespace }}</nz-descriptions-item>
                <nz-descriptions-item nzTitle="Pod 名称">{{ p.metadata.name }}</nz-descriptions-item>
                <nz-descriptions-item nzTitle="Pod IP"><code class="code-text">{{ p.status?.podIP || '未分配' }}</code></nz-descriptions-item>
                <nz-descriptions-item nzTitle="节点">{{ p.spec.nodeName || '未知' }}</nz-descriptions-item>
                <nz-descriptions-item nzTitle="状态"><nz-badge [nzStatus]="getStatusBadge(p.status?.phase)" [nzText]="p.status?.phase || '未知'"></nz-badge></nz-descriptions-item>
                <nz-descriptions-item nzTitle="重启次数"><span [class.restart-warning]="getRestartCount() > 0">{{ getRestartCount() }}</span></nz-descriptions-item>
                <nz-descriptions-item nzTitle="创建时间" [nzSpan]="2">{{ p.metadata.creationTimestamp | date:'yyyy-MM-dd HH:mm:ss' }}</nz-descriptions-item>
              </nz-descriptions>
              <nz-divider></nz-divider>
              <nz-descriptions nzTitle="容器列表" nzBordered [nzColumn]="1">
                <nz-descriptions-item nzTitle="容器"><nz-tag *ngFor="let c of containers" [nzColor]="'blue'">{{ c }}</nz-tag></nz-descriptions-item>
              </nz-descriptions>
            </nz-card>
            <nz-alert *ngIf="!pod && !loading" nzType="warning" nzMessage="无法加载 Pod 信息"></nz-alert>
          </nz-tab>

          <nz-tab nzTitle="日志">
            <nz-card class="logs-card">
              <div class="logs-toolbar">
                <div class="toolbar-item">
                  <label>容器</label>
                  <nz-select [(ngModel)]="selectedContainer" (ngModelChange)="loadLogs()" style="width: 200px;">
                    <nz-option *ngFor="let c of containers" [nzValue]="c" [nzLabel]="c"></nz-option>
                  </nz-select>
                </div>
                <div class="toolbar-item">
                  <label>行数</label>
                  <nz-input-number [(ngModel)]="tailLines" [nzMin]="100" [nzMax]="10000" [nzStep]="100" style="width: 120px;"></nz-input-number>
                </div>
                <div class="toolbar-actions">
                  <button nz-button nzType="default" nzSize="small" (click)="loadLogs()" nz-tooltip nzTooltipTitle="刷新"><i nz-icon nzType="reload"></i></button>
                  <button nz-button nzType="default" nzSize="small" (click)="copyLogs()" nz-tooltip nzTooltipTitle="复制"><i nz-icon nzType="copy"></i></button>
                  <button nz-button nzType="default" nzSize="small" (click)="downloadLogs()" nz-tooltip nzTooltipTitle="下载"><i nz-icon nzType="download"></i></button>
                </div>
              </div>
              <pre class="logs-content">{{ logs || '无日志或未选择容器' }}</pre>
            </nz-card>
          </nz-tab>

          <nz-tab nzTitle="JSON">
            <nz-card class="inspect-card">
              <div class="inspect-toolbar">
                <button nz-button nzType="default" nzSize="small" (click)="copyInspect()" nz-tooltip nzTooltipTitle="复制 JSON"><i nz-icon nzType="copy"></i> 复制</button>
              </div>
              <pre class="inspect-content">{{ inspectJson }}</pre>
            </nz-card>
          </nz-tab>

          <nz-tab nzTitle="Kubectl">
            <nz-card class="kubectl-card">
              <div class="cmd-section">
                <h4>Describe Pod</h4>
                <div class="cmd-row">
                  <code class="cmd-code">kubectl describe pod {{ podName }} -n {{ namespace }}</code>
                  <button nz-button nzType="link" nzSize="small" (click)="copy('kubectl describe pod ' + podName + ' -n ' + namespace)"><i nz-icon nzType="copy"></i></button>
                </div>
              </div>
              <nz-divider></nz-divider>
              <div class="cmd-section">
                <h4>查看日志</h4>
                <div class="cmd-row">
                  <code class="cmd-code">kubectl logs {{ podName }} -n {{ namespace }} -c {{ selectedContainer || containers[0] }}</code>
                  <button nz-button nzType="link" nzSize="small" (click)="copy('kubectl logs ' + podName + ' -n ' + namespace + ' -c ' + (selectedContainer || containers[0]))"><i nz-icon nzType="copy"></i></button>
                </div>
              </div>
              <nz-divider></nz-divider>
              <div class="cmd-section">
                <h4>进入容器</h4>
                <div class="cmd-row">
                  <code class="cmd-code">kubectl exec -it {{ podName }} -n {{ namespace }} -c {{ selectedContainer || containers[0] }} -- /bin/sh</code>
                  <button nz-button nzType="link" nzSize="small" (click)="copy('kubectl exec -it ' + podName + ' -n ' + namespace + ' -c ' + (selectedContainer || containers[0]) + ' -- /bin/sh')"><i nz-icon nzType="copy"></i></button>
                </div>
              </div>
              <nz-divider></nz-divider>
              <div class="cmd-section">
                <h4>删除 Pod (重启)</h4>
                <div class="cmd-row">
                  <code class="cmd-code cmd-danger">kubectl delete pod {{ podName }} -n {{ namespace }}</code>
                  <button nz-button nzType="link" nzSize="small" nzDanger (click)="copy('kubectl delete pod ' + podName + ' -n ' + namespace)"><i nz-icon nzType="copy"></i></button>
                </div>
              </div>
            </nz-card>
          </nz-tab>

          <nz-tab nzTitle="终端">
            <nz-card class="terminal-card">
              <ng-container *ngIf="containers.length > 0; else terminalNotReady">
                <app-webshell-dialog
                  [embedded]="true"
                  [namespace]="namespace"
                  [pod]="podName"
                  [containers]="containers"
                  [container]="selectedContainer || containers[0]">
                </app-webshell-dialog>
              </ng-container>
              <ng-template #terminalNotReady>
                <nz-alert nzType="info" nzShowIcon nzMessage="终端尚未就绪" nzDescription="正在加载 Pod/容器信息，请稍后重试。"></nz-alert>
              </ng-template>
              <div class="terminal-actions">
                <button nz-button nzType="default" (click)="openTerminal()">
                  <i nz-icon nzType="code"></i>
                  弹窗打开（遇到尺寸/连接问题时使用）
                </button>
              </div>
            </nz-card>
          </nz-tab>
        </nz-tabset>
      </nz-spin>
    </div>
  `,
  styles: [`
    .node-detail-page { padding: 24px; background: #f0f2f5; min-height: 100vh; }
    .page-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 24px; background: white; padding: 16px 24px; border-radius: 8px; }
    .header-left { display: flex; align-items: center; gap: 16px; }
    .header-title h1 { font-size: 20px; font-weight: 600; margin: 0; color: rgba(0, 0, 0, 0.88); }
    .breadcrumb { font-size: 13px; color: rgba(0, 0, 0, 0.45); margin: 4px 0 0 0; }
    .header-actions { display: flex; gap: 12px; }
    .detail-card, .logs-card, .inspect-card, .kubectl-card, .terminal-card { border-radius: 8px; }
    .code-text { font-family: monospace; background: #f5f5f5; padding: 2px 8px; border-radius: 4px; }
    .restart-warning { color: #faad14; font-weight: 600; }
    .logs-toolbar, .inspect-toolbar { display: flex; align-items: flex-end; gap: 16px; margin-bottom: 16px; flex-wrap: wrap; }
    .toolbar-item { display: flex; flex-direction: column; gap: 6px; }
    .toolbar-item label { font-size: 13px; color: rgba(0, 0, 0, 0.65); }
    .toolbar-actions { display: flex; gap: 8px; margin-left: auto; }
    .logs-content, .inspect-content { background: #0d1117; color: #c9d1d9; padding: 16px; border-radius: 8px; min-height: 400px; max-height: 600px; overflow: auto; font-family: monospace; font-size: 13px; line-height: 1.5; white-space: pre-wrap; word-break: break-all; }
    .cmd-section h4 { font-size: 14px; font-weight: 500; color: rgba(0, 0, 0, 0.85); margin: 0 0 12px 0; }
    .cmd-row { display: flex; align-items: center; gap: 12px; }
    .cmd-code { flex: 1; font-family: monospace; font-size: 13px; background: #f6f8fa; padding: 8px 12px; border-radius: 6px; border: 1px solid #e1e4e8; }
    .cmd-danger { background: #fff2f0; border-color: #ffccc7; }
    .terminal-card { min-height: 500px; }
    .terminal-actions { display: flex; justify-content: center; margin-top: 12px; }
    ::ng-deep .ant-tabs-nav { margin-bottom: 0; background: white; padding: 0 16px; border-radius: 8px 8px 0 0; }
    ::ng-deep .ant-tabs-content { background: white; padding: 24px; border-radius: 0 0 8px 8px; }
  `]
})
export class NodeDetailComponent implements OnInit, OnDestroy {
  private readonly route = inject(ActivatedRoute);
  private readonly router = inject(Router);
  private readonly api = inject(ApiService);
  private readonly message = inject(NzMessageService);
  private readonly modal = inject(NzModalService);
  private readonly cdr = inject(ChangeDetectorRef);
  private readonly destroy$ = new Subject<void>();

  namespace = 'default';
  podName = '';
  pod: Pod | null = null;
  containers: string[] = [];
  selectedContainer = '';
  tailLines = 1000;
  logs = '';
  inspectJson = '';
  loading = false;
  selectedTabIndex = 0;

  ngOnInit(): void {
    // Allow opening a specific tab via query param, e.g.:
    // /operations/nodes/:namespace/:name?tab=logs|json|kubectl|terminal|overview
    this.route.queryParamMap.pipe(takeUntil(this.destroy$)).subscribe(pm => {
      const tab = (pm.get('tab') || '').toLowerCase();
      this.selectedTabIndex =
        tab === 'logs' ? 1 :
        tab === 'json' ? 2 :
        tab === 'kubectl' ? 3 :
        tab === 'terminal' ? 4 :
        0;
      this.cdr.markForCheck();
    });

    this.route.params.pipe(takeUntil(this.destroy$)).subscribe(p => {
      this.namespace = p['namespace'] || 'default';
      this.podName = p['name'] || '';
      this.load();
    });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  goBack(): void {
    this.router.navigate(['/operations/nodes']);
  }

  showTerminalTab(): void {
    this.selectedTabIndex = 4;
    this.cdr.markForCheck();
  }

  load(): void {
    this.loading = true;
    this.cdr.markForCheck();
    this.api.getPod(this.namespace, this.podName).subscribe({
      next: (pod) => {
        this.pod = pod;
        this.containers = (pod.spec?.containers || []).map(c => c.name);
        this.selectedContainer = this.pickBestContainer(this.containers, this.selectedContainer);
        this.inspectJson = JSON.stringify(pod, null, 2);
        this.loading = false;
        this.cdr.markForCheck();
        this.loadLogs();
      },
      error: (err) => {
        this.loading = false;
        this.message.error('加载 Pod 失败: ' + err.message);
        this.cdr.markForCheck();
      }
    });
  }

  loadLogs(): void {
    if (!this.selectedContainer) { this.logs = ''; return; }
    this.api.getPodLogs(this.namespace, this.podName, this.selectedContainer, this.tailLines).subscribe({
      next: (text) => { this.logs = text || ''; this.cdr.markForCheck(); },
      error: (err) => {
        const cause = err?.error?.error?.cause || err?.error?.error?.message || err?.error?.message || err?.message || '(获取日志失败)';
        this.logs = `(获取日志失败)\n${cause}`;
        this.cdr.markForCheck();

        // If the selected container is unavailable, try a fallback container once.
        const msg = (cause || '').toString();
        if (msg.includes('is not available') && this.containers.length > 1) {
          const fallback = this.containers.find(c => c !== this.selectedContainer && c !== 'engine') || this.containers.find(c => c !== this.selectedContainer) || '';
          if (fallback) {
            this.selectedContainer = fallback;
            this.cdr.markForCheck();
            this.api.getPodLogs(this.namespace, this.podName, this.selectedContainer, this.tailLines).subscribe({
              next: (text) => { this.logs = text || ''; this.cdr.markForCheck(); },
              error: () => { /* keep original error */ }
            });
          }
        }
      }
    });
  }

  getRestartCount(): number {
    return (this.pod?.status?.containerStatuses || []).reduce((sum, cs) => sum + (cs.restartCount || 0), 0);
  }

  getStatusBadge(status: string | undefined): 'success' | 'processing' | 'error' | 'default' | 'warning' {
    switch (status?.toLowerCase()) {
      case 'running': return 'success';
      case 'pending': return 'processing';
      case 'failed': return 'error';
      case 'succeeded': return 'default';
      default: return 'warning';
    }
  }

  copy(text: string): void {
    navigator.clipboard.writeText(text).then(() => this.message.success('已复制到剪贴板')).catch(() => this.message.error('复制失败'));
  }

  copyLogs(): void { if (this.logs) this.copy(this.logs); }
  copyInspect(): void { if (this.inspectJson) this.copy(this.inspectJson); }

  downloadLogs(): void {
    const blob = new Blob([this.logs || ''], { type: 'text/plain;charset=utf-8' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = this.podName + '-' + (this.selectedContainer || 'container') + '.log';
    a.click();
    URL.revokeObjectURL(a.href);
  }

  private pickBestContainer(list: string[], prefer?: string): string {
    const items = (list || []).filter(Boolean);
    if (items.length === 0) return '';
    const lower = (s: string) => (s || '').toLowerCase();
    const negatives = ['prober', 'probe', 'exporter', 'agent', 'sidecar', 'pause', 'proxy', 'reloader', 'metrics', 'prom', 'istio', 'linkerd'];
    const positivesExact = ['engine', 'mysql', 'xstore', 'server', 'main', 'app', 'dn', 'cn', 'gms', 'cdc'];
    // Prefer containers that are actually running (avoid picking "engine" when it is terminated/unavailable).
    const runningSet = new Set((this.pod?.status?.containerStatuses || [])
      .filter(cs => !!cs?.state?.running)
      .map(cs => cs.name));
    for (const p of positivesExact) {
      const hit = items.find(c => lower(c) === p && runningSet.has(c));
      if (hit) return hit;
    }
    if (prefer && items.includes(prefer) && !negatives.some(n => lower(prefer).includes(n))) return prefer;
    for (const p of positivesExact) { const hit = items.find(c => lower(c) === p); if (hit) return hit; }
    const nonNeg = items.find(c => !negatives.some(n => lower(c).includes(n)));
    return nonNeg || items[0];
  }

  openTerminal(): void {
    import('../../components/webshell-dialog/webshell-dialog.component').then(m => {
      this.modal.create({
        nzTitle: '终端 - ' + this.podName,
        nzContent: m.WebShellDialogComponent,
        nzWidth: 900,
        nzData: { namespace: this.namespace, pod: this.podName, container: this.selectedContainer, containers: this.containers },
        nzFooter: null,
        nzBodyStyle: { padding: '0', height: '500px' }
      });
    });
  }

  openExec(): void {
    import('../../components/exec-command-dialog/exec-command-dialog.component').then(m => {
      this.modal.create({
        nzTitle: '执行命令 - ' + this.podName,
        nzContent: m.ExecCommandDialogComponent,
        nzWidth: 700,
        nzData: { namespace: this.namespace, pod: this.podName, containers: this.containers, defaultContainer: this.selectedContainer },
        nzFooter: null
      });
    });
  }
}
