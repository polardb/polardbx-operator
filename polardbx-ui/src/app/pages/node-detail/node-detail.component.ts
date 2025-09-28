import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ActivatedRoute, Router, RouterModule } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { MatTabsModule } from '@angular/material/tabs';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatInputModule } from '@angular/material/input';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatCardModule } from '@angular/material/card';
import { MatSnackBar, MatSnackBarModule } from '@angular/material/snack-bar';
import { MatDialog, MatDialogModule } from '@angular/material/dialog';

import { ApiService } from '../../services/api.service';
import { WebShellInlineComponent } from '../../components/webshell-inline/webshell-inline.component';
import { Pod } from '../../models/pod.model';

@Component({
  selector: 'app-node-detail-page',
  standalone: true,
  imports: [
    CommonModule, RouterModule, FormsModule,
    MatTabsModule, MatButtonModule, MatIconModule,
    MatFormFieldModule, MatSelectModule, MatInputModule, MatTooltipModule,
    MatCardModule, MatSnackBarModule, MatDialogModule,
    WebShellInlineComponent
  ],
  template: `
  <div class="page">
    <div class="header">
      <div class="title">
        <button mat-icon-button (click)="goBack()" matTooltip="返回列表"><mat-icon>arrow_back</mat-icon></button>
        <div class="texts">
          <div class="h1">节点详情</div>
          <div class="sub">{{ namespace }} / {{ podName }}</div>
        </div>
      </div>
      <div class="header-actions">
        <button mat-stroked-button color="primary" (click)="openTerminal()"><mat-icon>computer</mat-icon> 打开终端</button>
        <button mat-stroked-button (click)="openExec()"><mat-icon>play_arrow</mat-icon> 执行命令</button>
      </div>
    </div>

    <mat-tab-group animationDuration="0ms">
      <mat-tab label="Summary">
        <div class="kv-grid" *ngIf="pod as p; else loading">
          <div class="k">命名空间</div><div class="v">{{ namespace }}</div>
          <div class="k">Pod</div><div class="v">{{ p.metadata.name }}</div>
          <div class="k">IP</div><div class="v">{{ p.status?.podIP || '未知' }}</div>
          <div class="k">节点</div><div class="v">{{ p.spec.nodeName || '未知' }}</div>
          <div class="k">Phase</div><div class="v">{{ p.status?.phase }}</div>
          <div class="k">容器</div><div class="v">
            <span class="chip" *ngFor="let c of containers">{{ c }}</span>
          </div>
        </div>
        <ng-template #loading>
          <div class="loading">加载中...</div>
        </ng-template>
      </mat-tab>

      <mat-tab label="Logs">
        <div class="toolbar">
          <mat-form-field appearance="outline" class="w-200">
            <mat-label>容器</mat-label>
            <mat-select [(ngModel)]="selectedContainer" [disableOptionCentering]="true" panelClass="force-above-panel">
              <mat-option *ngFor="let c of containers" [value]="c">{{ c }}</mat-option>
            </mat-select>
          </mat-form-field>
          <mat-form-field appearance="outline" class="w-140">
            <mat-label>Tail</mat-label>
            <input matInput type="number" [(ngModel)]="tailLines"/>
          </mat-form-field>
          <button mat-icon-button (click)="loadLogs()" matTooltip="刷新"><mat-icon>refresh</mat-icon></button>
          <button mat-icon-button (click)="copyLogs()" matTooltip="复制"><mat-icon>content_copy</mat-icon></button>
          <button mat-icon-button (click)="downloadLogs()" matTooltip="下载"><mat-icon>download</mat-icon></button>
        </div>
        <pre class="logs">{{ logs || '无日志或未选择容器' }}</pre>
      </mat-tab>

      <mat-tab label="Inspect">
        <pre class="inspect">{{ inspectJson }}</pre>
      </mat-tab>

      <mat-tab label="Kube">
        <div class="cmds">
          <div class="cmd-row">
            <div class="label">Describe</div>
            <div class="code">kubectl describe pod {{ podName }} -n {{ namespace }}</div>
            <button mat-mini-button (click)="copy('kubectl describe pod ' + podName + ' -n ' + namespace)"><mat-icon>content_copy</mat-icon></button>
          </div>
          <div class="cmd-row">
            <div class="label">Logs</div>
            <div class="code">kubectl logs {{ podName }} -n {{ namespace }} -c {{ selectedContainer || containers[0] }}</div>
            <button mat-mini-button (click)="copy('kubectl logs ' + podName + ' -n ' + namespace + ' -c ' + (selectedContainer || containers[0]))"><mat-icon>content_copy</mat-icon></button>
          </div>
          <div class="cmd-row">
            <div class="label">Exec</div>
            <div class="code">kubectl exec -it {{ podName }} -n {{ namespace }} -c {{ selectedContainer || containers[0] }} -- /bin/sh</div>
            <button mat-mini-button (click)="copy('kubectl exec -it ' + podName + ' -n ' + namespace + ' -c ' + (selectedContainer || containers[0]) + ' -- /bin/sh')"><mat-icon>content_copy</mat-icon></button>
          </div>
        </div>
      </mat-tab>

      <mat-tab label="Terminal">
        <app-webshell-inline [namespace]="namespace" [pod]="podName" [containers]="containers" [container]="selectedContainer" />
      </mat-tab>
    </mat-tab-group>
  </div>
  `,
  styles: [`
    .page { padding: 16px; }
    .header { display:flex; align-items:center; justify-content: space-between; margin-bottom: 12px; }
    .title { display:flex; align-items:center; gap:8px; }
    .texts .h1 { font-size: 20px; font-weight: 600; }
    .texts .sub { color:#666; font-size: 12px; margin-top:2px; }
    .header-actions { display:flex; gap:8px; }
    .kv-grid { display:grid; grid-template-columns: 120px 1fr; row-gap:8px; column-gap:12px; font-size:13px; }
    .k { color:#666; }
    .chip { display:inline-block; background:#eef2f7; padding:2px 8px; border-radius:10px; margin-right:6px; }
    .toolbar { 
      display:flex; 
      align-items:center; 
      gap:8px; 
      margin: 8px 0; 
      position: relative; 
      z-index: 10; 
    }
    .w-200 { width:200px; }
    .w-140 { width:140px; }
    .logs, .inspect { 
      background:#0b1020; 
      color:#d6e4ff; 
      padding:12px; 
      border-radius:6px; 
      min-height:320px; 
      max-height:520px; 
      overflow:auto; 
      position: relative; 
      z-index: 1; 
    }
    .cmds { display:flex; flex-direction:column; gap:8px; }
    .cmd-row { display:grid; grid-template-columns: 90px 1fr auto; align-items:center; gap:8px; }
    .code { font-family: Menlo, monospace; font-size: 12px; background:#f6f8fa; padding:6px 8px; border-radius:4px; }
    .hint { font-size:12px; color:#666; margin-top:12px; }
    .loading { padding: 24px; color:#666; }

    /* 强制下拉面板显示在上方，避免被遮挡 */
    ::ng-deep .force-above-panel {
      z-index: 100000 !important;
      position: fixed !important;
      pointer-events: auto !important;
    }
    
    /* 确保在此页面内，下拉列表始终显示在最上层 */
    ::ng-deep .cdk-overlay-container {
      z-index: 100000 !important;
      pointer-events: none !important;
    }
    ::ng-deep .cdk-overlay-pane {
      z-index: 100000 !important;
      position: fixed !important;
      pointer-events: auto !important;
    }
    ::ng-deep .mat-mdc-select-panel {
      z-index: 100000 !important;
      position: fixed !important;
      pointer-events: auto !important;
      background: white !important;
      box-shadow: 0 8px 24px rgba(0,0,0,0.3) !important;
    }
    ::ng-deep .mat-mdc-option {
      z-index: 100000 !important;
      pointer-events: auto !important;
    }
    
    /* 专门针对 Logs 和 Terminal 标签页中的下拉框 */
    ::ng-deep mat-tab-group .mat-mdc-select-panel {
      z-index: 100000 !important;
      background: white !important;
      border: 1px solid #ddd !important;
    }
    
    /* 特别处理容器选择下拉框 */
    ::ng-deep .toolbar .mat-mdc-form-field {
      z-index: 50 !important;
      position: relative !important;
    }
    ::ng-deep .toolbar .mat-mdc-form-field .cdk-overlay-pane {
      z-index: 100000 !important;
      position: fixed !important;
      pointer-events: auto !important;
    }
    
    /* 确保深色背景区域不会覆盖下拉框 */
    .logs, .inspect {
      position: relative;
      z-index: 1 !important;
    }
    
    /* WebShell 组件的 z-index 控制 */
    ::ng-deep app-webshell-inline {
      position: relative;
      z-index: 1 !important;
    }
    ::ng-deep app-webshell-inline .mat-mdc-select-panel {
      z-index: 100000 !important;
      background: white !important;
      box-shadow: 0 8px 24px rgba(0,0,0,0.3) !important;
    }
  `]
})
export class NodeDetailComponent implements OnInit {
  private route = inject(ActivatedRoute);
  private router = inject(Router);
  private api = inject(ApiService);
  private snack = inject(MatSnackBar);
  private dialog = inject(MatDialog);

  namespace = 'default';
  podName = '';
  pod: Pod | null = null;
  containers: string[] = [];
  selectedContainer = '';
  tailLines = 1000;
  logs = '';
  inspectJson = '';

  ngOnInit(): void {
    this.route.params.subscribe(p => {
      this.namespace = p['namespace'] || 'default';
      this.podName = p['name'] || '';
      this.load();
    });
  }

  goBack(): void {
    this.router.navigate(['/operations/nodes']);
  }

  load(): void {
    this.api.getPod(this.namespace, this.podName).subscribe({
      next: (pod) => {
        this.pod = pod;
        this.containers = (pod.spec.containers || []).map(c => c.name);
        this.selectedContainer = this.pickBestContainerFromList(this.containers, this.selectedContainer);
        this.inspectJson = JSON.stringify(pod, null, 2);
        this.loadLogs();
      },
      error: (err) => {
        this.snack.open(`加载 Pod 失败: ${err.message}`, '关闭', { duration: 4000 });
      }
    });
  }

  loadLogs(): void {
    if (!this.selectedContainer) { this.logs = ''; return; }
    this.api.getPodLogs(this.namespace, this.podName, this.selectedContainer, this.tailLines).subscribe({
      next: (text) => this.logs = text || '',
      error: () => this.logs = '(获取日志失败)'
    });
  }

  copy(text: string): void { navigator.clipboard.writeText(text).catch(() => {}); }
  copyLogs(): void { if (this.logs) navigator.clipboard.writeText(this.logs).catch(()=>{}); }
  downloadLogs(): void {
    const blob = new Blob([this.logs || ''], { type: 'text/plain;charset=utf-8' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `${this.podName}-${this.selectedContainer||'container'}.log`;
    a.click();
    URL.revokeObjectURL(a.href);
  }

  private pickBestContainerFromList(list: string[], prefer?: string): string {
    const items = (list || []).filter(Boolean);
    if (items.length === 0) return '';
    const lower = (s: string) => (s || '').toLowerCase();
    const negatives = ['prober','probe','exporter','agent','sidecar','pause','proxy','reloader','metrics','prom','istio','linkerd','kube-rbac-proxy','configmap-reload','reloader'];
    const positivesExact = ['engine','mysql','xstore','server','main','app','dn','cn','gms','cdc'];
    const positivesContains = ['engine','mysql','xstore','server','main','app','dn-','cn-','gms','cdc'];

    if (prefer && items.some(c => c === prefer) && !negatives.some(n => lower(prefer).includes(n))) {
      return prefer;
    }
    for (const p of positivesExact) {
      const hit = items.find(c => lower(c) === p);
      if (hit) return hit;
    }
    for (const p of positivesContains) {
      const hit = items.find(c => lower(c).includes(p));
      if (hit) return hit;
    }
    const nonNeg = items.find(c => !negatives.some(n => lower(c).includes(n)));
    if (nonNeg) return nonNeg;
    return items[0];
  }

  openTerminal(): void {
    import('../../components/webshell-dialog/webshell-dialog.component').then(m => {
      this.dialog.open(m.WebShellDialogComponent, {
        width: '900px', height: '600px',
        data: { namespace: this.namespace, pod: this.podName, container: this.selectedContainer, containers: this.containers }
      });
    });
  }

  openExec(): void {
    import('../../components/exec-command-dialog/exec-command-dialog.component').then(m => {
      this.dialog.open(m.ExecCommandDialogComponent, {
        width: '700px', data: { namespace: this.namespace, pod: this.podName, containers: this.containers, defaultContainer: this.selectedContainer }
      });
    });
  }
}

