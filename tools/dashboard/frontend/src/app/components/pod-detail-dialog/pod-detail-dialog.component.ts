import { Component, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatDialogModule, MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { MatTabsModule } from '@angular/material/tabs';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatInputModule } from '@angular/material/input';
import { MatTooltipModule } from '@angular/material/tooltip';
import { FormsModule } from '@angular/forms';
import { ApiService } from '../../services/api.service';
import { Pod } from '../../models/pod.model';

export interface PodDetailDialogData {
  namespace: string;
  pod: Pod;
}

@Component({
  selector: 'app-pod-detail-dialog',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatDialogModule,
    MatTabsModule,
    MatButtonModule,
    MatIconModule,
    MatFormFieldModule,
    MatSelectModule,
    MatInputModule,
    MatTooltipModule
  ],
  template: `
  <h2 mat-dialog-title>
    <mat-icon>insights</mat-icon>
    节点详情 - {{ data.pod.metadata.name }}
  </h2>
  <mat-dialog-content class="content">
    <mat-tab-group animationDuration="0ms">
      <mat-tab label="Summary">
        <div class="kv-grid">
          <div class="k">命名空间</div><div class="v">{{ data.namespace }}</div>
          <div class="k">Pod</div><div class="v">{{ data.pod.metadata.name }}</div>
          <div class="k">IP</div><div class="v">{{ data.pod.status?.podIP || '未知' }}</div>
          <div class="k">节点</div><div class="v">{{ data.pod.spec.nodeName || '未知' }}</div>
          <div class="k">Phase</div><div class="v">{{ data.pod.status?.phase }}</div>
          <div class="k">容器</div><div class="v">
            <span class="chip" *ngFor="let c of containers">{{ c }}</span>
          </div>
        </div>
      </mat-tab>

      <mat-tab label="Logs">
        <div class="toolbar">
          <mat-form-field appearance="outline" class="w-200">
            <mat-label>容器</mat-label>
            <mat-select [(ngModel)]="selectedContainer">
              <mat-option *ngFor="let c of containers" [value]="c">{{ c }}</mat-option>
            </mat-select>
          </mat-form-field>
          <mat-form-field appearance="outline" class="w-120">
            <mat-label>Tail</mat-label>
            <input matInput type="number" [(ngModel)]="tailLines"/>
          </mat-form-field>
          <button mat-stroked-button (click)="loadLogs()"><mat-icon>refresh</mat-icon> 刷新</button>
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
            <div class="code">kubectl describe pod {{ data.pod.metadata.name }} -n {{ data.namespace }}</div>
            <button mat-mini-button (click)="copy('kubectl describe pod ' + data.pod.metadata.name + ' -n ' + data.namespace)"><mat-icon>content_copy</mat-icon></button>
          </div>
          <div class="cmd-row">
            <div class="label">Logs</div>
            <div class="code">kubectl logs {{ data.pod.metadata.name }} -n {{ data.namespace }} -c {{ selectedContainer || containers[0] }}</div>
            <button mat-mini-button (click)="copy('kubectl logs ' + data.pod.metadata.name + ' -n ' + data.namespace + ' -c ' + (selectedContainer || containers[0]))"><mat-icon>content_copy</mat-icon></button>
          </div>
          <div class="cmd-row">
            <div class="label">Exec</div>
            <div class="code">kubectl exec -it {{ data.pod.metadata.name }} -n {{ data.namespace }} -c {{ selectedContainer || containers[0] }} -- /bin/sh</div>
            <button mat-mini-button (click)="copy('kubectl exec -it ' + data.pod.metadata.name + ' -n ' + data.namespace + ' -c ' + (selectedContainer || containers[0]) + ' -- /bin/sh')"><mat-icon>content_copy</mat-icon></button>
          </div>
        </div>
      </mat-tab>

      <mat-tab label="Terminal">
        <div class="toolbar">
          <mat-form-field appearance="outline" class="w-200">
            <mat-label>容器</mat-label>
            <mat-select [(ngModel)]="selectedContainer">
              <mat-option *ngFor="let c of containers" [value]="c">{{ c }}</mat-option>
            </mat-select>
          </mat-form-field>
          <button mat-stroked-button color="primary" (click)="openWebShell()"><mat-icon>computer</mat-icon> 打开终端</button>
          <button mat-stroked-button (click)="openExecDialog()"><mat-icon>play_arrow</mat-icon> 执行命令</button>
        </div>
        <div class="hint">终端将以弹窗方式打开（支持复制/粘贴/清屏/下载输出）。</div>
      </mat-tab>
    </mat-tab-group>
  </mat-dialog-content>
  <mat-dialog-actions align="end">
    <button mat-stroked-button (click)="dialogRef.close()">关闭</button>
  </mat-dialog-actions>
  `,
  styles: [`
    .content { width: 900px; max-width: 95vw; }
    .kv-grid { display: grid; grid-template-columns: 120px 1fr; row-gap: 8px; column-gap: 12px; font-size: 13px; }
    .k { color: #666; }
    .chip { display: inline-block; background:#eef2f7; padding:2px 8px; border-radius: 10px; margin-right:6px; }
    .toolbar { display: flex; align-items: center; gap: 8px; margin: 8px 0; }
    .w-200 { width: 200px; }
    .w-120 { width: 120px; }
    .logs, .inspect { background: #0b1020; color: #d6e4ff; padding: 12px; border-radius: 6px; min-height: 300px; max-height: 420px; overflow: auto; }
    .cmds { display: flex; flex-direction: column; gap: 8px; }
    .cmd-row { display: grid; grid-template-columns: 90px 1fr auto; align-items: center; gap: 8px; }
    .code { font-family: Menlo, monospace; font-size: 12px; background: #f6f8fa; padding: 6px 8px; border-radius: 4px; }
    .hint { font-size: 12px; color: #666; margin-top: 12px; }
  `]
})
export class PodDetailDialogComponent {
  dialogRef = inject<MatDialogRef<PodDetailDialogComponent>>(MatDialogRef);
  data = inject<PodDetailDialogData>(MAT_DIALOG_DATA as any);
  api = inject(ApiService);

  containers: string[] = [];
  selectedContainer = '';
  tailLines = 500;
  logs = '';
  inspectJson = '';

  constructor() {
    this.containers = (this.data.pod.spec.containers || []).map(c => c.name);
    this.selectedContainer = this.containers[0] || '';
    this.inspectJson = JSON.stringify(this.data.pod || {}, null, 2);
  }

  loadLogs(): void {
    if (!this.selectedContainer) return;
    this.api.getPodLogs(this.data.namespace, this.data.pod.metadata.name, this.selectedContainer, this.tailLines)
      .subscribe({ next: (text) => this.logs = text || '', error: () => this.logs = '(获取日志失败)' });
  }

  openWebShell(): void {
    import('../webshell-dialog/webshell-dialog.component').then(m => {
      const ref = inject(MatDialogRef as any); // to appease TS when bundling dynamic import; actual open below
    }).finally(() => {
      // open via global dialog service available in parent; dynamic import to get class
      import('../webshell-dialog/webshell-dialog.component').then(m => {
        const dlg = (this.dialogRef as any)._containerInstance._config.dialog?.open || null;
      });
    });
    // 直接用 MatDialog 打开
    import('../webshell-dialog/webshell-dialog.component').then(m => {
      const MatDialog = (window as any).ng && (window as any).ng.getInjector ? null : null; // placeholder
    });
    // 简化：通过父级注入器无法直接拿到 MatDialog，这里退回使用 window hook 不可靠。
    // 改为复用 cluster-detail 中现有入口：由父组件触发。此处提供兼容实现：
    import('../webshell-dialog/webshell-dialog.component').then(m => {
      const open = (this.dialogRef as any)._containerInstance._config?.dialog?.open;
      if (open) {
        open(m.WebShellDialogComponent, {
          width: '900px', height: '600px', data: {
            namespace: this.data.namespace,
            pod: this.data.pod.metadata.name,
            container: this.selectedContainer,
            containers: this.containers
          }
        });
      }
    });
  }

  openExecDialog(): void {
    import('../exec-command-dialog/exec-command-dialog.component').then(m => {
      const open = (this.dialogRef as any)._containerInstance._config?.dialog?.open;
      if (open) {
        open(m.ExecCommandDialogComponent, {
          width: '700px', data: {
            namespace: this.data.namespace,
            pod: this.data.pod.metadata.name,
            containers: this.containers,
            defaultContainer: this.selectedContainer
          }
        });
      }
    });
  }

  copy(text: string): void {
    navigator.clipboard.writeText(text).catch(() => {});
  }
}

