import { Component, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { Router } from '@angular/router';
import { Observable } from 'rxjs';
import { GlobalInstallProgressService, InstallTask } from '../../services/global-install-progress.service';

@Component({
  selector: 'app-global-install-progress',
  standalone: true,
  imports: [CommonModule, NzAlertModule, NzButtonModule, NzIconModule],
  template: `
    <ng-container *ngIf="tasks$ | async as tasks">
      <div class="global-install-progress" *ngIf="tasks.length">
        <nz-alert nzType="info" nzShowIcon [nzMessage]="'安装任务进行中'">
          <ng-container *ngFor="let t of tasks; let i = index">
            <div class="task-item">
              <span class="icon"><i nz-icon [nzType]="t.kind === 'monitoring' ? 'dashboard' : 'file-text'"></i></span>
              <span class="text">
                <strong>{{ t.kind === 'monitoring' ? '监控安装' : '日志安装' }}</strong>
                <span>Job: {{ t.jobName }} ({{ t.namespace }})</span>
                <span>状态: {{ t.phase || 'Running' }}</span>
                <span *ngIf="t.message" class="msg">{{ t.message }}</span>
              </span>
              <span class="actions">
                <button nz-button nzSize="small" nzType="default" (click)="viewLogs(t)"><i nz-icon nzType="file-text"></i> 查看日志</button>
                <button nz-button nzSize="small" nzType="default" (click)="backToWizard(t)"><i nz-icon nzType="rollback"></i> 返回向导</button>
                <button nz-button nzSize="small" nzType="link" (click)="dismiss(t)"><i nz-icon nzType="close"></i> 关闭</button>
              </span>
            </div>
            <ng-container *ngIf="i < tasks.length - 1"><div class="divider"></div></ng-container>
          </ng-container>
        </nz-alert>
      </div>
    </ng-container>
  `,
  styles: [`
    .global-install-progress { margin-bottom: 12px; }
    .task-item {
      display: flex; align-items: center; gap: 12px; padding: 6px 0;
    }
    .icon { color: #1890ff; }
    .text { display: flex; gap: 12px; align-items: center; flex-wrap: wrap; color: rgba(0,0,0,0.75); }
    .text .msg { color: rgba(0,0,0,0.55); }
    .actions { margin-left: auto; display: flex; gap: 8px; }
    .divider { height: 1px; background: #f0f0f0; margin: 6px 0; }
  `]
})
export class GlobalInstallProgressComponent {
  private readonly service = inject(GlobalInstallProgressService);
  private readonly router = inject(Router);
  readonly tasks$: Observable<InstallTask[]> = this.service.getTasks();

  viewLogs(t: InstallTask): void {
    const ns = t.namespace;
    const name = t.jobName;
    const cmd = `kubectl logs -n ${ns} job/${name} --follow --tail=200`;
    this.copyToClipboard(cmd);
    // 简单提示：复制即可在终端查看
  }

  backToWizard(t: InstallTask): void {
    if (t.kind === 'monitoring') {
      this.router.navigate(['/operations/monitoring/enable-wizard']);
    } else {
      this.router.navigate(['/operations/logs/collector-install']);
    }
  }

  dismiss(t: InstallTask): void {
    this.service.dismiss(t);
  }

  private copyToClipboard(text: string): void {
    if (navigator?.clipboard && typeof navigator.clipboard.writeText === 'function') {
      navigator.clipboard.writeText(text).catch(() => this.fallbackCopy(text));
    } else {
      this.fallbackCopy(text);
    }
  }

  private fallbackCopy(text: string): void {
    const textarea = document.createElement('textarea');
    textarea.value = text;
    textarea.style.position = 'fixed';
    textarea.style.opacity = '0';
    document.body.appendChild(textarea);
    textarea.focus();
    textarea.select();
    try {
      document.execCommand('copy');
    } catch {
      // ignore copy failure
    }
    document.body.removeChild(textarea);
  }
}