import { Component, ElementRef, EventEmitter, Input, Output, ViewChild, inject } from '@angular/core';
import { CommonModule, DatePipe } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzMessageModule, NzMessageService } from 'ng-zorro-antd/message';

interface LogViewerRefreshPayload {
  tailLines: number;
}

@Component({
  selector: 'app-log-viewer',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    NzButtonModule,
    NzIconModule,
    NzSelectModule,
    NzSpinModule,
    NzEmptyModule,
    NzAlertModule,
    NzToolTipModule,
    NzMessageModule,
    DatePipe
  ],
  template: `
    <div class="log-viewer">
      <div class="log-viewer__toolbar">
        <div class="log-viewer__meta">
          <div class="meta-line">
            <span class="meta-label">Job：</span>
            <span class="meta-value">{{ jobName || '未知' }}</span>
          </div>
          <div class="meta-line" *ngIf="namespace">
            <span class="meta-label">Namespace：</span>
            <span class="meta-value">{{ namespace }}</span>
          </div>
          <div class="meta-line" *ngIf="podName">
            <span class="meta-label">Pod：</span>
            <span class="meta-value">{{ podName }}</span>
          </div>
          <div class="meta-line" *ngIf="lastUpdated">
            <span class="meta-label">最近刷新：</span>
            <span class="meta-value">{{ lastUpdated | date:'MM-dd HH:mm:ss' }}</span>
          </div>
        </div>
        <div class="log-viewer__actions">
          <nz-select
            nzSize="small"
            [(ngModel)]="tailLines"
            (ngModelChange)="onTailLinesChange($event)"
            [nzDropdownMatchSelectWidth]="false"
            class="tail-select">
            <nz-option *ngFor="let option of tailLinesOptions" [nzValue]="option" [nzLabel]="option + ' 行'"></nz-option>
          </nz-select>
          <button nz-button nzSize="small" nzType="default" (click)="onRefreshClick()" [disabled]="loading">
            <i nz-icon nzType="reload"></i>
            刷新
          </button>
          <button nz-button nzSize="small" nzType="default" (click)="copyLogs()" [disabled]="!logs.length">
            <i nz-icon nzType="copy"></i>
            复制
          </button>
        </div>
      </div>

      <nz-alert
        *ngIf="error"
        nzType="error"
        nzShowIcon
        [nzMessage]="'日志获取失败'"
        [nzDescription]="error"
        class="log-viewer__alert">
      </nz-alert>

      <div class="log-viewer__body">
        <nz-spin [nzSpinning]="loading" nzTip="正在获取日志...">
          <ng-container *ngIf="logs.length; else emptyTpl">
            <div class="log-viewer__scroll" #logContainer>
              <pre class="log-viewer__content">{{ logs.join('\n') }}</pre>
            </div>
          </ng-container>
          <ng-template #emptyTpl>
            <nz-empty nzNotFoundImage="simple" nzNotFoundDescription="暂无日志数据"></nz-empty>
          </ng-template>
        </nz-spin>
      </div>
    </div>
  `,
  styles: [`
    :host {
      display: block;
      height: 100%;
    }

    .log-viewer {
      display: flex;
      flex-direction: column;
      gap: 12px;
      height: 100%;
      padding: 16px;
      box-sizing: border-box;
      background: #fff;
    }

    .log-viewer__toolbar {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: flex-start;
    }

    .log-viewer__meta {
      display: flex;
      flex-direction: column;
      gap: 4px;
      font-size: 12px;
      color: rgba(0, 0, 0, 0.65);
    }

    .meta-line {
      display: flex;
      gap: 4px;
      align-items: center;
      line-height: 1.4;
    }

    .meta-label {
      color: rgba(0, 0, 0, 0.45);
    }

    .meta-value {
      color: rgba(0, 0, 0, 0.85);
      font-weight: 500;
    }

    .log-viewer__actions {
      display: flex;
      gap: 8px;
      align-items: center;
      flex-wrap: wrap;
    }

    .tail-select {
      min-width: 120px;
    }

    .log-viewer__alert {
      margin-bottom: 0;
    }

    .log-viewer__body {
      flex: 1;
      min-height: 320px;
    }

    .log-viewer__scroll {
      position: relative;
      background: #0f172a;
      color: #f8fafc;
      border-radius: 6px;
      padding: 12px;
      max-height: 420px;
      min-height: 320px;
      overflow-y: auto;
      font-family: 'SFMono-Regular', 'Menlo', 'Monaco', 'Consolas', 'Liberation Mono', 'Courier New', monospace;
      font-size: 12px;
      line-height: 1.6;
      border: 1px solid rgba(15, 23, 42, 0.4);
      box-shadow: inset 0 0 0 1px rgba(148, 163, 184, 0.1);
    }

    .log-viewer__content {
      margin: 0;
      white-space: pre-wrap;
      word-break: break-word;
    }

    @media (max-width: 768px) {
      .log-viewer {
        padding: 12px;
      }
      .log-viewer__toolbar {
        flex-direction: column;
        align-items: stretch;
        gap: 8px;
      }
      .log-viewer__actions {
        justify-content: flex-end;
      }
      .log-viewer__scroll {
        min-height: 240px;
        max-height: 320px;
      }
    }
  `]
})
export class LogViewerComponent {
  @ViewChild('logContainer') logContainer?: ElementRef<HTMLDivElement>;

  @Input() jobName = '';
  @Input() namespace = '';
  @Input() podName?: string;
  @Input() lastUpdated?: Date;
  @Input() loading = false;
  @Input() error?: string;
  @Input() tailLines = 200;
  @Input() tailLinesOptions: number[] = [100, 200, 500, 1000];

  private _logs: string[] = [];

  @Input()
  set logs(value: string[]) {
    this._logs = Array.isArray(value) ? value : [];
    setTimeout(() => this.scrollToBottom(), 0);
  }

  get logs(): string[] {
    return this._logs;
  }

  @Output() refresh = new EventEmitter<LogViewerRefreshPayload>();

  private readonly message = inject(NzMessageService);

  onRefreshClick(): void {
    this.refresh.emit({ tailLines: this.tailLines });
  }

  onTailLinesChange(value: number): void {
    if (typeof value === 'number' && value > 0) {
      this.tailLines = value;
      this.refresh.emit({ tailLines: this.tailLines });
    }
  }

  copyLogs(): void {
    if (!this.logs.length) {
      this.message.warning('暂无日志可复制');
      return;
    }
    const text = this.logs.join('\n');
    navigator.clipboard.writeText(text).then(() => {
      this.message.success('日志已复制到剪贴板');
    }).catch(() => {
      this.message.error('复制失败，请手动复制');
    });
  }

  private scrollToBottom(): void {
    const container = this.logContainer?.nativeElement;
    if (!container) {
      return;
    }
    requestAnimationFrame(() => {
      container.scrollTop = container.scrollHeight;
    });
  }
}
