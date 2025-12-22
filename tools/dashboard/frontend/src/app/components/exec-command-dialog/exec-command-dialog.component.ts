import { Component, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormBuilder, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { NzModalRef, NZ_MODAL_DATA } from 'ng-zorro-antd/modal';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzMessageService } from 'ng-zorro-antd/message';
import { ApiService } from '../../services/api.service';

export interface ExecDialogData {
  namespace: string;
  pod: string;
  containers: string[];
  defaultContainer?: string;
}

@Component({
  selector: 'app-exec-command-dialog',
  template: `
  <div class="modal-header">
    <i nz-icon nzType="tool" class="modal-title-icon"></i>
    <span class="modal-title-text">执行命令</span>
  </div>
  <div class="modal-body" [formGroup]="form">
    <div class="kv">
      <div class="k">命名空间</div>
      <div class="v">{{ data.namespace }}</div>
    </div>
    <div class="kv">
      <div class="k">Pod</div>
      <div class="v">{{ data.pod }}</div>
    </div>

    <nz-form-item>
      <nz-form-label [nzSpan]="5">容器</nz-form-label>
      <nz-form-control [nzSpan]="19">
        <nz-select formControlName="container" nzPlaceHolder="选择容器">
          <nz-option *ngFor="let c of data.containers" [nzValue]="c" [nzLabel]="c"></nz-option>
        </nz-select>
      </nz-form-control>
    </nz-form-item>

    <nz-form-item>
      <nz-form-label [nzSpan]="5">命令</nz-form-label>
      <nz-form-control [nzSpan]="19">
        <nz-input-group nzSuffixIcon="play-circle">
          <input nz-input formControlName="cmd" placeholder="例如：/bin/sh -lc 'uname -a'">
        </nz-input-group>
      </nz-form-control>
    </nz-form-item>

    <pre class="output" *ngIf="output">{{ output }}</pre>
  </div>
  <div class="modal-footer">
    <button nz-button nzType="default" (click)="onOpenTerminal()">
      <i nz-icon nzType="desktop"></i>
      <span>打开终端（预览）</span>
    </button>
    <button nz-button nzType="default" (click)="onCancel()">取消</button>
    <button nz-button nzType="primary" (click)="onRun()" [nzLoading]="running || form.invalid">
      <i nz-icon nzType="play-circle"></i>
      <span>运行</span>
    </button>
  </div>
  `,
  styles: [`
    .modal-header {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 8px 16px 4px 16px;
      font-size: 16px;
      font-weight: 600;
      border-bottom: 1px solid #f0f0f0;
    }
    .modal-title-icon {
      font-size: 18px;
      color: #1890ff;
    }
    .modal-title-text {
      flex: 1;
    }
    .modal-body {
      padding: 12px 16px 16px 16px;
    }
    .modal-footer {
      padding: 8px 16px 12px 16px;
      text-align: right;
      border-top: 1px solid #f0f0f0;
    }
    .w-100 { width: 100%; }
    .kv { display: flex; gap: 8px; margin: 6px 0; font-size: 13px; }
    .k { color: #666; min-width: 80px; }
    .v { font-weight: 500; }
    .output {
      background: #0b1020; color: #d6e4ff; padding: 12px; border-radius: 6px; max-height: 280px; overflow: auto;
      box-shadow: inset 0 0 0 1px rgba(255,255,255,0.06);
    }
  `],
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    NzFormModule,
    NzInputModule,
    NzSelectModule,
    NzButtonModule,
    NzIconModule
  ]
})
export class ExecCommandDialogComponent {
  // 支持通过 MatDialog 和 NzModalService 两种方式打开
  dialogRef = inject<MatDialogRef<ExecCommandDialogComponent> | null>(MatDialogRef, { optional: true });
  nzModalRef = inject<NzModalRef<ExecCommandDialogComponent> | null>(NzModalRef as any, { optional: true });
  // 兼容 MatDialog (MAT_DIALOG_DATA) 与 NzModal (NZ_MODAL_DATA) 两种数据来源
  private matDialogData = inject<ExecDialogData | null>(MAT_DIALOG_DATA as any, { optional: true });
  private nzModalData = inject<ExecDialogData | null>(NZ_MODAL_DATA, { optional: true });
  data: ExecDialogData = (this.matDialogData || this.nzModalData)!;
  fb = inject(FormBuilder);
  api = inject(ApiService);
  message = inject(NzMessageService);

  output = '';
  running = false;

  form = this.fb.group({
    container: [this.data.defaultContainer || (this.data.containers[0] || ''), []],
    cmd: ['/bin/sh -lc "uname -a"', [Validators.required, Validators.minLength(1)]]
  });

  private close(result?: any): void {
    if (this.dialogRef) {
      this.dialogRef.close(result);
    } else if (this.nzModalRef) {
      this.nzModalRef.close(result);
    }
  }

  onCancel(): void {
    this.close();
  }

  onRun(): void {
    const { container, cmd } = this.form.value as { container: string; cmd: string };
    if (!cmd) { return; }
    this.running = true;
    this.output = '';
    this.api.execPod(this.data.namespace, this.data.pod, container || '', cmd, true).subscribe({
      next: (text) => {
        this.output = text || '(no output)';
        this.running = false;
        this.close({ runOnce: true });
      },
      error: (err) => {
        this.running = false;
        this.output = err?.error || err?.message || '执行失败';
        this.message.error('执行失败');
      }
    });
  }

  onOpenTerminal(): void {
    this.close();
    // 交由外层触发 webshell 对话框
  }
}

