import { Component, Inject, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormBuilder, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatDialogModule, MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatSnackBar, MatSnackBarModule } from '@angular/material/snack-bar';
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
  <h2 mat-dialog-title>
    <mat-icon>terminal</mat-icon>
    执行命令
  </h2>
  <div mat-dialog-content [formGroup]="form">
    <div class="kv">
      <div class="k">命名空间</div>
      <div class="v">{{ data.namespace }}</div>
    </div>
    <div class="kv">
      <div class="k">Pod</div>
      <div class="v">{{ data.pod }}</div>
    </div>

    <mat-form-field appearance="outline" class="w-100">
      <mat-label>容器</mat-label>
      <mat-select formControlName="container">
        <mat-option *ngFor="let c of data.containers" [value]="c">{{ c }}</mat-option>
      </mat-select>
    </mat-form-field>

    <mat-form-field appearance="outline" class="w-100">
      <mat-label>命令</mat-label>
      <input matInput formControlName="cmd" placeholder="例如：/bin/sh -lc 'uname -a'">
      <mat-icon matSuffix>play_arrow</mat-icon>
    </mat-form-field>

    <pre class="output" *ngIf="output">{{ output }}</pre>
  </div>
  <div mat-dialog-actions align="end">
    <button mat-button (click)="onOpenTerminal()"><mat-icon>computer</mat-icon> 打开终端（预览）</button>
    <button mat-stroked-button (click)="onCancel()">取消</button>
    <button mat-raised-button color="primary" (click)="onRun()" [disabled]="form.invalid || running">
      <mat-icon>play_arrow</mat-icon>
      运行
    </button>
  </div>
  `,
  styles: [`
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
    MatDialogModule,
    MatFormFieldModule,
    MatInputModule,
    MatSelectModule,
    MatButtonModule,
    MatIconModule,
    MatSnackBarModule
  ]
})
export class ExecCommandDialogComponent {
  dialogRef = inject<MatDialogRef<ExecCommandDialogComponent>>(MatDialogRef);
  data = inject<ExecDialogData>(MAT_DIALOG_DATA as any);
  fb = inject(FormBuilder);
  api = inject(ApiService);
  snackBar = inject(MatSnackBar);

  output = '';
  running = false;

  form = this.fb.group({
    container: [this.data.defaultContainer || (this.data.containers[0] || ''), []],
    cmd: ['/bin/sh -lc "uname -a"', [Validators.required, Validators.minLength(1)]]
  });

  onCancel(): void {
    this.dialogRef.close();
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
        this.dialogRef.close({ runOnce: true });
      },
      error: (err) => {
        this.running = false;
        this.output = err?.error || err?.message || '执行失败';
        this.snackBar.open('执行失败', '关闭', { duration: 3000 });
      }
    });
  }

  onOpenTerminal(): void {
    this.dialogRef.close();
    // 交由外层触发 webshell 对话框
  }
}

