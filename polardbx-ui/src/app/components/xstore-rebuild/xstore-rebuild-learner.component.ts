import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatSnackBar, MatSnackBarModule } from '@angular/material/snack-bar';
import { ApiService } from '../../services/api.service';
import { MatSelectModule } from '@angular/material/select';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { Router } from '@angular/router';

@Component({
  selector: 'app-xstore-rebuild-learner',
  standalone: true,
  imports: [CommonModule, FormsModule, MatCardModule, MatButtonModule, MatIconModule, MatSnackBarModule, MatSelectModule, MatProgressBarModule],
  template: `
    <div class="page">
      <mat-card>
        <mat-card-header>
          <mat-card-title>
            <mat-icon>school</mat-icon>
            备库重搭 · 重搭 learner 节点
          </mat-card-title>
        </mat-card-header>
        <mat-card-content>
          <div class="form-row"><label>命名空间</label><input [(ngModel)]="namespace" placeholder="default"/></div>
          <div class="form-row"><label>目标 XStore</label><input [(ngModel)]="xstore" placeholder="cluster-dn"/></div>
          <div class="form-row"><label>候选 Pod</label>
            <div class="inline">
              <button mat-stroked-button (click)="loadPods()"><mat-icon>refresh</mat-icon> 加载候选</button>
            </div>
          </div>
          <mat-progress-bar *ngIf="loadingPods" mode="indeterminate" style="margin:4px 0 8px"></mat-progress-bar>
          <div class="form-row" *ngIf="!loadingPods">
            <label>Learner Pod</label>
            <mat-select [(ngModel)]="learnerPod" placeholder="选择运行中的 Learner Pod">
              <mat-option *ngFor="let p of filteredPods" [value]="p?.metadata?.name">{{ p?.metadata?.name }}</mat-option>
            </mat-select>
          </div>
          <div class="form-row"><label>名称(可选)</label><input [(ngModel)]="name" placeholder="留空自动生成"/></div>
          <div class="actions">
            <button mat-raised-button color="primary" (click)="submit()" [disabled]="submitting"><mat-icon>play_arrow</mat-icon> {{ submitting ? '提交中...' : '发起重搭' }}</button>
          </div>
        </mat-card-content>
      </mat-card>
    </div>
  `,
  styles: [`.page{padding:20px}.form-row{display:grid;grid-template-columns:120px 1fr;gap:8px;align-items:center;margin:12px 0}.form-row input{height:34px;padding:6px 10px;border:1px solid #e0e0e0;border-radius:6px}.actions{display:flex;gap:8px;align-items:center}`]
})
export class XStoreRebuildLearnerComponent {
  namespace = 'default';
  xstore = '';
  learnerPod = '';
  name = '';
  submitting = false;
  loadingPods = false;
  pods: any[] = [];
  filteredPods: any[] = [];
  constructor(private snack: MatSnackBar, private api: ApiService, private router: Router) {}

  private genName(base: string): string {
    const now = new Date();
    const ts = `${now.getFullYear()}${(now.getMonth()+1).toString().padStart(2,'0')}${now.getDate().toString().padStart(2,'0')}${now.getHours().toString().padStart(2,'0')}${now.getMinutes().toString().padStart(2,'0')}`;
    const rand = Math.random().toString(36).slice(2,6);
    const raw = `${base}-learner-${ts}-${rand}`.toLowerCase().replace(/[^a-z0-9-]/g,'-');
    return raw.length>63 ? raw.slice(0,63) : raw;
  }

  submit(): void {
    if (!this.isValid()) { this.snack.open('请完善必填项：命名空间 / XStore / Learner Pod', '关闭', { duration: 2500 }); return; }
    const req: any = {
      name: this.name?.trim() || this.genName(this.xstore),
      xStoreName: this.xstore,
      targetPodName: this.learnerPod?.trim() || undefined,
      local: true
    };
    this.submitting = true;
    this.api.rebuildLearner(this.namespace || 'default', req).subscribe({
      next: () => { this.snack.open('已提交 learner 重搭', '关闭', { duration: 3000 }); this.submitting = false; this.router.navigateByUrl('/storage/xstore-followers'); },
      error: (e) => { this.snack.open('提交失败: '+(e?.error?.message||e?.message||'未知错误'), '关闭', { duration: 4000 }); this.submitting = false; }
    });
  }

  loadPods(): void {
    if (!this.xstore) { this.snack.open('请先填写目标 XStore', '关闭', { duration: 2500 }); return; }
    this.loadingPods = true;
    this.api.getXStorePods(this.namespace || 'default', this.xstore).subscribe({
      next: (pods) => {
        this.pods = pods || [];
        this.filteredPods = (this.pods || []).filter(p => {
          const phase = (p as any)?.status?.phase;
          const role = (p as any)?.metadata?.labels?.['xstore/role'];
          return phase === 'Running' && (role === 'learner' || !role);
        });
        if (!this.filteredPods.length) {
          this.filteredPods = (this.pods || []).filter(p => (p as any)?.status?.phase === 'Running');
        }
        this.loadingPods = false;
      },
      error: () => { this.pods = []; this.filteredPods = []; this.loadingPods = false; }
    });
  }

  isValid(): boolean { return !!(this.namespace && this.xstore && this.learnerPod); }
}