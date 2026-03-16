import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReactiveFormsModule, FormBuilder, Validators } from '@angular/forms';
import { NzStepsModule } from 'ng-zorro-antd/steps';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzSpinModule } from 'ng-zorro-antd/spin';

import { ApiService } from '../../services/api.service';
import { PolarDBXCluster } from '../../models/cluster.model';

@Component({
  selector: 'app-upgrade-rollback-wizard',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    NzStepsModule,
    NzFormModule,
    NzSelectModule,
    NzInputModule,
    NzButtonModule,
    NzCardModule,
    NzAlertModule,
    NzIconModule,
    NzSpinModule
  ],
  template: `
    <div class="wizard">
      <nz-card nzTitle="升级/回滚向导">
        <nz-steps [nzCurrent]="current">
          <nz-step nzTitle="选择集群"></nz-step>
          <nz-step nzTitle="目标版本"></nz-step>
          <nz-step nzTitle="预检查"></nz-step>
          <nz-step nzTitle="执行"></nz-step>
        </nz-steps>

        <div class="content" *ngIf="current===0">
          <form [formGroup]="clusterForm" nz-form nzLayout="vertical">
            <div nz-row nzGutter="16">
              <div nz-col nzSpan="8">
                <label class="label">命名空间</label>
                <nz-select formControlName="namespace" [nzOptions]="nsOptions"></nz-select>
              </div>
              <div nz-col nzSpan="16">
                <label class="label">集群</label>
                <nz-select formControlName="clusterName">
                  <nz-option *ngFor="let c of clusters" [nzValue]="c.metadata.name" [nzLabel]="c.metadata.name"></nz-option>
                </nz-select>
              </div>
            </div>
          </form>
          <div class="actions">
            <button nz-button nzType="primary" [disabled]="clusterForm.invalid" (click)="next()">下一步</button>
          </div>
        </div>

        <div class="content" *ngIf="current===1">
          <form [formGroup]="versionForm" nz-form nzLayout="vertical">
            <div nz-row nzGutter="16">
              <div nz-col nzSpan="12">
                <label class="label">当前版本</label>
                <input nz-input [value]="currentVersion" disabled>
              </div>
              <div nz-col nzSpan="12">
                <label class="label">目标版本</label>
                <input nz-input formControlName="targetVersion" placeholder="例如 v1.7.0" />
              </div>
            </div>
            <div nz-row nzGutter="16" style="margin-top:8px;">
              <div nz-col nzSpan="12">
                <label class="label">回滚版本（可选）</label>
                <input nz-input formControlName="rollbackVersion" placeholder="回滚时使用" />
              </div>
            </div>
          </form>
          <div class="actions">
            <button nz-button (click)="prev()">上一步</button>
            <button nz-button nzType="primary" [disabled]="versionForm.invalid" (click)="next()">下一步</button>
          </div>
        </div>

        <div class="content" *ngIf="current===2">
          <button nz-button nzType="default" (click)="runPrecheck()" [disabled]="prechecking"><i nz-icon nzType="safety"></i> 执行预检查</button>
          <nz-spin *ngIf="prechecking"></nz-spin>
          <nz-alert *ngIf="precheckResult && precheckOK" nzType="success" nzMessage="预检查通过" nzShowIcon [nzDescription]="precheckText"></nz-alert>
          <nz-alert *ngIf="precheckResult && !precheckOK" nzType="warning" nzMessage="预检查未通过" nzShowIcon [nzDescription]="precheckText"></nz-alert>
          <div class="actions">
            <button nz-button (click)="prev()">上一步</button>
            <button nz-button nzType="primary" [disabled]="!precheckOK" (click)="next()">下一步</button>
          </div>
        </div>

        <div class="content" *ngIf="current===3">
          <div style="margin-bottom:8px;">
            <nz-alert nzType="info" nzShowIcon nzMessage="将先自动创建全量备份作为回退点，然后发起升级"></nz-alert>
          </div>
          <div class="actions">
            <button nz-button (click)="prev()">上一步</button>
            <button nz-button nzType="primary" [disabled]="executing" (click)="execute()"><i nz-icon nzType="play-circle"></i> 执行</button>
            <button nz-button nzDanger [disabled]="!rollbackEnabled || executing" (click)="rollback()"><i nz-icon nzType="undo"></i> 回滚</button>
          </div>
          <nz-spin *ngIf="executing"></nz-spin>
          <nz-alert *ngIf="execMsg" [nzType]="execOk? 'success':'warning'" [nzMessage]="execMsg" nzShowIcon></nz-alert>
        </div>
      </nz-card>
    </div>
  `,
  styles: [`
    .wizard { padding: 20px; }
    .content { margin-top: 16px; }
    .actions { margin-top: 16px; display: flex; gap: 8px; }
    .label { display:inline-block; margin-bottom:6px; color: rgba(0,0,0,.65); }
  `]
})
export class UpgradeRollbackWizardComponent implements OnInit {
  private api = inject(ApiService);
  private fb = inject(FormBuilder);

  current = 0;
  clusters: PolarDBXCluster[] = [];
  nsOptions = [{ label: 'default', value: 'default' }];
  currentVersion = '';

  clusterForm = this.fb.group({
    namespace: ['default', [Validators.required]],
    clusterName: ['', [Validators.required]]
  });

  versionForm = this.fb.group({
    targetVersion: ['', [Validators.required]],
    rollbackVersion: ['']
  });

  prechecking = false;
  precheckResult: any = null;
  precheckOK = false;
  precheckText = '';

  executing = false;
  rollbackEnabled = false;
  execMsg = '';
  execOk = false;

  ngOnInit(): void {
    this.api.getClusters().subscribe({
      next: (cs) => {
        this.clusters = cs || [];
      }
    });
  }

  next(): void { this.current = Math.min(3, this.current + 1); }
  prev(): void { this.current = Math.max(0, this.current - 1); }

  runPrecheck(): void {
    this.prechecking = true;
    const ns = this.clusterForm.value.namespace!;
    const name = this.clusterForm.value.clusterName!;
    this.api.runPrecheck(ns, name, 'upgrade').subscribe({
      next: (res: any) => {
        this.prechecking = false;
        this.precheckResult = res;
        const c: any = res?.checks || {};
        this.precheckOK = !!c.hasRecentBackup && (c.storageConnectivity === 'configured') && (typeof c.rpoLagSeconds === 'number');
        this.precheckText = `最近全备：${c.hasRecentBackup?'是':'否'}；存储：${c.storageConnectivity||'未知'}；RPO滞后：${c.rpoLagSeconds ?? '-'}s`;
      },
      error: (err: any) => {
        this.prechecking = false;
        const c: any = err?.error?.checks || {};
        this.precheckOK = false;
        this.precheckText = `最近全备：${c.hasRecentBackup?'是':'否'}；存储：${c.storageConnectivity||c.storage||'未知'}；RPO滞后：${c.rpoLagSeconds ?? '-'}s`;
        this.precheckResult = c;
      }
    });
  }

  execute(): void {
    if (this.executing) return;
    this.executing = true;
    this.execMsg = '';
    this.execOk = false;
    const ns = this.clusterForm.value.namespace!;
    const name = this.clusterForm.value.clusterName!;
    const target = this.versionForm.value.targetVersion!;

    const backupObj: any = { metadata: {}, spec: { cluster: { name }, storageProvider: { storageName: 'default' } } };
    this.api.createBackup(ns, name, backupObj).subscribe({
      next: () => {
        this.rollbackEnabled = true;
        this.api.upgradeCluster(ns, name, { targetVersion: target, strategy: 'rolling', maxUnavailable: 1 }).subscribe({
          next: () => {
            this.executing = false;
            this.execOk = true;
            this.execMsg = '升级任务已启动，已自动创建回退点（备份）。';
          },
          error: (e) => {
            this.executing = false;
            this.execOk = false;
            this.execMsg = `升级启动失败：${e?.message || '未知错误'}`;
          }
        });
      },
      error: (e) => {
        this.executing = false;
        this.execOk = false;
        this.execMsg = `创建备份失败：${e?.message || '未知错误'}`;
      }
    });
  }

  rollback(): void {
    if (this.executing) return;
    const ns = this.clusterForm.value.namespace!;
    const name = this.clusterForm.value.clusterName!;
    const v = this.versionForm.value.rollbackVersion || this.currentVersion;
    if (!v) { this.execMsg = '未提供回滚版本'; this.execOk = false; return; }
    this.executing = true;
    this.api.upgradeCluster(ns, name, { targetVersion: v, strategy: 'rolling', maxUnavailable: 1 }).subscribe({
      next: () => { this.executing = false; this.execOk = true; this.execMsg = '已触发回滚升级。'; },
      error: (e) => { this.executing = false; this.execOk = false; this.execMsg = `回滚失败：${e?.message||'未知错误'}`; }
    });
  }
}

