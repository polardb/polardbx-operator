import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReactiveFormsModule, FormBuilder, Validators } from '@angular/forms';
import { NzTabsModule } from 'ng-zorro-antd/tabs';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzListModule } from 'ng-zorro-antd/list';
import { NzDescriptionsModule } from 'ng-zorro-antd/descriptions';
import { NzInputNumberModule } from 'ng-zorro-antd/input-number';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzIconModule } from 'ng-zorro-antd/icon';

import { ApiService } from '../../services/api.service';

@Component({
  selector: 'app-runbook-slo',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    NzTabsModule,
    NzCardModule,
    NzListModule,
    NzDescriptionsModule,
    NzInputNumberModule,
    NzFormModule,
    NzButtonModule,
    NzAlertModule,
    NzIconModule
  ],
  template: `
    <div class="page-wrapper runbook-slo">
      <div class="page-header">
        <div class="title-block">
          <h2>
            <i nz-icon nzType="book" class="page-icon"></i>
            Runbook / SLO
          </h2>
          <p>常见故障处置手册与 SLO 阈值配置</p>
        </div>
      </div>

      <div class="page-content">
      <nz-card nzTitle="Runbook / 知识卡片" class="content-card">
        <nz-tabset>
          <nz-tab nzTitle="磁盘满">
            <nz-descriptions nzBordered [nzColumn]="1">
              <nz-descriptions-item nzTitle="识别">告警: Node/Pod 磁盘利用率 > 90%</nz-descriptions-item>
              <nz-descriptions-item nzTitle="定位">通过 Grafana 面板检查磁盘使用增长曲线</nz-descriptions-item>
              <nz-descriptions-item nzTitle="处置">清理无用日志/扩容磁盘/迁移数据；必要时重启无状态组件</nz-descriptions-item>
            </nz-descriptions>
          </nz-tab>
          <nz-tab nzTitle="复制落后">
            <nz-descriptions nzBordered [nzColumn]="1">
              <nz-descriptions-item nzTitle="识别">告警: RPO 滞后超过阈值</nz-descriptions-item>
              <nz-descriptions-item nzTitle="定位">检查 Binlog 吞吐、网络带宽、磁盘 IOPS</nz-descriptions-item>
              <nz-descriptions-item nzTitle="处置">调高带宽/IO 限额，优化备份/清理计划</nz-descriptions-item>
            </nz-descriptions>
          </nz-tab>
          <nz-tab nzTitle="备份失败">
            <nz-descriptions nzBordered [nzColumn]="1">
              <nz-descriptions-item nzTitle="识别">备份 Job 状态非 Complete</nz-descriptions-item>
              <nz-descriptions-item nzTitle="定位">查看 Operator/Job 日志，确认对象存储连通</nz-descriptions-item>
              <nz-descriptions-item nzTitle="处置">重试、切换存储、调整并发</nz-descriptions-item>
            </nz-descriptions>
          </nz-tab>
          <nz-tab nzTitle="监控缺失">
            <nz-descriptions nzBordered [nzColumn]="1">
              <nz-descriptions-item nzTitle="识别">面板/告警项缺失或 Prometheus 不可用</nz-descriptions-item>
              <nz-descriptions-item nzTitle="定位">执行 Monitoring Preflight 检查</nz-descriptions-item>
              <nz-descriptions-item nzTitle="处置">重新安装/同步面板配置，修复 ServiceMonitor</nz-descriptions-item>
            </nz-descriptions>
          </nz-tab>
        </nz-tabset>
      </nz-card>

      <nz-card nzTitle="SLO 目标与阈值" class="content-card">
        <form [formGroup]="sloForm" nz-form nzLayout="vertical">
          <div nz-row nzGutter="16">
            <div nz-col [nzSpan]="6">
              <nz-form-item>
                <nz-form-label>RPO 阈值</nz-form-label>
                <nz-form-control nzExtra="秒">
                  <nz-input-number formControlName="rpoThresholdSeconds" [nzMin]="0" style="width: 100%"></nz-input-number>
                </nz-form-control>
              </nz-form-item>
            </div>
            <div nz-col [nzSpan]="6">
              <nz-form-item>
                <nz-form-label>吞吐下限</nz-form-label>
                <nz-form-control nzExtra="MB/s">
                  <nz-input-number formControlName="throughputLowerBoundMBps" [nzMin]="0" [nzStep]="0.1" style="width: 100%"></nz-input-number>
                </nz-form-control>
              </nz-form-item>
            </div>
            <div nz-col [nzSpan]="6">
              <nz-form-item>
                <nz-form-label>诊断保留</nz-form-label>
                <nz-form-control nzExtra="天">
                  <nz-input-number formControlName="diagnosisRetentionDays" [nzMin]="0" style="width: 100%"></nz-input-number>
                </nz-form-control>
              </nz-form-item>
            </div>
            <div nz-col [nzSpan]="6">
              <nz-form-item>
                <nz-form-label>&nbsp;</nz-form-label>
                <nz-form-control>
                  <div style="display: flex; gap: 8px;">
                    <button nz-button nzType="default" (click)="load()">重置</button>
                    <button nz-button nzType="primary" [disabled]="sloForm.invalid" (click)="save()">保存</button>
                  </div>
                </nz-form-control>
              </nz-form-item>
            </div>
          </div>
        </form>
        <nz-alert *ngIf="msg" [nzType]="msgType" [nzMessage]="msg" nzShowIcon style="margin-top:12px;"></nz-alert>
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
      line-height: 1.6;
    }
    .page-icon { font-size: 22px; color: var(--primary-color, #ff6a00); }
    .page-content { display: flex; flex-direction: column; gap: 16px; }
    .content-card { border-radius: 10px; box-shadow: 0 2px 10px rgba(15, 23, 42, 0.04); }
    label { display:block; margin-bottom:4px; color: rgba(0,0,0,.65); }
    @media (max-width: 768px) { .page-wrapper { padding: 16px; } }
  `]
})
export class RunbookSloComponent implements OnInit {
  private api = inject(ApiService);
  private fb = inject(FormBuilder);

  sloForm = this.fb.group({
    rpoThresholdSeconds: [3600, [Validators.required, Validators.min(0)]],
    throughputLowerBoundMBps: [1.0, [Validators.required, Validators.min(0)]],
    diagnosisRetentionDays: [7, [Validators.required, Validators.min(0)]]
  });

  msg = '';
  msgType: 'success'|'warning'|'info'|'error' = 'success';

  ngOnInit(): void { this.load(); }

  load(): void {
    this.api.getBackupDashboardSettings().subscribe({
      next: (s) => this.sloForm.patchValue({
        rpoThresholdSeconds: s?.rpoThresholdSeconds ?? 3600,
        throughputLowerBoundMBps: s?.throughputLowerBoundMBps ?? 1.0,
        diagnosisRetentionDays: s?.diagnosisRetentionDays ?? 7
      }),
      error: () => {}
    });
  }

  save(): void {
    if (this.sloForm.invalid) return;
    const v = this.sloForm.value as any;
    this.api.updateBackupDashboardSettings(v).subscribe({
      next: () => { this.msgType = 'success'; this.msg = '保存成功'; },
      error: () => { this.msgType = 'error'; this.msg = '保存失败'; }
    });
  }
}

