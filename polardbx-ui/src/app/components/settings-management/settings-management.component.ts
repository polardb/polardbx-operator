import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormBuilder, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzInputNumberModule } from 'ng-zorro-antd/input-number';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzDescriptionsModule } from 'ng-zorro-antd/descriptions';
import { ApiService } from '../../services/api.service';

@Component({
  selector: 'app-settings-management',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    NzCardModule,
    NzFormModule,
    NzInputModule,
    NzInputNumberModule,
    NzButtonModule,
    NzIconModule,
    NzDividerModule,
    NzGridModule,
    NzDescriptionsModule
  ],
  template: `
    <div class="settings-management">
      <div class="page-header">
        <div class="header-content">
          <h1 class="page-title">
            <i nz-icon nzType="setting" class="page-icon"></i>
            阈值与设置
          </h1>
          <p class="page-description">配置系统运维相关的阈值参数和外部服务连接，确保系统稳定运行</p>
        </div>
      </div>

      <div class="page-content">
        <div class="settings-sections">
          <!-- 监控阈值设置 -->
          <nz-card class="setting-card" nzTitle="监控阈值" [nzExtra]="thresholdExtra">
            <ng-template #thresholdExtra>
              <i nz-icon nzType="dashboard" class="section-icon"></i>
            </ng-template>
            <form [formGroup]="form" nz-form nzLayout="vertical">
              <div nz-row nzGutter="16">
                <div nz-col [nzSpan]="8">
                  <nz-form-item>
                    <nz-form-label nzRequired>RPO 阈值</nz-form-label>
                    <nz-form-control nzExtra="RPO 恢复点目标超过此值将触发告警">
                      <nz-input-number formControlName="rpoThresholdSeconds" [nzMin]="0" nzPlaceHolder="秒" style="width: 100%"></nz-input-number>
                    </nz-form-control>
                  </nz-form-item>
                </div>
                <div nz-col [nzSpan]="8">
                  <nz-form-item>
                    <nz-form-label nzRequired>吞吐下限</nz-form-label>
                    <nz-form-control nzExtra="备份吞吐量低于此值将触发告警">
                      <nz-input-number formControlName="throughputLowerBoundMBps" [nzMin]="0" [nzStep]="0.1" nzPlaceHolder="MB/s" style="width: 100%"></nz-input-number>
                    </nz-form-control>
                  </nz-form-item>
                </div>
                <div nz-col [nzSpan]="8">
                  <nz-form-item>
                    <nz-form-label nzRequired>诊断保留天数</nz-form-label>
                    <nz-form-control nzExtra="诊断报告自动清理时间">
                      <nz-input-number formControlName="diagnosisRetentionDays" [nzMin]="0" nzPlaceHolder="天" style="width: 100%"></nz-input-number>
                    </nz-form-control>
                  </nz-form-item>
                </div>
              </div>
            </form>
          </nz-card>

          <!-- 外部服务配置 -->
          <nz-card class="setting-card" nzTitle="外部服务" [nzExtra]="serviceExtra">
            <ng-template #serviceExtra>
              <i nz-icon nzType="link" class="section-icon"></i>
            </ng-template>
            <form [formGroup]="form" nz-form nzLayout="vertical">
              <div nz-row nzGutter="16">
                <div nz-col [nzSpan]="12">
                  <nz-form-item>
                    <nz-form-label>Grafana URL</nz-form-label>
                    <nz-form-control nzExtra="监控面板服务地址">
                      <input nz-input type="url" formControlName="grafanaURL" placeholder="http://grafana.polardbx-monitor:3000" />
                    </nz-form-control>
                  </nz-form-item>
                </div>
                <div nz-col [nzSpan]="12">
                  <nz-form-item>
                    <nz-form-label>Alertmanager URL</nz-form-label>
                    <nz-form-control nzExtra="告警管理服务地址">
                      <input nz-input type="url" formControlName="alertmanagerURL" placeholder="http://alertmanager.polardbx-monitor:9093" />
                    </nz-form-control>
                  </nz-form-item>
                </div>
              </div>
            </form>
          </nz-card>

          <!-- 备份策略配置 -->
          <nz-card class="setting-card" nzTitle="备份策略" [nzExtra]="backupExtra">
            <ng-template #backupExtra>
              <i nz-icon nzType="cloud-upload" class="section-icon"></i>
            </ng-template>
            <form [formGroup]="form" nz-form nzLayout="vertical">
              <div nz-row nzGutter="16">
                <div nz-col [nzSpan]="6">
                  <nz-form-item>
                    <nz-form-label nzRequired>目标 RTO</nz-form-label>
                    <nz-form-control nzExtra="恢复时间目标">
                      <nz-input-number formControlName="targetRtoMinutes" [nzMin]="0" nzPlaceHolder="分钟" style="width: 100%"></nz-input-number>
                    </nz-form-control>
                  </nz-form-item>
                </div>
                <div nz-col [nzSpan]="6">
                  <nz-form-item>
                    <nz-form-label>保留数量</nz-form-label>
                    <nz-form-control nzExtra="最新 N 份备份">
                      <nz-input-number formControlName="retain" [nzMin]="0" nzPlaceHolder="份" style="width: 100%"></nz-input-number>
                    </nz-form-control>
                  </nz-form-item>
                </div>
                <div nz-col [nzSpan]="6">
                  <nz-form-item>
                    <nz-form-label>保留天数</nz-form-label>
                    <nz-form-control nzExtra="超过天数自动清理">
                      <nz-input-number formControlName="retainDays" [nzMin]="0" nzPlaceHolder="天" style="width: 100%"></nz-input-number>
                    </nz-form-control>
                  </nz-form-item>
                </div>
                <div nz-col [nzSpan]="6">
                  <nz-form-item>
                    <nz-form-label>保留小时</nz-form-label>
                    <nz-form-control nzExtra="增量备份保留时间">
                      <nz-input-number formControlName="retainHours" [nzMin]="0" nzPlaceHolder="小时" style="width: 100%"></nz-input-number>
                    </nz-form-control>
                  </nz-form-item>
                </div>
              </div>
              <div nz-row>
                <div nz-col [nzSpan]="24">
                  <nz-form-item>
                    <nz-form-label>冷热分层策略</nz-form-label>
                    <nz-form-control nzExtra="JSON 格式配置，定义热数据保留天数和冷存储位置">
                      <input nz-input formControlName="tieringPolicy" placeholder='{"hotDays":7,"coldSink":"s3"}' />
                    </nz-form-control>
                  </nz-form-item>
                </div>
              </div>
            </form>
          </nz-card>
        </div>

        <!-- 操作按钮 -->
        <div class="action-bar">
          <button nz-button nzSize="large" (click)="load()">
            <i nz-icon nzType="reload"></i>
            重置
          </button>
          <button nz-button nzType="primary" nzSize="large" (click)="save()" [disabled]="form.invalid">
            <i nz-icon nzType="save"></i>
            保存设置
          </button>
        </div>
      </div>
    </div>
  `,
  styles: [`
    .settings-management {
      padding: 16px;
      background: #f5f5f5;
      min-height: 100vh;
    }
    
    .page-header {
      margin-bottom: 16px;
    }
    
    .header-content {
      max-width: 1120px;
      margin: 0 auto;
    }
    
    .page-title {
      color: rgba(0, 0, 0, 0.87);
      font-size: 18px;
      font-weight: 500;
      margin: 0 0 4px 0;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    
    .page-icon {
      font-size: 20px;
      color: #1890ff;
    }
    
    .page-description {
      color: rgba(0, 0, 0, 0.6);
      font-size: 14px;
      margin: 0;
      line-height: 1.5;
    }
    
    .page-content {
      max-width: 1120px;
      margin: 0 auto;
    }
    
    .settings-sections {
      display: flex;
      flex-direction: column;
      gap: 16px;
      margin-bottom: 24px;
    }
    
    .setting-card {
      background: #ffffff;
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border: 1px solid #e0e0e0;
      overflow: hidden;
    }
    
    .section-icon {
      font-size: 16px;
      color: #1890ff;
    }
    
    .action-bar {
      display: flex;
      justify-content: center;
      gap: 16px;
      padding: 16px;
      background: #ffffff;
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border: 1px solid #e0e0e0;
    }
    
    /* 响应式设计 */
    @media (max-width: 768px) {
      .settings-management {
        padding: 16px;
      }
      
      .action-bar {
        flex-direction: column;
        align-items: center;
      }
      
      .action-bar button {
        width: 100%;
        max-width: 200px;
      }
    }
  `]
})
export class SettingsManagementComponent implements OnInit {
  private api = inject(ApiService);
  private msg = inject(NzMessageService);
  private fb = inject(FormBuilder);

  form: FormGroup = this.fb.group({
    rpoThresholdSeconds: [3600, [Validators.required, Validators.min(0)]],
    throughputLowerBoundMBps: [1.0, [Validators.required, Validators.min(0)]],
    diagnosisRetentionDays: [7, [Validators.required, Validators.min(0)]],
    grafanaURL: [''],
    alertmanagerURL: [''],
    targetRtoMinutes: [30, [Validators.required, Validators.min(0)]],
    retain: [7, [Validators.min(0)]],
    retainDays: [30, [Validators.min(0)]],
    retainHours: [168, [Validators.min(0)]],
    tieringPolicy: ['']
  });

  ngOnInit(): void { this.load(); }

  load(): void {
    this.api.getBackupDashboardSettings().subscribe({
      next: (s) => this.form.patchValue({
        rpoThresholdSeconds: s?.rpoThresholdSeconds ?? 3600,
        throughputLowerBoundMBps: s?.throughputLowerBoundMBps ?? 1.0,
        diagnosisRetentionDays: s?.diagnosisRetentionDays ?? 7
      }),
      error: () => {}
    });
    const g = localStorage.getItem('grafanaURL') || '';
    const a = localStorage.getItem('alertmanagerURL') || '';
    // 系统级策略（ConfigMap settings）
    this.api.getSystemSettings().subscribe({
      next: (cfg) => {
        const patch: any = { grafanaURL: g, alertmanagerURL: a };
        if (cfg) {
          if (cfg['backup.targetRtoMinutes']) patch.targetRtoMinutes = Number(cfg['backup.targetRtoMinutes']);
          if (cfg['backup.retain']) patch.retain = Number(cfg['backup.retain']);
          if (cfg['backup.retainDays']) patch.retainDays = Number(cfg['backup.retainDays']);
          if (cfg['backup.retainHours']) patch.retainHours = Number(cfg['backup.retainHours']);
          if (cfg['backup.tieringPolicy']) patch.tieringPolicy = cfg['backup.tieringPolicy'];
        }
        this.form.patchValue(patch, { emitEvent: false });
      },
      error: () => this.form.patchValue({ grafanaURL: g, alertmanagerURL: a }, { emitEvent: false })
    });
  }

  save(): void {
    if (this.form.invalid) return;
    const { rpoThresholdSeconds, throughputLowerBoundMBps, diagnosisRetentionDays, grafanaURL, alertmanagerURL, targetRtoMinutes, retain, retainDays, retainHours, tieringPolicy } = this.form.value;
    this.api.updateBackupDashboardSettings({ rpoThresholdSeconds, throughputLowerBoundMBps, diagnosisRetentionDays }).subscribe({
      next: () => {
        if (typeof grafanaURL === 'string') localStorage.setItem('grafanaURL', grafanaURL);
        if (typeof alertmanagerURL === 'string') localStorage.setItem('alertmanagerURL', alertmanagerURL);
        // 保存系统级策略键值
        const body: Record<string, any> = {
          'backup.targetRtoMinutes': targetRtoMinutes,
          'backup.retain': retain,
          'backup.retainDays': retainDays,
          'backup.retainHours': retainHours,
          'backup.tieringPolicy': tieringPolicy
        };
        this.api.updateSystemSettings(body).subscribe({
          next: () => this.msg.success('保存成功'),
          error: () => this.msg.warning('部分保存失败（系统策略）')
        });
      },
      error: () => this.msg.error('保存失败')
    });
  }
}