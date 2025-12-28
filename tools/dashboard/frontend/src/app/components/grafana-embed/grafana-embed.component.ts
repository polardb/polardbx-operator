import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { ApiService } from '../../services/api.service';

@Component({
  selector: 'app-grafana-embed',
  standalone: true,
  imports: [CommonModule, FormsModule, NzCardModule, NzFormModule, NzInputModule, NzButtonModule, NzIconModule, NzSelectModule, NzGridModule, NzDividerModule, NzAlertModule],
  template: `
    <div class="page-wrapper grafana-embed">
      <div class="page-header">
        <div class="title-block">
          <h2>
            <i nz-icon nzType="dashboard" class="page-icon"></i>
            Grafana
          </h2>
          <p>集成 Grafana 仪表盘，可视化监控 PolarDB-X 集群状态和性能指标</p>
        </div>
      </div>

      <div class="page-content">
        <nz-card class="dashboard-card" nzTitle="Grafana 集成配置" [nzExtra]="configExtra">
          <ng-template #configExtra>
            <i nz-icon nzType="dashboard" class="section-icon"></i>
          </ng-template>
          
          <nz-alert 
            nzType="info" 
            nzMessage="配置提示" 
            nzDescription="请确保 Grafana 服务可访问，并已配置相应的数据源和仪表盘。首次使用请参考官方监控安装文档。"
            nzShowIcon
            [nzCloseable]="true"
            class="config-alert">
          </nz-alert>

          <div class="config-section">
            <nz-row [nzGutter]="16" nzAlign="middle">
              <nz-col [nzSpan]="12">
                <nz-form-item>
                  <nz-form-label [nzSpan]="6">Grafana 地址</nz-form-label>
                  <nz-form-control [nzSpan]="18">
                    <input nz-input [(ngModel)]="grafanaUrl" placeholder="http://grafana.polardbx-monitor:3000" />
                  </nz-form-control>
                </nz-form-item>
              </nz-col>
              <nz-col [nzSpan]="12">
                <div class="action-buttons">
                  <button nz-button nzType="primary" (click)="saveUrl()">
                    <i nz-icon nzType="save"></i>
                    保存配置
                  </button>
                  <button nz-button nzType="default" *ngIf="grafanaUrl" (click)="openExternal()">
                    <i nz-icon nzType="link"></i>
                    新窗口打开
                  </button>
                </div>
              </nz-col>
            </nz-row>
          </div>

          <nz-divider></nz-divider>

          <div class="dashboard-display">
            <div class="grafana-link-container">
              <i nz-icon nzType="dashboard" class="grafana-icon"></i>
              <h3>Grafana 监控大盘</h3>
              <p *ngIf="grafanaUrl">点击下方按钮在新窗口中打开 Grafana 仪表盘</p>
              <p *ngIf="!grafanaUrl">请在上方输入 Grafana 服务地址</p>
              <div class="grafana-actions">
                <button nz-button nzType="primary" nzSize="large" [disabled]="!grafanaUrl" (click)="openExternal()">
                  <i nz-icon nzType="link"></i>
                  打开 Grafana 仪表盘
                </button>
              </div>
              <nz-alert 
                *ngIf="grafanaUrl"
                nzType="info" 
                nzMessage="安全提示"
                nzDescription="由于浏览器安全策略限制，Grafana 无法在页面内嵌入显示。请点击上方按钮在新标签页中打开。"
                nzShowIcon
                class="security-alert">
              </nz-alert>
            </div>
          </div>
        </nz-card>

        <nz-card class="version-card" nzTitle="仪表盘版本管理" [nzExtra]="versionExtra">
          <ng-template #versionExtra>
            <i nz-icon nzType="history" class="section-icon"></i>
          </ng-template>
          
          <div class="version-controls">
            <nz-row [nzGutter]="16" nzAlign="middle">
              <nz-col [nzSpan]="4">
                <div class="control-item">
                  <button nz-button nzType="default" (click)="reloadDashboards()">
                    <i nz-icon nzType="reload"></i>
                    刷新列表
                  </button>
                </div>
              </nz-col>
              <nz-col [nzSpan]="8">
                <div class="control-item">
                  <label class="control-label">选择仪表盘：</label>
                  <nz-select [(ngModel)]="selectedDashboard" (ngModelChange)="loadVersions()" nzPlaceHolder="请选择仪表盘" class="control-select">
                    <nz-option *ngFor="let d of dashboards" [nzValue]="d.name" [nzLabel]="d.name + ' (' + d.versions + ' 个版本)'"></nz-option>
                  </nz-select>
                </div>
              </nz-col>
              <nz-col [nzSpan]="6">
                <div class="control-item">
                  <label class="control-label">目标版本：</label>
                  <nz-select [(ngModel)]="selectedVersion" nzPlaceHolder="选择版本" class="control-select">
                    <nz-option *ngFor="let v of versions" [nzValue]="v" [nzLabel]="'v' + (v | number: '1.0-0')"></nz-option>
                  </nz-select>
                </div>
              </nz-col>
              <nz-col [nzSpan]="6">
                <div class="control-item">
                  <button nz-button nzType="primary" nzDanger [disabled]="!selectedDashboard || !selectedVersion" (click)="rollback()">
                    <i nz-icon nzType="undo"></i>
                    版本回滚
                  </button>
                </div>
              </nz-col>
            </nz-row>
          </div>
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

    .page-content {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }
    
    .dashboard-card, .version-card {
      background: #fff;
      border-radius: 10px;
      box-shadow: 0 2px 12px rgba(15, 23, 42, 0.05);
      border: 1px solid #e0e3e8;
    }
    
    .section-icon {
      font-size: 16px;
      color: var(--primary-color, #4a7c9b);
    }
    
    .config-alert {
      margin-bottom: 16px;
    }
    
    .config-section {
      margin-bottom: 16px;
    }
    
    .action-buttons {
      display: flex;
      gap: 8px;
      justify-content: flex-end;
    }
    
    .dashboard-display {
      margin-top: 16px;
    }
    
    .grafana-link-container {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      padding: 60px 20px;
      text-align: center;
      background: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 100%);
      border-radius: 12px;
      border: 2px dashed #cbd5e1;
    }
    
    .grafana-icon {
      font-size: 64px;
      color: #4a7c9b;
      margin-bottom: 20px;
    }
    
    .grafana-link-container h3 {
      margin: 0 0 12px 0;
      font-size: 20px;
      font-weight: 600;
      color: #1f2937;
    }
    
    .grafana-link-container p {
      margin: 0 0 24px 0;
      font-size: 14px;
      color: #6b7280;
    }
    
    .grafana-actions {
      margin-bottom: 24px;
    }
    
    .grafana-actions button {
      padding: 0 32px;
      height: 44px;
      font-size: 16px;
    }
    
    .security-alert {
      max-width: 500px;
    }
    
    .version-controls {
      margin-top: 8px;
    }
    
    .control-item {
      display: flex;
      align-items: center;
      gap: 8px;
      height: 100%;
    }
    
    .control-label {
      font-size: 14px;
      color: rgba(0, 0, 0, 0.85);
      font-weight: 500;
      min-width: 96px;
      text-align: right;
    }
    
    .control-select {
      width: 100%;
    }
    
    /* 响应式设计 */
    @media (max-width: 1200px) {
      .page-content {
        max-width: 100%;
        padding: 0 8px;
      }
    }
    
    @media (max-width: 768px) {
      .page-wrapper { padding: 12px; }
      
      .grafana-link-container {
        padding: 40px 16px;
      }
      
      .grafana-icon {
        font-size: 48px;
      }
      
      .action-buttons {
        flex-direction: column;
        align-items: stretch;
      }
    }
  `]
})
export class GrafanaEmbedComponent implements OnInit {
  grafanaUrl = '';
  dashboards: { name: string; versions: number }[] = [];
  selectedDashboard: string | null = null;
  versions: number[] = [];
  selectedVersion: number | null = null;

  constructor(private api: ApiService, private message: NzMessageService) {}

  ngOnInit(): void {
    this.grafanaUrl = localStorage.getItem('grafanaURL') || '';
    this.reloadDashboards();
  }

  saveUrl(): void {
    if (this.grafanaUrl) {
      localStorage.setItem('grafanaURL', this.grafanaUrl);
      this.message.success('Grafana 地址已保存');
    } else {
      this.message.warning('请输入有效的 Grafana 地址');
    }
  }

  openExternal(): void {
    if (this.grafanaUrl) {
      window.open(this.grafanaUrl, '_blank', 'noopener,noreferrer');
    }
  }

  reloadDashboards(): void {
    this.api.listDashboards().subscribe(r => { this.dashboards = r?.items || []; });
  }

  loadVersions(): void {
    this.selectedVersion = null;
    if (!this.selectedDashboard) { this.versions = []; return; }
    this.api.listDashboardVersions(this.selectedDashboard).subscribe(r => { this.versions = r?.versions || []; });
  }

  rollback(): void {
    if (!this.selectedDashboard || !this.selectedVersion) return;
    if (!confirm(`确认将仪表盘 "${this.selectedDashboard}" 回滚到版本 v${this.selectedVersion}？\n\n此操作会覆盖当前配置，请谨慎操作。`)) return;
    
    this.api.rollbackDashboard(this.selectedDashboard, this.selectedVersion).subscribe({
      next: () => {
        this.message.success(`仪表盘 "${this.selectedDashboard}" 已成功回滚到版本 v${this.selectedVersion}`);
        this.loadVersions();
      },
      error: (error) => {
        console.error('版本回滚失败:', error);
        this.message.error('版本回滚失败，请检查网络连接或重试');
      }
    });
  }
}