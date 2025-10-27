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
import { SafeUrlPipe } from '../../pipes/safe-url.pipe';
import { ApiService } from '../../services/api.service';

@Component({
  selector: 'app-grafana-embed',
  standalone: true,
  imports: [CommonModule, FormsModule, NzCardModule, NzFormModule, NzInputModule, NzButtonModule, NzIconModule, NzSelectModule, NzGridModule, NzDividerModule, NzAlertModule, SafeUrlPipe],
  template: `
    <div class="grafana-embed">
      <div class="page-header">
        <div class="header-content">
          <h1 class="page-title">
            <i nz-icon nzType="dashboard" class="page-icon"></i>
            监控大盘
          </h1>
          <p class="page-description">集成 Grafana 仪表盘，可视化监控 PolarDB-X 集群状态和性能指标</p>
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

          <div class="dashboard-display" *ngIf="grafanaUrl; else emptyHint">
            <div class="frame-container">
              <iframe [src]="grafanaUrl | safeUrl" title="Grafana Dashboard" referrerpolicy="no-referrer" loading="lazy"></iframe>
            </div>
          </div>
          
          <ng-template #emptyHint>
            <div class="empty-state">
              <i nz-icon nzType="dashboard" class="empty-icon"></i>
              <h3>未配置 Grafana 地址</h3>
              <p>请在上方输入 Grafana 服务地址以查看监控大盘</p>
            </div>
          </ng-template>
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
    .grafana-embed {
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
      display: flex;
      flex-direction: column;
      gap: 16px;
    }
    
    .dashboard-card, .version-card {
      background: #fff;
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border: 1px solid #e0e0e0;
    }
    
    .section-icon {
      font-size: 16px;
      color: #1890ff;
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
    
    .frame-container {
      position: relative;
      width: 100%;
      height: calc(100vh - 450px);
      min-height: 500px;
      border: 1px solid #e0e0e0;
      border-radius: 6px;
      overflow: hidden;
      background: #f5f5f5;
    }
    
    .frame-container iframe {
      width: 100%;
      height: 100%;
      border: 0;
      background: #fff;
    }
    
    .empty-state {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      padding: 80px 20px;
      color: rgba(0, 0, 0, 0.45);
      text-align: center;
    }
    
    .empty-icon {
      font-size: 48px;
      color: #d9d9d9;
      margin-bottom: 16px;
    }
    
    .empty-state h3 {
      margin: 0 0 8px 0;
      font-size: 16px;
      color: rgba(0, 0, 0, 0.65);
    }
    
    .empty-state p {
      margin: 0;
      font-size: 14px;
      color: rgba(0, 0, 0, 0.45);
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
      .grafana-embed {
        padding: 8px;
      }
      
      .frame-container {
        height: calc(100vh - 400px);
        min-height: 300px;
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