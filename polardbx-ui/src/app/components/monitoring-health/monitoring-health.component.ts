import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { ApiService } from '../../services/api.service';

@Component({
  selector: 'app-monitoring-health',
  standalone: true,
  imports: [CommonModule, NzCardModule, NzTableModule, NzButtonModule, NzIconModule, NzSpinModule, NzTagModule],
  template: `
    <div class="health">
      <div class="page-header">
        <div class="header-content">
          <h1 class="page-title">
            <i nz-icon nzType="heart" class="page-icon"></i>
            监控健康检查
          </h1>
          <p class="page-description">检测 Prometheus / Grafana / Alertmanager 组件状态</p>
        </div>
      </div>

      <div class="page-content">
        <nz-card class="control-panel" nzTitle="检查操作">
          <div class="toolbar">
            <button nz-button nzType="primary" (click)="reload()" [nzLoading]="loading">
              <i nz-icon nzType="reload"></i>
              刷新状态
            </button>
          </div>
        </nz-card>

        <div *ngIf="loading" class="loading-container">
          <nz-spin nzSize="large">
            <div class="loading-tip">正在检查组件状态...</div>
          </nz-spin>
        </div>

        <nz-card class="table-card" nzTitle="组件状态" *ngIf="!loading">
          <nz-table #healthTable [nzData]="rows" nzSize="middle" [nzShowPagination]="false">
            <thead>
              <tr>
                <th>组件</th>
                <th>状态</th>
                <th>详情</th>
              </tr>
            </thead>
            <tbody>
              <tr *ngFor="let row of healthTable.data">
                <td>
                  <strong>{{ row.component }}</strong>
                </td>
                <td>
                  <nz-tag [nzColor]="row.ready ? 'success' : 'error'">
                    <i nz-icon [nzType]="row.ready ? 'check-circle' : 'close-circle'"></i>
                    {{ row.ready ? '就绪' : '未就绪' }}
                  </nz-tag>
                </td>
                <td>
                  <span class="detail-text">{{ row.detail }}</span>
                </td>
              </tr>
            </tbody>
          </nz-table>
        </nz-card>
      </div>
    </div>
  `,
  styles: [`
    .health {
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
    
    .control-panel, .table-card {
      background: #fff;
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border: 1px solid #e0e0e0;
    }
    
    .toolbar {
      display: flex;
      align-items: center;
      gap: 16px;
    }
    
    .loading-container {
      display: flex;
      justify-content: center;
      align-items: center;
      padding: 80px 0;
    }

    .loading-tip {
      margin-top: 12px;
      text-align: center;
      color: rgba(0,0,0,0.65);
      letter-spacing: 0.5px;
    }
    
    .detail-text {
      color: rgba(0, 0, 0, 0.6);
      font-size: 13px;
    }
    
    /* 响应式设计 */
    @media (max-width: 768px) {
      .health {
        padding: 16px;
      }
      
      .toolbar {
        flex-direction: column;
        align-items: flex-start;
        gap: 8px;
      }
    }
  `]
})
export class MonitoringHealthComponent implements OnInit {
  loading = false;
  rows: { component: string; ready: boolean; detail?: string }[] = [];
  cols = ['component', 'ready', 'detail'];

  constructor(private api: ApiService) {}

  ngOnInit(): void { this.reload(); }

  reload() {
    this.loading = true;
    this.api.getSystemContext().subscribe({
      next: (ctx) => {
        const ns = ctx?.defaultNamespace;
        this.api.getMonitoringStatus(ns || undefined).subscribe({
          next: (s: any) => {
            const comp = s?.components || {};
            this.rows = [
              { component: 'Prometheus', ready: !!comp?.prometheus?.ready, detail: `readyReplicas=${comp?.prometheus?.readyReplicas ?? '-'} / replicas=${comp?.prometheus?.replicas ?? '-'}` },
              { component: 'Grafana', ready: !!comp?.grafana?.ready, detail: `readyReplicas=${comp?.grafana?.readyReplicas ?? '-'} / replicas=${comp?.grafana?.replicas ?? '-'}` },
              { component: 'Alertmanager', ready: !!comp?.alertmanager?.configured, detail: comp?.alertmanager?.configured ? '配置已发现' : '未配置' }
            ];
          },
          error: () => { this.rows = []; this.loading = false; },
          complete: () => { this.loading = false; }
        });
      },
      error: () => { this.rows = []; this.loading = false; }
    });
  }
}