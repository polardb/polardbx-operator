import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzDescriptionsModule } from 'ng-zorro-antd/descriptions';
import { ApiService } from '../../services/api.service';

@Component({
  selector: 'app-monitoring-preflight',
  standalone: true,
  imports: [CommonModule, NzCardModule, NzButtonModule, NzIconModule, NzSpinModule, NzTagModule, NzGridModule, NzDescriptionsModule],
  template: `
    <div class="page-wrapper monitoring-preflight">
      <div class="page-header">
        <div class="title-block">
          <h2>
            <i nz-icon nzType="safety-certificate" class="page-icon"></i>
            预检查
          </h2>
          <p>
            可用区分布、时钟同步等环境信息一览
            <button nz-button nzType="default" nzSize="small" (click)="reload()" [nzLoading]="loading">
              <i nz-icon nzType="reload"></i>
              刷新
            </button>
            <span class="timestamp" *ngIf="!loading && data?.timestamp">基准时间：{{ data?.timestamp }}</span>
          </p>
        </div>
      </div>

      <div class="page-content">
        <div *ngIf="loading" class="loading-container">
          <nz-spin nzSize="large">
            <div class="loading-tip">正在检查环境...</div>
          </nz-spin>
        </div>

        <div class="check-sections" *ngIf="!loading">
          <nz-card class="check-card" nzTitle="可用区分布检查" [nzExtra]="azExtra">
            <ng-template #azExtra>
              <i nz-icon nzType="global" class="section-icon"></i>
            </ng-template>
            <nz-descriptions nzBordered [nzColumn]="2">
              <nz-descriptions-item nzTitle="节点总数">{{ data?.az?.nodeCount || 0 }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="有区标记节点">{{ data?.az?.nodesWithZone || 0 }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="可用区数量">{{ data?.az?.count || 0 }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="多可用区部署">
                <nz-tag [nzColor]="data?.az?.hasMultiple ? 'success' : 'warning'">
                  {{ data?.az?.hasMultiple ? '是' : '否' }}
                </nz-tag>
              </nz-descriptions-item>
              <nz-descriptions-item nzTitle="可用区列表" [nzSpan]="2">
                {{ (data?.az?.zones || []).join(', ') || '未检测到' }}
              </nz-descriptions-item>
            </nz-descriptions>
          </nz-card>

          <nz-card class="check-card" nzTitle="时钟同步检查" [nzExtra]="clockExtra">
            <ng-template #clockExtra>
              <i nz-icon nzType="clock-circle" class="section-icon"></i>
            </ng-template>
            <nz-descriptions nzBordered [nzColumn]="2">
              <nz-descriptions-item nzTitle="控制面时间">{{ data?.clock?.controllerTime || '-' }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="漂移校验">{{ data?.clock?.skewAssessed ? '已完成' : '未完成' }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="同步状态" [nzSpan]="2">
                <nz-tag [nzColor]="data?.clock?.ok ? 'success' : 'error'">
                  {{ data?.clock?.ok ? '正常' : '异常' }}
                </nz-tag>
                <span style="margin-left: 8px;">{{ data?.clock?.message || '-' }}</span>
              </nz-descriptions-item>
            </nz-descriptions>
          </nz-card>

          <nz-card class="check-card" nzTitle="IOPS 性能检查" [nzExtra]="iopsExtra">
            <ng-template #iopsExtra>
              <i nz-icon nzType="dashboard" class="section-icon"></i>
            </ng-template>
            <nz-descriptions nzBordered [nzColumn]="2">
              <nz-descriptions-item nzTitle="性能估算">占位</nz-descriptions-item>
              <nz-descriptions-item nzTitle="IOPS 状态">
                <nz-tag nzColor="default">占位</nz-tag>
              </nz-descriptions-item>
              <nz-descriptions-item nzTitle="详细说明" [nzSpan]="2">
                占位：请在监控系统查看磁盘 IOPS
              </nz-descriptions-item>
            </nz-descriptions>
          </nz-card>
        </div>
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
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 16px;
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
      display: flex;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
    }
    
    .timestamp {
      color: rgba(0, 0, 0, 0.6);
      font-size: 12px;
    }

    .page-content {
      display: flex;
      flex-direction: column;
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
    
    .check-sections {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }
    
    .check-card {
      background: #fff;
      border-radius: 10px;
      box-shadow: 0 2px 12px rgba(15, 23, 42, 0.05);
      border: 1px solid #e0e3e8;
    }
    
    /* 响应式设计 */
    @media (max-width: 768px) {
      .page-wrapper { padding: 12px; }
    }
  `]
})
export class MonitoringPreflightComponent implements OnInit {
  loading = false;
  data: any = null;

  constructor(private api: ApiService) {}

  ngOnInit(): void { this.reload(); }

  reload(): void {
    this.loading = true;
    this.api.getMonitoringPreflight().subscribe({
      next: (d) => { this.data = d; },
      error: () => { this.data = null; },
      complete: () => { this.loading = false; }
    });
  }
}

