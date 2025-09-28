import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { RouterModule, Router } from '@angular/router';
import { NzButtonModule } from 'ng-zorro-antd/button';

@Component({
  selector: 'app-monitoring-overview',
  standalone: true,
  imports: [CommonModule, NzCardModule, NzIconModule, NzButtonModule, RouterModule],
  template: `
    <div class="overview">
      <nz-card nzTitle="监控与告警总览" class="section">
        <p>统一入口：监控配置、安装、健康检查、预检查、Grafana、告警聚合与管理。</p>
        <div class="quick-links">
          <button nz-button nzType="default" (click)="goto('config')"><i nz-icon nzType="setting"></i> 监控配置</button>
          <button nz-button nzType="default" (click)="goto('install')"><i nz-icon nzType="tool"></i> 安装向导</button>
          <button nz-button nzType="default" (click)="goto('health')"><i nz-icon nzType="dashboard"></i> 健康检查</button>
          <button nz-button nzType="default" (click)="goto('preflight')"><i nz-icon nzType="safety"></i> 预检查</button>
          <button nz-button nzType="default" (click)="goto('grafana')"><i nz-icon nzType="dashboard"></i> Grafana</button>
          <button nz-button nzType="default" (click)="goto('alerts')"><i nz-icon nzType="bell"></i> 告警聚合</button>
          <button nz-button nzType="default" (click)="goto('alerts-mgr')"><i nz-icon nzType="notification"></i> 告警管理</button>
        </div>
      </nz-card>
    </div>
  `,
  styles: [`
    .overview { padding: 8px; }
    .section { margin-bottom: 12px; }
    .quick-links { display: flex; flex-wrap: wrap; gap: 8px; }
  `]
})
export class MonitoringOverviewComponent {
  constructor(private router: Router) {}
  goto(path: string): void {
    this.router.navigate([`/operations/monitoring/${path}`]);
  }
}
