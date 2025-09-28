import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { RouterModule, Router } from '@angular/router';

@Component({
  selector: 'app-logs-overview',
  standalone: true,
  imports: [CommonModule, NzCardModule, NzIconModule, NzButtonModule, RouterModule],
  template: `
    <div class="overview">
      <nz-card nzTitle="日志与采集总览" class="section">
        <p>统一入口：服务仪表盘、采集器管理、ILM 策略、日志查询、安装向导。</p>
        <div class="quick-links">
          <button nz-button nzType="default" (click)="goto('dashboard')"><i nz-icon nzType="dashboard"></i> 服务仪表盘</button>
          <button nz-button nzType="default" (click)="goto('collectors')"><i nz-icon nzType="cluster"></i> 采集器管理</button>
          <button nz-button nzType="default" (click)="goto('ilm')"><i nz-icon nzType="clock-circle"></i> ILM 策略</button>
          <button nz-button nzType="default" (click)="goto('search')"><i nz-icon nzType="search"></i> 日志查询</button>
          <button nz-button nzType="default" (click)="goto('install')"><i nz-icon nzType="tool"></i> 安装向导</button>
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
export class LogsOverviewComponent {
  constructor(private router: Router) {}
  goto(path: string): void {
    this.router.navigate([`/operations/logs/${path}`]);
  }
}


