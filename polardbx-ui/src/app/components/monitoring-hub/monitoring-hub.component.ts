import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule, Router, ActivatedRoute, NavigationEnd } from '@angular/router';
import { NzTabsModule } from 'ng-zorro-antd/tabs';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { filter } from 'rxjs/operators';

@Component({
  selector: 'app-monitoring-hub',
  standalone: true,
  imports: [CommonModule, RouterModule, NzTabsModule, NzIconModule],
  template: `
    <div class="monitoring-hub">
      <nz-tabset nzType="card" class="tabs" [nzTabBarGutter]="8"
                 [nzSelectedIndex]="selectedIndex"
                 (nzSelectedIndexChange)="onTabChange($event)">
        <nz-tab nzTitle="监控开启向导">
          <ng-template #nzTabHeading>
            <i nz-icon nzType="tool"></i>
            <span>监控开启向导</span>
          </ng-template>
        </nz-tab>
        <nz-tab nzTitle="配置">
          <ng-template #nzTabHeading>
            <i nz-icon nzType="setting"></i>
            <span>配置</span>
          </ng-template>
        </nz-tab>
        <nz-tab nzTitle="健康检查">
          <ng-template #nzTabHeading>
            <i nz-icon nzType="heart"></i>
            <span>健康检查</span>
          </ng-template>
        </nz-tab>
        <nz-tab nzTitle="预检查">
          <ng-template #nzTabHeading>
            <i nz-icon nzType="audit"></i>
            <span>预检查</span>
          </ng-template>
        </nz-tab>
        <nz-tab nzTitle="Grafana">
          <ng-template #nzTabHeading>
            <i nz-icon nzType="line-chart"></i>
            <span>Grafana</span>
          </ng-template>
        </nz-tab>
        <nz-tab nzTitle="告警聚合">
          <ng-template #nzTabHeading>
            <i nz-icon nzType="bell"></i>
            <span>告警聚合</span>
          </ng-template>
        </nz-tab>
        <nz-tab nzTitle="告警管理">
          <ng-template #nzTabHeading>
            <i nz-icon nzType="alert"></i>
            <span>告警管理</span>
          </ng-template>
        </nz-tab>
        <nz-tab nzTitle="PrometheusRule">
          <ng-template #nzTabHeading>
            <i nz-icon nzType="fire"></i>
            <span>PrometheusRule</span>
          </ng-template>
        </nz-tab>
        <nz-tab nzTitle="告警接收器">
          <ng-template #nzTabHeading>
            <i nz-icon nzType="mail"></i>
            <span>告警接收器</span>
          </ng-template>
        </nz-tab>
      </nz-tabset>

      <div class="outlet">
        <router-outlet></router-outlet>
      </div>
    </div>
  `,
  styles: [`
    .monitoring-hub { padding: 8px 16px; background: #f5f5f5; min-height: 100vh; }
    .tabs { background: #fff; margin-bottom: 8px; }
    .outlet { background: #fff; border: 1px solid #e0e0e0; border-radius: 8px; padding: 12px; }

    ::ng-deep .ant-tabs-tab {
      .anticon {
        margin-right: 8px;
      }
    }
  `]
})
export class MonitoringHubComponent implements OnInit {
  selectedIndex = 0;
  private paths = ['enable-wizard','config', 'health', 'preflight', 'grafana', 'alerts', 'alerts-mgr', 'prometheus-rules', 'alert-receivers'];

  constructor(private router: Router, private route: ActivatedRoute) {}

  ngOnInit(): void {
    this.updateSelectedFromUrl();
    this.router.events.pipe(filter(e => e instanceof NavigationEnd)).subscribe(() => this.updateSelectedFromUrl());
  }

  onTabChange(idx: number): void {
    const p = this.paths[idx] || 'config';
    this.router.navigate([p], { relativeTo: this.route });
  }

  private updateSelectedFromUrl(): void {
    const child = this.route.firstChild;
    const seg = child?.snapshot?.url?.[0]?.path || 'enable-wizard';
    const idx = this.paths.indexOf(seg);
    this.selectedIndex = idx >= 0 ? idx : 0;
  }
}
