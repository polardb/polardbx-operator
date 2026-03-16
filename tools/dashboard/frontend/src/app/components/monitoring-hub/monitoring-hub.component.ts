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
        <nz-tab nzTitle="Dashboard 模板">
          <ng-template #nzTabHeading>
            <i nz-icon nzType="appstore"></i>
            <span>Dashboard 模板</span>
          </ng-template>
        </nz-tab>
        <nz-tab nzTitle="告警模板">
          <ng-template #nzTabHeading>
            <i nz-icon nzType="highlight"></i>
            <span>告警模板</span>
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
            <i nz-icon nzType="alert" nzTheme="outline"></i>
            <span>告警管理</span>
          </ng-template>
        </nz-tab>
        <nz-tab nzTitle="告警规则">
          <ng-template #nzTabHeading>
            <i nz-icon nzType="fire"></i>
            <span>告警规则</span>
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
    /* 统一 monitoring 子页面的外层留白/背景/容器 */
    .monitoring-hub { padding: 24px; background: #f9fafc; min-height: calc(100vh - 64px); }

    .tabs {
      background: #fff;
      margin-bottom: 12px;
      border-radius: 10px;
      padding: 8px 12px 0;
      box-shadow: 0 2px 10px rgba(15, 23, 42, 0.04);
    }

    .outlet {
      background: #f9fafc;
      border: 1px solid #e0e3e8;
      border-radius: 10px;
      padding: 0; /* 避免不同子页面额外“二次缩进”导致大小不一 */
      overflow: hidden;
      box-shadow: 0 2px 12px rgba(15, 23, 42, 0.04);
    }

    ::ng-deep .ant-tabs-tab {
      .anticon {
        margin-right: 8px;
      }
    }

    /* 让 tab 高度与按钮/输入框更一致 */
    ::ng-deep .ant-tabs-nav .ant-tabs-tab {
      padding: 10px 14px;
      line-height: 20px;
    }

    /* tab 超多时允许横向滚动，不挤压换行造成“高低不齐” */
    ::ng-deep .ant-tabs-nav {
      overflow-x: auto;
      overflow-y: hidden;
      scrollbar-width: thin;
    }
    ::ng-deep .ant-tabs-nav::-webkit-scrollbar { height: 6px; }
    ::ng-deep .ant-tabs-nav::-webkit-scrollbar-thumb { background: rgba(0,0,0,0.18); border-radius: 999px; }
  `]
})
export class MonitoringHubComponent implements OnInit {
  selectedIndex = 0;
  private paths = ['enable-wizard', 'health', 'preflight', 'dashboards', 'alert-templates', 'grafana', 'alerts', 'alerts-mgr', 'prometheus-rules', 'alert-receivers'];

  constructor(private router: Router, private route: ActivatedRoute) {}

  ngOnInit(): void {
    this.updateSelectedFromUrl();
    this.router.events.pipe(filter(e => e instanceof NavigationEnd)).subscribe(() => this.updateSelectedFromUrl());
  }

  onTabChange(idx: number): void {
  const p = this.paths[idx] || 'enable-wizard';
    this.router.navigate([p], { relativeTo: this.route });
  }

  private updateSelectedFromUrl(): void {
    const child = this.route.firstChild;
    const seg = child?.snapshot?.url?.[0]?.path || 'enable-wizard';
    const idx = this.paths.indexOf(seg);
    this.selectedIndex = idx >= 0 ? idx : 0;
  }
}
