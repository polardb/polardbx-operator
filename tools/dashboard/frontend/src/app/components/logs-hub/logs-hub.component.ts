import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule, Router, ActivatedRoute, NavigationEnd } from '@angular/router';
import { NzTabsModule } from 'ng-zorro-antd/tabs';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { filter } from 'rxjs/operators';

@Component({
  selector: 'app-logs-hub',
  standalone: true,
  imports: [CommonModule, RouterModule, NzTabsModule, NzIconModule],
  template: `
    <div class="logs-hub">
      <nz-tabset
        nzType="card"
        class="tabs"
        [nzTabBarGutter]="8"
        [nzSelectedIndex]="selectedIndex"
        (nzSelectedIndexChange)="onTabChange($event)">
        <nz-tab nzTitle="安装向导">
          <ng-template #nzTabHeading>
            <i nz-icon nzType="tool"></i>
            <span>安装向导</span>
          </ng-template>
        </nz-tab>
        <nz-tab nzTitle="服务仪表盘">
          <ng-template #nzTabHeading>
            <i nz-icon nzType="dashboard"></i>
            <span>服务仪表盘</span>
          </ng-template>
        </nz-tab>
        <nz-tab nzTitle="采集器管理">
          <ng-template #nzTabHeading>
            <i nz-icon nzType="cluster"></i>
            <span>采集器管理</span>
          </ng-template>
        </nz-tab>
        <nz-tab nzTitle="策略管理">
          <ng-template #nzTabHeading>
            <i nz-icon nzType="setting"></i>
            <span>策略管理</span>
          </ng-template>
        </nz-tab>
        <nz-tab nzTitle="日志查询">
          <ng-template #nzTabHeading>
            <i nz-icon nzType="search"></i>
            <span>日志查询</span>
          </ng-template>
        </nz-tab>
      </nz-tabset>

      <div class="outlet">
        <router-outlet></router-outlet>
      </div>
    </div>
  `,
  styles: [`
    .logs-hub {
      padding: 24px;
      background: #f9fafc;
      min-height: calc(100vh - 64px);
    }

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
      padding: 0; /* 避免子页面“二次缩进”导致布局不齐 */
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

    /* 响应式设计 */
    @media (max-width: 768px) {
      .logs-hub {
        padding: 16px;
      }
    }
  `]
})
export class LogsHubComponent implements OnInit {
  selectedIndex = 0;
  totalCollectors = 0;
  activePolicies = 0;
  private paths = ['install', 'dashboard', 'collectors', 'strategies', 'search'];

  constructor(private router: Router, private route: ActivatedRoute) {}

  ngOnInit(): void {
    this.updateSelectedFromUrl();
    this.router.events.pipe(filter(e => e instanceof NavigationEnd)).subscribe(() => this.updateSelectedFromUrl());
  }

  onTabChange(idx: number): void {
    const p = this.paths[idx] || 'dashboard';
    this.router.navigate([p], { relativeTo: this.route });
  }

  private updateSelectedFromUrl(): void {
    const child = this.route.firstChild;
    const seg = child?.snapshot?.url?.[0]?.path || 'install';
    const idx = this.paths.indexOf(seg);
    this.selectedIndex = idx >= 0 ? idx : 0;
  }
}


