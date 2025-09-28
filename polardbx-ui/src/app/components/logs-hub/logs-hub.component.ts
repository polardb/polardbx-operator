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
      <!-- 页面头部已移除 -->

      <!-- 导航标签 -->
      <div class="content">
        <nz-tabset nzType="card" class="main-tabs" [nzTabBarGutter]="8"
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
          <!-- 移除 ILM 策略 Tab -->
          <nz-tab nzTitle="日志查询">
            <ng-template #nzTabHeading>
              <i nz-icon nzType="search"></i>
              <span>日志查询</span>
            </ng-template>
          </nz-tab>
        </nz-tabset>

        <!-- 内容区域 -->
        <div class="tab-content">
          <router-outlet></router-outlet>
        </div>
      </div>
    </div>
  `,
  styles: [`
    .logs-hub {
      padding: 16px;
      background: #f5f5f5;
      min-height: 100vh;
      max-width: 1400px;
      margin: 0 auto;
    }

    /* 页面头部样式已移除 */

    /* 内容区域 */
    .content {
      background: white;
      border-radius: 8px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.06);
    }

    .main-tabs {
      ::ng-deep .ant-tabs-nav {
        margin-bottom: 0;
        padding: 0 16px;
      }

      ::ng-deep .ant-tabs-tab {
        padding: 12px 16px;
        
        .anticon {
          margin-right: 8px;
        }
      }
    }

    .tab-content {
      padding: 16px;
    }

    /* 响应式设计 */
    @media (max-width: 768px) {
      .logs-hub {
        padding: 12px;
      }

      .tab-content {
        padding: 12px;
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


