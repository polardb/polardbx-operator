import { Component, OnInit, OnDestroy, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatToolbarModule } from '@angular/material/toolbar';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatMenuModule } from '@angular/material/menu';
import { MatBadgeModule } from '@angular/material/badge';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatDividerModule } from '@angular/material/divider';
import { ThemePalette } from '@angular/material/core';
import { MatDialogModule, MatDialog } from '@angular/material/dialog';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { Router, NavigationEnd } from '@angular/router';
import { Subscription } from 'rxjs';
import { filter } from 'rxjs/operators';
import { LoadingService } from '../../services/loading.service';
import { PerformanceService, PerformanceMetrics } from '../../services/performance.service';
import { NotificationService } from '../../services/notification.service';
import { PerformanceMonitorComponent } from '../performance-monitor/performance-monitor.component';

@Component({
  selector: 'app-enhanced-toolbar',
  standalone: true,
  imports: [
    CommonModule,
    MatToolbarModule,
    MatIconModule,
    MatButtonModule,
    MatMenuModule,
    MatBadgeModule,
    MatTooltipModule,
    MatDialogModule,
    MatSnackBarModule,
    MatDividerModule
  ],
  template: `
    <mat-toolbar color="primary" class="enhanced-toolbar">
      <!-- 左侧：Logo 和标题 -->
      <div class="toolbar-left">
        <mat-icon class="app-icon">database</mat-icon>
        <span class="app-title">PolarDB-X 运维平台</span>
        
        <!-- 面包屑导航 -->
        <div class="breadcrumb" *ngIf="currentPageTitle">
          <mat-icon class="breadcrumb-separator">chevron_right</mat-icon>
          <span class="breadcrumb-title">{{ currentPageTitle }}</span>
        </div>
      </div>

      <!-- 中间：搜索和快捷操作 -->
      <div class="toolbar-center">
        <!-- 全局加载指示器 -->
        <div class="loading-indicator" *ngIf="hasActiveLoading$ | async">
          <mat-icon class="loading-icon spinning">sync</mat-icon>
          <span class="loading-text">处理中...</span>
        </div>
      </div>

      <!-- 右侧：状态和操作 -->
      <div class="toolbar-right">
        <!-- 性能监控 -->
        <button 
          mat-icon-button 
          (click)="openPerformanceMonitor()" 
          matTooltip="性能监控"
          class="performance-button"
          [class.performance-warning]="isPerformanceWarning()"
          [class.performance-error]="isPerformanceError()">
          <mat-icon 
            [matBadge]="getPerformanceScore()" 
            [matBadgeHidden]="!showPerformanceBadge()"
            matBadgeSize="small"
            [matBadgeColor]="getPerformanceBadgeColor()">
            speed
          </mat-icon>
        </button>

        <!-- 错误计数 -->
        <button 
          mat-icon-button 
          *ngIf="errorCount > 0"
          (click)="showErrorDetails()"
          matTooltip="查看错误详情"
          class="error-button">
          <mat-icon 
            [matBadge]="errorCount" 
            matBadgeSize="small"
            matBadgeColor="warn">
            error_outline
          </mat-icon>
        </button>

        <!-- 连接状态 -->
        <div class="connection-status" [class.connected]="isConnected">
          <mat-icon class="status-icon">{{ getConnectionIcon() }}</mat-icon>
          <span class="status-text">{{ getConnectionText() }}</span>
        </div>

        <!-- 用户菜单 -->
        <button mat-icon-button [matMenuTriggerFor]="userMenu" matTooltip="用户菜单">
          <mat-icon>account_circle</mat-icon>
        </button>
        <mat-menu #userMenu="matMenu">
          <button mat-menu-item (click)="openSettings()">
            <mat-icon>settings</mat-icon>
            <span>设置</span>
          </button>
          <button mat-menu-item (click)="openHelp()">
            <mat-icon>help</mat-icon>
            <span>帮助</span>
          </button>
          <button mat-menu-item (click)="exportLogs()">
            <mat-icon>download</mat-icon>
            <span>导出日志</span>
          </button>
          <mat-divider></mat-divider>
          <button mat-menu-item (click)="showAbout()">
            <mat-icon>info</mat-icon>
            <span>关于</span>
          </button>
        </mat-menu>

        <!-- 主题切换 -->
        <button 
          mat-icon-button 
          (click)="toggleTheme()" 
          matTooltip="切换主题">
          <mat-icon>{{ isDarkTheme ? 'light_mode' : 'dark_mode' }}</mat-icon>
        </button>

        <!-- 全屏切换 -->
        <button 
          mat-icon-button 
          (click)="toggleFullscreen()" 
          matTooltip="全屏切换">
          <mat-icon>{{ isFullscreen ? 'fullscreen_exit' : 'fullscreen' }}</mat-icon>
        </button>
      </div>
    </mat-toolbar>
  `,
  styleUrls: ['./enhanced-toolbar.component.scss']
})
export class EnhancedToolbarComponent implements OnInit, OnDestroy {
  private router = inject(Router);
  private dialog = inject(MatDialog);
  private loadingService = inject(LoadingService);
  private performanceService = inject(PerformanceService);
  private notificationService = inject(NotificationService);

  currentPageTitle = '';
  isConnected = false;
  errorCount = 0;
  isDarkTheme = false;
  isFullscreen = false;
  
  performanceMetrics: PerformanceMetrics = {
    loadTime: 0,
    renderTime: 0,
    apiResponseTime: 0,
    errorCount: 0,
    userActions: 0,
    timestamp: new Date()
  };

  hasActiveLoading$ = this.loadingService.hasActiveLoading$;
  private subscriptions = new Subscription();

  ngOnInit(): void {
    // 监听路由变化更新页面标题
    this.subscriptions.add(
      this.router.events.pipe(
        filter(event => event instanceof NavigationEnd)
      ).subscribe((event: NavigationEnd) => {
        this.updatePageTitle(event.url);
      })
    );

    // 监听性能指标
    this.subscriptions.add(
      this.performanceService.metrics$.subscribe(metrics => {
        this.performanceMetrics = metrics;
        this.errorCount = metrics.errorCount;
      })
    );

    // 检测主题
    this.detectTheme();
    
    // 检测全屏状态
    this.detectFullscreen();
    
    // 初始化页面标题
    this.updatePageTitle(this.router.url);
  }

  ngOnDestroy(): void {
    this.subscriptions.unsubscribe();
  }

  private updatePageTitle(url: string): void {
    const routes: Record<string, string> = {
      '/': '首页',
      '/clusters': '集群管理',
      '/cluster': '集群详情',
      '/pods': 'Pod 管理',
      '/logs': '日志查看',
      '/backups': '备份管理',
      '/monitoring': '监控面板',
      '/settings': '系统设置'
    };

    // 找到匹配的路由
    const matchedRoute = Object.keys(routes).find(route => 
      url.startsWith(route) && route !== '/'
    ) || '/';
    
    this.currentPageTitle = routes[matchedRoute] || '未知页面';
  }

  private detectTheme(): void {
    const saved = localStorage.getItem('theme');
    if (saved === 'dark') {
      this.isDarkTheme = true;
    } else if (saved === 'light') {
      this.isDarkTheme = false;
    } else {
      this.isDarkTheme = window.matchMedia('(prefers-color-scheme: dark)').matches;
    }
    document.body.classList.toggle('dark-theme', this.isDarkTheme);
  }

  private detectFullscreen(): void {
    this.isFullscreen = !!document.fullscreenElement;
    
    document.addEventListener('fullscreenchange', () => {
      this.isFullscreen = !!document.fullscreenElement;
    });
  }

  openPerformanceMonitor(): void {
    this.dialog.open(PerformanceMonitorComponent, {
      width: '90vw',
      maxWidth: '1200px',
      height: '80vh',
      panelClass: 'performance-monitor-dialog'
    });
  }

  showErrorDetails(): void {
    const summary = this.performanceService.getPerformanceSummary();
    const errorDetails = `
      错误统计：${this.errorCount} 次
      错误率：${summary.overall.errorRate.toFixed(2)}%
      
      建议：
      ${summary.recommendations.join('\n')}
    `;
    
    this.notificationService.showInfo(`错误详情\n\n${errorDetails}`);
  }

  getConnectionIcon(): string {
    return this.isConnected ? 'cloud_done' : 'cloud_off';
  }

  getConnectionText(): string {
    return this.isConnected ? '已连接' : '未连接';
  }

  getPerformanceScore(): number {
    const loadScore = this.getLoadPerformanceScore();
    const apiScore = this.getApiPerformanceScore();
    const stabilityScore = this.getStabilityScore();
    return Math.round((loadScore + apiScore + stabilityScore) / 3);
  }

  private getLoadPerformanceScore(): number {
    const loadTime = this.performanceMetrics.loadTime;
    if (loadTime <= 1000) return 100;
    if (loadTime <= 2000) return 80;
    if (loadTime <= 3000) return 60;
    if (loadTime <= 5000) return 40;
    return 20;
  }

  private getApiPerformanceScore(): number {
    const apiTime = this.performanceMetrics.apiResponseTime;
    if (apiTime <= 200) return 100;
    if (apiTime <= 500) return 80;
    if (apiTime <= 1000) return 60;
    if (apiTime <= 2000) return 40;
    return 20;
  }

  private getStabilityScore(): number {
    const errorRate = this.performanceMetrics.userActions > 0 
      ? (this.performanceMetrics.errorCount / this.performanceMetrics.userActions) * 100 
      : 0;
    if (errorRate <= 1) return 100;
    if (errorRate <= 3) return 80;
    if (errorRate <= 5) return 60;
    if (errorRate <= 10) return 40;
    return 20;
  }

  showPerformanceBadge(): boolean {
    return this.getPerformanceScore() < 80;
  }

  getPerformanceBadgeColor(): ThemePalette {
    const score = this.getPerformanceScore();
    if (score >= 60) return 'accent';
    return 'warn';
  }

  isPerformanceWarning(): boolean {
    const score = this.getPerformanceScore();
    return score >= 40 && score < 80;
  }

  isPerformanceError(): boolean {
    return this.getPerformanceScore() < 40;
  }

  toggleTheme(): void {
    this.isDarkTheme = !this.isDarkTheme;
    document.body.classList.toggle('dark-theme', this.isDarkTheme);
    localStorage.setItem('theme', this.isDarkTheme ? 'dark' : 'light');
    
    this.notificationService.showSuccess(
      `已切换到${this.isDarkTheme ? '深色' : '浅色'}主题`
    );
  }

  toggleFullscreen(): void {
    if (!document.fullscreenElement) {
      document.documentElement.requestFullscreen().then(() => {
        this.notificationService.showSuccess('已进入全屏模式');
      }).catch(() => {
        this.notificationService.showError('无法进入全屏模式');
      });
    } else {
      document.exitFullscreen().then(() => {
        this.notificationService.showSuccess('已退出全屏模式');
      }).catch(() => {
        this.notificationService.showError('无法退出全屏模式');
      });
    }
  }

  openSettings(): void {
    this.notificationService.showInfo('设置功能开发中...');
  }

  openHelp(): void {
    const helpContent = `
      PolarDB-X 可视化运维平台帮助
      
      主要功能：
      • 集群管理：创建、查看、更新、删除 PolarDB-X 集群
      • Pod 监控：查看集群中各个 Pod 的状态和日志
      • 备份管理：管理集群备份和恢复
      • 性能监控：实时监控应用性能指标
      
      快捷键：
      • F11: 切换全屏
      • Ctrl+Shift+P: 打开性能监控
      • Ctrl+Shift+T: 切换主题
      
      如需更多帮助，请联系技术支持。
    `;
    
    this.notificationService.showInfo(`帮助信息\n\n${helpContent}`);
  }

  exportLogs(): void {
    const performanceData = this.performanceService.exportPerformanceData();
    const blob = new Blob([performanceData], { type: 'application/json' });
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `polardbx-logs-${new Date().toISOString().split('T')[0]}.json`;
    link.click();
    window.URL.revokeObjectURL(url);
    
    this.notificationService.showSuccess('日志已导出');
  }

  showAbout(): void {
    const aboutContent = `
      PolarDB-X 可视化运维平台
      版本：v1.0.0
      
      基于 Angular 18 和 Material Design 构建
      提供直观的 PolarDB-X 集群管理界面
      
      特性：
      ✅ 响应式设计
      ✅ 实时性能监控
      ✅ 智能错误处理
      ✅ 用户体验优化
      ✅ 生产环境就绪
      
      © 2024 PolarDB-X Team
    `;
    
    this.notificationService.showInfo(`关于\n\n${aboutContent}`);
  }
}