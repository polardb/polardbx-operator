import { Component, OnInit, OnDestroy, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatCardModule } from '@angular/material/card';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { MatChipsModule } from '@angular/material/chips';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatExpansionModule } from '@angular/material/expansion';
import { Subscription } from 'rxjs';
import { EmptyStateComponent } from '../empty-state/empty-state.component';
import { PerformanceService, PerformanceMetrics, ApiPerformance, PerformanceSummary } from '../../services/performance.service';

@Component({
  selector: 'app-performance-monitor',
  standalone: true,
  imports: [
    CommonModule,
    MatCardModule,
    MatIconModule,
    MatButtonModule,
    MatProgressBarModule,
    MatChipsModule,
    MatTooltipModule,
    MatExpansionModule,
    EmptyStateComponent
  ],
  template: `
    <mat-card class="performance-monitor">
      <mat-card-header>
        <mat-card-title>
          <mat-icon>speed</mat-icon>
          性能监控
        </mat-card-title>
        <mat-card-subtitle>实时应用性能指标</mat-card-subtitle>
        <div class="header-actions">
          <button mat-icon-button (click)="refreshMetrics()" matTooltip="刷新指标">
            <mat-icon>refresh</mat-icon>
          </button>
          <button mat-icon-button (click)="exportData()" matTooltip="导出数据">
            <mat-icon>download</mat-icon>
          </button>
          <button mat-icon-button (click)="resetMetrics()" matTooltip="重置指标">
            <mat-icon>restore</mat-icon>
          </button>
        </div>
      </mat-card-header>

      <mat-card-content>
        <!-- 核心指标 -->
        <div class="metrics-grid">
          <div class="metric-card">
            <div class="metric-icon">
              <mat-icon [class]="getLoadTimeClass()">schedule</mat-icon>
            </div>
            <div class="metric-content">
              <div class="metric-value">{{ formatTime(currentMetrics.loadTime) }}</div>
              <div class="metric-label">页面加载时间</div>
            </div>
          </div>

          <div class="metric-card">
            <div class="metric-icon">
              <mat-icon [class]="getApiResponseClass()">cloud</mat-icon>
            </div>
            <div class="metric-content">
              <div class="metric-value">{{ formatTime(currentMetrics.apiResponseTime) }}</div>
              <div class="metric-label">API 响应时间</div>
            </div>
          </div>

          <div class="metric-card">
            <div class="metric-icon">
              <mat-icon [class]="getErrorRateClass()">error_outline</mat-icon>
            </div>
            <div class="metric-content">
              <div class="metric-value">{{ currentMetrics.errorCount }}</div>
              <div class="metric-label">错误次数</div>
            </div>
          </div>

          <div class="metric-card">
            <div class="metric-icon">
              <mat-icon class="metric-icon-info">touch_app</mat-icon>
            </div>
            <div class="metric-content">
              <div class="metric-value">{{ currentMetrics.userActions }}</div>
              <div class="metric-label">用户操作</div>
            </div>
          </div>
        </div>

        <!-- 性能评分 -->
        <div class="performance-score">
          <h3>性能评分</h3>
          <div class="score-container">
            <div class="score-circle" [class]="getScoreClass()">
              <span class="score-value">{{ getPerformanceScore() }}</span>
              <span class="score-label">分</span>
            </div>
            <div class="score-details">
              <div class="score-item">
                <span class="score-item-label">加载性能:</span>
                <mat-progress-bar 
                  mode="determinate" 
                  [value]="getLoadPerformanceScore()" 
                  [color]="getLoadPerformanceColor()">
                </mat-progress-bar>
                <span class="score-item-value">{{ getLoadPerformanceScore() }}%</span>
              </div>
              <div class="score-item">
                <span class="score-item-label">API 性能:</span>
                <mat-progress-bar 
                  mode="determinate" 
                  [value]="getApiPerformanceScore()" 
                  [color]="getApiPerformanceColor()">
                </mat-progress-bar>
                <span class="score-item-value">{{ getApiPerformanceScore() }}%</span>
              </div>
              <div class="score-item">
                <span class="score-item-label">稳定性:</span>
                <mat-progress-bar 
                  mode="determinate" 
                  [value]="getStabilityScore()" 
                  [color]="getStabilityColor()">
                </mat-progress-bar>
                <span class="score-item-value">{{ getStabilityScore() }}%</span>
              </div>
            </div>
          </div>
        </div>

        <!-- 详细信息 -->
        <mat-expansion-panel class="details-panel">
          <mat-expansion-panel-header>
            <mat-panel-title>
              <mat-icon>analytics</mat-icon>
              详细分析
            </mat-panel-title>
          </mat-expansion-panel-header>

          <!-- API 性能历史 -->
          <div class="api-performance">
            <h4>API 性能历史</h4>
            <div class="api-list" *ngIf="apiHistory.length > 0; else noApiData">
              <div class="api-item" *ngFor="let api of getRecentApiCalls()">
                <div class="api-info">
                  <span class="api-method" [class]="'method-' + api.method.toLowerCase()">{{ api.method }}</span>
                  <span class="api-endpoint">{{ api.endpoint }}</span>
                  <span class="api-status" [class]="getStatusClass(api.status)">{{ api.status }}</span>
                </div>
                <div class="api-timing">
                  <span class="api-duration" [class]="getDurationClass(api.duration)">{{ formatTime(api.duration) }}</span>
                  <span class="api-time">{{ formatTimestamp(api.timestamp) }}</span>
                </div>
              </div>
            </div>
            <ng-template #noApiData>
              <app-empty-state icon="lan" title="暂无 API 调用记录" hint="执行操作或刷新后再查看"></app-empty-state>
            </ng-template>
          </div>

          <!-- 性能建议 -->
          <div class="recommendations" *ngIf="recommendations.length > 0">
            <h4>性能优化建议</h4>
            <mat-chip-listbox class="recommendation-chips">
              <mat-chip-option *ngFor="let recommendation of recommendations" class="recommendation-chip">
                <mat-icon>lightbulb</mat-icon>
                {{ recommendation }}
              </mat-chip-option>
            </mat-chip-listbox>
          </div>
        </mat-expansion-panel>
      </mat-card-content>
    </mat-card>
  `,
  styleUrls: ['./performance-monitor.component.scss']
})
export class PerformanceMonitorComponent implements OnInit, OnDestroy {
  private performanceService = inject(PerformanceService);

  currentMetrics: PerformanceMetrics = {
    loadTime: 0,
    renderTime: 0,
    apiResponseTime: 0,
    errorCount: 0,
    userActions: 0,
    timestamp: new Date()
  };

  apiHistory: ApiPerformance[] = [];
  recommendations: string[] = [];
  private subscription = new Subscription();

  ngOnInit(): void {
    // 订阅性能指标更新
    this.subscription.add(
      this.performanceService.metrics$.subscribe(metrics => {
        this.currentMetrics = metrics;
      })
    );

    // 获取初始数据
    this.refreshMetrics();
  }

  ngOnDestroy(): void {
    this.subscription.unsubscribe();
  }

  refreshMetrics(): void {
    this.currentMetrics = this.performanceService.getCurrentMetrics();
    this.apiHistory = this.performanceService.getApiPerformanceHistory();
    const summary = this.performanceService.getPerformanceSummary();
    this.recommendations = summary.recommendations;
  }

  exportData(): void {
    const data = this.performanceService.exportPerformanceData();
    const blob = new Blob([data], { type: 'application/json' });
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `performance-data-${new Date().toISOString().split('T')[0]}.json`;
    link.click();
    window.URL.revokeObjectURL(url);
  }

  resetMetrics(): void {
    this.performanceService.resetMetrics();
    this.refreshMetrics();
  }

  formatTime(ms: number): string {
    if (ms < 1000) {
      return `${Math.round(ms)}ms`;
    }
    return `${(ms / 1000).toFixed(1)}s`;
  }

  formatTimestamp(timestamp: Date): string {
    return new Date(timestamp).toLocaleTimeString();
  }

  getPerformanceScore(): number {
    const loadScore = this.getLoadPerformanceScore();
    const apiScore = this.getApiPerformanceScore();
    const stabilityScore = this.getStabilityScore();
    return Math.round((loadScore + apiScore + stabilityScore) / 3);
  }

  getLoadPerformanceScore(): number {
    const loadTime = this.currentMetrics.loadTime;
    if (loadTime <= 1000) return 100;
    if (loadTime <= 2000) return 80;
    if (loadTime <= 3000) return 60;
    if (loadTime <= 5000) return 40;
    return 20;
  }

  getApiPerformanceScore(): number {
    const apiTime = this.currentMetrics.apiResponseTime;
    if (apiTime <= 200) return 100;
    if (apiTime <= 500) return 80;
    if (apiTime <= 1000) return 60;
    if (apiTime <= 2000) return 40;
    return 20;
  }

  getStabilityScore(): number {
    const errorRate = this.currentMetrics.userActions > 0 
      ? (this.currentMetrics.errorCount / this.currentMetrics.userActions) * 100 
      : 0;
    if (errorRate <= 1) return 100;
    if (errorRate <= 3) return 80;
    if (errorRate <= 5) return 60;
    if (errorRate <= 10) return 40;
    return 20;
  }

  getScoreClass(): string {
    const score = this.getPerformanceScore();
    if (score >= 80) return 'score-excellent';
    if (score >= 60) return 'score-good';
    if (score >= 40) return 'score-fair';
    return 'score-poor';
  }

  getLoadPerformanceColor(): string {
    const score = this.getLoadPerformanceScore();
    if (score >= 80) return 'primary';
    if (score >= 60) return 'accent';
    return 'warn';
  }

  getApiPerformanceColor(): string {
    const score = this.getApiPerformanceScore();
    if (score >= 80) return 'primary';
    if (score >= 60) return 'accent';
    return 'warn';
  }

  getStabilityColor(): string {
    const score = this.getStabilityScore();
    if (score >= 80) return 'primary';
    if (score >= 60) return 'accent';
    return 'warn';
  }

  getLoadTimeClass(): string {
    const loadTime = this.currentMetrics.loadTime;
    if (loadTime <= 1000) return 'metric-icon-success';
    if (loadTime <= 3000) return 'metric-icon-warning';
    return 'metric-icon-error';
  }

  getApiResponseClass(): string {
    const apiTime = this.currentMetrics.apiResponseTime;
    if (apiTime <= 500) return 'metric-icon-success';
    if (apiTime <= 1000) return 'metric-icon-warning';
    return 'metric-icon-error';
  }

  getErrorRateClass(): string {
    const errorCount = this.currentMetrics.errorCount;
    if (errorCount === 0) return 'metric-icon-success';
    if (errorCount <= 3) return 'metric-icon-warning';
    return 'metric-icon-error';
  }

  getRecentApiCalls(): ApiPerformance[] {
    return this.apiHistory.slice(-10).reverse();
  }

  getStatusClass(status: number): string {
    if (status >= 200 && status < 300) return 'status-success';
    if (status >= 300 && status < 400) return 'status-redirect';
    if (status >= 400 && status < 500) return 'status-client-error';
    return 'status-server-error';
  }

  getDurationClass(duration: number): string {
    if (duration <= 200) return 'duration-fast';
    if (duration <= 1000) return 'duration-normal';
    return 'duration-slow';
  }
}