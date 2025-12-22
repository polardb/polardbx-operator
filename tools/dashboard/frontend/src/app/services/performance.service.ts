import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable } from 'rxjs';

export interface PerformanceMetrics {
  loadTime: number;
  renderTime: number;
  apiResponseTime: number;
  errorCount: number;
  userActions: number;
  timestamp: Date;
}

export interface ApiPerformance {
  endpoint: string;
  method: string;
  duration: number;
  status: number;
  timestamp: Date;
}

export interface PerformanceSummary {
  overall: {
    loadTime: number;
    renderTime: number;
    errorRate: number;
    userEngagement: number;
  };
  api: {
    averageResponseTime: number;
    totalRequests: number;
    successRate: number;
    slowestEndpoint: string;
  };
  recommendations: string[];
}

@Injectable({
  providedIn: 'root'
})
export class PerformanceService {
  private metricsSubject = new BehaviorSubject<PerformanceMetrics>({
    loadTime: 0,
    renderTime: 0,
    apiResponseTime: 0,
    errorCount: 0,
    userActions: 0,
    timestamp: new Date()
  });

  public metrics$ = this.metricsSubject.asObservable();
  private apiPerformanceHistory: ApiPerformance[] = [];
  private userActionCount = 0;
  private errorCount = 0;
  private pageLoadStart = performance.now();

  constructor() {
    this.initializePerformanceMonitoring();
  }

  /**
   * 初始化性能监控
   */
  private initializePerformanceMonitoring(): void {
    // 监听页面加载完成
    if (document.readyState === 'complete') {
      this.recordPageLoad();
    } else {
      window.addEventListener('load', () => this.recordPageLoad());
    }

    // 监听用户交互
    this.setupUserInteractionTracking();

    // 定期更新指标
    setInterval(() => this.updateMetrics(), 5000);
  }

  /**
   * 记录页面加载时间
   */
  private recordPageLoad(): void {
    const loadTime = performance.now() - this.pageLoadStart;
    this.updateMetrics({ loadTime });
  }

  /**
   * 设置用户交互跟踪
   */
  private setupUserInteractionTracking(): void {
    const events = ['click', 'keydown', 'scroll', 'touchstart'];
    
    events.forEach(event => {
      document.addEventListener(event, () => {
        this.userActionCount++;
      }, { passive: true });
    });
  }

  /**
   * 记录 API 性能
   */
  recordApiPerformance(endpoint: string, method: string, duration: number, status: number): void {
    const apiPerformance: ApiPerformance = {
      endpoint,
      method,
      duration,
      status,
      timestamp: new Date()
    };

    this.apiPerformanceHistory.push(apiPerformance);
    
    // 限制历史记录数量
    if (this.apiPerformanceHistory.length > 100) {
      this.apiPerformanceHistory = this.apiPerformanceHistory.slice(-50);
    }

    // 更新平均 API 响应时间
    const avgResponseTime = this.calculateAverageApiResponseTime();
    this.updateMetrics({ apiResponseTime: avgResponseTime });
  }

  /**
   * 记录错误
   */
  recordError(): void {
    this.errorCount++;
    this.updateMetrics({ errorCount: this.errorCount });
  }

  /**
   * 记录渲染时间
   */
  recordRenderTime(componentName: string, renderTime: number): void {
    console.log(`Component ${componentName} rendered in ${renderTime}ms`);
    this.updateMetrics({ renderTime });
  }

  /**
   * 开始性能测量
   */
  startMeasurement(name: string): void {
    performance.mark(`${name}-start`);
  }

  /**
   * 结束性能测量
   */
  endMeasurement(name: string): number {
    performance.mark(`${name}-end`);
    performance.measure(name, `${name}-start`, `${name}-end`);
    
    const measure = performance.getEntriesByName(name, 'measure')[0];
    const duration = measure ? measure.duration : 0;
    
    // 清理标记
    performance.clearMarks(`${name}-start`);
    performance.clearMarks(`${name}-end`);
    performance.clearMeasures(name);
    
    return duration;
  }

  /**
   * 获取当前性能指标
   */
  getCurrentMetrics(): PerformanceMetrics {
    return this.metricsSubject.value;
  }

  /**
   * 获取 API 性能历史
   */
  getApiPerformanceHistory(): ApiPerformance[] {
    return [...this.apiPerformanceHistory];
  }

  /**
   * 获取性能摘要
   */
  getPerformanceSummary(): PerformanceSummary {
    const metrics = this.getCurrentMetrics();
    const apiHistory = this.getApiPerformanceHistory();
    
    return {
      overall: {
        loadTime: metrics.loadTime,
        renderTime: metrics.renderTime,
        errorRate: this.calculateErrorRate(),
        userEngagement: this.userActionCount
      },
      api: {
        averageResponseTime: metrics.apiResponseTime,
        totalRequests: apiHistory.length,
        successRate: this.calculateApiSuccessRate(),
        slowestEndpoint: this.getSlowestEndpoint()?.endpoint || 'N/A'
      },
      recommendations: this.generateRecommendations()
    };
  }

  /**
   * 更新指标
   */
  private updateMetrics(updates: Partial<PerformanceMetrics> = {}): void {
    const currentMetrics = this.metricsSubject.value;
    const newMetrics: PerformanceMetrics = {
      ...currentMetrics,
      ...updates,
      userActions: this.userActionCount,
      timestamp: new Date()
    };
    
    this.metricsSubject.next(newMetrics);
  }

  /**
   * 计算平均 API 响应时间
   */
  private calculateAverageApiResponseTime(): number {
    if (this.apiPerformanceHistory.length === 0) return 0;
    
    const totalTime = this.apiPerformanceHistory.reduce((sum, api) => sum + api.duration, 0);
    return totalTime / this.apiPerformanceHistory.length;
  }

  /**
   * 计算错误率
   */
  private calculateErrorRate(): number {
    const totalActions = this.userActionCount || 1;
    return (this.errorCount / totalActions) * 100;
  }

  /**
   * 计算 API 成功率
   */
  private calculateApiSuccessRate(): number {
    if (this.apiPerformanceHistory.length === 0) return 100;
    
    const successCount = this.apiPerformanceHistory.filter(api => api.status >= 200 && api.status < 400).length;
    return (successCount / this.apiPerformanceHistory.length) * 100;
  }

  /**
   * 获取最慢的端点
   */
  private getSlowestEndpoint(): ApiPerformance | null {
    if (this.apiPerformanceHistory.length === 0) return null;
    
    return this.apiPerformanceHistory.reduce((slowest, current) => 
      current.duration > slowest.duration ? current : slowest
    );
  }

  /**
   * 生成性能优化建议
   */
  private generateRecommendations(): string[] {
    const recommendations: string[] = [];
    const metrics = this.getCurrentMetrics();
    
    if (metrics.loadTime > 3000) {
      recommendations.push('页面加载时间较长，建议优化资源加载');
    }
    
    if (metrics.apiResponseTime > 1000) {
      recommendations.push('API 响应时间较慢，建议优化后端性能');
    }
    
    if (this.calculateErrorRate() > 5) {
      recommendations.push('错误率较高，建议检查错误处理逻辑');
    }
    
    if (this.calculateApiSuccessRate() < 95) {
      recommendations.push('API 成功率较低，建议检查网络连接和服务稳定性');
    }
    
    return recommendations;
  }

  /**
   * 重置指标
   */
  resetMetrics(): void {
    this.userActionCount = 0;
    this.errorCount = 0;
    this.apiPerformanceHistory = [];
    this.pageLoadStart = performance.now();
    
    this.updateMetrics({
      loadTime: 0,
      renderTime: 0,
      apiResponseTime: 0,
      errorCount: 0,
      userActions: 0
    });
  }

  /**
   * 导出性能数据
   */
  exportPerformanceData(): string {
    const data = {
      metrics: this.getCurrentMetrics(),
      apiHistory: this.getApiPerformanceHistory(),
      summary: this.getPerformanceSummary(),
      exportTime: new Date().toISOString()
    };
    
    return JSON.stringify(data, null, 2);
  }
}