import { Injectable } from '@angular/core';
import { MatSnackBar } from '@angular/material/snack-bar';
import { BehaviorSubject, Observable } from 'rxjs';

export type LoadingState = Record<string, boolean>;

@Injectable({
  providedIn: 'root'
})
export class LoadingService {
  private loadingSubject = new BehaviorSubject<LoadingState>({});
  public loading$ = this.loadingSubject.asObservable();
  public activeLoadingKeys$: Observable<string[]>;
  public hasActiveLoading$: Observable<boolean>;

  private loadingState: LoadingState = {};

  constructor(private snackBar: MatSnackBar) {
    this.activeLoadingKeys$ = new Observable(observer => {
      const subscription = this.loading$.subscribe(state => {
        const activeKeys = Object.keys(state).filter(key => state[key]);
        observer.next(activeKeys);
      });
      return () => subscription.unsubscribe();
    });

    this.hasActiveLoading$ = new Observable(observer => {
      const subscription = this.loading$.subscribe(state => {
        const hasActive = Object.values(state).some(loading => loading);
        observer.next(hasActive);
      });
      return () => subscription.unsubscribe();
    });
  }

  // 轻量提示封装
  showSnackBar(message: string, action: string = '关闭', durationMs: number = 3000): void {
    try {
      this.snackBar.open(message, action, {
        duration: durationMs,
        horizontalPosition: 'center',
        verticalPosition: 'top'
      });
    } catch {
      // 如果 Material 未加载，降级为 alert
      // eslint-disable-next-line no-alert
      alert(message);
    }
  }

  /**
   * 设置加载状态
   * @param key 加载状态的唯一标识
   * @param loading 是否正在加载
   */
  setLoading(key: string, loading: boolean): void {
    this.loadingState[key] = loading;
    this.loadingSubject.next({ ...this.loadingState });
  }

  /**
   * 获取特定的加载状态
   * @param key 加载状态的唯一标识
   * @returns 是否正在加载
   */
  isLoading(key: string): boolean {
    return this.loadingState[key] || false;
  }

  /**
   * 获取特定加载状态的 Observable
   * @param key 加载状态的唯一标识
   * @returns Observable<boolean>
   */
  getLoadingState(key: string): Observable<boolean> {
    return new Observable(observer => {
      const subscription = this.loading$.subscribe(state => {
        observer.next(state[key] || false);
      });
      return () => subscription.unsubscribe();
    });
  }

  /**
   * 检查是否有任何加载状态为 true
   * @returns 是否有任何操作正在加载
   */
  isAnyLoading(): boolean {
    return Object.values(this.loadingState).some(loading => loading);
  }

  /**
   * 获取全局加载状态的 Observable
   * @returns Observable<boolean>
   */
  getGlobalLoadingState(): Observable<boolean> {
    return new Observable(observer => {
      const subscription = this.loading$.subscribe(state => {
        observer.next(Object.values(state).some(loading => loading));
      });
      return () => subscription.unsubscribe();
    });
  }

  /**
   * 清除所有加载状态
   */
  clearAll(): void {
    this.loadingState = {};
    this.loadingSubject.next({});
  }

  /**
   * 清除特定的加载状态
   * @param key 加载状态的唯一标识
   */
  clear(key: string): void {
    delete this.loadingState[key];
    this.loadingSubject.next({ ...this.loadingState });
  }

  /**
   * 包装异步操作，自动管理加载状态
   * @param key 加载状态的唯一标识
   * @param operation 异步操作
   * @returns Promise
   */
  async wrapOperation<T>(key: string, operation: () => Promise<T>): Promise<T> {
    this.setLoading(key, true);
    try {
      const result = await operation();
      return result;
    } finally {
      this.setLoading(key, false);
    }
  }

  /**
   * 包装 Observable 操作，自动管理加载状态
   * @param key 加载状态的唯一标识
   * @param operation Observable 操作
   * @returns Observable
   */
  wrapObservable<T>(key: string, operation: Observable<T>): Observable<T> {
    return new Observable(observer => {
      this.setLoading(key, true);
      
      const subscription = operation.subscribe({
        next: (value) => observer.next(value),
        error: (error) => {
          this.setLoading(key, false);
          observer.error(error);
        },
        complete: () => {
          this.setLoading(key, false);
          observer.complete();
        }
      });

      return () => {
        this.setLoading(key, false);
        subscription.unsubscribe();
      };
    });
  }

  /**
   * 获取所有加载状态的键
   * @returns 所有加载状态的键数组
   */
  getAllLoadingKeys(): string[] {
    return Object.keys(this.loadingState);
  }

  /**
   * 获取当前所有加载状态
   * @returns 当前加载状态对象的副本
   */
  getCurrentState(): LoadingState {
    return { ...this.loadingState };
  }
}

// 常用的加载状态键常量
export const LoadingKeys = {
  CLUSTERS_LIST: 'clusters-list',
  CLUSTER_DETAIL: 'cluster-detail',
  CLUSTER_CREATE: 'cluster-create',
  CLUSTER_UPDATE: 'cluster-update',
  CLUSTER_DELETE: 'cluster-delete',
  PODS_LIST: 'pods-list',
  POD_LOGS: 'pod-logs',
  POD_DELETE: 'pod-delete',
  POD_DETAIL: 'pod-detail',
  BACKUPS_LIST: 'backups-list',
  BACKUP_CREATE: 'backup-create',
  BACKUP_DELETE: 'backup-delete',
  
  // XStore loading keys
  XSTORE_LIST: 'xstore-list',
  XSTORE_CREATE: 'xstore-create',
  XSTORE_DETAIL: 'xstore-detail',
  XSTORE_UPDATE: 'xstore-update',
  XSTORE_DELETE: 'xstore-delete',
  
  // Monitor loading keys  
  MONITOR_LIST: 'monitor-list',
  MONITOR_CREATE: 'monitor-create',
  MONITOR_DETAIL: 'monitor-detail',
  MONITOR_UPDATE: 'monitor-update',
  MONITOR_DELETE: 'monitor-delete',
  
  // BackupSchedule loading keys
  BACKUP_SCHEDULE_LIST: 'backup-schedule-list',
  BACKUP_SCHEDULE_CREATE: 'backup-schedule-create',
  BACKUP_SCHEDULE_DETAIL: 'backup-schedule-detail',
  BACKUP_SCHEDULE_UPDATE: 'backup-schedule-update',
  BACKUP_SCHEDULE_DELETE: 'backup-schedule-delete',
  
  // ParameterTemplate loading keys
  PARAMETER_TEMPLATE_LIST: 'parameter-template-list',
  PARAMETER_TEMPLATE_CREATE: 'parameter-template-create',
  PARAMETER_TEMPLATE_DETAIL: 'parameter-template-detail',
  PARAMETER_TEMPLATE_UPDATE: 'parameter-template-update',
  PARAMETER_TEMPLATE_DELETE: 'parameter-template-delete',
  
  // SystemTask loading keys
  SYSTEM_TASK_LIST: 'system-task-list',
  SYSTEM_TASK_CREATE: 'system-task-create',
  SYSTEM_TASK_DETAIL: 'system-task-detail',
  SYSTEM_TASK_UPDATE: 'system-task-update',
  SYSTEM_TASK_DELETE: 'system-task-delete',
  
  // LogCollector loading keys
  LOG_COLLECTOR_LIST: 'log-collector-list',
  LOG_COLLECTOR_CREATE: 'log-collector-create',
  LOG_COLLECTOR_DETAIL: 'log-collector-detail',
  LOG_COLLECTOR_UPDATE: 'log-collector-update',
  LOG_COLLECTOR_DELETE: 'log-collector-delete',

  // Logs
  LOGS_PRESETS: 'logs-presets',
  LOGS_QUERY: 'logs-query',
  
  // Log Service Dashboard
  LOG_SERVICE_STATUS: 'log-service-status',
  LOG_STRATEGIES_LIST: 'log-strategies-list',
  LOG_STRATEGY_SAVE: 'log-strategy-save',
  LOG_STRATEGY_DELETE: 'log-strategy-delete',
  ES_CONNECTION_TEST: 'es-connection-test',

  // Precheck loading keys
  PRECHECK_RUN: 'precheck-run',
  PRECHECK_TOKEN_VALIDATE: 'precheck-token-validate',

  // 🚨 CRITICAL MISSING FUNCTIONALITY: Recovery loading keys
  // Based on document analysis, these are the most critical missing loading states
  CLUSTER_RESTORE: 'cluster-restore',
  CLUSTER_PITR: 'cluster-pitr',
  RESTORE_STATUS: 'restore-status',
  RESTORE_JOB_LIST: 'restore-job-list',
  RESTORE_JOB_DETAIL: 'restore-job-detail',
  RESTORE_JOB_CANCEL: 'restore-job-cancel',

  // 🚨 HIGH PRIORITY: XStoreFollower loading keys
  // XStoreFollower for DN replica fault recovery (备库重搭)
  XSTORE_FOLLOWER_LIST: 'xstore-follower-list',
  XSTORE_FOLLOWER_CREATE: 'xstore-follower-create',
  XSTORE_FOLLOWER_DETAIL: 'xstore-follower-detail',
  XSTORE_FOLLOWER_UPDATE: 'xstore-follower-update',
  XSTORE_FOLLOWER_DELETE: 'xstore-follower-delete',

  // XStoreBackup loading keys - Complete the unified backup module
  XSTORE_BACKUP_LIST: 'xstore-backup-list',
  XSTORE_BACKUP_CREATE: 'xstore-backup-create',
  XSTORE_BACKUP_DETAIL: 'xstore-backup-detail',
  XSTORE_BACKUP_UPDATE: 'xstore-backup-update',
  XSTORE_BACKUP_DELETE: 'xstore-backup-delete',
  
  // XStore Backup Binlog loading keys - Standard edition incremental log backup
  XSTORE_BINLOG_LIST: 'xstore-binlog-list',
  XSTORE_BINLOG_CREATE: 'xstore-binlog-create',
  XSTORE_BINLOG_UPDATE: 'xstore-binlog-update',
  XSTORE_BINLOG_DELETE: 'xstore-binlog-delete',

  // BackupBinlog loading keys - Binlog backup & PITR
  BACKUP_BINLOG_LIST: 'backup-binlog-list',
  BACKUP_BINLOG_CREATE: 'backup-binlog-create',
  BACKUP_BINLOG_DETAIL: 'backup-binlog-detail',
  BACKUP_BINLOG_UPDATE: 'backup-binlog-update',
  BACKUP_BINLOG_DELETE: 'backup-binlog-delete',

  // PolarDBXClusterKnobs loading keys - Performance tuning
  CLUSTER_KNOBS_LIST: 'cluster-knobs-list',
  CLUSTER_KNOBS_CREATE: 'cluster-knobs-create',
  CLUSTER_KNOBS_DETAIL: 'cluster-knobs-detail',
  CLUSTER_KNOBS_UPDATE: 'cluster-knobs-update',
  CLUSTER_KNOBS_DELETE: 'cluster-knobs-delete',

  // Monitoring and Alerting
  MONITORING: 'monitoring',
  CLUSTER_LIST: 'cluster-list',
  SYSTEM: 'system',
  LOG_STRATEGY: 'log-strategy',
  GRAFANA_TEMPLATE_LIST: 'grafana-template-list',
  GRAFANA_TEMPLATE_DETAIL: 'grafana-template-detail',
  GRAFANA_TEMPLATE_IMPORT: 'grafana-template-import',
  ALERT_TEMPLATE_LIST: 'alert-template-list',
  ALERT_TEMPLATE_DETAIL: 'alert-template-detail',
  ALERT_TEMPLATE_APPLY: 'alert-template-apply',
  
  CONNECT: 'connect',
  GLOBAL: 'global'
} as const;

export type LoadingKey = typeof LoadingKeys[keyof typeof LoadingKeys];