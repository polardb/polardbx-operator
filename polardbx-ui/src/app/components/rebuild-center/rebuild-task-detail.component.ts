import { Component, OnInit, OnDestroy, ChangeDetectorRef, ChangeDetectionStrategy, NgZone } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ActivatedRoute, Router } from '@angular/router';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzProgressModule } from 'ng-zorro-antd/progress';
import { NzDescriptionsModule } from 'ng-zorro-antd/descriptions';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzModalService, NzModalModule } from 'ng-zorro-antd/modal';
import { NzResultModule } from 'ng-zorro-antd/result';
import { interval, Subscription } from 'rxjs';

import { ApiService } from '../../services/api.service';
import { XStoreFollower } from '../../models/xstore-follower.model';

@Component({
  selector: 'app-rebuild-task-detail',
  standalone: true,
  imports: [
    CommonModule,
    NzCardModule,
    NzButtonModule,
    NzIconModule,
    NzTagModule,
    NzSpinModule,
    NzProgressModule,
    NzDescriptionsModule,
    NzEmptyModule,
    NzModalModule,
    NzResultModule
  ],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="rebuild-task-detail-container">
      <div class="page-header">
        <button nz-button nzType="text" (click)="goBack()" class="back-button">
          <i nz-icon nzType="arrow-left"></i>
          返回任务列表
        </button>
        <h1 class="page-title">重搭任务详情</h1>
      </div>

      <!-- 调试信息 -->
      <nz-card class="debug-card" *ngIf="showDebugInfo">
        <h3>调试信息</h3>
        <p><strong>命名空间:</strong> {{ namespace || '未设置' }}</p>
        <p><strong>任务名:</strong> {{ taskName || '未设置' }}</p>
        <p><strong>加载状态:</strong> {{ loading ? '加载中' : '已完成' }}</p>
        <p><strong>任务数据:</strong> {{ task ? '已加载' : '未加载' }}</p>
        <p><strong>错误信息:</strong> {{ errorMessage || '无' }}</p>
        <p><strong>API调用次数:</strong> {{ apiCallCount }}</p>
        <p><strong>模板判断:</strong> 
          loading={{ loading }}, 
          task={{ !!task }}, 
          errorMessage={{ !!errorMessage }}
        </p>
        <button nz-button nzType="default" (click)="manualRefresh()">手动刷新</button>
        <button nz-button nzType="default" (click)="forceUpdate()">强制更新</button>
      </nz-card>

      <!-- 加载状态（仅在未拿到task前显示） -->
      <div class="loading-wrapper" *ngIf="loading && !task">
        <nz-spin nzSize="large">
          <div class="loading-tip">正在加载任务详情...</div>
        </nz-spin>
      </div>

      <!-- 错误状态 -->
      <nz-result 
        *ngIf="!loading && errorMessage && !task"
        nzStatus="error"
        nzTitle="加载失败"
        [nzSubTitle]="errorMessage">
        <div nz-result-extra>
          <button nz-button nzType="primary" (click)="manualRefresh()">重新加载</button>
          <button nz-button nzType="default" (click)="goBack()">返回列表</button>
        </div>
      </nz-result>

      <!-- 任务详情内容（极简无动画设计） -->
      <div class="task-content" *ngIf="!loading && task">
        <nz-card class="task-header-card">
          <div class="task-header">
            <div class="task-info">
              <h2 class="task-title">
                <i nz-icon [nzType]="getTaskIcon()"></i>
                {{ task.metadata.name }}
              </h2>
              <div class="task-meta">
                <nz-tag [nzColor]="getStatusColor()">{{ getDisplayStatus() }}</nz-tag>
                <nz-tag [nzColor]="getRoleColor()">{{ getRoleDisplayName() }}</nz-tag>
              </div>
            </div>
            <div class="task-actions">
              <button nz-button nzType="default" (click)="manualRefresh()">
                <i nz-icon nzType="reload"></i>
                刷新
              </button>
              <button nz-button nzType="default" (click)="toggleAutoRefresh()">
                <i nz-icon [nzType]="autoRefresh ? 'pause' : 'play-circle'"></i>
                {{ autoRefresh ? '停止自动刷新' : '开启自动刷新' }}
              </button>
            </div>
          </div>
          <div class="progress-section" *ngIf="!isEndPhase()">
            <nz-progress [nzPercent]="getProgressPercent()" [nzStatus]="getProgressStatus()" [nzStrokeWidth]="8"></nz-progress>
            <p class="progress-text">{{ getDisplayStatus() }}</p>
          </div>
        </nz-card>

        <nz-card class="detail-tabs-card">
          <div class="tab-content">
            <nz-descriptions nzTitle="任务详情" nzBordered [nzColumn]="3" nzSize="middle">
              <nz-descriptions-item nzTitle="任务名称">{{ task.metadata.name }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="命名空间">{{ task.metadata.namespace }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="角色类型">{{ getRoleDisplayName() }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="目标 XStore">{{ task.spec.xStoreName }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="构建方式">
                <nz-tag [nzColor]="task.spec.local ? 'blue' : 'orange'">{{ task.spec.local ? '本机构建' : '跨机构建' }}</nz-tag>
              </nz-descriptions-item>
              <nz-descriptions-item nzTitle="创建时间">{{ formatTime(task.metadata.creationTimestamp) }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="源 Pod" *ngIf="task.spec.fromPodName">{{ task.spec.fromPodName }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="目标 Pod" *ngIf="getTargetPodName()">{{ getTargetPodName() }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="目标节点" *ngIf="getTargetNodeName()">{{ getTargetNodeName() }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="当前状态"><nz-tag [nzColor]="getStatusColor()">{{ getDisplayStatus() }}</nz-tag></nz-descriptions-item>
              <nz-descriptions-item nzTitle="状态消息" *ngIf="task.status?.message" nzSpan="2">{{ task.status?.message }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="当前任务" *ngIf="task.status?.currentJobName" nzSpan="2">{{ task.status?.currentJobName }}</nz-descriptions-item>
            </nz-descriptions>

            <div class="action-buttons">
              <button nz-button nzType="default" *ngIf="!isEndPhase()" (click)="stopTask()"><i nz-icon nzType="pause"></i>停止任务</button>
              <button nz-button nzType="primary" *ngIf="canRetry()" (click)="retryTask()"><i nz-icon nzType="redo"></i>重试任务</button>
              <button nz-button nzDanger *ngIf="isEndPhase()" (click)="deleteTask()"><i nz-icon nzType="delete"></i>删除任务</button>
              <button nz-button nzType="default" (click)="toggleRaw()"><i nz-icon nzType="file-text"></i>{{ showRaw ? '隐藏原始数据' : '显示原始数据' }}</button>
            </div>

            <div *ngIf="showRaw" style="margin-top: 12px;">
              <pre class="json-display">{{ taskJson }}</pre>
            </div>

            <div style="margin-top: 16px;">
              <h4>执行步骤</h4>
              <ul>
                <li *ngFor="let step of getSteps()" [style.color]="step.status === 'error' ? '#ff4d4f' : step.status === 'finish' ? '#52c41a' : '#1890ff'">
                  <strong>{{ step.title }}</strong> - {{ step.description }}
                </li>
              </ul>
            </div>
          </div>
        </nz-card>
      </div>

      <!-- 无数据状态 -->
      <nz-result 
        *ngIf="!loading && !task && !errorMessage"
        nzStatus="404"
        nzTitle="任务不存在"
        nzSubTitle="未找到指定的重搭任务">
        <div nz-result-extra>
          <button nz-button nzType="primary" (click)="goBack()">返回任务列表</button>
        </div>
      </nz-result>
    </div>
  `,
  styleUrls: ['./rebuild-task-detail.component.scss']
})
export class RebuildTaskDetailComponent implements OnInit, OnDestroy {
  // 基础状态
  task: XStoreFollower | null = null;
  loading = true;
  errorMessage = '';
  namespace = '';
  taskName = '';
  taskJson = '';
  
  // 调试相关
  showDebugInfo = true; // 开发时显示调试信息
  apiCallCount = 0;
  
  // 轮询相关
  private pollingSubscription: Subscription | null = null;
  private readonly POLLING_INTERVAL = 5000; // 5秒轮询一次
  private inFlight = 0; // 当前进行中的请求计数，避免重复阻塞
  autoRefresh = true;
  showRaw = false;

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private apiService: ApiService,
    private message: NzMessageService,
    private modal: NzModalService,
    private cdr: ChangeDetectorRef,
    private zone: NgZone
  ) {}

  ngOnInit(): void {
    console.log('🚀 RebuildTaskDetailComponent 初始化');
    
    // 获取路由参数
    this.route.params.subscribe(params => {
      this.namespace = params['namespace'];
      this.taskName = params['name'];
      
      console.log('📍 路由参数:', { namespace: this.namespace, taskName: this.taskName });
      
      if (!this.namespace || !this.taskName) {
        this.errorMessage = '缺少必要的路由参数：namespace 或 name';
        this.loading = false;
        return;
      }
      
      // 开始加载数据
      this.loadTaskData();
      this.startPolling();
    });
  }

  ngOnDestroy(): void {
    console.log('💀 RebuildTaskDetailComponent 销毁');
    this.stopPolling();
  }

  /**
   * 加载任务数据
   */
  loadTaskData(): void {
    console.log('🔄 开始加载任务数据...', { namespace: this.namespace, taskName: this.taskName });
    
    this.loading = true;
    this.errorMessage = '';
    this.apiCallCount++;
    
    this.inFlight++;
    this.apiService.getXStoreFollower(this.namespace, this.taskName).subscribe({
      next: (data) => {
        console.log('✅ 任务数据加载成功:', data);
        this.task = data;
        // 预渲染 JSON 字符串，避免模板反复 stringify 带来的主线程开销
        this.taskJson = JSON.stringify(this.task, null, 2);
        this.loading = false;
        this.errorMessage = '';
        
        // 标记变更（OnPush）
        this.cdr.markForCheck();
        console.log('🔄 变更检测已触发，loading状态:', this.loading);
      },
      error: (error) => {
        console.error('❌ 任务数据加载失败:', error);
        this.loading = false;
        this.task = null;
        this.taskJson = '';
        
        if (error.status === 404) {
          this.errorMessage = '任务不存在或已被删除';
        } else if (error.status === 0) {
          this.errorMessage = '网络连接失败，请检查网络状态';
        } else {
          this.errorMessage = `加载失败: ${error.message || '未知错误'}`;
        }
        
        // 标记变更（OnPush）
        this.cdr.markForCheck();
        console.log('🔄 错误处理后变更检测已触发，loading状态:', this.loading);
      },
      complete: () => {
        this.inFlight = Math.max(this.inFlight - 1, 0);
      }
    });
  }

  /**
   * 手动刷新
   */
  manualRefresh(): void {
    console.log('🔄 手动刷新任务数据');
    console.log('🔍 刷新前状态:', { loading: this.loading, task: !!this.task, errorMessage: this.errorMessage });
    this.loadTaskData();
  }

  /**
   * 开始轮询
   */
  startPolling(): void {
    console.log('⏰ 开始轮询任务状态');
    
    // 在 Angular 之外启动计时器，降低变更检测频率
    this.zone.runOutsideAngular(() => {
      this.pollingSubscription = interval(this.POLLING_INTERVAL).subscribe(() => {
        if (document.visibilityState !== 'visible') return;
        // 只有在有任务且未到终态且当前不在显式加载时才轮询
        if (this.task && !this.isEndPhase() && !this.loading) {
          this.zone.run(() => {
            this.loadTaskDataSilently();
          });
        }
      });
    });
  }

  /**
   * 停止轮询
   */
  stopPolling(): void {
    if (this.pollingSubscription) {
      console.log('⏹️ 停止轮询');
      this.pollingSubscription.unsubscribe();
      this.pollingSubscription = null;
    }
  }

  /**
   * 静默加载数据（不显示loading状态）
   */
  private loadTaskDataSilently(): void {
    if (this.inFlight > 0) return; // 避免并发叠加
    this.inFlight++;
    this.apiService.getXStoreFollower(this.namespace, this.taskName).subscribe({
      next: (data) => {
        console.log('🔄 静默更新任务数据成功');
        this.task = data;
        this.taskJson = JSON.stringify(this.task, null, 2);
        // 标记变更（OnPush）
        this.cdr.markForCheck();
      },
      error: (error) => {
        console.warn('⚠️ 静默更新失败:', error);
      },
      complete: () => {
        this.inFlight = Math.max(this.inFlight - 1, 0);
      }
    });
  }

  /**
   * 返回任务列表
   */
  goBack(): void {
    this.router.navigate(['/storage/xstore-rebuild/rebuild/tasks']);
  }

  /**
   * 获取任务图标
   */
  getTaskIcon(): string {
    if (!this.task) return 'setting';
    switch (this.task.spec.role) {
      case 'learner': return 'experiment';
      case 'logger': return 'file-text';
      case 'follower': return 'share-alt';
      default: return 'setting';
    }
  }

  /**
   * 获取角色颜色
   */
  getRoleColor(): string {
    if (!this.task) return 'default';
    switch (this.task.spec.role) {
      case 'learner': return 'purple';
      case 'logger': return 'cyan';
      case 'follower': return 'blue';
      default: return 'default';
    }
  }

  /**
   * 获取角色显示名称
   */
  getRoleDisplayName(): string {
    if (!this.task) return '';
    switch (this.task.spec.role) {
      case 'learner': return 'Learner';
      case 'logger': return 'Logger';
      case 'follower': return 'Follower';
      default: return this.task.spec.role || '';
    }
  }

  /**
   * 获取状态显示文本
   */
  getDisplayStatus(): string {
    if (!this.task) return '';
    const phase = this.task.status?.phase || '';
    const statusMap: { [key: string]: string } = {
      '': '初始化中',
      'FollowerPhaseNew': '已创建',
      'FollowerPhaseCheck': '环境检查',
      'FollowerPhaseBackupPrepare': '备份准备',
      'FollowerPhaseBackupStart': '开始备份',
      'FollowerPhaseBackup': '备份中',
      'FollowerPhaseLoggerCreate': '创建日志器',
      'FollowerPhaseLoggerRebuild': '重建日志',
      'FollowerCreateRemotePod': '创建远程Pod',
      'FollowerPhaseMonitorBackup': '监控备份',
      'FollowerPhaseBeforeRestore': '准备恢复',
      'FollowerPhaseRestore': '恢复中',
      'FollowerPhaseAfterRestore': '完成恢复',
      'FollowerPhaseWaitSwitch': '等待切换',
      'FollowerPhaseSuccess': '成功',
      'FollowerPhaseFailed': '失败',
      'FollowerPhaseDeleting': '删除中'
    };
    return statusMap[phase] || phase || '未知状态';
  }

  /**
   * 获取状态颜色
   */
  getStatusColor(): string {
    if (!this.task) return 'default';
    const phase = this.task.status?.phase || '';
    switch (phase) {
      case 'FollowerPhaseSuccess': return 'green';
      case 'FollowerPhaseFailed': return 'red';
      case 'FollowerPhaseDeleting':
      case 'FollowerPhaseWaitSwitch': return 'orange';
      default: return 'blue';
    }
  }

  /**
   * 获取进度百分比
   */
  getProgressPercent(): number {
    if (!this.task) return 0;
    const phase = this.task.status?.phase || '';
    const progressMap: { [key: string]: number } = {
      '': 0,
      'FollowerPhaseNew': 5,
      'FollowerPhaseCheck': 10,
      'FollowerPhaseBackupPrepare': 20,
      'FollowerPhaseBackupStart': 25,
      'FollowerPhaseBackup': 40,
      'FollowerPhaseLoggerCreate': 35,
      'FollowerPhaseLoggerRebuild': 50,
      'FollowerCreateRemotePod': 45,
      'FollowerPhaseMonitorBackup': 60,
      'FollowerPhaseBeforeRestore': 70,
      'FollowerPhaseRestore': 80,
      'FollowerPhaseAfterRestore': 90,
      'FollowerPhaseWaitSwitch': 95,
      'FollowerPhaseSuccess': 100,
      'FollowerPhaseFailed': 0,
      'FollowerPhaseDeleting': 0
    };
    return progressMap[phase] || 0;
  }

  /**
   * 获取进度状态
   */
  getProgressStatus(): 'success' | 'exception' | 'active' | 'normal' {
    if (!this.task) return 'active';
    const phase = this.task.status?.phase || '';
    if (phase === 'FollowerPhaseSuccess') return 'success';
    if (phase === 'FollowerPhaseFailed') return 'exception';
    return 'active';
  }

  /**
   * 是否为结束阶段
   */
  isEndPhase(): boolean {
    if (!this.task) return false;
    const phase = this.task.status?.phase || '';
    return ['FollowerPhaseSuccess', 'FollowerPhaseFailed', 'FollowerPhaseDeleting'].includes(phase);
  }

  /**
   * 是否可以重试
   */
  canRetry(): boolean {
    return this.task?.status?.phase === 'FollowerPhaseFailed';
  }

  /**
   * 获取目标Pod名称
   */
  getTargetPodName(): string {
    if (!this.task) return '';
    return this.task.status?.targetPodName || this.task.spec.targetPodName || '';
  }

  /**
   * 获取目标节点名称
   */
  getTargetNodeName(): string {
    if (!this.task) return '';
    return this.task.status?.rebuildNodeName || this.task.spec.nodeName || '';
  }

  /**
   * 格式化时间
   */
  formatTime(timestamp?: string): string {
    if (!timestamp) return '-';
    return new Date(timestamp).toLocaleString('zh-CN', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    });
  }

  /**
   * 格式化任务JSON数据
   */
  formatTaskJson(): string {
    if (!this.task) return '';
    return JSON.stringify(this.task, null, 2);
  }

  /**
   * 获取执行步骤
   */
  getSteps(): Array<{title: string, description: string, status: string}> {
    if (!this.task) return [];
    
    const currentPhase = this.task.status?.phase || '';
    const steps = [
      { phase: 'FollowerPhaseNew', title: '任务创建', description: '创建重搭任务' },
      { phase: 'FollowerPhaseCheck', title: '环境检查', description: '检查重搭环境和条件' },
      { phase: 'FollowerPhaseBackupPrepare', title: '备份准备', description: '准备备份操作' },
      { phase: 'FollowerPhaseBackupStart', title: '开始备份', description: '启动数据备份' },
      { phase: 'FollowerPhaseBackup', title: '执行备份', description: '执行数据备份操作' },
      { phase: 'FollowerPhaseLoggerCreate', title: '创建日志器', description: '创建日志收集器' },
      { phase: 'FollowerPhaseLoggerRebuild', title: '重建日志', description: '重建日志节点' },
      { phase: 'FollowerCreateRemotePod', title: '创建远程Pod', description: '创建远程执行Pod' },
      { phase: 'FollowerPhaseMonitorBackup', title: '监控备份', description: '监控备份进度' },
      { phase: 'FollowerPhaseBeforeRestore', title: '准备恢复', description: '准备数据恢复' },
      { phase: 'FollowerPhaseRestore', title: '执行恢复', description: '执行数据恢复操作' },
      { phase: 'FollowerPhaseAfterRestore', title: '完成恢复', description: '完成数据恢复' },
      { phase: 'FollowerPhaseWaitSwitch', title: '等待切换', description: '等待角色切换' },
      { phase: 'FollowerPhaseSuccess', title: '任务完成', description: '重搭任务成功完成' }
    ];

    return steps.map(step => {
      let status = 'wait';
      if (step.phase === currentPhase) {
        status = currentPhase === 'FollowerPhaseFailed' ? 'error' : 'process';
      } else if (this.isPhaseReached(step.phase, currentPhase)) {
        status = 'finish';
      }
      
      return {
        title: step.title,
        description: step.description,
        status
      };
    });
  }

  /**
   * 获取当前步骤索引
   */
  getCurrentStepIndex(): number {
    if (!this.task) return 0;
    const steps = this.getSteps();
    const processIndex = steps.findIndex(step => step.status === 'process');
    return processIndex >= 0 ? processIndex : 0;
  }

  /**
   * 判断阶段是否已到达
   */
  private isPhaseReached(targetPhase: string, currentPhase: string): boolean {
    const phaseOrder = [
      'FollowerPhaseNew', 'FollowerPhaseCheck', 'FollowerPhaseBackupPrepare',
      'FollowerPhaseBackupStart', 'FollowerPhaseBackup', 'FollowerPhaseLoggerCreate',
      'FollowerPhaseLoggerRebuild', 'FollowerCreateRemotePod', 'FollowerPhaseMonitorBackup',
      'FollowerPhaseBeforeRestore', 'FollowerPhaseRestore', 'FollowerPhaseAfterRestore',
      'FollowerPhaseWaitSwitch', 'FollowerPhaseSuccess'
    ];
    
    const targetIndex = phaseOrder.indexOf(targetPhase);
    const currentIndex = phaseOrder.indexOf(currentPhase);
    
    return targetIndex < currentIndex;
  }

  /**
   * 停止任务
   */
  stopTask(): void {
    if (!this.task) return;
    
    this.modal.confirm({
      nzTitle: '确认停止任务',
      nzContent: `确定要停止重搭任务 "${this.task.metadata.name}" 吗？`,
      nzOnOk: () => new Promise<void>((resolve, reject) => {
        const t = this.task!;
        this.apiService.cancelXStoreFollower(t.metadata.namespace, t.metadata.name)
          .subscribe({
            next: () => { 
              this.message.success('任务停止成功'); 
              this.loadTaskData(); 
              resolve(); 
            },
            error: (error) => { 
              this.message.error('停止任务失败: ' + (error as any)?.message || '未知错误'); 
              reject(error); 
            }
          });
      })
    });
  }

  /**
   * 重试任务
   */
  retryTask(): void {
    if (!this.task) return;
    
    this.modal.confirm({
      nzTitle: '确认重试任务',
      nzContent: `确定要重试重搭任务 "${this.task.metadata.name}" 吗？`,
      nzOnOk: () => new Promise<void>((resolve, reject) => {
        const t = this.task!;
        this.apiService.retryXStoreFollower(t.metadata.namespace, t.metadata.name)
          .subscribe({
            next: () => { 
              this.message.success('任务重试成功'); 
              this.loadTaskData(); 
              resolve(); 
            },
            error: (error) => { 
              this.message.error('重试任务失败: ' + (error as any)?.message || '未知错误'); 
              reject(error); 
            }
          });
      })
    });
  }

  /**
   * 删除任务
   */
  deleteTask(): void {
    if (!this.task) return;
    
    this.modal.confirm({
      nzTitle: '确认删除任务',
      nzContent: `确定要删除重搭任务 "${this.task.metadata.name}" 吗？此操作不可撤销。`,
      nzOkDanger: true,
      nzOnOk: () => new Promise<void>((resolve, reject) => {
        const t = this.task!;
        this.apiService.deleteXStoreFollower(t.metadata.namespace, t.metadata.name)
          .subscribe({
            next: () => { 
              this.message.success('任务删除成功'); 
              this.goBack(); 
              resolve(); 
            },
            error: (error) => { 
              this.message.error('删除任务失败: ' + (error as any)?.message || '未知错误'); 
              reject(error); 
            }
          });
      })
    });
  }

  toggleAutoRefresh(): void {
    this.autoRefresh = !this.autoRefresh;
    if (!this.autoRefresh) {
      this.stopPolling();
    } else {
      this.startPolling();
    }
  }

  toggleRaw(): void {
    this.showRaw = !this.showRaw;
    this.cdr.markForCheck();
  }

  /**
   * 强制更新（调试用）
   */
  forceUpdate(): void {
    console.log('🔄 强制更新UI状态');
    console.log('当前状态:', { loading: this.loading, task: !!this.task, errorMessage: this.errorMessage });
    this.cdr.detectChanges();
  }
}