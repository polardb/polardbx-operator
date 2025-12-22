import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzProgressModule } from 'ng-zorro-antd/progress';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzDropDownModule } from 'ng-zorro-antd/dropdown';
import { NzModalModule, NzModalService } from 'ng-zorro-antd/modal';
import { Subject, timer, forkJoin, of, BehaviorSubject } from 'rxjs';
import { takeUntil, finalize, catchError, map } from 'rxjs/operators';

import { ApiService } from '../../services/api.service';
import { LoadingService } from '../../services/loading.service';
import { XStoreFollower } from '../../models/xstore-follower.model';

interface TaskListItem {
  task: XStoreFollower;
  displayStatus: string;
  statusColor: string;
  progressPercent: number;
  isEndPhase: boolean;
}

@Component({
  selector: 'app-rebuild-task-list',
  standalone: true,
  imports: [
    CommonModule,
    RouterModule,
    FormsModule,
    NzCardModule,
    NzTableModule,
    NzButtonModule,
    NzIconModule,
    NzTagModule,
    NzSpinModule,
    NzSelectModule,
    NzInputModule,
    NzToolTipModule,
    NzProgressModule,
    NzEmptyModule,
    NzDividerModule,
    NzDropDownModule,
    NzModalModule
  ],
  template: `
    <div class="rebuild-task-list-container">
      <!-- 工具栏 -->
      <nz-card class="toolbar-card">
        <div class="toolbar-content">
          <div class="toolbar-left">
            <h3 class="page-title">
              <i nz-icon nzType="bars"></i>
              重搭任务列表
            </h3>
          </div>
          <div class="toolbar-right">
            <div class="filters">
              <!-- 命名空间筛选 -->
              <nz-select 
                class="filter-control"
                [(ngModel)]="selectedNamespace" 
                (ngModelChange)="onNamespaceChange($event)"
                nzPlaceHolder="选择命名空间" 
                nzAllowClear>
                <nz-option *ngFor="let ns of namespaces" [nzValue]="ns" [nzLabel]="ns"></nz-option>
              </nz-select>
              
              <!-- 角色筛选 -->
              <nz-select 
                class="filter-control"
                [(ngModel)]="selectedRole" 
                (ngModelChange)="applyFilters()"
                nzPlaceHolder="选择角色" 
                nzAllowClear>
                <nz-option nzValue="learner" nzLabel="Learner"></nz-option>
                <nz-option nzValue="logger" nzLabel="Logger"></nz-option>
                <nz-option nzValue="follower" nzLabel="Follower"></nz-option>
              </nz-select>
              
              <!-- 状态筛选 -->
              <nz-select 
                class="filter-control"
                [(ngModel)]="selectedPhase" 
                (ngModelChange)="applyFilters()"
                nzPlaceHolder="选择状态" 
                nzAllowClear>
                <nz-option *ngFor="let phase of availablePhases" [nzValue]="phase.value" [nzLabel]="phase.label"></nz-option>
              </nz-select>
              
              <!-- XStore 筛选 -->
              <div class="search-box">
                <i nz-icon nzType="search"></i>
                <input 
                  nz-input
                  [(ngModel)]="xstoreFilter" 
                  (ngModelChange)="applyFilters()"
                  placeholder="搜索 XStore" />
                <button
                  nz-button
                  nzSize="small"
                  class="search-clear"
                  *ngIf="xstoreFilter"
                  (click)="clearXstoreFilter()">
                  <i nz-icon nzType="close"></i>
                </button>
              </div>
            </div>
            
            <nz-divider nzType="vertical"></nz-divider>
            
            <div class="actions">
              <button 
                nz-button 
                nzType="default" 
                (click)="refreshTasks()" 
                [nzLoading]="isLoading">
                <i nz-icon nzType="reload"></i>
                刷新
              </button>
              <button 
                nz-button 
                nzType="primary" 
                routerLink="/storage/xstore-rebuild/rebuild/new">
                <i nz-icon nzType="plus"></i>
                创建任务
              </button>
            </div>
          </div>
        </div>
      </nz-card>

      <!-- 任务表格 -->
      <nz-card class="table-card">
        <div class="table-container">
          <nz-table 
            [nzData]="filteredTasks" 
            [nzPageSize]="pageSize"
            [nzShowPagination]="filteredTasks.length > pageSize"
            [nzLoading]="isLoading"
            class="tasks-table">
            <thead>
              <tr>
                <th>任务名</th>
                <th>角色</th>
                <th>命名空间</th>
                <th>XStore</th>
                <th>源 Pod</th>
                <th>目标 Pod</th>
                <th>目标节点</th>
                <th>本机/跨机</th>
                <th>状态</th>
                <th>进度</th>
                <th>消息</th>
                <th>创建时间</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              <tr *ngFor="let item of filteredTasks; trackBy: trackByTaskName">
                <!-- 任务名 -->
                <td>
                  <div class="task-name">
                    <i nz-icon [nzType]="getTaskIcon(item.task)" class="task-icon"></i>
                    <span>{{ item.task.metadata.name }}</span>
                  </div>
                </td>
                
                <!-- 角色 -->
                <td>
                  <nz-tag [nzColor]="getRoleColor(item.task.spec.role || 'follower')">
                    {{ getRoleDisplayName(item.task.spec.role || 'follower') }}
                  </nz-tag>
                </td>
                
                <!-- 命名空间 -->
                <td>{{ item.task.metadata.namespace }}</td>
                
                <!-- XStore -->
                <td>{{ item.task.spec.xStoreName }}</td>
                
                <!-- 源 Pod -->
                <td>
                  <span *ngIf="item.task.spec.fromPodName; else noFromPod">
                    {{ item.task.spec.fromPodName }}
                  </span>
                  <ng-template #noFromPod>
                    <span class="no-data">-</span>
                  </ng-template>
                </td>
                
                <!-- 目标 Pod -->
                <td>
                  <span *ngIf="getTargetPodName(item.task); else noTargetPod">
                    {{ getTargetPodName(item.task) }}
                  </span>
                  <ng-template #noTargetPod>
                    <span class="no-data">-</span>
                  </ng-template>
                </td>
                
                <!-- 目标节点 -->
                <td>
                  <span *ngIf="getTargetNodeName(item.task); else noTargetNode">
                    {{ getTargetNodeName(item.task) }}
                  </span>
                  <ng-template #noTargetNode>
                    <span class="no-data">-</span>
                  </ng-template>
                </td>
                
                <!-- 本机/跨机 -->
                <td>
                  <nz-tag [nzColor]="item.task.spec.local ? 'blue' : 'orange'">
                    {{ item.task.spec.local ? '本机' : '跨机' }}
                  </nz-tag>
                </td>
                
                <!-- 状态 -->
                <td>
                  <nz-tag [nzColor]="item.statusColor" class="status-tag">
                    {{ item.displayStatus }}
                  </nz-tag>
                </td>
                
                <!-- 进度 -->
                <td>
                  <div class="progress-cell">
                    <nz-progress 
                      [nzPercent]="item.progressPercent" 
                      [nzStatus]="getProgressStatus(item)"
                      nzSize="small"
                      [nzShowInfo]="false"
                      style="width: 80px;">
                    </nz-progress>
                    <span class="progress-text">{{ item.progressPercent }}%</span>
                  </div>
                </td>
                
                <!-- 消息 -->
                <td>
                  <span 
                    *ngIf="item.task.status?.message; else noMessage"
                    nz-tooltip
                    [nzTooltipTitle]="item.task.status?.message"
                    class="message-text">
                    {{ (item.task.status?.message || '') | slice:0:30 }}{{ (item.task.status?.message || '').length > 30 ? '...' : '' }}
                  </span>
                  <ng-template #noMessage>
                    <span class="no-data">-</span>
                  </ng-template>
                </td>
                
                <!-- 创建时间 -->
                <td>
                  <span>{{ formatTime(item.task.metadata.creationTimestamp) }}</span>
                </td>
                
                <!-- 操作 -->
                <td>
                  <div class="action-buttons">
                    <button 
                      nz-button 
                      nzType="link" 
                      nzSize="small"
                      [routerLink]="['/storage/xstore-rebuild/rebuild/tasks', item.task.metadata.namespace, item.task.metadata.name]">
                      <i nz-icon nzType="eye"></i>
                      详情
                    </button>
                    
                    <button 
                      nz-button 
                      nzType="link" 
                      nzSize="small"
                      *ngIf="!item.isEndPhase"
                      (click)="stopTask(item.task)"
                      nz-tooltip="停止任务">
                      <i nz-icon nzType="pause"></i>
                    </button>
                    
                    <button 
                      nz-button 
                      nzType="link" 
                      nzSize="small"
                      *ngIf="canRetry(item.task)"
                      (click)="retryTask(item.task)"
                      nz-tooltip="重试任务">
                      <i nz-icon nzType="redo"></i>
                    </button>
                    
                    <button 
                      nz-button 
                      nzType="link" 
                      nzSize="small"
                      nzDanger
                      *ngIf="item.isEndPhase"
                      (click)="deleteTask(item.task)"
                      nz-tooltip="删除任务">
                      <i nz-icon nzType="delete"></i>
                    </button>
                  </div>
                </td>
              </tr>
            </tbody>
          </nz-table>
          
          <!-- 空状态 -->
          <nz-empty *ngIf="filteredTasks.length === 0" nzNotFoundContent="暂无重搭任务">
            <div nz-empty-footer>
              <button nz-button nzType="primary" routerLink="/storage/xstore-rebuild/rebuild/new">
                创建第一个重搭任务
              </button>
            </div>
          </nz-empty>
        </div>
      </nz-card>
    </div>
  `,
  styleUrls: ['./rebuild-task-list.component.scss']
})
export class RebuildTaskListComponent implements OnInit, OnDestroy {
  private destroy$ = new Subject<void>();
  
  tasks: XStoreFollower[] = [];
  filteredTasks: TaskListItem[] = [];
  private itemsMap = new Map<string, TaskListItem>();
  namespaces: string[] = [];
  isLoading = false;
  pageSize = 20;
  
  // 筛选条件
  selectedNamespace = '';
  selectedRole = '';
  selectedPhase = '';
  xstoreFilter = '';
  
  // 可用的状态选项
  availablePhases = [
    { value: '', label: '初始化中' },
    { value: 'FollowerPhaseNew', label: '已创建' },
    { value: 'FollowerPhaseCheck', label: '检查中' },
    { value: 'FollowerPhaseBackupPrepare', label: '备份准备' },
    { value: 'FollowerPhaseBackupStart', label: '开始备份' },
    { value: 'FollowerPhaseBackup', label: '备份中' },
    { value: 'FollowerPhaseLoggerCreate', label: '创建日志器' },
    { value: 'FollowerPhaseLoggerRebuild', label: '重建日志' },
    { value: 'FollowerCreateRemotePod', label: '创建远程Pod' },
    { value: 'FollowerPhaseMonitorBackup', label: '监控备份' },
    { value: 'FollowerPhaseBeforeRestore', label: '准备恢复' },
    { value: 'FollowerPhaseRestore', label: '恢复中' },
    { value: 'FollowerPhaseAfterRestore', label: '完成恢复' },
    { value: 'FollowerPhaseWaitSwitch', label: '等待切换' },
    { value: 'FollowerPhaseSuccess', label: '成功' },
    { value: 'FollowerPhaseFailed', label: '失败' },
    { value: 'FollowerPhaseDeleting', label: '删除中' }
  ];

  constructor(
    private apiService: ApiService,
    private loadingService: LoadingService,
    private message: NzMessageService,
    private modal: NzModalService
  ) {}

  ngOnInit(): void {
    this.loadNamespaces();
    this.startPolling();
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  private async loadNamespaces(): Promise<void> {
    try {
      // TODO: 实现获取命名空间的 API
      const namespaces = ['default', 'polardbx-operator-system'];
      this.namespaces = namespaces || ['default'];
    } catch (error) {
      console.error('Failed to load namespaces:', error);
      this.namespaces = ['default'];
    }
  }

  private startPolling(): void {
    this.loadTasks();
    timer(5000, 5000)
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => {
        if (document.visibilityState !== 'visible') return;
        const hasActive = this.filteredTasks.some(i => !i.isEndPhase);
        if (hasActive) {
          this.loadTasks(true /*silent*/);
        }
      });
  }

  clearXstoreFilter(): void {
    this.xstoreFilter = '';
    this.applyFilters();
  }

  private loadTasks(silent = false): void {
    if (this.isLoading && !silent) return;
    if (!silent) this.isLoading = true;

    if (this.selectedNamespace) {
      this.apiService.getXStoreFollowers(this.selectedNamespace)
        .pipe(
          takeUntil(this.destroy$),
          finalize(() => { if (!silent) this.isLoading = false; })
        )
        .subscribe({
          next: (tasks) => {
            this.mergeTasks(tasks || []);
          },
          error: (error) => {
            console.error('Failed to load rebuild tasks:', error);
          }
        });
      return;
    }

    // 加载所有命名空间的任务
    const requests = (this.namespaces && this.namespaces.length ? this.namespaces : ['default']).map(ns =>
      this.apiService.getXStoreFollowers(ns).pipe(
        catchError(error => {
          console.warn(`Failed to load tasks from namespace ${ns}:`, error);
          return of([] as XStoreFollower[]);
        })
      )
    );

    forkJoin(requests)
      .pipe(
        takeUntil(this.destroy$),
        finalize(() => { if (!silent) this.isLoading = false; }),
        map(results => results.flat())
      )
      .subscribe({
        next: (allTasks) => {
          this.mergeTasks(allTasks || []);
        },
        error: (error) => {
          console.error('Failed to load rebuild tasks:', error);
        }
      });
  }

  // 将新数据与现有列表做“静默合并”，仅更新变更行，避免闪动
  private mergeTasks(newTasks: XStoreFollower[]): void {
    this.tasks = newTasks;
    const nextMap = new Map<string, TaskListItem>();
    const nextList: TaskListItem[] = [];
    for (const t of newTasks) {
      const key = t.metadata?.name || Math.random().toString();
      const prev = this.itemsMap.get(key);
      // 仅当 phase 或关键信息变化时重建项，否则复用对象避免 DOM 重渲染
      const phase = t.status?.phase || '';
      const changed = !prev || prev.task.status?.phase !== phase || prev.task.spec !== t.spec || prev.task.status !== t.status;
      const item = changed ? this.convertToListItem(t) : { ...prev!, task: t };
      nextMap.set(key, item);
      nextList.push(item);
    }
    this.itemsMap = nextMap;
    this.applyFiltersFromItems(nextList);
  }

  private applyFiltersFromItems(items: TaskListItem[]): void {
    let list = items;
    if (this.selectedRole) {
      list = list.filter(it => it.task.spec.role === this.selectedRole);
    }
    if (this.selectedPhase) {
      list = list.filter(it => it.task.status?.phase === this.selectedPhase);
    }
    if (this.xstoreFilter) {
      const kw = this.xstoreFilter.toLowerCase();
      list = list.filter(it => (it.task.spec.xStoreName || '').toLowerCase().includes(kw));
    }
    this.filteredTasks = list;
  }

  onNamespaceChange(namespace: string): void {
    this.selectedNamespace = namespace;
    this.refreshTasks();
  }

  applyFilters(): void {
    // 从 itemsMap 做过滤，尽量复用现有行对象
    const items = Array.from(this.itemsMap.values());
    this.applyFiltersFromItems(items);
  }

  private convertToListItem(task: XStoreFollower): TaskListItem {
    const phase = task.status?.phase || '';
    return {
      task,
      displayStatus: this.getDisplayStatus(phase),
      statusColor: this.getStatusColor(phase),
      progressPercent: this.getProgressPercent(phase),
      isEndPhase: this.isEndPhase(phase)
    };
  }

  private getDisplayStatus(phase: string): string {
    const statusMap: { [key: string]: string } = {
      '': '初始化中',
      'FollowerPhaseNew': '已创建',
      'FollowerPhaseCheck': '检查中',
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
    return statusMap[phase] || '初始化中';
  }

  private getStatusColor(phase: string): string {
    switch (phase) {
      case 'FollowerPhaseSuccess':
        return 'green';
      case 'FollowerPhaseFailed':
        return 'red';
      case 'FollowerPhaseRestore':
      case 'FollowerPhaseCheck':
      case 'FollowerPhaseMonitorBackup':
      case 'FollowerPhaseLoggerRebuild':
      case 'FollowerPhaseBeforeRestore':
      case 'FollowerPhaseBackup':
      case 'FollowerPhaseBackupStart':
      case 'FollowerPhaseBackupPrepare':
      case 'FollowerPhaseLoggerCreate':
      case 'FollowerCreateRemotePod':
        return 'blue';
      case 'FollowerPhaseDeleting':
      case 'FollowerPhaseWaitSwitch':
        return 'orange';
      default:
        return 'default';
    }
  }

  private getProgressPercent(phase: string): number {
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

  private isEndPhase(phase: string): boolean {
    return ['FollowerPhaseSuccess', 'FollowerPhaseFailed', 'FollowerPhaseDeleting'].includes(phase);
  }

  getTaskIcon(task: XStoreFollower): string {
    switch (task.spec.role) {
      case 'learner': return 'experiment';
      case 'logger': return 'file-text';
      case 'follower': return 'share-alt';
      default: return 'setting';
    }
  }

  getRoleColor(role: string): string {
    switch (role) {
      case 'learner': return 'purple';
      case 'logger': return 'cyan';
      case 'follower': return 'blue';
      default: return 'default';
    }
  }

  getRoleDisplayName(role: string): string {
    switch (role) {
      case 'learner': return 'Learner';
      case 'logger': return 'Logger';
      case 'follower': return 'Follower';
      default: return role;
    }
  }

  getTargetPodName(task: XStoreFollower): string {
    return task.status?.targetPodName || task.spec.targetPodName || '';
  }

  getTargetNodeName(task: XStoreFollower): string {
    return (task.status as any)?.targetNodeName || task.spec.nodeName || '';
  }

  getProgressStatus(item: TaskListItem): 'success' | 'exception' | 'active' | 'normal' {
    if (item.task.status?.phase === 'FollowerPhaseSuccess') return 'success';
    if (item.task.status?.phase === 'FollowerPhaseFailed') return 'exception';
    return 'active';
  }

  formatTime(timestamp?: string): string {
    if (!timestamp) return '-';
    return new Date(timestamp).toLocaleString('zh-CN', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit'
    });
  }

  canRetry(task: XStoreFollower): boolean {
    return task.status?.phase === 'FollowerPhaseFailed';
  }

  trackByTaskName(index: number, item: TaskListItem): string {
    return item.task.metadata.name;
  }

  refreshTasks(): void {
    this.loadTasks();
  }

  stopTask(task: XStoreFollower): void {
    this.modal.confirm({
      nzTitle: '确认停止任务',
      nzContent: `确定要停止重搭任务 "${task.metadata.name}" 吗？`,
      nzOnOk: () => {
        return new Promise((resolve, reject) => {
          this.apiService.cancelXStoreFollower(task.metadata.namespace, task.metadata.name)
            .pipe(takeUntil(this.destroy$))
            .subscribe({
              next: () => {
                this.message.success('任务停止成功');
                this.refreshTasks();
                resolve(undefined);
              },
              error: (error) => {
                this.message.error('停止任务失败: ' + (error as any)?.message || '未知错误');
                reject(error);
              }
            });
        });
      }
    });
  }

  retryTask(task: XStoreFollower): void {
    this.modal.confirm({
      nzTitle: '确认重试任务',
      nzContent: `确定要重试重搭任务 "${task.metadata.name}" 吗？`,
      nzOnOk: () => {
        return new Promise((resolve, reject) => {
          this.apiService.retryXStoreFollower(task.metadata.namespace, task.metadata.name)
            .pipe(takeUntil(this.destroy$))
            .subscribe({
              next: () => {
                this.message.success('任务重试成功');
                this.refreshTasks();
                resolve(undefined);
              },
              error: (error) => {
                this.message.error('重试任务失败: ' + (error as any)?.message || '未知错误');
                reject(error);
              }
            });
        });
      }
    });
  }

  deleteTask(task: XStoreFollower): void {
    this.modal.confirm({
      nzTitle: '确认删除任务',
      nzContent: `确定要删除重搭任务 "${task.metadata.name}" 吗？此操作不可撤销。`,
      nzOkDanger: true,
      nzOnOk: () => {
        return new Promise((resolve, reject) => {
          this.apiService.deleteXStoreFollower(task.metadata.namespace, task.metadata.name)
            .pipe(takeUntil(this.destroy$))
            .subscribe({
              next: () => {
                this.message.success('任务删除成功');
                this.refreshTasks();
                resolve(undefined);
              },
              error: (error) => {
                this.message.error('删除任务失败: ' + (error as any)?.message || '未知错误');
                reject(error);
              }
            });
        });
      }
    });
  }
}
