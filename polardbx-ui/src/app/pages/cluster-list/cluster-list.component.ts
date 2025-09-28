import { Component, OnInit, OnDestroy, inject } from '@angular/core';
import { CommonModule, DatePipe } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { NzPageHeaderModule } from 'ng-zorro-antd/page-header';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzModalModule, NzModalService } from 'ng-zorro-antd/modal';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzSpaceModule } from 'ng-zorro-antd/space';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzStatisticModule } from 'ng-zorro-antd/statistic';
import { NzDropDownModule } from 'ng-zorro-antd/dropdown';
import { NzMenuModule } from 'ng-zorro-antd/menu';
import { ClusterCreationWizardComponent } from '../../components/cluster-creation-wizard/cluster-creation-wizard.component';
import { Subscription, interval, Observable, fromEvent } from 'rxjs';
import { ApiService } from '../../services/api.service';
import { PolarDBXCluster } from '../../models/cluster.model';
import { NotificationService } from '../../services/notification.service';
import { LoadingService, LoadingKeys } from '../../services/loading.service';


@Component({
  selector: 'app-cluster-list',
  standalone: true,
  imports: [
    CommonModule,
    DatePipe,
    FormsModule,
    NzPageHeaderModule,
    NzButtonModule,
    NzIconModule,
    NzTableModule,
    NzTagModule,
    NzSpinModule,
    NzToolTipModule,
    NzInputModule,
    NzCardModule,
    NzSelectModule,
    NzEmptyModule,
    NzSpaceModule,
    NzDividerModule,
    NzGridModule,
    NzStatisticModule,
    NzDropDownModule,
    NzMenuModule,
    NzModalModule
  ],
  templateUrl: './cluster-list.component.html',
  styleUrl: './cluster-list.component.scss'
})
export class ClusterListComponent implements OnInit, OnDestroy {
  private router = inject(Router);
  private apiService = inject(ApiService);
  private messageService = inject(NzMessageService);
  private modalService = inject(NzModalService);
  private notificationService = inject(NotificationService);
  private loadingService = inject(LoadingService);

  clusters: PolarDBXCluster[] = [];
  filteredClusters: PolarDBXCluster[] = [];
  searchTerm = '';
  isLoading$: Observable<boolean>;
  deleteLoading$: Observable<boolean>;
  private subscriptions: Subscription[] = [];
  private refreshInterval?: Subscription;
  private visibilitySub?: Subscription;
  displayedColumns: string[] = ['name', 'namespace', 'phase', 'creationTimestamp', 'actions'];

  refreshOptions = [
    { label: '关闭', value: 0 },
    { label: '5 秒', value: 5000 },
    { label: '30 秒', value: 30000 }
  ];

  // 默认关闭自动刷新（本地有配置则沿用）
  private refreshMs = Number(localStorage.getItem('clusterList.refreshMs') ?? 0);

  constructor() {
    this.isLoading$ = this.loadingService.getLoadingState(LoadingKeys.CLUSTERS_LIST);
    this.deleteLoading$ = this.loadingService.getLoadingState(LoadingKeys.CLUSTER_DELETE);
  }

  ngOnInit() {
    this.loadClusters();
    this.startPolling();
    // 页面可见性变化时控制轮询，并在恢复可见时立刻刷新
    this.visibilitySub = fromEvent(document, 'visibilitychange').subscribe(() => {
      if (document.visibilityState === 'visible') {
        this.loadClusters();
        this.startPolling();
      } else {
        this.stopPolling();
      }
    });
  }

  ngOnDestroy() {
    this.subscriptions.forEach(sub => sub.unsubscribe());
    this.stopPolling();
    if (this.visibilitySub) {
      this.visibilitySub.unsubscribe();
    }
  }

  private startPolling() {
    this.stopPolling();
    if (this.refreshMs <= 0) {
      return; // 关闭自动刷新
    }
    this.refreshInterval = interval(this.refreshMs).subscribe(() => {
      if (document.visibilityState === 'visible') {
        this.loadClusters(); // 静默刷新，不显示加载状态
      }
    });
  }

  private stopPolling() {
    if (this.refreshInterval) {
      this.refreshInterval.unsubscribe();
      this.refreshInterval = undefined;
    }
  }

  onChangeRefreshMs(ms: number) {
    this.refreshMs = Number(ms) || 0;
    localStorage.setItem('clusterList.refreshMs', String(this.refreshMs));
    this.startPolling();
    const label = this.refreshOptions.find(o => o.value === this.refreshMs)?.label || '关闭';
    this.messageService.success(`自动刷新：${label}`);
  }

  get currentRefreshMs(): number {
    return this.refreshMs;
  }

  loadClusters() {
    const subscription = this.apiService.getClusters().subscribe({
      next: (clusters) => {
        this.clusters = clusters;
        this.applyFilter();
        if (clusters.length === 0) {
          this.notificationService.info('暂无集群，点击"创建集群"开始使用');
        }
      },
      error: (error) => {
        console.error('Error loading clusters:', error);
        this.notificationService.loadError();
      }
    });
    this.subscriptions.push(subscription);
  }

  onSearchChange() {
    this.applyFilter();
  }

  applyFilter() {
    if (!this.searchTerm.trim()) {
      this.filteredClusters = [...this.clusters];
    } else {
      const searchLower = this.searchTerm.toLowerCase().trim();
      this.filteredClusters = this.clusters.filter(cluster => 
        cluster.metadata.name.toLowerCase().includes(searchLower) ||
        cluster.metadata.namespace.toLowerCase().includes(searchLower) ||
        (cluster.status?.phase || '').toLowerCase().includes(searchLower)
      );
    }

    if (this.filteredClusters.length === 0 && this.searchTerm.trim() && this.clusters.length > 0) {
      this.notificationService.info(`未找到包含 "${this.searchTerm}" 的集群`);
    }
  }

  clearSearch() {
    this.searchTerm = '';
    this.applyFilter();
  }

  viewClusterDetail(cluster: PolarDBXCluster): void {
    console.log('跳转到集群详情:', cluster.metadata.namespace, cluster.metadata.name);
    this.router.navigate(['/clusters', cluster.metadata.namespace, cluster.metadata.name]);
  }

  openCreateClusterDialog(): void {
    const modal = this.modalService.create({
      nzTitle: '创建集群',
      nzContent: ClusterCreationWizardComponent,
      nzWidth: '90vw',
      nzStyle: { top: '20px' },
      nzBodyStyle: { padding: '0' },
      nzMaskClosable: false,
      nzFooter: null
    });

    modal.afterClose.subscribe(result => {
      if (result && result.success) {
        this.messageService.success('集群创建成功！');
        // 刷新集群列表
        setTimeout(() => {
          this.loadClusters();
        }, 2000);
      }
    });
  }

  private createCluster(config: any) {
    console.log('Creating cluster with config:', config);
    this.notificationService.info('集群创建请求已提交，正在处理中...');
    setTimeout(() => {
      this.loadClusters();
      this.notificationService.success('集群创建任务已启动，请稍后查看状态');
    }, 1000);
  }

  deleteCluster(cluster: PolarDBXCluster) {
    const confirmSubscription = this.notificationService.confirmDelete(
      cluster.metadata.name,
      '集群'
    ).subscribe(confirmed => {
      if (confirmed) {
        this.performDelete(cluster);
      }
    });
    this.subscriptions.push(confirmSubscription);
  }

  private performDelete(cluster: PolarDBXCluster): void {
    const deleteSubscription = this.apiService.deleteCluster(
      cluster.metadata.namespace, 
      cluster.metadata.name
    ).subscribe({
      next: () => {
        this.notificationService.operationSuccess('删除集群', cluster.metadata.name);
        this.loadClusters();
      },
      error: (error) => {
        console.error('Error deleting cluster:', error);
        this.notificationService.operationError('删除集群', cluster.metadata.name);
      }
    });
    this.subscriptions.push(deleteSubscription);
  }

  getPhaseColor(phase?: string): string {
    switch (phase?.toLowerCase()) {
      case 'running':
        return 'primary';
      case 'creating':
        return 'accent';
      case 'failed':
        return 'warn';
      default:
        return '';
    }
  }

  getPhaseTagColor(phase?: string): string {
    switch (phase?.toLowerCase()) {
      case 'running':
        return 'green';
      case 'creating':
        return 'orange';
      case 'failed':
        return 'red';
      default:
        return 'default';
    }
  }

  getPhaseIcon(phase?: string): string {
    switch (phase?.toLowerCase()) {
      case 'running':
        return 'check-circle';
      case 'creating':
        return 'loading';
      case 'failed':
        return 'exclamation-circle';
      default:
        return 'question-circle';
    }
  }

  getClusterCountByPhase(phase: string): number {
    return this.clusters.filter(cluster => 
      cluster.status?.phase?.toLowerCase() === phase.toLowerCase()
    ).length;
  }

  disconnect() {
    const confirmSubscription = this.notificationService.confirmAction(
      '断开连接',
      '当前 Kubernetes 集群',
      '断开连接后需要重新上传 Kubeconfig 文件'
    ).subscribe(confirmed => {
      if (confirmed) {
        sessionStorage.removeItem('kubeconfig');
        this.notificationService.info('已断开连接');
        this.router.navigate(['/connect']);
      }
    });
    this.subscriptions.push(confirmSubscription);
  }
}
