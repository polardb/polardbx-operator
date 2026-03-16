import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ActivatedRoute } from '@angular/router';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { NzTabsModule } from 'ng-zorro-antd/tabs';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzPageHeaderModule } from 'ng-zorro-antd/page-header';

import { ClusterLogConfigComponent } from '../../components/cluster-log-config/cluster-log-config.component';
import { ApiService } from '../../services/api.service';
import { LoadingService } from '../../services/loading.service';
import { PolarDBXCluster } from '../../models/cluster.model';

@Component({
  selector: 'app-cluster-log-management',
  standalone: true,
  imports: [
    CommonModule,
    NzTabsModule,
    NzCardModule,
    NzButtonModule,
    NzIconModule,
    NzSpinModule,
    NzToolTipModule,
    NzPageHeaderModule,
    ClusterLogConfigComponent
  ],
  template: `
    <div class="cluster-log-management">
      <nz-page-header [nzGhost]="false" nzTitle="集群日志管理" [nzSubtitle]="clusterName ? '管理 ' + clusterName + ' 集群的日志采集配置' : ''"></nz-page-header>

      <nz-spin [nzSpinning]="isLoading" nzTip="正在加载集群信息...">
        <div class="content-container" *ngIf="!isLoading && clusterName">
          <nz-tabset class="main-tabs">
            <nz-tab nzTitle="日志配置">
              <div class="tab-content">
                <app-cluster-log-config [clusterName]="clusterName" [namespace]="namespace"></app-cluster-log-config>
              </div>
            </nz-tab>

            <nz-tab nzTitle="采集器状态">
              <div class="tab-content">
                <nz-card nzTitle="LogCollector 组件状态" class="status-overview-card">
                  <div class="status-grid">
                    <div class="status-item">
                      <i nz-icon nzType="folder-open" class="status-icon"></i>
                      <div class="status-info">
                        <div class="status-label">FileBeat DaemonSet</div>
                        <div class="status-value">{{filebeatStatus}}</div>
                      </div>
                    </div>
                    <div class="status-item">
                      <i nz-icon nzType="deployment-unit" class="status-icon"></i>
                      <div class="status-info">
                        <div class="status-label">LogStash Deployment</div>
                        <div class="status-value">{{logstashStatus}}</div>
                      </div>
                    </div>
                    <div class="status-item">
                      <i nz-icon nzType="cloud-upload" class="status-icon"></i>
                      <div class="status-info">
                        <div class="status-label">输出配置</div>
                        <div class="status-value">{{outputConfig}}</div>
                      </div>
                    </div>
                  </div>

                  <div class="actions-section">
                    <button nz-button nzType="default" (click)="refreshStatus()">
                      <i nz-icon nzType="reload"></i>
                      <span>刷新状态</span>
                    </button>
                    <button nz-button nzType="primary" (click)="viewLogStashLogs()">
                      <i nz-icon nzType="eye"></i>
                      <span>查看 LogStash 日志</span>
                    </button>
                  </div>
                </nz-card>
              </div>
            </nz-tab>

            <nz-tab nzTitle="安装指导">
              <div class="tab-content">
                <nz-card nzTitle="LogCollector 安装指导" class="installation-guide-card">
                  <div class="installation-steps">
                    <div class="step">
                      <h3><span class="step-number">1</span>安装 LogCollector</h3>
                      <p>使用 Helm 安装 PolarDB-X LogCollector：</p>
                      <div class="command-block">
                        <code>helm install --namespace polardbx-logcollector polardbx-logcollector https://github.com/polardb/polardbx-operator/releases/download/v1.7.0/polardbx-logcollector-1.6.2.tgz</code>
                        <button nz-button nzShape="circle" nzType="default" nz-tooltip nzTooltipTitle="复制命令" (click)="copyCommand('install')">
                          <i nz-icon nzType="copy"></i>
                        </button>
                      </div>
                    </div>
                    <div class="step">
                      <h3><span class="step-number">2</span>验证安装</h3>
                      <p>检查组件状态：</p>
                      <div class="command-block">
                        <code>kubectl get pods --namespace polardbx-logcollector</code>
                        <button nz-button nzShape="circle" nzType="default" nz-tooltip nzTooltipTitle="复制命令" (click)="copyCommand('check')">
                          <i nz-icon nzType="copy"></i>
                        </button>
                      </div>
                    </div>
                    <div class="step">
                      <h3><span class="step-number">3</span>开启日志采集</h3>
                      <p>使用上方的“日志配置”标签页开启 CN 和 DN 节点的日志采集。</p>
                    </div>
                    <div class="step">
                      <h3><span class="step-number">4</span>查看采集的日志</h3>
                      <p>查看 LogStash 标准输出中的日志：</p>
                      <div class="command-block">
                        <code>kubectl logs -f deployment/logstash -n polardbx-logcollector</code>
                        <button nz-button nzShape="circle" nzType="default" nz-tooltip nzTooltipTitle="复制命令" (click)="copyCommand('logs')">
                          <i nz-icon nzType="copy"></i>
                        </button>
                      </div>
                    </div>
                  </div>
                  <div class="resources-section">
                    <h3>相关资源</h3>
                    <ul>
                      <li>
                        <a href="https://doc.polardbx.com/operator/ops/logcollector/1-logcollector.html" target="_blank">
                          <i nz-icon nzType="export"></i>
                          <span>PolarDB-X LogCollector 官方文档</span>
                        </a>
                      </li>
                      <li>
                        <i nz-icon nzType="setting"></i>
                        <span>默认配置：FileBeat (500MB 内存 + 1 CPU)，LogStash (1.5GB 内存 + 2 CPU)</span>
                      </li>
                    </ul>
                  </div>
                </nz-card>
              </div>
            </nz-tab>
          </nz-tabset>
        </div>
      </nz-spin>

      <div class="error-container" *ngIf="!isLoading && !clusterName">
        <nz-card class="error-card">
          <div class="error-card-content">
            <i nz-icon nzType="close-circle" class="error-icon"></i>
            <h3>集群信息加载失败</h3>
            <p>无法获取集群信息，请检查集群名称和命名空间是否正确。</p>
            <button nz-button nzType="primary" (click)="loadClusterInfo()">
              <i nz-icon nzType="reload"></i>
              <span>重试</span>
            </button>
          </div>
        </nz-card>
      </div>
    </div>
  `,
  styles: [`
    .cluster-log-management { padding: 16px; max-width: 1400px; margin: 0 auto; }
    .main-tabs { background: #fff; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.04); }
    .tab-content { padding: 16px; }
    .status-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 16px; margin-bottom: 16px; }
    .status-item { display: flex; align-items: center; padding: 16px; background: #f8f9fa; border-radius: 8px; border-left: 4px solid #36d1dc; }
    .status-icon { margin-right: 12px; font-size: 20px; color: #36d1dc; }
    .status-label { font-size: 12px; color: #666; margin-bottom: 4px; }
    .status-value { font-size: 14px; font-weight: 500; }
    .actions-section { display: flex; gap: 12px; padding-top: 12px; border-top: 1px solid #f0f0f0; }
    .installation-steps .step { margin-bottom: 20px; padding: 16px; background: #f8f9fa; border-radius: 8px; border-left: 4px solid #ff9a9e; }
    .step-number { display: inline-flex; align-items: center; justify-content: center; width: 24px; height: 24px; background: #ff9a9e; color: #fff; border-radius: 50%; font-size: 12px; font-weight: 600; margin-right: 8px; }
    .command-block { display: flex; align-items: center; background: #2d3748; color: #e2e8f0; padding: 8px 12px; border-radius: 6px; margin: 6px 0; font-family: 'Courier New', monospace; }
    .command-block code { flex: 1; background: none; color: inherit; font-size: 12px; }
    .resources-section ul { list-style: none; padding: 0; }
    .resources-section li { display: flex; align-items: center; gap: 8px; padding: 6px 0; color: #666; }
    .error-container { display: flex; justify-content: center; padding: 32px 0; }
    .error-card { max-width: 420px; width: 100%; }
    .error-card-content { text-align: center; padding: 16px; }
    .error-icon { font-size: 36px; color: #ff4d4f; margin-bottom: 12px; }
  `]
})
export class ClusterLogManagementComponent implements OnInit, OnDestroy {
  clusterName: string = '';
  namespace: string = 'default';
  isLoading: boolean = true;
  
  // 状态信息
  filebeatStatus: string = '检查中...';
  logstashStatus: string = '检查中...';
  outputConfig: string = '检查中...';
  
  private destroy$ = new Subject<void>();

  constructor(
    private route: ActivatedRoute,
    private apiService: ApiService,
    private loadingService: LoadingService,
    private message: NzMessageService
  ) {}

  ngOnInit(): void {
    this.route.queryParams
      .pipe(takeUntil(this.destroy$))
      .subscribe(params => {
        this.clusterName = params['cluster'] || '';
        this.namespace = params['namespace'] || 'default';
        
        if (this.clusterName) {
          this.loadClusterInfo();
        } else {
          this.isLoading = false;
        }
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  loadClusterInfo(): void {
    if (!this.clusterName) {
      this.isLoading = false;
      return;
    }
    
    this.isLoading = true;
    
    this.apiService.getCluster(this.namespace, this.clusterName)
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: (cluster: PolarDBXCluster) => {
          this.isLoading = false;
          this.refreshStatus();
        },
        error: (error) => {
          console.error('Failed to load cluster:', error);
          this.isLoading = false;
          this.message.error('加载集群信息失败');
        }
      });
  }

  refreshStatus(): void {
    // TODO: 实现状态检查逻辑
    this.filebeatStatus = '运行正常 (3/3)';
    this.logstashStatus = '运行正常 (1/1)';
    this.outputConfig = 'stdout (默认)';
  }

  viewLogStashLogs(): void {
    // TODO: 实现 LogStash 日志查看功能
    this.message.info('LogStash 日志查看功能正在开发中');
  }

  copyCommand(type: string): void {
    let command = '';
    
    switch (type) {
      case 'install':
        command = 'helm install --namespace polardbx-logcollector polardbx-logcollector https://github.com/polardb/polardbx-operator/releases/download/v1.7.0/polardbx-logcollector-1.6.2.tgz';
        break;
      case 'check':
        command = 'kubectl get pods --namespace polardbx-logcollector';
        break;
      case 'logs':
        command = 'kubectl logs -f deployment/logstash -n polardbx-logcollector';
        break;
    }
    
    if (command) {
      navigator.clipboard.writeText(command).then(() => {
        this.message.success('命令已复制到剪贴板');
      });
    }
  }
}