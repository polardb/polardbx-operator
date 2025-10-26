import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { ApiService } from '../../services/api.service';

@Component({
  selector: 'app-monitoring-health',
  standalone: true,
  imports: [CommonModule, NzCardModule, NzTableModule, NzButtonModule, NzIconModule, NzSpinModule, NzTagModule],
  template: `
    <div class="health">
      <div class="page-header">
        <div class="header-content">
          <h1 class="page-title">
            <i nz-icon nzType="heart" class="page-icon"></i>
            监控健康检查
          </h1>
          <p class="page-description">检测 Prometheus / Grafana / Alertmanager 组件状态</p>
        </div>
      </div>

      <div class="page-content">
        <nz-card class="control-panel" nzTitle="检查操作">
          <div class="toolbar">
            <button nz-button nzType="primary" (click)="reload()" [nzLoading]="loading">
              <i nz-icon nzType="reload"></i>
              刷新状态
            </button>
          </div>
        </nz-card>

        <div *ngIf="loading" class="loading-container">
          <nz-spin nzSize="large">
            <div class="loading-tip">正在检查组件状态...</div>
          </nz-spin>
        </div>

        <nz-card class="table-card" nzTitle="组件状态" *ngIf="!loading">
          <nz-table #healthTable [nzData]="rows" nzSize="middle" [nzShowPagination]="false">
            <thead>
              <tr>
                <th>组件</th>
                <th>状态</th>
                <th>详情</th>
              </tr>
            </thead>
            <tbody>
              <tr *ngFor="let row of healthTable.data">
                <td>
                  <strong>{{ row.component }}</strong>
                </td>
                <td>
                  <nz-tag [nzColor]="row.ready ? 'success' : 'error'">
                    <i nz-icon [nzType]="row.ready ? 'check-circle' : 'close-circle'"></i>
                    {{ row.ready ? '就绪' : '未就绪' }}
                  </nz-tag>
                </td>
                <td>
                  <span class="detail-text">{{ row.detail }}</span>
                </td>
              </tr>
            </tbody>
          </nz-table>
        </nz-card>
      </div>
    </div>
  `,
  styles: [`
    .health {
      padding: 16px;
      background: #f5f5f5;
      min-height: 100vh;
    }
    
    .page-header {
      margin-bottom: 16px;
    }
    
    .header-content {
      max-width: 1120px;
      margin: 0 auto;
    }
    
    .page-title {
      color: rgba(0, 0, 0, 0.87);
      font-size: 18px;
      font-weight: 500;
      margin: 0 0 4px 0;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    
    .page-icon {
      font-size: 20px;
      color: #1890ff;
    }
    
    .page-description {
      color: rgba(0, 0, 0, 0.6);
      font-size: 14px;
      margin: 0;
      line-height: 1.5;
    }
    
    .page-content {
      max-width: 1120px;
      margin: 0 auto;
      display: flex;
      flex-direction: column;
      gap: 16px;
    }
    
    .control-panel, .table-card {
      background: #fff;
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border: 1px solid #e0e0e0;
    }
    
    .toolbar {
      display: flex;
      align-items: center;
      gap: 16px;
    }
    
    .loading-container {
      display: flex;
      justify-content: center;
      align-items: center;
      padding: 80px 0;
    }

    .loading-tip {
      margin-top: 12px;
      text-align: center;
      color: rgba(0,0,0,0.65);
      letter-spacing: 0.5px;
    }
    
    .detail-text {
      color: rgba(0, 0, 0, 0.6);
      font-size: 13px;
    }
    
    /* 响应式设计 */
    @media (max-width: 768px) {
      .health {
        padding: 16px;
      }
      
      .toolbar {
        flex-direction: column;
        align-items: flex-start;
        gap: 8px;
      }
    }
  `]
})
export class MonitoringHealthComponent implements OnInit {
  loading = false;
  rows: { component: string; ready: boolean; detail?: string }[] = [];
  cols = ['component', 'ready', 'detail'];
  private readonly monitoringNamespaceFallback = 'polardbx-monitor';

  constructor(private api: ApiService) {}

  ngOnInit(): void { this.reload(); }

  reload() {
    this.loading = true;
    this.rows = [];
    
    console.log('[监控健康] 开始重新加载状态');
    
    this.api.getSystemContext().subscribe({
      next: (ctx) => {
        const rawNamespace = (ctx?.defaultNamespace || '').trim();
        const candidateNamespace = this.normalizeNamespace(rawNamespace);
        // 始终显式传递 polardbx-monitor，避免后端使用 kubeconfig 默认命名空间
        const requestNamespace = this.monitoringNamespaceFallback;

        console.log('[监控健康] 系统上下文:', {
          rawNamespace,
          candidateNamespace,
          requestNamespace,
          fallback: this.monitoringNamespaceFallback
        });

        this.api.getMonitoringStatus(requestNamespace).subscribe({
          next: (status: any) => {
            console.log('[监控健康] 监控状态响应:', status);
            
            const namespace = status?.namespace || candidateNamespace || this.monitoringNamespaceFallback;
            const namespaceExists = status?.namespaceExists !== false;
            const namespaceError = status?.namespaceError ? `：${status.namespaceError}` : '';
            const prereq = status?.prerequisites;
            const comp = status?.components || {};

            console.log('[监控健康] 解析后的状态:', {
              namespace,
              namespaceExists,
              components: Object.keys(comp),
              prerequisites: prereq ? Object.keys(prereq) : []
            });

            this.rows = [
              {
                component: '命名空间',
                ready: namespaceExists,
                detail: namespaceExists
                  ? `正在检查 ${namespace}`
                  : `未找到命名空间 ${namespace}${namespaceError}`
              },
              this.formatComponentRow('Prometheus', comp?.prometheus, {
                missingMessage: '未发现 Prometheus StatefulSet',
                namespace,
                namespaceExists
              }),
              this.formatComponentRow('Grafana', comp?.grafana, {
                missingMessage: '未发现 Grafana Deployment',
                namespace,
                namespaceExists
              }),
              {
                component: 'Alertmanager',
                ready: !!comp?.alertmanager?.configured,
                detail: comp?.alertmanager?.configured ? 'Service 已配置' : '未配置 Alertmanager Service'
              }
            ].filter((row): row is { component: string; ready: boolean; detail?: string } => !!row);

            if (prereq?.crds) {
              const monitorCRD = prereq.crds.polardbxMonitor;
              if (monitorCRD) {
                this.rows.push({
                  component: 'PolarDBXMonitor CRD',
                  ready: !!monitorCRD.exists && !!monitorCRD.established,
                  detail: monitorCRD.exists ? 'CRD 已建立' : '未发现 CRD polardbxmonitors.polardbx.aliyun.com'
                });
              }
              const serviceMonitorCRD = prereq.crds.serviceMonitor;
              if (serviceMonitorCRD) {
                this.rows.push({
                  component: 'ServiceMonitor CRD',
                  ready: !!serviceMonitorCRD.exists && !!serviceMonitorCRD.established,
                  detail: serviceMonitorCRD.exists ? 'CRD 已建立' : '未发现 CRD servicemonitors.monitoring.coreos.com'
                });
              }
            }

            console.log('[监控健康] 最终行数据:', this.rows);
          },
          error: (err) => {
            console.error('[监控健康] 获取监控状态失败:', err);
            this.rows = [];
            this.loading = false;
          },
          complete: () => { this.loading = false; }
        });
      },
      error: (err) => {
        console.error('[监控健康] 获取系统上下文失败:', err);
        this.rows = [];
        this.loading = false;
      }
    });
  }

  private normalizeNamespace(namespace: string): string {
    if (!namespace) {
      return this.monitoringNamespaceFallback;
    }
    const lowered = namespace.toLowerCase();
    if (['default', 'kube-system', 'polardbx-operator-system'].includes(lowered)) {
      return this.monitoringNamespaceFallback;
    }
    return namespace;
  }

  private formatComponentRow(
    component: string,
    target: any,
    options: { missingMessage: string; namespace: string; namespaceExists: boolean }
  ): { component: string; ready: boolean; detail?: string } | null {
    console.log(`[监控健康] formatComponentRow - ${component}:`, {
      target,
      namespaceExists: options.namespaceExists,
      readyReplicas: target?.readyReplicas,
      service: target?.service,
      ready: target?.ready
    });

    if (!options.namespaceExists) {
      return {
        component,
        ready: false,
        detail: `无法检查，命名空间 ${options.namespace} 不存在`
      };
    }

    if (!target || (target.readyReplicas == null && !target.service)) {
      console.warn(`[监控健康] ${component} 检测失败 - 条件不满足:`, {
        targetExists: !!target,
        readyReplicasNull: target?.readyReplicas == null,
        noService: !target?.service
      });
      return { component, ready: false, detail: options.missingMessage };
    }

    const ready = !!target.ready;
    const readyReplicas = target.readyReplicas ?? '-';
    const replicas = target.replicas ?? '-';
    const access = target.accessUrl || (target.service ? '已发现 Service' : '');
    const segments = [`readyReplicas=${readyReplicas} / replicas=${replicas}`];
    if (access) {
      segments.push(typeof access === 'string' ? access : String(access));
    }
    
    console.log(`[监控健康] ${component} 格式化结果:`, {
      ready,
      readyReplicas,
      replicas,
      access
    });

    return {
      component,
      ready,
      detail: segments.join(' | ')
    };
  }}
