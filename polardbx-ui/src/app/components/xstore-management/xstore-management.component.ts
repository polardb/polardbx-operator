import { Component, OnInit, inject } from '@angular/core';
import { ReactiveFormsModule, FormBuilder, FormGroup, Validators } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzTabsModule } from 'ng-zorro-antd/tabs';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzInputNumberModule } from 'ng-zorro-antd/input-number';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzModalService, NzModalModule } from 'ng-zorro-antd/modal';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzPopconfirmModule } from 'ng-zorro-antd/popconfirm';
import { ApiService } from '../../services/api.service';
import { LoadingService, LoadingKeys } from '../../services/loading.service';
import { XStore } from '../../models/xstore.model';

@Component({
  selector: 'app-xstore-management',
  standalone: true,
  imports: [
    CommonModule,
    NzCardModule,
    NzButtonModule,
    NzIconModule,
    NzTableModule,
    NzTabsModule,
    NzSpinModule,
    NzTagModule,
    NzFormModule,
    NzInputModule,
    NzInputNumberModule,
    NzGridModule,
    NzEmptyModule,
    NzModalModule,
    NzToolTipModule,
    NzDividerModule,
    NzPopconfirmModule,
    ReactiveFormsModule
  ],
  template: `
    <div class="xstore-management">
      <!-- 页面头部 -->
      <div class="page-header">
        <div class="header-content">
          <h1 class="page-title">
            <i nz-icon nzType="database" class="page-icon"></i>
            存储节点管理
          </h1>
          <p class="page-description">管理 XStore 存储节点与拓扑，包括节点创建、状态监控和运维操作</p>
        </div>
      </div>

      <div class="page-content">

        <nz-tabset [(nzSelectedIndex)]="selectedTab" class="main-tabs" [nzTabPosition]="'top'">

          <nz-tab nzTitle="存储节点列表">
              <ng-template nz-tab>
                <div class="tab-content">
                  <nz-card 
                    class="list-card" 
                    nzTitle="存储节点配置" 
                    [nzExtra]="listExtra"
                    [nzLoading]="loadingService.isLoading(loadingKeys.XSTORE_LIST)">
                    <ng-template #listExtra>
                      <div class="extra-actions">
                        <button nz-button nzType="default" nzSize="small" (click)="refreshXStores()">
                          <i nz-icon nzType="reload"></i>
                          刷新
                        </button>
                        <button nz-button nzType="primary" nzSize="small" (click)="createNew()">
                          <i nz-icon nzType="plus"></i>
                          新建节点
                        </button>
                      </div>
                    </ng-template>
                    <div class="list-content">

                      <nz-table 
                        #xTable 
                        [nzData]="xstores" 
                        [nzLoading]="loadingService.isLoading(loadingKeys.XSTORE_LIST)"
                        [nzPageSize]="10"
                        [nzShowPagination]="xstores.length > 10"
                        [nzScroll]="{ x: '1200px' }">
                        <thead>
                          <tr>
                            <th nzWidth="160px">节点名称</th>
                            <th nzWidth="120px">命名空间</th>
                            <th nzWidth="100px">状态</th>
                            <th nzWidth="100px">副本数</th>
                            <th nzWidth="180px">创建时间</th>
                            <th nzWidth="120px" nzAlign="center">操作</th>
                          </tr>
                        </thead>
                        <tbody>
                          <tr *ngFor="let x of xTable.data">
                            <td>
                              <div class="xstore-name">
                                <i nz-icon 
                                   [nzType]="getStatusIcon(x.status?.phase)"
                                   [style.color]="getStatusColor(x.status?.phase)">
                                </i>
                                <span style="margin-left: 8px;">{{ x.metadata.name }}</span>
                              </div>
                            </td>
                            <td>
                              <nz-tag nzColor="blue">{{ x.metadata.namespace }}</nz-tag>
                            </td>
                            <td>
                              <nz-tag [nzColor]="getStatusColor(x.status?.phase)">
                                {{ getStatusLabel(x.status?.phase) }}
                              </nz-tag>
                            </td>
                            <td>{{ getReplicaDisplay(x) }}</td>
                            <td>{{ formatDate(x.metadata.creationTimestamp) }}</td>
                            <td nzAlign="center">
                              <button nz-button nzType="link" nzSize="small" (click)="viewDetails(x)">
                                <i nz-icon nzType="eye"></i>
                                查看
                              </button>
                              <nz-divider nzType="vertical"></nz-divider>
                              <button nz-button nzType="link" nzSize="small" nz-popconfirm 
                                      nzPopconfirmTitle="确定删除此存储节点？" 
                                      (nzOnConfirm)="deleteXStore(x)">
                                <i nz-icon nzType="delete"></i>
                                删除
                              </button>
                            </td>
                          </tr>
                        </tbody>
                      </nz-table>
                      
                      <nz-empty *ngIf="xstores.length === 0 && !loadingService.isLoading(loadingKeys.XSTORE_LIST)"
                               nzNotFoundImage="simple"
                               nzNotFoundDescription="暂无存储节点">
                        <div nz-empty-footer>
                          <button nz-button nzType="primary" (click)="createNew()">
                            <i nz-icon nzType="plus"></i>
                            创建第一个存储节点
                          </button>
                        </div>
                      </nz-empty>
                    </div>
                  </nz-card>
                </div>
              </ng-template>
          </nz-tab>

          <nz-tab nzTitle="创建存储节点">
            <div class="create-tab-content">
              <nz-card nzTitle="创建存储节点" class="create-form-card">
                <form nz-form [formGroup]="createForm" (ngSubmit)="submit()" nzLayout="vertical">
                  <nz-row [nzGutter]="[16, 16]">
                    <nz-col [nzSpan]="12">
                      <nz-form-item>
                        <nz-form-label nzRequired>名称</nz-form-label>
                        <nz-form-control nzErrorTip="请输入符合规范的名称">
                          <input nz-input formControlName="name" placeholder="xstore-example">
                        </nz-form-control>
                      </nz-form-item>
                    </nz-col>
                    <nz-col [nzSpan]="12">
                      <nz-form-item>
                        <nz-form-label nzRequired>命名空间</nz-form-label>
                        <nz-form-control nzErrorTip="请输入命名空间">
                          <input nz-input formControlName="namespace" placeholder="default">
                        </nz-form-control>
                      </nz-form-item>
                    </nz-col>
                  </nz-row>

                  <nz-row [nzGutter]="[16, 16]">
                    <nz-col [nzSpan]="12">
                      <nz-form-item>
                        <nz-form-label>引擎</nz-form-label>
                        <nz-form-control>
                          <input nz-input formControlName="engine" placeholder="galaxy">
                        </nz-form-control>
                      </nz-form-item>
                    </nz-col>
                    <nz-col [nzSpan]="12">
                      <nz-form-item>
                        <nz-form-label nzRequired>节点数</nz-form-label>
                        <nz-form-control nzErrorTip="节点数必须大于0">
                          <nz-input-number formControlName="nodeCount" [nzMin]="1" [nzMax]="50" nzPlaceHolder="2" style="width: 100%">
                          </nz-input-number>
                        </nz-form-control>
                      </nz-form-item>
                    </nz-col>
                  </nz-row>

                  <nz-row [nzGutter]="[16, 16]">
                    <nz-col [nzSpan]="12">
                      <nz-form-item>
                        <nz-form-label>CPU (limits)</nz-form-label>
                        <nz-form-control>
                          <input nz-input formControlName="cpu" placeholder="2">
                        </nz-form-control>
                      </nz-form-item>
                    </nz-col>
                    <nz-col [nzSpan]="12">
                      <nz-form-item>
                        <nz-form-label>内存 (limits)</nz-form-label>
                        <nz-form-control>
                          <input nz-input formControlName="memory" placeholder="4Gi">
                        </nz-form-control>
                      </nz-form-item>
                    </nz-col>
                  </nz-row>

                  <nz-row [nzGutter]="[16, 16]">
                    <nz-col [nzSpan]="12">
                      <nz-form-item>
                        <nz-form-label>数据卷大小</nz-form-label>
                        <nz-form-control>
                          <input nz-input formControlName="diskQuota" placeholder="100Gi">
                        </nz-form-control>
                      </nz-form-item>
                    </nz-col>
                    <nz-col [nzSpan]="12">
                      <nz-form-item>
                        <nz-form-label>存储类</nz-form-label>
                        <nz-form-control>
                          <input nz-input formControlName="storageClass" placeholder="(可选)">
                        </nz-form-control>
                      </nz-form-item>
                    </nz-col>
                  </nz-row>

                  <nz-row [nzGutter]="[16, 16]">
                    <nz-col [nzSpan]="12">
                      <nz-form-item>
                        <nz-form-label>CN 副本数</nz-form-label>
                        <nz-form-control>
                          <nz-input-number formControlName="cnReplicas" [nzMin]="0" [nzMax]="20" nzPlaceHolder="0" style="width: 100%">
                          </nz-input-number>
                        </nz-form-control>
                      </nz-form-item>
                    </nz-col>
                    <nz-col [nzSpan]="12">
                      <nz-form-item>
                        <nz-form-label>服务类型</nz-form-label>
                        <nz-form-control>
                          <input nz-input formControlName="serviceType" placeholder="NodePort">
                        </nz-form-control>
                      </nz-form-item>
                    </nz-col>
                  </nz-row>

                  <nz-form-item class="form-actions">
                    <button nz-button nzType="primary" nzSize="large" type="submit" [disabled]="createForm.invalid">
                      <i nz-icon nzType="plus"></i>
                      创建存储节点
                    </button>
                    <button nz-button nzType="default" nzSize="large" type="button" (click)="selectedTab = 0">
                      <i nz-icon nzType="arrow-left"></i>
                      返回列表
                    </button>
                  </nz-form-item>
                </form>
              </nz-card>
            </div>
          </nz-tab>
        </nz-tabset>
      </div>
    </div>
  `,
  styles: [`
    .xstore-management {
      padding: 16px;
      background: #f5f5f5;
      min-height: 100vh;
    }

    .page-header {
      margin-bottom: 24px;
    }
    
    .page-description {
      color: rgba(0, 0, 0, 0.6);
      font-size: 14px;
      margin: 4px 0 0 36px;
    }
    
    .page-content {
      max-width: 1120px;
      margin: 0 auto;
    }
    
    .main-tabs {
      background: white;
      border-radius: 8px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.06);
      overflow: hidden;
    }
    
    .tab-content {
      padding: 0;
    }
    
    .list-card {
      border: none;
      border-radius: 0;
      box-shadow: none;
    }
    
    .extra-actions {
      display: flex;
      gap: 8px;
      align-items: center;
    }
    
    .list-content {
      margin-top: 0;
    }
    
    .xstore-name {
      display: flex;
      align-items: center;
    }

    .header-content {
      max-width: 1120px;
      margin: 0 auto;
    }

    .page-title {
      font-size: 20px !important;
      font-weight: 600 !important;
      color: rgba(0, 0, 0, 0.88) !important;
      margin: 0 !important;
      display: flex;
      align-items: center;
      gap: 12px;
    }

    .page-icon {
      font-size: 24px !important;
      color: #1890ff !important;
    }

    .main-card {
      border-radius: 8px;
      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
    }

    .page-header-title {
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .subtitle { color: rgba(0, 0, 0, 0.65); font-size: 14px; }

    .main-tabs { background: #fff; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.06); border: 1px solid #e0e0e0; }
    .tab-content { padding: 16px; }

    .page-header {
      margin-bottom: 16px;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }

    .header-actions {
      display: flex;
      gap: 8px;
    }

    .xstore-table {
      background: white;
      border-radius: 6px;
    }

    .xstore-table th {
      background: #fafafa;
      color: rgba(0, 0, 0, 0.85);
      font-weight: 600;
    }

    .xstore-table td {
      padding: 12px 16px;
    }

    .replica-count {
      font-family: 'Monaco', 'Menlo', monospace;
      font-size: 13px;
      color: rgba(0, 0, 0, 0.75);
    }

    .created-time {
      font-size: 13px;
      color: rgba(0, 0, 0, 0.65);
    }

    .action-buttons {
      display: flex;
      gap: 4px;
    }

    .create-tab-content {
      padding: 24px 0;
      display: flex;
      justify-content: center;
    }

    .create-form-card {
      width: 100%;
      max-width: 800px;
      border-radius: 8px;
      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06);
    }

    .form-actions {
      margin-top: 24px;
      text-align: center;
    }

    .form-actions button {
      margin: 0 8px;
      min-width: 120px;
    }

    /* 响应式设计 */
    @media (max-width: 768px) {
      .xstore-management {
        padding: 8px;
      }

      .page-header {
        flex-direction: column;
        gap: 12px;
        align-items: stretch;
      }

      .header-actions {
        justify-content: center;
      }

      .create-form-card {
        margin: 0 8px;
      }

      .action-buttons {
        flex-direction: column;
        gap: 8px;
      }

      nz-col[nzSpan="12"] {
        flex: 0 0 100% !important;
        max-width: 100% !important;
      }
    }

    @media (min-width: 1200px) {
      .xstore-management {
        padding: 24px;
      }

      .tab-content {
        padding: 32px 0;
      }

      .create-tab-content {
        padding: 32px 0;
      }
    }

    /* ng-zorro特定样式优化 */
    nz-table {
      border-radius: 6px;
      overflow: hidden;
    }

    nz-tag {
      border-radius: 4px;
      font-weight: 500;
    }

    nz-empty {
      padding: 60px 20px;
    }

    nz-spin {
      min-height: 200px;
    }

    .ant-form-item-label > label {
      color: rgba(0, 0, 0, 0.85);
      font-weight: 500;
    }

    .ant-input-number {
      width: 100%;
    }

    .ant-card-head-title {
      display: flex;
      align-items: center;
    }

    .ant-tabs-card > .ant-tabs-content {
      margin-top: 0;
    }

    .ant-tabs-card > .ant-tabs-content > .ant-tabs-tabpane {
      background: transparent;
    }
  `]
})
export class XStoreManagementComponent implements OnInit {
  selectedTab = 0;
  loadingKeys = LoadingKeys;
  xstores: XStore[] = [];
  createForm!: FormGroup;

  private message = inject(NzMessageService);
  private modal = inject(NzModalService);
  public loadingService = inject(LoadingService);
  private apiService = inject(ApiService);
  private fb = inject(FormBuilder);

  ngOnInit(): void {
    this.createForm = this.fb.group({
      name: ['', [Validators.required, Validators.pattern(/^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/)]],
      namespace: ['default', Validators.required],
      engine: ['galaxy'],
      nodeCount: [2, [Validators.required, Validators.min(1)]],
      cpu: ['2'],
      memory: ['4Gi'],
      diskQuota: ['100Gi'],
      storageClass: [''],
      cnReplicas: [0, [Validators.min(0)]],
      serviceType: ['NodePort']
    });
    this.loadXStores();
  }

  loadXStores(): void {
    this.apiService.getXStores().subscribe({
      next: (xstores) => {
        this.xstores = xstores;
      },
      error: (err) => {
        this.message.error(`加载存储节点失败: ${err.error?.message || err.message}`);
        this.xstores = [];
      }
    });
  }

  refreshXStores(): void {
    this.loadXStores();
    this.message.success('存储节点列表已刷新');
  }

  createNew(): void {
    this.selectedTab = 1;
  }

  viewXStoreDetails(xstore: XStore): void {
    this.message.info(`查看存储节点详情: ${xstore.metadata.name}`);
  }

  deleteXStore(xstore: XStore): void {
    this.modal.confirm({
      nzTitle: '确认删除',
      nzContent: `确定删除存储节点 "${xstore.metadata.name}" 吗？此操作不可恢复。`,
      nzOkText: '确定删除',
      nzOkType: 'primary',
      nzOkDanger: true,
      nzCancelText: '取消',
      nzOnOk: () => {
        this.apiService.deleteXStore(xstore.metadata.namespace || 'default', xstore.metadata.name).subscribe({
          next: () => {
            this.message.success('删除成功！');
            this.loadXStores();
          },
          error: (err) => {
            this.message.error(`删除失败: ${err.error?.message || err.message}`);
          }
        });
      }
    });
  }

  submit(): void {
    if (this.createForm.invalid) return;
    const v = this.createForm.value;
    const req = {
      name: v.name,
      namespace: v.namespace,
      engine: v.engine,
      nodeCount: Number(v.nodeCount),
      resources: { limits: { cpu: v.cpu, memory: v.memory } },
      storage: { size: v.diskQuota, storageClass: v.storageClass },
      cnReplicas: Number(v.cnReplicas),
      serviceType: v.serviceType
    } as any;
    this.apiService.createXStore(v.namespace, req).subscribe({
      next: () => {
        this.message.success('创建成功！');
        this.selectedTab = 0;
        this.loadXStores();
      },
      error: (err) => {
        this.message.error(`创建失败: ${err.error?.message || err.message}`);
      }
    });
  }

  getStatusColor(status?: string): string {
    switch (status?.toLowerCase()) {
      case 'running':
        return 'green';
      case 'ready':
        return 'blue';
      case 'failed':
      case 'error':
        return 'red';
      case 'pending':
        return 'orange';
      default:
        return 'default';
    }
  }

  /**
   * 获取XStore副本数显示文本
   * 优先显示 ready/total，如果没有状态信息则显示规格中的节点数
   */
  getReplicaDisplay(xstore: XStore): string {
    // 优先使用运行时状态信息
    if (xstore.status?.replicaStatus) {
      const ready = xstore.status.replicaStatus.ready ?? 0;
      const total = xstore.status.replicaStatus.total ?? 0;
      return `${ready}/${total}`;
    }
    
    // 如果没有状态信息，使用规格中的节点总数
    if (xstore.spec?.topology?.nodeCount) {
      return `${xstore.spec.topology.nodeCount}`;
    }
    
    // 如果有NodeSets配置，计算总副本数
    if (xstore.spec?.topology?.nodeSets && xstore.spec.topology.nodeSets.length > 0) {
      const totalReplicas = xstore.spec.topology.nodeSets.reduce((sum: number, nodeSet: any) => {
        return sum + (nodeSet?.replicas || 0);
      }, 0 as number);
      return `${totalReplicas}`;
    }
    
    // 都没有则显示未知
    return 'N/A';
  }

  getStatusIcon(phase?: string): string {
    switch (phase) {
      case 'Running': return 'check-circle';
      case 'Creating': return 'loading';
      case 'Failed': return 'close-circle';
      case 'Deleting': return 'delete';
      default: return 'question-circle';
    }
  }

  getStatusLabel(phase?: string): string {
    switch (phase) {
      case 'Running': return '运行中';
      case 'Creating': return '创建中';
      case 'Failed': return '失败';
      case 'Deleting': return '删除中';
      default: return '未知';
    }
  }

  formatDate(dateStr?: string): string {
    if (!dateStr) return '-';
    return new Date(dateStr).toLocaleString('zh-CN');
  }

  viewDetails(xstore: XStore): void {
    // 实现查看详情逻辑
    console.log('View details for:', xstore);
    this.message.info('功能开发中...');
  }
}