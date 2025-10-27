import { Component, OnInit, OnDestroy, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzProgressModule } from 'ng-zorro-antd/progress';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzPageHeaderModule } from 'ng-zorro-antd/page-header';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { ApiService } from '../../services/api.service';
import { interval, Subscription } from 'rxjs';
import { ActivatedRoute } from '@angular/router';

interface DiagnosisReport {
  id: string;
  namespace: string;
  cluster?: string;
  createdAt?: string;
  status?: string; // running/succeeded/failed
}

@Component({
  selector: 'app-diagnostics-management',
  standalone: true,
  imports: [CommonModule, FormsModule, NzCardModule, NzButtonModule, NzIconModule, NzTableModule, NzProgressModule, NzTagModule, NzFormModule, NzInputModule, NzSpinModule, NzPageHeaderModule, NzGridModule],
  template: `
    <div class="diagnostics-page">
      <nz-page-header [nzGhost]="false" nzTitle="诊断与排障" nzSubtitle="触发诊断、查看历史报告并下载">
        <nz-page-header-extra>
          <i nz-icon nzType="bug" class="page-icon"></i>
        </nz-page-header-extra>
      </nz-page-header>

      <div class="content-grid">
        <nz-card class="action-card" nzTitle="启动诊断">
          <ng-template #title>
            <i nz-icon nzType="play-circle"></i>
            <span>启动诊断</span>
          </ng-template>
          
          <form nz-form nzLayout="vertical" class="diagnostic-form">
            <nz-form-item>
              <nz-form-label>命名空间</nz-form-label>
              <nz-form-control>
                <input nz-input [(ngModel)]="namespace" name="namespace" placeholder="default" />
              </nz-form-control>
            </nz-form-item>
            
            <nz-form-item>
              <nz-form-label>集群名</nz-form-label>
              <nz-form-control>
                <input nz-input [(ngModel)]="cluster" name="cluster" placeholder="pxc-1" />
              </nz-form-control>
            </nz-form-item>
            
            <nz-form-item>
              <nz-form-control>
                <div class="form-actions">
                  <button nz-button nzType="primary" (click)="start()" [nzLoading]="starting" [disabled]="!cluster">
                    <i nz-icon nzType="play-circle"></i>
                    {{ starting ? '启动中...' : '启动诊断' }}
                  </button>
                  <button nz-button nzType="default" (click)="refresh()">
                    <i nz-icon nzType="reload"></i>
                    刷新列表
                  </button>
                </div>
              </nz-form-control>
            </nz-form-item>
          </form>
          
          <nz-progress *ngIf="starting" [nzPercent]="0" nzStatus="active"></nz-progress>
        </nz-card>

        <nz-card class="list-card">
          <ng-template #title>
            <i nz-icon nzType="history"></i>
            <span>历史报告（{{ reports.length }}）</span>
          </ng-template>
          
          <nz-table [nzData]="reports" [nzShowPagination]="false" class="reports-table">
            <thead>
              <tr>
                <th>报告ID</th>
                <th>命名空间</th>
                <th>集群</th>
                <th>时间</th>
                <th>状态</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              <tr *ngFor="let r of reports">
                <td>{{ r.id }}</td>
                <td>{{ r.namespace }}</td>
                <td>{{ r.cluster || '-' }}</td>
                <td>{{ r.createdAt ? (r.createdAt | date:'yyyy-MM-dd HH:mm:ss') : '-' }}</td>
                <td>
                  <nz-tag [nzColor]="statusColor(r.status)">{{ r.status || '-' }}</nz-tag>
                </td>
                <td>
                  <button nz-button nzType="text" nzSize="small" (click)="download(r)">
                    <i nz-icon nzType="download"></i>
                  </button>
                </td>
              </tr>
            </tbody>
          </nz-table>
        </nz-card>
      </div>
    </div>
  `,
  styles: [`
    .diagnostics-page { 
      padding: 16px 24px; 
      background: #f5f5f5; 
      min-height: 100vh; 
    }
    
    .page-icon { 
      font-size: 16px; 
      color: #1890ff; 
    }
    
    .content-grid { 
      display: grid; 
      grid-template-columns: 400px 1fr; 
      gap: 24px; 
      margin-top: 16px; 
    }
    
    .action-card, .list-card {
      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06);
      border-radius: 8px;
    }
    
    .diagnostic-form {
      margin-top: 16px;
    }
    
    .form-actions { 
      display: flex; 
      gap: 12px; 
      align-items: center; 
      margin-top: 8px;
    }
    
    .reports-table { 
      width: 100%; 
    }
    
    .reports-table th {
      background: #fafafa;
      font-weight: 600;
      color: #262626;
    }
    
    .reports-table td {
      border-bottom: 1px solid #f0f0f0;
    }
    
    @media (max-width: 1200px) { 
      .content-grid { 
        grid-template-columns: 1fr; 
      } 
    }
  `]
})
export class DiagnosticsManagementComponent implements OnInit, OnDestroy {
  namespace = 'default';
  cluster = '';
  reports: DiagnosisReport[] = [];
  displayedColumns = ['id', 'namespace', 'cluster', 'createdAt', 'status', 'actions'];
  starting = false;
  private statusPoll?: Subscription;
  private message = inject(NzMessageService);
  private api = inject(ApiService);
  private route = inject(ActivatedRoute);

  ngOnInit(): void {
    // 解析 query 参数以便外部跳转自动填充并触发
    const qp = this.route.snapshot.queryParamMap;
    const ns = qp.get('namespace');
    const cl = qp.get('cluster');
    const auto = qp.get('autoStart');
    if (ns) this.namespace = ns;
    if (cl) this.cluster = cl;
    this.refresh();
    if (cl && (auto === '1' || auto === 'true')) {
      setTimeout(() => this.start(), 0);
    }
  }

  ngOnDestroy(): void {
    this.stopPolling();
  }

  statusColor(s?: string): string { 
    return (s === 'running') ? 'processing' : (s === 'succeeded' ? 'success' : (s === 'failed' ? 'error' : 'default')); 
  }

  refresh(): void {
    this.api.listDiagnosisReports().subscribe({
      next: (items) => { this.reports = (items || []) as any[]; },
      error: () => { this.reports = []; }
    });
  }

  start(): void {
    if (!this.cluster) return;
    this.starting = true;
    this.api.startDiagnosis(this.namespace, this.cluster).subscribe({
      next: (res: any) => {
        this.message.success('诊断已触发');
        const id = res?.id;
        this.refresh();
        if (id) this.pollStatus(id);
        this.starting = false;
      },
      error: () => { this.message.error('诊断触发失败'); this.starting = false; }
    });
  }

  pollStatus(id: string): void {
    this.stopPolling();
    this.statusPoll = interval(5000).subscribe(() => {
      this.api.getDiagnosisStatus(this.namespace, id).subscribe({
        next: (s: any) => {
          const status = s?.status || s?.phase;
          // 更新本地列表项状态
          const idx = this.reports.findIndex(r => r.id === id);
          if (idx >= 0) {
            const copy = [...this.reports];
            (copy[idx] as any).status = status;
            this.reports = copy;
          }
          if (status === 'succeeded' || status === 'failed' || status === 'completed') this.stopPolling();
        }
      });
    });
  }

  stopPolling(): void { if (this.statusPoll) { this.statusPoll.unsubscribe(); this.statusPoll = undefined; } }

  download(r: DiagnosisReport): void {
    if (!r?.id) return;
    this.api.downloadDiagnosisReport(r.namespace || 'default', r.id).subscribe({
      next: (resp) => {
        const url = (resp as any)?.url;
        if (url) window.open(url, '_blank');
        else this.message.warning('下载链接不可用');
      },
      error: () => this.message.error('下载失败')
    });
  }
}