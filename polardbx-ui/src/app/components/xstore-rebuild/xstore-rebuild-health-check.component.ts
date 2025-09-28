import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzProgressModule } from 'ng-zorro-antd/progress';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzFormModule } from 'ng-zorro-antd/form';
import { ApiService } from '../../services/api.service';

@Component({
  selector: 'app-xstore-rebuild-health-check',
  standalone: true,
  imports: [CommonModule, FormsModule, NzCardModule, NzButtonModule, NzIconModule, NzTagModule, NzProgressModule, NzInputModule, NzFormModule],
  template: `
    <div class="health-check-page">
      <div class="page-header">
        <div class="header-content">
          <h1 class="page-title">
            <i nz-icon nzType="safety-certificate" class="page-icon"></i>
            备库重搭健康检查
          </h1>
          <p class="subtitle">对目标集群/节点进行健康检查，评估重搭可行性</p>
        </div>
      </div>
      
      <nz-card>
        <div class="card-content">
          <div class="form-section">
            <nz-form-item>
              <nz-form-label>命名空间</nz-form-label>
              <nz-form-control>
                <input nz-input [(ngModel)]="namespace" placeholder="default"/>
              </nz-form-control>
            </nz-form-item>
            
            <nz-form-item>
              <nz-form-label>目标 XStore</nz-form-label>
              <nz-form-control>
                <input nz-input [(ngModel)]="xstore" placeholder="cluster-dn-0"/>
              </nz-form-control>
            </nz-form-item>
            
            <div class="actions">
              <button nz-button nzType="primary" (click)="run()" [nzLoading]="loading">
                <i nz-icon nzType="play-circle"></i> 执行检查
              </button>
            </div>
          </div>

          <nz-progress *ngIf="loading" [nzPercent]="50" [nzShowInfo]="false" style="margin:12px 0"></nz-progress>

          <div *ngIf="!loading && checked" class="results-section">
            <div class="result-row">
              <span class="k">XStore 对象</span>
              <span class="v">
                <nz-tag [nzColor]="xstoreOk ? 'success' : 'error'">{{ xstoreOk ? '存在' : '不存在' }}</nz-tag>
              </span>
            </div>
            <div class="result-row" *ngIf="xstoreOk">
              <span class="k">副本情况</span>
              <span class="v">{{ readyPods }}/{{ totalPods }} Ready</span>
            </div>
            <div class="result-row" *ngIf="xstoreOk">
              <span class="k">阶段</span>
              <span class="v">
                <nz-tag [nzColor]="phaseOk ? 'success' : 'warning'">{{ phase || '未知' }}</nz-tag>
              </span>
            </div>
            <div class="result-row">
              <span class="k">可进行重搭</span>
              <span class="v">
                <nz-tag [nzColor]="canRebuild ? 'success' : 'error'">{{ canRebuild ? '是' : '否' }}</nz-tag>
              </span>
            </div>
            <div class="result-row" *ngIf="message">
              <span class="k">提示</span>
              <span class="v message-text">{{ message }}</span>
            </div>
          </div>
        </div>
      </nz-card>
    </div>
  `,
  styles: [`
    .health-check-page {
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

    .subtitle {
      color: rgba(0, 0, 0, 0.65);
      font-size: 14px;
      margin: 4px 0 0 36px;
    }

    .card-content {
      padding: 24px;
    }

    .form-section {
      margin-bottom: 24px;
    }

    nz-form-item {
      margin-bottom: 16px;
    }

    .actions {
      margin-top: 16px;
    }

    .results-section {
      margin-top: 24px;
      padding-top: 24px;
      border-top: 1px solid #f0f0f0;
    }

    .result-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 8px 0;
      border-bottom: 1px solid #f5f5f5;
    }

    .result-row:last-child {
      border-bottom: none;
    }

    .k {
      font-weight: 500;
      color: rgba(0, 0, 0, 0.85);
    }

    .v {
      display: flex;
      align-items: center;
    }

    .message-text {
      color: rgba(0, 0, 0, 0.65);
      font-size: 14px;
      max-width: 300px;
      text-align: right;
    }
  `]
})
export class XStoreRebuildHealthCheckComponent {
  namespace = 'default';
  xstore = '';
  loading = false;
  checked = false;
  
  xstoreOk = false;
  readyPods = 0;
  totalPods = 0;
  phase = '';
  phaseOk = false;
  canRebuild = false;
  message = '';

  constructor(private api: ApiService, private messageService: NzMessageService) {}

  async run() {
    if (!this.namespace.trim() || !this.xstore.trim()) {
      this.messageService.warning('请填写命名空间和目标 XStore');
      return;
    }
    
    this.loading = true;
    this.checked = false;
    
    try {
      // 模拟健康检查调用，实际应该调用 API
      const result = await this.performHealthCheck(this.namespace, this.xstore);
      this.updateResults(result);
      this.checked = true;
    } catch (error) {
      this.messageService.error('健康检查失败: ' + (error as any)?.message || '未知错误');
    } finally {
      this.loading = false;
    }
  }

  private async performHealthCheck(namespace: string, xstore: string): Promise<any> {
    // 模拟 API 调用，替换为实际的健康检查逻辑
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve({
          xstoreExists: true,
          readyPods: 2,
          totalPods: 3,
          phase: 'Running',
          canRebuild: true,
          message: '集群状态良好，可以进行重搭操作'
        });
      }, 2000);
    });
  }

  private updateResults(result: any) {
    this.xstoreOk = result.xstoreExists;
    this.readyPods = result.readyPods || 0;
    this.totalPods = result.totalPods || 0;
    this.phase = result.phase || '';
    this.phaseOk = this.phase === 'Running';
    this.canRebuild = result.canRebuild;
    this.message = result.message || '';
  }
}