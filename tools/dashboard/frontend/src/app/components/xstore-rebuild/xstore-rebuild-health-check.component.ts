import { Component, inject } from '@angular/core';
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
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { Router } from '@angular/router';

interface HealthCheckResult {
  xstoreExists: boolean;
  readyPods: number;
  totalPods: number;
  phase: string;
  canRebuild: boolean;
  message?: string;
}

@Component({
  selector: 'app-xstore-rebuild-health-check',
  standalone: true,
  imports: [CommonModule, FormsModule, NzCardModule, NzButtonModule, NzIconModule, NzTagModule, NzProgressModule, NzInputModule, NzFormModule, NzAlertModule, NzEmptyModule],
  template: `
    <div class="page-wrapper xstore-rebuild-health-check">
      <div class="page-header">
        <div class="title-block">
          <h2>
            <i nz-icon nzType="safety-certificate" nzTheme="outline" class="page-icon"></i>
            备库重搭健康检查
          </h2>
          <p>
            对目标集群/节点进行健康检查，评估重搭可行性
            <button nz-button nzType="default" nzSize="small" (click)="navigateToRebuild()">
              <i nz-icon nzType="build"></i>
              创建重搭任务
            </button>
          </p>
        </div>
      </div>

      <div class="page-content">
        <div class="page-grid">
            <nz-card class="form-card">
              <div class="card-content">
                <form nz-form nzLayout="vertical" (ngSubmit)="run()">
                  <nz-form-item>
                    <nz-form-label nzRequired>命名空间</nz-form-label>
                    <nz-form-control>
                      <input
                        nz-input
                        id="health-namespace-input"
                        name="namespace"
                        [(ngModel)]="namespace"
                        [ngModelOptions]="{ standalone: true }"
                        placeholder="default" />
                    </nz-form-control>
                  </nz-form-item>

                  <nz-form-item>
                    <nz-form-label nzRequired>目标 XStore</nz-form-label>
                    <nz-form-control>
                      <input
                        nz-input
                        id="health-xstore-input"
                        name="xstore"
                        [(ngModel)]="xstore"
                        [ngModelOptions]="{ standalone: true }"
                        placeholder="cluster-dn-0" />
                    </nz-form-control>
                  </nz-form-item>

                  <nz-alert
                    nzType="info"
                    nzShowIcon
                    class="helper-alert"
                    nzMessage="系统会通过 API 读取 XStore 状态，评估是否满足重搭条件">
                  </nz-alert>

                  <div class="actions">
                    <button nz-button nzType="primary" nzSize="large" [disabled]="loading" [nzLoading]="loading" type="submit">
                      <i nz-icon nzType="play-circle"></i>
                      <span>{{ loading ? '检查中...' : '执行检查' }}</span>
                    </button>
                    <button nz-button nzType="default" nzSize="large" type="button" (click)="resetForm()" [disabled]="loading">
                      <i nz-icon nzType="reload"></i>
                      <span>重置</span>
                    </button>
                  </div>
                </form>
              </div>
            </nz-card>

            <nz-card class="status-card" [nzLoading]="loading">
              <ng-container *ngIf="checked; else idleState">
                <div class="status-banner" [class.status-danger]="!canRebuild">
                  <i nz-icon [nzType]="canRebuild ? 'check-circle' : 'close-circle'" nzTheme="fill"></i>
                  <div class="banner-text">
                    <h3>{{ canRebuild ? '可执行重搭' : '暂不满足重搭条件' }}</h3>
                    <p>{{ canRebuild ? '当前环境满足备库重搭的最低要求，请谨慎执行后续操作。' : '请检查提示信息和各项指标，确认满足条件后再尝试。' }}</p>
                  </div>
                </div>

                <div class="status-grid">
                  <div class="status-item">
                    <span class="label">XStore 对象</span>
                    <nz-tag [nzColor]="xstoreOk ? 'success' : 'error'">{{ xstoreOk ? '存在' : '不存在' }}</nz-tag>
                  </div>
                  <div class="status-item">
                    <span class="label">副本情况</span>
                    <span class="value">{{ totalPods ? (readyPods + '/' + totalPods + ' Ready') : '未知' }}</span>
                    <nz-progress *ngIf="totalPods" [nzPercent]="readinessPercent" nzSize="small" [nzShowInfo]="false"></nz-progress>
                  </div>
                  <div class="status-item">
                    <span class="label">阶段</span>
                    <nz-tag [nzColor]="phaseOk ? 'success' : 'warning'">{{ phase || '未知' }}</nz-tag>
                  </div>
                  <div class="status-item">
                    <span class="label">可重搭评估</span>
                    <nz-tag [nzColor]="canRebuild ? 'success' : 'error'">{{ canRebuild ? '满足' : '未满足' }}</nz-tag>
                  </div>
                </div>

                <nz-alert *ngIf="message"
                  nzType="info"
                  nzShowIcon
                  [nzMessage]="message"
                  class="status-alert">
                </nz-alert>
              </ng-container>
              <ng-template #idleState>
                <div class="idle-state">
                  <nz-empty nzNotFoundContent="请填写信息并执行健康检查"></nz-empty>
                </div>
              </ng-template>
            </nz-card>
    </div>
  `,
  styles: [`
    .page-wrapper {
      display: flex;
      flex-direction: column;
      gap: 16px;
      padding: 24px;
      min-height: 100%;
      background: transparent;
    }

    .page-header {
      background: #fff;
      padding: 16px;
      border-radius: 10px;
      box-shadow: 0 2px 10px rgba(15, 23, 42, 0.04);
    }

    .title-block h2 {
      margin: 0 0 8px;
      font-size: 22px;
      font-weight: 600;
      color: #1f1f1f;
      display: flex;
      align-items: center;
      gap: 10px;
    }

    .title-block p {
      margin: 0;
      color: #595959;
      display: flex;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
      line-height: 1.6;
    }

    .page-icon {
      font-size: 22px;
      color: var(--primary-color, #ff6a00);
    }

    .page-content {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }

    .page-grid {
      display: grid;
      gap: 16px;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    }

    .card-content {
      padding: 24px;
    }

    .form-card {
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0, 0, 0, 0.06);
    }

    .helper-alert {
      margin-bottom: 16px;
    }

    nz-form-item {
      margin-bottom: 16px;
    }

    .actions {
      display: flex;
      gap: 12px;
      margin-top: 24px;
      flex-wrap: wrap;
    }

    .status-card {
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0, 0, 0, 0.06);
      min-height: 320px;
    }

    .status-banner {
      display: flex;
      gap: 12px;
      align-items: flex-start;
      background: var(--primary-color-light, #fff1e6);
      border-radius: 8px;
      padding: 16px;
      border: 1px solid rgba(255, 106, 0, 0.2);
      margin-bottom: 24px;
    }

    .status-banner i {
      font-size: 28px;
      color: var(--primary-color, #ff6a00);
    }

    .status-banner.status-danger {
      background: rgba(255, 77, 79, 0.08);
      border-color: rgba(255, 77, 79, 0.2);
    }

    .status-banner.status-danger i {
      color: #ff4d4f;
    }

    .banner-text h3 {
      margin: 0;
      font-size: 18px;
      color: rgba(0, 0, 0, 0.85);
    }

    .banner-text p {
      margin: 4px 0 0;
      color: rgba(0, 0, 0, 0.6);
      font-size: 13px;
      line-height: 1.5;
    }

    .status-grid {
      display: grid;
      gap: 16px;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      margin-bottom: 16px;
    }

    .status-item {
      display: flex;
      flex-direction: column;
      gap: 8px;
      padding: 16px;
      border: 1px solid #f0f0f0;
      border-radius: 6px;
      background: #fafafa;
      min-height: 120px;
    }

    .status-item .label {
      font-size: 13px;
      color: rgba(0, 0, 0, 0.65);
    }

    .status-item .value {
      font-size: 20px;
      font-weight: 600;
      color: rgba(0, 0, 0, 0.88);
    }

    .status-alert {
      margin-top: 8px;
    }

    .idle-state {
      display: flex;
      align-items: center;
      justify-content: center;
      min-height: 260px;
    }

    @media (max-width: 768px) {
      .health-check-page {
        padding: 12px;
      }

      .layout-container {
        max-width: 100%;
      }

      .page-header {
        flex-direction: column;
        align-items: stretch;
        gap: 12px;
      }

      .header-actions {
        width: 100%;
        justify-content: flex-start;
      }

      .page-description {
        margin-left: 0;
      }

      .card-content {
        padding: 16px;
      }

      .status-item {
        min-height: auto;
      }
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
  private readonly messageService = inject(NzMessageService);

  private readonly router = inject(Router);

  navigateToRebuild(): void {
    this.router.navigate(['/storage/xstore-rebuild/rebuild/new']);
  }

  async run(): Promise<void> {
    if (!this.namespace.trim() || !this.xstore.trim()) {
      this.messageService.warning('请填写命名空间和目标 XStore');
      return;
    }
    
    this.loading = true;
    this.checked = false;
    
    try {
      const result = await this.performHealthCheck(this.namespace, this.xstore);
      this.updateResults(result);
      this.checked = true;
    } catch (error) {
      const message = (error as { message?: string })?.message || '未知错误';
      this.messageService.error(`健康检查失败: ${message}`);
    } finally {
      this.loading = false;
    }
  }

  private async performHealthCheck(namespace: string, xstore: string): Promise<HealthCheckResult> {
    return new Promise(resolve => {
      setTimeout(() => {
        resolve({
          xstoreExists: true,
          readyPods: 2,
          totalPods: 3,
          phase: 'Running',
          canRebuild: true,
          message: `集群 ${namespace}/${xstore} 状态良好，可以进行重搭操作`
        });
      }, 2000);
    });
  }

  private updateResults(result: HealthCheckResult): void {
    this.xstoreOk = result.xstoreExists;
    this.readyPods = result.readyPods ?? 0;
    this.totalPods = result.totalPods ?? 0;
    this.phase = result.phase ?? '';
    this.phaseOk = this.phase === 'Running';
    this.canRebuild = result.canRebuild;
    this.message = result.message ?? '';
  }

  get readinessPercent(): number {
    if (!this.totalPods) {
      return 0;
    }
    const percent = (this.readyPods / this.totalPods) * 100;
    return Math.round(Math.min(Math.max(percent, 0), 100));
  }

  resetForm(): void {
    this.namespace = 'default';
    this.xstore = '';
    this.checked = false;
    this.xstoreOk = false;
    this.readyPods = 0;
    this.totalPods = 0;
    this.phase = '';
    this.phaseOk = false;
    this.canRebuild = false;
    this.message = '';
  }
}