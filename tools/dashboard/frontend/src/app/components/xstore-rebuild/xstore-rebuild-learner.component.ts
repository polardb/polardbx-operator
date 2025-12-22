import { Component, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzMessageService, NzMessageModule } from 'ng-zorro-antd/message';
import { ApiService } from '../../services/api.service';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { Router } from '@angular/router';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzTagModule } from 'ng-zorro-antd/tag';

interface XStorePod {
  metadata?: {
    name?: string;
    labels?: Record<string, string>;
  };
  status?: {
    phase?: string;
  };
}

interface RebuildLearnerRequest {
  name: string;
  xStoreName: string;
  targetPodName?: string;
  local: boolean;
}

@Component({
  selector: 'app-xstore-rebuild-learner',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    NzCardModule,
    NzButtonModule,
    NzIconModule,
    NzMessageModule,
    NzSelectModule,
    NzFormModule,
    NzInputModule,
    NzAlertModule,
    NzTagModule
  ],
  template: `
    <div class="learner-rebuild-page">
      <div class="layout-container">
        <div class="page-header">
          <div class="header-content">
            <h1 class="page-title">
              <i nz-icon nzType="read" class="page-icon"></i>
              备库重搭 · 重搭 Learner 节点
            </h1>
            <p class="page-description">选择目标 XStore 的 Learner Pod 并触发重搭流程</p>
          </div>
        </div>

        <div class="page-content">
          <nz-card class="form-card">
            <div class="card-content">
              <form nz-form nzLayout="vertical" (ngSubmit)="submit()">
                <nz-form-item>
                  <nz-form-label nzRequired>命名空间</nz-form-label>
                  <nz-form-control>
                    <input
                      nz-input
                      id="learner-namespace-input"
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
                      id="learner-xstore-input"
                      name="xstore"
                      [(ngModel)]="xstore"
                      [ngModelOptions]="{ standalone: true }"
                      placeholder="cluster-dn" />
                  </nz-form-control>
                </nz-form-item>

                <nz-alert
                  nzType="info"
                  nzShowIcon
                  class="helper-alert"
                  nzMessage="候选 Learner Pod 会实时查询，优先展示运行中的节点">
                </nz-alert>

                <nz-form-item class="pod-select-item">
                  <nz-form-label nzRequired>Learner Pod</nz-form-label>
                  <nz-form-control>
                    <div class="pod-select-row">
                      <nz-select
                        class="pod-select"
                        [(ngModel)]="learnerPod"
                        name="learnerPod"
                        [ngModelOptions]="{ standalone: true }"
                        nzPlaceHolder="选择运行中的 Learner Pod"
                        nzShowSearch
                        [nzLoading]="loadingPods"
                        [nzDisabled]="loadingPods">
                        <nz-option
                          nzCustomContent
                          *ngFor="let pod of filteredPods"
                          [nzValue]="pod.metadata?.name"
                          [nzLabel]="pod.metadata?.name || ''">
                          <div class="pod-option">
                            <span class="pod-name">{{ pod.metadata?.name }}</span>
                            <nz-tag nzSize="small" [nzColor]="getPodRoleColor(pod)">{{ getPodRoleLabel(pod) }}</nz-tag>
                            <nz-tag nzSize="small" nzColor="processing">{{ pod.status?.phase || '未知' }}</nz-tag>
                          </div>
                        </nz-option>
                      </nz-select>
                      <button nz-button nzType="default" type="button" (click)="loadPods()" [nzLoading]="loadingPods">
                        <i nz-icon nzType="reload"></i>
                        <span>{{ loadingPods ? '加载中' : '刷新候选' }}</span>
                      </button>
                    </div>
                    <div class="pod-helper" *ngIf="!loadingPods && !filteredPods.length">
                      暂无符合条件的候选 Pod，请确认目标 XStore 处于 Running 状态。
                    </div>
                    <div class="fetch-meta" *ngIf="lastFetchedAt">
                      最近刷新：{{ lastFetchedAt | date:'MM-dd HH:mm:ss' }}
                    </div>
                  </nz-form-control>
                </nz-form-item>

                <nz-form-item>
                  <nz-form-label>任务名称 (可选)</nz-form-label>
                  <nz-form-control>
                    <input
                      nz-input
                      id="learner-name-input"
                      name="name"
                      [(ngModel)]="name"
                      [ngModelOptions]="{ standalone: true }"
                      placeholder="留空自动生成" />
                    <div class="form-hint">不填写将生成带有 Learner 标识的任务名称。</div>
                  </nz-form-control>
                </nz-form-item>

                <div class="actions">
                  <button nz-button nzType="primary" nzSize="large" type="submit" [nzLoading]="submitting">
                    <i nz-icon nzType="play-circle"></i>
                    <span>{{ submitting ? '提交中...' : '发起重搭' }}</span>
                  </button>
                  <button nz-button nzType="default" nzSize="large" type="button" (click)="resetForm()" [disabled]="submitting">
                    <i nz-icon nzType="reload"></i>
                    <span>重置</span>
                  </button>
                  <button nz-button nzType="default" nzSize="large" type="button" (click)="navigateFollowers()" [disabled]="submitting">
                    <i nz-icon nzType="team"></i>
                    <span>查看 Follower 列表</span>
                  </button>
                </div>
              </form>
            </div>
          </nz-card>
        </div>
      </div>
    </div>
  `,
  styles: [`
    .learner-rebuild-page {
      padding: 16px;
      background: #f5f5f5;
      min-height: 100vh;
    }

    .layout-container {
      max-width: 960px;
      margin: 0 auto;
    }

    .page-header {
      margin-bottom: 16px;
    }

    .header-content {
      width: 100%;
      margin: 0;
    }

    .page-title {
      font-size: 24px;
      font-weight: 600;
      color: rgba(0, 0, 0, 0.88);
      margin: 0;
      display: flex;
      align-items: center;
      gap: 12px;
    }

    .page-icon {
      font-size: 28px;
      color: var(--primary-color, #ff6a00);
    }

    .page-description {
      color: rgba(0, 0, 0, 0.6);
      font-size: 14px;
      margin: 4px 0 0 36px;
    }

    .page-content {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }

    .form-card {
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0, 0, 0, 0.06);
    }

    .card-content {
      padding: 24px;
    }

    .helper-alert {
      margin-bottom: 16px;
    }

    nz-form-item {
      margin-bottom: 16px;
    }

    .pod-select-row {
      display: flex;
      gap: 12px;
      align-items: stretch;
      flex-wrap: wrap;
    }

    .pod-select {
      flex: 1 1 240px;
    }

    .pod-option {
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .pod-name {
      font-weight: 500;
      color: rgba(0, 0, 0, 0.88);
    }

    .pod-helper {
      margin-top: 8px;
      font-size: 12px;
      color: #8c8c8c;
    }

    .fetch-meta {
      margin-top: 4px;
      font-size: 12px;
      color: rgba(0, 0, 0, 0.45);
    }

    .form-hint {
      margin-top: 6px;
      font-size: 12px;
      color: #8c8c8c;
    }

    .actions {
      display: flex;
      gap: 12px;
      margin-top: 24px;
      flex-wrap: wrap;
    }

    @media (max-width: 768px) {
      .learner-rebuild-page {
        padding: 12px;
      }

      .layout-container {
        max-width: 100%;
      }

      .card-content {
        padding: 16px;
      }

      .page-description {
        margin-left: 0;
      }
    }
  `]
})
export class XStoreRebuildLearnerComponent {
  namespace = 'default';
  xstore = '';
  learnerPod = '';
  name = '';
  submitting = false;
  loadingPods = false;
  pods: XStorePod[] = [];
  filteredPods: XStorePod[] = [];
  lastFetchedAt: Date | null = null;
  private readonly message = inject(NzMessageService);
  private readonly api = inject(ApiService);
  private readonly router = inject(Router);

  private genName(base: string): string {
    const now = new Date();
    const ts = `${now.getFullYear()}${(now.getMonth()+1).toString().padStart(2,'0')}${now.getDate().toString().padStart(2,'0')}${now.getHours().toString().padStart(2,'0')}${now.getMinutes().toString().padStart(2,'0')}`;
    const rand = Math.random().toString(36).slice(2,6);
    const raw = `${base}-learner-${ts}-${rand}`.toLowerCase().replace(/[^a-z0-9-]/g,'-');
    return raw.length>63 ? raw.slice(0,63) : raw;
  }

  submit(): void {
    const namespace = (this.namespace || '').trim() || 'default';
    const xstore = (this.xstore || '').trim();
    const learnerPod = (this.learnerPod || '').trim();

    if (!xstore || !learnerPod) {
      this.message.warning('请完善必填项：目标 XStore 和 Learner Pod');
      return;
    }

    const req: RebuildLearnerRequest = {
      name: (this.name || '').trim() || this.genName(xstore),
      xStoreName: xstore,
      targetPodName: learnerPod || undefined,
      local: true
    };
    this.submitting = true;
    this.api.rebuildLearner(namespace, req).subscribe({
      next: () => {
        this.message.success('已提交 Learner 重搭任务');
        this.submitting = false;
        this.router.navigateByUrl('/storage/xstore-followers');
      },
      error: (error: unknown) => {
        const message =
          (error as { error?: { message?: string }; message?: string })?.error?.message ||
          (error as { message?: string })?.message ||
          '未知错误';
        this.message.error(`提交失败: ${message}`);
        this.submitting = false;
      }
    });
  }

  loadPods(): void {
    const namespace = (this.namespace || '').trim() || 'default';
    const xstore = (this.xstore || '').trim();
    if (!xstore) {
      this.message.warning('请先填写目标 XStore');
      return;
    }
    this.loadingPods = true;
    this.api.getXStorePods(namespace, xstore).subscribe({
      next: (pods: XStorePod[] | null | undefined) => {
        this.pods = pods ?? [];
        this.filteredPods = this.pods.filter(pod => {
          const phase = pod.status?.phase;
          const role = pod.metadata?.labels?.['xstore/role'];
          return phase === 'Running' && (role === 'learner' || !role);
        });
        if (!this.filteredPods.length) {
          this.filteredPods = this.pods.filter(pod => pod.status?.phase === 'Running');
        }
        if (!this.filteredPods.length) {
          this.message.info('未找到符合条件的运行中 Learner Pod');
        } else {
          this.message.success(`已加载 ${this.filteredPods.length} 个候选 Pod`);
        }
        this.lastFetchedAt = new Date();
        this.loadingPods = false;
      },
      error: () => {
        this.pods = [];
        this.filteredPods = [];
        this.loadingPods = false;
        this.message.error('加载候选 Pod 失败，请稍后重试');
      }
    });
  }

  resetForm(): void {
    this.namespace = 'default';
    this.xstore = '';
    this.learnerPod = '';
    this.name = '';
    this.pods = [];
    this.filteredPods = [];
    this.lastFetchedAt = null;
  }

  navigateFollowers(): void {
    this.router.navigateByUrl('/storage/xstore-followers');
  }

  getPodRoleLabel(pod: XStorePod): string {
    return pod.metadata?.labels?.['xstore/role'] || '未标记';
  }

  getPodRoleColor(pod: XStorePod): string {
    const role = (pod.metadata?.labels?.['xstore/role'] || '').toLowerCase();
    switch (role) {
      case 'learner':
        return 'purple';
      case 'leader':
        return 'gold';
      case 'logger':
        return 'geekblue';
      case 'follower':
        return 'green';
      default:
        return 'default';
    }
  }
}