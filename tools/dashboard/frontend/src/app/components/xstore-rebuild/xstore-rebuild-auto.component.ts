import { Component, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzMessageService, NzMessageModule } from 'ng-zorro-antd/message';
import { ApiService } from '../../services/api.service';
import { Router } from '@angular/router';

interface AutoRebuildRequest {
  name?: string;
  xStoreName: string;
  strategy?: string;
}

@Component({
  selector: 'app-xstore-rebuild-auto',
  standalone: true,
  imports: [CommonModule, FormsModule, NzCardModule, NzButtonModule, NzIconModule, NzFormModule, NzInputModule, NzMessageModule],
  template: `
    <div class="auto-rebuild-page">
      <div class="page-header">
        <div class="header-content">
          <h1 class="page-title">
            <i nz-icon nzType="robot" class="page-icon"></i>
            自动备库重搭
          </h1>
          <p class="page-description">根据健康检查与策略自动选择并执行重搭任务</p>
        </div>
      </div>

      <div class="page-content">
        <nz-card class="form-card">
          <div class="card-content">
            <form nz-form nzLayout="vertical" class="auto-form">
              <nz-form-item>
                <nz-form-label nzRequired>命名空间</nz-form-label>
                <nz-form-control>
                  <input
                    nz-input
                    id="auto-namespace-input"
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
                    id="auto-xstore-input"
                    name="xstore"
                    [(ngModel)]="xstore"
                    [ngModelOptions]="{ standalone: true }"
                    placeholder="cluster-dn" />
                </nz-form-control>
              </nz-form-item>

              <div class="actions">
                <button nz-button nzType="primary" nzSize="large" (click)="start()" [nzLoading]="submitting">
                  <i nz-icon nzType="play-circle"></i>
                  <span>{{ submitting ? '执行中...' : '一键重搭' }}</span>
                </button>
                <button nz-button nzType="default" nzSize="large" (click)="resetForm()" [disabled]="submitting">
                  <i nz-icon nzType="reload"></i>
                  <span>重置</span>
                </button>
              </div>
            </form>
          </div>
        </nz-card>
      </div>
    </div>
  `,
  styles: [`
    .auto-rebuild-page {
      padding: 16px;
      background: #f5f5f5;
      min-height: 100vh;
    }

    .page-header {
      margin-bottom: 16px;
    }

    .header-content {
      width: 100%;
      max-width: none;
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
      width: 100%;
      max-width: none;
      margin: 0;
    }

    .form-card {
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0, 0, 0, 0.06);
    }

    .card-content {
      padding: 24px;
    }

    .auto-form {
      max-width: 480px;
    }

    .actions {
      display: flex;
      gap: 12px;
      margin-top: 24px;
    }

    @media (max-width: 768px) {
      .auto-rebuild-page {
        padding: 12px;
      }

      .card-content {
        padding: 16px;
      }

      .actions {
        flex-direction: column;
        align-items: stretch;
      }
    }
  `]
})
export class XStoreRebuildAutoComponent {
  namespace = 'default';
  xstore = '';
  submitting = false;
  private readonly message = inject(NzMessageService);
  private readonly api = inject(ApiService);
  private readonly router = inject(Router);

  private genName(base: string): string {
    const now = new Date();
    const ts = `${now.getFullYear()}${(now.getMonth()+1).toString().padStart(2,'0')}${now.getDate().toString().padStart(2,'0')}${now.getHours().toString().padStart(2,'0')}${now.getMinutes().toString().padStart(2,'0')}`;
    const rand = Math.random().toString(36).slice(2,6);
    const raw = `${base}-auto-rebuild-${ts}-${rand}`.toLowerCase().replace(/[^a-z0-9-]/g,'-');
    return raw.length>63 ? raw.slice(0,63) : raw;
  }

  start(): void {
    const namespace = (this.namespace || '').trim() || 'default';
    const xstore = (this.xstore || '').trim();
    if (!xstore) {
      this.message.warning('请填写目标 XStore');
      return;
    }
    const req: AutoRebuildRequest = {
      name: this.genName(xstore),
      xStoreName: xstore
    };
    this.submitting = true;
    this.api.autoRebuild(namespace, req).subscribe({
      next: () => {
        this.message.success('自动重搭已触发');
        this.submitting = false;
        this.router.navigateByUrl('/storage/xstore-followers');
      },
      error: (error: unknown) => {
        const message = (error as { error?: { message?: string }; message?: string })?.error?.message || (error as { message?: string })?.message || '未知错误';
        this.message.error(`触发失败: ${message}`);
        this.submitting = false;
      }
    });
  }

  resetForm(): void {
    this.namespace = 'default';
    this.xstore = '';
  }
}
