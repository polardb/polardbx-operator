import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReactiveFormsModule, FormBuilder, FormGroup, Validators } from '@angular/forms';
import { Router, ActivatedRoute } from '@angular/router';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzSwitchModule } from 'ng-zorro-antd/switch';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzDividerModule } from 'ng-zorro-antd/divider';
 
import { Subject } from 'rxjs';
import { takeUntil, finalize } from 'rxjs/operators';

import { ApiService } from '../../services/api.service';
import { LoadingService } from '../../services/loading.service';
import { CreateXStoreFollowerRequest } from '../../models/xstore-follower.model';
import { XStore } from '../../models/xstore.model';

interface RebuildFormData {
  namespace: string;
  role: 'learner' | 'logger' | 'follower';
  xStoreName: string;
  targetPodName?: string;
  fromPodName?: string;
  nodeName?: string;
  local: boolean;
  description?: string;
}

@Component({
  selector: 'app-rebuild-form',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    NzCardModule,
    NzButtonModule,
    NzIconModule,
    NzInputModule,
    NzSelectModule,
    NzFormModule,
    NzSwitchModule,
    NzSpinModule,
    NzDividerModule,
    
  ],
  template: `
    <div class="rebuild-form-container neutral-theme">
      <div class="page-header">
        <div class="header-content">
          <h1 class="page-title">
            <i nz-icon nzType="build" class="page-icon"></i>
            创建重搭任务
          </h1>
          <p class="subtitle">配置 XStore 重搭参数，快速创建重搭任务</p>
        </div>
      </div>
      
      <nz-card class="form-card">
        <div class="form-content">
          <form nz-form [formGroup]="rebuildForm" (ngSubmit)="onSubmit()">
            
            <!-- 基础信息 -->
            <div class="form-section">
              <h4 class="section-title">
                <i nz-icon nzType="setting"></i>
                <span>基础配置</span>
              </h4>
              
              <nz-form-item>
                <nz-form-label [nzSpan]="6" nzFor="namespace" nzRequired>命名空间</nz-form-label>
                <nz-form-control [nzSpan]="18" nzErrorTip="请选择命名空间">
                  <nz-select 
                    id="namespace" 
                    formControlName="namespace" 
                    nzPlaceHolder="选择命名空间">
                    <nz-option *ngFor="let ns of namespaces" [nzValue]="ns" [nzLabel]="ns"></nz-option>
                  </nz-select>
                </nz-form-control>
              </nz-form-item>

              <nz-form-item>
                <nz-form-label [nzSpan]="6" nzFor="role" nzRequired>重搭角色</nz-form-label>
                <nz-form-control [nzSpan]="18" nzErrorTip="请选择重搭角色">
                  <nz-select 
                    id="role" 
                    formControlName="role" 
                    nzPlaceHolder="选择角色类型">
                    <nz-option nzValue="learner" nzLabel="Learner（学习者）"></nz-option>
                    <nz-option nzValue="logger" nzLabel="Logger（日志）"></nz-option>
                    <nz-option nzValue="follower" nzLabel="Follower（从节点）"></nz-option>
                  </nz-select>
                </nz-form-control>
              </nz-form-item>

              <nz-form-item>
                <nz-form-label [nzSpan]="6" nzFor="xStoreName" nzRequired>目标 XStore</nz-form-label>
                <nz-form-control [nzSpan]="18" nzErrorTip="请选择目标 XStore">
                  <nz-select 
                    id="xStoreName" 
                    formControlName="xStoreName" 
                    nzPlaceHolder="选择 XStore"
                    [nzLoading]="isLoadingXStores">
                    <nz-option *ngFor="let xstore of availableXStores; trackBy: trackByXStoreName" [nzValue]="xstore.metadata.name" [nzLabel]="xstore.metadata.name"></nz-option>
                  </nz-select>
                </nz-form-control>
              </nz-form-item>
            </div>

            <nz-divider></nz-divider>

            <!-- 高级配置 -->
            <div class="form-section">
              <h4 class="section-title">
                <i nz-icon nzType="tool"></i>
                <span>高级配置</span>
              </h4>

              <!-- 本机构建选项 -->
              <nz-form-item>
                <nz-form-label [nzSpan]="6" nzFor="local">本机构建</nz-form-label>
                <nz-form-control [nzSpan]="18">
                  <nz-switch 
                    id="local" 
                    formControlName="local"
                    [nzLoading]="false">
                  </nz-switch>
                  <span class="switch-hint">
                    {{ rebuildForm.get('local')?.value ? '在当前节点进行重搭' : '跨节点或新建节点重搭' }}
                  </span>
                </nz-form-control>
              </nz-form-item>

              <!-- 目标 Pod 选择 -->
              <nz-form-item *ngIf="showTargetPod">
                <nz-form-label [nzSpan]="6" nzFor="targetPodName" [nzRequired]="targetPodRequired">
                  目标 Pod
                </nz-form-label>
                <nz-form-control [nzSpan]="18" [nzErrorTip]="targetPodErrorTip">
                  <nz-select 
                    id="targetPodName" 
                    formControlName="targetPodName" 
                    nzPlaceHolder="选择目标 Pod"
                    [nzLoading]="isLoadingPods"
                    nzAllowClear>
                    <nz-option *ngFor="let pod of filteredTargetPods; trackBy: trackByPodName" [nzValue]="pod.metadata.name" [nzLabel]="pod.metadata.name"></nz-option>
                  </nz-select>
                </nz-form-control>
              </nz-form-item>

              <!-- 源 Pod 选择（Logger 重搭时显示） -->
              <nz-form-item *ngIf="showFromPod">
                <nz-form-label [nzSpan]="6" nzFor="fromPodName">
                  源 Pod
                </nz-form-label>
                <nz-form-control [nzSpan]="18">
                  <nz-select 
                    id="fromPodName" 
                    formControlName="fromPodName" 
                    nzPlaceHolder="选择源 Pod（可选）"
                    [nzLoading]="isLoadingPods"
                    nzAllowClear>
                    <nz-option *ngFor="let pod of filteredSourcePods; trackBy: trackByPodName" [nzValue]="pod.metadata.name" [nzLabel]="pod.metadata.name"></nz-option>
                  </nz-select>
                </nz-form-control>
              </nz-form-item>

              <!-- 节点选择（跨机构建时显示） -->
              <nz-form-item *ngIf="showNodeName">
                <nz-form-label [nzSpan]="6" nzFor="nodeName">目标节点</nz-form-label>
                <nz-form-control [nzSpan]="18">
                  <input 
                    nz-input
                    id="nodeName" 
                    formControlName="nodeName" 
                    placeholder="输入目标节点名称（可选）" />
                </nz-form-control>
              </nz-form-item>

              <!-- 备注 -->
              <nz-form-item>
                <nz-form-label [nzSpan]="6" nzFor="description">备注</nz-form-label>
                <nz-form-control [nzSpan]="18">
                  <input 
                    nz-input
                    id="description" 
                    formControlName="description" 
                    placeholder="可选的任务描述" />
                </nz-form-control>
              </nz-form-item>
            </div>

            <!-- 提示信息（简化） -->
            <div class="form-section" *ngIf="roleHints.length > 0">
              <div *ngFor="let hint of roleHints" class="role-hint-text">{{ hint }}</div>
            </div>

            <!-- 操作按钮 -->
            <nz-form-item class="submit-buttons">
              <nz-form-control [nzOffset]="6" [nzSpan]="18">
                <button 
                  nz-button 
                  nzType="primary" 
                  [nzLoading]="isSubmitting"
                  [disabled]="!rebuildForm.valid"
                  type="submit">
                  <i nz-icon nzType="play-circle"></i>
                  <span>创建重搭任务</span>
                </button>
                <button 
                  nz-button 
                  nzType="default" 
                  (click)="onCancel()"
                  [disabled]="isSubmitting"
                  style="margin-left: 8px;">
                  <i nz-icon nzType="rollback"></i>
                  <span>取消</span>
                </button>
                <button 
                  nz-button 
                  nzType="default" 
                  (click)="goToTaskList()"
                  style="margin-left: 8px;">
                  <i nz-icon nzType="bars"></i>
                  <span>查看任务列表</span>
                </button>
              </nz-form-control>
            </nz-form-item>

          </form>
        </div>
      </nz-card>
    </div>
  `,
  styleUrls: ['./rebuild-form.component.scss'],
  styles: [`
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
  `]
})
export class RebuildFormComponent implements OnInit, OnDestroy {
  private destroy$ = new Subject<void>();
  
  rebuildForm!: FormGroup;
  namespaces: string[] = [];
  availableXStores: XStore[] = [];
  targetPods: any[] = [];
  filteredTargetPods: any[] = [];
  filteredSourcePods: any[] = [];
  
  isLoadingXStores = false;
  isLoadingPods = false;
  isSubmitting = false;
  showTargetPod = false;
  showFromPod = false;
  showNodeName = false;
  targetPodRequired = false;
  targetPodErrorTip = '';
  localHint = '';
  roleHints: string[] = [];

  constructor(
    private fb: FormBuilder,
    private router: Router,
    private route: ActivatedRoute,
    private apiService: ApiService,
    private loadingService: LoadingService,
    private message: NzMessageService
  ) {
    this.initForm();
  }

  ngOnInit(): void {
    this.loadNamespaces();
    this.handleQueryParams();

    this.rebuildForm.get('role')?.valueChanges
      .pipe(takeUntil(this.destroy$))
      .subscribe(role => {
        this.onRoleChange(role);
        this.refreshDerivedStates();
      });

    this.rebuildForm.get('local')?.valueChanges
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => this.refreshDerivedStates());

    this.rebuildForm.get('namespace')?.valueChanges
      .pipe(takeUntil(this.destroy$))
      .subscribe(ns => this.onNamespaceChange(ns));

    this.rebuildForm.get('xStoreName')?.valueChanges
      .pipe(takeUntil(this.destroy$))
      .subscribe(name => this.onXStoreChange(name));

    this.refreshDerivedStates();
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  private initForm(): void {
    this.rebuildForm = this.fb.group({
      namespace: ['default', [Validators.required]],
      role: ['learner', [Validators.required]],
      xStoreName: ['', [Validators.required]],
      targetPodName: [''],
      fromPodName: [''],
      nodeName: [''],
      local: [true],
      description: ['']
    });

    // Set validators based on role
    this.rebuildForm.get('role')?.valueChanges.subscribe(role => {
      this.updateFormValidators(role);
    });
  }

  private handleQueryParams(): void {
    this.route.queryParams.pipe(takeUntil(this.destroy$)).subscribe(params => {
      if (params['role']) {
        this.rebuildForm.patchValue({ role: params['role'] });
      }
      if (params['namespace']) {
        this.rebuildForm.patchValue({ namespace: params['namespace'] });
        this.onNamespaceChange(params['namespace']);
      }
      if (params['xstore']) {
        this.rebuildForm.patchValue({ xStoreName: params['xstore'] });
        this.onXStoreChange(params['xstore']);
      }
      if (params['targetPod']) {
        this.rebuildForm.patchValue({ targetPodName: params['targetPod'] });
      }
      if (params['fromPod']) {
        this.rebuildForm.patchValue({ fromPodName: params['fromPod'] });
      }
    });
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

  onNamespaceChange(namespace: string): void {
    if (!namespace) return;
    
    this.isLoadingXStores = true;
    this.apiService.getXStores(namespace)
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: (xstores) => {
          this.availableXStores = xstores || [];
          this.isLoadingXStores = false;
        },
        error: (error) => {
          console.error('Failed to load XStores:', error);
          this.availableXStores = [];
          this.isLoadingXStores = false;
        }
      });
  }

  onXStoreChange(xstoreName: string): void {
    if (!xstoreName) return;
    
    const namespace = this.rebuildForm.get('namespace')?.value;
    if (!namespace) return;

    this.isLoadingPods = true;
    this.apiService.getXStorePods(namespace, xstoreName)
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: (pods) => {
          this.targetPods = pods || [];
          this.filterPods();
          this.isLoadingPods = false;
        },
        error: (error) => {
          console.error('Failed to load XStore pods:', error);
          this.targetPods = [];
          this.filteredTargetPods = [];
          this.filteredSourcePods = [];
          this.isLoadingPods = false;
        }
      });
  }

  onRoleChange(role: string): void {
    this.updateFormValidators(role);
    this.filterPods();
    this.refreshDerivedStates();
  }

  private updateFormValidators(role: string): void {
    const targetPodControl = this.rebuildForm.get('targetPodName');
    
    if (role === 'learner') {
      // Learner requires targetPodName
      targetPodControl?.setValidators([Validators.required]);
      this.rebuildForm.patchValue({ local: true });
    } else {
      // Logger and follower don't require targetPodName
      targetPodControl?.clearValidators();
    }
    
    targetPodControl?.updateValueAndValidity();
  }

  private filterPods(): void {
    const role = this.rebuildForm.get('role')?.value;
    
    // Filter target pods (prefer running and non-leader)
    this.filteredTargetPods = this.targetPods.filter(pod => {
      const phase = pod.status?.phase;
      const podRole = this.getPodRole(pod);
      return phase === 'Running' && podRole !== 'leader';
    });
    
    // If no suitable target pods, show all running pods
    if (this.filteredTargetPods.length === 0) {
      this.filteredTargetPods = this.targetPods.filter(pod => pod.status?.phase === 'Running');
    }
    
    // Filter source pods (prefer running pods for logger rebuild)
    this.filteredSourcePods = this.targetPods.filter(pod => pod.status?.phase === 'Running');
    
    // Auto-select single candidate for learner
    if (role === 'learner' && this.filteredTargetPods.length === 1 && !this.rebuildForm.get('targetPodName')?.value) {
      this.rebuildForm.patchValue({ targetPodName: this.filteredTargetPods[0].metadata.name });
    }
  }

  shouldShowTargetPod(): boolean {
    const role = this.rebuildForm.get('role')?.value;
    return role === 'learner' || role === 'logger';
  }

  // 旧派生函数已移除（改为 refreshDerivedStates 计算好的属性）

  getPodRole(pod: any): string {
    return pod?.metadata?.labels?.['xstore/role'] || '';
  }

  getPodStatusColor(pod: any): string {
    const phase = pod?.status?.phase;
    switch (phase) {
      case 'Running': return 'green';
      case 'Pending': return 'blue';
      case 'Failed': return 'red';
      default: return 'default';
    }
  }

  getRoleColor(role: string): string {
    switch (role) {
      case 'leader': return 'gold';
      case 'follower': return 'blue';
      case 'logger': return 'cyan';
      default: return 'default';
    }
  }

  getXStoreStatusColor(xstore: XStore): string {
    const phase = (xstore.status as any)?.phase;
    switch (phase) {
      case 'Running': return 'green';
      case 'Pending': return 'blue';
      case 'Failed': return 'red';
      default: return 'default';
    }
  }

  getXStoreStatusText(xstore: XStore): string {
    return (xstore.status as any)?.phase || '未知';
  }

  trackByPodName(_index: number, pod: any): string { return pod?.metadata?.name; }
  trackByXStoreName(_index: number, xs: XStore): string { return xs?.metadata?.name; }

  private refreshDerivedStates(): void {
    const role = this.rebuildForm.get('role')?.value;
    const local = this.rebuildForm.get('local')?.value;

    this.showTargetPod = role === 'learner' || role === 'logger';
    this.showFromPod = role === 'logger';
    this.showNodeName = !local;
    this.targetPodRequired = role === 'learner';
    this.targetPodErrorTip = this.targetPodRequired ? '请选择目标 Pod' : '';
    this.localHint = local ? '在当前节点进行重搭' : '跨节点或新建节点重搭';

    const hints: string[] = [];
    if (role === 'learner') hints.push('Learner 重搭建议选择 Running 的目标 Pod');
    if (role === 'logger') hints.push('Logger 重搭可选源 Pod，不选将按系统策略自动选择');
    if (!local) hints.push('跨机重搭会在其他节点新建 Pod，可指定目标节点');
    this.roleHints = hints;
  }

  onSubmit(): void {
    if (!this.rebuildForm.valid) {
      Object.values(this.rebuildForm.controls).forEach(control => {
        if (control.invalid) {
          control.markAsDirty();
          control.updateValueAndValidity({ onlySelf: true });
        }
      });
      return;
    }

    this.isSubmitting = true;
    const formValue = this.rebuildForm.value as RebuildFormData;
    
    const request: CreateXStoreFollowerRequest = {
      xStoreName: formValue.xStoreName,
      role: formValue.role,
      local: formValue.local,
      targetPodName: formValue.targetPodName,
      fromPodName: formValue.fromPodName,
      nodeName: formValue.nodeName
    };

    this.apiService.createXStoreFollower(formValue.namespace, request)
      .pipe(
        takeUntil(this.destroy$),
        finalize(() => {
          this.isSubmitting = false;
        })
      )
      .subscribe({
        next: (result) => {
          this.message.success('重搭任务创建成功');
          
          // Navigate to task detail
          if (result?.metadata?.name) {
            this.router.navigate(['/storage/xstore-rebuild/rebuild/tasks', formValue.namespace, result.metadata.name]);
          } else {
            this.router.navigate(['/storage/xstore-rebuild/rebuild/tasks']);
          }
        },
        error: (error) => {
          console.error('Failed to create rebuild task:', error);
          this.message.error('创建重搭任务失败: ' + (error as any)?.message || '未知错误');
        }
      });
  }

  onCancel(): void {
    this.goToTaskList();
  }

  goToTaskList(): void {
    this.router.navigate(['/storage/xstore-rebuild/rebuild/tasks']);
  }
}
