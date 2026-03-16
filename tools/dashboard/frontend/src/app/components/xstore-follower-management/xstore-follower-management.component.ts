import { Component, inject, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormBuilder, FormGroup, Validators, ReactiveFormsModule, FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzPaginationModule } from 'ng-zorro-antd/pagination';
import { NzTabsModule } from 'ng-zorro-antd/tabs';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzProgressModule } from 'ng-zorro-antd/progress';
import { NzCheckboxModule } from 'ng-zorro-antd/checkbox';
import { NzMessageService, NzMessageModule } from 'ng-zorro-antd/message';
import { NzDropDownModule } from 'ng-zorro-antd/dropdown';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzModalService, NzModalModule } from 'ng-zorro-antd/modal';
import { NzDescriptionsModule } from 'ng-zorro-antd/descriptions';
import { ApiService } from '../../services/api.service';
import { LoadingService } from '../../services/loading.service';
import { XStoreFollower, XStoreFollowerWithStatus, CreateXStoreFollowerRequest } from '../../models/xstore-follower.model';
import { XStore } from '../../models/xstore.model';
import { Pod } from '../../models/pod.model';

export interface XStoreFollowerDialogData {
  mode: 'create' | 'edit' | 'view';
  follower?: XStoreFollower;
  namespace?: string;
}

@Component({
  selector: 'app-xstore-follower-management',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    ReactiveFormsModule,
    NzCardModule,
    NzButtonModule,
    NzFormModule,
    NzInputModule,
    NzSelectModule,
    NzSpinModule,
    NzIconModule,
    NzTableModule,
    NzPaginationModule,
    NzTabsModule,
    NzTagModule,
    NzToolTipModule,
    NzProgressModule,
    NzCheckboxModule,
    NzDropDownModule,
    NzDividerModule,
    NzEmptyModule,
    NzDescriptionsModule,
    NzModalModule,
    NzMessageModule
  ],
  templateUrl: './xstore-follower-management.component.html',
  styleUrls: ['./xstore-follower-management.component.scss']
})
export class XStoreFollowerManagementComponent implements OnInit {
  private fb = inject(FormBuilder);
  private apiService = inject(ApiService);
  private loadingService = inject(LoadingService);
  private message = inject(NzMessageService);
  private modal = inject(NzModalService);
  private router = inject(Router);
  
  // Component state
  isLoading = false;
  isProcessing = false;
  selectedTab = 0;
  data: XStoreFollowerDialogData = { mode: 'create' };

  // ===== Detail modals (align with other pages) =====
  detailsVisible = false;
  selectedFollower: XStoreFollowerWithStatus | null = null;
  selectedFollowerJson = '';

  progressVisible = false;
  progressLoading = false;
  progressError = '';
  progressData: any = null;
  progressJson = '';
  progressRow: any = null;

  podVisible = false;
  podLoading = false;
  podError = '';
  selectedPod: Pod | null = null;
  selectedPodJson = '';

  logsVisible = false;
  logsLoading = false;
  logsError = '';
  logsText = '';
  logsNamespace = '';
  logsPodName = '';
  logsContainers: string[] = [];
  logsContainerName = '';
  logsTailLines = 400;

  // Forms
  followerForm!: FormGroup;
  resourceForm!: FormGroup;

  // Data
  availableXStores: XStore[] = [];
  validSourceXStores: XStore[] = []; // Filtered XStores suitable as backup source
  targetPods: Pod[] = [];
  filteredTargetPods: Pod[] = [];
  followers: XStoreFollowerWithStatus[] = [];
  
  // Table configuration
  displayedColumns: string[] = [
    'name', 
    'xStoreName', 
    'phase', 
    'progress', 
    'lastActivity', 
    'status',
    'quick',
    'actions'
  ];

  // CPU and Memory options
  cpuOptions = ['0.5', '1', '2', '4', '8'];
  memoryOptions = ['1Gi', '2Gi', '4Gi', '8Gi', '16Gi'];
  storageOptions = ['10Gi', '20Gi', '50Gi', '100Gi', '200Gi'];

  constructor() {
    this.initializeForms();
  }

  ngOnInit(): void {
    this.loadInitialData();
    if (this.data.mode === 'edit' && this.data.follower) {
      this.populateFormFromFollower(this.data.follower);
    }
  }

  goToXStores(): void {
    this.router.navigate(['/storage/xstores']);
  }

  private initializeForms(): void {
    // Main follower configuration form
    this.followerForm = this.fb.group({
      name: ['', [
        Validators.required, 
        Validators.pattern(/^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/)
      ]],
      namespace: [this.data.namespace || 'default', Validators.required],
      xStoreName: ['', Validators.required],
      targetPodName: ['', Validators.required],
      fromXStore: [''],
      fromBackupSet: [''],
      forceRecreate: [false],
      priority: [1, [Validators.min(1), Validators.max(10)]]
    });

    // Resource configuration form
    this.resourceForm = this.fb.group({
      enableResourceLimits: [false],
      requestsCpu: ['1'],
      requestsMemory: ['2Gi'],
      requestsStorage: ['10Gi'],
      limitsCpu: ['2'],
      limitsMemory: ['4Gi'],
      limitsStorage: ['20Gi'],
      nodeSelector: this.fb.group({
        enabled: [false],
        key: [''],
        value: ['']
      })
    });

    // Watch for form changes
    this.resourceForm.get('enableResourceLimits')?.valueChanges.subscribe(enabled => {
      this.updateResourceValidators(enabled);
    });

    // Load target pods when xStoreName changes
    this.followerForm.get('xStoreName')?.valueChanges.subscribe((x: string) => {
      if (x) {
        this.loadTargetPods(x);
      } else {
        this.targetPods = [];
        this.filteredTargetPods = [];
        this.followerForm.patchValue({ targetPodName: '' });
      }
    });

    // Reload XStores when namespace changes
    this.followerForm.get('namespace')?.valueChanges.subscribe(async (ns: string) => {
      await this.onNamespaceChange(ns);
    });
  }

  private updateResourceValidators(enabled: boolean): void {
    const resourceFields = [
      'requestsCpu', 'requestsMemory', 'requestsStorage',
      'limitsCpu', 'limitsMemory', 'limitsStorage'
    ];

    resourceFields.forEach(field => {
      const control = this.resourceForm.get(field);
      if (enabled) {
        control?.setValidators([Validators.required]);
      } else {
        control?.clearValidators();
      }
      control?.updateValueAndValidity();
    });
  }

  private async loadInitialData(): Promise<void> {
    this.isLoading = true;
    try {
      // Load available XStores using current namespace from form (fallback to dialog data -> default)
      const ns = this.followerForm?.get('namespace')?.value || this.data.namespace || 'default';
      const xstoresRaw = await this.apiService.getXStores(ns).toPromise();
      this.availableXStores = this.normalizeList<XStore>(xstoresRaw);
      
      // Filter valid source XStores
      await this.filterValidSourceXStores();
      
      // Load existing followers
      await this.loadFollowers();

      // If form already has xStoreName (edit mode), load pods
      const x = this.followerForm.get('xStoreName')?.value;
      if (x) {
        await this.loadTargetPods(x);
      }
    } catch (error) {
      console.error('Failed to load initial data:', error);
      this.message.warning('初始化数据加载失败：无法获取 XStore 列表，可手动输入 XStore 名称继续');
    } finally {
      this.isLoading = false;
    }
  }

  private async loadTargetPods(xstoreName: string): Promise<void> {
    try {
      const ns = this.followerForm.get('namespace')?.value || this.data.namespace || 'default';
      const podsRaw = await this.apiService.getXStorePods(ns, xstoreName).toPromise();
      this.targetPods = this.normalizeList<Pod>(podsRaw);
      // Prefer running and non-leader
      this.filteredTargetPods = (this.targetPods || []).filter(p => {
        const phase = (p as any)?.status?.phase;
        const role = (p as any)?.metadata?.labels?.['xstore/role'];
        return phase === 'Running' && role !== 'leader';
      });
      if (this.filteredTargetPods.length === 0) {
        this.filteredTargetPods = (this.targetPods || []).filter(p => (p as any)?.status?.phase === 'Running');
      }
      // Auto-select single candidate
      if (this.filteredTargetPods.length === 1 && !this.followerForm.get('targetPodName')?.value) {
        this.followerForm.patchValue({ targetPodName: (this.filteredTargetPods[0] as any)?.metadata?.name });
      }
    } catch (err) {
      console.error('Failed to load target pods for xstore:', xstoreName, err);
      this.targetPods = [];
      this.filteredTargetPods = [];
    }
  }

  private async loadFollowers(): Promise<void> {
    try {
      const ns = this.followerForm?.get('namespace')?.value || this.data.namespace || 'default';
      const followersRaw = await this.apiService.getXStoreFollowers(ns).toPromise();
      const followers = this.normalizeList<XStoreFollower>(followersRaw);
      this.followers = followers.map((follower: XStoreFollower) => this.enrichFollowerWithStatus(follower));
    } catch (error) {
      console.error('Failed to load XStore followers:', error);
    }
  }

  private async filterValidSourceXStores(): Promise<void> {
    this.validSourceXStores = [];
    
    console.log(`Starting XStore filtering. Total XStores: ${this.availableXStores.length}`, this.availableXStores);
    
    for (const xstore of this.availableXStores) {
      const isValid = await this.isValidSourceXStore(xstore);
      console.log(`XStore ${xstore.metadata.name}: ${isValid ? 'VALID' : 'INVALID'} (phase: ${xstore.status?.phase})`);
      
      if (isValid) {
        this.validSourceXStores.push(xstore);
      }
    }
    
    console.log(`✅ Filtered ${this.validSourceXStores.length} valid source XStores:`, this.validSourceXStores.map(x => x.metadata.name));
    
    // Fallback: if no valid sources found, include all Running XStores
    if (this.validSourceXStores.length === 0) {
      console.warn('⚠️ No valid sources found, falling back to all Running XStores');
      this.validSourceXStores = this.availableXStores.filter(x => x.status?.phase === 'Running');
      console.log(`Fallback: Added ${this.validSourceXStores.length} Running XStores:`, this.validSourceXStores.map(x => x.metadata.name));
    }
    
    // Auto-select first valid source if available
    if (this.validSourceXStores.length > 0 && this.data.mode === 'create') {
      const autoSelected = this.validSourceXStores[0].metadata.name;
      this.followerForm.patchValue({ fromXStore: autoSelected });
      this.followerForm.get('fromXStore')?.setValue(autoSelected);
      this.followerForm.get('fromXStore')?.markAsDirty();
      this.followerForm.get('fromXStore')?.updateValueAndValidity();
    }
  }

  private async isValidSourceXStore(xstore: XStore): Promise<boolean> {
    try {
      if (xstore.status?.phase !== 'Running') {
        return false;
      }
      const replicaStatus = (xstore.status as any)?.replicaStatus;
      if (replicaStatus) {
        const readyReplicas = replicaStatus.ready || replicaStatus.available || 0;
        const totalReplicas = replicaStatus.total || 0;
        if (readyReplicas === 0 || totalReplicas === 0) {
          return false;
        }
        return true;
      }
      return true;
    } catch (error) {
      return false;
    }
  }

  private enrichFollowerWithStatus(follower: XStoreFollower): XStoreFollowerWithStatus {
    const status = follower.status as any;
    const phase = status?.phase || '';
    const isRecovering = ['FollowerPhaseCheck', 'FollowerPhaseBackupPrepare', 'FollowerPhaseBackupStart', 
                         'FollowerPhaseBackup', 'FollowerPhaseLoggerRebuild', 'FollowerPhaseMonitorBackup',
                         'FollowerPhaseBeforeRestore', 'FollowerPhaseRestore', 'FollowerPhaseAfterRestore',
                         'FollowerPhaseLoggerCreate', 'FollowerCreateRemotePod'].includes(phase);
    const isHealthy = phase === 'FollowerPhaseSuccess';
    const hasFailed = phase === 'FollowerPhaseFailed';
    return {
      ...follower,
      isRecovering,
      isHealthy,
      hasFailures: hasFailed,
      displayStatus: this.getDisplayStatus(phase),
      lastActivity: follower.metadata.creationTimestamp
    };
  }

  private getDisplayStatus(phase?: string): string {
    const p = phase || '';
    const statusMap: { [key: string]: string } = {
      '': '初始化中',
      'FollowerPhaseNew': '已创建',
      'FollowerPhaseCheck': '检查中',
      'FollowerPhaseBackupPrepare': '备份准备',
      'FollowerPhaseBackupStart': '开始备份',
      'FollowerPhaseBackup': '备份中',
      'FollowerPhaseLoggerCreate': '创建日志器',
      'FollowerPhaseLoggerRebuild': '重建日志',
      'FollowerCreateRemotePod': '创建远程Pod',
      'FollowerPhaseMonitorBackup': '监控备份',
      'FollowerPhaseBeforeRestore': '准备恢复',
      'FollowerPhaseRestore': '恢复中',
      'FollowerPhaseAfterRestore': '完成恢复',
      'FollowerPhaseWaitSwitch': '等待切换',
      'FollowerPhaseSuccess': '成功',
      'FollowerPhaseFailed': '失败',
      'FollowerPhaseDeleting': '删除中'
    };
    return statusMap[p] || '初始化中';
  }

  getPhaseTooltip(follower: any): string {
    const s = follower.status as any;
    const phase = s?.phase || '';
    const displayPhase = phase || '初始化中';
    const task = s?.currentJobTask ? `，任务：${s.currentJobTask}` : '';
    const message = s?.message ? `，消息：${s.message}` : '';
    return `阶段：${this.getDisplayStatus(phase)}${phase ? `（${phase}）` : ''}${task}${message}`;
  }

  getTargetPodName(follower: XStoreFollower): string {
    return (follower.status as any)?.targetPodName || (follower.spec as any)?.targetPodName || '';
  }

  getFollowerRoleLabel(role?: string): string {
    const r = (role || 'follower').toString().toLowerCase();
    switch (r) {
      case 'learner':
        return 'Learner';
      case 'logger':
        return 'Logger';
      case 'follower':
        return 'Follower';
      default:
        return role || 'Follower';
    }
  }

  getFollowerRoleColor(role?: string): string {
    const r = (role || 'follower').toString().toLowerCase();
    switch (r) {
      case 'learner':
        return 'purple';
      case 'logger':
        return 'cyan';
      case 'follower':
        return 'blue';
      default:
        return 'default';
    }
  }

  getStatusChipClass(phase?: string): string {
    switch (phase) {
      case 'FollowerPhaseSuccess':
        return 'success';
      case 'FollowerPhaseFailed':
        return 'error';
      case 'FollowerPhaseRestore':
      case 'FollowerPhaseCheck':
      case 'FollowerPhaseMonitorBackup':
      case 'FollowerPhaseLoggerRebuild':
      case 'FollowerPhaseBeforeRestore':
      case 'FollowerPhaseBackup':
      case 'FollowerPhaseBackupStart':
      case 'FollowerPhaseBackupPrepare':
      case 'FollowerPhaseLoggerCreate':
      case 'FollowerCreateRemotePod':
        return 'info';
      case 'FollowerPhaseDeleting':
      case 'FollowerPhaseWaitSwitch':
        return 'warning';
      default:
        return 'pending';
    }
  }

  resetForm(): void {
    this.followerForm.reset();
    this.initializeForms();
  }

  save(): void {
    if (this.followerForm.valid) {
      this.isLoading = true;
      const formValue = this.followerForm.value;
      const request: CreateXStoreFollowerRequest = {
        name: formValue.name,
        xStoreName: formValue.xStoreName,
        targetPodName: formValue.targetPodName,
        fromXStore: formValue.fromXStore
      };
      this.apiService.createXStoreFollower('default', request).subscribe({
        next: (result) => {
          this.message.success('XStore Follower 创建成功');
          this.isLoading = false;
          this.refreshFollowers();
          this.selectedTab = 0;
        },
        error: (error) => {
          this.message.error('创建失败: ' + error.message);
          this.isLoading = false;
        }
      });
    }
  }

  private populateFormFromFollower(follower: XStoreFollower): void {
    this.followerForm.patchValue({
      name: follower.metadata.name,
      namespace: follower.metadata.namespace,
      xStoreName: follower.spec.xStoreName,
      targetPodName: (follower.spec as any)?.targetPodName || '',
      fromXStore: follower.spec.fromXStore,
      fromBackupSet: follower.spec.fromBackupSet,
      forceRecreate: follower.spec.forceRecreate,
      priority: follower.spec.priority || 1
    });
    const resources = follower.spec.resources;
    if (resources) {
      this.resourceForm.patchValue({
        enableResourceLimits: true,
        requestsCpu: resources.requests?.cpu || '1',
        requestsMemory: resources.requests?.memory || '2Gi',
        requestsStorage: resources.requests?.storage || '10Gi',
        limitsCpu: resources.limits?.cpu || '2',
        limitsMemory: resources.limits?.memory || '4Gi',
        limitsStorage: resources.limits?.storage || '20Gi'
      });
    }
    const nodeSelector = follower.spec.nodeSelector;
    if (nodeSelector && Object.keys(nodeSelector).length > 0) {
      const firstKey = Object.keys(nodeSelector)[0];
      this.resourceForm.patchValue({
        nodeSelector: {
          enabled: true,
          key: firstKey,
          value: nodeSelector[firstKey]
        }
      });
    }
  }

  async saveFollower(): Promise<void> {
    if (!this.followerForm.valid) {
      return;
    }
    this.isProcessing = true;
    try {
      const formValue = this.followerForm.value;
      const resourceValue = this.resourceForm.value;
      const request: CreateXStoreFollowerRequest = {
        name: formValue.name,
        xStoreName: formValue.xStoreName,
        targetPodName: formValue.targetPodName && formValue.targetPodName.trim() !== '' ? formValue.targetPodName : undefined,
        ['fromXStore']: formValue.fromXStore && formValue.fromXStore.trim() !== '' ? formValue.fromXStore : undefined,
        ['fromBackupSet']: formValue.fromBackupSet && formValue.fromBackupSet.trim() !== '' ? formValue.fromBackupSet : undefined,
        ['forceRecreate']: formValue.forceRecreate,
        ['priority']: formValue.priority,
        ['resources']: resourceValue.enableResourceLimits ? {
          requests: { cpu: resourceValue.requestsCpu, memory: resourceValue.requestsMemory, storage: resourceValue.requestsStorage },
          limits: { cpu: resourceValue.limitsCpu, memory: resourceValue.limitsMemory, storage: resourceValue.limitsStorage }
        } : undefined,
        ['nodeSelector']: resourceValue.nodeSelector.enabled ? { [resourceValue.nodeSelector.key]: resourceValue.nodeSelector.value } : undefined
      };
      if (this.data.mode === 'create') {
        await this.apiService.createXStoreFollower(formValue.namespace, request).toPromise();
      } else if (this.data.mode === 'edit' && this.data.follower) {
        const updatedFollower: XStoreFollower = {
          ...this.data.follower,
          spec: {
            ...this.data.follower.spec,
            xStoreName: request.xStoreName,
            fromXStore: (request as any)['fromXStore'],
            fromBackupSet: (request as any)['fromBackupSet'],
            forceRecreate: (request as any)['forceRecreate'],
            priority: (request as any)['priority'],
            resources: (request as any)['resources'],
            nodeSelector: (request as any)['nodeSelector']
          }
        };
        await this.apiService.updateXStoreFollower(formValue.namespace, updatedFollower).toPromise();
      }
      const action = this.data.mode === 'create' ? '创建' : '更新';
      this.message.success(`XStore Follower ${action}成功！`);
      if (this.selectedTab === 0) {
        await this.loadFollowers();
      }
    } catch (error) {
      console.error('Failed to save XStore follower:', error);
    } finally {
      this.isProcessing = false;
    }
  }

  async deleteFollower(follower: any): Promise<void> {
    this.modal.confirm({
      nzTitle: '确认删除',
      nzContent: `确定要删除 XStore Follower "${follower.metadata.name}" 吗？此操作不可撤销。`,
      nzOkText: '确定删除',
      nzOkDanger: true,
      nzCancelText: '取消',
      nzOnOk: async () => {
        try {
          await this.apiService.deleteXStoreFollower(follower.metadata.namespace || 'default', follower.metadata.name).toPromise();
          this.message.success('删除成功');
          await this.loadFollowers();
        } catch (error) {
          console.error('Failed to delete XStore follower:', error);
          this.message.error(`删除失败：${this.getErrorMessage(error)}`);
        }
      }
    });
  }

  editFollower(follower: any): void {
    this.data.mode = 'edit';
    this.data.follower = follower;
    this.populateFormFromFollower(follower);
    this.selectedTab = 1;
  }

  viewFollower(follower: any): void {
    this.openFollowerDetails(follower);
  }

  // ===== Quick actions =====
  async viewProgress(row: any): Promise<void> {
    await this.openProgressModal(row);
  }

  async viewPodLogs(row: any): Promise<void> {
    await this.openLogsModal(row);
  }

  async describePod(row: any): Promise<void> {
    await this.openPodModal(row);
  }

  showFailureAdvice(row: any): void {
    this.openFailureAdviceModal(row);
  }

  async refreshFollowers(): Promise<void> {
    this.isLoading = true;
    try {
      await this.loadFollowers();
      this.message.success('列表已刷新');
    } finally {
      this.isLoading = false;
    }
  }

  async refreshXStores(): Promise<void> {
    try {
      this.isLoading = true;
      const ns = this.followerForm?.get('namespace')?.value || this.data.namespace || 'default';
      const xstoresRaw = await this.apiService.getXStores(ns).toPromise();
      this.availableXStores = this.normalizeList<XStore>(xstoresRaw);
      await this.filterValidSourceXStores();
      this.message.success('XStore 列表已刷新');
    } catch (error) {
      console.error('Failed to refresh XStores:', error);
      this.message.error(`刷新 XStore 列表失败：${this.getErrorMessage(error)}`);
    } finally {
      this.isLoading = false;
    }
  }

  private async onNamespaceChange(ns: string): Promise<void> {
    try {
      this.isLoading = true;
      const xstoresRaw = await this.apiService.getXStores(ns || 'default').toPromise();
      this.availableXStores = this.normalizeList<XStore>(xstoresRaw);
      await this.filterValidSourceXStores();
      this.followerForm.patchValue({ xStoreName: '', targetPodName: '' });
      this.targetPods = [];
      this.filteredTargetPods = [];
    } catch (e) {
      console.error('Failed to reload XStores for namespace:', ns, e);
      this.availableXStores = [];
      this.validSourceXStores = [];
      this.message.warning('加载 XStore 列表失败，可手动输入 XStore 名称');
    } finally {
      this.isLoading = false;
    }
  }

  getProgressPercentage(follower: any): number {
    const phase = follower.status?.phase || '';
    const progressMap: { [key: string]: number } = {
      '': 10,
      'FollowerPhaseNew': 10,
      'FollowerPhaseCheck': 15,
      'FollowerPhaseBackupPrepare': 25,
      'FollowerPhaseBackupStart': 35,
      'FollowerPhaseBackup': 50,
      'FollowerPhaseMonitorBackup': 60,
      'FollowerPhaseLoggerCreate': 45,
      'FollowerPhaseBeforeRestore': 65,
      'FollowerCreateRemotePod': 30,
      'FollowerPhaseRestore': 85,
      'FollowerPhaseLoggerRebuild': 70,
      'FollowerPhaseAfterRestore': 95,
      'FollowerPhaseWaitSwitch': 90,
      'FollowerPhaseDeleting': 50,
      'FollowerPhaseSuccess': 100,
      'FollowerPhaseFailed': 0
    };
    return progressMap[phase] ?? 10;
  }

  isFormValid(): boolean { return this.followerForm.valid && (!this.resourceForm.get('enableResourceLimits')?.value || this.resourceForm.valid); }

  cancel(): void { this.selectedTab = 0; }

  getFieldError(formGroup: FormGroup, fieldName: string): string {
    const field = formGroup.get(fieldName);
    if (field?.errors && field.touched) {
      if (field.errors['required']) return '此字段为必填项';
      if (field.errors['pattern']) return '格式不正确，请使用小写字母、数字和连字符';
      if (field.errors['min']) return `最小值为 ${field.errors['min'].min}`;
      if (field.errors['max']) return `最大值为 ${field.errors['max'].max}`;
    }
    return '';
  }

  async createNew(): Promise<void> {
    this.data.mode = 'create';
    this.followerForm.reset({ namespace: this.data.namespace || 'default', priority: 1, forceRecreate: false });
    this.resourceForm.reset({ enableResourceLimits: false, requestsCpu: '1', requestsMemory: '2Gi', requestsStorage: '10Gi', limitsCpu: '2', limitsMemory: '4Gi', limitsStorage: '20Gi', nodeSelector: { enabled: false, key: '', value: '' } });
    await this.filterValidSourceXStores();
    this.selectedTab = 1;
  }

  getTitle(): string { switch (this.data.mode) { case 'create': return '创建 XStore Follower'; case 'edit': return '编辑 XStore Follower'; case 'view': return 'XStore Follower 详情'; default: return 'XStore Follower 管理'; } }

  getXStoreReadyStatus(xstore: XStore): string {
    try {
      const replicaStatus = (xstore.status as any)?.replicaStatus;
      if (replicaStatus) { const ready = replicaStatus.ready || replicaStatus.available || 0; const total = replicaStatus.total || 0; return `${ready}/${total} Ready`; }
      const readyStatus = (xstore.status as any)?.readyStatus; if (readyStatus && typeof readyStatus === 'string') { return `${readyStatus}`; }
      const detailedStatus = (xstore.status as any)?.detailedStatus; if (detailedStatus && detailedStatus.replicaStatus) { const ready = detailedStatus.replicaStatus.ready || detailedStatus.replicaStatus.available || 0; const total = detailedStatus.replicaStatus.total || 0; return `${ready}/${total} Ready`; }
      return 'Available';
    } catch (error) { return '获取失败'; }
  }

  getStatusTagColor(phase?: string): string {
    switch (phase) {
      case 'FollowerPhaseSuccess':
        return 'success';
      case 'FollowerPhaseFailed':
        return 'error';
      case 'FollowerPhaseRestore':
      case 'FollowerPhaseCheck':
      case 'FollowerPhaseMonitorBackup':
      case 'FollowerPhaseLoggerRebuild':
      case 'FollowerPhaseBeforeRestore':
      case 'FollowerPhaseBackup':
      case 'FollowerPhaseBackupStart':
      case 'FollowerPhaseBackupPrepare':
      case 'FollowerPhaseLoggerCreate':
      case 'FollowerCreateRemotePod':
        return 'processing';
      case 'FollowerPhaseDeleting':
      case 'FollowerPhaseWaitSwitch':
        return 'warning';
      default:
        return 'default';
    }
  }

  getXStoreStatusColor(xstore: XStore): string {
    const phase = xstore.status?.phase;
    switch (phase) {
      case 'Running':
        return 'success';
      case 'Creating':
      case 'Updating':
        return 'processing';
      case 'Failed':
        return 'error';
      case 'Deleting':
        return 'warning';
      default:
        return 'default';
    }
  }

  // ============================================================================
  // Modal helpers (aligned UX)
  // ============================================================================

  private getNs(row: any): string {
    return (row?.metadata?.namespace || this.followerForm?.get('namespace')?.value || this.data.namespace || 'default') as string;
  }

  private getFollowerKey(row: any): string {
    return `${this.getNs(row)}/${row?.metadata?.name || ''}`;
  }

  private getErrorMessage(error: any): string {
    return (
      error?.error?.message ||
      error?.error?.error ||
      error?.message ||
      (typeof error === 'string' ? error : '未知错误')
    );
  }

  private stringify(obj: any): string {
    try {
      return JSON.stringify(obj, null, 2);
    } catch {
      return String(obj ?? '');
    }
  }

  private getTargetPodFromRow(row: any): string {
    return (
      ((row as any)?.status?.targetPodName || (row as any)?.status?.targetPod || (row as any)?.spec?.targetPodName || '') as string
    ).trim();
  }

  openFollowerDetails(follower: any): void {
    this.selectedFollower = follower as XStoreFollowerWithStatus;
    this.selectedFollowerJson = this.stringify(follower);
    this.detailsVisible = true;
  }

  closeFollowerDetails(): void {
    this.detailsVisible = false;
    this.selectedFollower = null;
    this.selectedFollowerJson = '';
  }

  goToTaskDetail(row: any): void {
    const ns = this.getNs(row);
    const name = row?.metadata?.name;
    if (!name) return;
    this.closeFollowerDetails();
    this.router.navigate(['/storage/xstore-rebuild/rebuild/tasks', ns, name]);
  }

  async openProgressModal(row: any): Promise<void> {
    this.progressRow = row;
    this.progressVisible = true;
    this.progressLoading = true;
    this.progressError = '';
    this.progressData = null;
    this.progressJson = '';

    try {
      const ns = this.getNs(row);
      const xname = row?.spec?.xStoreName || this.selectedFollower?.spec?.xStoreName;
      const name = row?.metadata?.name || this.selectedFollower?.metadata?.name;
      const res = await this.apiService.getRebuildProgress(ns, xname, name).toPromise();
      this.progressData = res || {};
      this.progressJson = this.stringify(res || {});
    } catch (e) {
      this.progressError = this.getErrorMessage(e);
    } finally {
      this.progressLoading = false;
    }
  }

  closeProgressModal(): void {
    this.progressVisible = false;
    this.progressLoading = false;
    this.progressError = '';
    this.progressData = null;
    this.progressJson = '';
    this.progressRow = null;
  }

  async refreshProgress(): Promise<void> {
    if (this.progressLoading) return;
    const row = this.progressRow || this.selectedFollower;
    if (!row) return;
    await this.openProgressModal(row);
  }

  async openPodModal(row: any): Promise<void> {
    const ns = this.getNs(row);
    const podName = this.getTargetPodFromRow(row);
    if (!podName) {
      this.message.warning('未能确定目标 Pod');
      return;
    }

    this.podVisible = true;
    this.podLoading = true;
    this.podError = '';
    this.selectedPod = null;
    this.selectedPodJson = '';

    try {
      const p = await this.apiService.getPod(ns, podName).toPromise();
      this.selectedPod = p as any;
      this.selectedPodJson = this.stringify(p);
    } catch (e) {
      this.podError = this.getErrorMessage(e);
    } finally {
      this.podLoading = false;
    }
  }

  closePodModal(): void {
    this.podVisible = false;
    this.podLoading = false;
    this.podError = '';
    this.selectedPod = null;
    this.selectedPodJson = '';
  }

  async openLogsModal(row: any): Promise<void> {
    const ns = this.getNs(row);
    const podName = this.getTargetPodFromRow(row);
    if (!podName) {
      this.message.warning('未能确定目标 Pod');
      return;
    }

    this.logsVisible = true;
    this.logsNamespace = ns;
    this.logsPodName = podName;
    this.logsError = '';
    this.logsText = '';
    this.logsContainers = [];
    this.logsContainerName = '';

    // Load pod -> containers
    try {
      const podObj = await this.apiService.getPod(ns, podName).toPromise();
      const containers: string[] = (((podObj as any)?.spec?.containers) || []).map((c: any) => c?.name).filter(Boolean);
      this.logsContainers = containers;
      if (containers.length === 1) {
        this.logsContainerName = containers[0];
        await this.refreshLogs();
      } else if (containers.length > 1) {
        this.logsContainerName = containers[0];
      }
    } catch (e) {
      this.logsError = `获取 Pod 容器列表失败：${this.getErrorMessage(e)}`;
    }
  }

  closeLogsModal(): void {
    this.logsVisible = false;
    this.logsLoading = false;
    this.logsError = '';
    this.logsText = '';
    this.logsNamespace = '';
    this.logsPodName = '';
    this.logsContainers = [];
    this.logsContainerName = '';
  }

  async refreshLogs(): Promise<void> {
    if (!this.logsNamespace || !this.logsPodName) return;
    if (!this.logsContainerName && this.logsContainers.length > 1) {
      this.message.warning('请选择容器');
      return;
    }
    this.logsLoading = true;
    this.logsError = '';
    try {
      const txt = await this.apiService.getPodLogs(this.logsNamespace, this.logsPodName, this.logsContainerName || '', this.logsTailLines).toPromise();
      this.logsText = txt || '';
      if (!this.logsText) {
        this.message.info('日志为空');
      }
    } catch (e) {
      this.logsError = this.getErrorMessage(e);
    } finally {
      this.logsLoading = false;
    }
  }

  async copyText(text: string): Promise<void> {
    const t = (text || '').toString();
    if (!t) {
      this.message.info('没有可复制的内容');
      return;
    }
    try {
      if (navigator?.clipboard?.writeText) {
        await navigator.clipboard.writeText(t);
        this.message.success('已复制到剪贴板');
        return;
      }
    } catch {
      // fall through to legacy
    }
    try {
      const ta = document.createElement('textarea');
      ta.value = t;
      ta.style.position = 'fixed';
      ta.style.left = '-10000px';
      ta.style.top = '-10000px';
      document.body.appendChild(ta);
      ta.focus();
      ta.select();
      const ok = document.execCommand('copy');
      document.body.removeChild(ta);
      if (ok) this.message.success('已复制到剪贴板');
      else this.message.error('复制失败');
    } catch (e) {
      this.message.error(`复制失败：${this.getErrorMessage(e)}`);
    }
  }

  openFailureAdviceModal(row: any): void {
    const phase = row?.status?.phase || '';
    if (phase !== 'FollowerPhaseFailed') {
      this.message.warning('该任务未处于失败状态');
      return;
    }
    const ns = this.getNs(row);
    const name = row?.metadata?.name;
    const msg = (row?.status as any)?.message || '';

    this.modal.info({
      nzTitle: `失败分析：${name || ''}`,
      nzWidth: 760,
      nzContent: `
失败原因：
${msg || '未知'}

建议动作：
- 点击“查看进度”确认卡在哪个阶段
- 点击“Pod 描述 / Pod 日志”查看容器状态与错误堆栈
- 如需重试：先确认资源/存储/网络正常，再执行“重试任务”
`,
      nzOkText: '关闭',
      nzOnOk: () => {}
    });
  }

  canRetry(row: any): boolean {
    return (row?.status?.phase || '') === 'FollowerPhaseFailed';
  }

  canCancel(row: any): boolean {
    const phase = (row?.status?.phase || '') as string;
    if (!phase) return true;
    return !['FollowerPhaseSuccess', 'FollowerPhaseFailed', 'FollowerPhaseDeleting'].includes(phase);
  }

  retryFollower(row: any): void {
    const ns = this.getNs(row);
    const name = row?.metadata?.name;
    if (!name) return;
    this.modal.confirm({
      nzTitle: '确认重试',
      nzContent: `确定要重试任务 "${name}" 吗？`,
      nzOkText: '重试',
      nzCancelText: '取消',
      nzOnOk: async () => {
        try {
          await this.apiService.retryXStoreFollower(ns, name).toPromise();
          this.message.success('已触发重试');
          await this.refreshFollowers();
        } catch (e) {
          this.message.error(`重试失败：${this.getErrorMessage(e)}`);
          throw e;
        }
      }
    });
  }

  cancelFollower(row: any): void {
    const ns = this.getNs(row);
    const name = row?.metadata?.name;
    if (!name) return;
    this.modal.confirm({
      nzTitle: '确认停止',
      nzContent: `确定要停止任务 "${name}" 吗？（将尝试取消当前重搭流程）`,
      nzOkText: '停止任务',
      nzOkDanger: true,
      nzCancelText: '取消',
      nzOnOk: async () => {
        try {
          await this.apiService.cancelXStoreFollower(ns, name).toPromise();
          this.message.success('已触发停止');
          await this.refreshFollowers();
        } catch (e) {
          this.message.error(`停止失败：${this.getErrorMessage(e)}`);
          throw e;
        }
      }
    });
  }

  // ============================================================================
  // Template helpers (avoid TS syntax in templates)
  // ============================================================================

  getFollowerMessage(f: any): string {
    const msg = f?.status?.message || (f?.status as any)?.message;
    return (msg || '-').toString();
  }

  getFollowerCurrentJob(f: any): string {
    const s = f?.status || {};
    const task = (s as any)?.currentJobTask || (s as any)?.currentJobName;
    return (task || '-').toString();
  }

  getPodPhase(pod: any): string {
    return ((pod as any)?.status?.phase || '-').toString();
  }

  getPodNodeName(pod: any): string {
    return ((pod as any)?.spec?.nodeName || '-').toString();
  }

  getPodIP(pod: any): string {
    return ((pod as any)?.status?.podIP || '-').toString();
  }

  getPodHostIP(pod: any): string {
    return ((pod as any)?.status?.hostIP || '-').toString();
  }

  private normalizeList<T>(raw: any): T[] {
    if (!raw) return [];
    if (Array.isArray(raw)) return raw as T[];
    const items = (raw as any)?.items;
    if (Array.isArray(items)) return items as T[];
    const data = (raw as any)?.data;
    if (Array.isArray(data)) return data as T[];
    return [];
  }
}
