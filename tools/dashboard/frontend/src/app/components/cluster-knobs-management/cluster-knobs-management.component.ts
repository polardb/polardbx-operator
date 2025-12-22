import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReactiveFormsModule, FormsModule, FormBuilder, FormGroup, Validators, FormArray } from '@angular/forms';
import { NzTabsModule } from 'ng-zorro-antd/tabs';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzSwitchModule } from 'ng-zorro-antd/switch';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzCollapseModule } from 'ng-zorro-antd/collapse';
import { NzDropDownModule } from 'ng-zorro-antd/dropdown';
import { NzMenuModule } from 'ng-zorro-antd/menu';
import { NzBadgeModule } from 'ng-zorro-antd/badge';
import { NzPageHeaderModule } from 'ng-zorro-antd/page-header';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { Subject } from 'rxjs';
import { takeUntil, finalize } from 'rxjs/operators';

import { ApiService } from '../../services/api.service';
import { LoadingService } from '../../services/loading.service';
import { 
  PolarDBXClusterKnobs, 
  PolarDBXClusterKnobsList, 
  CreateClusterKnobsRequest,
  getKnobsByCategory,
  validateKnobValue,
  formatKnobValue,
  getImpactColor,
  getImpactLabel
} from '../../models/cluster-knobs.model';
// Removed ConfirmationDialogComponent import - using native confirm() instead

@Component({
  selector: 'app-cluster-knobs-management',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    FormsModule,
    NzTabsModule,
    NzTableModule,
    NzCardModule,
    NzButtonModule,
    NzIconModule,
    NzInputModule,
    NzSelectModule,
    NzFormModule,
    NzTagModule,
    NzSpinModule,
    NzToolTipModule,
    NzSwitchModule,
    NzDividerModule,
    NzCollapseModule,
    NzDropDownModule,
    NzMenuModule,
    NzBadgeModule,
    NzPageHeaderModule,
    NzGridModule
  ],
  template: `
    <div class="cluster-knobs-management">
      <nz-page-header>
        <nz-page-header-title>
          <i nz-icon nzType="build" nzTheme="outline"></i>
          集群性能调优
        </nz-page-header-title>
        <nz-page-header-subtitle>管理PolarDB-X集群的性能调优参数</nz-page-header-subtitle>
      </nz-page-header>

      <nz-tabset [nzSelectedIndex]="selectedTab" (nzSelectChange)="onTabChange($event)">
        <!-- 调优配置列表 -->
        <nz-tab nzTitle="调优配置列表">
            <div class="tab-content">
              <div class="actions-toolbar">
                <button nz-button nzType="primary" (click)="refreshKnobs()" 
                        [nzLoading]="isLoading('CLUSTER_KNOBS_LIST')">
                  <i nz-icon nzType="reload"></i>
                  刷新
                </button>
                <button nz-button nzType="default" (click)="selectedTab = 1">
                  <i nz-icon nzType="plus"></i>
                  创建调优配置
                </button>
              </div>

              <nz-card class="table-card">
                <div class="table-container" *ngIf="!isLoading('CLUSTER_KNOBS_LIST'); else loadingTemplate">
                  <nz-table [nzData]="clusterKnobs" [nzSize]="'middle'" class="cluster-knobs-table">
                    <thead>
                      <tr>
                        <th>名称</th>
                        <th>命名空间</th>
                        <th>目标集群</th>
                        <th>参数数量</th>
                        <th>版本</th>
                        <th>最后更新</th>
                        <th>操作</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr *ngFor="let knobs of clusterKnobs">
                        <td>{{ knobs.metadata.name }}</td>
                        <td>{{ knobs.metadata.namespace }}</td>
                        <td>{{ knobs.spec?.clusterName || '-' }}</td>
                        <td>
                          <nz-tag [nzColor]="getKnobsCountColor(getKnobsCount(knobs))">
                            {{ getKnobsCount(knobs) }} 个参数
                          </nz-tag>
                        </td>
                        <td>
                          <span class="version-badge">v{{ knobs.status?.version || 0 }}</span>
                        </td>
                        <td>{{ formatDate(knobs.status?.lastUpdated) }}</td>
                        <td>
                          <button nz-button nzType="text" nz-dropdown [nzDropdownMenu]="knobsMenu" 
                                  [nzLoading]="isLoading('CLUSTER_KNOBS_UPDATE')">
                            <span>操作</span>
                            <i nz-icon nzType="down"></i>
                          </button>
                          <nz-dropdown-menu #knobsMenu="nzDropdownMenu">
                            <ul nz-menu>
                              <li nz-menu-item (click)="viewKnobsDetails(knobs)">
                                <i nz-icon nzType="eye"></i> 查看详情
                              </li>
                              <li nz-menu-item (click)="editKnobs(knobs)">
                                <i nz-icon nzType="edit"></i> 编辑配置
                              </li>
                              <li nz-menu-divider></li>
                              <li nz-menu-item (click)="deleteKnobs(knobs)" class="delete-action">
                                <i nz-icon nzType="delete"></i> 删除配置
                              </li>
                            </ul>
                          </nz-dropdown-menu>
                        </td>
                      </tr>
                    </tbody>
                  </nz-table>

                  <div *ngIf="clusterKnobs.length === 0" class="no-data">
                    <i nz-icon nzType="setting" nzTheme="outline"></i>
                    <p>暂无调优配置</p>
                    <button nz-button nzType="primary" (click)="selectedTab = 1">
                      创建第一个调优配置
                    </button>
                  </div>
                </div>

                <ng-template #loadingTemplate>
                  <div class="loading-container">
                    <nz-spin nzSize="large"></nz-spin>
                    <p>加载调优配置中...</p>
                  </div>
                </ng-template>
              </nz-card>
            </div>
        </nz-tab>

        <!-- 创建/编辑调优配置 -->
        <nz-tab nzTitle="调优配置">
            <div class="tab-content">
              <nz-card>
                <div nz-card-extra>
                  <i nz-icon [nzType]="editingKnobs ? 'edit' : 'plus'"></i>
                  {{ editingKnobs ? '编辑调优配置' : '创建调优配置' }}
                </div>
                <p>{{ editingKnobs ? '修改现有的性能调优参数' : '为集群创建新的性能调优配置' }}</p>
                  <form [formGroup]="knobsForm">
                    <!-- 基础信息 -->
                    <div class="form-section">
                      <h3>基础信息</h3>
                      <nz-row [nzGutter]="[16, 16]">
                        <nz-col [nzSpan]="12">
                          <nz-form-item>
                            <nz-form-label nzRequired>配置名称</nz-form-label>
                            <nz-form-control nzExtra="只能包含小写字母、数字和连字符" [nzErrorTip]="getFieldError(knobsForm, 'name')">
                              <input nz-input formControlName="name" placeholder="knobs-config-name" [disabled]="!!editingKnobs" />
                            </nz-form-control>
                          </nz-form-item>
                        </nz-col>
                        <nz-col [nzSpan]="12">
                          <nz-form-item>
                            <nz-form-label nzRequired>命名空间</nz-form-label>
                            <nz-form-control [nzErrorTip]="getFieldError(knobsForm, 'namespace')">
                              <input nz-input formControlName="namespace" placeholder="default" />
                            </nz-form-control>
                          </nz-form-item>
                        </nz-col>
                      </nz-row>

                      <nz-row [nzGutter]="[16, 16]">
                        <nz-col [nzSpan]="24">
                          <nz-form-item>
                            <nz-form-label nzRequired>目标集群</nz-form-label>
                            <nz-form-control nzExtra="要应用调优配置的PolarDB-X集群名称" [nzErrorTip]="getFieldError(knobsForm, 'clusterName')">
                              <input nz-input formControlName="clusterName" placeholder="target-cluster-name" />
                            </nz-form-control>
                          </nz-form-item>
                        </nz-col>
                      </nz-row>
                    </div>

                    <!-- 性能调优参数配置 -->
                    <div class="form-section">
                      <h3>性能调优参数</h3>
                      <div class="knobs-container">
                        <nz-collapse [nzBordered]="false">
                          <nz-collapse-panel *ngFor="let category of knobCategories" 
                                           [nzActive]="category.name === 'connection'"
                                           [nzHeader]="categoryHeader">
                            <ng-template #categoryHeader>
                              <div class="category-header">
                                <i nz-icon [nzType]="getCategoryIcon(category.name)"></i>
                                <span>{{ category.label }}</span>
                                <nz-tag [nzColor]="getCategoryKnobsCount(category.name) > 0 ? 'processing' : 'default'" class="category-badge">
                                  {{ getCategoryKnobsCount(category.name) }}
                                </nz-tag>
                              </div>
                              <p class="category-description">{{ category.description }}</p>
                            </ng-template>

                          <div class="knobs-grid">
                            <div *ngFor="let knob of category.knobs" class="knob-config">
                              <nz-switch
                                [ngModel]="isKnobEnabled(knob.name)"
                                (ngModelChange)="toggleKnob(knob.name, $event)">
                              </nz-switch>
                              <span class="knob-label">{{ knob.label }}</span>
                              
                              <div *ngIf="isKnobEnabled(knob.name)" class="knob-value-config">
                                <nz-form-item class="knob-value-field">
                                  <nz-form-label>{{ knob.label }}值</nz-form-label>
                                  <nz-form-control 
                                    nzExtra="{{ knob.description }} (默认: {{ formatKnobValue(knob.defaultValue) }})"
                                    [nzErrorTip]="getKnobError(knob.name) || ''">
                                    <input nz-input
                                      [value]="getKnobValue(knob.name)"
                                      (input)="setKnobValue(knob.name, $event)"
                                      [placeholder]="String(knob.defaultValue)"
                                      [type]="knob.type === 'number' ? 'number' : 'text'">
                                  </nz-form-control>
                                </nz-form-item>
                                
                                <div class="knob-metadata">
                                  <nz-tag [nzColor]="getImpactColor(knob.impact)" 
                                          [nz-tooltip]="getImpactLabel(knob.impact)">
                                    {{ getImpactLabel(knob.impact) }}
                                  </nz-tag>
                                  <nz-tag [nzColor]="knob.restartRequired ? 'error' : 'success'"
                                          [nz-tooltip]="knob.restartRequired ? '需要重启集群' : '动态生效'">
                                    <i nz-icon [nzType]="knob.restartRequired ? 'reload' : 'thunderbolt'"></i>
                                    {{ knob.restartRequired ? '需重启' : '动态' }}
                                  </nz-tag>
                                </div>
                              </div>
                            </div>
                          </div>
                          </nz-collapse-panel>
                        </nz-collapse>
                      </div>
                    </div>

                    <!-- 自定义参数 -->
                    <div class="form-section">
                      <h3>自定义参数</h3>
                      <nz-card class="custom-knobs-section">
                        <div formArrayName="customKnobs">
                          <nz-row *ngFor="let knobGroup of customKnobsArray.controls; let i = index" 
                               [formGroupName]="i" [nzGutter]="[16, 16]" class="custom-knob-row">
                            <nz-col [nzSpan]="10">
                              <nz-form-item>
                                <nz-form-label>参数名</nz-form-label>
                                <nz-form-control>
                                  <input nz-input formControlName="name" placeholder="custom_parameter_name" />
                                </nz-form-control>
                              </nz-form-item>
                            </nz-col>
                            <nz-col [nzSpan]="10">
                              <nz-form-item>
                                <nz-form-label>参数值</nz-form-label>
                                <nz-form-control>
                                  <input nz-input formControlName="value" placeholder="parameter_value" />
                                </nz-form-control>
                              </nz-form-item>
                            </nz-col>
                            <nz-col [nzSpan]="4">
                              <button nz-button nzType="text" nzDanger type="button" (click)="removeCustomKnob(i)">
                                <i nz-icon nzType="minus-circle"></i>
                              </button>
                            </nz-col>
                          </nz-row>
                        </div>
                        <button nz-button nzType="dashed" type="button" (click)="addCustomKnob()">
                          <i nz-icon nzType="plus-circle"></i>
                          添加自定义参数
                        </button>
                      </nz-card>
                    </div>
                  </form>

                <div class="form-actions">
                  <button nz-button (click)="resetForm()" [nzLoading]="isLoading('CLUSTER_KNOBS_CREATE')">
                    重置
                  </button>
                  <button nz-button nzType="primary" 
                          (click)="submitKnobs()" 
                          [disabled]="knobsForm.invalid || isLoading('CLUSTER_KNOBS_CREATE')"
                          [nzLoading]="isLoading('CLUSTER_KNOBS_CREATE')">
                    <i nz-icon [nzType]="editingKnobs ? 'save' : 'plus'"></i>
                    {{ editingKnobs ? '更新配置' : '创建配置' }}
                  </button>
                </div>
              </nz-card>
            </div>
        </nz-tab>
      </nz-tabset>
    </div>
  `,
  styles: [`
    .cluster-knobs-management {
      padding: 20px;
    }
    
    .header-card {
      margin-bottom: 20px;
    }
    
    .tab-content {
      padding: 20px;
    }
    
    .actions-toolbar {
      display: flex;
      gap: 12px;
      margin-bottom: 20px;
    }
    
    .table-card {
      min-height: 400px;
    }
    
    .cluster-knobs-table {
      width: 100%;
    }
    
    .no-data {
      text-align: center;
      padding: 40px;
      color: #666;
    }
    
    .no-data mat-icon {
      font-size: 48px;
      width: 48px;
      height: 48px;
      margin-bottom: 16px;
    }
    
    .loading-container {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      padding: 40px;
    }
    
    .form-section {
      margin-bottom: 32px;
    }
    
    .form-section h3 {
      margin-bottom: 16px;
      color: #333;
    }
    
    .form-row {
      display: flex;
      gap: 16px;
      margin-bottom: 16px;
    }
    
    .form-row mat-form-field {
      flex: 1;
    }
    
    .knobs-grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 16px;
      margin-top: 16px;
    }
    
    .knob-config {
      border: 1px solid #e0e0e0;
      border-radius: 8px;
      padding: 16px;
    }
    
    .knob-value-config {
      margin-top: 12px;
      display: flex;
      align-items: flex-start;
      gap: 16px;
    }
    
    .knob-value-field {
      flex: 1;
    }
    
    .knob-metadata {
      padding-top: 8px;
    }
    
    .category-badge {
      margin-left: auto;
    }
    
    .custom-knobs-section {
      border: 1px solid #e0e0e0;
      border-radius: 8px;
      padding: 16px;
    }
    
    .custom-knob-row {
      display: flex;
      gap: 16px;
      align-items: center;
      margin-bottom: 16px;
    }
    
    .custom-knob-row mat-form-field {
      flex: 1;
    }
    
    .version-badge {
      background-color: #e3f2fd;
      color: #1976d2;
      padding: 4px 8px;
      border-radius: 12px;
      font-size: 12px;
      font-weight: 500;
    }
    
    .delete-action {
      color: #f44336;
    }
    
    mat-card-title {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    
    mat-expansion-panel-header mat-panel-title {
      display: flex;
      align-items: center;
      gap: 8px;
    }
  `]
})
export class ClusterKnobsManagementComponent implements OnInit, OnDestroy {
  private destroy$ = new Subject<void>();
  
  selectedTab = 0;
  clusterKnobs: PolarDBXClusterKnobs[] = [];
  editingKnobs: PolarDBXClusterKnobs | null = null;
  
  knobsForm: FormGroup;
  knobCategories = [
    { name: 'connection', label: '连接', description: '连接相关参数', knobs: [] as any[] },
    { name: 'memory', label: '内存', description: '内存与缓存参数', knobs: [] as any[] },
    { name: 'query', label: '查询', description: '查询优化相关参数', knobs: [] as any[] },
    { name: 'logging', label: '日志', description: '日志记录及级别', knobs: [] as any[] }
  ];
  enabledKnobs: Set<string> = new Set();
  knobValues: Map<string, string | number> = new Map();
  knobErrors: Map<string, string> = new Map();
  
  displayedColumns: string[] = ['name', 'namespace', 'clusterName', 'knobsCount', 'version', 'lastUpdated', 'actions'];

  constructor(
    private fb: FormBuilder,
    private apiService: ApiService,
    private loadingService: LoadingService,
    private message: NzMessageService
  ) {
    this.knobsForm = this.createKnobsForm();
  }

  ngOnInit(): void {
    this.loadClusterKnobs();
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  private createKnobsForm(): FormGroup {
    return this.fb.group({
      name: ['', [Validators.required, Validators.pattern(/^[a-z0-9-]+$/)]],
      namespace: ['default', [Validators.required]],
      clusterName: ['', [Validators.required]],
      customKnobs: this.fb.array([])
    });
  }

  get customKnobsArray(): FormArray {
    return this.knobsForm.get('customKnobs') as FormArray;
  }

  loadClusterKnobs(): void {
    this.apiService.getClusterKnobsList().pipe(
      takeUntil(this.destroy$)
    ).subscribe({
      next: (response: PolarDBXClusterKnobsList) => {
        this.clusterKnobs = response.items || [];
      },
      error: (error) => {
        console.error('Failed to load cluster knobs:', error);
        this.message.error('加载调优配置失败');
      }
    });
  }

  refreshKnobs(): void {
    this.loadClusterKnobs();
  }

  onTabChange(event: any): void {
    this.selectedTab = event.index;
    if (event.index === 0) {
      this.loadClusterKnobs();
    }
  }

  // Knobs management methods
  isKnobEnabled(knobName: string): boolean {
    return this.enabledKnobs.has(knobName);
  }

  toggleKnob(knobName: string, enabled: boolean): void {
    if (enabled) {
      this.enabledKnobs.add(knobName);
      const knob = undefined as any;
      if (knob) {
        this.knobValues.set(knobName, knob.defaultValue);
      }
    } else {
      this.enabledKnobs.delete(knobName);
      this.knobValues.delete(knobName);
      this.knobErrors.delete(knobName);
    }
  }

  getKnobValue(knobName: string): string | number {
    return this.knobValues.get(knobName) || '';
  }

  setKnobValue(knobName: string, event: any): void {
    const value = event.target.value;
    this.knobValues.set(knobName, value);
    
    // Validate the value
    const knob = undefined as any;
    if (knob) {
      const ok = validateKnobValue(knob as any, value);
      if (ok !== true) {
        this.knobErrors.set(knobName, String((ok as any)?.message || '无效的值'));
      } else {
        this.knobErrors.delete(knobName);
      }
    }
  }

  getKnobError(knobName: string): string | null {
    return this.knobErrors.get(knobName) || null;
  }

  getCategoryKnobsCount(categoryName: string): number {
    const byCat = getKnobsByCategory([] as any);
    const categoryKnobs: any[] = byCat[categoryName] || [];
    return categoryKnobs.filter((knob: any) => this.isKnobEnabled(knob.name)).length;
  }

  getCategoryIcon(categoryName: string): string {
    const icons: Record<string, string> = {
      connection: 'link',
      memory: 'database',
      query: 'search',
      logging: 'file-text'
    };
    return icons[categoryName] || 'setting';
  }

  // Custom knobs management
  addCustomKnob(): void {
    const customKnobGroup = this.fb.group({
      name: ['', [Validators.required]],
      value: ['', [Validators.required]]
    });
    this.customKnobsArray.push(customKnobGroup);
  }

  removeCustomKnob(index: number): void {
    this.customKnobsArray.removeAt(index);
  }

  // Utility methods
  getKnobsCount(knobs: PolarDBXClusterKnobs): number {
    return Object.keys(knobs.spec?.knobs || {}).length;
  }

  getKnobsCountColor(count: number): string {
    if (count === 0) return 'basic';
    if (count <= 5) return 'primary';
    if (count <= 10) return 'accent';
    return 'warn';
  }

  formatDate(dateString?: string): string {
    if (!dateString) return '-';
    return new Date(dateString).toLocaleString('zh-CN');
  }

  formatKnobValue = formatKnobValue;
  getImpactColor = getImpactColor;
  getImpactLabel = getImpactLabel;
  String = String;

  // CRUD operations
  viewKnobsDetails(knobs: PolarDBXClusterKnobs): void {
    // Implementation for viewing knobs details
    console.log('View knobs details:', knobs);
  }

  editKnobs(knobs: PolarDBXClusterKnobs): void {
    this.editingKnobs = knobs;
    this.populateFormWithKnobs(knobs);
    this.selectedTab = 1;
  }

  private populateFormWithKnobs(knobs: PolarDBXClusterKnobs): void {
    this.knobsForm.patchValue({
      name: knobs.metadata.name,
      namespace: knobs.metadata.namespace,
      clusterName: knobs.spec?.clusterName
    });

    // Populate enabled knobs and values
    this.enabledKnobs.clear();
    this.knobValues.clear();
    
    if (knobs.spec?.knobs) {
      Object.entries(knobs.spec.knobs).forEach(([name, value]) => {
        this.enabledKnobs.add(name);
        const v: any = (typeof value === 'number') ? value : String(value ?? '');
        this.knobValues.set(name, v as any);
      });
    }
  }

  deleteKnobs(knobs: PolarDBXClusterKnobs): void {
    if (confirm(`删除调优配置\n\n确定要删除调优配置 "${knobs.metadata.name}" 吗？此操作不可撤销。`)) {
      this.apiService.deleteClusterKnobs(knobs.metadata.namespace!, knobs.metadata.name).pipe(
        takeUntil(this.destroy$)
      ).subscribe({
        next: () => {
          this.message.success('调优配置删除成功');
          this.loadClusterKnobs();
        },
        error: (error) => {
          console.error('Failed to delete cluster knobs:', error);
          this.message.error('删除调优配置失败');
        }
      });
    }
  }

  submitKnobs(): void {
    if (this.knobsForm.invalid) {
      this.markFormGroupTouched(this.knobsForm);
      return;
    }

    const formValue = this.knobsForm.value;
    
    // Build knobs object
    const knobs: Record<string, string | number> = {};
    
    // Add enabled predefined knobs
    this.enabledKnobs.forEach(knobName => {
      const value = this.knobValues.get(knobName);
      if (value !== undefined) {
        knobs[knobName] = value;
      }
    });
    
    // Add custom knobs
    formValue.customKnobs?.forEach((customKnob: any) => {
      if (customKnob.name && customKnob.value) {
        knobs[customKnob.name] = customKnob.value;
      }
    });

    const knobsRequest: CreateClusterKnobsRequest = {
      name: formValue.name,
      namespace: formValue.namespace,
      clusterName: formValue.clusterName,
      knobs: knobs
    };

    const operation = this.editingKnobs
      ? this.apiService.updateClusterKnobs(this.editingKnobs.metadata.namespace!, {
          ...this.editingKnobs,
          spec: {
            clusterName: (knobsRequest as any)['clusterName'],
            knobs: (knobsRequest as any)['knobs']
          }
        })
      : this.apiService.createClusterKnobs((knobsRequest as any)['namespace']!, knobsRequest);

    operation.pipe(
      takeUntil(this.destroy$),
      finalize(() => {})
    ).subscribe({
      next: (response) => {
        const action = this.editingKnobs ? '更新' : '创建';
        this.message.success(`调优配置${action}成功`);
        this.resetForm();
        this.selectedTab = 0;
        this.loadClusterKnobs();
      },
      error: (error) => {
        console.error('Failed to save cluster knobs:', error);
        const action = this.editingKnobs ? '更新' : '创建';
        this.message.error(`${action}调优配置失败`);
      }
    });
  }

  resetForm(): void {
    this.editingKnobs = null;
    this.knobsForm.reset();
    this.knobsForm.patchValue({
      namespace: 'default'
    });
    this.enabledKnobs.clear();
    this.knobValues.clear();
    this.knobErrors.clear();
    this.customKnobsArray.clear();
  }

  isLoading(key: string): boolean {
    return this.loadingService.isLoading(key);
  }

  getFieldError(form: FormGroup, fieldName: string): string {
    const field = form.get(fieldName);
    if (field && field.invalid && (field.dirty || field.touched)) {
      if (field.errors?.['required']) {
        return '此字段为必填项';
      }
      if (field.errors?.['pattern']) {
        return '格式不正确';
      }
    }
    return '';
  }

  private markFormGroupTouched(formGroup: FormGroup): void {
    Object.keys(formGroup.controls).forEach(field => {
      const control = formGroup.get(field);
      control?.markAsTouched({ onlySelf: true });
    });
  }
}