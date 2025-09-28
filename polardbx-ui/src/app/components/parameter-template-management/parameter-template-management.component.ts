import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReactiveFormsModule, FormBuilder, FormGroup, Validators, FormArray, FormsModule } from '@angular/forms';
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
import { NzModalService } from 'ng-zorro-antd/modal';
import { NzModalModule } from 'ng-zorro-antd/modal';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzSwitchModule } from 'ng-zorro-antd/switch';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzCollapseModule } from 'ng-zorro-antd/collapse';
import { NzDropDownModule } from 'ng-zorro-antd/dropdown';
import { NzMenuModule } from 'ng-zorro-antd/menu';
import { NzPageHeaderModule } from 'ng-zorro-antd/page-header';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { Subject } from 'rxjs';
import { EmptyStateComponent } from '../empty-state/empty-state.component';
import { takeUntil, finalize } from 'rxjs/operators';

import { ApiService } from '../../services/api.service';
import { LoadingService, LoadingKeys } from '../../services/loading.service';
import { 
  PolarDBXParameterTemplate, 
  CreateParameterTemplateRequest,
  UpdateParameterTemplateRequest,
  PARAMETER_UNIT_OPTIONS,
  PARAMETER_MODE_OPTIONS,
  NODE_TYPE_OPTIONS
} from '../../models/parameter-template.model';

@Component({
  selector: 'app-parameter-template-management',
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
    NzPageHeaderModule,
    NzGridModule,
    NzModalModule,
    EmptyStateComponent
  ],
  template: `
    <div class="parameter-template-management">
      <nz-page-header [nzGhost]="false" nzTitle="参数模板管理" nzSubtitle="管理 CN、DN 和 GMS 节点的参数模板">
        <nz-page-header-extra>
          <i nz-icon nzType="sliders" class="page-icon"></i>
        </nz-page-header-extra>
      </nz-page-header>

      <nz-tabset class="main-tabs" [(nzSelectedIndex)]="selectedTab" (nzSelectedIndexChange)="onTabChange($event)">
        <!-- 参数模板列表选项卡 -->
        <nz-tab nzTitle="参数模板">
          <div class="tab-content">
            <div class="actions-toolbar">
              <button nz-button nzType="primary" (click)="refreshTemplates()" 
                      [nzLoading]="isLoading('PARAMETER_TEMPLATE_LIST')">
                <i nz-icon nzType="reload"></i>
                刷新
              </button>
              <button nz-button nzType="default" (click)="goToCreateTab()">
                <i nz-icon nzType="plus"></i>
                创建模板
              </button>
            </div>

            <nz-card class="table-card">
              <div class="table-container" *ngIf="!isLoading('PARAMETER_TEMPLATE_LIST'); else loadingTemplate">
                <nz-table [nzData]="parameterTemplates" [nzShowPagination]="false" class="templates-table">
                  <thead>
                    <tr>
                      <th>名称</th>
                      <th>命名空间</th>
                      <th>节点类型</th>
                      <th>参数数量</th>
                      <th>创建时间</th>
                      <th>操作</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr *ngFor="let template of parameterTemplates">
                      <td>
                        <div class="template-name">
                          <i nz-icon nzType="sliders"></i>
                          <span>{{ template.metadata.name }}</span>
                        </div>
                      </td>
                      <td>{{ template.metadata.namespace }}</td>
                      <td>
                        <nz-tag *ngIf="(template.spec?.nodeType?.cn?.paramList?.length || 0) > 0" nzColor="blue">CN</nz-tag>
                        <nz-tag *ngIf="(template.spec?.nodeType?.dn?.paramList?.length || 0) > 0" nzColor="green">DN</nz-tag>
                        <nz-tag *ngIf="(template.spec?.nodeType?.gms?.paramList?.length || 0) > 0" nzColor="orange">GMS</nz-tag>
                      </td>
                      <td>
                        <span>{{ getParameterCount(template) }}</span>
                      </td>
                      <td>{{ formatDate(template.metadata.creationTimestamp) }}</td>
                      <td>
                        <button nz-button nzType="text" nz-dropdown [nzDropdownMenu]="tplMenu" [nzLoading]="isLoading('PARAMETER_TEMPLATE_UPDATE')">
                          <span>操作</span>
                          <i nz-icon nzType="down"></i>
                        </button>
                        <nz-dropdown-menu #tplMenu="nzDropdownMenu">
                          <ul nz-menu>
                            <li nz-menu-item (click)="viewTemplateDetails(template)"><i nz-icon nzType="eye"></i> 查看详情</li>
                            <li nz-menu-item (click)="editTemplate(template)"><i nz-icon nzType="edit"></i> 编辑模板</li>
                            <li nz-menu-item (click)="duplicateTemplate(template)"><i nz-icon nzType="copy"></i> 复制模板</li>
                            <li nz-menu-item (click)="deleteTemplate(template)" class="delete-action"><i nz-icon nzType="delete"></i> 删除模板</li>
                          </ul>
                        </nz-dropdown-menu>
                      </td>
                    </tr>
                  </tbody>
                </nz-table>

                <app-empty-state *ngIf="parameterTemplates.length === 0"
                                 icon="sliders"
                                 title="未找到参数模板"
                                 [hint]="'点击“创建模板”开始使用'"></app-empty-state>
              </div>
            </nz-card>
          </div>
        </nz-tab>

        <!-- 创建/编辑模板选项卡 -->
        <nz-tab nzTitle="创建模板">
          <div class="tab-content">
            <nz-card class="form-card">
              <ng-template #title>
                <span>{{ editingTemplate ? '编辑参数模板' : '创建新参数模板' }}</span>
              </ng-template>
              
              <form nz-form [formGroup]="templateForm" nzLayout="vertical" class="template-form">
                <!-- 基本信息 -->
                <nz-collapse [nzExpandIconPosition]="'end'">
                  <nz-collapse-panel nzHeader="基本信息" [nzActive]="true">
                    <nz-row [nzGutter]="16">
                      <nz-col [nzSpan]="12">
                        <nz-form-item>
                          <nz-form-label nzRequired>模板名称</nz-form-label>
                          <nz-form-control nzErrorTip="请输入模板名称">
                            <input nz-input formControlName="name" placeholder="输入模板名称" />
                          </nz-form-control>
                        </nz-form-item>
                      </nz-col>
                      <nz-col [nzSpan]="12">
                        <nz-form-item>
                          <nz-form-label>命名空间</nz-form-label>
                          <nz-form-control>
                            <input nz-input formControlName="namespace" placeholder="default" />
                          </nz-form-control>
                        </nz-form-item>
                      </nz-col>
                    </nz-row>
                  </nz-collapse-panel>

                  <!-- CN 参数 -->
                  <nz-collapse-panel nzHeader="CN 节点参数">
                    <div formArrayName="cnParams">
                      <div *ngFor="let param of cnParamsArray.controls; let i = index" [formGroupName]="i" class="param-row">
                        <nz-row [nzGutter]="8" nzAlign="middle">
                          <nz-col [nzSpan]="6">
                            <nz-form-item>
                              <nz-form-control>
                                <input nz-input formControlName="name" placeholder="参数名" />
                              </nz-form-control>
                            </nz-form-item>
                          </nz-col>
                          <nz-col [nzSpan]="6">
                            <nz-form-item>
                              <nz-form-control>
                                <input nz-input formControlName="defaultValue" placeholder="参数默认值" />
                              </nz-form-control>
                            </nz-form-item>
                          </nz-col>
                          <nz-col [nzSpan]="4">
                            <nz-form-item>
                              <nz-form-control>
                                <nz-select formControlName="unit" nzPlaceHolder="单位">
                                  <nz-option *ngFor="let unit of parameterUnits" [nzValue]="unit.value" [nzLabel]="unit.label"></nz-option>
                                </nz-select>
                              </nz-form-control>
                            </nz-form-item>
                          </nz-col>
                          <nz-col [nzSpan]="4">
                            <nz-form-item>
                              <nz-form-control>
                                <nz-select formControlName="mode" nzPlaceHolder="模式">
                                  <nz-option *ngFor="let mode of parameterModes" [nzValue]="mode.value" [nzLabel]="mode.label"></nz-option>
                                </nz-select>
                              </nz-form-control>
                            </nz-form-item>
                          </nz-col>
                          <nz-col [nzSpan]="4">
                            <nz-form-item>
                              <nz-form-control nzExtra="是否需要重启">
                                <nz-switch formControlName="restart"></nz-switch>
                              </nz-form-control>
                            </nz-form-item>
                          </nz-col>
                          <nz-col [nzSpan]="4">
                            <button nz-button nzType="text" nzDanger (click)="removeParameter('cn', i)">
                              <i nz-icon nzType="delete"></i>
                            </button>
                          </nz-col>
                        </nz-row>
                      </div>
                      <button nz-button nzType="dashed" (click)="addParameter('cn')" class="add-param-btn">
                        <i nz-icon nzType="plus"></i>
                        添加 CN 参数
                      </button>
                    </div>
                  </nz-collapse-panel>

                  <!-- DN 参数 -->
                  <nz-collapse-panel nzHeader="DN 节点参数">
                    <div formArrayName="dnParams">
                      <div *ngFor="let param of dnParamsArray.controls; let i = index" [formGroupName]="i" class="param-row">
                        <nz-row [nzGutter]="8" nzAlign="middle">
                          <nz-col [nzSpan]="6">
                            <nz-form-item>
                              <nz-form-control>
                                <input nz-input formControlName="name" placeholder="参数名" />
                              </nz-form-control>
                            </nz-form-item>
                          </nz-col>
                          <nz-col [nzSpan]="6">
                            <nz-form-item>
                              <nz-form-control>
                                <input nz-input formControlName="defaultValue" placeholder="参数默认值" />
                              </nz-form-control>
                            </nz-form-item>
                          </nz-col>
                          <nz-col [nzSpan]="4">
                            <nz-form-item>
                              <nz-form-control>
                                <nz-select formControlName="unit" nzPlaceHolder="单位">
                                  <nz-option *ngFor="let unit of parameterUnits" [nzValue]="unit.value" [nzLabel]="unit.label"></nz-option>
                                </nz-select>
                              </nz-form-control>
                            </nz-form-item>
                          </nz-col>
                          <nz-col [nzSpan]="4">
                            <nz-form-item>
                              <nz-form-control>
                                <nz-select formControlName="mode" nzPlaceHolder="模式">
                                  <nz-option *ngFor="let mode of parameterModes" [nzValue]="mode.value" [nzLabel]="mode.label"></nz-option>
                                </nz-select>
                              </nz-form-control>
                            </nz-form-item>
                          </nz-col>
                          <nz-col [nzSpan]="4">
                            <nz-form-item>
                              <nz-form-control nzExtra="是否需要重启">
                                <nz-switch formControlName="restart"></nz-switch>
                              </nz-form-control>
                            </nz-form-item>
                          </nz-col>
                          <nz-col [nzSpan]="4">
                            <button nz-button nzType="text" nzDanger (click)="removeParameter('dn', i)">
                              <i nz-icon nzType="delete"></i>
                            </button>
                          </nz-col>
                        </nz-row>
                      </div>
                      <button nz-button nzType="dashed" (click)="addParameter('dn')" class="add-param-btn">
                        <i nz-icon nzType="plus"></i>
                        添加 DN 参数
                      </button>
                    </div>
                  </nz-collapse-panel>

                  <!-- GMS 参数 -->
                  <nz-collapse-panel nzHeader="GMS 节点参数">
                    <div formArrayName="gmsParams">
                      <div *ngFor="let param of gmsParamsArray.controls; let i = index" [formGroupName]="i" class="param-row">
                        <nz-row [nzGutter]="8" nzAlign="middle">
                          <nz-col [nzSpan]="6">
                            <nz-form-item>
                              <nz-form-control>
                                <input nz-input formControlName="name" placeholder="参数名" />
                              </nz-form-control>
                            </nz-form-item>
                          </nz-col>
                          <nz-col [nzSpan]="6">
                            <nz-form-item>
                              <nz-form-control>
                                <input nz-input formControlName="defaultValue" placeholder="参数默认值" />
                              </nz-form-control>
                            </nz-form-item>
                          </nz-col>
                          <nz-col [nzSpan]="4">
                            <nz-form-item>
                              <nz-form-control>
                                <nz-select formControlName="unit" nzPlaceHolder="单位">
                                  <nz-option *ngFor="let unit of parameterUnits" [nzValue]="unit.value" [nzLabel]="unit.label"></nz-option>
                                </nz-select>
                              </nz-form-control>
                            </nz-form-item>
                          </nz-col>
                          <nz-col [nzSpan]="4">
                            <nz-form-item>
                              <nz-form-control>
                                <nz-select formControlName="mode" nzPlaceHolder="模式">
                                  <nz-option *ngFor="let mode of parameterModes" [nzValue]="mode.value" [nzLabel]="mode.label"></nz-option>
                                </nz-select>
                              </nz-form-control>
                            </nz-form-item>
                          </nz-col>
                          <nz-col [nzSpan]="4">
                            <nz-form-item>
                              <nz-form-control nzExtra="是否需要重启">
                                <nz-switch formControlName="restart"></nz-switch>
                              </nz-form-control>
                            </nz-form-item>
                          </nz-col>
                          <nz-col [nzSpan]="4">
                            <button nz-button nzType="text" nzDanger (click)="removeParameter('gms', i)">
                              <i nz-icon nzType="delete"></i>
                            </button>
                          </nz-col>
                        </nz-row>
                      </div>
                      <button nz-button nzType="dashed" (click)="addParameter('gms')" class="add-param-btn">
                        <i nz-icon nzType="plus"></i>
                        添加 GMS 参数
                      </button>
                    </div>
                  </nz-collapse-panel>
                </nz-collapse>

                <div class="form-actions">
                  <button nz-button nzType="default" (click)="resetForm()" [nzLoading]="isLoading('PARAMETER_TEMPLATE_CREATE')">
                    重置
                  </button>
                  <button nz-button nzType="primary" 
                          (click)="submitTemplate()" 
                          [nzLoading]="isLoading('PARAMETER_TEMPLATE_CREATE')"
                          [disabled]="templateForm.invalid">
                    <i nz-icon [nzType]="editingTemplate ? 'save' : 'plus'"></i>
                    {{ editingTemplate ? '更新模板' : '创建模板' }}
                  </button>
                </div>
              </form>
            </nz-card>
          </div>
        </nz-tab>
      </nz-tabset>
    </div>

    <!-- 加载模板 -->
    <ng-template #loadingTemplate>
      <div class="loading-container">
        <nz-spin nzSize="large">
          <p>正在加载参数模板...</p>
        </nz-spin>
      </div>
    </ng-template>
  `,
  styles: [`
    .parameter-template-management { 
      padding: 16px 24px; 
      background: #f5f5f5; 
      min-height: 100vh; 
    }
    
    .page-icon { 
      font-size: 16px; 
      color: #1890ff; 
    }
    
    .main-tabs {
      margin-top: 16px;
      background: #fff;
      border-radius: 8px;
      padding: 16px;
      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06);
    }
    
    .tab-content {
      padding: 16px 0;
    }
    
    .actions-toolbar { 
      display: flex; 
      gap: 12px; 
      margin-bottom: 16px; 
    }
    
    .table-card, .form-card {
      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06);
      border-radius: 8px;
    }
    
    .templates-table { 
      width: 100%; 
    }
    
    .templates-table th {
      background: #fafafa;
      font-weight: 600;
      color: #262626;
    }
    
    .templates-table td {
      border-bottom: 1px solid #f0f0f0;
    }
    
    .template-name {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    
    .template-name i {
      color: #1890ff;
    }
    
    .template-form {
      margin-top: 16px;
    }
    
    .param-row {
      margin-bottom: 12px;
      padding: 12px;
      background: #fafafa;
      border-radius: 6px;
    }
    
    .add-param-btn {
      width: 100%;
      margin-top: 8px;
    }
    
    .form-actions { 
      display: flex; 
      gap: 12px; 
      justify-content: flex-end;
      margin-top: 24px;
      padding-top: 16px;
      border-top: 1px solid #f0f0f0;
    }
    
    .loading-container {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      padding: 80px 20px;
    }
    
    .loading-container p {
      margin-top: 16px;
      color: #666;
    }
    
    .delete-action {
      color: #ff4d4f !important;
    }
  `]
})
export class ParameterTemplateManagementComponent implements OnInit, OnDestroy {
  private destroy$ = new Subject<void>();
  
  parameterTemplates: PolarDBXParameterTemplate[] = [];
  selectedTab = 0;
  editingTemplate: PolarDBXParameterTemplate | null = null;
  
  parameterUnits = PARAMETER_UNIT_OPTIONS;
  parameterModes = PARAMETER_MODE_OPTIONS;
  nodeTypes = NODE_TYPE_OPTIONS;
  
  templateForm: FormGroup;

  constructor(
    private apiService: ApiService,
    private loadingService: LoadingService,
    private fb: FormBuilder,
    private message: NzMessageService,
    private modal: NzModalService
  ) {
    this.templateForm = this.createTemplateForm();
  }

  goToCreateTab(): void {
    this.selectedTab = 1;
    this.message.success('已切换到创建模板');
  }

  ngOnInit(): void {
    this.loadParameterTemplates();
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  private createTemplateForm(): FormGroup {
    return this.fb.group({
      name: ['', [Validators.required]],
      namespace: ['default'],
      cnParams: this.fb.array([]),
      dnParams: this.fb.array([]),
      gmsParams: this.fb.array([])
    });
  }

  get cnParamsArray(): FormArray {
    return this.templateForm.get('cnParams') as FormArray;
  }

  get dnParamsArray(): FormArray {
    return this.templateForm.get('dnParams') as FormArray;
  }

  get gmsParamsArray(): FormArray {
    return this.templateForm.get('gmsParams') as FormArray;
  }

  isLoading(key: keyof typeof LoadingKeys): boolean {
    return this.loadingService.isLoading(LoadingKeys[key]);
  }

  onTabChange(index: number): void {
    this.selectedTab = index;
    if (index === 0) {
      this.editingTemplate = null;
      this.resetForm();
    }
  }

  loadParameterTemplates(): void {
    this.apiService.getParameterTemplates()
      .pipe(
        takeUntil(this.destroy$),
        finalize(() => {})
      )
      .subscribe({
        next: (templates: PolarDBXParameterTemplate[]) => {
          this.parameterTemplates = templates || [];
        },
        error: (error) => {
          console.error('加载参数模板失败:', error);
          this.message.error('加载参数模板失败');
        }
      });
  }

  refreshTemplates(): void {
    this.loadParameterTemplates();
    this.message.success('模板列表已刷新');
  }

  addParameter(nodeType: 'cn' | 'dn' | 'gms'): void {
    const array = nodeType === 'cn' ? this.cnParamsArray : 
                  nodeType === 'dn' ? this.dnParamsArray : this.gmsParamsArray;
    
    const paramGroup = this.fb.group({
      name: [''],
      defaultValue: [''],
      unit: ['STRING'],
      mode: ['readwrite'],
      restart: [false],
      optional: ['']
    });
    
    array.push(paramGroup);
  }

  removeParameter(nodeType: 'cn' | 'dn' | 'gms', index: number): void {
    const array = nodeType === 'cn' ? this.cnParamsArray : 
                  nodeType === 'dn' ? this.dnParamsArray : this.gmsParamsArray;
    array.removeAt(index);
  }

  submitTemplate(): void {
    if (this.templateForm.invalid) return;

    const formValue = this.templateForm.value;
    const templateRequest: CreateParameterTemplateRequest | UpdateParameterTemplateRequest = {
      name: formValue.name,
      namespace: formValue.namespace || 'default',
      templateName: formValue.name,
      cnParams: formValue.cnParams || [],
      dnParams: formValue.dnParams || [],
      gmsParams: formValue.gmsParams || []
    };

    const operation = this.editingTemplate
      ? this.apiService.updateParameterTemplate(this.editingTemplate.metadata?.['namespace']!, {
          metadata: {
            name: this.editingTemplate.metadata.name,
            namespace: this.editingTemplate.metadata.namespace,
            resourceVersion: this.editingTemplate.metadata?.['resourceVersion']
          },
          spec: {
            name: formValue.name,
            nodeType: {
              cn: { name: 'cn', paramList: formValue.cnParams || [] },
              dn: { name: 'dn', paramList: formValue.dnParams || [] },
              gms: (formValue.gmsParams && formValue.gmsParams.length) ? { name: 'gms', paramList: formValue.gmsParams } : undefined
            }
          }
        } as any)
      : this.apiService.createParameterTemplate((templateRequest as CreateParameterTemplateRequest).namespace!, templateRequest as CreateParameterTemplateRequest);

    operation.pipe(
      takeUntil(this.destroy$),
      finalize(() => {})
    ).subscribe({
      next: (template) => {
        const message = this.editingTemplate ? '参数模板更新成功' : '参数模板创建成功';
        this.message.success(message);
        this.resetForm();
        this.selectedTab = 0;
        this.loadParameterTemplates();
      },
      error: (error) => {
        console.error('保存参数模板失败:', error);
        this.message.error('保存参数模板失败');
      }
    });
  }

  editTemplate(template: PolarDBXParameterTemplate): void {
    this.editingTemplate = template;
    
    // 清空现有表单数组
    this.cnParamsArray.clear();
    this.dnParamsArray.clear();
    this.gmsParamsArray.clear();
    
    // 填充表单
    this.templateForm.patchValue({
      name: template.metadata.name,
      namespace: template.metadata.namespace || 'default'
    });
    
    // 填充参数数组
    if (template.spec?.nodeType?.cn?.paramList) {
      template.spec.nodeType.cn.paramList.forEach((param: any) => {
        this.cnParamsArray.push(this.fb.group({
          name: [param.name || ''],
          defaultValue: [param.defaultValue || ''],
          unit: [param.unit || 'STRING'],
          mode: [param.mode || 'readwrite'],
          restart: [!!param.restart],
          optional: [param.optional || '']
        }));
      });
    }
    
    if (template.spec?.nodeType?.dn?.paramList) {
      template.spec.nodeType.dn.paramList.forEach((param: any) => {
        this.dnParamsArray.push(this.fb.group({
          name: [param.name || ''],
          defaultValue: [param.defaultValue || ''],
          unit: [param.unit || 'STRING'],
          mode: [param.mode || 'readwrite'],
          restart: [!!param.restart],
          optional: [param.optional || '']
        }));
      });
    }
    
    if (template.spec?.nodeType?.gms?.paramList) {
      template.spec.nodeType.gms.paramList.forEach((param: any) => {
        this.gmsParamsArray.push(this.fb.group({
          name: [param.name || ''],
          defaultValue: [param.defaultValue || ''],
          unit: [param.unit || 'STRING'],
          mode: [param.mode || 'readwrite'],
          restart: [!!param.restart],
          optional: [param.optional || '']
        }));
      });
    }
    
    this.selectedTab = 1;
  }

  duplicateTemplate(template: PolarDBXParameterTemplate): void {
    this.editTemplate(template);
    this.editingTemplate = null;
    this.templateForm.patchValue({
      name: `${template.metadata.name}-copy`
    });
    this.message.success('已复制模板到编辑表单');
  }

  deleteTemplate(template: PolarDBXParameterTemplate): void {
    if (confirm(`删除参数模板\n\n您确定要删除参数模板 "${template.metadata.name}" 吗？`)) {
      this.apiService.deleteParameterTemplate(template.metadata.namespace!, template.metadata.name)
        .pipe(
          takeUntil(this.destroy$),
          finalize(() => {})
        )
        .subscribe({
          next: () => {
            this.message.success('参数模板删除成功');
            this.loadParameterTemplates();
          },
          error: (error) => {
            console.error('删除参数模板失败:', error);
            this.message.error('删除参数模板失败');
          }
        });
    }
  }

  viewTemplateDetails(template: PolarDBXParameterTemplate): void {
    this.message.info(`模板: ${template.metadata.name}（${template.metadata.namespace}）`);
  }

  resetForm(): void {
    this.editingTemplate = null;
    this.cnParamsArray.clear();
    this.dnParamsArray.clear();
    this.gmsParamsArray.clear();
    this.templateForm.reset({
      name: '',
      namespace: 'default'
    });
  }

  getParameterCount(template: PolarDBXParameterTemplate): number {
    const cnCount = template.spec?.nodeType?.cn?.paramList?.length || 0;
    const dnCount = template.spec?.nodeType?.dn?.paramList?.length || 0;
    const gmsCount = template.spec?.nodeType?.gms?.paramList?.length || 0;
    return cnCount + dnCount + gmsCount;
  }

  formatDate(timestamp?: string): string {
    if (!timestamp) return '-';
    return new Date(timestamp).toLocaleDateString('zh-CN', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit'
    });
  }
}