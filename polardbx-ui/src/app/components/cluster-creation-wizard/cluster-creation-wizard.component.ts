import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReactiveFormsModule, FormBuilder, FormGroup, Validators, FormArray } from '@angular/forms';
import { Router } from '@angular/router';
import { MatStepperModule } from '@angular/material/stepper';
import { MatCardModule } from '@angular/material/card';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatChipsModule } from '@angular/material/chips';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatSliderModule } from '@angular/material/slider';
import { MatTabsModule } from '@angular/material/tabs';
import { MatExpansionModule } from '@angular/material/expansion';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatSnackBarModule, MatSnackBar } from '@angular/material/snack-bar';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatDialogModule, MatDialog, MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { Inject, Optional } from '@angular/core';

import { ApiService } from '../../services/api.service';
import { LoadingService, LoadingKeys } from '../../services/loading.service';
import {
  ClusterCreationConfig,
  ClusterTemplate,
  ClusterCreationStep,
  CLUSTER_TEMPLATES,
  CREATION_STEPS,
  RESOURCE_PRESETS,
  STORAGE_CLASSES,
  SERVICE_TYPES
} from '../../models/cluster-creation.model';

@Component({
  selector: 'app-cluster-creation-wizard',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    MatStepperModule,
    MatCardModule,
    MatButtonModule,
    MatIconModule,
    MatInputModule,
    MatSelectModule,
    MatFormFieldModule,
    MatChipsModule,
    MatCheckboxModule,
    MatSliderModule,
    MatTabsModule,
    MatExpansionModule,
    MatTooltipModule,
    MatSnackBarModule,
    MatProgressSpinnerModule,
    MatDialogModule
  ],
  template: `
    <div class="cluster-creation-wizard">
      <div class="wizard-header">
        <mat-card>
          <mat-card-header>
            <mat-card-title>
              <mat-icon class="title-icon">add_circle</mat-icon>
              创建 PolarDB-X 集群
            </mat-card-title>
            <mat-card-subtitle>
              通过向导配置和创建新的 PolarDB-X 分布式数据库集群
            </mat-card-subtitle>
          </mat-card-header>
        </mat-card>
      </div>

      <div class="wizard-content">
        <mat-stepper [linear]="true" #stepper class="cluster-stepper">
          
          <!-- 步骤1: 选择模板 -->
          <mat-step [stepControl]="templateForm" label="选择模板">
            <form [formGroup]="templateForm" class="step-content">
              <div class="step-header">
                <h3>选择集群模板</h3>
                <p>选择适合您用途的预配置模板，或选择自定义配置</p>
              </div>

              <div class="templates-grid">
                <mat-card 
                  *ngFor="let template of clusterTemplates" 
                  class="template-card"
                  [class.selected]="selectedTemplate?.name === template.name"
                  [class.recommended]="template.recommended"
                  (click)="selectTemplate(template)">
                  
                  <mat-card-header>
                    <div class="template-icon">
                      <mat-icon>{{ template.icon }}</mat-icon>
                    </div>
                    <mat-card-title>{{ template.label }}</mat-card-title>
                    <div class="recommended-badge" *ngIf="template.recommended">
                      <mat-icon>star</mat-icon>
                      推荐
                    </div>
                  </mat-card-header>
                  
                  <mat-card-content>
                    <p>{{ template.description }}</p>
                    
                    <div class="template-specs" *ngIf="template.config?.['topology']">
                      <div class="spec-item">
                        <mat-icon>computer</mat-icon>
                        <span>CN: {{ template.config?.['topology']?.cn?.replicas || 0 }}</span>
                      </div>
                      <div class="spec-item">
                        <mat-icon>storage</mat-icon>
                        <span>DN: {{ template.config?.['topology']?.dn?.replicas || 0 }}</span>
                      </div>
                      <div class="spec-item">
                        <mat-icon>hub</mat-icon>
                        <span>GMS: {{ template.config?.['topology']?.gms?.replicas || 0 }}</span>
                      </div>
                    </div>
                  </mat-card-content>
                </mat-card>
              </div>

              <div class="step-actions">
                <button mat-raised-button color="primary" 
                        [disabled]="!selectedTemplate"
                        (click)="nextStep(stepper)">
                  下一步
                </button>
              </div>
            </form>
          </mat-step>

          <!-- 步骤2: 基本信息 -->
          <mat-step [stepControl]="basicForm" label="基本信息">
            <form [formGroup]="basicForm" class="step-content">
              <div class="step-header">
                <h3>基本信息配置</h3>
                <p>配置集群的基本信息，包括名称、命名空间等</p>
              </div>

              <div class="form-grid">
                <mat-form-field appearance="outline" class="full-width">
                  <mat-label>集群名称</mat-label>
                  <input matInput formControlName="name" placeholder="my-polardbx-cluster">
                  <mat-hint>只能包含小写字母、数字和连字符</mat-hint>
                  <mat-error *ngIf="basicForm.get('name')?.hasError('required')">
                    集群名称是必填项
                  </mat-error>
                  <mat-error *ngIf="basicForm.get('name')?.hasError('pattern')">
                    名称格式不正确
                  </mat-error>
                </mat-form-field>

                <mat-form-field appearance="outline" class="half-width">
                  <mat-label>命名空间</mat-label>
                  <mat-select formControlName="namespace">
                    <mat-option value="default">default</mat-option>
                    <mat-option value="polardbx">polardbx</mat-option>
                    <mat-option value="database">database</mat-option>
                  </mat-select>
                </mat-form-field>

                <mat-form-field appearance="outline" class="half-width">
                  <mat-label>版本</mat-label>
                  <mat-select formControlName="version">
                    <mat-option value="8.0.18">8.0.18 (稳定版)</mat-option>
                    <mat-option value="8.0.19">8.0.19 (最新版)</mat-option>
                  </mat-select>
                </mat-form-field>

                <mat-form-field appearance="outline" class="full-width">
                  <mat-label>描述 (可选)</mat-label>
                  <textarea matInput formControlName="description" 
                           placeholder="集群用途和描述信息"
                           rows="3"></textarea>
                </mat-form-field>
              </div>

              <div class="step-actions">
                <button mat-button matStepperPrevious>上一步</button>
                <button mat-raised-button color="primary" 
                        [disabled]="basicForm.invalid"
                        (click)="nextStep(stepper)">
                  下一步
                </button>
              </div>
            </form>
          </mat-step>

          <!-- 步骤3: 拓扑配置 -->
          <mat-step [stepControl]="topologyForm" label="拓扑配置">
            <form [formGroup]="topologyForm" class="step-content">
              <div class="step-header">
                <h3>集群拓扑配置</h3>
                <p>配置计算节点(CN)、数据节点(DN)、全局管理服务(GMS)等组件</p>
              </div>

              <mat-tab-group class="topology-tabs">
                <!-- CN节点配置 -->
                <mat-tab label="计算节点 (CN)">
                  <div class="tab-content" formGroupName="cn">
                    <div class="node-config">
                      <div class="node-header">
                        <mat-icon>computer</mat-icon>
                        <h4>计算节点配置</h4>
                        <p>负责SQL解析、优化和执行协调</p>
                      </div>

                      <div class="form-grid">
                        <mat-form-field appearance="outline">
                          <mat-label>副本数量</mat-label>
                          <input matInput type="number" formControlName="replicas" min="1" max="10">
                        </mat-form-field>

                        <mat-form-field appearance="outline" formGroupName="resources">
                          <mat-label>CPU</mat-label>
                          <mat-select formControlName="cpu">
                            <mat-option value="500m">0.5 核 (开发)</mat-option>
                            <mat-option value="1">1 核 (测试)</mat-option>
                            <mat-option value="2">2 核 (生产)</mat-option>
                            <mat-option value="4">4 核 (高性能)</mat-option>
                            <mat-option value="8">8 核 (企业级)</mat-option>
                          </mat-select>
                        </mat-form-field>

                        <mat-form-field appearance="outline" formGroupName="resources">
                          <mat-label>内存</mat-label>
                          <mat-select formControlName="memory">
                            <mat-option value="1Gi">1 GB (开发)</mat-option>
                            <mat-option value="2Gi">2 GB (测试)</mat-option>
                            <mat-option value="4Gi">4 GB (生产)</mat-option>
                            <mat-option value="8Gi">8 GB (高性能)</mat-option>
                            <mat-option value="16Gi">16 GB (企业级)</mat-option>
                          </mat-select>
                        </mat-form-field>
                      </div>
                    </div>
                  </div>
                </mat-tab>

                <!-- DN节点配置 -->
                <mat-tab label="数据节点 (DN)">
                  <div class="tab-content" formGroupName="dn">
                    <div class="node-config">
                      <div class="node-header">
                        <mat-icon>storage</mat-icon>
                        <h4>数据节点配置</h4>
                        <p>负责数据存储和本地计算</p>
                      </div>

                      <div class="form-grid">
                        <mat-form-field appearance="outline">
                          <mat-label>副本数量</mat-label>
                          <input matInput type="number" formControlName="replicas" min="1" max="10">
                        </mat-form-field>

                        <mat-form-field appearance="outline" formGroupName="resources">
                          <mat-label>CPU</mat-label>
                          <mat-select formControlName="cpu">
                            <mat-option value="500m">0.5 核 (开发)</mat-option>
                            <mat-option value="1">1 核 (测试)</mat-option>
                            <mat-option value="2">2 核 (生产)</mat-option>
                            <mat-option value="4">4 核 (高性能)</mat-option>
                            <mat-option value="8">8 核 (企业级)</mat-option>
                            <mat-option value="16">16 核 (超大规模)</mat-option>
                          </mat-select>
                        </mat-form-field>

                        <mat-form-field appearance="outline" formGroupName="resources">
                          <mat-label>内存</mat-label>
                          <mat-select formControlName="memory">
                            <mat-option value="1Gi">1 GB (开发)</mat-option>
                            <mat-option value="2Gi">2 GB (测试)</mat-option>
                            <mat-option value="4Gi">4 GB (生产)</mat-option>
                            <mat-option value="8Gi">8 GB (高性能)</mat-option>
                            <mat-option value="16Gi">16 GB (企业级)</mat-option>
                            <mat-option value="32Gi">32 GB (超大规模)</mat-option>
                          </mat-select>
                        </mat-form-field>
                      </div>
                    </div>
                  </div>
                </mat-tab>

                <!-- GMS节点配置 -->
                <mat-tab label="管理服务 (GMS)">
                  <div class="tab-content" formGroupName="gms">
                    <div class="node-config">
                      <div class="node-header">
                        <mat-icon>hub</mat-icon>
                        <h4>全局管理服务配置</h4>
                        <p>负责元数据管理和全局协调</p>
                      </div>

                      <div class="form-grid">
                        <mat-form-field appearance="outline">
                          <mat-label>副本数量</mat-label>
                          <input matInput type="number" formControlName="replicas" min="1" max="5">
                        </mat-form-field>

                        <mat-form-field appearance="outline" formGroupName="resources">
                          <mat-label>CPU</mat-label>
                          <mat-select formControlName="cpu">
                            <mat-option value="500m">0.5 核</mat-option>
                            <mat-option value="1">1 核</mat-option>
                            <mat-option value="2">2 核</mat-option>
                            <mat-option value="4">4 核</mat-option>
                          </mat-select>
                        </mat-form-field>

                        <mat-form-field appearance="outline" formGroupName="resources">
                          <mat-label>内存</mat-label>
                          <mat-select formControlName="memory">
                            <mat-option value="1Gi">1 GB</mat-option>
                            <mat-option value="2Gi">2 GB</mat-option>
                            <mat-option value="4Gi">4 GB</mat-option>
                            <mat-option value="8Gi">8 GB</mat-option>
                          </mat-select>
                        </mat-form-field>
                      </div>
                    </div>
                  </div>
                </mat-tab>

                <!-- CDC节点配置 (可选) -->
                <mat-tab label="CDC服务 (可选)">
                  <div class="tab-content">
                    <div class="node-config">
                      <div class="optional-header">
                        <mat-checkbox formControlName="enableCdc">启用 CDC 服务</mat-checkbox>
                        <p>变更数据捕获服务，用于实时数据同步</p>
                      </div>

                      <div *ngIf="topologyForm.get('enableCdc')?.value" formGroupName="cdc">
                        <div class="form-grid">
                          <mat-form-field appearance="outline">
                            <mat-label>副本数量</mat-label>
                            <input matInput type="number" formControlName="replicas" min="1" max="5">
                          </mat-form-field>

                          <mat-form-field appearance="outline" formGroupName="resources">
                            <mat-label>CPU</mat-label>
                            <mat-select formControlName="cpu">
                              <mat-option value="1">1 核</mat-option>
                              <mat-option value="2">2 核</mat-option>
                              <mat-option value="4">4 核</mat-option>
                            </mat-select>
                          </mat-form-field>

                          <mat-form-field appearance="outline" formGroupName="resources">
                            <mat-label>内存</mat-label>
                            <mat-select formControlName="memory">
                              <mat-option value="2Gi">2 GB</mat-option>
                              <mat-option value="4Gi">4 GB</mat-option>
                              <mat-option value="8Gi">8 GB</mat-option>
                            </mat-select>
                          </mat-form-field>
                        </div>
                      </div>
                    </div>
                  </div>
                </mat-tab>
              </mat-tab-group>

              <div class="topology-summary">
                <h4>拓扑总览</h4>
                <div class="summary-cards">
                  <div class="summary-card">
                    <mat-icon>computer</mat-icon>
                    <div class="summary-content">
                      <div class="summary-title">计算节点</div>
                      <div class="summary-value">{{ topologyForm.get('cn.replicas')?.value || 0 }} 个</div>
                    </div>
                  </div>
                  <div class="summary-card">
                    <mat-icon>storage</mat-icon>
                    <div class="summary-content">
                      <div class="summary-title">数据节点</div>
                      <div class="summary-value">{{ topologyForm.get('dn.replicas')?.value || 0 }} 个</div>
                    </div>
                  </div>
                  <div class="summary-card">
                    <mat-icon>hub</mat-icon>
                    <div class="summary-content">
                      <div class="summary-title">管理服务</div>
                      <div class="summary-value">{{ topologyForm.get('gms.replicas')?.value || 0 }} 个</div>
                    </div>
                  </div>
                </div>
              </div>

              <div class="step-actions">
                <button mat-button matStepperPrevious>上一步</button>
                <button mat-raised-button color="primary" 
                        [disabled]="topologyForm.invalid"
                        (click)="nextStep(stepper)">
                  下一步
                </button>
              </div>
            </form>
          </mat-step>

          <!-- 步骤4: 存储配置 -->
          <mat-step [stepControl]="storageForm" label="存储配置">
            <form [formGroup]="storageForm" class="step-content">
              <div class="step-header">
                <h3>存储配置</h3>
                <p>配置数据持久化存储的相关设置</p>
              </div>

              <div class="storage-config">
                <div class="form-grid">
                  <mat-form-field appearance="outline" class="full-width">
                    <mat-label>存储类</mat-label>
                    <mat-select formControlName="storageClassName">
                      <mat-option *ngFor="let storage of storageClasses" [value]="storage.value">
                        <div class="storage-option">
                          <div class="storage-label">{{ storage.label }}</div>
                          <div class="storage-description">{{ storage.description }}</div>
                        </div>
                      </mat-option>
                    </mat-select>
                  </mat-form-field>

                  <mat-form-field appearance="outline" class="half-width">
                    <mat-label>存储大小</mat-label>
                    <mat-select formControlName="size">
                      <mat-option value="10Gi">10 GB</mat-option>
                      <mat-option value="20Gi">20 GB</mat-option>
                      <mat-option value="50Gi">50 GB</mat-option>
                      <mat-option value="100Gi">100 GB</mat-option>
                      <mat-option value="200Gi">200 GB</mat-option>
                      <mat-option value="500Gi">500 GB</mat-option>
                      <mat-option value="1Ti">1 TB</mat-option>
                    </mat-select>
                  </mat-form-field>

                  <mat-form-field appearance="outline" class="half-width">
                    <mat-label>访问模式</mat-label>
                    <mat-select formControlName="accessMode">
                      <mat-option value="ReadWriteOnce">ReadWriteOnce</mat-option>
                      <mat-option value="ReadWriteMany">ReadWriteMany</mat-option>
                    </mat-select>
                  </mat-form-field>
                </div>

                <div class="storage-info">
                  <mat-icon>info</mat-icon>
                  <div class="info-content">
                    <h4>存储建议</h4>
                    <ul>
                      <li><strong>开发环境</strong>: 标准存储 + 20GB 即可满足基本需求</li>
                      <li><strong>测试环境</strong>: 标准存储 + 50GB 用于功能测试</li>
                      <li><strong>生产环境</strong>: 高性能SSD + 200GB+ 保证性能和容量</li>
                      <li><strong>数据节点</strong>: 建议使用高IOPS存储以获得更好的查询性能</li>
                    </ul>
                  </div>
                </div>
              </div>

              <div class="step-actions">
                <button mat-button matStepperPrevious>上一步</button>
                <button mat-raised-button color="primary" 
                        [disabled]="storageForm.invalid"
                        (click)="nextStep(stepper)">
                  下一步
                </button>
              </div>
            </form>
          </mat-step>

          <!-- 步骤5: 网络配置 -->
          <mat-step [stepControl]="networkForm" label="网络配置">
            <form [formGroup]="networkForm" class="step-content">
              <div class="step-header">
                <h3>网络配置</h3>
                <p>配置集群的网络访问方式和安全策略</p>
              </div>

              <div class="network-config">
                <div class="service-types">
                  <h4>服务访问类型</h4>
                  <div class="service-type-cards">
                    <mat-card *ngFor="let serviceType of serviceTypes" 
                             class="service-type-card"
                             [class.selected]="networkForm.get('serviceType')?.value === serviceType.value"
                             (click)="selectServiceType(serviceType.value)">
                      <mat-card-content>
                        <div class="service-type-content">
                          <mat-icon>{{ serviceType.icon }}</mat-icon>
                          <div class="service-type-info">
                            <div class="service-type-title">{{ serviceType.label }}</div>
                            <div class="service-type-description">{{ serviceType.description }}</div>
                          </div>
                        </div>
                      </mat-card-content>
                    </mat-card>
                  </div>
                </div>

                <div class="advanced-network" *ngIf="networkForm.get('serviceType')?.value === 'LoadBalancer'">
                  <mat-expansion-panel>
                    <mat-expansion-panel-header>
                      <mat-panel-title>负载均衡器配置</mat-panel-title>
                    </mat-expansion-panel-header>
                    
                    <div class="form-grid">
                      <mat-form-field appearance="outline">
                        <mat-label>负载均衡器类型</mat-label>
                        <mat-select formControlName="loadBalancerClass">
                          <mat-option value="">默认</mat-option>
                          <mat-option value="nginx">Nginx Ingress</mat-option>
                          <mat-option value="cloud">云服务商LB</mat-option>
                        </mat-select>
                      </mat-form-field>
                    </div>
                  </mat-expansion-panel>
                </div>

                <div class="security-config">
                  <h4>安全配置</h4>
                  <div class="form-grid">
                    <mat-checkbox formControlName="enableTLS">
                      启用 TLS 加密
                    </mat-checkbox>
                    
                    <mat-form-field appearance="outline" class="full-width" 
                                   *ngIf="networkForm.get('enableTLS')?.value">
                      <mat-label>证书密钥名称</mat-label>
                      <input matInput formControlName="tlsSecretName" 
                             placeholder="tls-secret-name">
                      <mat-hint>包含 TLS 证书和私钥的 Secret 名称</mat-hint>
                    </mat-form-field>
                  </div>
                </div>
              </div>

              <div class="step-actions">
                <button mat-button matStepperPrevious>上一步</button>
                <button mat-raised-button color="primary" 
                        [disabled]="networkForm.invalid"
                        (click)="nextStep(stepper)">
                  下一步
                </button>
              </div>
            </form>
          </mat-step>

          <!-- 步骤6: 高级配置 -->
          <mat-step [stepControl]="advancedForm" label="高级配置">
            <form [formGroup]="advancedForm" class="step-content">
              <div class="step-header">
                <h3>高级配置</h3>
                <p>配置监控、备份、日志采集等高级功能</p>
              </div>

              <div class="advanced-options">
                <mat-expansion-panel class="option-panel" [expanded]="true">
                  <mat-expansion-panel-header>
                    <mat-panel-title>功能开关</mat-panel-title>
                  </mat-expansion-panel-header>
                  
                  <div class="option-content">
                    <div class="option-item">
                      <mat-checkbox formControlName="enableMonitoring">
                        启用性能监控
                      </mat-checkbox>
                      <p>集成 Prometheus 和 Grafana 监控</p>
                    </div>
                    
                    <div class="option-item">
                      <mat-checkbox formControlName="enableBackup">
                        启用自动备份
                      </mat-checkbox>
                      <p>配置定期数据备份策略</p>
                    </div>
                    
                    <div class="option-item">
                      <mat-checkbox formControlName="enableLogCollection">
                        启用日志采集
                      </mat-checkbox>
                      <p>收集和聚合集群日志</p>
                    </div>
                  </div>
                </mat-expansion-panel>

                <mat-expansion-panel class="option-panel">
                  <mat-expansion-panel-header>
                    <mat-panel-title>标签和注解</mat-panel-title>
                  </mat-expansion-panel-header>
                  
                  <div class="option-content">
                    <mat-form-field appearance="outline" class="full-width">
                      <mat-label>自定义标签 (JSON格式)</mat-label>
                      <textarea matInput formControlName="customLabels" 
                               placeholder='{"environment": "production", "team": "database"}'
                               rows="3"></textarea>
                    </mat-form-field>
                    
                    <mat-form-field appearance="outline" class="full-width">
                      <mat-label>自定义注解 (JSON格式)</mat-label>
                      <textarea matInput formControlName="customAnnotations" 
                               placeholder='{"description": "Production database cluster"}'
                               rows="3"></textarea>
                    </mat-form-field>
                  </div>
                </mat-expansion-panel>
              </div>

              <div class="step-actions">
                <button mat-button matStepperPrevious>上一步</button>
                <button mat-raised-button color="primary" 
                        [disabled]="advancedForm.invalid"
                        (click)="nextStep(stepper)">
                  下一步
                </button>
              </div>
            </form>
          </mat-step>

          <!-- 步骤7: 确认创建 -->
          <mat-step label="确认创建">
            <div class="step-content">
              <div class="step-header">
                <h3>确认集群配置</h3>
                <p>请检查以下配置信息，确认无误后点击创建按钮</p>
              </div>

              <div class="config-review">
                <mat-card class="review-card">
                  <mat-card-header>
                    <mat-card-title>基本信息</mat-card-title>
                  </mat-card-header>
                  <mat-card-content>
                    <div class="review-item">
                      <span class="label">集群名称:</span>
                      <span class="value">{{ getConfigValue('name') }}</span>
                    </div>
                    <div class="review-item">
                      <span class="label">命名空间:</span>
                      <span class="value">{{ getConfigValue('namespace') }}</span>
                    </div>
                    <div class="review-item">
                      <span class="label">版本:</span>
                      <span class="value">{{ getConfigValue('version') }}</span>
                    </div>
                  </mat-card-content>
                </mat-card>

                <mat-card class="review-card">
                  <mat-card-header>
                    <mat-card-title>拓扑配置</mat-card-title>
                  </mat-card-header>
                  <mat-card-content>
                    <div class="topology-review">
                      <div class="node-review">
                        <mat-icon>computer</mat-icon>
                        <span>CN节点: {{ getTopologyValue('cn', 'replicas') }} 个</span>
                        <span class="resources">{{ getTopologyValue('cn', 'resources.cpu') }} / {{ getTopologyValue('cn', 'resources.memory') }}</span>
                      </div>
                      <div class="node-review">
                        <mat-icon>storage</mat-icon>
                        <span>DN节点: {{ getTopologyValue('dn', 'replicas') }} 个</span>
                        <span class="resources">{{ getTopologyValue('dn', 'resources.cpu') }} / {{ getTopologyValue('dn', 'resources.memory') }}</span>
                      </div>
                      <div class="node-review">
                        <mat-icon>hub</mat-icon>
                        <span>GMS节点: {{ getTopologyValue('gms', 'replicas') }} 个</span>
                        <span class="resources">{{ getTopologyValue('gms', 'resources.cpu') }} / {{ getTopologyValue('gms', 'resources.memory') }}</span>
                      </div>
                    </div>
                  </mat-card-content>
                </mat-card>

                <mat-card class="review-card">
                  <mat-card-header>
                    <mat-card-title>存储和网络</mat-card-title>
                  </mat-card-header>
                  <mat-card-content>
                    <div class="review-item">
                      <span class="label">存储类:</span>
                      <span class="value">{{ getConfigValue('storageClassName') }}</span>
                    </div>
                    <div class="review-item">
                      <span class="label">存储大小:</span>
                      <span class="value">{{ getConfigValue('size') }}</span>
                    </div>
                    <div class="review-item">
                      <span class="label">服务类型:</span>
                      <span class="value">{{ getConfigValue('serviceType') }}</span>
                    </div>
                  </mat-card-content>
                </mat-card>
              </div>

              <div class="creation-status" *ngIf="isCreating">
                <mat-progress-spinner mode="indeterminate"></mat-progress-spinner>
                <p>正在创建集群，请稍候...</p>
              </div>

              <div class="step-actions">
                <button mat-button matStepperPrevious [disabled]="isCreating">上一步</button>
                <button mat-button (click)="cancelCreation()" 
                        [disabled]="isCreating"
                        *ngIf="isInDialog()">
                  <mat-icon>cancel</mat-icon>
                  取消
                </button>
                <button mat-raised-button color="primary" 
                        [disabled]="isCreating"
                        (click)="createCluster()">
                  <mat-icon>add</mat-icon>
                  创建集群
                </button>
              </div>
            </div>
          </mat-step>
        </mat-stepper>
      </div>
    </div>
  `,
  styleUrl: './cluster-creation-wizard.component.scss'
})
export class ClusterCreationWizardComponent implements OnInit, OnDestroy {
  private destroy$ = new Subject<void>();
  
  // 表单组
  templateForm!: FormGroup;
  basicForm!: FormGroup;
  topologyForm!: FormGroup;
  storageForm!: FormGroup;
  networkForm!: FormGroup;
  advancedForm!: FormGroup;
  
  // 数据
  clusterTemplates = CLUSTER_TEMPLATES;
  storageClasses = STORAGE_CLASSES;
  serviceTypes = SERVICE_TYPES;
  
  selectedTemplate: ClusterTemplate | null = null;
  isCreating = false;

  constructor(
    private fb: FormBuilder,
    private apiService: ApiService,
    private loadingService: LoadingService,
    private snackBar: MatSnackBar,
    private dialog: MatDialog,
    private router: Router,
    @Optional() private dialogRef: MatDialogRef<ClusterCreationWizardComponent>
  ) {
    this.initializeForms();
  }

  ngOnInit(): void {
    // 初始化完成
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  private initializeForms(): void {
    this.templateForm = this.fb.group({
      template: ['', Validators.required]
    });

    this.basicForm = this.fb.group({
      name: ['', [Validators.required, Validators.pattern(/^[a-z0-9-]+$/)]],
      namespace: ['default', Validators.required],
      version: ['8.0.18', Validators.required],
      description: ['']
    });

    this.topologyForm = this.fb.group({
      enableCdc: [false],
      cn: this.fb.group({
        replicas: [1, [Validators.required, Validators.min(1)]],
        resources: this.fb.group({
          cpu: ['500m', Validators.required],
          memory: ['1Gi', Validators.required]
        })
      }),
      dn: this.fb.group({
        replicas: [1, [Validators.required, Validators.min(1)]],
        resources: this.fb.group({
          cpu: ['500m', Validators.required],
          memory: ['1Gi', Validators.required]
        })
      }),
      gms: this.fb.group({
        replicas: [1, [Validators.required, Validators.min(1)]],
        resources: this.fb.group({
          cpu: ['500m', Validators.required],
          memory: ['1Gi', Validators.required]
        })
      }),
      cdc: this.fb.group({
        replicas: [1, [Validators.min(1)]],
        resources: this.fb.group({
          cpu: ['2'],
          memory: ['4Gi']
        })
      })
    });

    this.storageForm = this.fb.group({
      storageClassName: ['standard', Validators.required],
      size: ['20Gi', Validators.required],
      accessMode: ['ReadWriteOnce', Validators.required]
    });

    this.networkForm = this.fb.group({
      serviceType: ['ClusterIP', Validators.required],
      loadBalancerClass: [''],
      enableTLS: [false],
      tlsSecretName: ['']
    });

    this.advancedForm = this.fb.group({
      enableMonitoring: [true],
      enableBackup: [false],
      enableLogCollection: [false],
      customLabels: [''],
      customAnnotations: ['']
    });
  }

  selectTemplate(template: ClusterTemplate): void {
    this.selectedTemplate = template;
    this.templateForm.patchValue({ template: template.name });
    
    // 应用模板配置
    if (template.config) {
      this.applyTemplateConfig(template.config);
    }
  }

  private applyTemplateConfig(config: Partial<ClusterCreationConfig>): void {
    if ((config as any)?.['topology']) {
      const topology: any = (config as any)?.['topology'];
      this.topologyForm.patchValue({
        cn: topology.cn,
        dn: topology.dn,
        gms: topology.gms,
        enableCdc: !!topology.cdc,
        cdc: topology.cdc || { replicas: 1, resources: { cpu: '2', memory: '4Gi' } }
      });
    }
    
    if ((config as any)?.['storage']) {
      this.storageForm.patchValue((config as any)?.['storage']);
    }
    
    if ((config as any)?.['network']) {
      this.networkForm.patchValue((config as any)?.['network']);
    }
    
    if ((config as any)?.['security']) {
      this.networkForm.patchValue({
        enableTLS: (config as any)?.['security']?.enableTLS || false
      });
    }
  }

  selectServiceType(serviceType: string): void {
    this.networkForm.patchValue({ serviceType });
  }

  nextStep(stepper: any): void {
    stepper.next();
  }

  getConfigValue(path: string): any {
    const form = this.getFormByPath(path);
    return form?.get(path)?.value || '';
  }

  getTopologyValue(node: string, path: string): any {
    return this.topologyForm.get(`${node}.${path}`)?.value || '';
  }

  private getFormByPath(path: string): FormGroup {
    if (path.includes('topology')) return this.topologyForm;
    if (path.includes('storage')) return this.storageForm;
    if (path.includes('network')) return this.networkForm;
    return this.basicForm;
  }

  createCluster(): void {
    if (!this.isAllFormsValid()) {
      this.snackBar.open('请检查表单配置', '关闭', { duration: 3000 });
      return;
    }

    this.isCreating = true;
    const clusterConfig = this.buildClusterConfig();

    this.apiService.createClusterFromConfig(clusterConfig?.['namespace'], clusterConfig)
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: (cluster) => {
          this.snackBar.open('集群创建成功', '关闭', { duration: 3000 });
          this.isCreating = false;
          
          // 如果是在对话框中，关闭对话框并返回成功结果
          if (this.dialogRef) {
            this.dialogRef.close({ success: true, cluster: cluster });
          } else {
            // 如果不是在对话框中，导航到集群详情页面
            this.router.navigate(['/clusters', cluster.metadata.namespace, cluster.metadata.name]);
          }
        },
        error: (error) => {
          console.error('创建集群失败:', error);
          this.snackBar.open('创建集群失败', '关闭', { duration: 5000 });
          this.isCreating = false;
        }
      });
  }

  cancelCreation(): void {
    if (this.dialogRef) {
      this.dialogRef.close({ success: false });
    }
  }

  isInDialog(): boolean {
    return this.dialogRef !== null;
  }

  private isAllFormsValid(): boolean {
    return this.templateForm.valid && 
           this.basicForm.valid && 
           this.topologyForm.valid && 
           this.storageForm.valid && 
           this.networkForm.valid && 
           this.advancedForm.valid;
  }

  private buildClusterConfig(): ClusterCreationConfig {
    const basicValues = this.basicForm.value;
    const topologyValues = this.topologyForm.value;
    const storageValues = this.storageForm.value;
    const networkValues = this.networkForm.value;
    const advancedValues = this.advancedForm.value;

    const config: ClusterCreationConfig = {
      name: basicValues.name,
      namespace: basicValues.namespace,
      description: basicValues.description,
      version: basicValues.version,
      
      topology: {
        cn: topologyValues.cn,
        dn: topologyValues.dn,
        gms: topologyValues.gms
      },
      
      storage: {
        storageClassName: storageValues.storageClassName,
        size: storageValues.size,
        accessModes: [storageValues.accessMode]
      },
      
      network: {
        serviceType: networkValues.serviceType,
        loadBalancerClass: networkValues.loadBalancerClass
      },
      
      security: {
        enableTLS: networkValues.enableTLS,
        secretName: networkValues.tlsSecretName
      },
      
      advanced: {
        enableMonitoring: advancedValues.enableMonitoring,
        enableBackup: advancedValues.enableBackup,
        enableLogCollection: advancedValues.enableLogCollection,
        customLabels: this.parseJSON(advancedValues.customLabels),
        customAnnotations: this.parseJSON(advancedValues.customAnnotations)
      }
    };

    // 如果启用CDC，添加CDC配置
    if (topologyValues.enableCdc) {
      (config as any)['topology'].cdc = topologyValues.cdc;
    }

    return config;
  }

  private parseJSON(jsonString: string): Record<string, string> | undefined {
    if (!jsonString?.trim()) return undefined;
    try {
      return JSON.parse(jsonString);
    } catch {
      return undefined;
    }
  }
}