import { Component, inject, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormBuilder, FormGroup, Validators, ReactiveFormsModule } from '@angular/forms';
import { NzModalRef, NZ_MODAL_DATA } from 'ng-zorro-antd/modal';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzStepsModule } from 'ng-zorro-antd/steps';
import { NzRadioModule } from 'ng-zorro-antd/radio';
import { NzDatePickerModule } from 'ng-zorro-antd/date-picker';
import { NzCheckboxModule } from 'ng-zorro-antd/checkbox';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzCollapseModule } from 'ng-zorro-antd/collapse';
import { NzResultModule } from 'ng-zorro-antd/result';
import { NzMessageService } from 'ng-zorro-antd/message';
import { ApiService } from '../../services/api.service';
import { LoadingService } from '../../services/loading.service';
import { RestoreJobWithStatus } from '../../models/restore.model';
import { PolarDBXCluster } from '../../models/cluster.model';
import { PolarDBXBackup } from '../../models/backup.model';
import { Observable } from 'rxjs';
import { ActivatedRoute } from '@angular/router';

export interface RecoveryWizardDialogData {
  cluster?: PolarDBXCluster;
  suggestedBackupSet?: string;
}

@Component({
  selector: 'app-recovery-wizard',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    NzButtonModule,
    NzFormModule,
    NzInputModule,
    NzSelectModule,
    NzCardModule,
    NzSpinModule,
    NzIconModule,
    NzStepsModule,
    NzRadioModule,
    NzDatePickerModule,
    NzCheckboxModule,
    NzDividerModule,
    NzTagModule,
    NzAlertModule,
    NzGridModule,
    NzCollapseModule,
    NzResultModule
  ],
  template: `
    <div class="recovery-wizard">
      <div class="page-header">
        <div class="header-content">
          <h1 class="page-title">
            <i nz-icon nzType="build" class="page-icon"></i>
            恢复向导
          </h1>
          <p class="page-description">分步式引导完成集群恢复操作，支持备份恢复和时间点恢复</p>
        </div>
      </div>

      <div class="page-content">
        <nz-card class="wizard-card">
          <nz-steps [nzCurrent]="currentStep" [nzStatus]="stepStatus" class="wizard-steps">
            <nz-step nzTitle="选择恢复类型" nzDescription="选择备份恢复或时间点恢复"></nz-step>
            <nz-step nzTitle="配置恢复源" nzDescription="选择源集群和恢复点"></nz-step>
            <nz-step nzTitle="选择恢复目标" nzDescription="配置目标集群信息"></nz-step>
            <nz-step nzTitle="存储配置" nzDescription="配置存储参数（可选）"></nz-step>
            <nz-step nzTitle="确认执行" nzDescription="确认配置并执行恢复"></nz-step>
          </nz-steps>

          <div class="step-content">
            <!-- Step 1: 选择恢复类型 -->
            <div *ngIf="currentStep === 0" class="step-panel">
              <form [formGroup]="restoreTypeForm">
                <h3>选择恢复方式</h3>
                <p class="step-description">请选择您希望使用的恢复方式</p>
                
                <nz-radio-group formControlName="restoreType" class="restore-type-options">
                  <div class="restore-option">
                    <label nz-radio nzValue="backup" class="option-radio">
                      <div class="option-content">
                        <div class="option-header">
                          <i nz-icon nzType="save" class="option-icon"></i>
                          <span class="option-title">备份恢复</span>
                        </div>
                        <p class="option-description">从特定的备份集恢复集群到备份时的状态</p>
                      </div>
                    </label>
                  </div>
                  
                  <div class="restore-option">
                    <label nz-radio nzValue="pitr" class="option-radio">
                      <div class="option-content">
                        <div class="option-header">
                          <i nz-icon nzType="history" class="option-icon"></i>
                          <span class="option-title">时间点恢复 (PITR)</span>
                        </div>
                        <p class="option-description">恢复集群到指定的时间点状态</p>
                      </div>
                    </label>
                  </div>
                </nz-radio-group>
              </form>
            </div>

            <!-- Step 2: 配置恢复源 -->
            <div *ngIf="currentStep === 1" class="step-panel">
              <form [formGroup]="sourceSelectionForm">
                <h3>配置恢复源</h3>
                <p class="step-description">选择要恢复的源集群和恢复点</p>

                <nz-row [nzGutter]="16">
                  <nz-col [nzSpan]="12">
                    <nz-form-item>
                      <nz-form-label nzRequired>源集群</nz-form-label>
                      <nz-form-control [nzErrorTip]="getFieldError(sourceSelectionForm, 'sourceCluster')">
                        <nz-select formControlName="sourceCluster" nzPlaceHolder="选择源集群">
                          <nz-option *ngFor="let cluster of availableClusters" [nzValue]="cluster.metadata.name" 
                                     [nzLabel]="cluster.metadata.name + ' (' + cluster.metadata.namespace + ')'">
                          </nz-option>
                        </nz-select>
                      </nz-form-control>
                    </nz-form-item>
                  </nz-col>
                  <nz-col [nzSpan]="12">
                    <nz-form-item>
                      <nz-form-label nzRequired>命名空间</nz-form-label>
                      <nz-form-control [nzErrorTip]="getFieldError(sourceSelectionForm, 'sourceNamespace')">
                        <input nz-input formControlName="sourceNamespace" placeholder="集群所在命名空间" />
                      </nz-form-control>
                    </nz-form-item>
                  </nz-col>
                </nz-row>

                <div *ngIf="isBackupRestore">
                  <nz-form-item>
                    <nz-form-label nzRequired>备份集</nz-form-label>
                    <nz-form-control [nzErrorTip]="getFieldError(sourceSelectionForm, 'backupSet')">
                      <nz-select formControlName="backupSet" nzPlaceHolder="选择备份集">
                        <nz-option *ngFor="let backup of availableBackups" [nzValue]="backup.metadata.name" [nzLabel]="backup.metadata.name" nzCustomContent>
                          <div>
                            <strong>{{ backup.metadata.name }}</strong>
                            <br />
                            <small>{{ backup.metadata.creationTimestamp | date:'yyyy-MM-dd HH:mm:ss' }}</small>
                          </div>
                        </nz-option>
                      </nz-select>
                    </nz-form-control>
                  </nz-form-item>
                </div>

                <div *ngIf="isPitrRestore">
                  <nz-row [nzGutter]="16">
                    <nz-col [nzSpan]="12">
                      <nz-form-item>
                        <nz-form-label nzRequired>恢复时间点</nz-form-label>
                        <nz-form-control [nzErrorTip]="getFieldError(sourceSelectionForm, 'pitrTime')">
                          <nz-date-picker 
                            formControlName="pitrTime"
                            nzShowTime 
                            nzFormat="yyyy-MM-dd HH:mm:ss"
                            nzPlaceHolder="选择恢复时间点"
                            style="width: 100%;">
                          </nz-date-picker>
                        </nz-form-control>
                      </nz-form-item>
                    </nz-col>
                    <nz-col [nzSpan]="12">
                      <nz-form-item>
                        <nz-form-label>时区</nz-form-label>
                        <nz-form-control>
                          <nz-select formControlName="pitrTimezone" nzPlaceHolder="选择时区">
                            <nz-option *ngFor="let tz of timezones" [nzValue]="tz.value" [nzLabel]="tz.label"></nz-option>
                          </nz-select>
                        </nz-form-control>
                      </nz-form-item>
                    </nz-col>
                  </nz-row>
                </div>
              </form>
            </div>

            <!-- Steps 3-5 will be implemented incrementally -->
            <div *ngIf="currentStep === 2" class="step-panel">
              <form [formGroup]="targetSelectionForm">
                <h3>配置恢复目标</h3>
                <p class="step-description">选择恢复的目标集群</p>

                <nz-form-item>
                  <nz-form-control>
                    <label nz-checkbox formControlName="createNewCluster">创建新集群进行恢复</label>
                  </nz-form-control>
                </nz-form-item>

                <div *ngIf="!createNewCluster">
                  <nz-form-item>
                    <nz-form-label nzRequired>目标集群</nz-form-label>
                    <nz-form-control [nzErrorTip]="getFieldError(targetSelectionForm, 'targetCluster')">
                      <nz-select formControlName="targetCluster" nzPlaceHolder="选择目标集群">
                        <nz-option *ngFor="let cluster of availableClusters" [nzValue]="cluster.metadata.name"
                                   [nzLabel]="cluster.metadata.name + ' (' + cluster.metadata.namespace + ')'">
                        </nz-option>
                      </nz-select>
                    </nz-form-control>
                  </nz-form-item>
                </div>

                <div *ngIf="createNewCluster">
                  <nz-form-item>
                    <nz-form-label nzRequired>新集群名称</nz-form-label>
                    <nz-form-control [nzErrorTip]="getFieldError(targetSelectionForm, 'newClusterName')" nzExtra="只能包含小写字母、数字和连字符">
                      <input nz-input formControlName="newClusterName" placeholder="输入新集群名称" />
                    </nz-form-control>
                  </nz-form-item>
                </div>

                <nz-form-item>
                  <nz-form-label nzRequired>目标命名空间</nz-form-label>
                  <nz-form-control [nzErrorTip]="getFieldError(targetSelectionForm, 'targetNamespace')">
                    <input nz-input formControlName="targetNamespace" placeholder="目标命名空间" />
                  </nz-form-control>
                </nz-form-item>
              </form>
            </div>

            <div *ngIf="currentStep === 3" class="step-panel">
              <form [formGroup]="storageConfigForm">
                <h3>存储配置</h3>
                <p class="step-description">配置恢复所需的存储设置（可选）</p>

                <nz-form-item>
                  <nz-form-control>
                    <label nz-checkbox formControlName="useCustomStorage">使用自定义存储配置</label>
                  </nz-form-control>
                </nz-form-item>

                <nz-collapse *ngIf="useCustomStorage" [nzBordered]="false">
                  <nz-collapse-panel nzHeader="存储配置详情" [nzActive]="true">
                    <nz-form-item>
                      <nz-form-label>存储类型</nz-form-label>
                      <nz-form-control>
                        <nz-select formControlName="storageType" nzPlaceHolder="选择存储类型">
                          <nz-option *ngFor="let provider of storageProviders" [nzValue]="provider.value" [nzLabel]="provider.label"></nz-option>
                        </nz-select>
                      </nz-form-control>
                    </nz-form-item>

                    <div *ngIf="selectedStorageType === 'oss'" formGroupName="ossConfig">
                      <nz-divider nzText="阿里云 OSS 配置" nzOrientation="left"></nz-divider>
                      <nz-row [nzGutter]="16">
                        <nz-col [nzSpan]="12"><nz-form-item><nz-form-label>Access Key ID</nz-form-label><nz-form-control><input nz-input formControlName="accessKeyId" /></nz-form-control></nz-form-item></nz-col>
                        <nz-col [nzSpan]="12"><nz-form-item><nz-form-label>Access Key Secret</nz-form-label><nz-form-control><input nz-input formControlName="accessKeySecret" type="密码" /></nz-form-control></nz-form-item></nz-col>
                      </nz-row>
                      <nz-row [nzGutter]="16">
                        <nz-col [nzSpan]="12"><nz-form-item><nz-form-label>Bucket</nz-form-label><nz-form-control><input nz-input formControlName="bucket" /></nz-form-control></nz-form-item></nz-col>
                        <nz-col [nzSpan]="12"><nz-form-item><nz-form-label>Endpoint</nz-form-label><nz-form-control><input nz-input formControlName="endpoint" /></nz-form-control></nz-form-item></nz-col>
                      </nz-row>
                    </div>

                    <div *ngIf="selectedStorageType === 's3'" formGroupName="s3Config">
                      <nz-divider nzText="Amazon S3 配置" nzOrientation="left"></nz-divider>
                      <nz-row [nzGutter]="16">
                        <nz-col [nzSpan]="12"><nz-form-item><nz-form-label>Access Key ID</nz-form-label><nz-form-control><input nz-input formControlName="accessKeyId" /></nz-form-control></nz-form-item></nz-col>
                        <nz-col [nzSpan]="12"><nz-form-item><nz-form-label>Secret Access Key</nz-form-label><nz-form-control><input nz-input formControlName="secretAccessKey" type="password" /></nz-form-control></nz-form-item></nz-col>
                      </nz-row>
                      <nz-row [nzGutter]="16">
                        <nz-col [nzSpan]="8"><nz-form-item><nz-form-label>Bucket</nz-form-label><nz-form-control><input nz-input formControlName="bucket" /></nz-form-control></nz-form-item></nz-col>
                        <nz-col [nzSpan]="8"><nz-form-item><nz-form-label>Region</nz-form-label><nz-form-control><input nz-input formControlName="region" /></nz-form-control></nz-form-item></nz-col>
                        <nz-col [nzSpan]="8"><nz-form-item><nz-form-label>Endpoint (可选)</nz-form-label><nz-form-control><input nz-input formControlName="endpoint" /></nz-form-control></nz-form-item></nz-col>
                      </nz-row>
                    </div>

                    <div *ngIf="selectedStorageType === 'sftp'" formGroupName="sftpConfig">
                      <nz-divider nzText="SFTP 配置" nzOrientation="left"></nz-divider>
                      <nz-row [nzGutter]="16">
                        <nz-col [nzSpan]="12"><nz-form-item><nz-form-label>主机</nz-form-label><nz-form-control><input nz-input formControlName="host" /></nz-form-control></nz-form-item></nz-col>
                        <nz-col [nzSpan]="12"><nz-form-item><nz-form-label>端口</nz-form-label><nz-form-control><input nz-input formControlName="port" type="number" /></nz-form-control></nz-form-item></nz-col>
                      </nz-row>
                      <nz-row [nzGutter]="16">
                        <nz-col [nzSpan]="12"><nz-form-item><nz-form-label>用户名</nz-form-label><nz-form-control><input nz-input formControlName="username" /></nz-form-control></nz-form-item></nz-col>
                        <nz-col [nzSpan]="12"><nz-form-item><nz-form-label>密码</nz-form-label><nz-form-control><input nz-input formControlName="password" type="password" /></nz-form-control></nz-form-item></nz-col>
                      </nz-row>
                      <nz-row>
                        <nz-col [nzSpan]="24"><nz-form-item><nz-form-label>远程路径</nz-form-label><nz-form-control><input nz-input formControlName="remotePath" /></nz-form-control></nz-form-item></nz-col>
                      </nz-row>
                    </div>
                  </nz-collapse-panel>
                </nz-collapse>
              </form>
            </div>

            <div *ngIf="currentStep === 4" class="step-panel">
              <form [formGroup]="confirmationForm">
                <h3>确认恢复配置</h3>
                <p class="step-description">请仔细检查以下配置信息</p>

                <nz-card nzTitle="恢复配置摘要" class="summary-card">
                  <nz-collapse [nzBordered]="false" [nzExpandIconPosition]="'end'">
                    <nz-collapse-panel nzHeader="恢复类型" [nzActive]="true">
                      <nz-tag [nzColor]="isBackupRestore ? 'blue' : 'green'">{{ isBackupRestore ? '备份恢复' : '时间点恢复 (PITR)' }}</nz-tag>
                    </nz-collapse-panel>

                    <nz-collapse-panel nzHeader="源配置" [nzActive]="true">
                      <div class="summary-item"><strong>源集群:</strong> {{ sourceSelectionForm.get('sourceCluster')?.value || '-' }}</div>
                      <div class="summary-item"><strong>命名空间:</strong> {{ sourceSelectionForm.get('sourceNamespace')?.value || '-' }}</div>
                      <div *ngIf="isBackupRestore" class="summary-item"><strong>备份集:</strong> {{ sourceSelectionForm.get('backupSet')?.value || '-' }}</div>
                      <div *ngIf="isPitrRestore" class="summary-item"><strong>恢复时间:</strong> {{ sourceSelectionForm.get('pitrTime')?.value ? (sourceSelectionForm.get('pitrTime')?.value | date:'yyyy-MM-dd HH:mm:ss') : '-' }}</div>
                      <div *ngIf="isPitrRestore" class="summary-item"><strong>时区:</strong> {{ sourceSelectionForm.get('pitrTimezone')?.value || '-' }}</div>
                    </nz-collapse-panel>

                    <nz-collapse-panel nzHeader="目标配置" [nzActive]="true">
                      <div *ngIf="createNewCluster" class="summary-item"><strong>新集群名称:</strong> {{ targetSelectionForm.get('newClusterName')?.value || '-' }}</div>
                      <div *ngIf="!createNewCluster" class="summary-item"><strong>目标集群:</strong> {{ targetSelectionForm.get('targetCluster')?.value || '-' }}</div>
                      <div class="summary-item"><strong>目标命名空间:</strong> {{ targetSelectionForm.get('targetNamespace')?.value || '-' }}</div>
                      <div class="summary-item"><strong>操作类型:</strong> <nz-tag [nzColor]="createNewCluster ? 'green' : 'orange'">{{ createNewCluster ? '创建新集群' : '覆盖现有集群' }}</nz-tag></div>
                    </nz-collapse-panel>

                    <nz-collapse-panel *ngIf="useCustomStorage" nzHeader="存储配置">
                      <div class="summary-item"><strong>存储类型:</strong> <nz-tag nzColor="blue">{{ selectedStorageType.toUpperCase() }}</nz-tag></div>
                    </nz-collapse-panel>
                  </nz-collapse>
                </nz-card>

                <nz-alert nzType="warning" nzMessage="重要提醒" nzDescription="恢复操作将修改目标集群的数据，请确保已备份重要数据。恢复过程中集群可能暂时不可用。" nzShowIcon class="warning-alert"></nz-alert>

                <div class="confirmation-options">
                  <nz-form-item>
                    <nz-form-control><label nz-checkbox formControlName="validateData">在恢复前验证数据完整性</label></nz-form-control>
                  </nz-form-item>
                  <nz-form-item>
                    <nz-form-control><label nz-checkbox formControlName="confirmed">我已仔细检查上述配置，确认执行恢复操作</label></nz-form-control>
                  </nz-form-item>
                </div>
              </form>
            </div>
          </div>

          <div class="step-actions">
            <button nz-button nzType="default" *ngIf="currentStep > 0" (click)="previousStep()">
              <i nz-icon nzType="left"></i>
              上一步
            </button>
            
            <button nz-button nzType="primary" *ngIf="currentStep < 4" [disabled]="!isStepValid(currentStep)" (click)="nextStep()">
              下一步
              <i nz-icon nzType="right"></i>
            </button>

            <button nz-button nzType="primary" nzDanger *ngIf="currentStep === 4" 
                    [disabled]="!isFormValid() || isProcessing"
                    [nzLoading]="isProcessing"
                    (click)="executeRestore()">
              <i nz-icon nzType="play-circle"></i>
              {{ isProcessing ? '正在执行恢复...' : '开始恢复' }}
            </button>
          </div>
        </nz-card>
      </div>
    </div>
  `,
  styles: [`
    .recovery-wizard {
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
    
    .page-description {
      color: rgba(0, 0, 0, 0.6);
      font-size: 14px;
      margin: 0;
      line-height: 1.5;
    }
    
    .page-content {
      max-width: 1120px;
      margin: 0 auto;
    }
    
    .wizard-card {
      background: #ffffff;
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border: 1px solid #e0e0e0;
    }
    
    .wizard-steps {
      margin-bottom: 32px;
    }
    
    .step-content {
      min-height: 400px;
      margin-bottom: 24px;
    }
    
    .step-panel h3 {
      color: rgba(0, 0, 0, 0.87);
      font-size: 16px;
      font-weight: 500;
      margin: 0 0 8px 0;
    }
    
    .step-description {
      color: rgba(0, 0, 0, 0.6);
      font-size: 14px;
      margin: 0 0 24px 0;
      line-height: 1.5;
    }
    
    .restore-type-options {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }
    
    .restore-option {
      border: 1px solid #e0e0e0;
      border-radius: 8px;
      padding: 16px;
      transition: all 0.3s ease;
    }
    
    .restore-option:hover {
      border-color: #1890ff;
      box-shadow: 0 2px 8px rgba(24, 144, 255, 0.1);
    }
    
    .option-radio {
      width: 100%;
      display: flex;
      align-items: flex-start;
      gap: 12px;
    }
    
    .option-content {
      flex: 1;
    }
    
    .option-header {
      display: flex;
      align-items: center;
      gap: 8px;
      margin-bottom: 8px;
    }
    
    .option-icon {
      font-size: 20px;
      color: #1890ff;
    }
    
    .option-title {
      font-weight: 500;
      color: rgba(0, 0, 0, 0.87);
    }
    
    .option-description {
      color: rgba(0, 0, 0, 0.6);
      font-size: 13px;
      margin: 0;
      line-height: 1.4;
    }
    
    .step-actions {
      display: flex;
      justify-content: flex-end;
      gap: 12px;
      padding-top: 16px;
      border-top: 1px solid #e0e0e0;
    }
    
    /* 响应式设计 */
    @media (max-width: 1200px) {
      .page-content {
        max-width: 100%;
        padding: 0 8px;
      }
    }
    
    @media (max-width: 768px) {
      .recovery-wizard {
        padding: 8px;
      }
      
      .step-actions {
        flex-direction: column;
      }
      
      .restore-type-options {
        gap: 12px;
      }
    }
  `]
})
export class RecoveryWizardComponent implements OnInit {
  private fb = inject(FormBuilder);
  private apiService = inject(ApiService);
  private loadingService = inject(LoadingService);
  dialogRef = inject<NzModalRef<RecoveryWizardComponent>>(NzModalRef, { optional: true });
  data = inject<RecoveryWizardDialogData>(NZ_MODAL_DATA, { optional: true }) || {} as RecoveryWizardDialogData;
  private message = inject(NzMessageService);
  private route = inject(ActivatedRoute);

  // Step forms
  restoreTypeForm!: FormGroup;
  sourceSelectionForm!: FormGroup;
  targetSelectionForm!: FormGroup;
  storageConfigForm!: FormGroup;
  confirmationForm!: FormGroup;

  // Data
  availableClusters: PolarDBXCluster[] = [];
  availableBackups: PolarDBXBackup[] = [];
  
  // UI State
  isProcessing = false;
  selectedRestoreType: 'backup' | 'pitr' = 'backup';
  createNewCluster = false;
  currentStep = 0;
  stepStatus: 'wait' | 'process' | 'finish' | 'error' = 'wait';
  
  // Storage provider options
  storageProviders = [
    { value: 'oss', label: 'Alibaba Cloud OSS' },
    { value: 's3', label: 'Amazon S3' },
    { value: 'sftp', label: 'SFTP' }
  ];

  // Timezone options
  timezones = [
    { value: 'UTC', label: 'UTC' },
    { value: 'Asia/Shanghai', label: 'Asia/Shanghai' },
    { value: 'Asia/Tokyo', label: 'Asia/Tokyo' },
    { value: 'America/New_York', label: 'America/New_York' },
    { value: 'Europe/London', label: 'Europe/London' }
  ];

  constructor() {
    this.initializeForms();
    this.setupSourceWatchers();
  }

  ngOnInit(): void {
    this.route.queryParamMap.subscribe(q => {
      const mode = (q.get('mode') || q.get('type') || '').toLowerCase();
      if (mode === 'pitr') {
        this.selectedRestoreType = 'pitr';
        this.restoreTypeForm?.patchValue({ restoreType: 'pitr' });
      }
    });
    this.loadInitialData();
  }

  private initializeForms(): void {
    // Step 1: Restore Type Selection
    this.restoreTypeForm = this.fb.group({
      restoreType: ['backup', Validators.required]
    });

    // Step 2: Source Selection
    this.sourceSelectionForm = this.fb.group({
      sourceCluster: [this.data.cluster?.metadata.name || '', Validators.required],
      sourceNamespace: [this.data.cluster?.metadata.namespace || 'default', Validators.required],
      backupSet: [this.data.suggestedBackupSet || ''],
      pitrTime: [''],
      pitrTimezone: ['UTC']
    });

    // Step 3: Target Selection
    this.targetSelectionForm = this.fb.group({
      createNewCluster: [false],
      targetCluster: [''],
      newClusterName: [''],
      targetNamespace: ['default', Validators.required]
    });

    // Step 4: Storage Configuration
    this.storageConfigForm = this.fb.group({
      useCustomStorage: [false],
      storageType: ['oss'],
      ossConfig: this.fb.group({
        accessKeyId: [''],
        accessKeySecret: [''],
        bucket: [''],
        endpoint: [''],
        prefix: ['']
      }),
      s3Config: this.fb.group({
        accessKeyId: [''],
        secretAccessKey: [''],
        bucket: [''],
        region: [''],
        endpoint: [''],
        prefix: ['']
      }),
      sftpConfig: this.fb.group({
        host: [''],
        port: [22],
        username: [''],
        password: [''],
        remotePath: ['']
      })
    });

    // Step 5: Confirmation
    this.confirmationForm = this.fb.group({
      validateData: [true],
      confirmed: [false, Validators.requiredTrue]
    });

    // Watch for form changes
    this.restoreTypeForm.get('restoreType')?.valueChanges.subscribe(value => {
      this.selectedRestoreType = value;
      this.updateSourceFormValidators();
    });

    this.targetSelectionForm.get('createNewCluster')?.valueChanges.subscribe(value => {
      this.createNewCluster = value;
      this.updateTargetFormValidators();
    });

    // 初始化时立即应用一次校验规则，避免首次进入时未勾选导致提交 400
    this.updateSourceFormValidators();
    this.updateTargetFormValidators();
  }

  private updateSourceFormValidators(): void {
    const backupSetControl = this.sourceSelectionForm.get('backupSet');
    const pitrTimeControl = this.sourceSelectionForm.get('pitrTime');

    if (this.selectedRestoreType === 'backup') {
      backupSetControl?.setValidators([Validators.required]);
      pitrTimeControl?.clearValidators();
    } else {
      backupSetControl?.clearValidators();
      pitrTimeControl?.setValidators([Validators.required]);
    }

    backupSetControl?.updateValueAndValidity();
    pitrTimeControl?.updateValueAndValidity();
  }

  private updateTargetFormValidators(): void {
    const targetClusterControl = this.targetSelectionForm.get('targetCluster');
    const newClusterNameControl = this.targetSelectionForm.get('newClusterName');

    if (this.createNewCluster) {
      targetClusterControl?.clearValidators();
      newClusterNameControl?.setValidators([
        Validators.required,
        Validators.pattern(/^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/)
      ]);
    } else {
      targetClusterControl?.setValidators([Validators.required]);
      newClusterNameControl?.clearValidators();
    }

    targetClusterControl?.updateValueAndValidity();
    newClusterNameControl?.updateValueAndValidity();
  }

  private async loadInitialData(): Promise<void> {
    try {
      // Load clusters
      this.availableClusters = await this.apiService.getClusters().toPromise() || [];
      
      // Load backups if cluster is specified
      if (this.data.cluster) {
        this.availableBackups = await this.apiService.getBackups(
          this.data.cluster.metadata.namespace,
          this.data.cluster.metadata.name
        ).toPromise() || [];
      }
    } catch (error) {
      console.error('Failed to load initial data:', error);
    }
  }

  // Get restoration summary
  getRestoreSummary(): any {
    return {
      sourceCluster: this.sourceSelectionForm.get('sourceCluster')?.value,
      targetCluster: this.createNewCluster ? 
        this.targetSelectionForm.get('newClusterName')?.value :
        this.targetSelectionForm.get('targetCluster')?.value,
      restoreType: this.selectedRestoreType,
      backupSet: this.selectedRestoreType === 'backup' ? 
        this.sourceSelectionForm.get('backupSet')?.value : undefined,
      pitrTime: this.selectedRestoreType === 'pitr' ? 
        this.sourceSelectionForm.get('pitrTime')?.value : undefined,
      pitrTimezone: this.selectedRestoreType === 'pitr' ? 
        this.sourceSelectionForm.get('pitrTimezone')?.value : undefined,
      storageProvider: this.getStorageProvider(),
      validateData: this.confirmationForm.get('validateData')?.value,
      createNewCluster: this.createNewCluster,
      newClusterName: this.createNewCluster ? 
        this.targetSelectionForm.get('newClusterName')?.value : undefined
    };
  }

  private getStorageProvider(): any | undefined {
    if (!this.storageConfigForm.get('useCustomStorage')?.value) {
      return undefined;
    }

    const storageType = this.storageConfigForm.get('storageType')?.value;
    const config: { [key: string]: string } = {};

    switch (storageType) {
      case 'oss':
        const ossConfig = this.storageConfigForm.get('ossConfig')?.value;
        Object.assign(config, ossConfig);
        break;
      case 's3':
        const s3Config = this.storageConfigForm.get('s3Config')?.value;
        Object.assign(config, s3Config);
        break;
      case 'sftp':
        const sftpConfig = this.storageConfigForm.get('sftpConfig')?.value;
        Object.assign(config, sftpConfig);
        break;
    }

    return {
      type: storageType,
      config
    };
  }

  // Execute the restoration
  async executeRestore(): Promise<void> {
    if (!this.isFormValid()) {
      return;
    }

    this.isProcessing = true;

    try {
      const summary = this.getRestoreSummary();
      const namespace = this.sourceSelectionForm.get('sourceNamespace')?.value;
      const clusterName = summary.sourceCluster;

      if (summary.restoreType === 'pitr') {
        let timeIso = '' as string;
        const t: any = summary.pitrTime as any;
        if (t instanceof Date && !isNaN(t.getTime())) {
          timeIso = t.toISOString();
        } else if (typeof t === 'string') {
          const d = new Date(t);
          if (!isNaN(d.getTime())) {
            timeIso = d.toISOString();
          } else {
            throw new Error('恢复时间格式无效，请重新选择');
          }
        } else {
          throw new Error('请先选择恢复时间');
        }

        const pitrRequest = {
          time: timeIso,
          backupSet: summary.backupSet,
          targetCluster: summary.targetCluster,
          timezone: summary.pitrTimezone
        };

        await this.apiService.initiatePITR(namespace, clusterName, pitrRequest).toPromise();
      } else {
        const restoreRequest = {
          backupSet: summary.backupSet,
          targetCluster: summary.targetCluster,
          storageProvider: summary.storageProvider ? {
            type: summary.storageProvider.type,
            config: summary.storageProvider.config
          } : undefined
        };

        await this.apiService.restoreCluster(namespace, clusterName, restoreRequest).toPromise();
      }

      this.loadingService.showSnackBar('已发起恢复任务，正在创建目标集群…');
      this.dialogRef?.close(summary);
      // 跳转到恢复任务列表
      setTimeout(() => (window.location.hash = '#/recovery/restore-jobs'), 150);
    } catch (error) {
      console.error('Failed to execute restore:', error);
      const anyErr: any = error;
      const msg = anyErr?.error?.details || anyErr?.message || '执行失败，请稍后重试';
      this.loadingService.showSnackBar(`恢复任务提交失败：${msg}`);
    } finally {
      this.isProcessing = false;
    }
  }

  // Form validation helpers
  isFormValid(): boolean {
    return this.restoreTypeForm.valid &&
           this.sourceSelectionForm.valid &&
           this.targetSelectionForm.valid &&
           this.confirmationForm.valid;
  }

  isStepValid(stepIndex: number): boolean {
    switch (stepIndex) {
      case 0: return this.restoreTypeForm.valid;
      case 1: return this.sourceSelectionForm.valid;
      case 2: return this.targetSelectionForm.valid;
      case 3: return this.storageConfigForm.valid;
      case 4: return this.confirmationForm.valid;
      default: return false;
    }
  }

  // UI helpers
  get selectedStorageType(): string {
    return this.storageConfigForm.get('storageType')?.value || 'oss';
  }

  get useCustomStorage(): boolean {
    return this.storageConfigForm.get('useCustomStorage')?.value || false;
  }

  get isBackupRestore(): boolean {
    return this.selectedRestoreType === 'backup';
  }

  get isPitrRestore(): boolean {
    return this.selectedRestoreType === 'pitr';
  }

  // Cancel operation
  cancel(): void {
    this.dialogRef?.close();
  }

  // Get field error message
  getFieldError(formGroup: FormGroup, fieldName: string): string {
    const field = formGroup.get(fieldName);
    if (field?.errors && field.touched) {
      if (field.errors['required']) {
        return '此字段为必填项';
      }
      if (field.errors['pattern']) {
        return '格式不正确，请使用小写字母、数字和连字符';
      }
      if (field.errors['requiredTrue']) {
        return '请确认此选项';
      }
    }
    return '';
  }

  // Format datetime for display
  formatDateTime(date: Date): string {
    return date.toISOString().slice(0, 19);
  }

  // Load backups for selected cluster
  async onSourceClusterChange(): Promise<void> {
    const clusterName = this.sourceSelectionForm.get('sourceCluster')?.value;
    const namespace = this.sourceSelectionForm.get('sourceNamespace')?.value;
    
    if (clusterName && namespace) {
      try {
        this.availableBackups = await this.apiService.getBackups(namespace, clusterName).toPromise() || [];
      } catch (error) {
        console.error('Failed to load backups:', error);
        this.availableBackups = [];
      }
    }
  }

  nextStep(): void {
    if (this.currentStep < 4) {
      this.currentStep++;
    }
  }

  previousStep(): void {
    if (this.currentStep > 0) {
      this.currentStep--;
    }
  }

  // 监听源集群/命名空间变化刷新备份列表
  private setupSourceWatchers(): void {
    this.sourceSelectionForm.get('sourceCluster')?.valueChanges.subscribe(() => this.onSourceClusterChange());
    this.sourceSelectionForm.get('sourceNamespace')?.valueChanges.subscribe(() => this.onSourceClusterChange());
  }
}