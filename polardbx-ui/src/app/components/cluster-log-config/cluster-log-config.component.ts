import { Component, OnInit, OnDestroy, Input } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReactiveFormsModule, FormBuilder, FormGroup } from '@angular/forms';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzSwitchModule } from 'ng-zorro-antd/switch';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzCollapseModule } from 'ng-zorro-antd/collapse';
import { Subject } from 'rxjs';
import { takeUntil, finalize } from 'rxjs/operators';

import { ApiService } from '../../services/api.service';
import { LoadingService, LoadingKeys } from '../../services/loading.service';
import { PolarDBXCluster } from '../../models/cluster.model';

interface LogConfig {
  enableAuditLog?: boolean;
  logLevel?: string;
  auditLogFilter?: string;
  slowLogThreshold?: number;
}

interface ClusterLogConfig {
  cn: LogConfig;
  dn: LogConfig;
}

@Component({
  selector: 'app-cluster-log-config',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    NzCardModule,
    NzButtonModule,
    NzIconModule,
    NzSwitchModule,
    NzFormModule,
    NzInputModule,
    NzSelectModule,
    NzToolTipModule,
    NzCollapseModule
  ],
  template: `
    <div class="cluster-log-config">
      <nz-card class="config-card" nzTitle="集群日志配置" [nzExtra]="clusterName ? '控制 ' + clusterName + ' 集群的日志采集和审计功能' : ''">
        <form [formGroup]="configForm">
            
            <!-- CN 节点日志配置 -->
            <nz-collapse class="config-panel" [nzAccordion]="false">
              <nz-collapse-panel nzHeader="CN 计算节点日志配置" [nzActive]="true">
                <div class="config-section" formGroupName="cn">
                  <!-- CN 审计日志开关 -->
                  <div class="config-row">
                    <div class="config-label">
                      <i nz-icon nzType="eye"></i>
                      <span>审计日志采集</span>
                      <i nz-icon nzType="question-circle" nz-tooltip nzTooltipTitle="开启后将采集 SQL 审计日志，记录所有 SQL 执行信息" class="help-icon"></i>
                    </div>
                    <label>
                      <nz-switch formControlName="enableAuditLog" (ngModelChange)="onCNAuditLogToggle($event)"></nz-switch>
                      <span style="margin-left:8px;">{{configForm.get('cn.enableAuditLog')?.value ? '已开启' : '已关闭'}}</span>
                    </label>
                  </div>
                  
                  <!-- CN 日志级别 -->
                  <div class="config-row" *ngIf="configForm.get('cn.enableAuditLog')?.value">
                    <div class="config-label">
                      <i nz-icon nzType="sliders"></i>
                      <span>日志级别</span>
                    </div>
                    <nz-form-item class="config-field">
                      <nz-form-control>
                        <nz-select formControlName="logLevel">
                          <nz-option nzValue="ERROR" nzLabel="ERROR - 仅错误"></nz-option>
                          <nz-option nzValue="WARN" nzLabel="WARN - 警告及以上"></nz-option>
                          <nz-option nzValue="INFO" nzLabel="INFO - 信息及以上"></nz-option>
                          <nz-option nzValue="DEBUG" nzLabel="DEBUG - 调试级别"></nz-option>
                        </nz-select>
                      </nz-form-control>
                    </nz-form-item>
                  </div>
                  
                  <!-- CN 慢查询阈值 -->
                  <div class="config-row" *ngIf="configForm.get('cn.enableAuditLog')?.value">
                    <div class="config-label">
                      <i nz-icon nzType="field-time"></i>
                      <span>慢查询阈值</span>
                      <i nz-icon nzType="question-circle" nz-tooltip nzTooltipTitle="超过此时间（毫秒）的查询将被记录为慢查询" class="help-icon"></i>
                    </div>
                    <nz-form-item class="config-field">
                      <nz-form-control>
                        <input nz-input formControlName="slowLogThreshold" type="number" placeholder="毫秒" />
                      </nz-form-control>
                    </nz-form-item>
                  </div>
                  
                  <!-- CN 审计过滤器 -->
                  <div class="config-row" *ngIf="configForm.get('cn.enableAuditLog')?.value">
                    <div class="config-label">
                      <i nz-icon nzType="filter"></i>
                      <span>审计过滤器</span>
                      <i nz-icon nzType="question-circle" nz-tooltip nzTooltipTitle="使用正则表达式过滤要审计的 SQL 语句" class="help-icon"></i>
                    </div>
                    <nz-form-item class="config-field">
                      <nz-form-control>
                        <input nz-input formControlName="auditLogFilter" placeholder="例如：SELECT.*FROM.*users" />
                      </nz-form-control>
                    </nz-form-item>
                  </div>
                </div>
              </nz-collapse-panel>
            </nz-collapse>
            
            <!-- DN 节点日志配置 -->
            <nz-collapse class="config-panel">
              <nz-collapse-panel nzHeader="DN 数据节点日志配置" [nzActive]="false">
                <div class="config-section" formGroupName="dn">
                  <!-- DN 审计日志开关 -->
                  <div class="config-row">
                    <div class="config-label">
                      <i nz-icon nzType="eye"></i>
                      <span>审计日志采集</span>
                      <i nz-icon nzType="question-circle" nz-tooltip nzTooltipTitle="开启后将采集 MySQL 审计日志，记录数据操作信息" class="help-icon"></i>
                    </div>
                    <label>
                      <nz-switch formControlName="enableAuditLog" (ngModelChange)="onDNAuditLogToggle($event)"></nz-switch>
                      <span style="margin-left:8px;">{{configForm.get('dn.enableAuditLog')?.value ? '已开启' : '已关闭'}}</span>
                    </label>
                  </div>
                  
                  <!-- DN 日志级别 -->
                  <div class="config-row" *ngIf="configForm.get('dn.enableAuditLog')?.value">
                    <div class="config-label">
                      <i nz-icon nzType="sliders"></i>
                      <span>日志级别</span>
                    </div>
                    <nz-form-item class="config-field">
                      <nz-form-control>
                        <nz-select formControlName="logLevel">
                          <nz-option nzValue="ERROR" nzLabel="ERROR - 仅错误"></nz-option>
                          <nz-option nzValue="WARN" nzLabel="WARN - 警告及以上"></nz-option>
                          <nz-option nzValue="INFO" nzLabel="INFO - 信息及以上"></nz-option>
                          <nz-option nzValue="DEBUG" nzLabel="DEBUG - 调试级别"></nz-option>
                        </nz-select>
                      </nz-form-control>
                    </nz-form-item>
                  </div>
                  
                  <!-- DN 慢查询阈值 -->
                  <div class="config-row" *ngIf="configForm.get('dn.enableAuditLog')?.value">
                    <div class="config-label">
                      <i nz-icon nzType="field-time"></i>
                      <span>慢查询阈值</span>
                      <i nz-icon nzType="question-circle" nz-tooltip nzTooltipTitle="超过此时间（秒）的查询将被记录为慢查询" class="help-icon"></i>
                    </div>
                    <nz-form-item class="config-field">
                      <nz-form-control>
                        <nz-select formControlName="slowLogThreshold">
                          <nz-option [nzValue]="1" nzLabel="1 秒"></nz-option>
                          <nz-option [nzValue]="2" nzLabel="2 秒"></nz-option>
                          <nz-option [nzValue]="5" nzLabel="5 秒"></nz-option>
                          <nz-option [nzValue]="10" nzLabel="10 秒"></nz-option>
                        </nz-select>
                      </nz-form-control>
                    </nz-form-item>
                  </div>
                </div>
              </nz-collapse-panel>
            </nz-collapse>
        </form>
        <div class="actions" style="display:flex; gap:12px; justify-content:flex-end; padding:12px 0;">
          <button nz-button nzType="default" (click)="resetConfig()" [disabled]="isUpdating()">
            <i nz-icon nzType="reload"></i>
            <span>重置</span>
          </button>
          <button nz-button nzType="primary" (click)="saveConfig()" [disabled]="isUpdating() || !configForm.dirty">
            <i nz-icon nzType="save"></i>
            <span>保存配置</span>
          </button>
        </div>
      </nz-card>
      
      <!-- 当前状态展示 -->
      <nz-card class="status-card" nzTitle="当前日志采集状态">
          <div class="status-grid">
            <div class="status-item">
              <i nz-icon nzType="cluster" [class]="getStatusClass('cn')"></i>
              <div class="status-info">
                <div class="status-label">CN 审计日志</div>
                <div class="status-value">{{getCNAuditStatus()}}</div>
              </div>
            </div>
            
            <div class="status-item">
              <i nz-icon nzType="database" [class]="getStatusClass('dn')"></i>
              <div class="status-info">
                <div class="status-label">DN 审计日志</div>
                <div class="status-value">{{getDNAuditStatus()}}</div>
              </div>
            </div>
            
            <div class="status-item">
              <i nz-icon nzType="field-time" class="status-icon-info"></i>
              <div class="status-info">
                <div class="status-label">最后更新</div>
                <div class="status-value">{{lastUpdateTime}}</div>
              </div>
            </div>
          </div>
      </nz-card>
      
      <!-- 快捷命令展示 -->
      <nz-card class="commands-card" nzTitle="等效 kubectl 命令" nzExtra="以下是对应的命令行操作，供参考">
          <div class="commands-section">
            <h4>开启 CN 审计日志：</h4>
            <div class="command-block">
              <code [innerText]="cnPatchExample"></code>
              <button nz-button nzType="default" nzShape="circle" nz-tooltip nzTooltipTitle="复制命令" (click)="copyCommand('cn-enable')">
                <i nz-icon nzType="copy"></i>
              </button>
            </div>
            
            <h4>开启 DN 审计日志：</h4>
            <div class="command-block">
              <code [innerText]="dnPatchExample"></code>
              <button nz-button nzType="default" nzShape="circle" nz-tooltip nzTooltipTitle="复制命令" (click)="copyCommand('dn-enable')">
                <i nz-icon nzType="copy"></i>
              </button>
            </div>
          </div>
      </nz-card>
    </div>
  `,
  styles: [`
    .cluster-log-config {
      padding: 16px;
      max-width: 1200px;
      margin: 0 auto;
    }
    
    .config-card, .status-card, .commands-card {
      margin-bottom: 24px;
      border-radius: 12px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.1);
    }
    
    .config-card .mat-card-header {
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      color: white;
      border-radius: 12px 12px 0 0;
      margin: -24px -24px 24px -24px;
      padding: 24px;
    }
    
    .config-card .mat-card-header .mat-icon {
      margin-right: 8px;
    }
    
    .config-panel {
      margin-bottom: 16px;
      border-radius: 8px;
      border: 1px solid #e0e0e0;
    }
    
    .config-panel .mat-expansion-panel-header {
      padding: 16px 24px;
    }
    
    .config-panel .mat-panel-title .mat-icon {
      margin-right: 8px;
      vertical-align: middle;
    }
    
    .config-section {
      padding: 16px 24px;
    }
    
    .config-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 16px;
      padding: 12px;
      background: #f8f9fa;
      border-radius: 8px;
    }
    
    .config-label {
      display: flex;
      align-items: center;
      min-width: 200px;
      font-weight: 500;
    }
    
    .config-label .mat-icon {
      margin-right: 8px;
      color: #666;
    }
    
    .help-icon {
      margin-left: 8px;
      font-size: 16px;
      color: #999;
      cursor: help;
    }
    
    .config-field {
      min-width: 200px;
      max-width: 300px;
    }
    
    .status-card .mat-card-header {
      background: linear-gradient(135deg, #36d1dc 0%, #5b86e5 100%);
      color: white;
      border-radius: 12px 12px 0 0;
      margin: -24px -24px 24px -24px;
      padding: 24px;
    }
    
    .status-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 16px;
    }
    
    .status-item {
      display: flex;
      align-items: center;
      padding: 16px;
      background: #f8f9fa;
      border-radius: 8px;
      border-left: 4px solid #ddd;
    }
    
    .status-item .mat-icon {
      margin-right: 12px;
      font-size: 24px;
      width: 24px;
      height: 24px;
    }
    
    .status-icon-enabled {
      color: #4caf50;
      border-left-color: #4caf50;
    }
    
    .status-icon-disabled {
      color: #9e9e9e;
      border-left-color: #9e9e9e;
    }
    
    .status-icon-info {
      color: #2196f3;
    }
    
    .status-info {
      flex: 1;
    }
    
    .status-label {
      font-size: 12px;
      color: #666;
      margin-bottom: 4px;
    }
    
    .status-value {
      font-size: 14px;
      font-weight: 500;
    }
    
    .commands-card .mat-card-header {
      background: linear-gradient(135deg, #ff9a9e 0%, #fecfef 100%);
      color: #333;
      border-radius: 12px 12px 0 0;
      margin: -24px -24px 24px -24px;
      padding: 24px;
    }
    
    .commands-section h4 {
      margin: 16px 0 8px 0;
      color: #333;
      font-size: 14px;
    }
    
    .command-block {
      display: flex;
      align-items: center;
      background: #2d3748;
      color: #e2e8f0;
      padding: 12px 16px;
      border-radius: 8px;
      margin-bottom: 12px;
      font-family: 'Courier New', monospace;
    }
    
    .command-block code {
      flex: 1;
      background: none;
      color: inherit;
      font-size: 12px;
      line-height: 1.4;
    }
    
    .command-block button {
      margin-left: 8px;
      color: #e2e8f0;
    }
    
    .mat-card-actions {
      padding: 16px 24px;
      border-top: 1px solid #e0e0e0;
    }
    
    .mat-raised-button .mat-spinner {
      margin-left: 8px;
    }
  `]
})
export class ClusterLogConfigComponent implements OnInit, OnDestroy {
  @Input() clusterName: string = '';
  @Input() namespace: string = 'default';
  
  configForm: FormGroup;
  lastUpdateTime: string = '从未更新';
  cnPatchExample: string = '';
  dnPatchExample: string = '';
  
  private destroy$ = new Subject<void>();

  constructor(
    private fb: FormBuilder,
    private apiService: ApiService,
    private loadingService: LoadingService,
    private message: NzMessageService
  ) {
    this.configForm = this.createForm();
  }

  ngOnInit(): void {
    if (this.clusterName) {
      this.loadCurrentConfig();
    }
    this.cnPatchExample = `kubectl patch pxc ${this.clusterName} --patch '{"spec":{"config":{"cn":{"enableAuditLog":true}}}}' --type merge`;
    this.dnPatchExample = `kubectl patch pxc ${this.clusterName} --patch '{"spec":{"config":{"dn":{"enableAuditLog":true}}}}' --type merge`;
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  private createForm(): FormGroup {
    return this.fb.group({
      cn: this.fb.group({
        enableAuditLog: [false],
        logLevel: ['INFO'],
        auditLogFilter: [''],
        slowLogThreshold: [1000]
      }),
      dn: this.fb.group({
        enableAuditLog: [false],
        logLevel: ['INFO'],
        slowLogThreshold: [2]
      })
    });
  }

  isLoading(key: string): boolean {
    return this.loadingService.isLoading(key as any);
  }

  isUpdating(): boolean {
    return this.isLoading('UPDATE_CN_LOG_CONFIG') || this.isLoading('UPDATE_DN_LOG_CONFIG');
  }

  loadCurrentConfig(): void {
    this.apiService.getCluster(this.namespace, this.clusterName)
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: (cluster: PolarDBXCluster) => {
          this.updateFormFromCluster(cluster);
          this.lastUpdateTime = new Date().toLocaleString('zh-CN');
        },
        error: (error) => {
          console.error('Failed to load cluster config:', error);
          this.message.error('加载集群配置失败');
        }
      });
  }

  private updateFormFromCluster(cluster: PolarDBXCluster): void {
    const cfg: any = cluster.spec?.config as any || {};
    const cnConfig: Partial<LogConfig> = (cfg.cn as Partial<LogConfig>) || {};
    const dnConfig: Partial<LogConfig> = (cfg.dn as Partial<LogConfig>) || {};
    
    this.configForm.patchValue({
      cn: {
        enableAuditLog: cnConfig.enableAuditLog || false,
        logLevel: cnConfig.logLevel || 'INFO',
        auditLogFilter: cnConfig.auditLogFilter || '',
        slowLogThreshold: cnConfig.slowLogThreshold || 1000
      },
      dn: {
        enableAuditLog: dnConfig.enableAuditLog || false,
        logLevel: dnConfig.logLevel || 'INFO',
        slowLogThreshold: dnConfig.slowLogThreshold || 2
      }
    });
    
    this.configForm.markAsPristine();
  }

  onCNAuditLogToggle(enabled: boolean): void {
    this.updateClusterLogConfig('cn', { enableAuditLog: enabled });
  }

  onDNAuditLogToggle(enabled: boolean): void {
    this.updateClusterLogConfig('dn', { enableAuditLog: enabled });
  }

  private updateClusterLogConfig(nodeType: 'cn' | 'dn', config: Partial<LogConfig>): void {
    const loadingKey = nodeType === 'cn' ? 'UPDATE_CN_LOG_CONFIG' : 'UPDATE_DN_LOG_CONFIG';
    
    this.apiService.updateClusterLogConfig(this.namespace, this.clusterName, nodeType, config)
      .pipe(
        takeUntil(this.destroy$),
        finalize(() => this.loadingService.setLoading(loadingKey as any, false))
      )
      .subscribe({
        next: () => {
          this.message.success(`${nodeType.toUpperCase()} 日志配置更新成功`);
          this.lastUpdateTime = new Date().toLocaleString('zh-CN');
          this.configForm.markAsPristine();
        },
        error: (error) => {
          console.error(`Failed to update ${nodeType} log config:`, error);
          this.message.error(`${nodeType.toUpperCase()} 日志配置更新失败: ${error.message}`);
          // 回滚表单状态
          this.loadCurrentConfig();
        }
      });
  }

  saveConfig(): void {
    if (this.configForm.invalid || !this.configForm.dirty) {
      return;
    }

    const formValue = this.configForm.value;
    
    // 批量更新配置
    const updates = [];
    
    if (this.configForm.get('cn')?.dirty) {
      updates.push(
        this.apiService.updateClusterLogConfig(this.namespace, this.clusterName, 'cn', formValue.cn)
      );
    }
    
    if (this.configForm.get('dn')?.dirty) {
      updates.push(
        this.apiService.updateClusterLogConfig(this.namespace, this.clusterName, 'dn', formValue.dn)
      );
    }

    if (updates.length === 0) {
      return;
    }

    this.loadingService.setLoading('UPDATE_CN_LOG_CONFIG' as any, true);
    this.loadingService.setLoading('UPDATE_DN_LOG_CONFIG' as any, true);

    // 并发执行所有更新
    Promise.allSettled(updates.map(obs => obs.toPromise()))
      .then(results => {
        const successful = results.filter(r => r.status === 'fulfilled').length;
        const failed = results.filter(r => r.status === 'rejected').length;
        
        if (successful > 0) {
          this.message.success(`成功更新 ${successful} 项配置`);
          this.lastUpdateTime = new Date().toLocaleString('zh-CN');
          this.configForm.markAsPristine();
        }
        
        if (failed > 0) {
          this.message.error(`${failed} 项配置更新失败`);
        }
      })
      .finally(() => {
        this.loadingService.setLoading('UPDATE_CN_LOG_CONFIG' as any, false);
        this.loadingService.setLoading('UPDATE_DN_LOG_CONFIG' as any, false);
        this.loadCurrentConfig(); // 重新加载最新状态
      });
  }

  resetConfig(): void {
    this.loadCurrentConfig();
  }

  getCNAuditStatus(): string {
    return this.configForm.get('cn.enableAuditLog')?.value ? '已开启' : '已关闭';
  }

  getDNAuditStatus(): string {
    return this.configForm.get('dn.enableAuditLog')?.value ? '已开启' : '已关闭';
  }

  getStatusClass(nodeType: 'cn' | 'dn'): string {
    const enabled = this.configForm.get(`${nodeType}.enableAuditLog`)?.value;
    return enabled ? 'status-icon-enabled' : 'status-icon-disabled';
  }

  copyCommand(type: string): void {
    let command = '';
    
    switch (type) {
      case 'cn-enable':
        command = `kubectl patch pxc ${this.clusterName} --patch '{"spec":{"config":{"cn":{"enableAuditLog":true}}}}' --type merge`;
        break;
      case 'dn-enable':
        command = `kubectl patch pxc ${this.clusterName} --patch '{"spec":{"config":{"dn":{"enableAuditLog":true}}}}' --type merge`;
        break;
    }
    
    if (command) {
      navigator.clipboard.writeText(command).then(() => {
        this.message.success('命令已复制到剪贴板');
      });
    }
  }
}