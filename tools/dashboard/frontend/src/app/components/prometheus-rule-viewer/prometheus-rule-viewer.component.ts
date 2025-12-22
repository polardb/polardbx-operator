import { Component, OnInit, ChangeDetectionStrategy, ChangeDetectorRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzTabsModule } from 'ng-zorro-antd/tabs';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzDescriptionsModule } from 'ng-zorro-antd/descriptions';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzCodeEditorModule } from 'ng-zorro-antd/code-editor';

import { YamlPreviewComponent } from '../yaml-preview/yaml-preview.component';
import { ApiService } from '../../services/api.service';

interface PrometheusRule {
  metadata: {
    name: string;
    namespace: string;
    creationTimestamp: string;
    labels?: Record<string, string>;
  };
  spec: {
    groups: Array<{
      name: string;
      interval?: string;
      rules: Array<{
        alert?: string;
        expr: string;
        for?: string;
        labels?: Record<string, string>;
        annotations?: Record<string, string>;
        record?: string;
      }>;
    }>;
  };
}

@Component({
  selector: 'app-prometheus-rule-viewer',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    NzCardModule,
    NzButtonModule,
    NzIconModule,
    NzInputModule,
    NzAlertModule,
    NzSpinModule,
    NzTabsModule,
    NzTagModule,
    NzDescriptionsModule,
    NzEmptyModule,
    NzCodeEditorModule,
    YamlPreviewComponent
  ],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="page-wrapper prometheus-rule-viewer">
      <div class="page-header">
        <div class="title-block">
          <h2>
            <i nz-icon nzType="alert" nzTheme="outline" class="page-icon"></i>
            PrometheusRule 规则查看
          </h2>
          <p>
            查看和校验 Prometheus 告警规则配置
            <button nz-button nzType="default" nzSize="small" (click)="refreshRules()" [disabled]="loading">
              <i nz-icon nzType="reload"></i>
              刷新
            </button>
          </p>
        </div>
      </div>

      <div class="page-content">
        <!-- 系统规则卡片 -->
        <nz-card 
          nzTitle="系统告警规则集"
          class="system-rules-card"
          [nzExtra]="systemRulesExtra">
          
          <ng-template #systemRulesExtra>
            <div class="card-actions">
              <button 
                nz-button 
                nzType="default" 
                nzSize="small"
                (click)="viewSystemRules()"
                [nzLoading]="loadingSystemRules">
                <i nz-icon nzType="eye"></i>
                查看 YAML
              </button>
              <button 
                nz-button 
                nzType="primary" 
                nzSize="small"
                (click)="copySystemRulesCommand()">
                <i nz-icon nzType="copy"></i>
                复制命令
              </button>
            </div>
          </ng-template>

          <div class="system-rules-content">
            <nz-alert 
              nzType="info"
              nzMessage="系统规则集说明"
              nzDescription="polardbx-alert-rules 包含 PolarDB-X 的核心告警规则，建议不要直接修改系统规则。"
              nzShowIcon
              class="info-alert">
            </nz-alert>

            <nz-descriptions nzBordered nzSize="small" *ngIf="systemRuleInfo">
              <nz-descriptions-item nzTitle="规则集名称">{{ systemRuleInfo.name }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="命名空间">{{ systemRuleInfo.namespace }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="规则组数量">{{ systemRuleInfo.groupCount }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="告警规则数量">{{ systemRuleInfo.alertRuleCount }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="记录规则数量">{{ systemRuleInfo.recordRuleCount }}</nz-descriptions-item>
              <nz-descriptions-item nzTitle="创建时间">{{ systemRuleInfo.creationTime | date:'yyyy-MM-dd HH:mm:ss' }}</nz-descriptions-item>
            </nz-descriptions>

            <div class="kubectl-hint" *ngIf="!systemRuleInfo && !loadingSystemRules">
              <nz-alert 
                nzType="warning"
                nzMessage="无法通过 API 访问"
                nzDescription="系统检测到无法直接访问 PrometheusRule 资源，请使用 kubectl 命令查看。"
                nzShowIcon>
              </nz-alert>
              <div class="command-example">
                <h4>查看命令示例</h4>
                <pre>kubectl get prometheusrule polardbx-alert-rules -n polardbx-monitor -o yaml</pre>
              </div>
            </div>
          </div>
        </nz-card>

        <!-- YAML 查看器 -->
        <nz-card 
          nzTitle="YAML 内容"
          class="yaml-viewer-card"
          *ngIf="systemRulesYaml"
          [nzExtra]="yamlViewerExtra">
          
          <ng-template #yamlViewerExtra>
            <div class="yaml-actions">
              <button 
                nz-button 
                nzType="default" 
                nzSize="small"
                (click)="closeYamlViewer()">
                <i nz-icon nzType="close"></i>
                关闭
              </button>
            </div>
          </ng-template>

          <app-yaml-preview
            [yamlContent]="systemRulesYaml"
            filename="polardbx-alert-rules.yaml"
            [readonly]="true"
            [showValidateButton]="true"
            [validateFunction]="validatePrometheusRule">
          </app-yaml-preview>
        </nz-card>

        <!-- 自定义规则校验 -->
        <nz-card 
          nzTitle="自定义规则校验"
          class="validation-card">
          
          <nz-alert 
            nzType="info"
            nzMessage="规则校验工具"
            nzDescription="粘贴 PrometheusRule YAML 内容进行校验，检查语法和规则有效性。"
            nzShowIcon
            class="info-alert">
          </nz-alert>

          <div class="validation-section">
            <nz-tabset [nzAnimated]="false">
              <nz-tab nzTitle="YAML 输入" nzKey="yaml">
                <div class="yaml-input-section">
                  <div class="input-actions">
                    <button 
                      nz-button 
                      nzType="primary"
                      (click)="validateCustomRule()"
                      [nzLoading]="validatingCustom"
                      [disabled]="!customRuleYaml.trim()">
                      <i nz-icon nzType="check-circle"></i>
                      校验规则
                    </button>
                    <button 
                      nz-button 
                      nzType="default"
                      (click)="clearCustomRule()">
                      <i nz-icon nzType="close"></i>
                      清空
                    </button>
                    <button 
                      nz-button 
                      nzType="dashed"
                      (click)="loadExampleRule()">
                      <i nz-icon nzType="file-add"></i>
                      加载示例
                    </button>
                  </div>

                  <div class="yaml-editor">
                    <nz-code-editor
                      [(ngModel)]="customRuleYaml"
                      [nzEditorOption]="editorOptions"
                      style="height: 400px;">
                    </nz-code-editor>
                  </div>
                </div>
              </nz-tab>

              <nz-tab nzTitle="校验结果" nzKey="result">
                <div class="validation-result">
                  <div class="empty-result" *ngIf="!validationResult && !validatingCustom">
                    <nz-empty 
                      nzNotFoundImage="simple"
                      nzNotFoundContent="请先在 YAML 输入页面进行校验">
                    </nz-empty>
                  </div>

                  <nz-spin [nzSpinning]="validatingCustom" nzTip="正在校验规则...">
                    <div class="result-content" *ngIf="validationResult">
                      <nz-alert 
                        [nzType]="validationResult.success ? 'success' : 'error'"
                        [nzMessage]="validationResult.success ? '校验通过' : '校验失败'"
                        [nzDescription]="validationResult.message"
                        nzShowIcon>
                      </nz-alert>

                      <div class="result-details" *ngIf="validationResult.details">
                        <h4>详细信息</h4>
                        <div class="detail-item" *ngFor="let detail of validationResult.details">
                          <nz-tag [nzColor]="detail.level === 'error' ? 'red' : 'orange'">
                            {{ detail.level }}
                          </nz-tag>
                          <span class="detail-message">{{ detail.message }}</span>
                        </div>
                      </div>
                    </div>
                  </nz-spin>
                </div>
              </nz-tab>
            </nz-tabset>
          </div>
        </nz-card>
      </div>
    </div>
  `,
  styles: [`
    /* 对齐其它 monitoring 子页的设计语言（与告警模板页一致） */
    .page-wrapper {
      display: flex;
      flex-direction: column;
      gap: 16px;
      padding: 24px;
      min-height: 100%;
      background-color: transparent;
    }

    .page-header {
      background: #fff;
      padding: 16px;
      border-radius: 10px;
      box-shadow: 0 2px 10px rgba(15, 23, 42, 0.04);
    }

    .title-block {
      h2 {
        margin: 0 0 8px;
        font-size: 22px;
        font-weight: 600;
        color: #1f1f1f;
        display: flex;
        align-items: center;
        gap: 10px;
      }

      p {
        margin: 0;
        color: #595959;
        display: flex;
        align-items: center;
        gap: 12px;

        button {
          margin-left: 8px;
        }
      }
    }

    .page-content {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }

    .system-rules-card,
    .yaml-viewer-card,
    .validation-card {
      border-radius: 10px;
      border: 1px solid #e0e3e8;
      box-shadow: 0 2px 12px rgba(15, 23, 42, 0.05);
    }

    .card-actions,
    .yaml-actions {
      display: flex;
      gap: 8px;
    }

    .system-rules-content {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }

    .info-alert {
      margin-bottom: 16px;
    }

    .kubectl-hint {
      margin-top: 16px;
    }

    .command-example {
      margin-top: 12px;
    }

    .command-example h4 {
      margin: 0 0 8px 0;
      color: rgba(0, 0, 0, 0.85);
      font-size: 14px;
      font-weight: 500;
    }

    .command-example pre {
      background: #f6f8fa;
      border: 1px solid #e1e4e8;
      border-radius: 6px;
      padding: 12px;
      margin: 0;
      font-family: 'SFMono-Regular', 'Monaco', 'Menlo', 'Courier New', monospace;
      font-size: 13px;
      line-height: 1.4;
      color: #24292e;
      overflow-x: auto;
    }

    .validation-section {
      margin-top: 16px;
    }

    .yaml-input-section {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }

    .input-actions {
      display: flex;
      gap: 8px;
      align-items: center;
    }

    .yaml-editor {
      border: 1px solid #e8e8e8;
      border-radius: 6px;
      overflow: hidden;
    }

    .validation-result {
      min-height: 200px;
    }

    .empty-result {
      display: flex;
      align-items: center;
      justify-content: center;
      min-height: 200px;
    }

    .result-content {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }

    .result-details h4 {
      margin: 0 0 12px 0;
      color: rgba(0, 0, 0, 0.85);
      font-size: 14px;
      font-weight: 500;
    }

    .detail-item {
      display: flex;
      align-items: center;
      gap: 8px;
      margin-bottom: 8px;
    }

    .detail-message {
      color: rgba(0, 0, 0, 0.85);
      font-size: 13px;
    }

    /* Ensure editor style consistency */
    :deep(.monaco-editor) {
      font-family: 'SFMono-Regular', 'Monaco', 'Menlo', 'Courier New', monospace;
    }

    /* Responsive design */
    @media (max-width: 1200px) {
      .page-wrapper { padding: 16px; }
    }

    @media (max-width: 768px) {
      .page-wrapper { padding: 12px; }

      .card-actions,
      .yaml-actions,
      .input-actions {
        flex-wrap: wrap;
      }
    }
  `]
})
export class PrometheusRuleViewerComponent implements OnInit {
  loading = false;
  loadingSystemRules = false;
  validatingCustom = false;

  systemRuleInfo: any = null;
  systemRulesYaml = '';
  customRuleYaml = '';
  validationResult: any = null;

  editorOptions: any = {
    theme: 'vs',
    language: 'yaml',
    readOnly: false,
    minimap: { enabled: false },
    scrollBeyondLastLine: false,
    fontSize: 13,
    lineNumbers: 'on',
    folding: true,
    automaticLayout: true,
    wordWrap: 'on',
    wrappingIndent: 'indent'
  };

  constructor(
    private api: ApiService,
    private message: NzMessageService,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.loadSystemRuleInfo();
  }

  refreshRules(): void {
    this.loadSystemRuleInfo();
    if (this.systemRulesYaml) {
      this.viewSystemRules();
    }
  }

  private loadSystemRuleInfo(): void {
    this.loading = true;

    // Try to get system rule information
    this.api.getPrometheusRules('polardbx-monitor').subscribe({
      next: (rules: PrometheusRule[]) => {
        if (rules && Array.isArray(rules)) {
          const polardbxRules = rules.find(rule =>
            rule.metadata.name === 'polardbx-alert-rules'
          );

          if (polardbxRules) {
            this.systemRuleInfo = {
              name: polardbxRules.metadata.name,
              namespace: polardbxRules.metadata.namespace,
              creationTime: new Date(polardbxRules.metadata.creationTimestamp),
              groupCount: polardbxRules.spec.groups.length,
              alertRuleCount: this.countAlertRules(polardbxRules),
              recordRuleCount: this.countRecordRules(polardbxRules)
            };
          } else {
            this.systemRuleInfo = null;
          }
        } else {
          this.systemRuleInfo = null;
        }
        
        this.loading = false;
        this.cdr.markForCheck();
      },
      error: (error: any) => {
        console.error('加载 PrometheusRule 失败:', error);
        this.systemRuleInfo = null;
        this.loading = false;
        this.cdr.markForCheck();
      }
    });
  }

  private countAlertRules(rule: PrometheusRule): number {
    return rule.spec.groups.reduce((count, group) => {
      return count + group.rules.filter(r => r.alert).length;
    }, 0);
  }

  private countRecordRules(rule: PrometheusRule): number {
    return rule.spec.groups.reduce((count, group) => {
      return count + group.rules.filter(r => r.record).length;
    }, 0);
  }

  viewSystemRules(): void {
    this.loadingSystemRules = true;
    
    this.api.getPrometheusRuleYaml('polardbx-monitor', 'polardbx-alert-rules').subscribe({
      next: (yaml: string) => {
        this.systemRulesYaml = yaml;
        this.loadingSystemRules = false;
        this.cdr.markForCheck();
      },
      error: (error: any) => {
        console.error('获取系统规则 YAML 失败:', error);
        this.message.error('获取系统规则内容失败，请使用 kubectl 命令查看');
        this.loadingSystemRules = false;
        this.cdr.markForCheck();
      }
    });
  }

  closeYamlViewer(): void {
    this.systemRulesYaml = '';
  }

  copySystemRulesCommand(): void {
    const command = 'kubectl get prometheusrule polardbx-alert-rules -n polardbx-monitor -o yaml';
    navigator.clipboard.writeText(command).then(() => {
      this.message.success('命令已复制到剪贴板');
    }).catch(() => {
      this.message.error('复制失败');
    });
  }

  validateCustomRule(): void {
    if (!this.customRuleYaml.trim()) {
      this.message.warning('请输入 PrometheusRule YAML 内容');
      return;
    }

    this.validatingCustom = true;
    this.validationResult = null;

    // Call backend validation interface
    this.api.validatePrometheusRule(this.customRuleYaml).subscribe({
      next: (result: any) => {
        this.validationResult = result;
        this.validatingCustom = false;
        this.cdr.markForCheck();
      },
      error: (error: any) => {
        console.error('校验失败:', error);
        this.validationResult = {
          success: false,
          message: '校验过程出现错误: ' + (error.error?.message || error.message || '未知错误')
        };
        this.validatingCustom = false;
        this.cdr.markForCheck();
      }
    });
  }

  clearCustomRule(): void {
    this.customRuleYaml = '';
    this.validationResult = null;
  }

  loadExampleRule(): void {
    this.customRuleYaml = `apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: example-alert-rules
  namespace: polardbx-monitor
  labels:
    prometheus: kube-prometheus
    role: alert-rules
spec:
  groups:
  - name: example.rules
    interval: 30s
    rules:
    - alert: HighErrorRate
      expr: |
        (
          rate(http_requests_total{status=~"5.."}[5m])
          /
          rate(http_requests_total[5m])
        ) > 0.1
      for: 5m
      labels:
        severity: warning
      annotations:
        summary: High error rate detected
        description: "Error rate is {{ $value | humanizePercentage }} for {{ $labels.instance }}"
    
    - record: job:http_requests:rate5m
      expr: rate(http_requests_total[5m])
`;
    this.validationResult = null;
  }

  // Validation function for YamlPreviewComponent
  validatePrometheusRule = async (yaml: string): Promise<{ success: boolean; message: string }> => {
    try {
      const result = await this.api.validatePrometheusRule(yaml).toPromise();
      return {
        success: result.success,
        message: result.message || (result.success ? '校验通过' : '校验失败')
      };
    } catch (error) {
      return {
        success: false,
        message: '校验失败: ' + (error as Error).message
      };
    }
  };
}
