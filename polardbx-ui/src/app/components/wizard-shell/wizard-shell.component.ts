import { Component, Input, Output, EventEmitter, TemplateRef, OnInit, ChangeDetectionStrategy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzStepsModule } from 'ng-zorro-antd/steps';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzSpinModule } from 'ng-zorro-antd/spin';

export interface WizardStep {
  id: string;
  title: string;
  description?: string;
  icon?: string;
  status?: 'wait' | 'process' | 'finish' | 'error';
  disabled?: boolean;
  template?: TemplateRef<any>;
}

export interface WizardAction {
  text: string;
  type?: 'default' | 'primary' | 'dashed';
  icon?: string;
  disabled?: boolean;
  loading?: boolean;
  hidden?: boolean;
  handler: () => void;
}

@Component({
  selector: 'app-wizard-shell',
  standalone: true,
  imports: [
    CommonModule,
    NzCardModule,
    NzStepsModule,
    NzButtonModule,
    NzIconModule,
    NzGridModule,
    NzAlertModule,
    NzSpinModule
  ],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="wizard-shell">
      <!-- 顶部信息条 -->
      <div class="wizard-header" *ngIf="showHeader">
        <div class="header-content">
          <div class="header-main">
            <h1 class="wizard-title">
              <i nz-icon [nzType]="titleIcon" class="title-icon" *ngIf="titleIcon"></i>
              {{ title }}
            </h1>
            <p class="wizard-subtitle" *ngIf="subtitle">{{ subtitle }}</p>
          </div>
          <div class="header-meta" *ngIf="namespace || objectName || docLink">
            <div class="meta-item" *ngIf="namespace">
              <span class="meta-label">命名空间:</span>
              <span class="meta-value">{{ namespace }}</span>
            </div>
            <div class="meta-item" *ngIf="objectName">
              <span class="meta-label">{{ objectLabel || '对象' }}:</span>
              <span class="meta-value">{{ objectName }}</span>
            </div>
            <div class="meta-item" *ngIf="docLink">
              <a [href]="docLink" target="_blank" nz-button nzType="link" nzSize="small">
                <i nz-icon nzType="question-circle"></i>
                文档
              </a>
            </div>
          </div>
        </div>
      </div>

      <div class="wizard-body">
        <nz-row [nzGutter]="24">
          <!-- 左侧步骤导航 -->
          <nz-col [nzSpan]="6">
            <div class="wizard-nav">
              <nz-steps 
                nzDirection="vertical" 
                nzSize="small" 
                [nzCurrent]="currentStepIndex"
                class="wizard-steps">
                <nz-step 
                  *ngFor="let step of steps; let i = index; trackBy: trackByStep"
                  [nzTitle]="step.title"
                  [nzDescription]="step.description"
                  [nzStatus]="getStepStatus(step, i)"
                  [nzIcon]="step.icon"
                  [nzDisabled]="step.disabled">
                </nz-step>
              </nz-steps>
            </div>
          </nz-col>

          <!-- 右侧内容区域 -->
          <nz-col [nzSpan]="18">
            <div class="wizard-content">
              <nz-card 
                class="step-card"
                [nzTitle]="currentStep?.title"
                [nzExtra]="extraTemplate"
                [nzLoading]="loading">
                
                <ng-container *ngIf="currentStep?.template as tpl">
                  <ng-container *ngTemplateOutlet="tpl"></ng-container>
                </ng-container>
                
                <div class="content-placeholder" *ngIf="!currentStep?.template">
                  <nz-alert 
                    nzType="info" 
                    nzMessage="步骤内容"
                    nzDescription="请为当前步骤提供内容模板"
                    nzShowIcon>
                  </nz-alert>
                </div>
              </nz-card>
            </div>
          </nz-col>
        </nz-row>
      </div>

      <!-- 底部固定操作条 -->
      <div class="wizard-footer" *ngIf="actions.length > 0">
        <div class="footer-content">
          <div class="footer-actions">
            <button 
              *ngFor="let action of actions; trackBy: trackByAction"
              nz-button 
              [nzType]="action.type || 'default'"
              [nzLoading]="action.loading"
              [disabled]="action.disabled"
              [class.hidden]="action.hidden"
              (click)="action.handler()">
              <i nz-icon [nzType]="action.icon" *ngIf="action.icon && !action.loading"></i>
              {{ action.text }}
            </button>
          </div>
        </div>
      </div>
    </div>
  `,
  styles: [`
    .wizard-shell {
      display: flex;
      flex-direction: column;
      height: 100vh;
      background: #f5f5f5;
    }

    .wizard-header {
      background: #fff;
      border-bottom: 1px solid #e8e8e8;
      padding: 16px 24px;
      flex-shrink: 0;
    }

    .header-content {
      max-width: 1200px;
      margin: 0 auto;
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
    }

    .header-main {
      flex: 1;
    }

    .wizard-title {
      color: rgba(0, 0, 0, 0.87);
      font-size: 20px;
      font-weight: 500;
      margin: 0 0 4px 0;
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .title-icon {
      font-size: 22px;
      color: #1890ff;
    }

    .wizard-subtitle {
      color: rgba(0, 0, 0, 0.6);
      font-size: 14px;
      margin: 0;
      line-height: 1.5;
    }

    .header-meta {
      display: flex;
      align-items: center;
      gap: 16px;
      flex-shrink: 0;
    }

    .meta-item {
      display: flex;
      align-items: center;
      gap: 4px;
      font-size: 13px;
    }

    .meta-label {
      color: rgba(0, 0, 0, 0.65);
    }

    .meta-value {
      color: rgba(0, 0, 0, 0.85);
      font-weight: 500;
    }

    .wizard-body {
      flex: 1;
      padding: 24px;
      max-width: 1200px;
      margin: 0 auto;
      width: 100%;
      overflow-y: auto;
    }

    .wizard-nav {
      background: #fff;
      border-radius: 8px;
      padding: 24px 16px;
      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06);
      border: 1px solid #e8e8e8;
      position: sticky;
      top: 0;
    }

    .wizard-steps {
      /* 禁用动画 */
      :deep(.ant-steps-item-process .ant-steps-item-icon) {
        transition: none !important;
      }
      
      :deep(.ant-steps-item-wait .ant-steps-item-icon) {
        transition: none !important;
      }
      
      :deep(.ant-steps-item-finish .ant-steps-item-icon) {
        transition: none !important;
      }
    }

    .wizard-content {
      height: 100%;
    }

    .step-card {
      min-height: 500px;
      border-radius: 8px;
      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06);
      border: 1px solid #e8e8e8;
    }

    .content-placeholder {
      padding: 40px;
      text-align: center;
    }

    .wizard-footer {
      background: #fff;
      border-top: 1px solid #e8e8e8;
      padding: 12px 24px;
      flex-shrink: 0;
      position: sticky;
      bottom: 0;
      z-index: 10;
    }

    .footer-content {
      max-width: 1200px;
      margin: 0 auto;
    }

    .footer-actions {
      display: flex;
      justify-content: flex-end;
      align-items: center;
      gap: 8px;
    }

    .footer-actions button.hidden {
      display: none;
    }

    /* 响应式设计 */
    @media (max-width: 1200px) {
      .wizard-body {
        max-width: 100%;
        padding: 16px;
      }
      
      .header-content {
        max-width: 100%;
        padding: 0 8px;
      }
      
      .footer-content {
        max-width: 100%;
        padding: 0 8px;
      }
    }

    @media (max-width: 768px) {
      .wizard-body {
        padding: 8px;
      }
      
      .header-content {
        flex-direction: column;
        gap: 12px;
        align-items: flex-start;
      }
      
      .header-meta {
        flex-direction: column;
        align-items: flex-start;
        gap: 8px;
      }
      
      .footer-actions {
        flex-direction: column-reverse;
        gap: 8px;
      }
      
      .footer-actions button {
        width: 100%;
      }
      
      /* 在移动端将左导航改为水平 */
      :deep(.ant-col-6) {
        width: 100% !important;
      }
      
      :deep(.ant-col-18) {
        width: 100% !important;
        margin-top: 16px;
      }
      
      .wizard-steps {
        :deep(.ant-steps) {
          flex-direction: row !important;
        }
        
        :deep(.ant-steps-vertical .ant-steps-item) {
          display: flex !important;
          flex-direction: column !important;
        }
      }
    }
  `]
})
export class WizardShellComponent implements OnInit {
  @Input() title = '';
  @Input() subtitle = '';
  @Input() titleIcon = '';
  @Input() showHeader = true;
  @Input() namespace = '';
  @Input() objectName = '';
  @Input() objectLabel = '';
  @Input() docLink = '';
  @Input() steps: WizardStep[] = [];
  @Input() currentStepIndex = 0;
  @Input() actions: WizardAction[] = [];
  @Input() loading = false;
  @Input() extraTemplate?: TemplateRef<any>;

  @Output() stepChange = new EventEmitter<{ from: number; to: number; step: WizardStep }>();

  get currentStep(): WizardStep | undefined {
    return this.steps[this.currentStepIndex];
  }

  ngOnInit(): void {
    // 确保至少有一个步骤
    if (this.steps.length === 0) {
      this.steps = [
        {
          id: 'default',
          title: '默认步骤',
          description: '请配置向导步骤'
        }
      ];
    }
  }

  getStepStatus(step: WizardStep, index: number): string {
    if (step.status) {
      return step.status;
    }
    
    if (index < this.currentStepIndex) {
      return 'finish';
    } else if (index === this.currentStepIndex) {
      return 'process';
    } else {
      return 'wait';
    }
  }

  goToStep(stepIndex: number): void {
    if (stepIndex >= 0 && stepIndex < this.steps.length && stepIndex !== this.currentStepIndex) {
      const fromStep = this.currentStepIndex;
      const toStep = stepIndex;
      const step = this.steps[stepIndex];
      
      if (!step.disabled) {
        this.currentStepIndex = stepIndex;
        this.stepChange.emit({ from: fromStep, to: toStep, step });
      }
    }
  }

  nextStep(): void {
    if (this.currentStepIndex < this.steps.length - 1) {
      this.goToStep(this.currentStepIndex + 1);
    }
  }

  prevStep(): void {
    if (this.currentStepIndex > 0) {
      this.goToStep(this.currentStepIndex - 1);
    }
  }

  // trackBy 避免 *ngFor 重建 DOM
  trackByStep(index: number, step: WizardStep): string {
    return step.id || `${index}-${step.title}`;
  }

  trackByAction(index: number, action: WizardAction): string {
    return `${action.text}-${action.icon || ''}-${action.type || ''}`;
  }
}
