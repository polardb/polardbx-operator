import { Component, Input, OnInit, ChangeDetectionStrategy, ChangeDetectorRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { NzCardModule } from 'ng-zorro-antd/card';
import { FormsModule } from '@angular/forms';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzCodeEditorModule } from 'ng-zorro-antd/code-editor';

@Component({
  selector: 'app-yaml-preview',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    NzCardModule,
    NzButtonModule,
    NzIconModule,
    NzAlertModule,
    NzSpinModule,
    NzCodeEditorModule
  ],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="yaml-preview">
      <nz-card 
        nzTitle="YAML 预览"
        [nzExtra]="actionTemplate"
        class="preview-card">
        
        <ng-template #actionTemplate>
          <div class="action-buttons">
            <button 
              nz-button 
              nzType="default" 
              nzSize="small"
              (click)="copyToClipboard()"
              [nzLoading]="copying">
              <i nz-icon nzType="copy"></i>
              复制
            </button>
            <button 
              nz-button 
              nzType="default" 
              nzSize="small"
              (click)="downloadYaml()"
              [nzLoading]="downloading">
              <i nz-icon nzType="download"></i>
              下载
            </button>
            <button 
              *ngIf="showValidateButton"
              nz-button 
              nzType="primary" 
              nzSize="small"
              (click)="validateYaml()"
              [nzLoading]="validating">
              <i nz-icon nzType="check-circle"></i>
              校验
            </button>
          </div>
        </ng-template>

        <div class="preview-content">
          <nz-alert 
            *ngIf="errorMessage"
            nzType="error"
            [nzMessage]="errorMessage"
            nzShowIcon
            class="error-alert">
          </nz-alert>

          <nz-alert 
            *ngIf="validationResult"
            [nzType]="validationResult.success ? 'success' : 'error'"
            [nzMessage]="validationResult.message"
            nzShowIcon
            class="validation-alert">
          </nz-alert>

          <div class="yaml-container" *ngIf="yamlContent">
            <nz-code-editor
              class="yaml-editor"
              [ngModel]="yamlContent"
              [nzLoading]="loading"
              [nzEditorOption]="editorOptions"
              (ngModelChange)="onYamlChange($event)">
            </nz-code-editor>
          </div>

          <div class="empty-content" *ngIf="!yamlContent && !loading">
            <nz-alert 
              nzType="info"
              nzMessage="暂无内容"
              nzDescription="请先完成前面的配置步骤以生成 YAML"
              nzShowIcon>
            </nz-alert>
          </div>

          <nz-spin 
            *ngIf="loading" 
            nzTip="正在生成 YAML..."
            class="loading-spin">
          </nz-spin>
        </div>
      </nz-card>
    </div>
  `,
  styles: [`
    .yaml-preview {
      height: 100%;
    }

    .preview-card {
      height: 100%;
      border-radius: 8px;
      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06);
      border: 1px solid #e8e8e8;
    }

    .action-buttons {
      display: flex;
      gap: 8px;
      align-items: center;
    }

    .preview-content {
      position: relative;
      height: 100%;
      min-height: 400px;
    }

    .error-alert,
    .validation-alert {
      margin-bottom: 16px;
    }

    .yaml-container {
      height: 100%;
      min-height: 400px;
    }

    .yaml-editor {
      height: 100%;
      min-height: 400px;
      border: 1px solid #e8e8e8;
      border-radius: 6px;
    }

    .empty-content {
      display: flex;
      align-items: center;
      justify-content: center;
      height: 100%;
      min-height: 300px;
    }

    .loading-spin {
      display: flex;
      align-items: center;
      justify-content: center;
      height: 100%;
      min-height: 200px;
    }

    /* 确保编辑器主题与中性风格一致 */
    :deep(.monaco-editor) {
      font-family: 'SFMono-Regular', 'Monaco', 'Menlo', 'Courier New', monospace;
    }

    :deep(.monaco-editor .margin) {
      background-color: #fafafa;
    }

    :deep(.monaco-editor .monaco-editor-background) {
      background-color: #fff;
    }
  `]
})
export class YamlPreviewComponent implements OnInit {
  @Input() yamlContent = '';
  @Input() filename = 'config.yaml';
  @Input() loading = false;
  @Input() readonly = false;
  @Input() showValidateButton = false;
  @Input() validateFunction?: (yaml: string) => Promise<{ success: boolean; message: string }>;

  copying = false;
  downloading = false;
  validating = false;
  errorMessage = '';
  validationResult: { success: boolean; message: string } | null = null;

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
    private message: NzMessageService,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.editorOptions.readOnly = this.readonly;
  }

  onYamlChange(content: string): void {
    if (!this.readonly) {
      this.yamlContent = content;
      this.clearValidationResult();
    }
  }

  async copyToClipboard(): Promise<void> {
    if (!this.yamlContent) {
      this.message.warning('没有内容可复制');
      return;
    }

    this.copying = true;
    try {
      await navigator.clipboard.writeText(this.yamlContent);
      this.message.success('已复制到剪贴板');
    } catch (error) {
      console.error('复制失败:', error);
      this.message.error('复制失败，请手动选择内容复制');
    } finally {
      this.copying = false;
      this.cdr.markForCheck();
    }
  }

  downloadYaml(): void {
    if (!this.yamlContent) {
      this.message.warning('没有内容可下载');
      return;
    }

    this.downloading = true;
    try {
      const blob = new Blob([this.yamlContent], { type: 'text/yaml' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = this.filename;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);
      this.message.success('文件下载已开始');
    } catch (error) {
      console.error('下载失败:', error);
      this.message.error('下载失败');
    } finally {
      this.downloading = false;
      this.cdr.markForCheck();
    }
  }

  async validateYaml(): Promise<void> {
    if (!this.yamlContent) {
      this.message.warning('没有内容可校验');
      return;
    }

    if (!this.validateFunction) {
      this.message.warning('未配置校验函数');
      return;
    }

    this.validating = true;
    this.clearValidationResult();
    
    try {
      this.validationResult = await this.validateFunction(this.yamlContent);
      this.cdr.markForCheck();
    } catch (error) {
      console.error('校验失败:', error);
      this.validationResult = {
        success: false,
        message: '校验过程出现错误: ' + (error as Error).message
      };
      this.cdr.markForCheck();
    } finally {
      this.validating = false;
      this.cdr.markForCheck();
    }
  }

  private clearValidationResult(): void {
    this.validationResult = null;
    this.errorMessage = '';
  }

  setError(message: string): void {
    this.errorMessage = message;
    this.cdr.markForCheck();
  }

  clearError(): void {
    this.errorMessage = '';
    this.cdr.markForCheck();
  }
}
