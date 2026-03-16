import {
  Component,
  Input,
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  ViewChild,
  ElementRef,
  OnChanges,
  SimpleChanges,
  OnDestroy,
  NgZone,
  inject
} from '@angular/core';
import { CommonModule } from '@angular/common';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzCodeEditorService } from 'ng-zorro-antd/code-editor';
import { Subscription } from 'rxjs';
import type { editor, IDisposable } from 'monaco-editor';

type Monaco = typeof import('monaco-editor');

interface MonacoAmdRequire {
  (modules: string[], onLoad: (...args: unknown[]) => void, onError?: (err: unknown) => void): void;
  (module: string): unknown;
}

interface MonacoWindow extends Window {
  monaco?: Monaco;
  require?: MonacoAmdRequire;
}

@Component({
  selector: 'app-yaml-preview',
  standalone: true,
  imports: [
    CommonModule,
    NzCardModule,
    NzButtonModule,
    NzIconModule,
    NzAlertModule,
    NzSpinModule
  ],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="yaml-preview">
      <nz-card 
        [nzTitle]="cardTitle"
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
            <div 
              class="yaml-editor" 
              #editorContainer
              *ngIf="!usePlainRenderer && !fallbackMode">
            </div>
            <pre
              *ngIf="usePlainRenderer || fallbackMode"
              class="plain-preview">{{ yamlContent }}</pre>
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

    .plain-preview {
      white-space: pre-wrap;
      word-break: break-word;
      font-family: 'SFMono-Regular', 'Monaco', 'Menlo', 'Courier New', monospace;
      font-size: 13px;
      line-height: 1.6;
      background: #0d1117;
      color: #d4d4d4;
      padding: 16px;
      border-radius: 6px;
      border: 1px solid rgba(148, 163, 184, 0.25);
      min-height: 400px;
      overflow: auto;
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

    /* Ensure editor theme matches neutral style */
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
export class YamlPreviewComponent implements OnChanges, OnDestroy {
  @Input() yamlContent = '';
  @Input() filename = 'config.yaml';
  @Input() loading = false;
  @Input() readonly = false;
  @Input() showValidateButton = false;
  @Input() language = 'yaml';
  @Input() validateFunction?: (yaml: string) => Promise<{ success: boolean; message: string }>;
  @Input() cardTitle = 'YAML 预览';
  @Input() usePlainRenderer = false;

  @ViewChild('editorContainer')
  set editorContainer(container: ElementRef<HTMLDivElement> | undefined) {
    if (container) {
      this.editorHost = container;
      this.initializeEditor();
    } else {
      this.disposeEditor();
      this.editorHost = undefined;
    }
  }

  copying = false;
  downloading = false;
  validating = false;
  errorMessage = '';
  validationResult: { success: boolean; message: string } | null = null;

  private readonly message = inject(NzMessageService);
  private readonly cdr = inject(ChangeDetectorRef);
  private readonly codeEditorService = inject(NzCodeEditorService);
  private readonly ngZone = inject(NgZone);

  private editorHost?: ElementRef<HTMLDivElement>;
  private monaco?: Monaco;
  private editorInstance?: editor.IStandaloneCodeEditor;
  private editorDisposables: IDisposable[] = [];
  private suppressModelChange = false;
  private monacoInitSubscription?: Subscription;
  private pendingContent = '';
  private yamlLanguageLoaded = false;
  private initializingEditor = false;
  private currentLanguage = 'yaml';
  fallbackMode = false;

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['usePlainRenderer']) {
      if (this.usePlainRenderer) {
        this.enableFallback();
      } else if (this.fallbackMode) {
        this.fallbackMode = false;
        this.initializeEditor();
      }
    }

    if (changes['yamlContent']) {
      this.pendingContent = this.yamlContent || '';
      this.setEditorValue(this.pendingContent);
    }

    if (changes['readonly'] && !changes['readonly'].firstChange) {
      this.updateReadonlyState();
    }

    if (changes['language'] && !changes['language'].firstChange) {
      this.updateLanguage();
    }
  }

  ngOnDestroy(): void {
    this.monacoInitSubscription?.unsubscribe();
    this.monacoInitSubscription = undefined;
    this.disposeEditor();
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
      const mime = this.language === 'json' ? 'application/json' : 'text/yaml';
      const blob = new Blob([this.yamlContent], { type: mime });
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
        message: '校验过程出现错误: ' + (error instanceof Error ? error.message : String(error))
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

  private initializeEditor(): void {
    if (this.editorInstance || !this.editorHost || this.initializingEditor || this.fallbackMode || this.usePlainRenderer) {
      return;
    }

    if (typeof window === 'undefined') {
      this.enableFallback('Monaco 编辑器仅在浏览器环境下可用');
      return;
    }

    this.initializingEditor = true;
    this.monacoInitSubscription?.unsubscribe();
    this.monacoInitSubscription = this.codeEditorService.requestToInit().subscribe({
      next: () => {
        this.monacoInitSubscription?.unsubscribe();
        this.monacoInitSubscription = undefined;

        const monacoGlobal = (window as unknown as MonacoWindow).monaco;
        if (!monacoGlobal) {
          this.enableFallback('Monaco 编辑器资源加载失败');
          this.initializingEditor = false;
          return;
        }

        this.monaco = monacoGlobal;

        this.ensureLanguageSupport(monacoGlobal, this.language || 'yaml')
          .then(() => {
            try {
              this.createEditor(monacoGlobal);
            } catch (err) {
              console.error('创建 Monaco 编辑器失败，回退到纯文本模式:', err);
              this.enableFallback('Monaco 编辑器初始化失败，已切换到纯文本模式');
            } finally {
              this.initializingEditor = false;
            }
          })
          .catch(error => {
            console.error('加载 Monaco YAML 支持失败:', error);
            this.enableFallback('无法加载语法高亮资源，已切换为纯文本显示');
            this.initializingEditor = false;
          });
      },
      error: error => {
        console.error('初始化 Monaco Editor 失败:', error);
        this.enableFallback('无法加载编辑器资源，已切换为纯文本模式');
        this.initializingEditor = false;
      }
    });
  }

  private ensureLanguageSupport(monacoGlobal: Monaco, language: string): Promise<void> {
    if (language !== 'yaml') {
      return Promise.resolve();
    }

    if (this.yamlLanguageLoaded) {
      return Promise.resolve();
    }

    // New versions of Monaco editor have built-in basic language support
    // First check if YAML language is already available
    const languages = monacoGlobal.languages.getLanguages();
    if (languages.some(lang => lang.id === 'yaml')) {
      this.yamlLanguageLoaded = true;
      return Promise.resolve();
    }

    // If not available, try loading via AMD loader (only for older versions)
    const loaderWindow = window as unknown as MonacoWindow;
    const amdRequire = loaderWindow.require;
    if (amdRequire) {
      return new Promise((resolve) => {
        // 尝试新的模块路径格式
        const possiblePaths = [
          'vs/basic-languages/yaml/yaml.contribution',
          'vs/language/yaml/monaco.contribution'
        ];
        
        let tried = 0;
        const tryNext = (): void => {
          if (tried >= possiblePaths.length) {
            // All paths failed, but Monaco basic functionality may still be available
            // Check if language is already registered (may have been registered during loading attempts)
            if (monacoGlobal.languages.getLanguages().some(lang => lang.id === 'yaml')) {
              this.yamlLanguageLoaded = true;
            }
            resolve();
            return;
          }
          
          amdRequire(
            [possiblePaths[tried]],
            () => {
              this.yamlLanguageLoaded = true;
              resolve();
            },
            () => {
              tried++;
              tryNext();
            }
          );
        };
        
        tryNext();
      });
    }

    // AMD loader not available, assume language support is built-in
    this.yamlLanguageLoaded = true;
    return Promise.resolve();
  }

  private createEditor(monacoGlobal: Monaco): void {
    const hostElement = this.editorHost?.nativeElement;
    if (!hostElement) {
      return;
    }

    const initialValue = this.pendingContent || this.yamlContent || '';
    const language = this.language || 'yaml';

    this.ngZone.runOutsideAngular(() => {
      this.editorInstance = monacoGlobal.editor.create(hostElement, {
        value: initialValue,
        language,
        theme: 'vs',
        readOnly: this.readonly,
        minimap: { enabled: false },
        scrollBeyondLastLine: false,
        fontSize: 13,
        lineNumbers: 'on',
        folding: true,
        automaticLayout: true,
        wordWrap: 'on',
        wrappingIndent: 'indent'
      });

      this.editorDisposables.push(
        this.editorInstance.onDidChangeModelContent(() => this.handleEditorContentChange())
      );
    });

    this.setEditorValue(initialValue);
    this.updateReadonlyState();
    this.currentLanguage = language;
    this.updateLanguage();
    this.ngZone.run(() => this.cdr.markForCheck());
  }

  private handleEditorContentChange(): void {
    if (this.readonly || !this.editorInstance || this.suppressModelChange) {
      return;
    }

    const value = this.editorInstance.getValue();
    if (value === this.yamlContent) {
      return;
    }

    this.ngZone.run(() => {
      this.yamlContent = value;
      this.clearValidationResult();
      this.cdr.markForCheck();
    });
  }

  private setEditorValue(content: string): void {
    this.pendingContent = content ?? '';

    if (!this.editorInstance) {
      return;
    }

    const current = this.editorInstance.getValue();
    if (current === this.pendingContent) {
      return;
    }

    this.suppressModelChange = true;
    this.editorInstance.setValue(this.pendingContent);
    this.suppressModelChange = false;
  }

  private updateReadonlyState(): void {
    if (!this.editorInstance) {
      return;
    }

    this.editorInstance.updateOptions({ readOnly: this.readonly });
  }

  private updateLanguage(): void {
    const targetLanguage = this.language || 'yaml';
    if (targetLanguage === this.currentLanguage) {
      return;
    }

    if (!this.monaco || !this.editorInstance) {
      this.currentLanguage = targetLanguage;
      return;
    }

    this.ensureLanguageSupport(this.monaco, targetLanguage)
      .then(() => {
        const model = this.editorInstance?.getModel();
        if (model) {
          this.monaco!.editor.setModelLanguage(model, targetLanguage);
          this.currentLanguage = targetLanguage;
        }
      })
      .catch(error => {
        console.error('切换代码语言失败:', error);
      });
  }

  private disposeEditor(): void {
    // Clean up event listeners first to avoid triggering unnecessary callbacks
    this.editorDisposables.forEach(disposable => {
      try {
        disposable.dispose();
      } catch (err: unknown) {
        if (err instanceof Error && err.message === 'Canceled') {
          return;
        }
        console.warn('释放编辑器监听器失败:', err);
      }
    });
    this.editorDisposables = [];

    if (this.editorInstance) {
      try {
        this.editorInstance.dispose();
      } catch (err: unknown) {
        if (err instanceof Error && err.message === 'Canceled') {
          return;
        }
        console.warn('释放 Monaco 编辑器失败:', err);
      }
      this.editorInstance = undefined;
    }

    this.initializingEditor = false;
  }

  private enableFallback(message?: string): void {
    if (this.fallbackMode) {
      return;
    }
    this.fallbackMode = true;
    this.disposeEditor();
    if (message) {
      this.message.info(message);
    }
    this.ngZone.run(() => this.cdr.markForCheck());
  }
}
