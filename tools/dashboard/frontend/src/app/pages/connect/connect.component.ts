import { Component, inject } from '@angular/core';
import { Router } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { finalize } from 'rxjs/operators';
import { DomSanitizer, SafeHtml } from '@angular/platform-browser';

// NG-ZORRO components
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzIconModule, NzIconService } from 'ng-zorro-antd/icon';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzCollapseModule } from 'ng-zorro-antd/collapse';
import { NzSegmentedModule } from 'ng-zorro-antd/segmented';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzDividerModule } from 'ng-zorro-antd/divider';

// Icon imports
import {
  CloudServerOutline,
  DatabaseOutline,
  SyncOutline,
  DashboardOutline,
  CloudUploadOutline,
  CheckCircleOutline,
  FileTextOutline,
  CloseOutline,
  CopyOutline,
  ExpandOutline,
  CodeOutline,
  FolderOpenOutline,
  CloudOutline,
  QuestionCircleOutline,
  LoginOutline,
  StopOutline,
  InfoCircleOutline,
  BulbOutline,
  UploadOutline,
  EditOutline
} from '@ant-design/icons-angular/icons';

import { ApiService } from '../../services/api.service';
import { AuthService } from '../../services/auth.service';

// List of icons to use
const icons = [
  CloudServerOutline,
  DatabaseOutline,
  SyncOutline,
  DashboardOutline,
  CloudUploadOutline,
  CheckCircleOutline,
  FileTextOutline,
  CloseOutline,
  CopyOutline,
  ExpandOutline,
  CodeOutline,
  FolderOpenOutline,
  CloudOutline,
  QuestionCircleOutline,
  LoginOutline,
  StopOutline,
  InfoCircleOutline,
  BulbOutline,
  UploadOutline,
  EditOutline
];

@Component({
  selector: 'app-connect',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    NzCardModule,
    NzButtonModule,
    NzInputModule,
    NzIconModule,
    NzToolTipModule,
    NzSpinModule,
    NzCollapseModule,
    NzSegmentedModule,
    NzAlertModule,
    NzTagModule,
    NzDividerModule
  ],
  templateUrl: './connect.component.html',
  styleUrls: ['./connect.component.scss']
})
export class ConnectComponent {
  private apiService = inject(ApiService);
  private router = inject(Router);
  private message = inject(NzMessageService);
  private authService = inject(AuthService);
  private sanitizer = inject(DomSanitizer);
  private iconService = inject(NzIconService);

  constructor() {
    // Register icons
    this.iconService.addIcon(...icons);
  }

  kubeconfig = '';
  inputMethod: 'file' | 'text' = 'file';
  inputOptions = [
    { label: '上传文件', value: 'file', icon: 'upload' },
    { label: '文本输入', value: 'text', icon: 'edit' }
  ];
  selectedFile: File | null = null;
  isConnecting = false;
  isDragOver = false;
  previewExpanded = true;

  onFileSelected(event: any): void {
    const file: File = event.target.files[0];
    if (file) {
      this.selectedFile = file;
      const reader = new FileReader();
      reader.onload = (e: any) => {
        this.kubeconfig = e.target.result;
      };
      reader.readAsText(file);
    }
  }

  onDragOver(event: DragEvent): void {
    event.preventDefault();
    this.isDragOver = true;
  }

  onDragLeave(event: DragEvent): void {
    event.preventDefault();
    this.isDragOver = false;
  }

  onDrop(event: DragEvent): void {
    event.preventDefault();
    this.isDragOver = false;
    
    const files = event.dataTransfer?.files;
    if (files && files.length > 0) {
      const file = files[0];
      this.selectedFile = file;
      const reader = new FileReader();
      reader.onload = (e: any) => {
        this.kubeconfig = e.target.result;
      };
      reader.readAsText(file);
    }
  }

  onInputMethodChange(value: string | number): void {
    this.inputMethod = value as 'file' | 'text';
    this.kubeconfig = '';
    this.selectedFile = null;
  }

  clearFile(): void {
    this.selectedFile = null;
    this.kubeconfig = '';
  }

  getFileSize(bytes: number): string {
    if (!bytes || bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }

  validateKubeconfig(): boolean {
    if (!this.kubeconfig.trim()) {
      return false;
    }
    
    try {
      const hasApiVersion = this.kubeconfig.includes('apiVersion');
      const hasClusters = this.kubeconfig.includes('clusters');
      const hasUsers = this.kubeconfig.includes('users');
      const hasContexts = this.kubeconfig.includes('contexts');
      
      return hasApiVersion && hasClusters && hasUsers && hasContexts;
    } catch {
      return false;
    }
  }

  copyToClipboard(): void {
    if (this.kubeconfig) {
      navigator.clipboard.writeText(this.kubeconfig).then(() => {
        this.message.success('已复制到剪贴板');
      }).catch(err => {
        console.error('复制失败:', err);
        this.message.error('复制失败');
      });
    }
  }

  getLineCount(): number {
    if (!this.kubeconfig) return 0;
    return this.kubeconfig.split('\n').length;
  }

  getLineNumbers(): number[] {
    const count = this.getLineCount();
    return Array.from({ length: count }, (_, i) => i + 1);
  }

  getHighlightedYaml(): SafeHtml {
    if (!this.kubeconfig) return '';
    
    const lines = this.kubeconfig.split('\n');
    const highlightedLines = lines.map(line => this.highlightYamlLine(line));
    return this.sanitizer.bypassSecurityTrustHtml(highlightedLines.join('\n'));
  }

  private highlightYamlLine(line: string): string {
    if (!line.trim()) return line;

    if (line.trim().startsWith('#')) {
      return `<span class="yaml-comment">${this.escapeHtml(line)}</span>`;
    }

    const colonIndex = line.indexOf(':');
    if (colonIndex !== -1) {
      const key = line.substring(0, colonIndex);
      const rest = line.substring(colonIndex);
      
      const listMatch = key.match(/^(\s*-\s*)/);
      if (listMatch) {
        const prefix = listMatch[1];
        const actualKey = key.substring(prefix.length);
        return `<span class="yaml-list">${this.escapeHtml(prefix)}</span><span class="yaml-key">${this.escapeHtml(actualKey)}</span>${this.highlightValue(rest)}`;
      }
      
      return `<span class="yaml-key">${this.escapeHtml(key)}</span>${this.highlightValue(rest)}`;
    }

    const listOnlyMatch = line.match(/^(\s*-\s*)(.*)$/);
    if (listOnlyMatch) {
      return `<span class="yaml-list">${this.escapeHtml(listOnlyMatch[1])}</span>${this.highlightValue(': ' + listOnlyMatch[2]).substring(1)}`;
    }

    return this.escapeHtml(line);
  }

  private highlightValue(rest: string): string {
    const colonMatch = rest.match(/^(:\s*)(.*)/);
    if (!colonMatch) return this.escapeHtml(rest);
    
    const colon = colonMatch[1];
    const value = colonMatch[2];
    
    if (!value.trim()) {
      return `<span class="yaml-colon">${this.escapeHtml(colon)}</span>`;
    }
    
    if (value.startsWith('"') || value.startsWith("'")) {
      return `<span class="yaml-colon">${this.escapeHtml(colon)}</span><span class="yaml-string">${this.escapeHtml(value)}</span>`;
    }
    
    if (/^(true|false)$/i.test(value.trim())) {
      return `<span class="yaml-colon">${this.escapeHtml(colon)}</span><span class="yaml-boolean">${this.escapeHtml(value)}</span>`;
    }
    
    if (/^-?\d+(\.\d+)?$/.test(value.trim())) {
      return `<span class="yaml-colon">${this.escapeHtml(colon)}</span><span class="yaml-number">${this.escapeHtml(value)}</span>`;
    }
    
    if (/^https?:\/\//.test(value.trim())) {
      return `<span class="yaml-colon">${this.escapeHtml(colon)}</span><span class="yaml-url">${this.escapeHtml(value)}</span>`;
    }
    
    return `<span class="yaml-colon">${this.escapeHtml(colon)}</span><span class="yaml-value">${this.escapeHtml(value)}</span>`;
  }

  private escapeHtml(text: string): string {
    return text
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  openInModal(): void {
    const newWindow = window.open('', '_blank', 'width=900,height=700');
    if (newWindow) {
      newWindow.document.write(`
        <!DOCTYPE html>
        <html>
        <head>
          <title>Kubeconfig - ${this.selectedFile?.name || 'config'}</title>
          <style>
            body {
              margin: 0;
              padding: 24px;
              background: #1e1e1e;
              color: #d4d4d4;
              font-family: 'SF Mono', 'Monaco', 'Consolas', monospace;
              font-size: 13px;
              line-height: 1.6;
            }
            pre {
              margin: 0;
              white-space: pre-wrap;
              word-wrap: break-word;
            }
          </style>
        </head>
        <body>
          <pre>${this.escapeHtml(this.kubeconfig)}</pre>
        </body>
        </html>
      `);
      newWindow.document.close();
    }
  }

  onConnect(): void {
    if (!this.kubeconfig.trim()) {
      this.message.warning('请输入 kubeconfig 内容');
      return;
    }

    if (!this.validateKubeconfig()) {
      this.message.error('kubeconfig 格式不正确，请检查配置');
      return;
    }

    this.isConnecting = true;
    this.apiService.connect(this.kubeconfig)
      .pipe(finalize(() => {
        this.isConnecting = false;
      }))
      .subscribe({
        next: () => {
          this.authService.saveKubeconfig(this.kubeconfig);
          this.message.success('🎉 连接成功！正在跳转到集群管理页面...');
          this.router.navigate(['/clusters']);
        },
        error: (error) => {
          console.error('连接失败:', error);
          let errorMessage = '连接失败，请检查 kubeconfig 配置';
          if (error.status === 401) {
            errorMessage = '认证失败，请检查 kubeconfig 中的凭据信息';
          } else if (error.status === 0) {
            errorMessage = '无法连接到 Kubernetes API 服务器，请检查网络和配置';
          }
          this.message.error(errorMessage);
        }
      });
  }
}
