import { Component, inject } from '@angular/core';
import { Router } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatSnackBar, MatSnackBarModule } from '@angular/material/snack-bar';
import { MatButtonToggleModule } from '@angular/material/button-toggle';
import { MatIconModule } from '@angular/material/icon';
import { MatExpansionModule } from '@angular/material/expansion';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { ApiService } from '../../services/api.service';
import { AuthService } from '../../services/auth.service';
import { CommonModule } from '@angular/common';

@Component({
  selector: 'app-connect',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatCardModule,
    MatFormFieldModule,
    MatInputModule,
    MatButtonModule,
    MatSnackBarModule,
    MatButtonToggleModule,
    MatIconModule,
    MatExpansionModule,
    MatTooltipModule,
    MatProgressSpinnerModule
  ],
  templateUrl: './connect.component.html',
  styleUrls: ['./connect.component.scss']
})
export class ConnectComponent {
  private apiService = inject(ApiService);
  private router = inject(Router);
  private snackBar = inject(MatSnackBar);
  private authService = inject(AuthService);

  kubeconfig = '';
  inputMethod: 'text' | 'file' = 'file';
  selectedFile: File | null = null;
  isConnecting = false;
  isDragOver = false;

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

  onInputMethodChange(method?: 'text' | 'file'): void {
    if (method) {
      this.inputMethod = method;
    }
    this.kubeconfig = '';
    this.selectedFile = null;
  }

  clearFile(): void {
    this.selectedFile = null;
    this.kubeconfig = '';
  }

  getFileSize(bytes: number): string {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }

  validateKubeconfig(): boolean {
    if (!this.kubeconfig.trim()) {
      return false;
    }
    
    // 基本的 YAML 格式检查
    try {
      // 检查是否包含基本的 kubeconfig 字段
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
        this.snackBar.open('已复制到剪贴板', '关闭', {
          duration: 2000
        });
      }).catch(err => {
        console.error('复制失败:', err);
        this.snackBar.open('复制失败', '关闭', {
          duration: 2000
        });
      });
    }
  }

  onConnect() {
    if (!this.kubeconfig.trim()) {
      this.snackBar.open('请输入 kubeconfig 内容', '关闭', {
        duration: 3000,
        panelClass: ['error-snackbar']
      });
      return;
    }

    if (!this.validateKubeconfig()) {
      this.snackBar.open('kubeconfig 格式不正确，请检查配置', '关闭', {
        duration: 4000,
        panelClass: ['error-snackbar']
      });
      return;
    }

    this.isConnecting = true;
    this.apiService.connect(this.kubeconfig).subscribe({
      next: () => {
        console.log('API连接成功，开始保存kubeconfig和跳转...');
        // 保存 kubeconfig 到 localStorage
        this.authService.saveKubeconfig(this.kubeconfig);
        console.log('kubeconfig已保存，认证状态:', this.authService.isAuthenticated());
        this.snackBar.open('🎉 连接成功！正在跳转到集群管理页面...', '关闭', {
          duration: 3000,
          panelClass: ['success-snackbar']
        });
        // 跳转到集群列表页
        console.log('准备跳转到 /clusters');
        this.router.navigate(['/clusters']).then(success => {
          console.log('路由跳转结果:', success);
        });
      },
      error: (error) => {
        console.error('连接失败:', error);
        let errorMessage = '连接失败，请检查 kubeconfig 配置';
        if (error.status === 401) {
          errorMessage = '认证失败，请检查 kubeconfig 中的凭据信息';
        } else if (error.status === 0) {
          errorMessage = '无法连接到 Kubernetes API 服务器，请检查网络和配置';
        }
        this.snackBar.open(errorMessage, '关闭', {
          duration: 6000,
          panelClass: ['error-snackbar']
        });
      },
      complete: () => {
        this.isConnecting = false;
      }
    });
  }
}
