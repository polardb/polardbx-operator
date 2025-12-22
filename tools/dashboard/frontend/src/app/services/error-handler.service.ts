import { Injectable, inject } from '@angular/core';
import { MatSnackBar } from '@angular/material/snack-bar';
import { Router } from '@angular/router';
import { HttpErrorResponse } from '@angular/common/http';

export interface ErrorInfo {
  message: string;
  code?: string;
  details?: unknown;
  timestamp: Date;
  userAction?: string;
}

@Injectable({
  providedIn: 'root'
})
export class ErrorHandlerService {
  private snackBar = inject(MatSnackBar);
  private router = inject(Router);

  private errorHistory: ErrorInfo[] = [];
  private readonly MAX_ERROR_HISTORY = 50;

  /**
   * 处理 HTTP 错误
   */
  handleHttpError(error: HttpErrorResponse, context?: string): void {
    let errorMessage = '操作失败';
    let userAction = '';
    
    // 根据状态码提供具体的错误信息
    switch (error.status) {
      case 0:
        errorMessage = '网络连接失败，请检查网络连接';
        userAction = '检查网络';
        break;
      case 400:
        errorMessage = '请求参数错误';
        userAction = '检查输入';
        break;
      case 401:
        errorMessage = 'Kubeconfig 认证失败，请重新连接';
        userAction = '重新连接';
        this.handleAuthError();
        break;
      case 403:
        errorMessage = '权限不足，请检查 Kubeconfig 权限';
        userAction = '检查权限';
        break;
      case 404:
        errorMessage = '资源不存在';
        userAction = '刷新页面';
        break;
      case 409:
        errorMessage = '资源冲突，可能已存在同名资源';
        userAction = '检查资源名称';
        break;
      case 422:
        errorMessage = '数据验证失败';
        userAction = '检查输入格式';
        break;
      case 500:
        errorMessage = '服务器内部错误';
        userAction = '稍后重试';
        break;
      case 502:
      case 503:
      case 504:
        errorMessage = '服务暂时不可用';
        userAction = '稍后重试';
        break;
      default:
        errorMessage = `操作失败 (${error.status})`;
        userAction = '稍后重试';
    }

    // 如果有具体的错误信息，使用服务器返回的信息
    if (error.error && typeof error.error === 'string') {
      errorMessage = error.error;
    } else if (error.error && error.error.message) {
      errorMessage = error.error.message;
    }

    // 添加上下文信息
    if (context) {
      errorMessage = `${context}: ${errorMessage}`;
    }

    this.logError({
      message: errorMessage,
      code: error.status.toString(),
      details: error,
      timestamp: new Date(),
      userAction
    });

    this.showErrorSnackBar(errorMessage, userAction);
  }

  /**
   * 处理一般错误
   */
  handleError(error: unknown, context?: string): void {
    let errorMessage = '发生未知错误';
    
    if (error instanceof Error) {
      errorMessage = error.message;
    } else if (typeof error === 'string') {
      errorMessage = error;
    }

    if (context) {
      errorMessage = `${context}: ${errorMessage}`;
    }

    this.logError({
      message: errorMessage,
      details: error,
      timestamp: new Date()
    });

    this.showErrorSnackBar(errorMessage);
  }

  /**
   * 处理认证错误
   */
  private handleAuthError(): void {
    // 清除会话数据
    sessionStorage.removeItem('kubeconfig');
    
    // 延迟跳转，让用户看到错误信息
    setTimeout(() => {
      this.router.navigate(['/connect']);
    }, 2000);
  }

  /**
   * 显示错误提示
   */
  private showErrorSnackBar(message: string, action?: string): void {
    const snackBarRef = this.snackBar.open(message, action || '关闭', {
      duration: action ? 8000 : 5000,
      panelClass: ['error-snackbar'],
      horizontalPosition: 'center',
      verticalPosition: 'top'
    });

    // 如果有用户操作，处理点击事件
    if (action) {
      snackBarRef.onAction().subscribe(() => {
        this.handleUserAction(action);
      });
    }
  }

  /**
   * 处理用户操作
   */
  private handleUserAction(action: string): void {
    switch (action) {
      case '重新连接':
        this.router.navigate(['/connect']);
        break;
      case '刷新页面':
        window.location.reload();
        break;
      case '检查网络':
        this.showNetworkDiagnostics();
        break;
      default:
        // 其他操作暂不处理
        break;
    }
  }

  /**
   * 显示网络诊断信息
   */
  private showNetworkDiagnostics(): void {
    this.snackBar.open(
      '请检查：1. 网络连接 2. 后端服务是否运行 3. 防火墙设置',
      '知道了',
      {
        duration: 10000,
        panelClass: ['info-snackbar']
      }
    );
  }

  /**
   * 记录错误
   */
  private logError(errorInfo: ErrorInfo): void {
    // 添加到错误历史
    this.errorHistory.unshift(errorInfo);
    
    // 限制历史记录数量
    if (this.errorHistory.length > this.MAX_ERROR_HISTORY) {
      this.errorHistory = this.errorHistory.slice(0, this.MAX_ERROR_HISTORY);
    }

    // 控制台输出（开发环境）- 结构化日志，避免 [object Object]
    try {
      const printable = {
        message: errorInfo.message,
        code: errorInfo.code,
        userAction: errorInfo.userAction,
        timestamp: errorInfo.timestamp,
        details: errorInfo.details instanceof Error ? {
          name: (errorInfo.details as any).name,
          message: (errorInfo.details as any).message,
          stack: (errorInfo.details as any).stack
        } : errorInfo.details
      };
      console.error('Error:', printable);
    } catch (_) {
      console.error('Error:', errorInfo);
    }

    // 可以在这里添加错误上报逻辑
    // this.reportError(errorInfo);
  }

  /**
   * 获取错误历史
   */
  getErrorHistory(): ErrorInfo[] {
    return [...this.errorHistory];
  }

  /**
   * 清除错误历史
   */
  clearErrorHistory(): void {
    this.errorHistory = [];
  }

  /**
   * 显示成功消息
   */
  showSuccess(message: string): void {
    this.snackBar.open(message, '关闭', {
      duration: 3000,
      panelClass: ['success-snackbar'],
      horizontalPosition: 'center',
      verticalPosition: 'top'
    });
  }

  /**
   * 显示信息消息
   */
  showInfo(message: string): void {
    this.snackBar.open(message, '关闭', {
      duration: 4000,
      panelClass: ['info-snackbar'],
      horizontalPosition: 'center',
      verticalPosition: 'top'
    });
  }

  /**
   * 显示警告消息
   */
  showWarning(message: string): void {
    this.snackBar.open(message, '关闭', {
      duration: 5000,
      panelClass: ['warning-snackbar'],
      horizontalPosition: 'center',
      verticalPosition: 'top'
    });
  }
}