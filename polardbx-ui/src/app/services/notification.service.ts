import { Injectable, inject } from '@angular/core';
import { MatDialog } from '@angular/material/dialog';
import { MatSnackBar } from '@angular/material/snack-bar';
import { Observable } from 'rxjs';
// Removed ConfirmationDialogComponent import - using native confirm() instead

export interface NotificationOptions {
  duration?: number;
  action?: string;
  panelClass?: string[];
  horizontalPosition?: 'start' | 'center' | 'end' | 'left' | 'right';
  verticalPosition?: 'top' | 'bottom';
}

@Injectable({
  providedIn: 'root'
})
export class NotificationService {
  private snackBar = inject(MatSnackBar);
  private dialog = inject(MatDialog);


  /**
   * 显示成功通知
   */
  success(message: string, options?: NotificationOptions): void {
    const defaultOptions: NotificationOptions = {
      duration: 3000,
      panelClass: ['success-snackbar'],
      horizontalPosition: 'center',
      verticalPosition: 'top'
    };
    
    const finalOptions = { ...defaultOptions, ...options };
    
    this.snackBar.open(message, finalOptions.action || '关闭', {
      duration: finalOptions.duration,
      panelClass: finalOptions.panelClass,
      horizontalPosition: finalOptions.horizontalPosition,
      verticalPosition: finalOptions.verticalPosition
    });
  }

  /**
   * 显示错误通知
   */
  error(message: string, options?: NotificationOptions): void {
    const defaultOptions: NotificationOptions = {
      duration: 5000,
      panelClass: ['error-snackbar'],
      horizontalPosition: 'center',
      verticalPosition: 'top'
    };
    
    const finalOptions = { ...defaultOptions, ...options };
    
    this.snackBar.open(message, finalOptions.action || '关闭', {
      duration: finalOptions.duration,
      panelClass: finalOptions.panelClass,
      horizontalPosition: finalOptions.horizontalPosition,
      verticalPosition: finalOptions.verticalPosition
    });
  }

  /**
   * 显示警告通知
   */
  warning(message: string, options?: NotificationOptions): void {
    const defaultOptions: NotificationOptions = {
      duration: 4000,
      panelClass: ['warning-snackbar'],
      horizontalPosition: 'center',
      verticalPosition: 'top'
    };
    
    const finalOptions = { ...defaultOptions, ...options };
    
    this.snackBar.open(message, finalOptions.action || '关闭', {
      duration: finalOptions.duration,
      panelClass: finalOptions.panelClass,
      horizontalPosition: finalOptions.horizontalPosition,
      verticalPosition: finalOptions.verticalPosition
    });
  }

  /**
   * 显示信息通知
   */
  info(message: string, options?: NotificationOptions): void {
    const defaultOptions: NotificationOptions = {
      duration: 4000,
      panelClass: ['info-snackbar'],
      horizontalPosition: 'center',
      verticalPosition: 'top'
    };
    
    const finalOptions = { ...defaultOptions, ...options };
    
    this.snackBar.open(message, finalOptions.action || '关闭', {
      duration: finalOptions.duration,
      panelClass: finalOptions.panelClass,
      horizontalPosition: finalOptions.horizontalPosition,
      verticalPosition: finalOptions.verticalPosition
    });
  }

  /**
   * 显示确认对话框
   */
  confirm(data: any): Observable<boolean> {
    return new Observable(observer => {
      const result = window.confirm(`${data.title}\n\n${data.message}`);
      observer.next(result);
      observer.complete();
    });
  }

  /**
   * 显示删除确认对话框
   */
  confirmDelete(itemName: string, itemType = '项目'): Observable<boolean> {
    return this.confirm({
      title: `删除${itemType}`,
      message: `确定要删除 "${itemName}" 吗？此操作无法撤销。`,
      confirmText: '删除',
      cancelText: '取消',
      type: 'danger'
    });
  }

  /**
   * 显示操作确认对话框
   */
  confirmAction(
    action: string, 
    target: string, 
    warning?: string
  ): Observable<boolean> {
    let message = `确定要${action} "${target}" 吗？`;
    if (warning) {
      message += `\n\n${warning}`;
    }

    return this.confirm({
      title: `${action}确认`,
      message: message,
      confirmText: action,
      cancelText: '取消',
      type: 'warning'
    });
  }

  /**
   * 显示带详情的确认对话框
   */
  confirmWithDetails(
    title: string,
    message: string,
    details: string,
    type: 'warning' | 'danger' | 'info' = 'info'
  ): Observable<boolean> {
    return this.confirm({
      title: title,
      message: message,
      details: details,
      confirmText: '确认',
      cancelText: '取消',
      type: type
    });
  }

  /**
   * 显示操作成功通知
   */
  operationSuccess(operation: string, target?: string): void {
    let message = `${operation}成功`;
    if (target) {
      message = `${operation} "${target}" 成功`;
    }
    this.success(message);
  }

  /**
   * 显示操作失败通知
   */
  operationError(operation: string, target?: string, error?: string): void {
    let message = `${operation}失败`;
    if (target) {
      message = `${operation} "${target}" 失败`;
    }
    if (error) {
      message += `：${error}`;
    }
    this.error(message);
  }

  /**
   * 显示加载提示
   */
  loading(message: string): void {
    this.info(message, { duration: 0 });
  }

  /**
   * 关闭所有通知
   */
  dismissAll(): void {
    this.snackBar.dismiss();
  }

  /**
   * 显示网络错误通知
   */
  networkError(): void {
    this.error('网络连接失败，请检查网络设置', {
      action: '重试',
      duration: 8000
    });
  }

  /**
   * 显示权限错误通知
   */
  permissionError(): void {
    this.error('权限不足，请检查 Kubeconfig 配置', {
      action: '重新连接',
      duration: 8000
    });
  }

  /**
   * 显示服务器错误通知
   */
  serverError(): void {
    this.error('服务器错误，请稍后重试', {
      action: '重试',
      duration: 6000
    });
  }

  /**
   * 显示数据保存成功通知
   */
  saveSuccess(): void {
    this.success('保存成功');
  }

  /**
   * 显示数据加载失败通知
   */
  loadError(): void {
    this.error('数据加载失败，请刷新页面重试', {
      action: '刷新',
      duration: 6000
    });
  }

  // 别名方法，保持向后兼容性
  showSuccess(message: string, options?: NotificationOptions): void {
    this.success(message, options);
  }

  showError(message: string, options?: NotificationOptions): void {
    this.error(message, options);
  }

  showInfo(message: string, options?: NotificationOptions): void {
    this.info(message, options);
  }

  showWarning(message: string, options?: NotificationOptions): void {
    this.warning(message, options);
  }
}