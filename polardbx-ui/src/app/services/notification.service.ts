import { Injectable, inject } from '@angular/core';
import { NzMessageRef, NzMessageService } from 'ng-zorro-antd/message';
import { NzModalService, ModalOptions } from 'ng-zorro-antd/modal';
import { Observable } from 'rxjs';

export interface NotificationOptions {
  duration?: number;
  action?: string;
  panelClass?: string[];
  horizontalPosition?: 'start' | 'center' | 'end' | 'left' | 'right';
  verticalPosition?: 'top' | 'bottom';
  pauseOnHover?: boolean;
}

@Injectable({
  providedIn: 'root'
})
export class NotificationService {
  private message = inject(NzMessageService);
  private modal = inject(NzModalService);
  private persistentMessages: NzMessageRef[] = [];


  /**
   * 显示成功通知
   */
  success(message: string, options?: NotificationOptions): void {
    const defaultOptions: NotificationOptions = {
      duration: 3000,
      panelClass: ['success-snackbar'],
      horizontalPosition: 'center',
      verticalPosition: 'top',
      pauseOnHover: true
    };
    
    const finalOptions = { ...defaultOptions, ...options };
    
    this.openMessage('success', message, finalOptions);
  }

  /**
   * 显示错误通知
   */
  error(message: string, options?: NotificationOptions): void {
    const defaultOptions: NotificationOptions = {
      duration: 5000,
      panelClass: ['error-snackbar'],
      horizontalPosition: 'center',
      verticalPosition: 'top',
      pauseOnHover: true
    };
    
    const finalOptions = { ...defaultOptions, ...options };
    
    this.openMessage('error', message, finalOptions);
  }

  /**
   * 显示警告通知
   */
  warning(message: string, options?: NotificationOptions): void {
    const defaultOptions: NotificationOptions = {
      duration: 4000,
      panelClass: ['warning-snackbar'],
      horizontalPosition: 'center',
      verticalPosition: 'top',
      pauseOnHover: true
    };
    
    const finalOptions = { ...defaultOptions, ...options };
    
    this.openMessage('warning', message, finalOptions);
  }

  /**
   * 显示信息通知
   */
  info(message: string, options?: NotificationOptions): void {
    const defaultOptions: NotificationOptions = {
      duration: 4000,
      panelClass: ['info-snackbar'],
      horizontalPosition: 'center',
      verticalPosition: 'top',
      pauseOnHover: true
    };
    
    const finalOptions = { ...defaultOptions, ...options };
    
    this.openMessage('info', message, finalOptions);
  }

  /**
   * 显示确认对话框
   */
  confirm(data: any): Observable<boolean> {
    const options: ModalOptions = {
      nzTitle: data?.title || '确认操作',
      nzContent: this.buildModalContent(data?.message, data?.details),
      nzOkText: data?.confirmText || '确认',
      nzCancelText: data?.cancelText || '取消',
      nzOkDanger: data?.type === 'danger',
      nzClassName: data?.type === 'danger' ? 'notification-modal-danger' : undefined
    };

    return new Observable(observer => {
      const modalRef = this.modal.confirm({
        ...options,
        nzOnOk: () => {
          observer.next(true);
          observer.complete();
        },
        nzOnCancel: () => {
          observer.next(false);
          observer.complete();
        }
      });

      return () => modalRef.destroy();
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
    const ref = this.message.loading(message, {
      nzDuration: 0,
      nzPauseOnHover: true
    });
    this.persistentMessages.push(ref);
  }

  /**
   * 关闭所有通知
   */
  dismissAll(): void {
    this.persistentMessages.forEach(ref => {
      if (ref && ref.messageId) {
        this.message.remove(ref.messageId);
      }
    });
    this.persistentMessages = [];
    this.message.remove();
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

  private openMessage(type: 'success' | 'error' | 'warning' | 'info', message: string, options: NotificationOptions): void {
    const nzDuration = options.duration ?? 3000;
    const actionSuffix = options.action ? `（${options.action}）` : '';
    this.message.create(type, `${message}${actionSuffix}`, {
      nzDuration,
      nzPauseOnHover: options.pauseOnHover ?? true,
      nzAnimate: true
    });
  }

  private buildModalContent(message?: string, details?: string): string {
    if (!details) {
      return message || '';
    }
    const escapedDetails = details.replace(/[&<>'"]/g, (char) => {
      const map: Record<string, string> = {
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;'
      };
      return map[char] || char;
    });
    return `
      <div class="notification-modal-content">
        <p>${message || ''}</p>
        <pre>${escapedDetails}</pre>
      </div>
    `;
  }
}