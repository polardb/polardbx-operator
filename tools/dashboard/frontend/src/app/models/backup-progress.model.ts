/**
 * 备份进度统一数据模型
 * 参考: Kubernetes Job/Pod status, AWS Backup, Velero
 */

/**
 * 备份任务阶段
 */
export type BackupPhase = 
  | 'Pending'           // 等待中
  | 'Running'           // 运行中
  | 'Completed'         // 已完成
  | 'Failed'            // 失败
  | 'Cancelled'         // 已取消
  | 'Paused'            // 已暂停
  | 'Unknown';          // 未知

/**
 * 备份任务子阶段 (用于细粒度进度)
 */
export type BackupSubPhase =
  | 'Initializing'      // 初始化
  | 'Validating'        // 验证
  | 'Snapshotting'      // 快照中
  | 'Transferring'      // 传输中
  | 'Compressing'       // 压缩中
  | 'Encrypting'        // 加密中
  | 'Verifying'         // 校验中
  | 'Finalizing'        // 完成中
  | 'CleaningUp';       // 清理中

/**
 * 进度状态 (UI展示状态)
 */
export type ProgressStatus = 'success' | 'exception' | 'active' | 'normal';

/**
 * 进度信息接口
 */
export interface ProgressInfo {
  /** 百分比 (0-100) */
  percentage: number;
  
  /** 已处理字节数 */
  processedBytes?: number;
  
  /** 总字节数 */
  totalBytes?: number;
  
  /** 已处理文件数 */
  processedFiles?: number;
  
  /** 总文件数 */
  totalFiles?: number;
  
  /** 传输速率 (bytes/s) */
  transferRate?: number;
  
  /** 预计剩余时间 (秒) */
  estimatedTimeRemaining?: number;
  
  /** 最后更新时间 */
  lastUpdateTime?: string;
}

/**
 * 备份进度元数据
 */
export interface BackupProgressMetadata {
  /** 主阶段 */
  phase: BackupPhase;
  
  /** 子阶段 (可选) */
  subPhase?: BackupSubPhase;
  
  /** 进度信息 */
  progress?: ProgressInfo;
  
  /** 开始时间 */
  startTime?: string;
  
  /** 完成时间 */
  completionTime?: string;
  
  /** 错误消息 */
  errorMessage?: string;
  
  /** 警告信息 */
  warnings?: string[];
  
  /** 状态消息 */
  message?: string;
  
  /** 重试次数 */
  retryCount?: number;
  
  /** 最大重试次数 */
  maxRetries?: number;
}

/**
 * 进度计算选项
 */
export interface ProgressCalculationOptions {
  /** 是否基于字节数计算 */
  useBytes?: boolean;
  
  /** 是否基于文件数计算 */
  useFiles?: boolean;
  
  /** 是否基于阶段权重计算 */
  usePhaseWeights?: boolean;
  
  /** 阶段权重映射 */
  phaseWeights?: Record<BackupPhase | BackupSubPhase, number>;
  
  /** 默认进度 (当无法计算时) */
  defaultProgress?: number;
  
  /** 是否启用智能估算 */
  enableSmartEstimation?: boolean;
}

/**
 * 进度显示选项
 */
export interface ProgressDisplayOptions {
  /** 显示模式 */
  mode?: 'line' | 'circle' | 'dashboard';
  
  /** 尺寸 */
  size?: 'small' | 'default' | 'large';
  
  /** 是否显示百分比文本 */
  showPercentage?: boolean;
  
  /** 是否显示详细信息 */
  showDetails?: boolean;
  
  /** 是否显示速率 */
  showRate?: boolean;
  
  /** 是否显示剩余时间 */
  showTimeRemaining?: boolean;
  
  /** 是否显示文件计数 */
  showFileCount?: boolean;
  
  /** 是否显示字节计数 */
  showByteCount?: boolean;
  
  /** 自定义格式化函数 */
  customFormat?: (percent: number, metadata: BackupProgressMetadata) => string;
  
  /** 是否启用动画 */
  animated?: boolean;
  
  /** 主题色 */
  color?: string;
}

/**
 * 进度事件
 */
export interface ProgressEvent {
  /** 事件类型 */
  type: 'start' | 'progress' | 'complete' | 'error' | 'cancel';
  
  /** 时间戳 */
  timestamp: Date;
  
  /** 进度元数据 */
  metadata: BackupProgressMetadata;
  
  /** 附加数据 */
  data?: unknown;
}

/**
 * 进度监听器
 */
export type ProgressListener = (event: ProgressEvent) => void;
