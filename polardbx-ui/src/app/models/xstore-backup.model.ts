// XStoreBackup Model - Storage-level Backup
// Based on document analysis, this completes the unified backup module
// This addresses the gap in storage-level backup management

export interface BackupStorageOSS {
  readonly accessKeyId: string;
  readonly accessKeySecret: string;
  readonly bucket: string;
  readonly endpoint: string;
  readonly prefix?: string;
}

export interface BackupStorageS3 {
  readonly accessKeyId: string;
  readonly secretAccessKey: string;
  readonly bucket: string;
  readonly region: string;
  readonly endpoint?: string;
  readonly prefix?: string;
}

export interface BackupStorageSFTP {
  readonly host: string;
  readonly port?: number;
  readonly username: string;
  readonly password?: string;
  readonly privateKey?: string;
  readonly remotePath: string;
}

export interface XStoreBackupSpec {
  readonly xStoreName: string;                   // Target XStore name
  readonly xStoreUid?: string;                  // Target XStore UID
  readonly backupType?: 'full' | 'incremental'; // Backup type
  readonly storageProvider: {
    type: 'oss' | 's3' | 'sftp';
    oss?: BackupStorageOSS;
    s3?: BackupStorageS3;
    sftp?: BackupStorageSFTP;
    config?: { [key: string]: string };
  };
  readonly retentionPolicy?: {
    retain?: number;                            // Number of backups to retain
    retainDays?: number;                        // Days to retain backups
    retainHours?: number;                       // Hours to retain backups
  };
  readonly resources?: {
    requests?: {
      cpu?: string;
      memory?: string;
      storage?: string;
    };
    limits?: {
      cpu?: string;
      memory?: string;
      storage?: string;
    };
  };
  readonly schedule?: string;                   // Cron expression for scheduled backups
  readonly compression?: boolean;               // Enable compression
  readonly encryption?: {
    enabled?: boolean;
    key?: string;
    algorithm?: string;
  };
  readonly tolerations?: Array<{
    key?: string;
    operator?: string;
    value?: string;
    effect?: string;
    tolerationSeconds?: number;
  }>;
  readonly nodeSelector?: { [key: string]: string };
}

export interface XStoreBackupStatus {
  readonly phase?: string;                      // Current phase: Pending, Running, Completed, Failed
  readonly stage?: string;                      // Current stage within phase
  readonly conditions?: Array<{
    type: string;
    status: string;
    lastTransitionTime?: string;
    lastUpdateTime?: string;
    reason?: string;
    message?: string;
  }>;
  readonly observedGeneration?: number;
  readonly startTime?: string;                  // Backup start time
  readonly completionTime?: string;             // Backup completion time
  readonly backupSize?: number;                 // Size of backup in bytes
  readonly compressedSize?: number;             // Compressed size in bytes
  readonly progress?: {
    percentage?: number;
    estimatedTimeRemaining?: string;
    bytesTransferred?: number;
    totalBytes?: number;
  };
  readonly lastSuccessfulBackup?: string;       // Last successful backup time
  readonly failureCount?: number;               // Number of failed backup attempts
  readonly backupLocation?: string;             // Storage location of backup
  readonly checksum?: string;                   // Backup checksum for integrity
}

export interface XStoreBackup {
  readonly apiVersion?: string;
  readonly kind?: string;
  readonly metadata: {
    name: string;
    namespace: string;
    uid?: string;
    resourceVersion?: string;
    generation?: number;
    creationTimestamp?: string;
    deletionTimestamp?: string;
    labels?: { [key: string]: string };
    annotations?: { [key: string]: string };
    finalizers?: string[];
  };
  readonly spec: XStoreBackupSpec;
  readonly status?: XStoreBackupStatus;
}

export interface XStoreBackupList {
  readonly apiVersion?: string;
  readonly kind?: string;
  readonly items: XStoreBackup[];
  readonly metadata?: {
    continue?: string;
    remainingItemCount?: number;
    resourceVersion?: string;
    selfLink?: string;
  };
}

// Request/Response types for API calls
export interface CreateXStoreBackupRequest {
  readonly xStoreName: string;
  readonly backupType?: 'full' | 'incremental';
  readonly storageProvider: XStoreBackupSpec['storageProvider'];
  readonly retentionPolicy?: XStoreBackupSpec['retentionPolicy'];
  readonly schedule?: string;
  readonly compression?: boolean;
  readonly encryption?: XStoreBackupSpec['encryption'];
  readonly resources?: XStoreBackupSpec['resources'];
  readonly nodeSelector?: { [key: string]: string };
}

export interface XStoreBackupRestoreRequest {
  readonly backupName: string;
  readonly namespace: string;
  readonly targetXStore: string;
  readonly restoreType?: 'full' | 'incremental';
  readonly pointInTime?: string;
}

// UI-specific interfaces
export interface XStoreBackupWithStatus extends XStoreBackup {
  readonly isRunning?: boolean;
  readonly isCompleted?: boolean;
  readonly isFailed?: boolean;
  readonly displayStatus?: string;
  readonly displaySize?: string;
  readonly displayDuration?: string;
  readonly canRestore?: boolean;
  readonly storageType?: string;
  readonly compressionRatio?: number;
}