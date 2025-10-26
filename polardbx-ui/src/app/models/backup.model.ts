export interface PolarDBXBackup {
  metadata: {
    name: string;
    namespace: string;
    creationTimestamp: string;
    uid?: string;
    resourceVersion?: string;
  };
  spec: {
    cluster: {
      name: string;
    };
    backupType?: 'Snapshot' | 'BinlogBackup';
    storageProvider?: {
      storageName?: string;
      sink?: string;
    };
  };
  status?: {
    phase?: 'New' | 'FullBackuping' | 'BackupCollecting' | 'BackupCalculating' | 'BinlogBackuping' | 'MetadataBackuping' | 'Finished' | 'Failed' | 'Deleting' | '';
    startTime?: string;
    endTime?: string;
    backupRootPath?: string;
    backups?: { [key: string]: string };
    xstores?: string[];
    backupSetTimestamp?: { [key: string]: string };
    latestRecoverableTimestamp?: string;
    collectStartIndexMap?: { [key: string]: string };
    collectEndIndexMap?: { [key: string]: string };
    message?: string;
    completionTime?: string;  // 保留兼容性
    heartbeat?: string;  // 心跳时间戳
    clusterSpecSnapshot?: any;  // 集群快照
  };
}

export interface BackupInfo {
  id: string;
  name: string;
  namespace: string;
  completedTime: string;
  type: string;
  status: string;
  phase?: string;
  message?: string;
  backupObject?: PolarDBXBackup;
}

export interface CreateBackupRequest {
  name: string;
  backupType?: 'Snapshot' | 'BinlogBackup';
  storageProvider?: {
    storageName?: string;
    sink?: string;
  };
} 