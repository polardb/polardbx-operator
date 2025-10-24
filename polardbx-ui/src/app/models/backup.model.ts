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
    phase?: 'Pending' | 'Running' | 'Completed' | 'Failed';
    startTime?: string;
    completionTime?: string;
    message?: string;
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