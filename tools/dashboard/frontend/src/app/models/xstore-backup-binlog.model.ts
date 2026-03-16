export interface XStoreBackupBinlog {
  apiVersion: string;
  kind: string;
  metadata: {
    name: string;
    namespace: string;
    uid?: string;
    creationTimestamp?: string;
    labels?: Record<string, string>;
    annotations?: Record<string, string>;
  };
  spec: {
    xstoreName: string;
    xstoreUid?: string;
    remoteExpireLogHours?: number | string;
    localExpireLogHours?: number | string;
    maxLocalBinlogCount?: number;
    pointInTimeRecover?: boolean;
    binlogChecksum?: string;
    storageProvider: {
      storageName: string;
      sink: string;
    };
  };
  status?: {
    phase?: string;
    message?: string;
    startTime?: string;
    endTime?: string;
  };
}

export interface CreateXStoreBackupBinlogRequest {
  metadata: {
    name: string;
    namespace: string;
    labels?: Record<string, string>;
    annotations?: Record<string, string>;
  };
  spec: {
    xstoreName: string;
    xstoreUid?: string;
    remoteExpireLogHours?: number | string;
    localExpireLogHours?: number | string;
    maxLocalBinlogCount?: number;
    pointInTimeRecover?: boolean;
    binlogChecksum?: string;
    storageProvider: {
      storageName: string;
      sink: string;
    };
  };
}

export interface UpdateXStoreBackupBinlogRequest {
  metadata?: {
    labels?: Record<string, string>;
    annotations?: Record<string, string>;
  };
  spec?: {
    xstoreName?: string;
    xstoreUid?: string;
    remoteExpireLogHours?: number | string;
    localExpireLogHours?: number | string;
    maxLocalBinlogCount?: number;
    pointInTimeRecover?: boolean;
    binlogChecksum?: string;
    storageProvider?: {
      storageName?: string;
      sink?: string;
    };
  };
}
