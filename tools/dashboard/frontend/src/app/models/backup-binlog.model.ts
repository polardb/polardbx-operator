// Kubernetes metadata interface
export interface K8sMetadata {
  name: string;
  namespace?: string;
  uid?: string;
  resourceVersion?: string;
  creationTimestamp?: string;
  labels?: Record<string, string>;
  annotations?: Record<string, string>;
}

// Storage provider types
export type BackupStorageType = 'oss' | 'sftp' | 's3';

// Backup binlog phases
export type BackupBinlogPhase = '' | 'running' | 'checkExpiredFile' | 'deleting';

// IntOrString type for compatibility with Kubernetes API
export interface IntOrString {
  type?: number;
  intVal?: number;
  strVal?: string;
}

// Helper function to create IntOrString from number
export function intOrStringFromInt(value: number): IntOrString {
  return {
    type: 0,
    intVal: value,
  };
}

// Helper function to create IntOrString from string
export function intOrStringFromString(value: string): IntOrString {
  return {
    type: 1,
    strVal: value,
  };
}

// Helper function to get value from IntOrString
export function getIntOrStringValue(value: IntOrString | undefined): string {
  if (!value) return '';
  if (value.type === 1 && value.strVal) return value.strVal;
  if (value.type === 0 && value.intVal !== undefined) return value.intVal.toString();
  return '';
}

// BackupStorageProvider interface
export interface BackupStorageProvider {
  storageName?: BackupStorageType;
  sink?: string;
}

// PolarDBXBackupBinlogSpec interface based on actual CRD
export interface PolarDBXBackupBinlogSpec {
  pxcName: string; // Required field
  pxcUid?: string;
  remoteExpireLogHours?: IntOrString;
  localExpireLogHours?: IntOrString;
  maxLocalBinlogCount?: number;
  pointInTimeRecover?: boolean;
  storageProvider?: BackupStorageProvider;
  binlogChecksum?: string;
}

// PolarDBXBackupBinlogStatus interface based on actual CRD
export interface PolarDBXBackupBinlogStatus {
  observedGeneration?: number;
  phase?: BackupBinlogPhase;
  checkExpireFileLastTime?: number;
  lastDeletedFiles?: string[];
}

// Main PolarDBXBackupBinlog interface
export interface PolarDBXBackupBinlog {
  apiVersion?: string;
  kind?: string;
  metadata: K8sMetadata;
  spec: PolarDBXBackupBinlogSpec;
  status?: PolarDBXBackupBinlogStatus;
}

// PolarDBXBackupBinlogList interface
export interface PolarDBXBackupBinlogList {
  apiVersion?: string;
  kind?: string;
  metadata?: {
    continue?: string;
    remainingItemCount?: number;
    resourceVersion?: string;
    selfLink?: string;
  };
  items: PolarDBXBackupBinlog[];
}

// Helper interfaces for creating backup binlogs
export interface CreateBackupBinlogRequest {
  name: string;
  namespace?: string;
  pxcName: string;
  pxcUid?: string;
  remoteExpireLogHours?: number | string;
  localExpireLogHours?: number | string;
  maxLocalBinlogCount?: number;
  pointInTimeRecover?: boolean;
  storageProvider?: BackupStorageProvider;
  binlogChecksum?: string;
}

export interface UpdateBackupBinlogRequest extends Partial<CreateBackupBinlogRequest> {
  resourceVersion?: string;
}

// Validation helpers
export interface BackupBinlogValidationError {
  field: string;
  message: string;
}

// Storage provider options
export interface StorageProviderOption {
  label: string;
  value: BackupStorageType;
  description: string;
  icon: string;
  defaultSink: string;
}

export const STORAGE_PROVIDER_OPTIONS: StorageProviderOption[] = [
  {
    label: 'Alibaba Cloud OSS',
    value: 'oss',
    description: 'Alibaba Cloud Object Storage Service',
    icon: 'cloud',
    defaultSink: 'oss://bucket-name/binlogs/'
  },
  {
    label: 'Amazon S3',
    value: 's3',
    description: 'S3-compatible storage (Amazon S3, MinIO, etc.)',
    icon: 'aws',
    defaultSink: 's3://bucket-name/binlogs/'
  },
  {
    label: 'SFTP Server',
    value: 'sftp',
    description: 'Secure File Transfer Protocol',
    icon: 'server',
    defaultSink: 'sftp://server:22/path/to/binlogs/'
  }
];

// Checksum algorithm options
export interface ChecksumOption {
  label: string;
  value: string;
  description: string;
}

export const CHECKSUM_OPTIONS: ChecksumOption[] = [
  {
    label: 'CRC32',
    value: 'CRC32',
    description: 'Fast and lightweight checksum algorithm (default)'
  },
  {
    label: 'MD5',
    value: 'MD5',
    description: 'MD5 hash algorithm'
  },
  {
    label: 'SHA256',
    value: 'SHA256',
    description: 'Secure SHA-256 algorithm'
  }
];

// Retention policy presets
export interface RetentionPolicy {
  name: string;
  label: string;
  description: string;
  remoteExpireHours: number;
  localExpireHours: number;
  maxLocalBinlogCount: number;
}

export const RETENTION_POLICIES: RetentionPolicy[] = [
  {
    name: 'development',
    label: 'Development',
    description: 'Short retention for development environments',
    remoteExpireHours: 24, // 1 day
    localExpireHours: 2,   // 2 hours
    maxLocalBinlogCount: 20
  },
  {
    name: 'testing',
    label: 'Testing',
    description: 'Medium retention for testing environments',
    remoteExpireHours: 168, // 7 days
    localExpireHours: 7,    // 7 hours
    maxLocalBinlogCount: 60
  },
  {
    name: 'production',
    label: 'Production',
    description: 'Long retention for production environments',
    remoteExpireHours: 720, // 30 days
    localExpireHours: 24,   // 1 day
    maxLocalBinlogCount: 200
  },
  {
    name: 'compliance',
    label: 'Compliance',
    description: 'Extended retention for compliance requirements',
    remoteExpireHours: 8760, // 1 year
    localExpireHours: 48,    // 2 days
    maxLocalBinlogCount: 500
  }
];

// Helper functions
export function validatePxcName(name: string): boolean {
  // Kubernetes DNS subdomain name validation
  const nameRegex = /^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/;
  return nameRegex.test(name) && name.length <= 253;
}

export function validateStorageSink(sink: string, storageType: BackupStorageType): boolean {
  if (!sink) return false;
  
  switch (storageType) {
    case 'oss':
      return sink.startsWith('oss://');
    case 's3':
      return sink.startsWith('s3://');
    case 'sftp':
      return sink.startsWith('sftp://');
    default:
      return false;
  }
}

export function getBackupBinlogDescription(binlog: PolarDBXBackupBinlog): string {
  const provider = binlog.spec.storageProvider?.storageName || 'unknown';
  const pitr = binlog.spec.pointInTimeRecover ? 'PITR enabled' : 'PITR disabled';
  return `${binlog.spec.pxcName} → ${provider.toUpperCase()} (${pitr})`;
}

export function getPhaseColor(phase: BackupBinlogPhase): string {
  switch (phase) {
    case 'running': return 'success';
    case 'checkExpiredFile': return 'info';
    case 'deleting': return 'warning';
    case '': return 'secondary';
    default: return 'secondary';
  }
}

export function getPhaseLabel(phase: BackupBinlogPhase): string {
  switch (phase) {
    case 'running': return 'Running';
    case 'checkExpiredFile': return 'Checking Files';
    case 'deleting': return 'Deleting';
    case '': return 'Initializing';
    default: return 'Unknown';
  }
}

export function getStorageIcon(storageType: BackupStorageType): string {
  const option = STORAGE_PROVIDER_OPTIONS.find(opt => opt.value === storageType);
  return option?.icon || 'storage';
}

export function formatExpireTime(hours: IntOrString | undefined): string {
  if (!hours) return 'Not specified';
  
  const value = getIntOrStringValue(hours);
  const numValue = parseInt(value);
  
  if (isNaN(numValue)) return value;
  
  if (numValue < 24) {
    return `${numValue} hours`;
  } else if (numValue < 168) {
    return `${Math.round(numValue / 24)} days`;
  } else if (numValue < 8760) {
    return `${Math.round(numValue / 168)} weeks`;
  } else {
    return `${Math.round(numValue / 8760)} years`;
  }
}

export function getRetentionSummary(binlog: PolarDBXBackupBinlog): string {
  const remoteTime = formatExpireTime(binlog.spec.remoteExpireLogHours);
  const localTime = formatExpireTime(binlog.spec.localExpireLogHours);
  const maxCount = binlog.spec.maxLocalBinlogCount || 0;
  
  return `Remote: ${remoteTime}, Local: ${localTime} (max ${maxCount} files)`;
}

export function getLastDeletedFilesCount(status: PolarDBXBackupBinlogStatus | undefined): number {
  return status?.lastDeletedFiles?.length || 0;
}

export function formatLastCheckTime(timestamp: number | undefined): string {
  if (!timestamp) return 'Never';
  return new Date(timestamp * 1000).toLocaleString();
}

export function getHealthStatus(binlog: PolarDBXBackupBinlog): 'healthy' | 'warning' | 'error' {
  if (!binlog.status) return 'warning';
  
  const phase = binlog.status.phase;
  if (phase === 'running') return 'healthy';
  if (phase === 'deleting') return 'error';
  return 'warning';
}

export function getHealthColor(health: 'healthy' | 'warning' | 'error'): string {
  switch (health) {
    case 'healthy': return 'success';
    case 'warning': return 'warning';
    case 'error': return 'danger';
    default: return 'secondary';
  }
}

export function getPITRCapability(binlog: PolarDBXBackupBinlog): string {
  if (!binlog.spec.pointInTimeRecover) {
    return 'Point-in-Time Recovery disabled';
  }
  
  const remoteRetention = formatExpireTime(binlog.spec.remoteExpireLogHours);
  return `Point-in-Time Recovery enabled (${remoteRetention} retention)`;
}

export function generateBackupBinlogName(clusterName: string, suffix?: string): string {
  const baseName = `${clusterName}-binlog`;
  return suffix ? `${baseName}-${suffix}` : baseName;
}

export function createDefaultSpec(pxcName: string): PolarDBXBackupBinlogSpec {
  return {
    pxcName,
    remoteExpireLogHours: intOrStringFromInt(168), // 7 days
    localExpireLogHours: intOrStringFromInt(7),    // 7 hours
    maxLocalBinlogCount: 60,
    pointInTimeRecover: true,
    binlogChecksum: 'CRC32',
    storageProvider: {
      storageName: 'oss',
      sink: 'oss://your-bucket/binlogs/'
    }
  };
}