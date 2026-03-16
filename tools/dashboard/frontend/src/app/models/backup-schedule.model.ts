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

// BackupStorage types
export type BackupStorage = 'oss' | 'sftp' | 's3';

// CleanPolicy types
export type CleanPolicyType = 'Retain' | 'Delete' | 'OnFailure';

// BackupStorageProvider interface
export interface BackupStorageProvider {
  storageName: BackupStorage;
  sink: string;
}

// PolarDBXClusterReference interface
export interface PolarDBXClusterReference {
  name: string;
  namespace?: string;
}

// Duration interface (matches metav1.Duration)
export interface Duration {
  duration: number; // Duration in nanoseconds
}

// PolarDBXBackupSpec interface
export interface PolarDBXBackupSpec {
  cluster: PolarDBXClusterReference;
  retentionTime?: Duration;
  cleanPolicy?: CleanPolicyType;
  storageProvider?: BackupStorageProvider;
  preferredBackupRole?: string;
}

// PolarDBXBackupScheduleSpec interface
export interface PolarDBXBackupScheduleSpec {
  schedule: string; // Cron expression
  suspend?: boolean;
  maxBackupCount?: number;
  backupSpec: PolarDBXBackupSpec;
}

// PolarDBXBackupScheduleStatus interface
export interface PolarDBXBackupScheduleStatus {
  lastBackupTime?: string; // ISO timestamp
  nextBackupTime?: string; // ISO timestamp
  lastBackup?: string;
}

// Main PolarDBXBackupSchedule interface
export interface PolarDBXBackupSchedule {
  apiVersion?: string;
  kind?: string;
  metadata: K8sMetadata;
  spec: PolarDBXBackupScheduleSpec;
  status?: PolarDBXBackupScheduleStatus;
}

// PolarDBXBackupScheduleList interface
export interface PolarDBXBackupScheduleList {
  apiVersion?: string;
  kind?: string;
  metadata?: {
    continue?: string;
    remainingItemCount?: number;
    resourceVersion?: string;
    selfLink?: string;
  };
  items: PolarDBXBackupSchedule[];
}

// Helper interfaces for creating backup schedules
export interface CreateBackupScheduleRequest {
  name: string;
  namespace?: string;
  schedule: string;
  suspend?: boolean;
  maxBackupCount?: number;
  clusterName: string;
  retentionTime?: Duration;
  cleanPolicy?: CleanPolicyType;
  storageProvider?: BackupStorageProvider;
  preferredBackupRole?: string;
}

export interface UpdateBackupScheduleRequest extends Partial<CreateBackupScheduleRequest> {
  resourceVersion?: string;
}

// Validation helpers
export interface BackupScheduleValidationError {
  field: string;
  message: string;
}

// Predefined cron schedules for UI
export interface CronScheduleOption {
  label: string;
  value: string;
  description: string;
}

export const PREDEFINED_CRON_SCHEDULES: CronScheduleOption[] = [
  {
    label: 'Daily at 2:00 AM',
    value: '0 2 * * *',
    description: 'Run backup every day at 2:00 AM'
  },
  {
    label: 'Daily at 3:00 AM',
    value: '0 3 * * *',
    description: 'Run backup every day at 3:00 AM'
  },
  {
    label: 'Weekly (Sunday 2:00 AM)',
    value: '0 2 * * 0',
    description: 'Run backup every Sunday at 2:00 AM'
  },
  {
    label: 'Weekly (Monday 1:00 AM)',
    value: '0 1 * * 1',
    description: 'Run backup every Monday at 1:00 AM'
  },
  {
    label: 'Every 6 hours',
    value: '0 */6 * * *',
    description: 'Run backup every 6 hours'
  },
  {
    label: 'Every 12 hours',
    value: '0 */12 * * *',
    description: 'Run backup every 12 hours'
  },
  {
    label: 'Hourly',
    value: '0 * * * *',
    description: 'Run backup every hour'
  }
];

// Storage provider options
export const STORAGE_PROVIDER_OPTIONS = [
  { label: 'Alibaba Cloud OSS', value: 'oss' as BackupStorage },
  { label: 'Amazon S3/MinIO', value: 's3' as BackupStorage },
  { label: 'SFTP Server', value: 'sftp' as BackupStorage }
];

// Clean policy options
export const CLEAN_POLICY_OPTIONS = [
  { label: 'Retain (Keep backup files)', value: 'Retain' as CleanPolicyType },
  { label: 'Delete (Remove backup files)', value: 'Delete' as CleanPolicyType },
  { label: 'On Failure (Delete only on failure)', value: 'OnFailure' as CleanPolicyType }
];

// Backup role options
export const BACKUP_ROLE_OPTIONS = [
  { label: 'Follower (Recommended)', value: 'follower' },
  { label: 'Leader', value: 'leader' },
  { label: 'Any', value: '' }
];