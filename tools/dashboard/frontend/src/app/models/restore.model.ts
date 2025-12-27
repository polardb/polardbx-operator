// Restore and PITR Models
// Based on document analysis, these are the most critical missing APIs
// PolarDB-X Operator has complete recovery capabilities but Management Platform had ZERO recovery support

type StringStringMap = Record<string, string>;

interface RestoreProgress {
  readonly percentage?: number;
  readonly currentStep?: string;
  readonly estimatedTimeRemaining?: string;
}

export interface RestoreStorageProvider {
  readonly type: 'oss' | 's3' | 'sftp';
  readonly config: StringStringMap;
}

export interface RestoreFromSpec {
  // Note: Backend field name is XStoreName but json tag is "clusterName"
  readonly clusterName?: string;
  readonly backupSelector?: StringStringMap;
  readonly backupSetPath?: string;
}

export interface RestoreBinlogSource {
  readonly namespace?: string;
  readonly checksum?: string;
  readonly storageProvider?: RestoreStorageProvider;
}

export interface RestoreSpec {
  // Note: Backend json tag is "backupset" (lowercase)
  readonly backupset?: string;
  readonly time?: string;
  readonly from?: RestoreFromSpec;
  readonly storageProvider?: RestoreStorageProvider;
  readonly timezone?: string;
  // Note: Backend has typo in json tag: "pitrEndpoiint" instead of "pitrEndpoint"
  readonly pitrEndpoiint?: string;
  readonly binlogSource?: RestoreBinlogSource;
}

export interface PITRStatus {
  readonly prepareJobEndpoint?: string;         // PITR preparation job endpoint
  readonly job?: string;                        // Job name
  readonly phase?: string;                      // Current phase
  readonly progress?: RestoreProgress;
}

// Request types for API calls
export interface RestoreClusterRequest {
  // REST API accepts both legacy and canonical field names.
  readonly backupSet?: string;
  readonly backupName?: string;
  readonly targetCluster?: string;
  readonly targetName?: string;
  readonly storageProvider?: RestoreStorageProvider;
  readonly time?: string;
  readonly timezone?: string;
}

export interface PITRRequest {
  readonly time?: string;
  readonly targetTime?: string;
  readonly timezone?: string;
  readonly backupSet?: string;
  readonly backupName?: string;
  readonly targetCluster?: string;
  readonly targetName?: string;
  readonly storageProvider?: RestoreStorageProvider;
}

export interface RestoreStatusResponse {
  readonly clusterName: string;
  readonly namespace: string;
  readonly phase: string;
  readonly stage: string;
  readonly isRestoring: boolean;
  readonly restoreSpec?: RestoreSpec;
  readonly pitrStatus?: PITRStatus;
  readonly observedGeneration?: number;
  readonly conditions?: readonly RestoreCondition[];
}

export interface RestoreJob {
  readonly clusterName: string;
  readonly namespace: string;
  readonly phase: string;
  readonly stage: string;
  readonly restoreSpec?: RestoreSpec;
  readonly pitrStatus?: PITRStatus;
  // Note: Backend has typo in json tag: "pitrEndpoiint"
  readonly pitrEndpoiint?: string;
  // Keep backward compatibility
  readonly pitrEndpoint?: string;
  readonly observedGeneration?: number;
  readonly conditions?: readonly RestoreCondition[];
}

// UI-specific interfaces for restoration
export interface RestoreJobWithStatus extends RestoreJob {
  readonly isRunning?: boolean;
  readonly isCompleted?: boolean;
  readonly isFailed?: boolean;
  readonly displayStatus?: string;
  readonly displayProgress?: string;
  readonly startTime?: string;
  readonly estimatedCompletion?: string;
  readonly canCancel?: boolean;
  readonly restoreType?: 'backup' | 'pitr';
  readonly sourceCluster?: string;
  readonly creationTimestamp?: string;
}

export interface RestoreWizardStep {
  readonly stepNumber: number;
  readonly title: string;
  readonly description: string;
  readonly isCompleted: boolean;
  readonly isActive: boolean;
  readonly canSkip?: boolean;
}

export interface RestoreWizardData {
  readonly sourceCluster: string;
  readonly targetCluster?: string;
  readonly restoreType: 'backup' | 'pitr';
  readonly backupSet?: string;
  readonly pitrTime?: string;
  readonly pitrTimezone?: string;
  readonly storageProvider?: RestoreStorageProvider;
  readonly validateData?: boolean;
  readonly createNewCluster?: boolean;
  readonly newClusterName?: string;
}

// API Response types
export interface RestoreResponse {
  readonly message: string;
  readonly cluster: unknown;                    // PolarDBXCluster object
  readonly restoreSpec: RestoreSpec;
}

export interface PITRResponse {
  readonly message: string;
  readonly cluster: unknown;                    // PolarDBXCluster object
  readonly pitrTime: string;
  readonly restoreSpec: RestoreSpec;
}

export interface CancelRestoreResponse {
  readonly message: string;
  readonly cluster: unknown;                    // PolarDBXCluster object
}

// Constants for UI
export const RESTORE_PHASES = {
  PENDING: 'Pending',
  RESTORING: 'Restoring',
  RUNNING: 'Running',
  FAILED: 'Failed'
} as const;

export const RESTORE_STAGES = {
  EMPTY: 'Empty',
  CLEAN: 'Clean',
  REBALANCE_START: 'RebalanceStart',
  REBALANCE_WATCH: 'RebalanceWatch'
} as const;

export type RestorePhase = typeof RESTORE_PHASES[keyof typeof RESTORE_PHASES];
export type RestoreStage = typeof RESTORE_STAGES[keyof typeof RESTORE_STAGES];

export interface RestoreCondition {
  readonly type: string;
  readonly status: string;
  readonly lastTransitionTime?: string;
  readonly lastUpdateTime?: string;
  readonly reason?: string;
  readonly message?: string;
}
