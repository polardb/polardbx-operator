// Restore and PITR Models
// Based on document analysis, these are the most critical missing APIs
// PolarDB-X Operator has complete recovery capabilities but Management Platform had ZERO recovery support

export interface RestoreStorageProvider {
  readonly type: 'oss' | 's3' | 'sftp';
  readonly config: { [key: string]: string };
}

export interface RestoreFromSpec {
  readonly polardbxName?: string;
  readonly backupSet?: string;
  readonly xStoreName?: string;
}

export interface RestoreSpec {
  readonly backupSet?: string;                  // Backup set name for restoration
  readonly time?: string;                       // Time for PITR (Point-in-Time Recovery)
  readonly from?: RestoreFromSpec;              // Source information
  readonly storageProvider?: RestoreStorageProvider;
  readonly timezone?: string;                   // Timezone for time parsing
}

export interface PITRStatus {
  readonly prepareJobEndpoint?: string;         // PITR preparation job endpoint
  readonly job?: string;                        // Job name
  readonly phase?: string;                      // Current phase
  readonly progress?: {
    percentage?: number;
    currentStep?: string;
    estimatedTimeRemaining?: string;
  };
}

// Request types for API calls
export interface RestoreClusterRequest {
  readonly backupSet?: string;
  readonly time?: string;                       // For PITR
  readonly targetCluster?: string;              // For restore to new cluster
  readonly storageProvider?: {
    type: 'oss' | 's3' | 'sftp';
    config: { [key: string]: string };
  };
}

export interface PITRRequest {
  readonly time: string;                        // Required: PITR timestamp
  readonly backupSet?: string;                  // Optional: specific backup set
  readonly targetCluster?: string;              // Optional: restore to different cluster
  readonly timezone?: string;                   // Optional: timezone for time parsing
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
  readonly conditions?: Array<{
    type: string;
    status: string;
    lastTransitionTime?: string;
    lastUpdateTime?: string;
    reason?: string;
    message?: string;
  }>;
}

export interface RestoreJob {
  readonly clusterName: string;
  readonly namespace: string;
  readonly phase: string;
  readonly stage: string;
  readonly restoreSpec?: RestoreSpec;
  readonly pitrStatus?: PITRStatus;
  readonly pitrEndpoint?: string;
  readonly observedGeneration?: number;
  readonly conditions?: Array<{
    type: string;
    status: string;
    lastTransitionTime?: string;
    lastUpdateTime?: string;
    reason?: string;
    message?: string;
  }>;
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
  readonly cluster: any;                        // PolarDBXCluster object
  readonly restoreSpec: RestoreSpec;
}

export interface PITRResponse {
  readonly message: string;
  readonly cluster: any;                        // PolarDBXCluster object
  readonly pitrTime: string;
  readonly restoreSpec: RestoreSpec;
}

export interface CancelRestoreResponse {
  readonly message: string;
  readonly cluster: any;                        // PolarDBXCluster object
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