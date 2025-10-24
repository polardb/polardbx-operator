// XStoreFollower Model - DN Replica Fault Recovery
// Based on document analysis, this is for 备库重搭 (backup database reconstruction)
// This addresses the critical gap in fault recovery management identified in the completeness report

export interface XStoreFollowerSpec {
  readonly local?: boolean;                      // Build the FromPod locally
  readonly role?: 'learner' | 'logger' | 'follower'; // Role type
  readonly nodeName?: string;                    // Dest Node to build the follower on
  readonly fromPodName?: string;                 // Name of the pod used as backup source
  readonly targetPodName?: string;               // Configuration of pod which affect resource schedule
  readonly xStoreName: string;                   // Name of xstore which the follower belongs to
  
  // Legacy fields for compatibility
  readonly xStoreUid?: string;                   // Target XStore UID
  readonly fromXStore?: string;                  // Source XStore for recovery
  readonly fromBackupSet?: string;               // Backup set to recover from
  readonly forceRecreate?: boolean;              // Force recreation of follower
  readonly priority?: number;                    // Recovery priority
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
  readonly tolerations?: Array<{
    key?: string;
    operator?: string;
    value?: string;
    effect?: string;
    tolerationSeconds?: number;
  }>;
  readonly nodeSelector?: { [key: string]: string };
}

export interface XStoreFollowerStatus {
  readonly phase?: string;                       // Running phase of the task
  readonly message?: string;                     // Message about current step
  readonly backupJobName?: string;               // Name of the backup job
  readonly restoreJobName?: string;              // Name of the restore job
  readonly currentJobName?: string;              // Name of the current job
  readonly currentJobTask?: string;              // Task name of the current job
  readonly targetPodName?: string;               // Target pod name
  readonly rebuildPodName?: string;              // Temporary pod name
  readonly toCleanHostPathVolume?: any;          // Host path volume to clean
  readonly rebuildNodeName?: string;             // New node name of the pod (targetNodeName)
  readonly flowFlags?: Array<{
    name?: string;
    value?: boolean;
  }>;
  
  // Legacy fields for compatibility
  readonly stage?: string;                       // Current stage within phase
  readonly conditions?: Array<{
    type: string;
    status: string;
    lastTransitionTime?: string;
    lastUpdateTime?: string;
    reason?: string;
    message?: string;
  }>;
  readonly observedGeneration?: number;
  readonly primaryXStore?: string;               // Primary XStore being followed
  readonly recoveryProgress?: {
    percentage?: number;
    estimatedTimeRemaining?: string;
    bytesTransferred?: number;
    totalBytes?: number;
  };
  readonly lastRecoveryTime?: string;            // Last successful recovery time
  readonly failureCount?: number;                // Number of failed recovery attempts
}

export interface XStoreFollower {
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
  readonly spec: XStoreFollowerSpec;
  readonly status?: XStoreFollowerStatus;
}

export interface XStoreFollowerList {
  readonly apiVersion?: string;
  readonly kind?: string;
  readonly items: XStoreFollower[];
  readonly metadata?: {
    continue?: string;
    remainingItemCount?: number;
    resourceVersion?: string;
    selfLink?: string;
  };
}

// Request/Response types for API calls
export interface CreateXStoreFollowerRequest {
  readonly name?: string;
  readonly xStoreName: string;
  readonly role?: 'learner' | 'logger' | 'follower';
  readonly local?: boolean;
  readonly targetPodName?: string;
  readonly fromPodName?: string;
  readonly nodeName?: string;
  readonly fromXStore?: string;
  readonly fromBackupSet?: string;
  readonly forceRecreate?: boolean;
  readonly priority?: number;
  readonly resources?: XStoreFollowerSpec['resources'];
  readonly nodeSelector?: { [key: string]: string };
}

export interface XStoreFollowerRecoveryRequest {
  readonly followerName: string;
  readonly namespace: string;
  readonly forceRestart?: boolean;
}

// UI-specific interfaces
export interface XStoreFollowerWithStatus extends XStoreFollower {
  readonly isRecovering?: boolean;
  readonly isHealthy?: boolean;
  readonly hasFailures?: boolean;
  readonly displayStatus?: string;
  readonly lastActivity?: string;
}