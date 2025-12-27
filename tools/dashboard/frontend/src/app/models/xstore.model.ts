import type { RestoreBinlogSource, RestoreStorageProvider } from './restore.model';

type StringMap = Record<string, string>;
type UnknownMap = Record<string, unknown>;

interface SchedulingToleration {
  readonly key?: string;
  readonly operator?: string;
  readonly value?: string;
  readonly effect?: string;
  readonly tolerationSeconds?: number;
}

export interface XStore {
  metadata: {
    name: string;
    namespace: string;
    creationTimestamp: string;
    uid?: string;
    resourceVersion?: string;
    labels?: Record<string, string>;
    annotations?: Record<string, string>;
  };
  spec: {
    engine?: string;
    serviceName?: string;
    serviceType?: string;
    serviceLabels?: StringMap;
    privileges?: XStorePrivilege[];
    topology: XStoreTopology;
    config: XStoreConfig;
    upgradeStrategy?: 'Force' | 'BestEffort';
    parameterTemplate: XStoreParameterTemplate;
    readonly?: boolean;
    primaryCluster?: string;
    primaryXStore?: string;
    restore?: XStoreRestoreSpec;
    tde?: XStoreTDE;
    exclusive?: boolean;
  tolerations?: readonly SchedulingToleration[];
  };
  status?: {
    phase?: 'Creating' | 'Running' | 'Updating' | 'Failed' | 'Deleting';
    observedGeneration?: number;
    conditions?: XStoreCondition[];
    stage?: string;
    replicaStatus?: XStoreReplicaStatus;
    detailedStatus?: unknown;
  };
}

export interface XStorePrivilege {
  type: string;
  name: string;
  host?: string;
  privileges?: string[];
}

export interface XStoreTopology {
  nodeCount: number;
  nodeSets?: XStoreNodeSet[];
}

export interface XStoreNodeSet {
  name: string;
  role: 'Leader' | 'Follower' | 'Logger';
  replicas: number;
  hostNetwork?: boolean;
  template: XStoreNodeTemplate;
}

export interface XStoreNodeTemplate {
  metadata?: {
    labels?: StringMap;
    annotations?: StringMap;
  };
  spec: {
    image?: string;
    imagePullPolicy?: string;
  imagePullSecrets?: readonly UnknownMap[];
    resources?: {
      requests?: StringMap;
      limits?: StringMap;
    };
    hostNetwork?: boolean;
  tolerations?: readonly SchedulingToleration[];
    affinity?: unknown;
    volumes?: XStoreVolume[];
  };
}

export interface XStoreVolume {
  name: string;
  size: string;
  storageClass?: string;
  hostPath?: {
    path: string;
    type?: string;
  };
}

export interface XStoreConfig {
  dynamic?: UnknownMap;
  mycnf?: UnknownMap;
}

export interface XStoreParameterTemplate {
  name?: string;
  namespace?: string;
}

export interface XStoreRestoreSpec {
  backupset?: string;
  from?: {
    clusterName?: string;
    backupSelector?: StringMap;
    backupSetPath?: string;
  };
  storageProvider?: RestoreStorageProvider;
  time?: string;
  timezone?: string;
  pitrEndpoint?: string;
  binlogSource?: RestoreBinlogSource;
}

export interface XStoreTDE {
  enable?: boolean;
  keyringPath?: string;
}

export interface XStoreCondition {
  type: string;
  status: 'True' | 'False' | 'Unknown';
  lastTransitionTime: string;
  reason?: string;
  message?: string;
}

export interface XStoreReplicaStatus {
  ready: number;
  total: number;
}

export interface CreateXStoreRequest {
  name: string;
  namespace?: string;
  engine?: string;
  nodeCount: number;
  resources?: {
    requests?: StringMap;
    limits?: StringMap;
  };
  storage?: {
    size: string;
    storageClass?: string;
  };
  // Extended options for topology/templates
  serviceType?: 'ClusterIP' | 'NodePort' | 'LoadBalancer';
  hostNetwork?: boolean;
  cnReplicas?: number;
  cnCpu?: string;
  cnMemory?: string;
  gmsCpu?: string;
  gmsMemory?: string;
  diskQuota?: string;
  version?: string;
  parameterTemplateName?: string;
  storageClass?: string;
}