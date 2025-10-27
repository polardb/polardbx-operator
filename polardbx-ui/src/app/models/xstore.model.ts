export interface XStore {
  metadata: {
    name: string;
    namespace: string;
    creationTimestamp: string;
    uid?: string;
    resourceVersion?: string;
  };
  spec: {
    engine?: string;
    serviceName?: string;
    serviceType?: string;
    serviceLabels?: { [key: string]: string };
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
    tolerations?: any[];
  };
  status?: {
    phase?: 'Creating' | 'Running' | 'Updating' | 'Failed' | 'Deleting';
    observedGeneration?: number;
    conditions?: XStoreCondition[];
    stage?: string;
    replicaStatus?: XStoreReplicaStatus;
    detailedStatus?: any;
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
    labels?: { [key: string]: string };
    annotations?: { [key: string]: string };
  };
  spec: {
    image?: string;
    imagePullPolicy?: string;
    imagePullSecrets?: any[];
    resources?: {
      requests?: { [key: string]: string };
      limits?: { [key: string]: string };
    };
    hostNetwork?: boolean;
    tolerations?: any[];
    affinity?: any;
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
  dynamic?: { [key: string]: any };
  mycnf?: { [key: string]: any };
}

export interface XStoreParameterTemplate {
  name?: string;
  namespace?: string;
}

export interface XStoreRestoreSpec {
  backupset?: string;
  from?: {
    clusterName?: string;
    backupSelector?: { [key: string]: string };
    backupSetPath?: string;
  };
  storageProvider?: any;
  time?: string;
  timezone?: string;
  pitrEndpoint?: string;
  binlogSource?: any;
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
    requests?: { [key: string]: string };
    limits?: { [key: string]: string };
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