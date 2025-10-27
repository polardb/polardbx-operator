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

// SystemTask specific types based on actual CRD definition
export type SystemTaskType = 'BalanceResource';

// SystemTask phases
export type SystemTaskPhase = '' | 'RebuildTaskPhase' | 'BalanceRolePhase' | 'Success';

// Resource requirements for containers
export interface ResourceRequirements {
  requests?: ResourceList;
  limits?: ResourceList;
}

export interface ResourceList {
  cpu?: string;
  memory?: string;
}

// SystemTaskSpec interface based on actual CRD
export interface SystemTaskSpec {
  taskType?: SystemTaskType;
  cnReplicas?: number;
  cnResources?: ResourceRequirements;
  dnResources?: ResourceRequirements;
  nodes?: string[];
}

// Balance resource status (specific to BalanceResource task type)
export interface StBalanceResourceStatus {
  rebuildTaskName?: string;
  rebuildFinish?: boolean;
  balanceLeaderFinish?: boolean;
}

// SystemTaskStatus interface based on actual CRD
export interface SystemTaskStatus {
  phase?: SystemTaskPhase;
  stBalanceResourceStatus?: StBalanceResourceStatus;
}

// Main SystemTask interface
export interface SystemTask {
  apiVersion?: string;
  kind?: string;
  metadata: K8sMetadata;
  spec: SystemTaskSpec;
  status?: SystemTaskStatus;
}

// SystemTaskList interface
export interface SystemTaskList {
  apiVersion?: string;
  kind?: string;
  metadata?: {
    continue?: string;
    remainingItemCount?: number;
    resourceVersion?: string;
    selfLink?: string;
  };
  items: SystemTask[];
}

// Helper interfaces for creating system tasks
export interface CreateSystemTaskRequest {
  name: string;
  namespace?: string;
  taskType?: SystemTaskType;
  cnReplicas?: number;
  cnResources?: ResourceRequirements;
  dnResources?: ResourceRequirements;
  nodes?: string[];
}

export interface UpdateSystemTaskRequest extends Partial<CreateSystemTaskRequest> {
  resourceVersion?: string;
}

// Validation helpers
export interface SystemTaskValidationError {
  field: string;
  message: string;
}

// Predefined task types for UI (based on actual CRD)
export interface TaskTypeOption {
  label: string;
  value: SystemTaskType;
  description: string;
  icon: string;
}

export const TASK_TYPE_OPTIONS: TaskTypeOption[] = [
  {
    label: 'Balance Resource',
    value: 'BalanceResource',
    description: 'Balance resources across cluster nodes',
    icon: 'balance'
  }
];

// Resource configuration presets
export interface ResourcePreset {
  name: string;
  label: string;
  description: string;
  cnResources: ResourceRequirements;
  dnResources: ResourceRequirements;
}

export const RESOURCE_PRESETS: ResourcePreset[] = [
  {
    name: 'small',
    label: 'Small',
    description: 'Small resource allocation for development',
    cnResources: {
      requests: { cpu: '100m', memory: '256Mi' },
      limits: { cpu: '500m', memory: '512Mi' }
    },
    dnResources: {
      requests: { cpu: '200m', memory: '512Mi' },
      limits: { cpu: '1', memory: '1Gi' }
    }
  },
  {
    name: 'medium',
    label: 'Medium',
    description: 'Medium resource allocation for testing',
    cnResources: {
      requests: { cpu: '1', memory: '2Gi' },
      limits: { cpu: '2', memory: '4Gi' }
    },
    dnResources: {
      requests: { cpu: '2', memory: '4Gi' },
      limits: { cpu: '4', memory: '8Gi' }
    }
  },
  {
    name: 'large',
    label: 'Large',
    description: 'Large resource allocation for production',
    cnResources: {
      requests: { cpu: '4', memory: '8Gi' },
      limits: { cpu: '8', memory: '16Gi' }
    },
    dnResources: {
      requests: { cpu: '8', memory: '16Gi' },
      limits: { cpu: '16', memory: '32Gi' }
    }
  }
];

// Helper functions
export function getTaskTypeIcon(type: SystemTaskType): string {
  const option = TASK_TYPE_OPTIONS.find(opt => opt.value === type);
  return option?.icon || 'task';
}

export function getTaskDescription(task: SystemTask): string {
  const typeLabel = TASK_TYPE_OPTIONS.find(opt => opt.value === task.spec.taskType)?.label || task.spec.taskType || 'Unknown';
  const replicas = task.spec.cnReplicas || 0;
  const nodes = task.spec.nodes?.length || 0;
  
  return `${typeLabel} - ${replicas} CN replicas, ${nodes} nodes`;
}

export function formatResourceValue(value: string | undefined): string {
  if (!value) return 'Not specified';
  return value;
}

export function getPhaseColor(phase: SystemTaskPhase): string {
  switch (phase) {
    case 'Success': return 'success';
    case 'RebuildTaskPhase': return 'primary';
    case 'BalanceRolePhase': return 'info';
    case '': return 'secondary';
    default: return 'secondary';
  }
}

export function getPhaseLabel(phase: SystemTaskPhase): string {
  switch (phase) {
    case 'Success': return 'Success';
    case 'RebuildTaskPhase': return 'Rebuilding';
    case 'BalanceRolePhase': return 'Balancing';
    case '': return 'Initializing';
    default: return 'Unknown';
  }
}