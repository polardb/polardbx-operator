export interface NodeResources {
  cpu: string;
  memory: string;
}

export interface ClusterNodeConfig extends Record<string, unknown> {
  replicas: number;
  resources: NodeResources;
}

export interface ClusterTopologyConfig extends Record<string, unknown> {
  cn: ClusterNodeConfig;
  dn: ClusterNodeConfig;
  gms: ClusterNodeConfig;
  cdc?: ClusterNodeConfig;
}

export interface StorageConfig extends Record<string, unknown> {
  storageClassName?: string;
  size?: string;
  accessMode?: string;
  accessModes?: string[];
}

export interface NetworkConfig extends Record<string, unknown> {
  serviceType?: string;
  loadBalancerClass?: string;
  hostNetwork?: boolean;  // 使用宿主网络模式
}

export interface SecurityConfig extends Record<string, unknown> {
  enableTLS?: boolean;
  secretName?: string;
}

export interface AdvancedConfig extends Record<string, unknown> {
  enableMonitoring?: boolean;
  enableBackup?: boolean;
  enableLogCollection?: boolean;
  customLabels?: Record<string, string>;
  customAnnotations?: Record<string, string>;
  nodeSelector?: Record<string, string>;  // 节点选择器
  shareGMS?: boolean;  // GMS共享极简模式
}

// 镜像配置
export interface ImageConfig extends Record<string, unknown> {
  repository?: string;  // 镜像仓库
  tag?: string;         // 镜像标签
  pullPolicy?: 'Always' | 'IfNotPresent' | 'Never';  // 拉取策略
}

export interface ClusterCreationConfig extends Record<string, unknown> {
  name?: string;
  namespace?: string;
  description?: string;
  version?: string;
  image?: ImageConfig;       // 镜像配置
  topology: ClusterTopologyConfig;
  storage: StorageConfig;
  network?: NetworkConfig;
  security?: SecurityConfig;
  advanced?: AdvancedConfig;
}

export type ClusterCreationStep = Record<string, unknown>;

export interface ClusterTemplate {
  name: string;
  label?: string;
  icon?: string;
  recommended?: boolean;
  description?: string;
  config: ClusterCreationConfig;
}

export const CLUSTER_TEMPLATES: readonly ClusterTemplate[] = [
  {
    name: '最小化',
    label: 'minimal',
    icon: 'bolt',
    recommended: true,
    description: '单副本配置，适合开发测试环境，资源占用最少',
    config: {
      topology: {
        cn: { replicas: 1, resources: { cpu: '500m', memory: '1Gi' } },
        dn: { replicas: 1, resources: { cpu: '500m', memory: '1Gi' } },
        gms: { replicas: 1, resources: { cpu: '500m', memory: '1Gi' } }
      },
      storage: { storageClassName: 'standard', size: '20Gi', accessMode: 'ReadWriteOnce' },
      network: { serviceType: 'ClusterIP' },
      security: { enableTLS: false }
    }
  },
  {
    name: '标准版',
    label: 'standard',
    icon: 'cluster',
    recommended: false,
    description: '多副本高可用配置，适合预生产和小型生产环境',
    config: {
      topology: {
        cn: { replicas: 2, resources: { cpu: '2', memory: '4Gi' } },
        dn: { replicas: 2, resources: { cpu: '2', memory: '4Gi' } },
        gms: { replicas: 1, resources: { cpu: '1', memory: '2Gi' } }
      },
      storage: { storageClassName: 'standard', size: '50Gi', accessMode: 'ReadWriteOnce' },
      network: { serviceType: 'ClusterIP' },
      security: { enableTLS: false }
    }
  },
  {
    name: '生产版',
    label: 'production',
    icon: 'cloud-server',
    recommended: false,
    description: '高性能高可用配置，适合大规模生产环境',
    config: {
      topology: {
        cn: { replicas: 3, resources: { cpu: '4', memory: '8Gi' } },
        dn: { replicas: 3, resources: { cpu: '4', memory: '8Gi' } },
        gms: { replicas: 3, resources: { cpu: '2', memory: '4Gi' } }
      },
      storage: { storageClassName: 'standard', size: '100Gi', accessMode: 'ReadWriteOnce' },
      network: { serviceType: 'LoadBalancer' },
      security: { enableTLS: true }
    }
  },
  {
    name: '自定义',
    label: 'custom',
    icon: 'setting',
    recommended: false,
    description: '完全自定义配置，适合有特殊需求的场景',
    config: {
      topology: {
        cn: { replicas: 1, resources: { cpu: '1', memory: '2Gi' } },
        dn: { replicas: 1, resources: { cpu: '1', memory: '2Gi' } },
        gms: { replicas: 1, resources: { cpu: '500m', memory: '1Gi' } }
      },
      storage: { storageClassName: 'standard', size: '20Gi', accessMode: 'ReadWriteOnce' },
      network: { serviceType: 'ClusterIP' },
      security: { enableTLS: false }
    }
  }
];

export const CREATION_STEPS: ClusterCreationStep[] = [];

export interface ResourcePreset {
  cpu: string;
  memory: string;
}

export const RESOURCE_PRESETS: readonly ResourcePreset[] = [
  { cpu: '500m', memory: '1Gi' },
  { cpu: '1', memory: '2Gi' },
  { cpu: '2', memory: '4Gi' }
];

// 存储类选项（从API动态加载时使用）
export interface StorageClassOption {
  value: string;
  label: string;
  description?: string;
  isDefault?: boolean;
  provisioner?: string;
}

// 默认的存储类列表（当API不可用时的后备）
export const STORAGE_CLASSES: readonly StorageClassOption[] = [
  { value: 'standard', label: 'standard', description: '默认存储类' }
];

// 命名空间选项
export interface NamespaceOption {
  name: string;
  status: string;
}

// PolarDB-X 版本信息
export interface PolarDBXVersionInfo {
  version: string;
  label: string;
  description?: string;
  recommended?: boolean;
  deprecated?: boolean;
}

// 默认版本列表（当API不可用时的后备）
export const DEFAULT_POLARDBX_VERSIONS: readonly PolarDBXVersionInfo[] = [
  { version: '8.0.18', label: '8.0.18 (最新稳定版)', recommended: true },
  { version: '8.0.17', label: '8.0.17' },
  { version: '8.0.16', label: '8.0.16' },
  { version: '5.7.14', label: '5.7.14 (旧版本)', deprecated: true }
];

// 存储大小选项
export interface StorageSizeOption {
  value: string;
  label: string;
}

export const STORAGE_SIZES: readonly StorageSizeOption[] = [
  { value: '10Gi', label: '10 GB' },
  { value: '20Gi', label: '20 GB' },
  { value: '50Gi', label: '50 GB' },
  { value: '100Gi', label: '100 GB' },
  { value: '200Gi', label: '200 GB' },
  { value: '500Gi', label: '500 GB' },
  { value: '1Ti', label: '1 TB' }
];

export interface ServiceTypeOption {
  value: string;
  label: string;
  icon: string;
  description?: string;
}

export const SERVICE_TYPES: readonly ServiceTypeOption[] = [
  { value: 'ClusterIP', label: 'ClusterIP', icon: 'lan', description: '仅集群内访问' },
  { value: 'NodePort', label: 'NodePort', icon: 'upload', description: '通过节点端口访问' },
  { value: 'LoadBalancer', label: 'LoadBalancer', icon: 'cloud', description: '通过云负载均衡访问' }
];

// 后端验证错误
export interface ValidationError {
  field: string;
  message: string;
}

// API 响应格式
export interface ClusterCreationResponse {
  error?: string;
  details?: string;
  validationErrors?: ValidationError[];
}


