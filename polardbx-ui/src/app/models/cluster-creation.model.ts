export interface ClusterCreationConfig { [k: string]: any }
export interface ClusterCreationStep { [k: string]: any }
export interface ClusterTemplate { name: string; label?: string; icon?: string; recommended?: boolean; description?: string; config: any }

export const CLUSTER_TEMPLATES: ClusterTemplate[] = [
  { name: 'minimal', label: '最小化', icon: 'bolt', recommended: true, description: '单副本开发测试', config: { topology: { cn: { replicas: 1, resources: { cpu: '500m', memory: '1Gi' } }, dn: { replicas: 1, resources: { cpu: '500m', memory: '1Gi' } }, gms: { replicas: 1, resources: { cpu: '500m', memory: '1Gi' } } }, storage: { storageClassName: 'standard', size: '20Gi', accessMode: 'ReadWriteOnce' }, network: { serviceType: 'ClusterIP' }, security: { enableTLS: false } } }
];

export const CREATION_STEPS: ClusterCreationStep[] = [];
export const RESOURCE_PRESETS: Array<{ cpu: string; memory: string }> = [
  { cpu: '500m', memory: '1Gi' },
  { cpu: '1', memory: '2Gi' },
  { cpu: '2', memory: '4Gi' }
];

export const STORAGE_CLASSES: Array<{ value: string; label: string; description?: string }> = [
  { value: 'standard', label: 'standard', description: '默认存储类' }
];

export const SERVICE_TYPES: Array<{ value: string; label: string; icon: string; description?: string }> = [
  { value: 'ClusterIP', label: 'ClusterIP', icon: 'lan', description: '仅集群内访问' },
  { value: 'NodePort', label: 'NodePort', icon: 'upload', description: '通过节点端口访问' },
  { value: 'LoadBalancer', label: 'LoadBalancer', icon: 'cloud', description: '通过云负载均衡访问' }
];


