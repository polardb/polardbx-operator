export interface PolarDBXCluster {
  metadata: {
    name: string;
    namespace: string;
    uid?: string;
    resourceVersion?: string;
    creationTimestamp?: string;
    labels?: Record<string, string>;
    annotations?: Record<string, string>;
  };
  spec: {
    topology: {
      nodes: {
        cn: {
          replicas: number;
          template: {
            spec: {
              resources: {
                requests: {
                  cpu: string;
                  memory: string;
                };
                limits: {
                  cpu: string;
                  memory: string;
                };
              };
            };
          };
        };
        dn: {
          replicas: number;
          template: {
            spec: {
              resources: {
                requests: {
                  cpu: string;
                  memory: string;
                };
                limits: {
                  cpu: string;
                  memory: string;
                };
              };
            };
          };
        };
        gms: {
          replicas: number;
          template: {
            spec: {
              resources: {
                requests: {
                  cpu: string;
                  memory: string;
                };
                limits: {
                  cpu: string;
                  memory: string;
                };
              };
            };
          };
        };
        cdc?: {
          replicas: number;
          template: {
            spec: {
              resources: {
                requests: {
                  cpu: string;
                  memory: string;
                };
                limits: {
                  cpu: string;
                  memory: string;
                };
              };
            };
          };
        };
      };
    };
    config?: {
      cn?: {
        dynamic?: Record<string, unknown>;
        static?: Record<string, unknown>;
      };
      dn?: {
        dynamic?: Record<string, unknown>;
        static?: Record<string, unknown>;
      };
    };
    upgradeStrategy?: {
      type: string;
      rollingUpdate?: {
        maxUnavailable?: string | number;
        maxSurge?: string | number;
      };
    };
    serviceType?: string;
    version?: string;
  };
  status?: {
    phase: 'Creating' | 'Running' | 'Failed' | 'Deleting' | 'Unknown';
    conditions?: {
      type: string;
      status: string;
      lastTransitionTime: string;
      reason?: string;
      message?: string;
    }[];
    observedGeneration?: number;
    detailedStatus?: {
      stage: string;
      conditions?: {
        type: string;
        status: string;
        lastTransitionTime: string;
        reason?: string;
        message?: string;
      }[];
    };
    replicaStatus?: {
      cn?: {
        ready: number;
        total: number;
      };
      dn?: {
        ready: number;
        total: number;
      };
      gms?: {
        ready: number;
        total: number;
      };
      cdc?: {
        ready: number;
        total: number;
      };
    };
  };
}

export interface ClusterSummary {
  name: string;
  namespace: string;
  phase: string;
  age: string;
  cnReplicas: string;
  dnReplicas: string;
  gmsReplicas: string;
  cdcReplicas?: string;
  version?: string;
}

export interface ClusterMetrics {
  cpu: {
    used: number;
    total: number;
    percentage: number;
  };
  memory: {
    used: number;
    total: number;
    percentage: number;
  };
  storage: {
    used: number;
    total: number;
    percentage: number;
  };
  connections: {
    active: number;
    total: number;
  };
}

export interface ClusterEvent {
  type: 'Normal' | 'Warning';
  reason: string;
  message: string;
  timestamp: string;
  source: string;
}

export interface ClusterCreateRequest {
  name: string;
  namespace: string;
  topology: {
    cn: {
      replicas: number;
      cpu: string;
      memory: string;
    };
    dn: {
      replicas: number;
      cpu: string;
      memory: string;
    };
    gms: {
      replicas: number;
      cpu: string;
      memory: string;
    };
    cdc?: {
      replicas: number;
      cpu: string;
      memory: string;
    };
  };
  version?: string;
  config?: {
    cn?: Record<string, unknown>;
    dn?: Record<string, unknown>;
  };
}

export interface ClusterUpdateRequest {
  topology?: {
    cn?: {
      replicas?: number;
      cpu?: string;
      memory?: string;
    };
    dn?: {
      replicas?: number;
      cpu?: string;
      memory?: string;
    };
    gms?: {
      replicas?: number;
      cpu?: string;
      memory?: string;
    };
    cdc?: {
      replicas?: number;
      cpu?: string;
      memory?: string;
    };
  };
  config?: {
    cn?: Record<string, unknown>;
    dn?: Record<string, unknown>;
  };
}

// 工具函数
export class ClusterUtils {
  static getPhaseColor(phase: string): string {
    switch (phase) {
      case 'Running':
        return 'primary';
      case 'Creating':
        return 'accent';
      case 'Failed':
        return 'warn';
      case 'Deleting':
        return 'warn';
      default:
        return 'basic';
    }
  }

  static getPhaseIcon(phase: string): string {
    switch (phase) {
      case 'Running':
        return 'check_circle';
      case 'Creating':
        return 'hourglass_empty';
      case 'Failed':
        return 'error';
      case 'Deleting':
        return 'delete';
      default:
        return 'help';
    }
  }

  static formatAge(creationTimestamp: string): string {
    const now = new Date();
    const created = new Date(creationTimestamp);
    const diffMs = now.getTime() - created.getTime();
    const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));
    const diffHours = Math.floor((diffMs % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
    const diffMinutes = Math.floor((diffMs % (1000 * 60 * 60)) / (1000 * 60));

    if (diffDays > 0) {
      return `${diffDays}d ${diffHours}h`;
    } else if (diffHours > 0) {
      return `${diffHours}h ${diffMinutes}m`;
    } else {
      return `${diffMinutes}m`;
    }
  }

  static getReplicaStatus(cluster: PolarDBXCluster, component: 'cn' | 'dn' | 'gms' | 'cdc'): string {
    const status = cluster.status?.replicaStatus?.[component];
    if (!status) {
      return '0/0';
    }
    return `${status.ready}/${status.total}`;
  }

  static isHealthy(cluster: PolarDBXCluster): boolean {
    if (cluster.status?.phase !== 'Running') {
      return false;
    }

    const replicaStatus = cluster.status?.replicaStatus;
    if (!replicaStatus) {
      return false;
    }

    // 检查所有组件是否健康
    const components = ['cn', 'dn', 'gms'] as const;
    return components.every(component => {
      const status = replicaStatus[component];
      return status && status.ready === status.total && status.total > 0;
    });
  }

  static getTotalReplicas(cluster: PolarDBXCluster): number {
    const replicaStatus = cluster.status?.replicaStatus;
    if (!replicaStatus) {
      return 0;
    }

    let total = 0;
    if (replicaStatus.cn) total += replicaStatus.cn.total;
    if (replicaStatus.dn) total += replicaStatus.dn.total;
    if (replicaStatus.gms) total += replicaStatus.gms.total;
    if (replicaStatus.cdc) total += replicaStatus.cdc.total;

    return total;
  }

  static getReadyReplicas(cluster: PolarDBXCluster): number {
    const replicaStatus = cluster.status?.replicaStatus;
    if (!replicaStatus) {
      return 0;
    }

    let ready = 0;
    if (replicaStatus.cn) ready += replicaStatus.cn.ready;
    if (replicaStatus.dn) ready += replicaStatus.dn.ready;
    if (replicaStatus.gms) ready += replicaStatus.gms.ready;
    if (replicaStatus.cdc) ready += replicaStatus.cdc.ready;

    return ready;
  }
}