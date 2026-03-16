export interface Pod {
  metadata: {
    name: string;
    namespace: string;
    uid?: string;
    resourceVersion?: string;
    creationTimestamp?: string;
    labels?: Record<string, string>;
    annotations?: Record<string, string>;
    ownerReferences?: {
      apiVersion: string;
      kind: string;
      name: string;
      uid: string;
      controller?: boolean;
      blockOwnerDeletion?: boolean;
    }[];
  };
  spec: {
    containers: Container[];
    restartPolicy?: string;
    terminationGracePeriodSeconds?: number;
    dnsPolicy?: string;
    serviceAccountName?: string;
    serviceAccount?: string;
    nodeName?: string;
    hostNetwork?: boolean;
    hostPID?: boolean;
    hostIPC?: boolean;
    shareProcessNamespace?: boolean;
    securityContext?: {
      runAsUser?: number;
      runAsGroup?: number;
      runAsNonRoot?: boolean;
      fsGroup?: number;
      seLinuxOptions?: unknown;
      windowsOptions?: unknown;
      fsGroupChangePolicy?: string;
      supplementalGroups?: number[];
    };
    imagePullSecrets?: {
      name: string;
    }[];
    hostname?: string;
    subdomain?: string;
    affinity?: unknown;
    tolerations?: unknown[];
    nodeSelector?: Record<string, string>;
    volumes?: Volume[];
  };
  status?: {
    phase: 'Pending' | 'Running' | 'Succeeded' | 'Failed' | 'Unknown';
    conditions?: PodCondition[];
    message?: string;
    reason?: string;
    nominatedNodeName?: string;
    hostIP?: string;
    podIP?: string;
    podIPs?: {
      ip: string;
    }[];
    startTime?: string;
    initContainerStatuses?: ContainerStatus[];
    containerStatuses?: ContainerStatus[];
    qosClass?: 'Guaranteed' | 'Burstable' | 'BestEffort';
    ephemeralContainerStatuses?: ContainerStatus[];
  };
}

export interface Container {
  name: string;
  image: string;
  command?: string[];
  args?: string[];
  workingDir?: string;
  ports?: ContainerPort[];
  envFrom?: {
    configMapRef?: {
      name: string;
      optional?: boolean;
    };
    secretRef?: {
      name: string;
      optional?: boolean;
    };
    prefix?: string;
  }[];
  env?: EnvVar[];
  resources?: {
    limits?: {
      cpu?: string;
      memory?: string;
      storage?: string;
      [key: string]: string | undefined;
    };
    requests?: {
      cpu?: string;
      memory?: string;
      storage?: string;
      [key: string]: string | undefined;
    };
  };
  volumeMounts?: VolumeMount[];
  livenessProbe?: Probe;
  readinessProbe?: Probe;
  startupProbe?: Probe;
  lifecycle?: {
    postStart?: {
      exec?: {
        command: string[];
      };
      httpGet?: {
        path?: string;
        port: number | string;
        host?: string;
        scheme?: string;
        httpHeaders?: {
          name: string;
          value: string;
        }[];
      };
      tcpSocket?: {
        port: number | string;
        host?: string;
      };
    };
    preStop?: {
      exec?: {
        command: string[];
      };
      httpGet?: {
        path?: string;
        port: number | string;
        host?: string;
        scheme?: string;
        httpHeaders?: {
          name: string;
          value: string;
        }[];
      };
      tcpSocket?: {
        port: number | string;
        host?: string;
      };
    };
  };
  terminationMessagePath?: string;
  terminationMessagePolicy?: string;
  imagePullPolicy?: 'Always' | 'Never' | 'IfNotPresent';
  securityContext?: {
    capabilities?: {
      add?: string[];
      drop?: string[];
    };
    privileged?: boolean;
    seLinuxOptions?: unknown;
    windowsOptions?: unknown;
    runAsUser?: number;
    runAsGroup?: number;
    runAsNonRoot?: boolean;
    readOnlyRootFilesystem?: boolean;
    allowPrivilegeEscalation?: boolean;
    procMount?: string;
    seccompProfile?: unknown;
  };
  stdin?: boolean;
  stdinOnce?: boolean;
  tty?: boolean;
}

export interface ContainerPort {
  name?: string;
  hostPort?: number;
  containerPort: number;
  protocol?: 'TCP' | 'UDP' | 'SCTP';
  hostIP?: string;
}

export interface EnvVar {
  name: string;
  value?: string;
  valueFrom?: {
    fieldRef?: {
      apiVersion?: string;
      fieldPath: string;
    };
    resourceFieldRef?: {
      containerName?: string;
      resource: string;
      divisor?: string;
    };
    configMapKeyRef?: {
      name: string;
      key: string;
      optional?: boolean;
    };
    secretKeyRef?: {
      name: string;
      key: string;
      optional?: boolean;
    };
  };
}

export interface VolumeMount {
  name: string;
  readOnly?: boolean;
  mountPath: string;
  subPath?: string;
  mountPropagation?: string;
  subPathExpr?: string;
}

export interface Volume {
  name: string;
  hostPath?: {
    path: string;
    type?: string;
  };
  emptyDir?: {
    medium?: string;
    sizeLimit?: string;
  };
  gcePersistentDisk?: unknown;
  awsElasticBlockStore?: unknown;
  gitRepo?: unknown;
  secret?: {
    secretName: string;
    items?: {
      key: string;
      path: string;
      mode?: number;
    }[];
    defaultMode?: number;
    optional?: boolean;
  };
  nfs?: {
    server: string;
    path: string;
    readOnly?: boolean;
  };
  iscsi?: unknown;
  glusterfs?: unknown;
  persistentVolumeClaim?: {
    claimName: string;
    readOnly?: boolean;
  };
  rbd?: unknown;
  flexVolume?: unknown;
  cinder?: unknown;
  cephfs?: unknown;
  flocker?: unknown;
  downwardAPI?: {
    items?: {
      path: string;
      fieldRef?: {
        apiVersion?: string;
        fieldPath: string;
      };
      resourceFieldRef?: {
        containerName?: string;
        resource: string;
        divisor?: string;
      };
      mode?: number;
    }[];
    defaultMode?: number;
  };
  fc?: unknown;
  azureFile?: unknown;
  configMap?: {
    name: string;
    items?: {
      key: string;
      path: string;
      mode?: number;
    }[];
    defaultMode?: number;
    optional?: boolean;
  };
  vsphereVolume?: unknown;
  quobyte?: unknown;
  azureDisk?: unknown;
  photonPersistentDisk?: unknown;
  projected?: {
    sources: {
      secret?: {
        name: string;
        items?: {
          key: string;
          path: string;
          mode?: number;
        }[];
        optional?: boolean;
      };
      downwardAPI?: {
        items?: {
          path: string;
          fieldRef?: {
            apiVersion?: string;
            fieldPath: string;
          };
          resourceFieldRef?: {
            containerName?: string;
            resource: string;
            divisor?: string;
          };
          mode?: number;
        }[];
      };
      configMap?: {
        name: string;
        items?: {
          key: string;
          path: string;
          mode?: number;
        }[];
        optional?: boolean;
      };
      serviceAccountToken?: {
        audience?: string;
        expirationSeconds?: number;
        path: string;
      };
    }[];
    defaultMode?: number;
  };
  portworxVolume?: unknown;
  scaleIO?: unknown;
  storageos?: unknown;
  csi?: {
    driver: string;
    readOnly?: boolean;
    fsType?: string;
    volumeAttributes?: Record<string, string>;
    nodePublishSecretRef?: {
      name: string;
    };
  };
  ephemeral?: {
    volumeClaimTemplate?: unknown;
  };
}

export interface Probe {
  exec?: {
    command: string[];
  };
  httpGet?: {
    path?: string;
    port: number | string;
    host?: string;
    scheme?: 'HTTP' | 'HTTPS';
    httpHeaders?: {
      name: string;
      value: string;
    }[];
  };
  tcpSocket?: {
    port: number | string;
    host?: string;
  };
  grpc?: {
    port: number;
    service?: string;
  };
  initialDelaySeconds?: number;
  timeoutSeconds?: number;
  periodSeconds?: number;
  successThreshold?: number;
  failureThreshold?: number;
  terminationGracePeriodSeconds?: number;
}

export interface PodCondition {
  type: string;
  status: 'True' | 'False' | 'Unknown';
  lastProbeTime?: string;
  lastTransitionTime?: string;
  reason?: string;
  message?: string;
}

export interface ContainerStatus {
  name: string;
  state: {
    waiting?: {
      reason?: string;
      message?: string;
    };
    running?: {
      startedAt?: string;
    };
    terminated?: {
      exitCode: number;
      signal?: number;
      reason?: string;
      message?: string;
      startedAt?: string;
      finishedAt?: string;
      containerID?: string;
    };
  };
  lastState?: {
    waiting?: {
      reason?: string;
      message?: string;
    };
    running?: {
      startedAt?: string;
    };
    terminated?: {
      exitCode: number;
      signal?: number;
      reason?: string;
      message?: string;
      startedAt?: string;
      finishedAt?: string;
      containerID?: string;
    };
  };
  ready: boolean;
  restartCount: number;
  image: string;
  imageID: string;
  containerID?: string;
  started?: boolean;
}

export interface PodMetrics {
  metadata: {
    name: string;
    namespace: string;
    creationTimestamp: string;
  };
  timestamp: string;
  window: string;
  containers: {
    name: string;
    usage: {
      cpu: string;
      memory: string;
    };
  }[];
}

export interface PodSummary {
  name: string;
  namespace: string;
  phase: string;
  ready: string;
  status: string;
  restarts: number;
  age: string;
  ip?: string;
  node?: string;
  component?: string;
}

// 工具函数
export class PodUtils {
  static getPhaseColor(phase: string): string {
    switch (phase) {
      case 'Running':
        return 'primary';
      case 'Pending':
        return 'accent';
      case 'Succeeded':
        return 'primary';
      case 'Failed':
        return 'warn';
      default:
        return 'basic';
    }
  }

  static getPhaseIcon(phase: string): string {
    switch (phase) {
      case 'Running':
        return 'play_circle_filled';
      case 'Pending':
        return 'hourglass_empty';
      case 'Succeeded':
        return 'check_circle';
      case 'Failed':
        return 'error';
      default:
        return 'help';
    }
  }

  static getReadyStatus(pod: Pod): string {
    if (!pod.status?.containerStatuses) {
      return '0/0';
    }

    const total = pod.status.containerStatuses.length;
    const ready = pod.status.containerStatuses.filter(c => c.ready).length;
    return `${ready}/${total}`;
  }

  static getTotalRestarts(pod: Pod): number {
    if (!pod.status?.containerStatuses) {
      return 0;
    }

    return pod.status.containerStatuses.reduce((total, container) => {
      return total + container.restartCount;
    }, 0);
  }

  static getAge(pod: Pod): string {
    if (!pod.metadata.creationTimestamp) {
      return 'Unknown';
    }

    const now = new Date();
    const created = new Date(pod.metadata.creationTimestamp);
    const diffMs = now.getTime() - created.getTime();
    const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));
    const diffHours = Math.floor((diffMs % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
    const diffMinutes = Math.floor((diffMs % (1000 * 60 * 60)) / (1000 * 60));

    if (diffDays > 0) {
      return `${diffDays}d`;
    } else if (diffHours > 0) {
      return `${diffHours}h`;
    } else {
      return `${diffMinutes}m`;
    }
  }

  static getComponent(pod: Pod): string {
    const labels = pod.metadata.labels || {};
    return labels['polardbx/role'] || labels['app.kubernetes.io/component'] || labels['component'] || 'unknown';
  }

  static isReady(pod: Pod): boolean {
    if (pod.status?.phase !== 'Running') {
      return false;
    }

    if (!pod.status?.containerStatuses) {
      return false;
    }

    return pod.status.containerStatuses.every(container => container.ready);
  }

  static getStatusMessage(pod: Pod): string {
    if (pod.status?.message) {
      return pod.status.message;
    }

    if (pod.status?.reason) {
      return pod.status.reason;
    }

    if (pod.status?.containerStatuses) {
      for (const container of pod.status.containerStatuses) {
        if (container.state.waiting) {
          return container.state.waiting.reason || 'Waiting';
        }
        if (container.state.terminated) {
          return container.state.terminated.reason || 'Terminated';
        }
      }
    }

    return pod.status?.phase || 'Unknown';
  }

  static getContainerImage(pod: Pod, containerName: string): string {
    const container = pod.spec.containers.find(c => c.name === containerName);
    return container?.image || 'Unknown';
  }

  static getContainerStatus(pod: Pod, containerName: string): ContainerStatus | undefined {
    return pod.status?.containerStatuses?.find(c => c.name === containerName);
  }

  static formatResourceValue(value: string): string {
    if (value.endsWith('m')) {
      // CPU in millicores
      const millicores = parseInt(value.slice(0, -1));
      if (millicores >= 1000) {
        return `${(millicores / 1000).toFixed(1)} cores`;
      }
      return `${millicores}m`;
    }

    if (value.endsWith('Mi') || value.endsWith('Gi') || value.endsWith('Ki')) {
      return value;
    }

    // Try to parse as bytes and convert to human readable
    const bytes = parseInt(value);
    if (!isNaN(bytes)) {
      if (bytes >= 1024 * 1024 * 1024) {
        return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)}Gi`;
      }
      if (bytes >= 1024 * 1024) {
        return `${(bytes / (1024 * 1024)).toFixed(1)}Mi`;
      }
      if (bytes >= 1024) {
        return `${(bytes / 1024).toFixed(1)}Ki`;
      }
      return `${bytes}B`;
    }

    return value;
  }
}