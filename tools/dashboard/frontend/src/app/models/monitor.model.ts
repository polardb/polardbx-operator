export interface PolarDBXMonitor {
  metadata: {
    name: string;
    namespace: string;
    creationTimestamp: string;
    uid?: string;
    resourceVersion?: string;
  };
  spec: {
    clusterName: string;
    monitorInterval?: string; // e.g., "30s"
    scrapeTimeout?: string;   // e.g., "10s"
  };
  status?: {
    monitorStatus?: MonitorStatus;
    monitorSpecSnapshot?: PolarDBXMonitorSpec;
  };
}

export interface PolarDBXMonitorSpec {
  clusterName: string;
  monitorInterval?: string;
  scrapeTimeout?: string;
}

export interface MonitorStatus {
  phase?: 'Creating' | 'Running' | 'Failed' | 'Deleting';
  message?: string;
  lastUpdateTime?: string;
}

export interface MonitorInfo {
  id: string;
  name: string;
  namespace: string;
  clusterName: string;
  status: string;
  monitorInterval: string;
  scrapeTimeout: string;
  createdTime: string;
  monitorObject?: PolarDBXMonitor;
}

export interface CreateMonitorRequest {
  name: string;
  namespace?: string;
  clusterName: string;
  monitorInterval?: string;
  scrapeTimeout?: string;
}