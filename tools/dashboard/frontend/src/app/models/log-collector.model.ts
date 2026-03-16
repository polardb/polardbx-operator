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

// PolarDBXLogCollectorSpec interface based on actual CRD
export interface PolarDBXLogCollectorSpec {
  fileBeatName?: string;
  logStashName?: string;
}

// LogCollectorConfigStatus interface based on actual CRD
export interface LogCollectorConfigStatus {
  fileBeatReadyCount?: number;
  fileBeatCount?: number;
  fileBeatConfigId?: string;
  logStashReadyCount?: number;
  logStashCount?: number;
  logStashConfigId?: string;
}

// PolarDBXLogCollectorStatus interface based on actual CRD
export interface PolarDBXLogCollectorStatus {
  configStatus?: LogCollectorConfigStatus;
  specSnapshot?: PolarDBXLogCollectorSpec;
}

// Main PolarDBXLogCollector interface
export interface PolarDBXLogCollector {
  apiVersion?: string;
  kind?: string;
  metadata: K8sMetadata;
  spec: PolarDBXLogCollectorSpec;
  status?: PolarDBXLogCollectorStatus;
}

// PolarDBXLogCollectorList interface
export interface PolarDBXLogCollectorList {
  apiVersion?: string;
  kind?: string;
  metadata?: {
    continue?: string;
    remainingItemCount?: number;
    resourceVersion?: string;
    selfLink?: string;
  };
  items: PolarDBXLogCollector[];
}

// Helper interfaces for creating log collectors
export interface CreateLogCollectorRequest {
  name: string;
  namespace?: string;
  fileBeatName?: string;
  logStashName?: string;
}

export interface UpdateLogCollectorRequest extends Partial<CreateLogCollectorRequest> {
  resourceVersion?: string;
}

// Validation helpers
export interface LogCollectorValidationError {
  field: string;
  message: string;
}

// Component configuration presets
export interface ComponentPreset {
  name: string;
  label: string;
  description: string;
  fileBeatName?: string;
  logStashName?: string;
}

export const COMPONENT_PRESETS: ComponentPreset[] = [
  {
    name: 'filebeat-only',
    label: 'FileBeat Only',
    description: 'Log collection with FileBeat only',
    fileBeatName: 'filebeat-main',
  },
  {
    name: 'logstash-only',
    label: 'LogStash Only',
    description: 'Log processing with LogStash only',
    logStashName: 'logstash-main',
  },
  {
    name: 'full-stack',
    label: 'Full Stack',
    description: 'Complete log collection and processing stack',
    fileBeatName: 'filebeat-main',
    logStashName: 'logstash-main',
  },
  {
    name: 'production',
    label: 'Production Setup',
    description: 'Production-ready log collection setup',
    fileBeatName: 'filebeat-prod-cluster',
    logStashName: 'logstash-prod-pipeline',
  }
];

// Common component naming patterns
export interface ComponentNamingPattern {
  label: string;
  description: string;
  fileBeatPattern: string;
  logStashPattern: string;
}

export const NAMING_PATTERNS: ComponentNamingPattern[] = [
  {
    label: 'Environment Based',
    description: 'Names based on environment (dev, test, prod)',
    fileBeatPattern: 'filebeat-{env}-{cluster}',
    logStashPattern: 'logstash-{env}-pipeline'
  },
  {
    label: 'Cluster Based',
    description: 'Names based on cluster identifier',
    fileBeatPattern: 'filebeat-{cluster}',
    logStashPattern: 'logstash-{cluster}'
  },
  {
    label: 'Function Based',
    description: 'Names based on function or purpose',
    fileBeatPattern: 'filebeat-{function}',
    logStashPattern: 'logstash-{function}-processor'
  }
];

// Helper functions
export function validateComponentName(name: string): boolean {
  // Kubernetes DNS subdomain name validation
  const nameRegex = /^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/;
  return nameRegex.test(name) && name.length <= 253;
}

export function getCollectorDescription(collector: PolarDBXLogCollector): string {
  const hasFileBeat = collector.spec.fileBeatName ? 'FileBeat' : '';
  const hasLogStash = collector.spec.logStashName ? 'LogStash' : '';
  
  if (hasFileBeat && hasLogStash) {
    return `Full stack: ${collector.spec.fileBeatName} → ${collector.spec.logStashName}`;
  } else if (hasFileBeat) {
    return `FileBeat only: ${collector.spec.fileBeatName}`;
  } else if (hasLogStash) {
    return `LogStash only: ${collector.spec.logStashName}`;
  } else {
    return 'No components configured';
  }
}

export function getReadinessPercentage(status: LogCollectorConfigStatus | undefined): number {
  if (!status) return 0;
  
  const totalFileBeat = status.fileBeatCount || 0;
  const readyFileBeat = status.fileBeatReadyCount || 0;
  const totalLogStash = status.logStashCount || 0;
  const readyLogStash = status.logStashReadyCount || 0;
  
  const totalComponents = totalFileBeat + totalLogStash;
  const readyComponents = readyFileBeat + readyLogStash;
  
  if (totalComponents === 0) return 0;
  return Math.round((readyComponents / totalComponents) * 100);
}

export function getCollectorStatusColor(collector: PolarDBXLogCollector): string {
  const readiness = getReadinessPercentage(collector.status?.configStatus);
  
  if (readiness >= 100) return 'success';
  if (readiness >= 75) return 'info';
  if (readiness >= 50) return 'warning';
  return 'danger';
}

export function getCollectorStatusText(collector: PolarDBXLogCollector): string {
  const readiness = getReadinessPercentage(collector.status?.configStatus);
  const status = collector.status?.configStatus;
  
  if (!status) return 'Unknown';
  
  const fileBeatText = status.fileBeatCount ? `${status.fileBeatReadyCount}/${status.fileBeatCount} FileBeat` : '';
  const logStashText = status.logStashCount ? `${status.logStashReadyCount}/${status.logStashCount} LogStash` : '';
  
  const components = [fileBeatText, logStashText].filter(Boolean).join(', ');
  return components ? `${components} (${readiness}% ready)` : 'No components';
}

export function generateComponentName(pattern: string, variables: Record<string, string>): string {
  let result = pattern;
  Object.entries(variables).forEach(([key, value]) => {
    result = result.replace(new RegExp(`\\{${key}\\}`, 'g'), value);
  });
  return result;
}