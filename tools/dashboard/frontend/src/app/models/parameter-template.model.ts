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

// Parameter unit types
export type ParameterUnit = 'STRING' | 'INT' | 'DOUBLE' | 'TZ' | 'HOUR_RANGE';

// Parameter mode types
export type ParameterMode = 'readonly' | 'readwrite';

// Template parameter definition
export interface TemplateParams {
  name: string;
  defaultValue: string;
  mode: ParameterMode;
  restart: boolean;
  unit: ParameterUnit;
  divisibilityFactor?: number;
  optional?: string; // Validation rules, e.g., "[1-65535]" or "true|false"
}

// Template node definition
export interface TemplateNode {
  name: string;
  paramList: TemplateParams[];
}

// Node type definitions
export interface TemplateNodeType {
  cn: TemplateNode;   // Compute Node
  dn: TemplateNode;   // Data Node
  gms?: TemplateNode; // Global Metadata Service (optional)
}

// PolarDBXParameterTemplateSpec interface
export interface PolarDBXParameterTemplateSpec {
  name: string;
  nodeType: TemplateNodeType;
}

// PolarDBXParameterTemplateStatus type (currently placeholder for future fields)
export type PolarDBXParameterTemplateStatus = Record<string, unknown>;

// Main PolarDBXParameterTemplate interface
export interface PolarDBXParameterTemplate {
  apiVersion?: string;
  kind?: string;
  metadata: K8sMetadata;
  spec: PolarDBXParameterTemplateSpec;
  status?: PolarDBXParameterTemplateStatus;
}

// PolarDBXParameterTemplateList interface
export interface PolarDBXParameterTemplateList {
  apiVersion?: string;
  kind?: string;
  metadata?: {
    continue?: string;
    remainingItemCount?: number;
    resourceVersion?: string;
    selfLink?: string;
  };
  items: PolarDBXParameterTemplate[];
}

// Helper interfaces for creating parameter templates
export interface CreateParameterTemplateRequest {
  name: string;
  namespace?: string;
  templateName: string;
  cnParams?: TemplateParams[];
  dnParams?: TemplateParams[];
  gmsParams?: TemplateParams[];
}

export interface UpdateParameterTemplateRequest extends Partial<CreateParameterTemplateRequest> {
  resourceVersion?: string;
}

// Validation helpers
export interface ParameterTemplateValidationError {
  field: string;
  message: string;
}

// Predefined parameter units for UI
export interface ParameterUnitOption {
  label: string;
  value: ParameterUnit;
  description: string;
}

export const PARAMETER_UNIT_OPTIONS: ParameterUnitOption[] = [
  {
    label: 'String',
    value: 'STRING',
    description: 'Text string parameter'
  },
  {
    label: 'Integer',
    value: 'INT',
    description: 'Integer numeric parameter'
  },
  {
    label: 'Double',
    value: 'DOUBLE',
    description: 'Decimal numeric parameter'
  },
  {
    label: 'Timezone',
    value: 'TZ',
    description: 'Timezone parameter'
  },
  {
    label: 'Hour Range',
    value: 'HOUR_RANGE',
    description: 'Hour range parameter (e.g., 09:00-17:00)'
  }
];

// Parameter mode options
export interface ParameterModeOption {
  label: string;
  value: ParameterMode;
  description: string;
}

export const PARAMETER_MODE_OPTIONS: ParameterModeOption[] = [
  {
    label: 'Read Only',
    value: 'readonly',
    description: 'Parameter cannot be modified'
  },
  {
    label: 'Read Write',
    value: 'readwrite',
    description: 'Parameter can be modified'
  }
];

// Node type options
export interface NodeTypeOption {
  label: string;
  value: keyof TemplateNodeType;
  description: string;
}

export const NODE_TYPE_OPTIONS: NodeTypeOption[] = [
  {
    label: 'CN (Compute Node)',
    value: 'cn',
    description: 'Parameters for compute nodes'
  },
  {
    label: 'DN (Data Node)',
    value: 'dn',
    description: 'Parameters for data nodes'
  },
  {
    label: 'GMS (Global Metadata Service)',
    value: 'gms',
    description: 'Parameters for global metadata service'
  }
];

// Common parameter templates for quick setup
export interface PredefinedTemplateOption {
  name: string;
  label: string;
  description: string;
  template: Partial<PolarDBXParameterTemplateSpec>;
}

export const PREDEFINED_TEMPLATE_OPTIONS: PredefinedTemplateOption[] = [
  {
    name: 'basic-template',
    label: 'Basic Template',
    description: 'Basic parameter template with essential parameters',
    template: {
      nodeType: {
        cn: {
          name: 'cnTemplate',
          paramList: [
            {
              name: 'CN_CPU_CORE',
              defaultValue: '8',
              mode: 'readonly',
              restart: true,
              unit: 'INT',
              optional: '[1-128]'
            },
            {
              name: 'ENABLE_HTAP',
              defaultValue: 'true',
              mode: 'readwrite',
              restart: false,
              unit: 'STRING',
              optional: 'true|false'
            }
          ]
        },
        dn: {
          name: 'dnTemplate',
          paramList: [
            {
              name: 'auto_increment_increment',
              defaultValue: '1',
              mode: 'readwrite',
              restart: false,
              unit: 'INT',
              optional: '[1-65535]'
            },
            {
              name: 'max_connections',
              defaultValue: '1000',
              mode: 'readwrite',
              restart: false,
              unit: 'INT',
              optional: '[1-100000]'
            }
          ]
        }
      }
    }
  },
  {
    name: 'performance-template',
    label: 'Performance Template',
    description: 'Optimized template for performance scenarios',
    template: {
      nodeType: {
        cn: {
          name: 'cnTemplate',
          paramList: [
            {
              name: 'CN_CPU_CORE',
              defaultValue: '16',
              mode: 'readonly',
              restart: true,
              unit: 'INT',
              optional: '[1-128]'
            },
            {
              name: 'ENABLE_PARALLEL_QUERY',
              defaultValue: 'true',
              mode: 'readwrite',
              restart: false,
              unit: 'STRING',
              optional: 'true|false'
            }
          ]
        },
        dn: {
          name: 'dnTemplate',
          paramList: [
            {
              name: 'innodb_buffer_pool_size',
              defaultValue: '1G',
              mode: 'readwrite',
              restart: true,
              unit: 'STRING'
            },
            {
              name: 'max_connections',
              defaultValue: '5000',
              mode: 'readwrite',
              restart: false,
              unit: 'INT',
              optional: '[1-100000]'
            }
          ]
        }
      }
    }
  }
];

// Validation rules for common parameter patterns
export interface ParameterValidationRule {
  pattern: RegExp;
  description: string;
}

export const PARAMETER_VALIDATION_RULES: Record<string, ParameterValidationRule> = {
  intRange: {
    pattern: /^\[(\d+)-(\d+)\]$/,
    description: 'Integer range format: [min-max]'
  },
  booleanValue: {
    pattern: /^(true|false)$/,
    description: 'Boolean value: true or false'
  },
  enumValues: {
    pattern: /^[\w|]+$/,
    description: 'Enumeration values separated by |'
  },
  memorySize: {
    pattern: /^\d+[KMGT]?$/,
    description: 'Memory size format: number followed by K/M/G/T'
  },
  timeFormat: {
    pattern: /^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$/,
    description: 'Time format: HH:MM'
  }
};

// Helper functions for parameter validation
export function validateParameterValue(value: string, optional: string, unit: ParameterUnit): boolean {
  if (!optional) return true;
  
  // Check integer range
  const intMatch = optional.match(PARAMETER_VALIDATION_RULES['intRange'].pattern);
  if (intMatch && unit === 'INT') {
    const num = parseInt(value);
    const min = parseInt(intMatch[1]);
    const max = parseInt(intMatch[2]);
    return num >= min && num <= max;
  }
  
  // Check boolean values
  if (optional.match(PARAMETER_VALIDATION_RULES['booleanValue'].pattern)) {
    return value === 'true' || value === 'false';
  }
  
  // Check enumeration values
  if (optional.includes('|')) {
    const validValues = optional.split('|');
    return validValues.includes(value);
  }
  
  return true;
}

export function getParameterDescription(param: TemplateParams): string {
  const mode = param.mode === 'readonly' ? 'Read-only' : 'Read/Write';
  const restart = param.restart ? ', requires restart' : '';
  const range = param.optional ? `, valid values: ${param.optional}` : '';
  
  return `${mode} ${param.unit.toLowerCase()} parameter${restart}${range}`;
}