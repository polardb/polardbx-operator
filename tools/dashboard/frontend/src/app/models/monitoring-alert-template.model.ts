export interface AlertRuleTemplateSummary {
  name: string;
  title: string;
  description: string;
  categories?: string[];
  primarySeverity?: string;
  groups?: AlertRuleGroupSummary[];
  labels?: Record<string, string>;
  annotations?: Record<string, string>;
  source: string;
  file: string;
  size: number;
  updatedAt: string;
}

export interface AlertRuleGroupSummary {
  name: string;
  rules: number;
  interval?: string;
  severities?: string[];
}

export interface AlertRuleTemplateDetail extends AlertRuleTemplateSummary {
  content: string;
}

export interface AlertRuleTemplateManifest {
  templates: AlertRuleTemplateMeta[];
  categories?: AlertRuleTemplateCategory[];
  lastUpdated?: string;
}

export interface AlertRuleTemplateMeta {
  name: string;
  title: string;
  description: string;
  category?: string;
  severity?: string;
  docLink?: string;
  recommended?: boolean;
  tags?: string[];
  labels?: Record<string, string>;
  annotations?: Record<string, string>;
  defaultNamespace?: string;
}

export interface AlertRuleTemplateCategory {
  id: string;
  name: string;
  description?: string;
}

export interface ApplyAlertRuleTemplatePayload {
  template?: string;
  namespace?: string;
  name?: string;
  content?: string;
  labels?: Record<string, string>;
  annotations?: Record<string, string>;
  overwrite?: boolean;
  dryRun?: boolean;
}

export interface ApplyAlertRuleTemplateResponse {
  name: string;
  namespace: string;
  message: string;
  overwrite: boolean;
  warnings?: string[];
  validation?: {
    success: boolean;
    message: string;
    details?: { level: string; message: string }[];
    warnings?: string[];
  };
}
