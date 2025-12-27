export interface GrafanaTemplateSummary {
  name: string;
  title: string;
  description: string;
  tags?: string[];
  file: string;
  source: string;
  size: number;
  updatedAt: string;
}

export interface GrafanaTemplateDetail extends GrafanaTemplateSummary {
  content: unknown;
}

export interface DashboardTemplateManifest {
  templates: DashboardTemplateMeta[];
  categories?: { id: string; name: string; description?: string }[];
  lastUpdated?: string;
}

export interface DashboardTemplateMeta {
  name: string;
  title: string;
  description: string;
  chart?: string;
  category?: string;
  tags?: string[];
  docLink?: string;
  recommended?: boolean;
  quickActions?: string[];
}
