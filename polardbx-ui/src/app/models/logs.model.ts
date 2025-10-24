export interface LogPreset {
  indexPattern: string;
  facets?: string[];
  histogram?: { intervals: string[] };
}

export interface LogsPresetList { items: LogPreset[] }

export interface LogsQueryRequest {
  index: string;
  size?: number;
  normalize?: boolean;
  timeRange?: { field: string; from: string; to: string };
  facets?: Array<{ name: string; field: string; size?: number; order?: 'count'|'key' }>;
  histogram?: { name: string; field: string; interval?: string };
  [k: string]: any;
}

export interface NormalizedResponse {
  total?: number;
  took?: number;
  items?: any[];
  facets?: Record<string, Array<{ key: string; count: number }>>;
  histogram?: Array<{ key: string; count: number }>;
  [k: string]: any;
}


