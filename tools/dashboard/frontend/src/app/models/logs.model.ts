export type UnknownRecord = Record<string, unknown>;

export type LogItem = Record<string, unknown>;

export interface LogPresetHistogram extends UnknownRecord {
  field?: string;
  intervals: readonly string[];
}

export interface LogPreset extends UnknownRecord {
  indexPattern: string;
  facets?: readonly string[];
  histogram?: LogPresetHistogram;
}

export interface LogsPresetList extends UnknownRecord {
  total?: number;
  items: readonly LogPreset[];
}

export interface TimeRange extends UnknownRecord {
  field: string;
  from: string;
  to: string;
}

export interface LogFacetRequest extends UnknownRecord {
  name: string;
  field: string;
  size?: number;
  order?: 'count' | 'key';
}

export interface LogHistogramRequest extends UnknownRecord {
  name: string;
  field: string;
  interval?: string;
}

export interface LogsQueryRequest extends UnknownRecord {
  index: string;
  size?: number;
  normalize?: boolean;
  timeRange?: TimeRange;
  facets?: readonly LogFacetRequest[];
  histogram?: LogHistogramRequest;
}

export interface FacetBucket extends UnknownRecord {
  key: string;
  count: number;
}

export interface HistogramBucket extends UnknownRecord {
  key: string;
  count: number;
}

export interface NormalizedResponse extends UnknownRecord {
  total?: number;
  took?: number;
  items?: readonly LogItem[];
  facets?: Record<string, readonly FacetBucket[]>;
  histogram?: readonly HistogramBucket[];
}


