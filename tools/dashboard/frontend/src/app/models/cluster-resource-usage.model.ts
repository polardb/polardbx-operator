export interface ClusterResourceUsage {
  available: boolean;
  source: string;
  timestampMs: number;
  message?: string;

  podsTotal?: number;
  podsWithMetrics?: number;
  containersMatched?: number;

  cpu: {
    usageCores: number;
    requestsCores: number;
    limitsCores: number;
    pctOfRequests?: number;
    pctOfLimits?: number;
  };

  memory: {
    usageBytes: number;
    usageGiB: number;
    requestsBytes: number;
    limitsBytes: number;
    pctOfRequests?: number;
    pctOfLimits?: number;
  };
}

