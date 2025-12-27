export type ResourceMetadata = Record<string, unknown> & {
  name: string;
  namespace?: string;
};

export interface ClusterKnobsSpec extends Record<string, unknown> {
  clusterName?: string;
  knobs?: Record<string, unknown>;
}

export interface ClusterKnobsStatus extends Record<string, unknown> {
  version?: number | string;
  lastUpdated?: string;
}

export interface PolarDBXClusterKnobs {
  metadata: ResourceMetadata;
  spec?: ClusterKnobsSpec;
  status?: ClusterKnobsStatus;
}

export interface PolarDBXClusterKnobsList {
  items: PolarDBXClusterKnobs[];
}

export interface CreateClusterKnobsRequest extends Record<string, unknown> {
  name: string;
  namespace: string;
  clusterName: string;
  knobs?: Record<string, unknown>;
}

export interface Knob {
  name: string;
  category?: string;
  impact?: 'low' | 'medium' | 'high';
  value?: unknown;
}

export function getKnobsByCategory(knobs: readonly Knob[] = []): Record<string, Knob[]> {
  return knobs.reduce<Record<string, Knob[]>>((acc, knob) => {
    const category = (knob.category ?? 'General').toString();
    if (!acc[category]) {
      acc[category] = [];
    }
    acc[category].push(knob);
    return acc;
  }, {});
}

export function validateKnobValue(name: string, value: unknown): boolean {
  void name;
  void value;
  return true;
}

export function formatKnobValue(v: unknown): string {
  return v === undefined || v === null ? '-' : String(v);
}

export function getImpactColor(impact?: string): string {
  switch ((impact || '').toLowerCase()) {
    case 'high': return 'red';
    case 'medium': return 'orange';
    case 'low':
    default: return 'blue';
  }
}
export function getImpactLabel(impact?: string): string {
  const map: Record<string, string> = { high: '高', medium: '中', low: '低' };
  return map[(impact || '').toLowerCase()] || '未知';
}
