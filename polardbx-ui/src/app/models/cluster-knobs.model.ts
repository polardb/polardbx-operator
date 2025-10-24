export interface PolarDBXClusterKnobs { metadata: { name: string; namespace?: string; [k: string]: any }; spec?: any; status?: any }
export interface PolarDBXClusterKnobsList { items: PolarDBXClusterKnobs[] }
export interface CreateClusterKnobsRequest { [k: string]: any }

export type Knob = { name: string; category?: string; impact?: 'low'|'medium'|'high'; value?: unknown };
export function getKnobsByCategory(knobs: Knob[] = []): Record<string, Knob[]> {
  return (knobs || []).reduce((acc: Record<string, Knob[]>, k: Knob) => {
    const cat = (k?.category || 'General') as string;
    if (!acc[cat]) acc[cat] = [];
    acc[cat].push(k);
    return acc;
  }, {});
}

export function validateKnobValue(_name: string, _value: unknown): boolean { return true; }
export function formatKnobValue(v: unknown): string { return v === undefined || v === null ? '-' : String(v); }
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
