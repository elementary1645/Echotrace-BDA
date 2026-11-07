export interface Provenance { dataset_id: string; origin: string; source: string; engine: string; exported_at: string; granularity: string; event_count: number; threshold: number }
export interface Envelope { provenance: Provenance; served_at: string; stale: boolean; total: number; offset: number; limit: number; rows: Row[] }
export interface Row { [key: string]: any }
export async function read<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(`/api/v1/analytics/${path}`, { signal });
  const body = await response.json();
  if (!response.ok) throw new Error(typeof body.error === 'string' ? body.error : typeof body.detail === 'string' ? body.detail : `Request failed (${response.status}). Check filter boundaries.`);
  return body as T;
}
export function countBy<T>(rows: T[], key: (r: T) => string) {
  const counts = new Map<string, number>();
  rows.forEach(r => { const k = key(r); counts.set(k, (counts.get(k) ?? 0) + 1); });
  return Array.from(counts, ([label, value]) => ({ label, value }));
}
export function median(values: number[]) {
  const a = [...values].sort((x,y)=>x-y), m = Math.floor(a.length/2);
  return a.length ? a.length % 2 ? a[m] : (a[m-1]+a[m])/2 : 0;
}
export function benchmarkGroups(rows: Row[], experiment: string, metric: string) {
  const selected = rows.filter(r => r.experiment === experiment && !r.warmup);
  return ['python','spark'].map(engine => ({label: engine, value: median(selected.filter(r=>r.engine===engine).map(r=>r[metric]))}));
}
export function download(rows: Row[], name: string) {
  const url = URL.createObjectURL(new Blob([JSON.stringify(rows,null,2)], {type:'application/json'}));
  const link = document.createElement('a'); link.href = url; link.download = name; link.click();
  setTimeout(()=>URL.revokeObjectURL(url),1000);
}
