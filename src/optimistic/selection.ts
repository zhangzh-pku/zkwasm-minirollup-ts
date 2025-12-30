export type TraceLike = {
  reads?: string[];
  writes?: Array<{ index: string; data?: unknown }>;
  getRecords?: Array<{ hash: number[] }>;
  updateRecords?: Array<{ hash: number[] }>;
};

function recordKey(hash: readonly number[]): string {
  return hash.join(",");
}

export function intersects(a: Set<string>, b: Set<string>): boolean {
  for (const x of a) {
    if (b.has(x)) return true;
  }
  return false;
}

export function buildRwSets(trace: TraceLike): { reads: Set<string>; writes: Set<string> } {
  const reads = new Set<string>();
  const writes = new Set<string>();
  for (const idx of trace.reads ?? []) {
    reads.add(`leaf:${idx}`);
  }
  for (const w of trace.writes ?? []) {
    writes.add(`leaf:${w.index}`);
  }
  for (const rec of trace.getRecords ?? []) {
    if (!rec || !Array.isArray(rec.hash)) continue;
    reads.add(`record:${recordKey(rec.hash)}`);
  }
  for (const rec of trace.updateRecords ?? []) {
    if (!rec || !Array.isArray(rec.hash)) continue;
    writes.add(`record:${recordKey(rec.hash)}`);
  }
  return { reads, writes };
}

export function shouldEnterSerialFallback(
  chosen: number,
  deferred: number,
  threshold: number,
): boolean {
  if (deferred <= 0) return false;
  const considered = chosen + deferred;
  if (considered <= 0) return false;
  return chosen / considered < threshold;
}
