import { parentPort } from "node:worker_threads";

import initBootstrap, * as bootstrap from "../bootstrap/bootstrap.js";
import initApplication, * as application from "../application/application.js";
import { signature_to_u64array } from "../signature.js";

import type { TxWitness } from "../prover.js";

type PreexecRequest = {
  id: number;
  root: bigint[];
  signature: TxWitness;
  skipVerify?: boolean;
};

type Trace = {
  reads?: string[];
  writes?: Array<{ index: string; data: number[] }>;
  getRecords?: Array<{ hash: number[] }>;
  updateRecords?: Array<{ hash: number[]; data: string[] }>;
};

type PreexecResponse =
  | PreexecOk
  | PreexecErr;

type PreexecOk = {
  id: number;
  ok: true;
  result: bigint[];
  finalRoot: bigint[];
  trace: {
    reads: string[];
    writes: Array<{ index: string; data: number[] }>;
    getRecords: Array<{ hash: number[] }>;
    updateRecords: Array<{ hash: number[]; data: string[] }>;
  };
  timingMs: {
    verify?: number;
    handleTx: number;
  };
};

type PreexecErr = {
  id: number;
  ok: false;
  error: string;
};

function makeEmptyTrace(): Trace {
  return { reads: [], writes: [], getRecords: [], updateRecords: [] };
}

function normalizeTrace(trace: Trace): PreexecOk["trace"] {
  const reads = Array.isArray(trace.reads) ? trace.reads : [];
  const writes = Array.isArray(trace.writes) ? trace.writes : [];
  const getRecords = Array.isArray(trace.getRecords) ? trace.getRecords : [];
  const updateRecords = Array.isArray(trace.updateRecords) ? trace.updateRecords : [];

  const uniqReads = Array.from(new Set(reads));
  const lastWriteByIndex = new Map<string, number[]>();
  for (const w of writes) {
    if (!w || typeof w.index !== "string" || !Array.isArray(w.data)) continue;
    lastWriteByIndex.set(w.index, w.data);
  }
  const uniqWrites = Array.from(lastWriteByIndex.entries()).map(([index, data]) => ({
    index,
    data,
  }));

  const uniqGetRecords = [];
  const seenGetRecords = new Set<string>();
  for (const r of getRecords) {
    if (!r || !Array.isArray(r.hash)) continue;
    const key = r.hash.join(",");
    if (seenGetRecords.has(key)) continue;
    seenGetRecords.add(key);
    uniqGetRecords.push(r);
  }

  const lastUpdateByHash = new Map<string, { hash: number[]; data: string[] }>();
  for (const r of updateRecords) {
    if (!r || !Array.isArray(r.hash) || !Array.isArray(r.data)) continue;
    const key = r.hash.join(",");
    lastUpdateByHash.set(key, r);
  }
  const uniqUpdateRecords = Array.from(lastUpdateByHash.values());

  return {
    reads: uniqReads,
    writes: uniqWrites,
    getRecords: uniqGetRecords,
    updateRecords: uniqUpdateRecords,
  };
}

function setGlobalTrace(trace: Trace | null) {
  (globalThis as any).__MERKLE_TRACE = trace;
}

function setTraceOnlyWrites(enabled: boolean) {
  (globalThis as any).__MERKLE_TRACE_ONLY_WRITES = enabled;
}

function setOverlay(overlay: { leaves: Map<string, number[]>; records: Map<string, bigint[]> } | null) {
  (globalThis as any).__MERKLE_TRACE_OVERLAY = overlay;
}

await (initBootstrap as any)();
await (initApplication as any)(bootstrap);

if (!parentPort) {
  throw new Error("preexec_worker must run as a Worker");
}

const PREEXEC_REUSE_INITIALIZE = process.env.PREEXEC_REUSE_INITIALIZE === "1";
let lastRootKey: string | null = null;

parentPort.on("message", (msg: PreexecRequest) => {
  void (async () => {
    try {
      const rootKey = msg.root.join(",");
      if (!PREEXEC_REUSE_INITIALIZE || lastRootKey !== rootKey) {
        const root = new BigUint64Array(msg.root);
        application.initialize(root);
        lastRootKey = rootKey;
      }

      const u64array = signature_to_u64array(msg.signature);
      const timingMs: PreexecOk["timingMs"] = { handleTx: 0 };

      if (!msg.skipVerify) {
        const verifyStart = performance.now();
        application.verify_tx_signature(u64array);
        timingMs.verify = performance.now() - verifyStart;
      }

      const trace = makeEmptyTrace();
      setGlobalTrace(trace);
      setTraceOnlyWrites(true);
      setOverlay({ leaves: new Map(), records: new Map() });
      const start = performance.now();
      const result = application.handle_tx(u64array);
      timingMs.handleTx = performance.now() - start;
      setTraceOnlyWrites(false);
      setOverlay(null);
      setGlobalTrace(null);

      const finalRoot = application.query_root();

      const response: PreexecResponse = {
        id: msg.id,
        ok: true,
        result: Array.from(result),
        finalRoot: Array.from(finalRoot),
        trace: normalizeTrace(trace),
        timingMs,
      };
      parentPort!.postMessage(response);
    } catch (err) {
      const response: PreexecResponse = {
        id: msg.id,
        ok: false,
        error: err instanceof Error ? err.stack || err.message : String(err),
      };
      parentPort!.postMessage(response);
    }
  })();
});
