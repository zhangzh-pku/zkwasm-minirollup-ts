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

  return {
    reads: uniqReads,
    writes: uniqWrites,
    getRecords,
    updateRecords,
  };
}

function setGlobalTrace(trace: Trace | null) {
  (globalThis as any).__MERKLE_TRACE = trace;
}

await (initBootstrap as any)();
await (initApplication as any)(bootstrap);

if (!parentPort) {
  throw new Error("preexec_worker must run as a Worker");
}

parentPort.on("message", (msg: PreexecRequest) => {
  void (async () => {
    try {
      const root = new BigUint64Array(msg.root);
      application.initialize(root);

      const u64array = signature_to_u64array(msg.signature);
      const timingMs: PreexecOk["timingMs"] = { handleTx: 0 };

      if (!msg.skipVerify) {
        const verifyStart = performance.now();
        application.verify_tx_signature(u64array);
        timingMs.verify = performance.now() - verifyStart;
      }

      const trace = makeEmptyTrace();
      setGlobalTrace(trace);
      const start = performance.now();
      const result = application.handle_tx(u64array);
      timingMs.handleTx = performance.now() - start;
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
