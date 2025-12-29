import { parentPort } from "node:worker_threads";

import { apply_txs_async, apply_txs_final_async } from "../bootstrap/rpcbind.js";

type ApplyLeafWrite = { index: string; data: Uint8Array | number[] };
type ApplyRecordUpdate = { hash: number[]; data: string[] };
type ApplyTxTrace = { writes: ApplyLeafWrite[]; updateRecords: ApplyRecordUpdate[] };

type CommitRequest = {
  id: number;
  mode: "final" | "roots";
  rootBytes: number[];
  txs: ApplyTxTrace[];
  session?: string | null;
};

type CommitOk = {
  id: number;
  ok: true;
  mode: CommitRequest["mode"];
  finalRoot?: number[];
  roots?: number[][];
  timingMs: {
    apply: number;
  };
};

type CommitErr = {
  id: number;
  ok: false;
  error: string;
};

type CommitResponse = CommitOk | CommitErr;

function setMerkleSession(session: string | null) {
  (globalThis as any).__MERKLE_SESSION = session;
}

if (!parentPort) {
  throw new Error("commit_worker must run as a Worker");
}

parentPort.on("message", (msg: CommitRequest) => {
  void (async () => {
    try {
      setMerkleSession(typeof msg.session === "string" ? msg.session : null);
      const start = performance.now();
      if (msg.mode === "final") {
        const finalRoot = await apply_txs_final_async(msg.rootBytes, msg.txs);
        const end = performance.now();
        const resp: CommitResponse = {
          id: msg.id,
          ok: true,
          mode: msg.mode,
          finalRoot,
          timingMs: { apply: end - start },
        };
        parentPort!.postMessage(resp);
        return;
      }
      const roots = await apply_txs_async(msg.rootBytes, msg.txs);
      const end = performance.now();
      const resp: CommitResponse = {
        id: msg.id,
        ok: true,
        mode: msg.mode,
        roots,
        timingMs: { apply: end - start },
      };
      parentPort!.postMessage(resp);
    } catch (err) {
      const resp: CommitResponse = {
        id: msg.id,
        ok: false,
        error: err instanceof Error ? err.stack || err.message : String(err),
      };
      parentPort!.postMessage(resp);
    }
  })();
});
