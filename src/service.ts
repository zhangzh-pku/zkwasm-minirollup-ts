//import initHostBind, * as hostbind from "./wasmbind/hostbind.js";
import initBootstrap, * as bootstrap from "./bootstrap/bootstrap.js";
import {
  apply_txs,
  apply_txs_async,
  apply_txs_final,
  apply_txs_final_async,
  begin_session,
  commit_session,
  drop_session,
} from "./bootstrap/rpcbind.js";
import initApplication, * as application from "./application/application.js";
import { test_merkle_db_service } from "./test.js";
import { LeHexBN, sign, PlayerConvention, ZKWasmAppRpc, createCommand } from "zkwasm-minirollup-rpc";
import { signature_to_u64array } from "./signature.js";
import { base64ToU64ArrayLE, u64ArrayToBase64LE } from "./u64.js";
import { Queue, Worker, Job } from 'bullmq';
import IORedis from 'ioredis';
import express, {Express} from 'express';
import { cpus } from "node:os";
import { Worker as NodeWorker } from "node:worker_threads";
import { submitProofWithRetry, has_uncomplete_task, TxWitness, get_latest_proof, has_task } from "./prover.js";
import { ensureIndexes } from "./commit.js";
import cors from "cors";
import { get_chain_id, get_contract_addr, get_mongoose_db, get_queue_name, get_shard_count, get_shard_id, get_server_admin_key, get_service_port, modelBundle, modelJob, modelRand, modelTx } from "./config.js";
import { getMerkleArray } from "./contract.js";
import { ZkWasmUtil } from "zkwasm-service-helper";
import dotenv from 'dotenv';
import mongoose from 'mongoose';
import {hexStringToMerkleRoot, merkleRootToBeHexString} from "./lib.js";
import {sha256} from "ethers";
import {TxStateManager} from "./commit.js";
import {ensureAccountIndexes, queryAccounts, storeAccount} from "./account.js";
import { MongoWriteBuffer } from "./mongo_write_buffer.js";
import { shardForPkx } from "./sharding.js";

// Load environment variables from .env file
dotenv.config();

const LOG_TX = process.env.LOG_TX === '1';
const LOG_BUNDLE = process.env.LOG_BUNDLE === '1';
const LOG_QUEUE_STATS = process.env.LOG_QUEUE_STATS === '1';
const LOG_AUTOJOB = process.env.LOG_AUTOJOB === '1';
const LOG_OPTIMISTIC = process.env.LOG_OPTIMISTIC === '1';
const LOG_OPTIMISTIC_PROFILE = process.env.LOG_OPTIMISTIC_PROFILE === '1';
const DISABLE_AUTOTICK = process.env.DISABLE_AUTOTICK === '1';
const AUTOJOB_FATAL = process.env.AUTOJOB_FATAL === '1';
const DISABLE_SNAPSHOT = process.env.DISABLE_SNAPSHOT === '1';
const DISABLE_MONGO_TX_STORE = process.env.DISABLE_MONGO_TX_STORE === '1';
const DISABLE_MONGO_JOB_STORE = process.env.DISABLE_MONGO_JOB_STORE === '1';
const ASYNC_MONGO_WRITES = process.env.ASYNC_MONGO_WRITES === '1';
const BATCH_MONGO_WRITES = process.env.BATCH_MONGO_WRITES === '1';
const MONGO_BATCH_SIZE = Number.parseInt(process.env.MONGO_BATCH_SIZE ?? "200", 10);
const MONGO_FLUSH_MS = Number.parseInt(process.env.MONGO_FLUSH_MS ?? "50", 10);
const MONGO_BATCH_FATAL = process.env.MONGO_BATCH_FATAL !== '0';
const MONGO_JOB_BATCH_FATAL = process.env.MONGO_JOB_BATCH_FATAL === '1';
const LIGHT_JOB_RESULT = process.env.LIGHT_JOB_RESULT === '1';
const MERKLE_SESSION_OVERLAY = process.env.MERKLE_SESSION_OVERLAY === '1';
const ENFORCE_SHARD = process.env.ENFORCE_SHARD !== '0';

// Experimental: single-root optimistic concurrency via parallel pre-exec + conflict filtering.
// - Off by default; enable with OPTIMISTIC_APPLY=1.
// - Designed for higher TPS; correctness depends on complete trace coverage of state reads/writes.
const OPTIMISTIC_APPLY = process.env.OPTIMISTIC_APPLY === '1';
const OPTIMISTIC_WORKERS = (() => {
  const raw = Number.parseInt(process.env.OPTIMISTIC_WORKERS ?? "", 10);
  if (Number.isFinite(raw) && raw > 0) return raw;
  return Math.max(1, cpus().length);
})();
const OPTIMISTIC_BATCH = (() => {
  const raw = Number.parseInt(process.env.OPTIMISTIC_BATCH ?? "32", 10);
  if (Number.isFinite(raw) && raw > 0) return raw;
  return 32;
})();
const OPTIMISTIC_FLUSH_MS = (() => {
  const raw = Number.parseInt(process.env.OPTIMISTIC_FLUSH_MS ?? "5", 10);
  if (Number.isFinite(raw) && raw >= 0) return raw;
  return 5;
})();
const OPTIMISTIC_BUNDLE_SIZE = (() => {
  const raw = Number.parseInt(process.env.OPTIMISTIC_BUNDLE_SIZE ?? "100", 10);
  if (Number.isFinite(raw) && raw > 0) return raw;
  return 100;
})();
const OPTIMISTIC_SERIAL_THRESHOLD = (() => {
  const raw = Number.parseFloat(process.env.OPTIMISTIC_SERIAL_THRESHOLD ?? "0.25");
  if (!Number.isFinite(raw)) return 0.25;
  if (raw < 0) return 0;
  if (raw > 1) return 1;
  return raw;
})();
const OPTIMISTIC_SERIAL_COOLDOWN_MS = (() => {
  const raw = Number.parseInt(process.env.OPTIMISTIC_SERIAL_COOLDOWN_MS ?? "1000", 10);
  if (Number.isFinite(raw) && raw >= 0) return raw;
  return 1000;
})();
const OPTIMISTIC_QUEUE_CONCURRENCY = (() => {
  const raw = Number.parseInt(process.env.OPTIMISTIC_QUEUE_CONCURRENCY ?? "", 10);
  if (Number.isFinite(raw) && raw > 0) return raw;
  if (OPTIMISTIC_APPLY) {
    // Need >1 so multiple jobs can enqueue into OptimisticSequencer and form batches.
    return Math.max(OPTIMISTIC_BATCH, OPTIMISTIC_WORKERS * 4);
  }
  return 1;
})();

// M3: move Merkle commit (apply_txs*) into a dedicated Worker thread, and optionally pipeline
// preexec of the next batch while committing the current batch.
const OPTIMISTIC_PIPELINE = process.env.OPTIMISTIC_PIPELINE === "1";
const OPTIMISTIC_COMMIT_WORKER = OPTIMISTIC_PIPELINE || process.env.OPTIMISTIC_COMMIT_WORKER === "1";

// /send hot path optimizations: batch enqueue jobs to Redis (BullMQ) to reduce per-request overhead.
// Defaults to enabled when OPTIMISTIC_APPLY=1, since that mode targets max throughput.
const SEND_ENQUEUE_BATCH = (() => {
  const raw = Number.parseInt(process.env.SEND_ENQUEUE_BATCH ?? "", 10);
  if (Number.isFinite(raw) && raw > 0) return raw;
  return OPTIMISTIC_APPLY ? 64 : 1;
})();
const SEND_ENQUEUE_FLUSH_MS = (() => {
  const raw = Number.parseInt(process.env.SEND_ENQUEUE_FLUSH_MS ?? "", 10);
  if (Number.isFinite(raw) && raw >= 0) return raw;
  return OPTIMISTIC_APPLY ? 1 : 0;
})();
const SEND_ENQUEUE_MAX_PENDING = (() => {
  const raw = Number.parseInt(process.env.SEND_ENQUEUE_MAX_PENDING ?? "", 10);
  if (Number.isFinite(raw) && raw > 0) return raw;
  return OPTIMISTIC_APPLY ? 20000 : 5000;
})();

// Reuse parsed BigUint64Array from /send on the worker hot path.
// - `SEND_U64_CACHE=1` stores u64 arrays in-process keyed by jobId (best-effort, local-only).
// - `SEND_U64_IN_JOBDATA=1` additionally embeds a base64 payload into BullMQ job data (slower enqueue, cross-process).
const SEND_U64_CACHE = OPTIMISTIC_APPLY && process.env.SEND_U64_CACHE !== "0";
const SEND_U64_CACHE_MAX = (() => {
  const raw = Number.parseInt(process.env.SEND_U64_CACHE_MAX ?? "", 10);
  if (Number.isFinite(raw) && raw > 0) return raw;
  return SEND_ENQUEUE_MAX_PENDING;
})();
const SEND_U64_IN_JOBDATA = process.env.SEND_U64_IN_JOBDATA === "1";

// Increase TCP accept backlog to reduce connection drops under extreme client concurrency.
const HTTP_HOST = process.env.HTTP_HOST ?? "0.0.0.0";
const HTTP_BACKLOG = (() => {
  const raw = Number.parseInt(process.env.HTTP_BACKLOG ?? "", 10);
  if (Number.isFinite(raw) && raw > 0) return raw;
  return 2048;
})();

type PreexecTrace = {
  reads: string[];
  writes: Array<{ index: string; data: Uint8Array | number[] }>;
  getRecords: Array<{ hash: number[] }>;
  updateRecords: Array<{ hash: number[]; data: string[] }>;
};

type PreexecOk = {
  id: number;
  ok: true;
  result: bigint[];
  finalRoot: bigint[];
  trace: PreexecTrace;
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

type PreexecResponse = PreexecOk | PreexecErr;

type PreexecRequest = {
  id: number;
  root: bigint[];
  signature?: TxWitness;
  u64?: string;
  u64array?: BigUint64Array;
  skipVerify?: boolean;
  session?: string | null;
};

function u64ToLeBytes(value: bigint): number[] {
  const out: number[] = [];
  let v = value;
  for (let i = 0; i < 8; i++) {
    out.push(Number(v & 0xffn));
    v >>= 8n;
  }
  return out;
}

function rootU64ToBytes(root: BigUint64Array): number[] {
  const bytes: number[] = [];
  for (const limb of root) {
    bytes.push(...u64ToLeBytes(limb));
  }
  if (bytes.length !== 32) {
    throw new Error(`invalid root byte length: ${bytes.length}`);
  }
  return bytes;
}

function bytes32ToRootU64(bytes: readonly number[]): BigUint64Array {
  if (bytes.length !== 32) {
    throw new Error(`expected 32 bytes, got ${bytes.length}`);
  }
  const limbs: bigint[] = [];
  for (let i = 0; i < 4; i++) {
    let limb = 0n;
    for (let j = 0; j < 8; j++) {
      limb |= BigInt(bytes[i * 8 + j] ?? 0) << (8n * BigInt(j));
    }
    limbs.push(limb);
  }
  return new BigUint64Array(limbs);
}

function recordKey(hash: readonly number[]): string {
  return hash.join(",");
}

function intersects(a: Set<string>, b: Set<string>): boolean {
  for (const x of a) {
    if (b.has(x)) return true;
  }
  return false;
}

function buildRwSets(trace: PreexecTrace): { reads: Set<string>; writes: Set<string> } {
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

function withMerkleSessionDisabled<T>(fn: () => T): T {
  const g = globalThis as any;
  const prev = g.__MERKLE_SESSION;
  g.__MERKLE_SESSION = null;
  try {
    return fn();
  } finally {
    g.__MERKLE_SESSION = prev;
  }
}

async function withMerkleSessionDisabledAsync<T>(fn: () => Promise<T>): Promise<T> {
  const g = globalThis as any;
  const prev = g.__MERKLE_SESSION;
  g.__MERKLE_SESSION = null;
  try {
    return await fn();
  } finally {
    g.__MERKLE_SESSION = prev;
  }
}

function txJobId(tx: TxWitness): string | undefined {
  const hash = (tx as any)?.hash;
  const pkx = (tx as any)?.pkx;
  if (typeof hash !== "string" || typeof pkx !== "string") return undefined;
  return `${hash}${pkx}`;
}

class OverloadedError extends Error {
  statusCode: number;
  constructor(message = "Overloaded") {
    super(message);
    this.name = "OverloadedError";
    this.statusCode = 503;
  }
}

type PendingEnqueue = {
  value: TxWitness;
  u64?: string;
  resolve: (job: Job) => void;
  reject: (err: Error) => void;
};

class SendEnqueueBuffer {
  private pending: PendingEnqueue[] = [];
  private flushing = false;
  private timer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    private queue: Queue,
    private batchSize: number,
    private flushMs: number,
    private maxPending: number,
  ) {}

  enqueue(value: TxWitness, u64?: string): Promise<Job> {
    if (this.pending.length >= this.maxPending) {
      return Promise.reject(new OverloadedError(`Overloaded: pending=${this.pending.length}, max=${this.maxPending}`));
    }
    return new Promise((resolve, reject) => {
      this.pending.push({ value, u64, resolve, reject });
      this.scheduleFlush();
    });
  }

  private scheduleFlush() {
    if (this.flushing) return;
    if (this.pending.length >= this.batchSize) {
      void this.flush();
      return;
    }
    if (this.timer) return;
    this.timer = setTimeout(() => {
      this.timer = null;
      void this.flush();
    }, this.flushMs);
  }

  private async flush(): Promise<void> {
    if (this.flushing) return;
    this.flushing = true;
    try {
      while (this.pending.length > 0) {
        const batch = this.pending.splice(0, this.batchSize);
        try {
          const bulk = batch.map((item) => ({
            name: "transaction",
            data: { value: item.value, verified: true, u64: item.u64 },
            opts: (() => {
              const jobId = txJobId(item.value);
              return jobId ? { jobId } : {};
            })(),
          })) as any[];
          const jobs = (await this.queue.addBulk(bulk)) as unknown as Job[];
          if (!Array.isArray(jobs) || jobs.length !== batch.length) {
            throw new Error(
              `queue.addBulk returned ${Array.isArray(jobs) ? jobs.length : typeof jobs} jobs, expected ${batch.length}`,
            );
          }
          for (let i = 0; i < batch.length; i++) {
            batch[i]!.resolve(jobs[i]!);
          }
        } catch (e) {
          const err = e instanceof Error ? e : new Error(String(e));
          for (const item of batch) {
            item.reject(err);
          }

          // If enqueue is failing, reject everything pending to avoid unbounded memory growth.
          if (this.pending.length > 0) {
            const remaining = this.pending.splice(0, this.pending.length);
            for (const item of remaining) {
              item.reject(err);
            }
          }
          break;
        }
      }
    } finally {
      this.flushing = false;
      if (this.timer) {
        clearTimeout(this.timer);
        this.timer = null;
      }
      if (this.pending.length > 0) {
        this.scheduleFlush();
      }
    }
  }
}

class TxU64Cache {
  private map = new Map<string, BigUint64Array>();

  constructor(private maxEntries: number) {}

  get(jobId: string): BigUint64Array | undefined {
    return this.map.get(jobId);
  }

  set(jobId: string, u64array: BigUint64Array) {
    this.map.set(jobId, u64array);
    while (this.map.size > this.maxEntries) {
      const oldest = this.map.keys().next().value as string | undefined;
      if (typeof oldest !== "string") break;
      this.map.delete(oldest);
    }
  }

  delete(jobId: string) {
    this.map.delete(jobId);
  }
}

class PreexecPool {
  private workers: NodeWorker[];
  private idle: NodeWorker[];
  private queue: Array<{ req: PreexecRequest; resolve: (resp: PreexecResponse) => void; reject: (err: Error) => void }> = [];
  private inflight: Map<number, { resolve: (resp: PreexecResponse) => void; reject: (err: Error) => void }> = new Map();

  constructor(private workerUrl: URL, size: number) {
    const n = Number.isFinite(size) && size > 0 ? Math.floor(size) : 1;
    this.workers = [];
    this.idle = [];
    for (let i = 0; i < n; i++) {
      const w = new NodeWorker(workerUrl, { type: "module" } as any);
      w.on("message", (msg: PreexecResponse) => {
        const pending = this.inflight.get(msg.id);
        if (!pending) return;
        this.inflight.delete(msg.id);
        this.idle.push(w);
        pending.resolve(msg);
        this.drain();
      });
      w.on("error", (err) => {
        // bubble errors to all inflight tasks
        for (const [, pending] of this.inflight) {
          pending.reject(err instanceof Error ? err : new Error(String(err)));
        }
        this.inflight.clear();
      });
      this.workers.push(w);
      this.idle.push(w);
    }
  }

  exec(req: PreexecRequest): Promise<PreexecResponse> {
    return new Promise((resolve, reject) => {
      this.queue.push({ req, resolve, reject });
      this.drain();
    });
  }

  private drain() {
    while (this.idle.length > 0 && this.queue.length > 0) {
      const w = this.idle.pop()!;
      const task = this.queue.shift()!;
      this.inflight.set(task.req.id, { resolve: task.resolve, reject: task.reject });
      w.postMessage(task.req);
    }
  }

  async close(): Promise<void> {
    await Promise.all(this.workers.map((w) => w.terminate()));
  }
}

type CommitMode = "final" | "roots";

type ApplyLeafWrite = { index: string; data: Uint8Array | number[] };
type ApplyRecordUpdate = { hash: number[]; data: string[] };
type ApplyTxTrace = { writes: ApplyLeafWrite[]; updateRecords: ApplyRecordUpdate[] };

type CommitRequest = {
  id: number;
  mode: CommitMode;
  rootBytes: number[];
  txs: ApplyTxTrace[];
  session?: string | null;
};

type CommitOk = {
  id: number;
  ok: true;
  mode: CommitMode;
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

class CommitPool {
  private worker: NodeWorker;
  private queue: Array<{ req: CommitRequest; resolve: (resp: CommitResponse) => void; reject: (err: Error) => void }> = [];
  private inflight: Map<number, { resolve: (resp: CommitResponse) => void; reject: (err: Error) => void }> = new Map();
  private busy = false;
  private nextId = 1;

  constructor(workerUrl: URL) {
    this.worker = new NodeWorker(workerUrl, { type: "module" } as any);
    this.worker.on("message", (msg: CommitResponse) => {
      const pending = this.inflight.get(msg.id);
      if (!pending) return;
      this.inflight.delete(msg.id);
      pending.resolve(msg);
      this.busy = false;
      this.drain();
    });
    this.worker.on("error", (err) => {
      for (const [, pending] of this.inflight) {
        pending.reject(err instanceof Error ? err : new Error(String(err)));
      }
      this.inflight.clear();
      this.busy = false;
    });
  }

  exec(mode: CommitMode, rootBytes: number[], txs: ApplyTxTrace[], session: string | null): Promise<CommitResponse> {
    const id = this.nextId++;
    const req: CommitRequest = { id, mode, rootBytes, txs, session };
    return new Promise((resolve, reject) => {
      this.queue.push({ req, resolve, reject });
      this.drain();
    });
  }

  private drain() {
    if (this.busy) return;
    const task = this.queue.shift();
    if (!task) return;
    this.busy = true;
    this.inflight.set(task.req.id, { resolve: task.resolve, reject: task.reject });
    this.worker.postMessage(task.req);
  }

  async close(): Promise<void> {
    await this.worker.terminate();
  }
}

type PendingJob = {
  job: Job;
  resolve: (value: unknown) => void;
  reject: (err: unknown) => void;
};

class OptimisticSequencer {
  private pool: PreexecPool;
  private commit: CommitPool | null;
  private pending: PendingJob[] = [];
  private flushing = false;
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  private nextId = 1;
  private rootBytes: number[];
  private bundleTxCount = 0;
  private serialUntilMs = 0;

  constructor(private service: Service) {
    this.pool = new PreexecPool(new URL("./preexec/preexec_worker.js", import.meta.url), OPTIMISTIC_WORKERS);
    this.commit = OPTIMISTIC_COMMIT_WORKER ? new CommitPool(new URL("./optimistic/commit_worker.js", import.meta.url)) : null;
    this.rootBytes = rootU64ToBytes(service.merkleRoot);
  }

  enqueue(job: Job): Promise<unknown> {
    return new Promise((resolve, reject) => {
      this.pending.push({ job, resolve, reject });
      this.scheduleFlush();
    });
  }

  private scheduleFlush() {
    if (this.flushing) return;
    if (this.pending.length >= OPTIMISTIC_BATCH) {
      void this.flush();
      return;
    }
    if (this.flushTimer) return;
    this.flushTimer = setTimeout(() => {
      this.flushTimer = null;
      void this.flush();
    }, OPTIMISTIC_FLUSH_MS);
  }

  private async flush() {
    if (this.flushing) return;
    this.flushing = true;
    try {
      const prefetchEnabled = OPTIMISTIC_PIPELINE && this.commit !== null;
      let prefetched:
        | {
            batch: PendingJob[];
            preexecs: Promise<PreexecResponse[]>;
            validateWrites: Set<string>;
          }
        | null = null;

      while (this.pending.length > 0 || prefetched) {
        const batchStart = performance.now();
        let preexecWallMs = 0;
        let applyWallMs = 0;
        let installWallMs = 0;
        let serialWallMs = 0;
        let preexecHandleTxSumMs = 0;
        let preexecVerifySumMs = 0;
        let preexecOk = 0;
        let preexecErr = 0;
        let applyWrites = 0;
        let applyUpdateRecords = 0;
        let validateWritesSize = 0;
        let validateConflicts = 0;
        let didPrefetch = false;

        if (OPTIMISTIC_SERIAL_COOLDOWN_MS > 0 && Date.now() < this.serialUntilMs) {
          if (prefetched) {
            // Do not keep prefetched work while in serial fallback: we must execute on latest root.
            this.pending = prefetched.batch.concat(this.pending);
            prefetched = null;
          }
          const batch = this.pending.splice(0, OPTIMISTIC_BATCH);
          for (const p of batch) {
            try {
              const serialStart = performance.now();
              const result = await this.applySerial(p);
              serialWallMs += performance.now() - serialStart;
              p.resolve(result);
            } catch (e) {
              p.reject(e);
            }
          }
          if (batch.length === 0) break;
          continue;
        }

        let batch: PendingJob[] = [];
        let preexecPromise: Promise<PreexecResponse[]>;
        let validateWrites = new Set<string>();
        if (prefetched) {
          batch = prefetched.batch;
          preexecPromise = prefetched.preexecs;
          validateWrites = prefetched.validateWrites;
          validateWritesSize = validateWrites.size;
          prefetched = null;
        } else {
          batch = this.pending.splice(0, OPTIMISTIC_BATCH);
          const snapshotRoot = bytes32ToRootU64(this.rootBytes);
          const rootArr = Array.from(snapshotRoot);
          preexecPromise = Promise.all(
            batch.map((p) => {
              const jobId = String(p.job.id);
              const data = p.job.data as any;
              const cached = this.service.txU64Cache?.get(jobId);
              const b64 = typeof data?.u64 === "string" ? data.u64 : undefined;
              const hasU64 = cached || b64;
              return this.pool.exec({
                id: this.nextId++,
                root: rootArr,
                u64array: cached,
                u64: b64,
                signature: hasU64 ? undefined : (data.value as TxWitness),
                skipVerify: data.verified === true,
                session: MERKLE_SESSION_OVERLAY ? this.service.merkleSession : null,
              });
            }),
          );
        }

        const preexecStart = performance.now();
        const preexecs = await preexecPromise;
        preexecWallMs += performance.now() - preexecStart;

        const unionWrites = new Set<string>();
        const unionReads = new Set<string>();
        const deferred: PendingJob[] = [];
        const chosen: Array<{ p: PendingJob; resp: PreexecOk; isReplay: boolean }> = [];

        for (let i = 0; i < batch.length; i++) {
          const p = batch[i]!;
          const resp = preexecs[i]!;
          if (!resp.ok) {
            preexecErr += 1;
            this.service.txU64Cache?.delete(String(p.job.id));
            p.reject(new Error(resp.error));
            continue;
          }
          preexecOk += 1;
          preexecHandleTxSumMs += resp.timingMs.handleTx ?? 0;
          if (typeof resp.timingMs.verify === "number") {
            preexecVerifySumMs += resp.timingMs.verify;
          }

          const { reads, writes } = buildRwSets(resp.trace);
          if (validateWritesSize > 0) {
            const staleConflict = intersects(writes, validateWrites) || intersects(reads, validateWrites);
            if (staleConflict) {
              validateConflicts += 1;
              deferred.push(p);
              continue;
            }
          }
          const conflict =
            intersects(writes, unionWrites) ||
            intersects(reads, unionWrites) ||
            intersects(writes, unionReads);

          const errCode = resp.result?.[0] ?? 0n;
          const isOk = typeof errCode === "bigint" ? errCode === 0n : BigInt(errCode) === 0n;

          if (conflict) {
            deferred.push(p);
            continue;
          }

          if (!isOk) {
            // If it doesn't depend on prior writes (no conflict), treat it as permanent failure.
            const errorMsg = application.decode_error(Number(errCode));
            this.service.txU64Cache?.delete(String(p.job.id));
            p.reject(new Error(errorMsg));
            continue;
          }

          chosen.push({ p, resp, isReplay: p.job.name === "replay" });
          for (const r of reads) unionReads.add(r);
          for (const w of writes) unionWrites.add(w);
        }

        // M3 pipeline: if this batch is fully conflict-free, prefetch the next batch on the current root
        // (which is about to be advanced), and validate it against this batch's committed write-set.
        if (prefetchEnabled && deferred.length === 0 && chosen.length > 0 && this.pending.length > 0) {
          const nextBatch = this.pending.splice(0, OPTIMISTIC_BATCH);
          if (nextBatch.length > 0) {
            const snapshotRoot = bytes32ToRootU64(this.rootBytes);
            const rootArr = Array.from(snapshotRoot);
            const nextPreexecs = Promise.all(
              nextBatch.map((p) => {
                const jobId = String(p.job.id);
                const data = p.job.data as any;
                const cached = this.service.txU64Cache?.get(jobId);
                const b64 = typeof data?.u64 === "string" ? data.u64 : undefined;
                const hasU64 = cached || b64;
                return this.pool.exec({
                  id: this.nextId++,
                  root: rootArr,
                  u64array: cached,
                  u64: b64,
                  signature: hasU64 ? undefined : (data.value as TxWitness),
                  skipVerify: data.verified === true,
                  session: MERKLE_SESSION_OVERLAY ? this.service.merkleSession : null,
                });
              }),
            );
            prefetched = {
              batch: nextBatch,
              preexecs: nextPreexecs,
              validateWrites: new Set(unionWrites),
            };
            didPrefetch = true;
          }
        }

        let cursor = 0;
        while (cursor < chosen.length) {
          const remainingInBundle = OPTIMISTIC_BUNDLE_SIZE - this.bundleTxCount;
          const take = Math.min(remainingInBundle, chosen.length - cursor);
          const segment = chosen.slice(cursor, cursor + take);
          cursor += take;

          let roots: number[][] = [];
          let finalRootBytes: number[] | null = null;
          try {
            const applyStart = performance.now();
            const txs = segment.map((item) => ({
              writes: item.resp.trace.writes ?? [],
              updateRecords: item.resp.trace.updateRecords ?? [],
            }));
            for (const tx of txs) {
              applyWrites += tx.writes.length;
              applyUpdateRecords += tx.updateRecords.length;
            }
            if (LIGHT_JOB_RESULT) {
              if (this.commit) {
                const resp = await this.commit.exec(
                  "final",
                  this.rootBytes,
                  txs,
                  MERKLE_SESSION_OVERLAY ? this.service.merkleSession : null,
                );
                if (!resp.ok) throw new Error(resp.error);
                finalRootBytes = resp.finalRoot ?? null;
              } else {
                const doApply = () => apply_txs_final_async(this.rootBytes, txs);
                finalRootBytes = MERKLE_SESSION_OVERLAY ? await doApply() : await withMerkleSessionDisabledAsync(doApply);
              }
              if (!Array.isArray(finalRootBytes) || finalRootBytes.length !== 32) {
                throw new Error(
                  `apply_txs_final returned ${Array.isArray(finalRootBytes) ? finalRootBytes.length : typeof finalRootBytes} bytes, expected 32`,
                );
              }
            } else {
              if (this.commit) {
                const resp = await this.commit.exec(
                  "roots",
                  this.rootBytes,
                  txs,
                  MERKLE_SESSION_OVERLAY ? this.service.merkleSession : null,
                );
                if (!resp.ok) throw new Error(resp.error);
                roots = resp.roots ?? [];
              } else {
                const doApply = () => apply_txs_async(this.rootBytes, txs);
                roots = MERKLE_SESSION_OVERLAY ? await doApply() : await withMerkleSessionDisabledAsync(doApply);
              }
              if (!Array.isArray(roots) || roots.length !== segment.length) {
                throw new Error(
                  `apply_txs returned ${Array.isArray(roots) ? roots.length : typeof roots} roots, expected ${segment.length}`,
                );
              }
            }
            applyWallMs += performance.now() - applyStart;
          } catch (e) {
            for (const item of segment) {
              this.service.txU64Cache?.delete(String(item.p.job.id));
              item.p.reject(e);
            }
            break;
          }

          if (finalRootBytes) {
            this.rootBytes = finalRootBytes;
            this.service.optimisticHeadRoot = bytes32ToRootU64(this.rootBytes);
          }

          const installStart = performance.now();
          for (let i = 0; i < segment.length; i++) {
            const item = segment[i]!;
            const signature = (item.p.job.data as any).value as TxWitness;
            const jobId = item.p.job.id;
            try {
              const events = new BigUint64Array(item.resp.result);
              if (!finalRootBytes) {
                this.rootBytes = roots[i]!;
                this.service.optimisticHeadRoot = bytes32ToRootU64(this.rootBytes);
              }
              await this.service.optimisticInstall(signature, jobId, events, item.isReplay);
              item.p.resolve(await this.buildJobResult(item.p.job, signature));

              this.bundleTxCount += 1;
              if (this.bundleTxCount >= OPTIMISTIC_BUNDLE_SIZE) {
                await this.flushBundle(this.rootBytes);
                this.bundleTxCount = 0;
              }
            } catch (e) {
              item.p.reject(e);
            } finally {
              this.service.txU64Cache?.delete(String(jobId));
            }
          }
          installWallMs += performance.now() - installStart;
        }

        for (const p of deferred) {
          try {
            const serialStart = performance.now();
            const result = await this.applySerial(p);
            serialWallMs += performance.now() - serialStart;
            p.resolve(result);
          } catch (e) {
            p.reject(e);
          }
        }

        const considered = chosen.length + deferred.length;
        if (considered > 0) {
          const ratio = chosen.length / considered;
          if (deferred.length > 0 && ratio < OPTIMISTIC_SERIAL_THRESHOLD) {
            this.serialUntilMs = Date.now() + OPTIMISTIC_SERIAL_COOLDOWN_MS;
            if (LOG_OPTIMISTIC) {
              console.log("optimistic apply: high conflict, switching to serial fallback", {
                ratio: ratio.toFixed(3),
                chosen: chosen.length,
                deferred: deferred.length,
                cooldownMs: OPTIMISTIC_SERIAL_COOLDOWN_MS,
              });
            }
          }
        }

        if (considered === 0) {
          // All jobs in this batch failed during preexec; yield to allow more enqueues.
          break;
        }

        if (LOG_OPTIMISTIC_PROFILE) {
          const batchWallMs = performance.now() - batchStart;
          const ratio = considered > 0 ? chosen.length / considered : 0;
          console.log("optimistic profile batch", {
            pending: batch.length,
            chosen: chosen.length,
            deferred: deferred.length,
            preexecOk,
            preexecErr,
            chosenRatio: ratio.toFixed(3),
            validateWrites: validateWritesSize,
            validateConflicts,
            prefetch: didPrefetch,
            preexecWallMs: preexecWallMs.toFixed(2),
            applyWallMs: applyWallMs.toFixed(2),
            installWallMs: installWallMs.toFixed(2),
            serialWallMs: serialWallMs.toFixed(2),
            preexecHandleTxSumMs: preexecHandleTxSumMs.toFixed(2),
            preexecVerifySumMs: preexecVerifySumMs.toFixed(2),
            applyWrites,
            applyUpdateRecords,
            batchWallMs: batchWallMs.toFixed(2),
          });
        }
      }
    } finally {
      this.flushing = false;
      if (this.flushTimer) {
        clearTimeout(this.flushTimer);
        this.flushTimer = null;
      }
      if (this.pending.length > 0) {
        this.scheduleFlush();
      }
    }
  }

  private async flushBundle(postRootBytes: number[]) {
    const profileStart = performance.now();
    const postRootU64 = bytes32ToRootU64(postRootBytes);

    // Track bundle using pre-root (service.merkleRoot), then advance to post-root.
    const trackStart = performance.now();
    await this.service.trackBundle("");
    const trackMs = performance.now() - trackStart;

    const bundledTxs = transactions_witness;
    this.service.preMerkleRoot = this.service.merkleRoot;
    this.service.merkleRoot = postRootU64;
    this.service.optimisticHeadRoot = this.service.merkleRoot;

    const batchStart = performance.now();
    await this.service.txBatched(
      bundledTxs,
      merkleRootToBeHexString(this.service.preMerkleRoot),
      merkleRootToBeHexString(this.service.merkleRoot),
    );
    const batchMs = performance.now() - batchStart;

    transactions_witness = new Array();

    const resetStart = performance.now();
    if (process.env.RELOAD_WASM_ON_BUNDLE === "1") {
      await (initApplication as any)(bootstrap);
    }
    application.initialize(this.service.merkleRoot);
    await this.service.txManager.moveToCommit(merkleRootToBeHexString(this.service.merkleRoot));
    const resetMs = performance.now() - resetStart;

    const commitStart = performance.now();
    if (MERKLE_SESSION_OVERLAY && this.service.merkleSession) {
      const commitResult = commit_session(this.service.merkleSession);
      if (LOG_BUNDLE) {
        console.log("merkle overlay committed:", commitResult);
      }
    }
    const commitMs = performance.now() - commitStart;

    const mongoStart = performance.now();
    if (this.service.mongoWriteBuffer) {
      await this.service.mongoWriteBuffer.flush("bundle");
    }
    const mongoMs = performance.now() - mongoStart;

    if (LOG_OPTIMISTIC_PROFILE) {
      console.log("optimistic profile bundle", {
        txs: bundledTxs.length,
        trackMs: trackMs.toFixed(2),
        txBatchedMs: batchMs.toFixed(2),
        resetMs: resetMs.toFixed(2),
        merkleCommitMs: commitMs.toFixed(2),
        mongoFlushMs: mongoMs.toFixed(2),
        totalMs: (performance.now() - profileStart).toFixed(2),
      });
    }
  }

  private async buildJobResult(job: Job, signature: TxWitness) {
    if (LIGHT_JOB_RESULT) {
      return { bundle: this.service.txManager.currentUncommitMerkleRoot };
    }
    // Sync wasm instance to the latest root before serving get_state/snapshot.
    application.initialize(bytes32ToRootU64(this.rootBytes));
    if (!DISABLE_SNAPSHOT) {
      snapshot = JSON.parse(application.snapshot());
    }

    let player = null;
    if (job.name !== "replay") {
      const pkx = new LeHexBN(signature.pkx).toU64Array();
      player = JSON.parse(application.get_state(pkx));
    }
    return {
      player,
      state: snapshot,
      bundle: this.service.txManager.currentUncommitMerkleRoot,
    };
  }

  private async applySerial(p: PendingJob): Promise<unknown> {
    const signature = (p.job.data as any).value as TxWitness;
    const jobId = String(p.job.id);
    const u64array = (() => {
      const cached = this.service.txU64Cache?.get(jobId);
      if (cached) return cached;
      const b64 = (p.job.data as any)?.u64;
      if (typeof b64 === "string" && b64.length > 0) {
        try {
          return base64ToU64ArrayLE(b64);
        } catch {
          // fall through
        }
      }
      return signature_to_u64array(signature);
    })();

    try {
      application.initialize(bytes32ToRootU64(this.rootBytes));

      if (!(p.job.name === "transaction" && (p.job.data as any).verified === true)) {
        application.verify_tx_signature(u64array);
      }

      const txResult = MERKLE_SESSION_OVERLAY ? application.handle_tx(u64array) : withMerkleSessionDisabled(() => application.handle_tx(u64array));
      const errorCode = txResult[0];
      if (errorCode !== 0n) {
        throw new Error(application.decode_error(Number(errorCode)));
      }

      const postRoot = application.query_root();
      this.rootBytes = rootU64ToBytes(postRoot);
      this.service.optimisticHeadRoot = postRoot;

      await this.service.optimisticInstall(signature, p.job.id, txResult, p.job.name === "replay");

      this.bundleTxCount += 1;
      if (this.bundleTxCount >= OPTIMISTIC_BUNDLE_SIZE) {
        await this.flushBundle(this.rootBytes);
        this.bundleTxCount = 0;
      }

      return await this.buildJobResult(p.job, signature);
    } finally {
      this.service.txU64Cache?.delete(jobId);
    }
  }
}

let deploymode = false;
let remote = false;
let migrate = false;
let redisHost = 'localhost';
let redisPort = 6379;

//if md5 is invalid, this will throw an error; if unspecified, this will return false
let hasTask = await has_task();
let contractAddr = get_contract_addr();

if (process.env.DEPLOY) {
  deploymode = true;
}

if (hasTask) {
  remote = true;
}

if (!hasTask && contractAddr != "unspecified") {
  try {
    let merkleRoot = await getMerkleArray();
    console.log('migrate merkle root:', merkleRoot);
    migrate = true;
  } catch (e) {
    console.log(e);
  }
}

if (process.env.REDISHOST) {
  redisHost = process.env.REDISHOST;
}
if (process.env.REDIS_PORT) {
  const parsed = Number.parseInt(process.env.REDIS_PORT, 10);
  if (Number.isFinite(parsed) && parsed > 0) {
    redisPort = parsed;
  }
}

let taskid: string | null = null;

if (process.env.TASKID) {
  taskid = process.env.TASKID;
}

/* Global Params */

let transactions_witness = new Array();

let snapshot = JSON.parse("{}");

function getTimestamp(): string {
  return `${performance.now().toFixed(3)}ms`;
}

function randByte()  {
  return Math.floor(Math.random() * 0xff);
}

async function generateRandomSeed() {
  let randSeed = [randByte(), randByte(), randByte(), randByte(), randByte(), randByte(), randByte(), randByte()];
  let sha = sha256(new Uint8Array(randSeed));
  const mask64 = BigInt("0xFFFFFFFFFFFFFFFF");
  const shaCommitment = BigInt(sha) & mask64;
  const randRecord = new modelRand({
    commitment: shaCommitment.toString(),
    seed: randSeed,
  });
  try {
    await randRecord.save();
    //console.log("Generated Rand Seed:", randSeed, shaCommitment);
    return shaCommitment;
  } catch (e) {
    console.log("Generating random seed error!");
    process.exit(1)
  }
}

export class Service {
  worker: null | Worker;
  queue: null | Queue;
  sendEnqueueBuffer: SendEnqueueBuffer | null;
  txU64Cache: TxU64Cache | null;
  txCallback: (arg: TxWitness, events: BigUint64Array) => Promise<void>;
  txBatched: (arg: TxWitness[], preMerkleHexRoot: string, postMerkleRoot: string ) => Promise<void>;
  playerIndexer: (arg: any) => number;
  registerAPICallback: (app: Express) => void;
  merkleRoot: BigUint64Array;
  bundleIndex: number;
  preMerkleRoot: BigUint64Array | null;
  txManager: TxStateManager;
  blocklist: Map<string, number>;
  mongoWriteBuffer: MongoWriteBuffer | null;
  merkleSession: string | null;
  optimisticSequencer: OptimisticSequencer | null;
  optimisticHeadRoot: BigUint64Array | null;

  constructor(
      cb: (arg: TxWitness, events: BigUint64Array) => Promise<void> = async (arg: TxWitness, events: BigUint64Array) => {},
      txBatched: (arg: TxWitness[], merkleHexRoot: string, postMerkleRoot: string)=> Promise<void> = async (arg: TxWitness[], merkleHexRoot: string, postMerkleRoot: string) => {},
      registerAPICallback: (app: Express) => void = (app: Express) => {},
      registerPlayerIndexer: (player: any) => number = (player: any) => {return 0},
  ) {
    this.worker = null;
    this.queue = null;
    this.sendEnqueueBuffer = null;
    this.txU64Cache = SEND_U64_CACHE ? new TxU64Cache(SEND_U64_CACHE_MAX) : null;
    this.txCallback = cb;
    this.txBatched = txBatched;
    this.registerAPICallback = registerAPICallback;
    this.playerIndexer = registerPlayerIndexer;
    this.merkleRoot = new BigUint64Array([
      14789582351289948625n,
      10919489180071018470n,
      10309858136294505219n,
      2839580074036780766n,
    ]);
    this.bundleIndex = 0;
    this.preMerkleRoot = null;
    this.txManager = new TxStateManager(merkleRootToBeHexString(this.merkleRoot));
    this.blocklist = new Map();
    this.mongoWriteBuffer = null;
    this.merkleSession = null;
    this.optimisticSequencer = null;
    this.optimisticHeadRoot = null;
  }

  async syncToLatestMerkelRoot() {
    let currentMerkle = merkleRootToBeHexString(this.merkleRoot);
    let prevMerkle = null;
    let bundle = await this.findBundleByMerkle(currentMerkle);
    while (bundle != null && bundle.postMerkleRoot != null) {
      const postMerkle = new BigUint64Array(hexStringToMerkleRoot(bundle.postMerkleRoot));
      console.log("sync merkle:", currentMerkle, "taskId:", bundle.taskId);
      bundle = await this.findBundleByMerkle(merkleRootToBeHexString(postMerkle));
      if(bundle != null) {
        currentMerkle = bundle.merkleRoot;
        prevMerkle = bundle.preMerkleRoot;
        this.bundleIndex += 1;
      }
    }
    console.log("final merkle:", currentMerkle);
    this.merkleRoot = new BigUint64Array(hexStringToMerkleRoot(currentMerkle));
    if (prevMerkle) {
      this.preMerkleRoot = new BigUint64Array(hexStringToMerkleRoot(prevMerkle));

    }
  }

  async findBundleByMerkle(merkleHexRoot: string) {
    const prevBundle = await modelBundle.findOne(
      {
        merkleRoot: merkleHexRoot
      },
    );
    return prevBundle;
  }

  async findBundleIndex(merkleRoot: BigUint64Array) {
      try {
        const prevBundle = await modelBundle.findOne(
          {
            merkleRoot: merkleRootToBeHexString(merkleRoot),
          },
        );
        if (prevBundle != null) {
          return prevBundle!.bundleIndex;
        } else {
          throw Error("BundleNotFound");
        }
      } catch (e) {
        console.log(`fatal: bundle for ${merkleRoot} is not recorded`);
        process.exit();
      }
  }

  async trackBundle(taskId: string) {
    if (LOG_BUNDLE) {
      console.log("track bundle:", this.bundleIndex);
    }
    let preMerkleRootStr = "";
    if (this.preMerkleRoot != null) {
      preMerkleRootStr = merkleRootToBeHexString(this.preMerkleRoot);
      if (LOG_BUNDLE) {
        console.log("update merkle chain ...", preMerkleRootStr);
      }
      try {
        const prevBundle = await modelBundle.findOneAndUpdate({
          merkleRoot: merkleRootToBeHexString(this.preMerkleRoot),
        }, {
          postMerkleRoot: merkleRootToBeHexString(this.merkleRoot),
        }, {});
        if (this.bundleIndex != prevBundle!.bundleIndex) {
          console.log(`fatal: bundleIndex does not match: ${this.bundleIndex}, ${prevBundle!.bundleIndex}`);
          throw Error(`Bundle Index does not match: current index is ${this.bundleIndex}, previous index is ${prevBundle!.bundleIndex}`);
        }
        if (LOG_BUNDLE) {
          console.log("merkle chain prev is", prevBundle);
        }
      } catch (e) {
        console.log(`fatal: can not find bundle for previous MerkleRoot: ${merkleRootToBeHexString(this.preMerkleRoot)}`);
        throw Error(`fatal: can not find bundle for previous MerkleRoot: ${merkleRootToBeHexString(this.preMerkleRoot)}`);
      }
    }
    this.bundleIndex += 1;
    if (LOG_BUNDLE) {
      console.log("add transaction bundle:", this.bundleIndex, merkleRootToBeHexString(this.merkleRoot));
    }
    const bundleRecord = new modelBundle({
      merkleRoot: merkleRootToBeHexString(this.merkleRoot),
      preMerkleRoot: preMerkleRootStr,
      taskId: taskId,
      bundleIndex: this.bundleIndex,
    });
    try {
      await bundleRecord.save();
      if (LOG_BUNDLE) {
        console.log(`task recorded with key: ${merkleRootToBeHexString(this.merkleRoot)}`);
      }
    }
    catch (e) {
      let record = await modelBundle.findOneAndUpdate({
        merkleRoot: merkleRootToBeHexString(this.merkleRoot),
      }, {
        taskId: taskId,
        postMerkleRoot: "",
        preMerkleRoot: preMerkleRootStr,
        bundleIndex: this.bundleIndex,
      }, {});
      console.log("fatal: conflict db merkle");
      // TODO: do we need to trim the corrputed branch?
      if (LOG_BUNDLE) {
        console.log(record);
      }
      //throw e
    }
    return bundleRecord;
  }


  async install_transactions(tx: TxWitness, jobid: string | undefined, events: BigUint64Array, isReplay = false): Promise<boolean> {
    // const installStartTime = performance.now();
    let bundled = false;
    if (LOG_TX) {
      console.log("installing transaction into rollup ...");
    }
    transactions_witness.push(tx);
    // if (!isReplay) {
    // const insertStart = performance.now();
    const handled = await this.txManager.insertTxIntoCommit(tx, isReplay);
    // const insertEnd = performance.now();
    // console.log(`[${getTimestamp()}] insertTxIntoCommit took: ${insertEnd - insertStart}ms, handled: ${handled}`);
    if (handled == false) {
        // const callbackStart = performance.now();
        await this.txCallback(tx, events);
        // const callbackEnd = performance.now();
        // console.log(`[${getTimestamp()}] txCallback took: ${callbackEnd - callbackStart}ms`);
    }
    // }
    if (!DISABLE_SNAPSHOT) {
      snapshot = JSON.parse(application.snapshot());
    }
    if (LOG_TX) {
      console.log("transaction installed, rollup pool length is:", transactions_witness.length);
    }
    if (!isReplay && !DISABLE_MONGO_TX_STORE) {
      if (this.mongoWriteBuffer) {
        this.mongoWriteBuffer.enqueueTx({
          msg: tx.msg,
          pkx: tx.pkx,
          sigx: tx.sigx,
        });
      } else {
        const txRecord = new modelTx(tx);
        if (ASYNC_MONGO_WRITES) {
          void txRecord.save().catch((e) => {
            console.log("fatal: store tx failed ... process will terminate", e);
            process.exit(1);
          });
        } else {
          try {
            await txRecord.save();
          } catch (e) {
            console.log("fatal: store tx failed ... process will terminate", e);
            process.exit(1);
          }
        }
      }
    }
    if (application.preempt()) {
      bundled = true;
      // const preemptStart = performance.now();
      if (LOG_BUNDLE) {
        console.log("rollup reach its preemption point, generating proof:");
      }
      let txdata = application.finalize();
      // const finalizeEnd = performance.now();
      // console.log(`[${getTimestamp()}] application.finalize took: ${finalizeEnd - preemptStart}ms`);
      if (LOG_BUNDLE) {
        console.log("txdata is:", txdata);
      }
      let task_id = null;

      // TODO: store a bundle before we fail
      // let bundle = await this.trackBundle('');
      if (deploymode) {
        try {
          task_id = await submitProofWithRetry(this.merkleRoot, transactions_witness, txdata);
        } catch (e) {
          console.log(e);
          process.exit(1); // this should never happen and we stop the whole process
        }
      }
      try {
        if (LOG_BUNDLE) {
          console.log("proving task submitted at:", task_id);
          console.log("tracking task in db current ...", merkleRootToBeHexString(this.merkleRoot));
        }

        // const trackStart = performance.now();
        await this.trackBundle(task_id);
        // const trackEnd = performance.now();
        // console.log(`[${getTimestamp()}] trackBundle took: ${trackEnd - trackStart}ms`);

        // preserve witness for batch callback before clearing
        const bundledTxs = transactions_witness;

        // set pre/post roots for next bundle
        this.preMerkleRoot = this.merkleRoot;

        // need to update merkle_root as the input of next proof
        this.merkleRoot = application.query_root();

        // record all the txs externally so that the external db can preserve a snap shot
        // const batchStart = performance.now();
        await this.txBatched(bundledTxs, merkleRootToBeHexString(this.preMerkleRoot), merkleRootToBeHexString(this.merkleRoot));
        // const batchEnd = performance.now();
        // console.log(`[${getTimestamp()}] txBatched took: ${batchEnd - batchStart}ms`);

        // clear witness queue for next bundle
        transactions_witness = new Array();

        // reset application here
        if (LOG_BUNDLE) {
          console.log("restore root:", this.merkleRoot);
        }
        // const resetStart = performance.now();
        if (process.env.RELOAD_WASM_ON_BUNDLE === '1') {
          await (initApplication as any)(bootstrap);
        }
        application.initialize(this.merkleRoot);
        await this.txManager.moveToCommit(merkleRootToBeHexString(this.merkleRoot));

        if (MERKLE_SESSION_OVERLAY && this.merkleSession) {
          const commitResult = commit_session(this.merkleSession);
          if (LOG_BUNDLE) {
            console.log("merkle overlay committed:", commitResult);
          }
        }
        // const resetEnd = performance.now();
        // console.log(`[${getTimestamp()}] Application reset took: ${resetEnd - resetStart}ms`);
      } catch (e) {
        console.log(e);
        process.exit(1); // this should never happen and we stop the whole process
      }
    }
    // let current_merkle_root = application.query_root();
    // const installEndTime = performance.now();
    // console.log("transaction installed with last root:", current_merkle_root);
    return bundled;
  }

  // Optimistic apply path: records tx/commit metadata but does not execute WASM in this process.
  // The actual state transition is applied from pre-exec traces (Merkle leaf/record writes).
  async optimisticInstall(tx: TxWitness, jobid: string | undefined, events: BigUint64Array, isReplay = false): Promise<boolean> {
    transactions_witness.push(tx);

    const handled = await this.txManager.insertTxIntoCommit(tx, isReplay);
    if (handled === false) {
      await this.txCallback(tx, events);
    }

    if (!isReplay && !DISABLE_MONGO_TX_STORE) {
      if (this.mongoWriteBuffer) {
        this.mongoWriteBuffer.enqueueTx({
          msg: tx.msg,
          pkx: tx.pkx,
          sigx: tx.sigx,
        });
      } else {
        const txRecord = new modelTx({
          msg: tx.msg,
          pkx: tx.pkx,
          sigx: tx.sigx,
        });
        if (ASYNC_MONGO_WRITES) {
          void txRecord.save().catch((e) => {
            console.log("fatal: store tx failed ... process will terminate", e);
            process.exit(1);
          });
        } else {
          await txRecord.save();
        }
      }
    }

    return false;
  }

  async initialize() {
    await mongoose.connect(get_mongoose_db(), {
      //useNewUrlParser: true,
      //useUnifiedTopology: true,
    });

    const db = mongoose.connection;
    db.on('error', () => {
      console.error('fatal: mongoose connection error ... process will terminate');
      process.exit(1);
    });
    db.once('open', () => {
      console.log('Connected to MongoDB');
    });
    
    // Call ensureIndexes after connection is established
    await ensureIndexes();
    await ensureAccountIndexes();

    if (BATCH_MONGO_WRITES) {
      this.mongoWriteBuffer = new MongoWriteBuffer({
        txCollection: modelTx.collection as any,
        jobCollection: modelJob.collection as any,
        batchSize: MONGO_BATCH_SIZE,
        flushMs: MONGO_FLUSH_MS,
        fatalTxError: MONGO_BATCH_FATAL,
        fatalJobError: MONGO_JOB_BATCH_FATAL,
      });
    }

    console.log("connecting redis server:", `${redisHost}:${redisPort}`);
    const connection = new IORedis(
      {
        host: redisHost,  // Your Redis server host
        port: redisPort,        // Your Redis server port
        reconnectOnError: (err) => {
          console.log("reconnect on error", err);
          return true;
        },
        maxRetriesPerRequest: null  // Important: set this to null
      }
    );

    connection.on('end', () => {
      console.log("fatal: redis disconnected unexpected ...");
      process.exit(1);
    });



    // bootstrap the application
    console.log(`bootstrapping ... (deploymode: ${deploymode}, remote: ${remote}, migrate: ${migrate})`);
    await (initBootstrap as any)();
    //console.log(bootstrap);
    console.log("loading wasm application ...");
    //console.log(application);
    await (initApplication as any)(bootstrap);

    console.log("check merkel database connection ...");
    test_merkle_db_service();

    if (MERKLE_SESSION_OVERLAY) {
      try {
        this.merkleSession = begin_session();
        (globalThis as any).__MERKLE_SESSION = this.merkleSession;
        console.log("merkle session overlay enabled:", this.merkleSession);
        process.once("exit", () => {
          if (!this.merkleSession) return;
          try {
            drop_session(this.merkleSession);
          } catch {
            // ignore
          }
        });
      } catch (e) {
        console.log("fatal: begin merkle session failed", e);
        process.exit(1);
      }
    }

    if (migrate) {
      if (remote) {
        throw Error("Can't migrate in remote mode");
      }
      this.merkleRoot = await getMerkleArray();
      console.log("Migrate: updated merkle root", this.merkleRoot);
    } else if(remote) {
    //initialize merkle_root based on the latest task
      while (true) {
        const hasTasks = await has_uncomplete_task();
        if (hasTasks) {
          console.log("remote = 1, There are uncompleted tasks. Trying again in 5 second...");
          await new Promise(resolve => setTimeout(resolve, 5000)); // Sleep for 5 second
        } else {
          console.log("remote = 1, No incomplete tasks. Proceeding...");
          break; // Exit the loop if there are no incomplete tasks
        }
      }

      let task = await get_latest_proof(taskid);
      console.log("latest taskId got from remote:", task?._id);
      console.log("latest task", task?.instances);
      if (task) {
        const instances = ZkWasmUtil.bytesToBN(task?.instances);
        this.merkleRoot = new BigUint64Array([
          BigInt(instances[4].toString()),
          BigInt(instances[5].toString()),
          BigInt(instances[6].toString()),
          BigInt(instances[7].toString()),
        ]);
        this.preMerkleRoot = new BigUint64Array([
          BigInt(instances[0].toString()),
          BigInt(instances[1].toString()),
          BigInt(instances[2].toString()),
          BigInt(instances[3].toString()),
        ]);

        this.bundleIndex = await this.findBundleIndex(this.preMerkleRoot);
        console.log("updated merkle root", this.merkleRoot, this.bundleIndex);
      }
    } else {
      await this.syncToLatestMerkelRoot();
    }

    const shardCount = get_shard_count();
    const shardId = get_shard_id();
    const queueName = get_queue_name();
    console.log("initialize sequener queue ...", { queueName, shardId, shardCount });
    const myQueue = new Queue(queueName, {connection});

    const waitingCount = await myQueue.getWaitingCount();
    console.log("waiting Count is:", waitingCount, " perform draining ...");
    await myQueue.drain();

    this.queue = myQueue;
    this.sendEnqueueBuffer =
      SEND_ENQUEUE_BATCH > 1 ? new SendEnqueueBuffer(myQueue, SEND_ENQUEUE_BATCH, SEND_ENQUEUE_FLUSH_MS, SEND_ENQUEUE_MAX_PENDING) : null;

    console.log("initialize application merkle db ...");


    this.txManager.loadCommit(merkleRootToBeHexString(this.merkleRoot));
    application.initialize(this.merkleRoot);

    // update the merkle root variable
    this.merkleRoot = application.query_root();
    this.optimisticHeadRoot = this.merkleRoot;

    if (OPTIMISTIC_APPLY) {
      console.log("optimistic apply enabled", {
        workers: OPTIMISTIC_WORKERS,
        batch: OPTIMISTIC_BATCH,
        flushMs: OPTIMISTIC_FLUSH_MS,
        bundleSize: OPTIMISTIC_BUNDLE_SIZE,
      });
      this.optimisticSequencer = new OptimisticSequencer(this);
    }

    // Automatically add a job to the queue every few seconds
    if (!DISABLE_AUTOTICK && application.autotick()) {
      setInterval(async () => {
        try {
          await myQueue.add('autoJob', {command:0});
        } catch (error) {
          console.error('Error adding automatic job to the queue:', error);
          process.exit(1);
        }
      }, 5000); // Change the interval as needed (e.g., 5000ms for every 5 seconds)
    }

    setInterval(() => {
      this.blocklist.clear();
    }, 30000);

    // Monitor queue length every 2 seconds (disabled by default for TPS)
    if (LOG_QUEUE_STATS) {
      setInterval(async () => {
        try {
          const waitingCount = await myQueue.getWaitingCount();
          const activeCount = await myQueue.getActiveCount();
          const delayedCount = await myQueue.getDelayedCount();
          const completedCount = await myQueue.getCompletedCount();
          const failedCount = await myQueue.getFailedCount();

          console.log(`[${getTimestamp()}] Queue Stats - Waiting: ${waitingCount}, Active: ${activeCount}, Delayed: ${delayedCount}, Completed: ${completedCount}, Failed: ${failedCount}`);
        } catch (error) {
          console.error('Error getting queue stats:', error);
        }
      }, 2000);
    }

    this.worker = new Worker(queueName, async job => {
      const jobStartTime = performance.now();
      // console.log(`[${getTimestamp()}] Worker started processing job: ${job.name}, id: ${job.id}`);
      
      if (job.name == 'autoJob') {
        if (OPTIMISTIC_APPLY) {
          try {
            let rand = await generateRandomSeed();
            if (this.optimisticHeadRoot) {
              application.initialize(this.optimisticHeadRoot);
            }
            let oldSeed = application.randSeed();

            let seed = 0n;
            if (oldSeed != 0n) {
              const randRecord = await modelRand.find({
                commitment: oldSeed.toString(),
              });
              seed = randRecord[0].seed!.readBigInt64LE();
            }

            const signature = sign(createCommand(0n, 0n, [seed, rand, 0n, 0n]), get_server_admin_key());
            await myQueue.add('transaction', { value: signature, verified: true });
            return { enqueued: true };
          } catch (error) {
            const jobEndTime = performance.now();
            console.log(`[${getTimestamp()}] AutoJob failed after ${jobEndTime - jobStartTime}ms:`, error);
            if (AUTOJOB_FATAL) {
              console.log("fatal: handling auto tick error, process will terminate.", error);
              process.exit(1);
            }
            throw error;
          }
        }
        // console.log(`[${getTimestamp()}] AutoJob tick started`);
        try {
          const randStartTime = performance.now();
          let rand = await generateRandomSeed();
          const randEndTime = performance.now();
          if (LOG_AUTOJOB) {
            console.log(`[${getTimestamp()}] AutoJob generateRandomSeed took: ${randEndTime - randStartTime}ms`);
          }

          const oldSeedStartTime = performance.now();
          let oldSeed = application.randSeed();
          const oldSeedEndTime = performance.now();
          if (LOG_AUTOJOB) {
            console.log(`[${getTimestamp()}] AutoJob get old seed took: ${oldSeedEndTime - oldSeedStartTime}ms`);
          }

          const oldSeedFindStartTime = performance.now();
          let seed = 0n;
          if (oldSeed != 0n) {
            const randRecord = await modelRand.find({
              commitment: oldSeed.toString(),
            });
            seed = randRecord[0].seed!.readBigInt64LE();
          };
          const oldSeedFindEndTime = performance.now();
          if (LOG_AUTOJOB) {
            console.log(`[${getTimestamp()}] AutoJob find old seed took: ${oldSeedFindEndTime - oldSeedFindStartTime}ms`);
          }

          let signature = sign(createCommand(0n, 0n, [seed, rand, 0n, 0n]), get_server_admin_key());
          //console.log("signautre is", signature);
          let u64array = signature_to_u64array(signature);

          const verifyTxSignatureStartTime = performance.now();
          application.verify_tx_signature(u64array);
          const verifyTxSignatureEndTime = performance.now();
          if (LOG_AUTOJOB) {
            console.log(`[${getTimestamp()}] AutoJob verify_tx_signature took: ${verifyTxSignatureEndTime - verifyTxSignatureStartTime}ms`);
          }

          const handleTxStart = performance.now();
          let txResult = application.handle_tx(u64array);
          const handleTxEnd = performance.now();
          if (LOG_AUTOJOB) {
            console.log(`[${getTimestamp()}] AutoJob handle_tx took: ${handleTxEnd - handleTxStart}ms`);
          }
          
          const installStart = performance.now();
          const bundled = await this.install_transactions(signature, job.id, txResult);
          if (bundled && this.mongoWriteBuffer) {
            await this.mongoWriteBuffer.flush("bundle");
          }
          const installEnd = performance.now();
          if (LOG_AUTOJOB) {
            console.log(`[${getTimestamp()}] AutoJob install_transactions took: ${installEnd - installStart}ms`);
          }
        } catch (error) {
          const jobEndTime = performance.now();
          console.log(`[${getTimestamp()}] AutoJob failed after ${jobEndTime - jobStartTime}ms:`, error);
          if (AUTOJOB_FATAL) {
            console.log("fatal: handling auto tick error, process will terminate.", error);
            process.exit(1);
          }
          throw error;
        }
        const jobEndTime = performance.now();
        if (LOG_AUTOJOB) {
          console.log(`[${getTimestamp()}] AutoJob completed in ${jobEndTime - jobStartTime}ms`);
        }
      } else if (job.name == 'transaction' || job.name == 'replay') {
        if (OPTIMISTIC_APPLY) {
          if (!this.optimisticSequencer) {
            throw new Error("optimistic apply enabled but sequencer not initialized");
          }
          try {
            const signature = job.data.value;
            const result = await this.optimisticSequencer.enqueue(job);
            if (job.name != 'replay' && !DISABLE_MONGO_JOB_STORE) {
              if (this.mongoWriteBuffer) {
                this.mongoWriteBuffer.enqueueJob({
                  jobId: (signature as any).hash + (signature as any).pkx,
                  message: (signature as any).message,
                  result: "succeed",
                });
              } else {
                const jobRecord = new modelJob({
                  jobId: (signature as any).hash + (signature as any).pkx,
                  message: (signature as any).message,
                  result: "succeed",
                });
                if (ASYNC_MONGO_WRITES) {
                  void jobRecord.save().catch((e) => {
                    console.log("Error: store transaction job error", e);
                  });
                } else {
                  try {
                    await jobRecord.save();
                  } catch (e) {
                    console.log("Error: store transaction job error", e);
                    throw e;
                  }
                }
              }
            }
            return result;
          } catch (e) {
            const jobEndTime = performance.now();
            console.log(`[${getTimestamp()}] ${job.name} failed after ${jobEndTime - jobStartTime}ms:`, e);
            const pkx = job.data.value.pkx;
            const fc = this.blocklist.get(pkx) || 0;
            this.blocklist.set(pkx, fc + 1);
            console.log("error optimistic apply", e);
            throw e;
          }
        }
        if (LOG_TX) {
          console.log("handle transaction ...");
        }
        try {
          let signature = job.data.value;
          const verifySignatureStart = performance.now();
          let u64array = signature_to_u64array(signature);
          if (!(job.name === 'transaction' && job.data.verified === true)) {
            application.verify_tx_signature(u64array);
          }
          const verifySignatureEnd = performance.now();
          if (LOG_TX) {
            console.log(`[${getTimestamp()}] ${job.name} verify_tx_signature took: ${verifySignatureEnd - verifySignatureStart}ms`);
          }
          const handleTxStart = performance.now();
          let txResult = application.handle_tx(u64array);
          const handleTxEnd = performance.now();
          if (LOG_TX) {
            console.log(`[${getTimestamp()}] ${job.name} handle_tx took: ${handleTxEnd - handleTxStart}ms`);
          }
          
          let errorCode = txResult[0];
          if (errorCode == 0n) {
            // make sure install transaction will succeed
            const installStart = performance.now();
            const bundled = await this.install_transactions(signature, job.id, txResult, job.name=='replay');
            const installEnd = performance.now();
            if (LOG_TX) {
              console.log(`[${getTimestamp()}] ${job.name} install_transactions took: ${installEnd - installStart}ms`);
            }
            if (job.name != 'replay' && !DISABLE_MONGO_JOB_STORE) {
              if (this.mongoWriteBuffer) {
                this.mongoWriteBuffer.enqueueJob({
                  jobId: signature.hash + signature.pkx,
                  message: signature.message,
                  result: "succeed",
                });
              } else {
                const jobRecord = new modelJob({
                  jobId: signature.hash + signature.pkx,
                  message: signature.message,
                  result: "succeed",
                });
                if (ASYNC_MONGO_WRITES) {
                  void jobRecord.save().catch((e) => {
                    console.log("Error: store transaction job error", e);
                  });
                } else {
                  try {
                    await jobRecord.save();
                  } catch (e) {
                    console.log("Error: store transaction job error", e);
                    throw e;
                  }
                }
              }
            }
            if (bundled && this.mongoWriteBuffer) {
              await this.mongoWriteBuffer.flush("bundle");
            }
          } else {
            let errorMsg = application.decode_error(Number(errorCode));
            throw Error(errorMsg)
          }

          // const jobEndTime = performance.now();
          if (LOG_TX) {
            console.log("done");
          }
          if (LIGHT_JOB_RESULT) {
            return { bundle: this.txManager.currentUncommitMerkleRoot };
          }
          let player = null;
          const getStateStartTime = performance.now();
          if (job.name != "replay") {
            // in replay mode we do not need the return value for player
            const pkx = new LeHexBN(job.data.value.pkx).toU64Array();
            let jstr = application.get_state(pkx);
            player = JSON.parse(jstr);
          }
          let result = {
            player: player,
            state: snapshot,
            bundle: this.txManager.currentUncommitMerkleRoot,
          };
          const getStateEndTime = performance.now();
          if (LOG_TX) {
            console.log(`[${getTimestamp()}] ${job.name} get_state took: ${getStateEndTime - getStateStartTime}ms`);
          }
          return result
        } catch (e) {
          const jobEndTime = performance.now();
          console.log(`[${getTimestamp()}] ${job.name} failed after ${jobEndTime - jobStartTime}ms:`, e);
          let pkx = job.data.value.pkx;
          let fc = this.blocklist.get(pkx) || 0;
          this.blocklist.set(pkx, fc + 1);
          console.log("error handle_tx", e);
          throw e
        }
      }
      const jobEndTime = performance.now();
      if (LOG_TX) {
        console.log(`[${getTimestamp()}] ${job.name} completed in ${jobEndTime - jobStartTime}ms`);
      }
    }, {
      connection,
      concurrency: OPTIMISTIC_QUEUE_CONCURRENCY,
    });
  }

  async serve() {
    // replay uncommitted transactions
    console.log("install bootstrap txs");
    for (const value of await this.txManager.getTxFromCommit(merkleRootToBeHexString(this.merkleRoot))) {
      const u64 = (() => {
        if (!SEND_U64_IN_JOBDATA) return undefined;
        try {
          return u64ArrayToBase64LE(signature_to_u64array(value));
        } catch {
          return undefined;
        }
      })();
      this.queue!.add('replay', { value, u64 });
    }
    console.log("start express server");
    const app = express();
    const port = (() => {
      const raw = get_service_port();
      const parsed = Number.parseInt(String(raw), 10);
      if (Number.isFinite(parsed) && parsed > 0) return parsed;
      return 3000;
    })();
    const shardCount = get_shard_count();
    const shardId = get_shard_id();

    app.use(express.json());
    app.use(cors());

    app.post('/send', async (req, res) => {
      const value = req.body;
      //console.log("value is", value);
      if (!value || (typeof value === "object" && Object.keys(value).length === 0)) {
        return res.status(400).send('Value is required');
      }

      try {
        let u64array: BigUint64Array;
        let u64b64: string | undefined;
        try {
          u64array = signature_to_u64array(value);
          application.verify_tx_signature(u64array);
          if (SEND_U64_IN_JOBDATA) {
            u64b64 = u64ArrayToBase64LE(u64array);
          }
        } catch (err) {
          console.error('Invalid signature:', err);
          return res.status(500).send('Invalid signature');
        }

        const fc = this.blocklist.get(value.pkx) || 0;
        if (fc > 3) {
          return res
            .status(500)
            .send(
              'This account is blocked for 1 minutes for multiple incorrect arguments'
            );
        }

        if (ENFORCE_SHARD && shardCount > 1) {
          const target = shardForPkx(value.pkx, shardCount);
          if (target !== shardId) {
            return res.status(409).send({
              success: false,
              error: "WrongShard",
              shard: target,
            });
          }
        }

        const hintedJobId = txJobId(value);
        if (this.txU64Cache && hintedJobId) {
          this.txU64Cache.set(hintedJobId, u64array);
        }

        const job = (() => {
          if (this.sendEnqueueBuffer !== null) {
            return this.sendEnqueueBuffer.enqueue(value, u64b64);
          }
          if (hintedJobId) {
            return this.queue!.add('transaction', { value, verified: true, u64: u64b64 }, { jobId: hintedJobId });
          }
          return this.queue!.add('transaction', { value, verified: true, u64: u64b64 });
        })();
        const enqueued = await job.catch((e) => {
          if (this.txU64Cache && hintedJobId) {
            this.txU64Cache.delete(hintedJobId);
          }
          throw e;
        });
        if (this.txU64Cache && !hintedJobId) {
          this.txU64Cache.set(String(enqueued.id), u64array);
        }
        return res.status(201).send({
          success: true,
          jobid: enqueued.id,
        });
      } catch (error) {
        console.error('Error adding job to the queue:', error);
        const status = (error as any)?.statusCode;
        if (typeof status === "number" && status >= 400 && status < 600) {
          return res.status(status).send((error as Error).message);
        }
        res.status(500).send('Failed to add job to the queue');
      }
    });

    app.post('/querytx', async (req, res) => {
      const value = req.body;
      if (!value) {
        return res.status(400).send('Value is required');
      }
      //console.log("receive query command on: ", value.pkx);
      try {
        const hash = value.hash;
        const pkx = value.pkx;
        let job = await modelJob.findOne({
            jobId: hash + pkx,
        });
        res.status(201).send({
          success: true,
          data: job,
        });
      } catch(e) {
        res.status(500).send('Get Tx Info Error');
      }
    });

    app.post('/query', async (req, res) => {
      const value = req.body;
      if (!value) {
        return res.status(400).send('Value is required');
      }
      //console.log("receive query command on: ", value.pkx);

      try {
        if (OPTIMISTIC_APPLY && this.optimisticHeadRoot) {
          application.initialize(this.optimisticHeadRoot);
          if (!DISABLE_SNAPSHOT) {
            snapshot = JSON.parse(application.snapshot());
          }
        }
        const pkx = new LeHexBN(value.pkx).toU64Array();
        let u64array = new BigUint64Array(4);
        u64array.set(pkx);
        let jstr = application.get_state(pkx);
        let player = JSON.parse(jstr);
        let result = {
          player: player,
          state: snapshot
        }
        void storeAccount(value.pkx, player, this.playerIndexer).catch((error) => {
          console.error('storeAccount failed:', error);
        });
        res.status(201).send({
          success: true,
          data: JSON.stringify(result),
        });

      } catch (error) {
        res.status(500).send('Get Status Error');
      }
    });

    app.get('/data/players/:start?', async(req:any, res) => {
      let start = req.params.start;
      if (Number.isNaN(start)) {
        res.status(201).send({
          success: false,
          data: [],
        });
      } else {
        let data = await queryAccounts(Number(start));
        res.status(201).send({
          success: true,
          data: data,
        });
      }
    });

    app.get('/data/bundles/:merkleroot?', async(req:any, res) => {
      let merkleRootStr = req.params.merkleroot;
      try {
        if (merkleRootStr == 'latest') {
          merkleRootStr = merkleRootToBeHexString(this.preMerkleRoot!);
        }
        let bundle = await modelBundle.findOne({
          merkleRoot: merkleRootStr,
        });
        let after = bundle;
        const bundles = [];
        while (bundle != null && bundles.length < 10) {
          bundles.unshift(bundle);
          bundle = await modelBundle.findOne({
            merkleRoot: (bundle.preMerkleRoot),
          });
        }
        bundle = after;
        const len = bundles.length;
        if (bundle) {
          bundle = await modelBundle.findOne({
            merkleRoot: (bundle.postMerkleRoot),
          });
        }
        while (bundle != null && bundles.length < len + 10) {
          bundles.push(bundle);
          bundle = await modelBundle.findOne({
            merkleRoot: (bundle.postMerkleRoot),
          });
        }
        return res.status(201).json({
          success: true,
          data: bundles
        });
      } catch (e: any) {
        return res.status(201).json({
          success: false,
          error: e.toString(),
          data: []
        });
      }
    });

    app.get('/job_status/:id', async (req, res) => {
      try {
        const jobId = req.params.id;
        const job = await Job.fromId(this.queue!, jobId);
        if (!job) {
          return res.status(404).json({ message: 'Job not found' });
        }
        return res.status(200).json({
          id: job.id,
          name: job.name,
          processedOn: job.processedOn,
          finishedOn: job.finishedOn,
          failedReason: job.failedReason,
        });
      } catch (err) {
        res.status(500).json({ message: (err as Error).toString() });
      }
    });

    app.get('/job/:id', async (req, res) => {
      try {
        let jobId = req.params.id;
        const job = await Job.fromId(this.queue!, jobId);
        return res.status(201).json(job);
      } catch (err) {
        // job not tracked
        console.log(err);
        res.status(500).json({ message: (err as Error).toString()});
      }
    });

    app.get('/global', async (req, res) => {
      return res.status(201).json(snapshot);
    });


    app.get('/prooftask/:root', async (req, res) => {
      try {
        let merkleRootString = req.params.root;
        let merkleRoot = new BigUint64Array(hexStringToMerkleRoot(merkleRootString));
        let record = await modelBundle.findOne({ merkleRoot: merkleRoot});
        if (record) {
          return res.status(201).json(record);
        } else {
          throw Error("TaskNotFound");
        }
      } catch (err) {
        // job not tracked
        console.log(err);
        res.status(500).json({ message: (err as Error).toString()});
      }
    });


    app.post('/config', async (req, res) => {
      try {
        let jstr = application.get_config();
        res.status(201).send({
          success: true,
          data: jstr
        });

      } catch (error) {
        res.status(500).send('Get Status Error');
      }
    });

    this.registerAPICallback(app);

    // Start the server
    app.listen(port, HTTP_HOST, HTTP_BACKLOG, () => {
      console.log(`Server is running on http://${HTTP_HOST}:${port}`);
    });
  }

}
