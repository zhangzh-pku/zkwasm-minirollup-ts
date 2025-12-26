import { Worker } from "node:worker_threads";
import { setTimeout as sleep } from "node:timers/promises";
import { createCommand, sign } from "zkwasm-minirollup-rpc";

const TOTAL = parseInt(process.env.TOTAL ?? "200", 10);
const WORKERS = parseInt(process.env.WORKERS ?? "8", 10);
const KEY_COUNT = parseInt(process.env.KEY_COUNT ?? "1", 10);
const KEY_BASE = BigInt(process.env.KEY_BASE ?? "1234567");
const NONCE_BASE = BigInt(process.env.NONCE_BASE ?? Date.now());
const SKIP_VERIFY = process.env.SKIP_VERIFY === "1";
const COMMAND = BigInt(process.env.COMMAND ?? "0");

// Default to sync HTTP inside worker threads (avoids per-worker child-process spawn in syncproc mode).
process.env.MERKLE_RPC_MODE = process.env.MERKLE_RPC_MODE ?? "http";

// Default genesis root (same as Service constructor).
const ROOT = [
  14789582351289948625n,
  10919489180071018470n,
  10309858136294505219n,
  2839580074036780766n,
];

function keyFor(i) {
  if (KEY_COUNT <= 1) return KEY_BASE.toString();
  return (KEY_BASE + BigInt(i % KEY_COUNT)).toString();
}

function log(...args) {
  console.log("[preexec-probe]", ...args);
}

function intersects(a, b) {
  for (const x of a) {
    if (b.has(x)) return true;
  }
  return false;
}

function greedyConflictFreeSubset(items) {
  const unionWrites = new Set();
  const unionReads = new Set();
  const chosen = [];

  const sorted = [...items].sort((a, b) => a.writes.size - b.writes.size);
  for (const item of sorted) {
    if (intersects(item.writes, unionWrites)) continue;
    if (intersects(item.reads, unionWrites)) continue;
    if (intersects(item.writes, unionReads)) continue;

    chosen.push(item);
    for (const r of item.reads) unionReads.add(r);
    for (const w of item.writes) unionWrites.add(w);
  }

  return chosen;
}

class WorkerPool {
  constructor(workerUrl, size) {
    this.workerUrl = workerUrl;
    this.workers = [];
    this.idle = [];
    this.queue = [];
    this.inflight = new Map();

    for (let i = 0; i < size; i++) {
      const w = new Worker(workerUrl, { type: "module" });
      w.on("message", (msg) => {
        const pending = this.inflight.get(msg.id);
        if (!pending) return;
        this.inflight.delete(msg.id);
        this.idle.push(w);
        pending.resolve(msg);
        this._drain();
      });
      w.on("error", (err) => {
        log("worker error", err);
      });
      this.workers.push(w);
      this.idle.push(w);
    }
  }

  exec(payload) {
    return new Promise((resolve, reject) => {
      this.queue.push({ payload, resolve, reject });
      this._drain();
    });
  }

  _drain() {
    while (this.idle.length > 0 && this.queue.length > 0) {
      const w = this.idle.pop();
      const task = this.queue.shift();
      this.inflight.set(task.payload.id, task);
      w.postMessage(task.payload);
    }
  }

  async close() {
    await Promise.all(this.workers.map((w) => w.terminate()));
  }
}

const workerUrl = new URL("../src/preexec/preexec_worker.js", import.meta.url);
log("starting", { TOTAL, WORKERS, KEY_COUNT, MERKLE_RPC_MODE: process.env.MERKLE_RPC_MODE });

const pool = new WorkerPool(workerUrl, WORKERS);
try {
  const payloads = [];
  for (let i = 0; i < TOTAL; i++) {
    const cmd = createCommand(NONCE_BASE + BigInt(i), COMMAND, [0n, 0n, 0n, 0n]);
    const sig = sign(cmd, keyFor(i));
    payloads.push({
      id: i,
      root: ROOT,
      signature: sig,
      skipVerify: SKIP_VERIFY,
    });
  }

  const start = performance.now();
  const results = await Promise.all(payloads.map((p) => pool.exec(p)));
  const end = performance.now();

  let okCount = 0;
  let failCount = 0;
  let totalReads = 0;
  let totalWrites = 0;
  let totalGetRecords = 0;
  let totalUpdateRecords = 0;
  const writerCount = new Map();
  const readerCount = new Map();

  const items = [];

  for (const r of results) {
    if (!r.ok) {
      failCount += 1;
      continue;
    }
    okCount += 1;
    totalReads += r.trace.reads.length;
    totalWrites += r.trace.writes.length;
    totalGetRecords += r.trace.getRecords.length;
    totalUpdateRecords += r.trace.updateRecords.length;

    const reads = new Set(r.trace.reads);
    const writes = new Set(r.trace.writes.map((w) => w.index));
    items.push({ id: r.id, reads, writes });

    for (const idx of reads) {
      readerCount.set(idx, (readerCount.get(idx) ?? 0) + 1);
    }
    for (const idx of writes) {
      writerCount.set(idx, (writerCount.get(idx) ?? 0) + 1);
    }
  }

  const chosen = greedyConflictFreeSubset(items);

  const durationSec = (end - start) / 1000;
  log("preexec done", {
    durationSec: durationSec.toFixed(2),
    ok: okCount,
    fail: failCount,
    preexecTps: (okCount / durationSec).toFixed(2),
    avgReads: okCount > 0 ? (totalReads / okCount).toFixed(2) : "0",
    avgWrites: okCount > 0 ? (totalWrites / okCount).toFixed(2) : "0",
    avgGetRecords: okCount > 0 ? (totalGetRecords / okCount).toFixed(2) : "0",
    avgUpdateRecords: okCount > 0 ? (totalUpdateRecords / okCount).toFixed(2) : "0",
    greedyConflictFree: `${chosen.length}/${okCount}`,
  });

  const hotWriters = Array.from(writerCount.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10);
  if (hotWriters.length > 0) {
    log("top write-hotspots", hotWriters);
  }
} finally {
  await pool.close();
  // Let stdout flush (worker termination can be noisy).
  await sleep(50);
}
