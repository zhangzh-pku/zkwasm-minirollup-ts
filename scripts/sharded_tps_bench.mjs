import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { cpus } from "node:os";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { Worker } from "node:worker_threads";

import { createCommand, sign } from "zkwasm-minirollup-rpc";
import { shardForPkx } from "../src/sharding.js";
import { merkleDbUriForShard, resolveMerkleDbUriBase } from "../src/sharded_bench_utils.js";

const PORT_BASE = parseInt(process.env.PORT_BASE ?? "3001", 10);
const SHARD_COUNT = parseInt(process.env.SHARD_COUNT ?? "1", 10);
const TOTAL = parseInt(process.env.TOTAL ?? "2000", 10);
const CONCURRENCY = parseInt(process.env.CONCURRENCY ?? "200", 10);
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS ?? "200", 10);
const JOB_TIMEOUT_MS = parseInt(process.env.JOB_TIMEOUT_MS ?? "1200000", 10);
const REDIS_HOST = process.env.REDISHOST ?? "localhost";
const REDIS_PORT = parseInt(process.env.REDIS_PORT ?? "6379", 10);
const QUEUE_PREFIX = process.env.QUEUE_PREFIX ?? "sequencer";
const ADMIN_KEY = process.env.SERVER_ADMIN_KEY ?? "1234567";
const COMMAND = BigInt(process.env.COMMAND ?? "0");
const IMAGE = process.env.IMAGE ?? null;
const START_SERVICE = process.env.START_SERVICE !== "0";
const NONCE_BASE = BigInt(process.env.NONCE_BASE ?? Date.now());
const KEY_COUNT = parseInt(process.env.KEY_COUNT ?? "1", 10);
const KEY_BASE = BigInt(process.env.KEY_BASE ?? ADMIN_KEY);
const MERKLE_RPC_MODE = process.env.MERKLE_RPC_MODE ?? "syncproc";
const GEN_WORKERS = (() => {
  const raw = parseInt(process.env.GEN_WORKERS ?? "", 10);
  if (Number.isFinite(raw) && raw > 0) return raw;
  return Math.max(1, cpus().length);
})();

const TS_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PAYLOAD_WORKER_URL = new URL("./tps_payload_worker.mjs", import.meta.url);

function log(...args) {
  console.log("[sharded-tps]", ...args);
}

function keyFor(i) {
  if (KEY_COUNT <= 1) return KEY_BASE.toString();
  return (KEY_BASE + BigInt(i % KEY_COUNT)).toString();
}

async function waitForReady(port) {
  const baseUrl = `http://127.0.0.1:${port}`;
  for (let i = 0; i < 60; i++) {
    try {
      const resp = await fetch(`${baseUrl}/config`, { method: "POST" });
      const data = await resp.json();
      if (data?.success === true) return;
    } catch {
      // retry
    }
    await sleep(500);
  }
  throw new Error(`service not ready: ${baseUrl}`);
}

async function sendPayload(port, payload) {
  const resp = await fetch(`http://127.0.0.1:${port}/send`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!resp.ok) {
    throw new Error(`send failed: ${resp.status}`);
  }
  const data = await resp.json();
  if (!data?.jobid) {
    throw new Error(`missing jobid: ${JSON.stringify(data)}`);
  }
  return data.jobid;
}

function queueNameForShard(shardId) {
  if (SHARD_COUNT <= 1) return QUEUE_PREFIX;
  return `${QUEUE_PREFIX}-${shardId}`;
}

function randomImage() {
  return randomBytes(16).toString("hex");
}

async function generatePayloads(total) {
  if (GEN_WORKERS <= 1 || total <= 1) {
    const payloads = [];
    for (let i = 0; i < total; i++) {
      const cmd = createCommand(NONCE_BASE + BigInt(i), COMMAND, [0n, 0n, 0n, 0n]);
      payloads.push(sign(cmd, keyFor(i)));
    }
    return payloads;
  }

  const workerCount = Math.min(GEN_WORKERS, total);
  log("generating payloads", { total, workers: workerCount });

  const payloads = new Array(total);
  const perWorker = Math.ceil(total / workerCount);
  const tasks = [];

  // Match local `keyFor()` semantics when KEY_COUNT <= 1 (use KEY_BASE, not ADMIN_KEY).
  const adminKeyForWorker = KEY_BASE.toString();
  for (let w = 0; w < workerCount; w++) {
    const start = w * perWorker;
    const end = Math.min(total, start + perWorker);
    if (start >= end) break;

    tasks.push(
      new Promise((resolve, reject) => {
        const worker = new Worker(PAYLOAD_WORKER_URL, {
          type: "module",
          workerData: {
            start,
            end,
            nonceBase: NONCE_BASE.toString(),
            command: COMMAND.toString(),
            adminKey: adminKeyForWorker,
            keyCount: KEY_COUNT,
            keyBase: KEY_BASE.toString(),
          },
        });
        worker.once("message", (msg) => {
          try {
            const { start: offset, payloads: chunk } = msg ?? {};
            if (!Number.isInteger(offset) || !Array.isArray(chunk)) {
              throw new Error("invalid worker message");
            }
            for (let i = 0; i < chunk.length; i++) {
              payloads[offset + i] = chunk[i];
            }
            resolve();
          } catch (err) {
            reject(err);
          } finally {
            worker.terminate().catch(() => {});
          }
        });
        worker.once("error", reject);
        worker.once("exit", (code) => {
          if (code !== 0) reject(new Error(`payload worker exited with code ${code}`));
        });
      }),
    );
  }

  await Promise.all(tasks);
  return payloads;
}

let services = [];
let redisMonitor = null;

try {
  if (!Number.isFinite(SHARD_COUNT) || SHARD_COUNT <= 0) {
    throw new Error(`invalid SHARD_COUNT=${process.env.SHARD_COUNT}`);
  }

  const image = IMAGE ?? randomImage();
  log("starting", { SHARD_COUNT, PORT_BASE, IMAGE: image });

  if (START_SERVICE) {
    const merkleDbBase = resolveMerkleDbUriBase(process.env);
    if (MERKLE_RPC_MODE === "native" && !merkleDbBase) {
      throw new Error("MERKLE_DB_URI is required when MERKLE_RPC_MODE=native");
    }
    for (let shardId = 0; shardId < SHARD_COUNT; shardId++) {
      const port = PORT_BASE + shardId;
      const env = {
        ...process.env,
        PORT: String(port),
        IMAGE: image,
        SHARD_ID: String(shardId),
        SHARD_COUNT: String(SHARD_COUNT),
        QUEUE_PREFIX,
        ENFORCE_SHARD: "1",
      };
      if (MERKLE_RPC_MODE === "native") {
        env.MERKLE_DB_URI = merkleDbUriForShard({
          base: merkleDbBase,
          image,
          shardId,
          shardCount: SHARD_COUNT,
        });
      }
      const service = spawn("node", ["src/run.js"], {
        cwd: TS_ROOT,
        stdio: ["ignore", "inherit", "inherit"],
        env,
      });
      services.push({ shardId, port, proc: service });
    }
  } else {
    services = Array.from({ length: SHARD_COUNT }, (_, shardId) => ({
      shardId,
      port: PORT_BASE + shardId,
      proc: null,
    }));
  }

  await Promise.all(services.map((s) => waitForReady(s.port)));
  log("all services ready");

  const { Queue } = await import("bullmq");
  const { default: IORedis } = await import("ioredis");
  const connection = new IORedis({
    host: REDIS_HOST,
    port: REDIS_PORT,
    maxRetriesPerRequest: null,
  });

  const queues = [];
  for (let shardId = 0; shardId < SHARD_COUNT; shardId++) {
    const queue = new Queue(queueNameForShard(shardId), { connection });
    const counts = await queue.getJobCounts("completed", "failed");
    queues.push({
      shardId,
      queue,
      startCompleted: counts.completed ?? 0,
      startFailed: counts.failed ?? 0,
    });
  }
  redisMonitor = { connection, queues };

  const payloads = await generatePayloads(TOTAL);

  let cursor = 0;
  let sendErrors = 0;
  const start = performance.now();

  const sendWorkers = Array.from({ length: CONCURRENCY }, async () => {
    while (true) {
      const idx = cursor++;
      if (idx >= payloads.length) return;
      const payload = payloads[idx];
      const shardId = shardForPkx(payload.pkx, SHARD_COUNT);
      const port = PORT_BASE + shardId;
      try {
        await sendPayload(port, payload);
      } catch {
        sendErrors += 1;
      }
    }
  });

  await Promise.all(sendWorkers);
  log("sent", { total: TOTAL, sendErrors });

  const deadline = Date.now() + JOB_TIMEOUT_MS;
  let done = 0;
  let failed = 0;

  while (Date.now() <= deadline) {
    done = 0;
    failed = 0;
    for (const m of redisMonitor.queues) {
      const counts = await m.queue.getJobCounts("completed", "failed");
      const completed = (counts.completed ?? 0) - m.startCompleted;
      const failCount = (counts.failed ?? 0) - m.startFailed;
      done += completed + failCount;
      failed += failCount;
    }
    if (done >= TOTAL - sendErrors) break;
    await sleep(POLL_INTERVAL_MS);
  }

  const end = performance.now();
  const durationSec = (end - start) / 1000;
  const finished = Math.min(TOTAL - sendErrors, done) - failed;
  const tps = finished / durationSec;

  log("done", {
    durationSec: durationSec.toFixed(2),
    finished,
    failed,
    pending: Math.max(0, TOTAL - sendErrors - done),
    tps: tps.toFixed(2),
  });
} catch (err) {
  console.error("[sharded-tps] error", err);
  process.exitCode = 1;
} finally {
  if (redisMonitor) {
    for (const m of redisMonitor.queues) {
      try {
        await m.queue.close();
      } catch {
        // ignore
      }
    }
    redisMonitor.connection.disconnect();
  }
  for (const s of services) {
    if (s.proc) s.proc.kill("SIGTERM");
  }
}
