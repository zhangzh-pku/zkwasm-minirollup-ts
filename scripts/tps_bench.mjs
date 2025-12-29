import { spawn } from "node:child_process";
import { cpus } from "node:os";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { Worker } from "node:worker_threads";
import { createCommand, sign } from "zkwasm-minirollup-rpc";

const PORT = parseInt(process.env.PORT ?? "3001", 10);
const TOTAL = parseInt(process.env.TOTAL ?? "200", 10);
const CONCURRENCY = parseInt(process.env.CONCURRENCY ?? "20", 10);
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS ?? "200", 10);
const JOB_TIMEOUT_MS = parseInt(process.env.JOB_TIMEOUT_MS ?? "120000", 10);
const POLL_MODE = process.env.POLL_MODE ?? "redis";
const REDIS_HOST = process.env.REDISHOST ?? "localhost";
const REDIS_PORT = parseInt(process.env.REDIS_PORT ?? "6379", 10);
const QUEUE_NAME = process.env.QUEUE_NAME ?? "sequencer";
const ADMIN_KEY = process.env.SERVER_ADMIN_KEY ?? "1234567";
const COMMAND = BigInt(process.env.COMMAND ?? "0");
const IMAGE = process.env.IMAGE ?? "0123456789abcdef0123456789abcdef";
const START_SERVICE = process.env.START_SERVICE !== "0";
const NONCE_BASE = BigInt(process.env.NONCE_BASE ?? Date.now());
const WARMUP = parseInt(process.env.WARMUP ?? "0", 10);
const KEY_COUNT = parseInt(process.env.KEY_COUNT ?? "1", 10);
const KEY_BASE_RAW = process.env.KEY_BASE ?? ADMIN_KEY;
let KEY_BASE = null;
try {
  KEY_BASE = BigInt(KEY_BASE_RAW);
} catch {
  KEY_BASE = null;
}
const GEN_WORKERS = (() => {
  const raw = parseInt(process.env.GEN_WORKERS ?? "", 10);
  if (Number.isFinite(raw) && raw > 0) return raw;
  return Math.max(1, cpus().length);
})();

const BASE_URL = `http://127.0.0.1:${PORT}`;
const TS_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PAYLOAD_WORKER_URL = new URL("./tps_payload_worker.mjs", import.meta.url);

function log(...args) {
  console.log("[tps]", ...args);
}

function keyFor(i) {
  if (KEY_COUNT <= 1) return ADMIN_KEY;
  if (KEY_BASE === null) {
    throw new Error("KEY_BASE must be a valid integer when KEY_COUNT > 1");
  }
  return (KEY_BASE + BigInt(i % KEY_COUNT)).toString();
}

async function waitForReady() {
  for (let i = 0; i < 30; i++) {
    try {
      const resp = await fetch(`${BASE_URL}/config`, { method: "POST" });
      const data = await resp.json();
      if (data?.success === true) {
        return;
      }
    } catch {
      // retry
    }
    await sleep(500);
  }
  throw new Error("service not ready");
}

async function generatePayloads(totalWithWarmup) {
  const command = BigInt(process.env.COMMAND ?? "0");
  const nonceBase = BigInt(process.env.NONCE_BASE ?? Date.now());

  if (GEN_WORKERS <= 1 || totalWithWarmup <= 1) {
    const payloads = new Array(totalWithWarmup);
    for (let i = 0; i < totalWithWarmup; i++) {
      const cmd = createCommand(nonceBase + BigInt(i), command, [0n, 0n, 0n, 0n]);
      payloads[i] = sign(cmd, keyFor(i));
    }
    return payloads;
  }

  const workerCount = Math.min(GEN_WORKERS, totalWithWarmup);
  log("generating payloads", { total: totalWithWarmup, workers: workerCount });

  const payloads = new Array(totalWithWarmup);
  const perWorker = Math.ceil(totalWithWarmup / workerCount);

  const tasks = [];
  for (let w = 0; w < workerCount; w++) {
    const start = w * perWorker;
    const end = Math.min(totalWithWarmup, start + perWorker);
    if (start >= end) break;

    tasks.push(
      new Promise((resolve, reject) => {
        const worker = new Worker(PAYLOAD_WORKER_URL, {
          type: "module",
          workerData: {
            start,
            end,
            nonceBase: nonceBase.toString(),
            command: command.toString(),
            adminKey: ADMIN_KEY,
            keyCount: KEY_COUNT,
            keyBase: KEY_BASE === null ? null : KEY_BASE.toString(),
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
      })
    );
  }

  await Promise.all(tasks);
  return payloads;
}

async function sendPayload(payload) {
  const resp = await fetch(`${BASE_URL}/send`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!resp.ok) {
    throw new Error(`send failed: ${resp.status}`);
  }
  const data = await resp.json();
  if (!data?.jobid) {
    throw new Error("missing jobid");
  }
  return data.jobid;
}

async function pollJobs(jobIds) {
  const pending = new Set(jobIds);
  const failed = new Map();
  const deadline = Date.now() + JOB_TIMEOUT_MS;

  async function fetchJobStatus(jobId) {
    let resp = await fetch(`${BASE_URL}/job_status/${jobId}`);
    if (!resp.ok) {
      resp = await fetch(`${BASE_URL}/job/${jobId}`);
    }
    const data = await resp.json();
    return data?.id ? data : data?.data;
  }

  while (pending.size > 0) {
    if (Date.now() > deadline) {
      break;
    }
    const batch = Array.from(pending);
    for (let i = 0; i < batch.length; i += CONCURRENCY) {
      const slice = batch.slice(i, i + CONCURRENCY);
      await Promise.all(
        slice.map(async (jobId) => {
          try {
            const job = await fetchJobStatus(jobId);
            if (job?.finishedOn || job?.failedReason) {
              pending.delete(jobId);
              if (job?.failedReason) {
                failed.set(jobId, job.failedReason);
              }
            }
          } catch {
            // keep pending
          }
        })
      );
    }
    if (pending.size > 0) {
      await sleep(POLL_INTERVAL_MS);
    }
  }

  return { pending, failed };
}

async function pollQueueByRedis(expected, monitor) {
  const pending = new Set();
  const failed = { size: 0 };
  const deadline = Date.now() + JOB_TIMEOUT_MS;

  const { queue, startCompleted, startFailed } = monitor;
  let finalCounts = await queue.getJobCounts("completed", "failed");

  while (Date.now() <= deadline) {
    finalCounts = await queue.getJobCounts("completed", "failed");
    const completed = (finalCounts.completed ?? 0) - startCompleted;
    const failCount = (finalCounts.failed ?? 0) - startFailed;
    const done = completed + failCount;
    if (done >= expected) {
      break;
    }
    await sleep(POLL_INTERVAL_MS);
  }

  // If we timed out, expose a synthetic pending size to keep output stable.
  const completed = (finalCounts.completed ?? 0) - startCompleted;
  const failCount = (finalCounts.failed ?? 0) - startFailed;
  failed.size = failCount;
  const done = Math.min(expected, completed + failCount);
  for (let i = done; i < expected; i++) {
    pending.add(String(i));
  }

  return { pending, failed };
}

let service;
let redisMonitor;
try {
  if (START_SERVICE) {
    log("starting service", { PORT, IMAGE });
    service = spawn("node", ["src/run.js"], {
      cwd: TS_ROOT,
      stdio: ["ignore", "inherit", "inherit"],
      env: { ...process.env, PORT: String(PORT), IMAGE },
    });
  }

  const totalWithWarmup = TOTAL + WARMUP;
  const payloadPromise = generatePayloads(totalWithWarmup);

  await waitForReady();
  log("service ready");

  if (POLL_MODE === "redis") {
    const { Queue } = await import("bullmq");
    const { default: IORedis } = await import("ioredis");
    const connection = new IORedis({
      host: REDIS_HOST,
      port: REDIS_PORT,
      maxRetriesPerRequest: null,
    });
    const queue = new Queue(QUEUE_NAME, { connection });
    const startCounts = await queue.getJobCounts("completed", "failed");
    redisMonitor = {
      queue,
      connection,
      startCompleted: startCounts.completed ?? 0,
      startFailed: startCounts.failed ?? 0,
    };
  }

  const payloads = await payloadPromise;

  if (WARMUP > 0) {
    log("warmup", { WARMUP });
    const warmPayloads = payloads.slice(0, Math.min(WARMUP, payloads.length));
    const warmJobIds = [];
    for (const payload of warmPayloads) {
      warmJobIds.push(await sendPayload(payload));
    }
    await pollJobs(warmJobIds);
  }

  const runPayloads = payloads.slice(WARMUP);

  const jobIds = [];
  let sendErrors = 0;
  let cursor = 0;
  const start = performance.now();

  const sendWorkers = Array.from({ length: CONCURRENCY }, async () => {
    while (true) {
      const idx = cursor++;
      if (idx >= runPayloads.length) return;
      try {
        const jobId = await sendPayload(runPayloads[idx]);
        jobIds[idx] = jobId;
      } catch {
        sendErrors += 1;
      }
    }
  });

  await Promise.all(sendWorkers);
  log("sent", { total: runPayloads.length, sendErrors });

  const expected = jobIds.filter(Boolean).length;
  const { pending, failed } =
    POLL_MODE === "redis"
      ? await pollQueueByRedis(expected, redisMonitor)
      : await pollJobs(jobIds.filter(Boolean));
  const end = performance.now();
  const durationSec = (end - start) / 1000;
  const finished = jobIds.length - pending.size - sendErrors;
  const tps = finished / durationSec;

  log("done", {
    durationSec: durationSec.toFixed(2),
    finished,
    pending: pending.size,
    failed: failed.size,
    tps: tps.toFixed(2),
  });
} catch (err) {
  console.error("[tps] error", err);
  process.exitCode = 1;
} finally {
  if (redisMonitor) {
    try {
      await redisMonitor.queue.close();
    } finally {
      redisMonitor.connection.disconnect();
    }
  }
  if (service) {
    service.kill("SIGTERM");
  }
}
