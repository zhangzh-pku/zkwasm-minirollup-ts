import { spawn } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";

import { createCommand, sign } from "zkwasm-minirollup-rpc";

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

const TS_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function log(...args) {
  console.log("[sharded-tps]", ...args);
}

function shardForPkx(pkx, shardCount) {
  if (shardCount <= 1) return 0;
  const hex = pkx.startsWith("0x") ? pkx.slice(2) : pkx;
  if (!/^[0-9a-fA-F]*$/.test(hex)) return 0;
  const buf = Buffer.from(hex.length % 2 === 0 ? hex : `0${hex}`, "hex");
  const digest = createHash("sha256").update(buf).digest();
  return digest.readUInt32LE(0) % shardCount;
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

let services = [];
let redisMonitor = null;

try {
  if (!Number.isFinite(SHARD_COUNT) || SHARD_COUNT <= 0) {
    throw new Error(`invalid SHARD_COUNT=${process.env.SHARD_COUNT}`);
  }

  const image = IMAGE ?? randomImage();
  log("starting", { SHARD_COUNT, PORT_BASE, IMAGE: image });

  if (START_SERVICE) {
    for (let shardId = 0; shardId < SHARD_COUNT; shardId++) {
      const port = PORT_BASE + shardId;
      const service = spawn("node", ["src/run.js"], {
        cwd: TS_ROOT,
        stdio: ["ignore", "inherit", "inherit"],
        env: {
          ...process.env,
          PORT: String(port),
          IMAGE: image,
          SHARD_ID: String(shardId),
          SHARD_COUNT: String(SHARD_COUNT),
          QUEUE_PREFIX,
          ENFORCE_SHARD: "1",
        },
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

  const payloads = [];
  for (let i = 0; i < TOTAL; i++) {
    const cmd = createCommand(NONCE_BASE + BigInt(i), COMMAND, [0n, 0n, 0n, 0n]);
    payloads.push(sign(cmd, keyFor(i)));
  }

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
