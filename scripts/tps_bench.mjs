import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import { createCommand, sign } from "zkwasm-minirollup-rpc";

const PORT = parseInt(process.env.PORT ?? "3001", 10);
const TOTAL = parseInt(process.env.TOTAL ?? "200", 10);
const CONCURRENCY = parseInt(process.env.CONCURRENCY ?? "20", 10);
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS ?? "200", 10);
const JOB_TIMEOUT_MS = parseInt(process.env.JOB_TIMEOUT_MS ?? "120000", 10);
const ADMIN_KEY = process.env.SERVER_ADMIN_KEY ?? "1234567";
const IMAGE = process.env.IMAGE ?? "0123456789abcdef0123456789abcdef";
const START_SERVICE = process.env.START_SERVICE !== "0";
const NONCE_BASE = BigInt(process.env.NONCE_BASE ?? Date.now());
const WARMUP = parseInt(process.env.WARMUP ?? "0", 10);

const BASE_URL = `http://127.0.0.1:${PORT}`;

function log(...args) {
  console.log("[tps]", ...args);
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

let service;
try {
  if (START_SERVICE) {
    log("starting service", { PORT, IMAGE });
    service = spawn("node", ["src/run.js"], {
      stdio: ["ignore", "inherit", "inherit"],
      env: { ...process.env, PORT: String(PORT), IMAGE },
    });
  }

  await waitForReady();
  log("service ready");

  const payloads = [];
  const totalWithWarmup = TOTAL + WARMUP;
  for (let i = 0; i < totalWithWarmup; i++) {
    const cmd = createCommand(NONCE_BASE + BigInt(i), 0n, [0n, 0n, 0n, 0n]);
    payloads.push(sign(cmd, ADMIN_KEY));
  }

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

  const { pending, failed } = await pollJobs(jobIds.filter(Boolean));
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
  if (service) {
    service.kill("SIGTERM");
  }
}
