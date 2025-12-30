import assert from "node:assert";
import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { after, before, test } from "node:test";
import { fileURLToPath } from "node:url";

import { createCommand, sign } from "zkwasm-minirollup-rpc";
import { MongoClient } from "mongodb";

import { shardForPkx } from "../src/sharding.js";

const TS_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const MERKLE_NATIVE_ADDON_PATH = path.join(
  TS_ROOT,
  "native",
  "merkle-native",
  "merkle_native.node",
);

function hasCmd(cmd) {
  const r = spawnSync("bash", ["-lc", `command -v ${cmd}`], { stdio: "ignore" });
  return r.status === 0;
}

const HAS_REDIS = hasCmd("redis-server") && hasCmd("redis-cli");
const HAS_MONGO = hasCmd("mongod");
const HAS_BUILT_JS = fs.existsSync(path.join(TS_ROOT, "src", "run.js"));
const HAS_NATIVE_ADDON = fs.existsSync(MERKLE_NATIVE_ADDON_PATH);
const CAN_BUILD_NATIVE = hasCmd("cargo");

const CAN_RUN_INTEGRATION = HAS_REDIS && HAS_MONGO && HAS_BUILT_JS && (HAS_NATIVE_ADDON || CAN_BUILD_NATIVE);
const INTEGRATION_SKIP = CAN_RUN_INTEGRATION
  ? false
  : "requires redis-server, redis-cli, mongod, built JS (npm run build) and native addon (or cargo to build it)";

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function getFreePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      if (!addr || typeof addr !== "object") {
        server.close(() => reject(new Error("failed to allocate port")));
        return;
      }
      const { port } = addr;
      server.close((err) => (err ? reject(err) : resolve(port)));
    });
  });
}

async function waitForHttpReady({ port, path: pathname, timeoutMs }) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const resp = await fetch(`http://127.0.0.1:${port}${pathname}`, { method: "POST" });
      if (resp.ok) {
        const data = await resp.json().catch(() => null);
        if (data?.success === true) return;
      }
    } catch {
      // retry
    }
    await sleep(200);
  }
  throw new Error(`service not ready: http://127.0.0.1:${port}${pathname}`);
}

async function pollJobFinished({ port, jobId, timeoutMs }) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const resp = await fetch(`http://127.0.0.1:${port}/job_status/${jobId}`);
    if (resp.status === 404) return { found: false, job: null };
    if (!resp.ok) {
      await sleep(200);
      continue;
    }
    const job = await resp.json();
    if (job?.failedReason) return { found: true, job };
    if (job?.finishedOn) return { found: true, job };
    await sleep(200);
  }
  throw new Error(`timeout waiting job ${jobId} on port ${port}`);
}

function spawnService(env) {
  const proc = spawn(process.execPath, ["src/run.js"], {
    cwd: TS_ROOT,
    env: { ...process.env, ...env },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const logs = [];
  proc.stdout.on("data", (buf) => logs.push(buf.toString("utf8")));
  proc.stderr.on("data", (buf) => logs.push(buf.toString("utf8")));
  return { proc, logs };
}

async function killProc(child) {
  if (!child || child.killed) return;
  child.kill("SIGTERM");
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    if (child.exitCode !== null) return;
    await sleep(50);
  }
  child.kill("SIGKILL");
}

async function queryPlayerState({ port, pkx }) {
  const resp = await fetch(`http://127.0.0.1:${port}/query`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ pkx }),
  });
  assert.strictEqual(resp.status, 201);
  const body = await resp.json();
  assert.strictEqual(body?.success, true);
  assert.ok(typeof body?.data === "string" && body.data.length > 0);
  return JSON.parse(body.data);
}

let redisProc;
let mongoProc;
let redisLogs;
let mongoLogs;
let redisPort;
let mongoPort;
let tmpRoot;

before(async () => {
  if (!CAN_RUN_INTEGRATION) return;

  tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "zkwasm-itest-"));
  const redis = await (async () => {
    const redisDir = path.join(tmpRoot, "redis");
    fs.mkdirSync(redisDir, { recursive: true });

    for (let attempt = 0; attempt < 5; attempt++) {
      const port = await getFreePort();
      const logs = [];
      const proc = spawn(
        "redis-server",
        ["--port", String(port), "--bind", "127.0.0.1", "--dir", redisDir, "--save", "", "--appendonly", "no"],
        { stdio: ["ignore", "pipe", "pipe"] },
      );
      proc.stdout.on("data", (buf) => logs.push(buf.toString("utf8")));
      proc.stderr.on("data", (buf) => logs.push(buf.toString("utf8")));

      let ready = false;
      const deadline = Date.now() + 10000;
      while (Date.now() < deadline) {
        if (proc.exitCode !== null) break;
        const ping = spawnSync(
          "redis-cli",
          ["-h", "127.0.0.1", "-p", String(port), "ping"],
          { stdio: ["ignore", "pipe", "pipe"], timeout: 500 },
        );
        const out = (ping.stdout ?? Buffer.from("")).toString("utf8").trim();
        if (ping.status === 0 && out === "PONG") {
          ready = true;
          break;
        }
        await sleep(200);
      }

      if (ready) {
        return { proc, port, logs };
      }
      await killProc(proc);
      await sleep(200);
    }
    throw new Error("failed to start redis-server after retries");
  })();

  redisProc = redis.proc;
  redisPort = redis.port;
  redisLogs = redis.logs;

  const mongo = await (async () => {
    const mongoDir = path.join(tmpRoot, "mongo");
    fs.mkdirSync(mongoDir, { recursive: true });

    for (let attempt = 0; attempt < 5; attempt++) {
      const port = await getFreePort();
      const logs = [];
      const proc = spawn(
        "mongod",
        ["--dbpath", mongoDir, "--bind_ip", "127.0.0.1", "--port", String(port)],
        { stdio: ["ignore", "pipe", "pipe"] },
      );
      proc.stdout.on("data", (buf) => logs.push(buf.toString("utf8")));
      proc.stderr.on("data", (buf) => logs.push(buf.toString("utf8")));

      const uri = `mongodb://127.0.0.1:${port}`;
      let ready = false;
      const deadline = Date.now() + 20000;
      while (Date.now() < deadline) {
        if (proc.exitCode !== null) break;
        try {
          const client = new MongoClient(uri, { serverSelectionTimeoutMS: 1000 });
          await client.connect();
          await client.db().admin().command({ ping: 1 });
          await client.close();
          ready = true;
          break;
        } catch {
          await sleep(200);
        }
      }

      if (ready) {
        return { proc, port, logs };
      }
      await killProc(proc);
      await sleep(200);
    }
    throw new Error("failed to start mongod after retries");
  })();

  mongoProc = mongo.proc;
  mongoPort = mongo.port;
  mongoLogs = mongo.logs;

  // Ensure native addon exists (required by MERKLE_RPC_MODE=native).
  if (!fs.existsSync(MERKLE_NATIVE_ADDON_PATH)) {
    const r = spawnSync("npm", ["run", "build:native"], { cwd: TS_ROOT, stdio: "inherit" });
    if (r.status !== 0) {
      throw new Error("failed to build native merkle addon for integration tests");
    }
  }
});

after(async () => {
  if (!CAN_RUN_INTEGRATION) return;
  await killProc(redisProc);
  await killProc(mongoProc);
  if (tmpRoot) {
    try {
      fs.rmSync(tmpRoot, { recursive: true, force: true });
    } catch {
      // ignore
    }
  }
});

test(
  "sharding integration: /send enforces shard + jobs isolated per queue + per-shard mongo DB",
  { timeout: 5 * 60 * 1000, skip: INTEGRATION_SKIP },
  async () => {
    const basePort0 = await getFreePort();
    const basePort1 = await getFreePort();
    const queuePrefix = `itest-seq-${process.pid}-${Date.now()}`;
    const mongoUri = `mongodb://127.0.0.1:${mongoPort}`;
    const image = "unspecified"; // skip remote has_task()

    const merkleRootDir = path.join(tmpRoot, "merkledb");
    fs.mkdirSync(merkleRootDir, { recursive: true });
    const merkleDb0 = path.join(merkleRootDir, "shard0");
    const merkleDb1 = path.join(merkleRootDir, "shard1");

    const commonEnv = {
      URI: mongoUri,
      REDISHOST: "127.0.0.1",
      REDIS_PORT: String(redisPort),
      DISABLE_AUTOTICK: "1",
      DISABLE_SNAPSHOT: "1",
      IMAGE: image,
      MERKLE_RPC_MODE: "native",
      MERKLE_NATIVE_PARALLEL: "0",
      RAYON_NUM_THREADS: "1",
      QUEUE_PREFIX: queuePrefix,
      SHARD_COUNT: "2",
      ENFORCE_SHARD: "1",
      // Keep processing simple and deterministic.
      OPTIMISTIC_APPLY: "0",
      BATCH_MONGO_WRITES: "1",
      BATCH_COMMIT_WRITES: "1",
      ASYNC_COMMIT_WRITES: "1",
    };

    const s0 = spawnService({
      ...commonEnv,
      PORT: String(basePort0),
      SHARD_ID: "0",
      MERKLE_DB_URI: merkleDb0,
    });
    const s1 = spawnService({
      ...commonEnv,
      PORT: String(basePort1),
      SHARD_ID: "1",
      MERKLE_DB_URI: merkleDb1,
    });

    try {
      await Promise.all([
        waitForHttpReady({ port: basePort0, path: "/config", timeoutMs: 90000 }),
        waitForHttpReady({ port: basePort1, path: "/config", timeoutMs: 90000 }),
      ]);

      const keyShard1 = "1234567";
      const keyShard0 = "1234568";
      const payloadShard1 = sign(createCommand(1n, 1n, [0n, 0n, 0n, 0n]), keyShard1);
      const payloadShard0 = sign(createCommand(2n, 1n, [0n, 0n, 0n, 0n]), keyShard0);

      assert.strictEqual(shardForPkx(payloadShard0.pkx, 2), 0);
      assert.strictEqual(shardForPkx(payloadShard1.pkx, 2), 1);

      // Correct shard requests succeed.
      {
        const resp = await fetch(`http://127.0.0.1:${basePort0}/send`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payloadShard0),
        });
        assert.strictEqual(resp.status, 201);
        const body = await resp.json();
        assert.strictEqual(body?.success, true);
        assert.ok(body?.jobid);

        const jobId = String(body.jobid);
        const status0 = await pollJobFinished({ port: basePort0, jobId, timeoutMs: 60000 });
        assert.strictEqual(status0.found, true);
        assert.ok(!status0.job.failedReason);

        const status1 = await pollJobFinished({ port: basePort1, jobId, timeoutMs: 2000 });
        assert.strictEqual(status1.found, false);

        const state = await queryPlayerState({ port: basePort0, pkx: payloadShard0.pkx });
        assert.ok(state?.player, `expected player to exist after tx, got: ${JSON.stringify(state)}`);
        assert.strictEqual(state.player.counter ?? 0, 0);
      }
      {
        const resp = await fetch(`http://127.0.0.1:${basePort1}/send`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payloadShard1),
        });
        assert.strictEqual(resp.status, 201);
        const body = await resp.json();
        assert.strictEqual(body?.success, true);
        assert.ok(body?.jobid);

        const jobId = String(body.jobid);
        const status1 = await pollJobFinished({ port: basePort1, jobId, timeoutMs: 60000 });
        assert.strictEqual(status1.found, true);
        assert.ok(!status1.job.failedReason);

        const status0 = await pollJobFinished({ port: basePort0, jobId, timeoutMs: 2000 });
        assert.strictEqual(status0.found, false);

        const state = await queryPlayerState({ port: basePort1, pkx: payloadShard1.pkx });
        assert.ok(state?.player, `expected player to exist after tx, got: ${JSON.stringify(state)}`);
        assert.strictEqual(state.player.counter ?? 0, 0);
      }

      // Business logic contract: installing the same player twice should fail deterministically.
      {
        const payload2 = sign(createCommand(3n, 1n, [0n, 0n, 0n, 0n]), keyShard0);
        const resp = await fetch(`http://127.0.0.1:${basePort0}/send`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload2),
        });
        assert.strictEqual(resp.status, 201);
        const body = await resp.json();
        assert.strictEqual(body?.success, true);
        const status0 = await pollJobFinished({ port: basePort0, jobId: String(body.jobid), timeoutMs: 60000 });
        assert.strictEqual(status0.found, true);
        assert.ok(String(status0.job.failedReason ?? "").includes("PlayerAlreadyExist"));
      }
      {
        const payload2 = sign(createCommand(2n, 1n, [0n, 0n, 0n, 0n]), keyShard1);
        const resp = await fetch(`http://127.0.0.1:${basePort1}/send`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload2),
        });
        assert.strictEqual(resp.status, 201);
        const body = await resp.json();
        assert.strictEqual(body?.success, true);
        const status1 = await pollJobFinished({ port: basePort1, jobId: String(body.jobid), timeoutMs: 60000 });
        assert.strictEqual(status1.found, true);
        assert.ok(String(status1.job.failedReason ?? "").includes("PlayerAlreadyExist"));
      }

      // /send request contract (missing body / invalid signature / blocklist).
      {
        const resp = await fetch(`http://127.0.0.1:${basePort0}/send`, { method: "POST" });
        assert.strictEqual(resp.status, 400);
        assert.ok((await resp.text()).includes("Value is required"));
      }
      {
        const resp = await fetch(`http://127.0.0.1:${basePort0}/send`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ pkx: payloadShard0.pkx }),
        });
        assert.strictEqual(resp.status, 500);
        assert.ok((await resp.text()).includes("Invalid signature"));
      }
      // After enough failed jobs, the account should be blocked on /send (fc > 3).
      {
        for (let i = 0; i < 3; i++) {
          const payload2 = sign(createCommand(BigInt(10 + i), 1n, [0n, 0n, 0n, 0n]), keyShard0);
          const resp = await fetch(`http://127.0.0.1:${basePort0}/send`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload2),
          });
          assert.strictEqual(resp.status, 201);
          const body = await resp.json();
          const status = await pollJobFinished({ port: basePort0, jobId: String(body.jobid), timeoutMs: 60000 });
          assert.strictEqual(status.found, true);
          assert.ok(String(status.job.failedReason ?? "").includes("PlayerAlreadyExist"));
        }

        const payloadBlocked = sign(createCommand(99n, 1n, [0n, 0n, 0n, 0n]), keyShard0);
        const resp = await fetch(`http://127.0.0.1:${basePort0}/send`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payloadBlocked),
        });
        assert.strictEqual(resp.status, 500);
        assert.ok((await resp.text()).includes("blocked"));
      }

      // Wrong shard requests are rejected.
      {
        const resp = await fetch(`http://127.0.0.1:${basePort0}/send`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payloadShard1),
        });
        assert.strictEqual(resp.status, 409);
        const body = await resp.json();
        assert.strictEqual(body?.error, "WrongShard");
        assert.strictEqual(body?.shard, 1);
      }
      {
        const resp = await fetch(`http://127.0.0.1:${basePort1}/send`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payloadShard0),
        });
        assert.strictEqual(resp.status, 409);
        const body = await resp.json();
        assert.strictEqual(body?.error, "WrongShard");
        assert.strictEqual(body?.shard, 0);
      }

      // Mongo DB is sharded by suffix.
      {
        const client = new MongoClient(mongoUri);
        await client.connect();
        const { databases } = await client.db().admin().listDatabases();
        const names = databases.map((d) => d.name);
        assert.ok(names.includes(`${image}_job-tracker_shard0`));
        assert.ok(names.includes(`${image}_job-tracker_shard1`));
        await client.close();
      }
    } catch (err) {
      const logs = [
        "=== shard0 logs ===",
        ...(s0.logs ?? []),
        "=== shard1 logs ===",
        ...(s1.logs ?? []),
      ].join("");
      throw new Error(`${err instanceof Error ? err.message : String(err)}\n${logs}`);
    } finally {
      await killProc(s0.proc);
      await killProc(s1.proc);
    }
  },
);

test(
  "send enqueue buffer: addBulk batches and enforces max pending",
  { timeout: 5 * 60 * 1000, skip: INTEGRATION_SKIP },
  async () => {
    const port = await getFreePort();
    const queuePrefix = `itest-enq-${process.pid}-${Date.now()}`;
    const mongoUri = `mongodb://127.0.0.1:${mongoPort}`;
    const image = `itest-enq-${process.pid}-${Date.now()}`;

    const merkleDb = fs.mkdtempSync(path.join(tmpRoot, "merkledb-enq-"));

    const env = {
      URI: mongoUri,
      REDISHOST: "127.0.0.1",
      REDIS_PORT: String(redisPort),
      DISABLE_AUTOTICK: "1",
      DISABLE_SNAPSHOT: "1",
      IMAGE: image,
      MERKLE_RPC_MODE: "native",
      MERKLE_NATIVE_PARALLEL: "0",
      RAYON_NUM_THREADS: "1",
      QUEUE_PREFIX: queuePrefix,
      SHARD_COUNT: "1",
      SHARD_ID: "0",
      ENFORCE_SHARD: "0",
      // Keep processing simple and deterministic.
      OPTIMISTIC_APPLY: "0",
      BATCH_MONGO_WRITES: "1",
      BATCH_COMMIT_WRITES: "1",
      ASYNC_COMMIT_WRITES: "1",
      // Force SendEnqueueBuffer path (queue.addBulk) + overload behavior.
      SEND_ENQUEUE_BATCH: "100",
      SEND_ENQUEUE_FLUSH_MS: "2000",
      SEND_ENQUEUE_MAX_PENDING: "2",
      MERKLE_DB_URI: merkleDb,
      PORT: String(port),
    };

    const svc = spawnService(env);
    try {
      await waitForHttpReady({ port, path: "/config", timeoutMs: 90000 });

      const payloads = [
        sign(createCommand(1n, 1n, [0n, 0n, 0n, 0n]), "9000001"),
        sign(createCommand(1n, 1n, [0n, 0n, 0n, 0n]), "9000002"),
        sign(createCommand(1n, 1n, [0n, 0n, 0n, 0n]), "9000003"),
        sign(createCommand(1n, 1n, [0n, 0n, 0n, 0n]), "9000004"),
        sign(createCommand(1n, 1n, [0n, 0n, 0n, 0n]), "9000005"),
      ];

      const responses = await Promise.all(
        payloads.map((p) =>
          fetch(`http://127.0.0.1:${port}/send`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(p),
          }),
        ),
      );
      const statuses = responses.map((r) => r.status);
      statuses.sort((a, b) => a - b);
      assert.deepStrictEqual(statuses, [201, 201, 503, 503, 503]);

      const okBodies = [];
      for (const r of responses) {
        if (r.status === 201) {
          okBodies.push(await r.json());
        } else {
          assert.ok((await r.text()).includes("Overloaded"));
        }
      }
      assert.strictEqual(okBodies.length, 2);
      for (const body of okBodies) {
        assert.strictEqual(body?.success, true);
        assert.ok(body?.jobid);
        const status = await pollJobFinished({ port, jobId: String(body.jobid), timeoutMs: 60000 });
        assert.strictEqual(status.found, true);
        assert.ok(!status.job.failedReason);
      }
    } catch (err) {
      throw new Error(`${err instanceof Error ? err.message : String(err)}\n${(svc.logs ?? []).join("")}`);
    } finally {
      await killProc(svc.proc);
    }
  },
);

test(
  "optimistic apply: conflict-free batch commits and conflicting txs serialize",
  { timeout: 8 * 60 * 1000, skip: INTEGRATION_SKIP },
  async () => {
    const port = await getFreePort();
    const queuePrefix = `itest-opt-${process.pid}-${Date.now()}`;
    const mongoUri = `mongodb://127.0.0.1:${mongoPort}`;
    const image = `itest-opt-${process.pid}-${Date.now()}`;
    const merkleDb = fs.mkdtempSync(path.join(tmpRoot, "merkledb-opt-"));

    const env = {
      URI: mongoUri,
      REDISHOST: "127.0.0.1",
      REDIS_PORT: String(redisPort),
      DISABLE_AUTOTICK: "1",
      DISABLE_SNAPSHOT: "1",
      DISABLE_MONGO_TX_STORE: "1",
      DISABLE_MONGO_JOB_STORE: "1",
      IMAGE: image,
      MERKLE_RPC_MODE: "native",
      MERKLE_NATIVE_PARALLEL: "0",
      RAYON_NUM_THREADS: "1",
      QUEUE_PREFIX: queuePrefix,
      SHARD_COUNT: "1",
      SHARD_ID: "0",
      ENFORCE_SHARD: "0",
      // Optimistic apply + pipeline (commit worker).
      OPTIMISTIC_APPLY: "1",
      OPTIMISTIC_PIPELINE: "1",
      OPTIMISTIC_WORKERS: "2",
      OPTIMISTIC_BATCH: "4",
      OPTIMISTIC_FLUSH_MS: "1",
      OPTIMISTIC_BUNDLE_SIZE: "1000",
      OPTIMISTIC_QUEUE_CONCURRENCY: "8",
      LIGHT_JOB_RESULT: "1",
      MERKLE_DB_URI: merkleDb,
      PORT: String(port),
    };

    const svc = spawnService(env);
    try {
      await waitForHttpReady({ port, path: "/config", timeoutMs: 90000 });

      // Conflict-free: distinct accounts should all succeed.
      const okPayloads = Array.from({ length: 8 }, (_, i) =>
        sign(createCommand(1n, 1n, [0n, 0n, 0n, 0n]), String(9100001 + i)),
      );
      const okResponses = await Promise.all(
        okPayloads.map((p) =>
          fetch(`http://127.0.0.1:${port}/send`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(p),
          }),
        ),
      );
      const okJobIds = [];
      for (const r of okResponses) {
        assert.strictEqual(r.status, 201);
        const body = await r.json();
        assert.strictEqual(body?.success, true);
        okJobIds.push(String(body.jobid));
      }
      const okStatuses = await Promise.all(
        okJobIds.map((jobId) => pollJobFinished({ port, jobId, timeoutMs: 60000 })),
      );
      for (const s of okStatuses) {
        assert.strictEqual(s.found, true);
        assert.ok(!s.job.failedReason);
      }
      for (const p of okPayloads) {
        const state = await queryPlayerState({ port, pkx: p.pkx });
        assert.ok(state?.player, `expected player to exist, got: ${JSON.stringify(state)}`);
        assert.strictEqual(state.player.counter ?? 0, 0);
      }

      // Conflicting: two installs for the same *new* account should result in exactly one success.
      const keyDup = "9200001";
      const dupA = sign(createCommand(1n, 1n, [0n, 0n, 0n, 0n]), keyDup);
      const dupB = sign(createCommand(2n, 1n, [0n, 0n, 0n, 0n]), keyDup);
      const dupResponses = await Promise.all(
        [dupA, dupB].map((p) =>
          fetch(`http://127.0.0.1:${port}/send`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(p),
          }),
        ),
      );
      const dupJobIds = [];
      for (const r of dupResponses) {
        assert.strictEqual(r.status, 201);
        const body = await r.json();
        assert.strictEqual(body?.success, true);
        dupJobIds.push(String(body.jobid));
      }
      const dupStatuses = await Promise.all(
        dupJobIds.map((jobId) => pollJobFinished({ port, jobId, timeoutMs: 60000 })),
      );
      const dupOk = dupStatuses.filter((s) => !s.job.failedReason);
      const dupFail = dupStatuses.filter((s) => s.job.failedReason);
      assert.strictEqual(dupOk.length, 1);
      assert.strictEqual(dupFail.length, 1);
      assert.ok(String(dupFail[0].job.failedReason ?? "").includes("PlayerAlreadyExist"));

      const state = await queryPlayerState({ port, pkx: dupA.pkx });
      assert.ok(state?.player);
      assert.strictEqual(state.player.counter ?? 0, 0);
    } catch (err) {
      throw new Error(`${err instanceof Error ? err.message : String(err)}\n${(svc.logs ?? []).join("")}`);
    } finally {
      await killProc(svc.proc);
    }
  },
);
