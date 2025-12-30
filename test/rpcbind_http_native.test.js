import assert from "node:assert";
import { fork } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

const TS_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function u64ToLeBytes(value) {
  const out = [];
  let v = BigInt(value);
  for (let i = 0; i < 8; i++) {
    out.push(Number(v & 0xffn));
    v >>= 8n;
  }
  return out;
}

function defaultMerkleRootBytes() {
  // Keep in sync with `new BigUint64Array([...])` in `src/service.ts`.
  const rootU64 = [
    14789582351289948625n,
    10919489180071018470n,
    10309858136294505219n,
    2839580074036780766n,
  ];
  const bytes = [];
  for (const limb of rootU64) bytes.push(...u64ToLeBytes(limb));
  assert.strictEqual(bytes.length, 32);
  return new Uint8Array(bytes);
}

async function startRpcServerProcess(env = {}) {
  return await new Promise((resolve, reject) => {
    const child = fork(path.join(TS_ROOT, "test", "rpc-server.mjs"), {
      stdio: ["ignore", "ignore", "ignore", "ipc"],
      env: { ...process.env, ...env },
    });

    let settled = false;
    child.on("message", (msg) => {
      if (msg?.type === "ready") {
        settled = true;
        resolve({ child, port: msg.port });
      }
    });
    child.on("error", (err) => {
      if (!settled) reject(err);
    });
    child.on("exit", (code, signal) => {
      if (!settled) reject(new Error(`rpc-server exited before ready: code=${code} signal=${signal}`));
    });
  });
}

async function stopRpcServerProcess(child) {
  if (!child || child.killed) return;
  child.send?.({ type: "close" });
  await new Promise((resolve) => child.once("exit", resolve));
}

function restoreEnv(snapshot) {
  for (const key of Object.keys(snapshot)) {
    const v = snapshot[key];
    if (v === undefined) delete process.env[key];
    else process.env[key] = v;
  }
}

test(
  "rpcbind executes MERKLE_RPC_MODE=http (sync + async) and MERKLE_RPC_MODE=native",
  { timeout: 120_000 },
  async () => {
    const envSnapshot = {
      MERKLE_RPC_MODE: process.env.MERKLE_RPC_MODE,
      MERKLE_SERVER: process.env.MERKLE_SERVER,
      MERKLE_DB_URI: process.env.MERKLE_DB_URI,
    };

    const prevLog = console.log;
    const prevError = console.error;
    console.log = () => {};
    console.error = () => {};

    try {
      // --- http mode: success paths (sync-fetch + async fetch) ---
      const defaultRoot = defaultMerkleRootBytes();
      const leafIndex1 = 2n ** 32n - 1n;
      const leafIndex2 = 2n ** 32n;
      const data9 = new Uint8Array(32).fill(9);
      const hash = new Uint8Array(32).fill(7);

      const httpDb = fs.mkdtempSync(path.join(os.tmpdir(), "zkwasm-merkle-http-backend-"));
      const okServer = await startRpcServerProcess({
        RPC_SERVER_BACKEND: "native",
        RPC_SERVER_MERKLE_DB: httpDb,
      });

      process.env.MERKLE_RPC_MODE = "http";
      process.env.MERKLE_SERVER = `http://127.0.0.1:${okServer.port}`;
      const rpcHttp = await import(`../src/bootstrap/rpcbind.js?mode=http&ts=${Date.now()}`);

      assert.strictEqual(rpcHttp.ping(), true);

      assert.deepStrictEqual(rpcHttp.get_leaf(defaultRoot, leafIndex1), new Array(32).fill(0));
      const rootAfterUpdateHttp = rpcHttp.update_leaf(defaultRoot, leafIndex1, data9);
      assert.ok(Array.isArray(rootAfterUpdateHttp) && rootAfterUpdateHttp.length === 32);
      assert.deepStrictEqual(rpcHttp.get_leaf(rootAfterUpdateHttp, leafIndex1), Array.from(data9));

      assert.strictEqual(rpcHttp.update_record(hash, [11n, 22n]), null);
      assert.deepStrictEqual(rpcHttp.get_record(hash), [11n, 22n]);

      const txs = [
        {
          writes: [
            { index: leafIndex1.toString(), data: new Uint8Array(32).fill(1) },
            { index: leafIndex2.toString(), data: new Uint8Array(32).fill(2) },
          ],
          updateRecords: [{ hash, data: ["33", "44"] }],
        },
      ];
      const rootsHttp = rpcHttp.apply_txs(defaultRoot, txs);
      assert.ok(Array.isArray(rootsHttp) && rootsHttp.length === 1);
      assert.ok(Array.isArray(rootsHttp[0]) && rootsHttp[0].length === 32);
      const finalRootHttp = rpcHttp.apply_txs_final(defaultRoot, txs);
      assert.ok(Array.isArray(finalRootHttp) && finalRootHttp.length === 32);
      assert.deepStrictEqual(rootsHttp[0], finalRootHttp);
      assert.deepStrictEqual(await rpcHttp.apply_txs_async(defaultRoot, txs), rootsHttp);
      assert.deepStrictEqual(await rpcHttp.apply_txs_final_async(defaultRoot, txs), finalRootHttp);
      assert.deepStrictEqual(rpcHttp.get_leaf(finalRootHttp, leafIndex1), new Array(32).fill(1));
      assert.deepStrictEqual(rpcHttp.get_leaf(finalRootHttp, leafIndex2), new Array(32).fill(2));
      assert.deepStrictEqual(rpcHttp.get_record(hash), [33n, 44n]);

      const sessionHttp = rpcHttp.begin_session();
      assert.ok(typeof sessionHttp === "string" && sessionHttp.length > 0);
      globalThis.__MERKLE_SESSION = sessionHttp;
      rpcHttp.update_leaf(defaultRoot, leafIndex2, new Uint8Array(32).fill(3));
      const commitHttp = rpcHttp.commit_session(sessionHttp);
      assert.ok(commitHttp?.merkle_records > 0);
      assert.ok(commitHttp?.data_records >= 0);
      assert.strictEqual(rpcHttp.reset_session(sessionHttp), true);
      assert.strictEqual(rpcHttp.drop_session(sessionHttp), true);
      delete globalThis.__MERKLE_SESSION;

      await stopRpcServerProcess(okServer.child);

      // --- http mode: error paths (non-2xx + JSON-RPC error) ---
      const force500 = await startRpcServerProcess({ RPC_SERVER_FORCE_STATUS: "500" });

      process.env.MERKLE_RPC_MODE = "http";
      process.env.MERKLE_SERVER = `http://127.0.0.1:${force500.port}`;
      const rpcHttp500 = await import(`../src/bootstrap/rpcbind.js?mode=http-500&ts=${Date.now()}`);
      assert.throws(() => rpcHttp500.ping(), /merkle rpc failed: 500/);
      await assert.rejects(
        () => rpcHttp500.apply_txs_async(defaultRoot, [{ writes: [], updateRecords: [] }]),
        /merkle rpc failed: 500/,
      );
      await stopRpcServerProcess(force500.child);

      const jsonrpcError = await startRpcServerProcess({
        RPC_SERVER_ERROR_ON_METHOD: "get_leaf",
        RPC_SERVER_ERROR_ON_INDEX: "bad",
      });
      process.env.MERKLE_RPC_MODE = "http";
      process.env.MERKLE_SERVER = `http://127.0.0.1:${jsonrpcError.port}`;
      const rpcHttpErr = await import(`../src/bootstrap/rpcbind.js?mode=http-jsonrpc-err&ts=${Date.now()}`);
      assert.throws(
        () => rpcHttpErr.get_leaf(defaultRoot, { toString: () => "bad" }),
        (e) => String(e).includes("Failed to get leaf"),
      );
      await stopRpcServerProcess(jsonrpcError.child);

      // --- native mode: missing MERKLE_DB_URI should throw on first call ---
      delete process.env.MERKLE_DB_URI;
      process.env.MERKLE_RPC_MODE = "native";
      delete process.env.MERKLE_SERVER;
      const rpcNativeNoDb = await import(`../src/bootstrap/rpcbind.js?mode=native-nodb&ts=${Date.now()}`);
      assert.throws(() => rpcNativeNoDb.ping(), /MERKLE_DB_URI is required/);

      // --- native mode: end-to-end (open DB, leaf + record + session) ---
      const tmpDb = fs.mkdtempSync(path.join(os.tmpdir(), "zkwasm-merkle-native-test-"));
      process.env.MERKLE_DB_URI = tmpDb;
      process.env.MERKLE_RPC_MODE = "native";

      const rpcNative = await import(`../src/bootstrap/rpcbind.js?mode=native&ts=${Date.now()}`);
      assert.strictEqual(rpcNative.ping(), true);

      const session = rpcNative.begin_session();
      assert.ok(typeof session === "string" && session.length > 0);
      globalThis.__MERKLE_SESSION = session;

      assert.throws(() => rpcNative.update_leaf(new Uint8Array(31), leafIndex1, defaultRoot), /expected 32 bytes/);

      const nextRoot = rpcNative.update_leaf(defaultRoot, leafIndex1, data9);
      assert.ok(nextRoot instanceof Uint8Array || Buffer.isBuffer(nextRoot));
      assert.strictEqual(nextRoot.length, 32);

      const leaf = rpcNative.get_leaf(nextRoot, leafIndex1);
      assert.ok(leaf instanceof Uint8Array || Buffer.isBuffer(leaf));
      assert.strictEqual(leaf.length, 32);
      assert.strictEqual(Number(leaf[0]), 9);

      rpcNative.update_record(hash, [11n, 22n]);
      assert.deepStrictEqual(rpcNative.get_record(hash), [11n, 22n]);

      const commit1 = rpcNative.commit_session(session);
      assert.ok(commit1?.merkle_records > 0);
      assert.ok(commit1?.data_records > 0);

      assert.strictEqual(rpcNative.reset_session(session), true);
      assert.strictEqual(rpcNative.drop_session(session), true);
      delete globalThis.__MERKLE_SESSION;

      // Smoke: native apply_txs(_final) normalizes txs and returns roots.
      const roots = rpcNative.apply_txs(defaultRoot, txs);
      assert.ok(Array.isArray(roots));
      assert.strictEqual(roots.length, 1);
      assert.ok(Array.isArray(roots[0]) && roots[0].length === 32);
      assert.deepStrictEqual(roots, rootsHttp);

      const finalRoot = rpcNative.apply_txs_final(defaultRoot, txs);
      assert.ok(Array.isArray(finalRoot) && finalRoot.length === 32);
      assert.deepStrictEqual(finalRoot, finalRootHttp);
      assert.deepStrictEqual(Array.from(rpcNative.get_leaf(finalRoot, leafIndex1)), new Array(32).fill(1));
      assert.deepStrictEqual(Array.from(rpcNative.get_leaf(finalRoot, leafIndex2)), new Array(32).fill(2));
      assert.deepStrictEqual(rpcNative.get_record(hash), [33n, 44n]);

      restoreEnv(envSnapshot);
    } finally {
      console.log = prevLog;
      console.error = prevError;
      restoreEnv(envSnapshot);
    }
  },
);
