import assert from "node:assert";
import { fork } from "node:child_process";
import path from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const TS_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const require = createRequire(import.meta.url);

async function startRpcServer() {
  return await new Promise((resolve, reject) => {
    const child = fork(path.join(TS_ROOT, "test", "rpc-server.mjs"), {
      stdio: ["ignore", "ignore", "ignore", "ipc"],
    });

    const requests = [];
    let resolved = false;

    child.on("message", (msg) => {
      if (msg?.type === "request") {
        requests.push(msg.data);
      } else if (msg?.type === "ready") {
        resolved = true;
        resolve({ child, port: msg.port, requests });
      }
    });
    child.on("error", (err) => {
      if (!resolved) reject(err);
    });
    child.on("exit", (code, signal) => {
      if (!resolved) {
        reject(new Error(`rpc-server exited before ready: code=${code} signal=${signal}`));
      }
    });
  });
}

async function stopRpcServer(child) {
  if (!child || child.killed) return;
  child.send?.({ type: "close" });
  await new Promise((resolve) => child.once("exit", resolve));
}

test(
  "syncrpc spawns dbprocess and returns one-line JSON response",
  { timeout: 60_000 },
  async () => {
    const { child, port } = await startRpcServer();
    process.env.MERKLE_SERVER = `http://127.0.0.1:${port}`;

    try {
      const prevLog = console.log;
      console.log = () => {};
      try {
        const requestMerkleData = require("../src/bootstrap/syncrpc.cjs");

        // Browser-like env should be rejected.
        globalThis.window = {};
        assert.throws(
          () => requestMerkleData({ jsonrpc: "2.0", method: "ping", params: {}, id: 1 }),
          /Process not supported/,
        );
        delete globalThis.window;

        const out1 = requestMerkleData({
          jsonrpc: "2.0",
          method: "get_leaf",
          params: { root: Array.from(new Uint8Array(32)), index: "42" },
          id: 2,
        });
        const resp1 = JSON.parse(out1);
        assert.deepStrictEqual(resp1?.result, { leaf: "42" });

        const out2 = requestMerkleData({
          jsonrpc: "2.0",
          method: "get_record",
          params: { hash: Array.from(new Uint8Array(32).fill(7)) },
          id: 3,
        });
        const resp2 = JSON.parse(out2);
        assert.deepStrictEqual(resp2?.result, ["7", "8"]);
      } finally {
        console.log = prevLog;
      }
    } finally {
      await stopRpcServer(child);
      delete process.env.MERKLE_SERVER;
    }
  },
);
