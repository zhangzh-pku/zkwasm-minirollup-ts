import assert from "node:assert";
import http from "node:http";
import { spawn } from "node:child_process";
import path from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

const TS_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

async function startServer() {
  return await new Promise((resolve, reject) => {
    const server = http.createServer((req, res) => {
      let body = "";
      req.on("data", (chunk) => {
        body += chunk.toString("utf8");
      });
      req.on("end", () => {
        let parsed;
        try {
          parsed = JSON.parse(body);
        } catch {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "bad json" }));
          return;
        }
        let result;
        switch (parsed?.method) {
          case "ping":
            result = true;
            break;
          default:
            result = { ok: true };
        }
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ jsonrpc: "2.0", id: parsed?.id, result }));
      });
    });
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      if (!addr || typeof addr !== "object") {
        server.close(() => reject(new Error("failed to bind server")));
        return;
      }
      resolve({ server, port: addr.port });
    });
  });
}

async function waitForStdout(child, token, timeoutMs) {
  return await new Promise((resolve, reject) => {
    const deadline = setTimeout(() => reject(new Error("timeout waiting child ready")), timeoutMs);
    let buf = "";
    child.stdout.on("data", (chunk) => {
      buf += chunk.toString("utf8");
      if (buf.includes(token)) {
        clearTimeout(deadline);
        resolve();
      }
    });
    child.on("error", (err) => {
      clearTimeout(deadline);
      reject(err);
    });
    child.on("exit", (code, signal) => {
      clearTimeout(deadline);
      reject(new Error(`child exited before ready: code=${code} signal=${signal}`));
    });
  });
}

async function runSignalCase(signal, expectedCode) {
  const { server, port } = await startServer();
  try {
    const script = `
      const path = require('node:path');
      process.env.MERKLE_SERVER = 'http://127.0.0.1:${port}';
      const requestMerkleData = require(path.join(process.cwd(), 'src', 'bootstrap', 'syncrpc.cjs'));
      requestMerkleData({ jsonrpc: '2.0', method: 'ping', params: {}, id: 1 });
      process.stdout.write('READY\\n');
      setInterval(() => {}, 1000);
    `;
    const child = spawn(process.execPath, ["-e", script], {
      cwd: TS_ROOT,
      stdio: ["ignore", "pipe", "pipe"],
      env: { ...process.env },
    });
    await waitForStdout(child, "READY", 30_000);

    child.kill(signal);
    const { code, signal: gotSignal } = await new Promise((resolve) => {
      child.once("exit", (code, signal) => resolve({ code, signal }));
    });
    assert.strictEqual(gotSignal, null);
    assert.strictEqual(code, expectedCode);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

test("syncrpc handles SIGTERM and exits 143", { timeout: 60_000 }, async () => {
  await runSignalCase("SIGTERM", 143);
});

test("syncrpc handles SIGINT and exits 130", { timeout: 60_000 }, async () => {
  await runSignalCase("SIGINT", 130);
});

