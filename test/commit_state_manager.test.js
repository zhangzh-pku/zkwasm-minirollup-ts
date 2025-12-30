import assert from "node:assert";
import { test } from "node:test";

function resetEnv(prev) {
  for (const key of Object.keys(process.env)) {
    if (!(key in prev)) {
      delete process.env[key];
    }
  }
  for (const [key, value] of Object.entries(prev)) {
    process.env[key] = value;
  }
}

async function loadCommitModule() {
  const suffix = `${Date.now()}-${Math.random()}`;
  return import(`../src/commit.js?test=${suffix}`);
}

test("TxStateManager flushes writes added during inflight batch", async () => {
  const prev = { ...process.env };
  process.env.BATCH_COMMIT_WRITES = "1";
  process.env.ASYNC_COMMIT_WRITES = "0";
  process.env.COMMIT_BATCH_SIZE = "2";
  process.env.COMMIT_FLUSH_MS = "1000";
  process.env.COMMIT_BATCH_FATAL = "0";

  try {
    const mod = await loadCommitModule();
    const { TxStateManager, CommitModel } = mod;

    const calls = [];
    let release;
    const gate = new Promise((resolve) => {
      release = resolve;
    });
    let callCount = 0;
    CommitModel.findOneAndUpdate = async (_filter, update) => {
      calls.push(update);
      callCount += 1;
      if (callCount === 1) {
        await gate;
      }
      return {};
    };

    const manager = new TxStateManager("root");
    const tx = () => ({
      msg: "0x01",
      pkx: "0x02",
      pky: "0x03",
      sigx: "0x04",
      sigy: "0x05",
      sigr: "0x06",
    });

    const p1 = manager.insertTxIntoCommit(tx());
    const p2 = manager.insertTxIntoCommit(tx());
    await new Promise((resolve) => setTimeout(resolve, 0));
    const p3 = manager.insertTxIntoCommit(tx());
    release();

    await Promise.all([p1, p2, p3]);
    await manager.flushPending("final");

    assert.ok(calls.length >= 2);
    assert.strictEqual(calls[0].$push.items.$each.length, 2);
    assert.strictEqual(calls[1].$push.items.$each.length, 1);
  } finally {
    resetEnv(prev);
  }
});
