import assert from "node:assert";
import { test } from "node:test";

import { __test__ } from "../src/service.js";

const { PreexecPool, CommitPool } = __test__;

const workerUrl = new URL("./fixtures/worker_error.js", import.meta.url);

test("PreexecPool rejects inflight jobs on worker error", async () => {
  const pool = new PreexecPool(workerUrl, 1);
  await assert.rejects(pool.exec({ id: 1, root: [] }), /worker boom/);
  await pool.close();
});

test("CommitPool rejects inflight jobs on worker error", async () => {
  const pool = new CommitPool(workerUrl);
  await assert.rejects(pool.exec("final", [], [], null), /worker boom/);
  await pool.close();
});
