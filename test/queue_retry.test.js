import assert from "node:assert";
import { test } from "node:test";

import { Queue } from "bullmq";
import { __test__ } from "../src/service.js";

const { createQueueWithRetry, setQueueFactory } = __test__;

function restoreQueueFactory() {
  setQueueFactory((name, options) => new Queue(name, options));
}

test("createQueueWithRetry retries and succeeds", async () => {
  let attempts = 0;
  setQueueFactory(() => {
    attempts += 1;
    if (attempts < 2) {
      throw new Error("connect fail");
    }
    return {
      waitUntilReady: async () => {},
    };
  });

  try {
    const queue = await createQueueWithRetry("q", {} , 1, 0);
    assert.ok(queue);
    assert.strictEqual(attempts, 2);
  } finally {
    restoreQueueFactory();
  }
});

test("createQueueWithRetry propagates error after retries", async () => {
  let attempts = 0;
  setQueueFactory(() => {
    attempts += 1;
    throw new Error("connect fail");
  });

  try {
    await assert.rejects(() => createQueueWithRetry("q", {}, 1, 0), /connect fail/);
    assert.strictEqual(attempts, 2);
  } finally {
    restoreQueueFactory();
  }
});
