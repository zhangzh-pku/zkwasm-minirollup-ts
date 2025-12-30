import assert from "node:assert";
import { test } from "node:test";

import { __test__ } from "../src/service.js";

const { SendEnqueueBuffer, OverloadedError } = __test__;

const sampleTx = {
  msg: "0x01",
  pkx: "0x02",
  pky: "0x03",
  sigx: "0x04",
  sigy: "0x05",
  sigr: "0x06",
};

test("SendEnqueueBuffer flushes when batch size reached", async () => {
  const queue = {
    calls: [],
    addBulk: async (bulk) => {
      queue.calls.push(bulk);
      return bulk.map((_, idx) => ({ id: idx + 1 }));
    },
  };
  const buffer = new SendEnqueueBuffer(queue, 2, 1000, 10);
  await Promise.all([buffer.enqueue(sampleTx), buffer.enqueue(sampleTx)]);
  assert.strictEqual(queue.calls.length, 1);
  assert.strictEqual(queue.calls[0].length, 2);
});

test("SendEnqueueBuffer flushes on timer", async () => {
  const queue = {
    calls: [],
    addBulk: async (bulk) => {
      queue.calls.push(bulk);
      return bulk.map((_, idx) => ({ id: idx + 1 }));
    },
  };
  const buffer = new SendEnqueueBuffer(queue, 10, 5, 10);
  await buffer.enqueue(sampleTx);
  assert.strictEqual(queue.calls.length, 1);
});

test("SendEnqueueBuffer rejects when overloaded", async () => {
  const queue = {
    addBulk: async (bulk) => bulk.map((_, idx) => ({ id: idx + 1 })),
  };
  const buffer = new SendEnqueueBuffer(queue, 10, 5, 1);
  const first = buffer.enqueue(sampleTx);
  await assert.rejects(() => buffer.enqueue(sampleTx), OverloadedError);
  await first;
});

test("SendEnqueueBuffer rejects pending on enqueue failure", async () => {
  const err = new Error("queue down");
  const queue = {
    addBulk: async () => {
      throw err;
    },
  };
  const buffer = new SendEnqueueBuffer(queue, 2, 1000, 10);
  const results = await Promise.allSettled([
    buffer.enqueue(sampleTx),
    buffer.enqueue(sampleTx),
  ]);
  assert.ok(results.every((r) => r.status === "rejected"));
});
