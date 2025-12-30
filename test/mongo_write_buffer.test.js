import assert from "node:assert";
import { test } from "node:test";

import { MongoWriteBuffer } from "../src/mongo_write_buffer.js";

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

test("MongoWriteBuffer flushes tx batch on size", async () => {
  const txCalls = [];
  const buffer = new MongoWriteBuffer({
    txCollection: {
      insertMany: async (docs) => {
        txCalls.push(docs);
      },
    },
    jobCollection: {
      insertMany: async () => {},
    },
    batchSize: 2,
    flushMs: 1000,
    fatalTxError: false,
    fatalJobError: false,
  });

  buffer.enqueueTx({ id: 1 });
  buffer.enqueueTx({ id: 2 });
  await buffer.flush();

  assert.strictEqual(txCalls.length, 1);
  assert.deepStrictEqual(txCalls[0], [{ id: 1 }, { id: 2 }]);
});

test("MongoWriteBuffer flushes on timer", async () => {
  const txCalls = [];
  const buffer = new MongoWriteBuffer({
    txCollection: {
      insertMany: async (docs) => {
        txCalls.push(docs);
      },
    },
    jobCollection: {
      insertMany: async () => {},
    },
    batchSize: 10,
    flushMs: 5,
    fatalTxError: false,
    fatalJobError: false,
  });

  buffer.enqueueTx({ id: 1 });
  await delay(20);

  assert.strictEqual(txCalls.length, 1);
  assert.deepStrictEqual(txCalls[0], [{ id: 1 }]);
});

test("MongoWriteBuffer ignores duplicate key job errors", async () => {
  let calls = 0;
  const buffer = new MongoWriteBuffer({
    txCollection: {
      insertMany: async () => {},
    },
    jobCollection: {
      insertMany: async () => {
        calls += 1;
        const err = new Error("dup");
        err.writeErrors = [{ code: 11000 }];
        throw err;
      },
    },
    batchSize: 1,
    flushMs: 1000,
    fatalTxError: false,
    fatalJobError: false,
  });

  buffer.enqueueJob({ id: 1 });
  await buffer.flush();

  assert.strictEqual(calls, 1);
});

test("MongoWriteBuffer triggers process exit on fatal tx error", async () => {
  const buffer = new MongoWriteBuffer({
    txCollection: {
      insertMany: async () => {
        throw new Error("boom");
      },
    },
    jobCollection: {
      insertMany: async () => {},
    },
    batchSize: 1,
    flushMs: 1000,
    fatalTxError: true,
    fatalJobError: false,
  });

  const prevExit = process.exit;
  let exitCode;
  process.exit = (code) => {
    exitCode = code;
    throw new Error("exit");
  };

  try {
    buffer.enqueueTx({ id: 1 });
    await buffer.flush();
  } finally {
    process.exit = prevExit;
  }

  assert.strictEqual(exitCode, 1);
});
