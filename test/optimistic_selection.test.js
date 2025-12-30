import assert from "node:assert";
import { test } from "node:test";

import {
  buildRwSets,
  intersects,
  shouldEnterSerialFallback,
} from "../src/optimistic/selection.js";

test("buildRwSets tracks leaf and record reads/writes", () => {
  const trace = {
    reads: ["1", "1"],
    writes: [
      { index: "2", data: [1] },
      { index: "3", data: [2] },
    ],
    getRecords: [{ hash: [7, 8] }, { hash: [7, 8] }],
    updateRecords: [{ hash: [9] }, { hash: [10] }],
  };

  const { reads, writes } = buildRwSets(trace);
  assert.ok(reads.has("leaf:1"));
  assert.ok(reads.has("record:7,8"));
  assert.ok(writes.has("leaf:2"));
  assert.ok(writes.has("leaf:3"));
  assert.ok(writes.has("record:9"));
  assert.ok(writes.has("record:10"));
});

test("intersects detects overlaps between sets", () => {
  const a = new Set(["a", "b"]);
  const b = new Set(["c", "b"]);
  const c = new Set(["d"]);
  assert.strictEqual(intersects(a, b), true);
  assert.strictEqual(intersects(a, c), false);
});

test("shouldEnterSerialFallback respects threshold and deferred count", () => {
  assert.strictEqual(shouldEnterSerialFallback(1, 4, 0.5), true);
  assert.strictEqual(shouldEnterSerialFallback(4, 1, 0.5), false);
  assert.strictEqual(shouldEnterSerialFallback(1, 0, 0.1), false);
});
