import assert from "node:assert";
import { test } from "node:test";

import { shardForPkx } from "../src/sharding.js";

test("shardForPkx: shardCount<=1 always maps to shard 0", () => {
  assert.strictEqual(shardForPkx("0x01", 1), 0);
  assert.strictEqual(shardForPkx("01", 1), 0);
  assert.strictEqual(shardForPkx("not-hex", 1), 0);
});

test("shardForPkx: invalid hex maps to shard 0", () => {
  assert.strictEqual(shardForPkx("0xzz", 4), 0);
  assert.strictEqual(shardForPkx("zz", 4), 0);
});

test("shardForPkx: stable mapping and supports 0x prefix / odd length", () => {
  assert.strictEqual(shardForPkx("0x01", 4), 3);
  assert.strictEqual(shardForPkx("01", 4), 3);

  assert.strictEqual(shardForPkx("0x1234", 4), 2);
  assert.strictEqual(shardForPkx("1234", 4), 2);

  assert.strictEqual(shardForPkx("0xabc", 4), 0);
  assert.strictEqual(shardForPkx("abc", 4), 0);
});

test("shardForPkx: result is always in range", () => {
  for (const shardCount of [2, 3, 4, 6, 8, 16]) {
    const shard = shardForPkx("0x01", shardCount);
    assert.ok(Number.isInteger(shard));
    assert.ok(shard >= 0 && shard < shardCount);
  }
});

