import assert from "node:assert";
import { test } from "node:test";

import { mongooseDbNameForShard, queueNameForShard } from "../src/config.js";

test("queueNameForShard: no sharding keeps prefix", () => {
  assert.strictEqual(queueNameForShard("sequencer", 1, 0), "sequencer");
  assert.strictEqual(queueNameForShard("custom", 1, 999), "custom");
});

test("queueNameForShard: sharded queue gets -<id> suffix", () => {
  assert.strictEqual(queueNameForShard("sequencer", 4, 0), "sequencer-0");
  assert.strictEqual(queueNameForShard("sequencer", 4, 2), "sequencer-2");
});

test("queueNameForShard: invalid SHARD_ID throws", () => {
  assert.throws(() => queueNameForShard("sequencer", 4, 4), /invalid SHARD_ID=4/);
  assert.throws(() => queueNameForShard("sequencer", 4, 999), /invalid SHARD_ID=999/);
});

test("mongooseDbNameForShard: no sharding keeps db name", () => {
  assert.strictEqual(
    mongooseDbNameForShard("mongodb://localhost:27017", "img", 1, 0),
    "mongodb://localhost:27017/img_job-tracker",
  );
});

test("mongooseDbNameForShard: sharded db gets _shard<id> suffix", () => {
  assert.strictEqual(
    mongooseDbNameForShard("mongodb://localhost:27017", "img", 4, 2),
    "mongodb://localhost:27017/img_job-tracker_shard2",
  );
});

