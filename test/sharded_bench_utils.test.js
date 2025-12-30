import assert from "node:assert";
import { test } from "node:test";

import { merkleDbUriForShard, resolveMerkleDbUriBase } from "../src/sharded_bench_utils.js";

test("resolveMerkleDbUriBase: resolves in priority order", () => {
  assert.strictEqual(resolveMerkleDbUriBase({ MERKLE_DB_URI: "/a", MERKLE_DB_PATH: "/b", MERKLE_DB: "/c" }), "/a");
  assert.strictEqual(resolveMerkleDbUriBase({ MERKLE_DB_PATH: "/b", MERKLE_DB: "/c" }), "/b");
  assert.strictEqual(resolveMerkleDbUriBase({ MERKLE_DB: "/c" }), "/c");
  assert.strictEqual(resolveMerkleDbUriBase({}), "");
});

test("merkleDbUriForShard: replaces {image} and appends shard suffix by default", () => {
  const uri = merkleDbUriForShard({
    base: "/dev/shm/merkledb_{image}",
    image: "deadbeef",
    shardId: 3,
    shardCount: 4,
  });
  assert.strictEqual(uri, "/dev/shm/merkledb_deadbeef_shard3");
});

test("merkleDbUriForShard: respects explicit shard placeholders", () => {
  const uri = merkleDbUriForShard({
    base: "/tmp/merkledb_{image}_{shardId}",
    image: "deadbeef",
    shardId: 3,
    shardCount: 4,
  });
  assert.strictEqual(uri, "/tmp/merkledb_deadbeef_3");
});

test("merkleDbUriForShard: shardCount=1 does not append shard suffix", () => {
  const uri = merkleDbUriForShard({
    base: "/tmp/merkledb_{image}",
    image: "deadbeef",
    shardId: 0,
    shardCount: 1,
  });
  assert.strictEqual(uri, "/tmp/merkledb_deadbeef");
});

