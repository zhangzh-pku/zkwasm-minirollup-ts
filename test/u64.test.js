import assert from "node:assert";
import { test } from "node:test";

import { base64ToU64ArrayLE, u64ArrayToBase64LE } from "../src/u64.js";

test("u64 base64 round-trip preserves values", () => {
  const words = new BigUint64Array([1n, 2n, 3n]);
  const encoded = u64ArrayToBase64LE(words);
  const decoded = base64ToU64ArrayLE(encoded);
  assert.deepStrictEqual(Array.from(decoded), Array.from(words));
});

test("base64ToU64ArrayLE rejects odd byte length", () => {
  const bytes = Buffer.from([1, 2, 3]);
  const encoded = bytes.toString("base64");
  assert.throws(() => base64ToU64ArrayLE(encoded), /invalid u64 byte length/);
});
