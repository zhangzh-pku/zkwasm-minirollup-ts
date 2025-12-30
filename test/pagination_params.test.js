import assert from "node:assert";
import { test } from "node:test";

import { __test__ } from "../src/service.js";

const { parseStartParam } = __test__;

test("parseStartParam handles zero and large offsets", () => {
  assert.strictEqual(parseStartParam("0"), 0);
  assert.strictEqual(parseStartParam("1000000"), 1000000);
});

test("parseStartParam rejects invalid or negative values", () => {
  assert.strictEqual(parseStartParam("not-a-number"), null);
  assert.strictEqual(parseStartParam("-1"), null);
});
