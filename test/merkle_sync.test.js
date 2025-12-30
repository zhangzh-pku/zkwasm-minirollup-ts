import assert from "node:assert";
import { test } from "node:test";

import { Service } from "../src/service.js";
import { merkleRootToBeHexString } from "../src/lib.js";

class TestService extends Service {
  constructor(bundles) {
    super();
    this._bundles = bundles;
  }

  async findBundleByMerkle(merkleHexRoot) {
    return this._bundles.get(merkleHexRoot) ?? null;
  }
}

test("syncToLatestMerkelRoot stops on chain break", async () => {
  const rootA = new BigUint64Array([1n, 2n, 3n, 4n]);
  const rootB = new BigUint64Array([5n, 6n, 7n, 8n]);
  const rootAHex = merkleRootToBeHexString(rootA);
  const rootBHex = merkleRootToBeHexString(rootB);

  const bundles = new Map([
    [
      rootAHex,
      {
        merkleRoot: rootAHex,
        preMerkleRoot: "",
        postMerkleRoot: rootBHex,
        bundleIndex: 1,
      },
    ],
  ]);

  const service = new TestService(bundles);
  service.merkleRoot = rootA;

  await service.syncToLatestMerkelRoot();

  assert.deepStrictEqual(Array.from(service.merkleRoot), Array.from(rootA));
  assert.strictEqual(service.preMerkleRoot, null);
});
