import assert from "node:assert";
import { test } from "node:test";
import { createCommand, sign } from "zkwasm-minirollup-rpc";
import {
  signatureToU64ArrayCompat,
  signatureToU64ArrayFast,
  signature_to_u64array,
} from "../src/signature.js";

test("signatureToU64ArrayFast matches compat parser", () => {
  const cmd = createCommand(1n, 0n, [0n, 0n, 0n, 0n]);
  const payload = sign(cmd, "1234567");

  const compat = signatureToU64ArrayCompat(payload);
  const fast = signatureToU64ArrayFast(payload);

  assert.deepStrictEqual(Array.from(fast), Array.from(compat));
});

test("signature_to_u64array selects compat parser by env", () => {
  const cmd = createCommand(1n, 0n, [0n, 0n, 0n, 0n]);
  const payload = sign(cmd, "1234567");

  const prev = process.env.SIG_U64_PARSER;
  process.env.SIG_U64_PARSER = "compat";
  try {
    const got = signature_to_u64array(payload);
    const compat = signatureToU64ArrayCompat(payload);
    assert.deepStrictEqual(Array.from(got), Array.from(compat));
  } finally {
    if (prev === undefined) delete process.env.SIG_U64_PARSER;
    else process.env.SIG_U64_PARSER = prev;
  }
});

test("signature_to_u64array rejects wrong command size", () => {
  assert.throws(
    () =>
      signature_to_u64array({
        msg: "0x0100",
        pkx: "0x00",
        pky: "0x00",
        sigx: "0x00",
        sigy: "0x00",
        sigr: "0x00",
      }),
    /Wrong Command Size/,
  );
});
