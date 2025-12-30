import { LeHexBN } from "zkwasm-minirollup-rpc";

function strip0x(hex: string): string {
  return hex.startsWith("0x") ? hex.slice(2) : hex;
}

function normalizeHex(value: unknown, label: string): string {
  if (typeof value !== "string") {
    throw new Error(`${label} must be a hex string`);
  }
  const raw = strip0x(value);
  if (raw.length === 0) {
    throw new Error(`${label} is empty`);
  }
  if (!/^[0-9a-fA-F]+$/.test(raw)) {
    throw new Error(`Invalid ${label} hex`);
  }
  return raw;
}

function assertMaxWords(raw: string, label: string, wordCount: number) {
  const maxLen = wordCount * 16;
  if (raw.length > maxLen) {
    throw new Error(`${label} exceeds ${wordCount} words`);
  }
}

function hexLeToU64Array(raw: string, wordCount: number, label: string): BigUint64Array {
  assertMaxWords(raw, label, wordCount);
  if (raw.length % 2 !== 0) {
    raw = `0${raw}`;
  }

  const out = new BigUint64Array(wordCount);
  for (let i = 0; i < wordCount; i++) {
    const chunk = raw.slice(i * 16, i * 16 + 16);
    if (!chunk) break;
    const buf = Buffer.from(chunk.padEnd(16, "0"), "hex");
    out[i] = buf.readBigUint64LE(0);
  }
  return out;
}

export function signatureToU64ArrayCompat(value: any): BigUint64Array {
  const msgHex = normalizeHex(value.msg, "msg");
  const msgWordCount = Math.ceil(msgHex.length / 16);
  const pkxHex = normalizeHex(value.pkx, "pkx");
  const pkyHex = normalizeHex(value.pky, "pky");
  const sigxHex = normalizeHex(value.sigx, "sigx");
  const sigyHex = normalizeHex(value.sigy, "sigy");
  const sigrHex = normalizeHex(value.sigr, "sigr");

  assertMaxWords(pkxHex, "pkx", 4);
  assertMaxWords(pkyHex, "pky", 4);
  assertMaxWords(sigxHex, "sigx", 4);
  assertMaxWords(sigyHex, "sigy", 4);
  assertMaxWords(sigrHex, "sigr", 4);

  const msg = new LeHexBN(`0x${msgHex}`).toU64Array(msgWordCount);
  const pkx = new LeHexBN(`0x${pkxHex}`).toU64Array();
  const pky = new LeHexBN(`0x${pkyHex}`).toU64Array();
  const sigx = new LeHexBN(`0x${sigxHex}`).toU64Array();
  const sigy = new LeHexBN(`0x${sigyHex}`).toU64Array();
  const sigr = new LeHexBN(`0x${sigrHex}`).toU64Array();

  const u64array = new BigUint64Array(20 + msgWordCount);
  u64array.set(pkx, 0);
  u64array.set(pky, 4);
  u64array.set(sigx, 8);
  u64array.set(sigy, 12);
  u64array.set(sigr, 16);
  u64array.set(msg, 20);
  const cmdLength = (msg[0] >> 8n) & 0xffn;
  if (Number(cmdLength) !== msg.length) {
    throw Error("Wrong Command Size");
  }
  return u64array;
}

export function signatureToU64ArrayFast(value: any): BigUint64Array {
  const msgHex = normalizeHex(value.msg, "msg");
  const msgWordCount = Math.ceil(msgHex.length / 16);
  const pkxHex = normalizeHex(value.pkx, "pkx");
  const pkyHex = normalizeHex(value.pky, "pky");
  const sigxHex = normalizeHex(value.sigx, "sigx");
  const sigyHex = normalizeHex(value.sigy, "sigy");
  const sigrHex = normalizeHex(value.sigr, "sigr");

  const msg = hexLeToU64Array(msgHex, msgWordCount, "msg");
  const pkx = hexLeToU64Array(pkxHex, 4, "pkx");
  const pky = hexLeToU64Array(pkyHex, 4, "pky");
  const sigx = hexLeToU64Array(sigxHex, 4, "sigx");
  const sigy = hexLeToU64Array(sigyHex, 4, "sigy");
  const sigr = hexLeToU64Array(sigrHex, 4, "sigr");

  const u64array = new BigUint64Array(20 + msgWordCount);
  u64array.set(pkx, 0);
  u64array.set(pky, 4);
  u64array.set(sigx, 8);
  u64array.set(sigy, 12);
  u64array.set(sigr, 16);
  u64array.set(msg, 20);
  const cmdLength = (msg[0] >> 8n) & 0xffn;
  if (Number(cmdLength) !== msg.length) {
    throw Error("Wrong Command Size");
  }
  return u64array;
}

export function signature_to_u64array(value: any): BigUint64Array {
  if (process.env.SIG_U64_PARSER === "compat") {
    return signatureToU64ArrayCompat(value);
  }
  return signatureToU64ArrayFast(value);
}
