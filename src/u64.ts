export function u64ArrayToBase64LE(words: BigUint64Array): string {
  const bytes = Buffer.alloc(words.length * 8);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  for (let i = 0; i < words.length; i++) {
    view.setBigUint64(i * 8, words[i]!, true);
  }
  return bytes.toString("base64");
}

export function base64ToU64ArrayLE(base64: string): BigUint64Array {
  const bytes = Buffer.from(base64, "base64");
  if (bytes.byteLength % 8 !== 0) {
    throw new Error(`invalid u64 byte length: ${bytes.byteLength}`);
  }
  const out = new BigUint64Array(bytes.byteLength / 8);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  for (let i = 0; i < out.length; i++) {
    out[i] = view.getBigUint64(i * 8, true);
  }
  return out;
}

