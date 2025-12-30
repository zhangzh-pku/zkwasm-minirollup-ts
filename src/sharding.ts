import { createHash } from "node:crypto";

export function shardForPkx(pkx: string, shardCount: number): number {
  if (shardCount <= 1) return 0;
  const hex = pkx.startsWith("0x") ? pkx.slice(2) : pkx;
  if (!/^[0-9a-fA-F]*$/.test(hex)) return 0;
  const buf = Buffer.from(hex.length % 2 === 0 ? hex : `0${hex}`, "hex");
  const digest = createHash("sha256").update(buf).digest();
  return digest.readUInt32LE(0) % shardCount;
}

