import { parentPort, workerData } from "node:worker_threads";
import { createCommand, sign } from "zkwasm-minirollup-rpc";

function keyFor({ i, keyCount, adminKey, keyBase }) {
  if (keyCount <= 1) return adminKey;
  if (keyBase === null) {
    throw new Error("KEY_BASE must be a valid integer when KEY_COUNT > 1");
  }
  return (keyBase + BigInt(i % keyCount)).toString();
}

const {
  start,
  end,
  nonceBase,
  command,
  adminKey,
  keyCount,
  keyBase,
} = workerData;

const commandBig = BigInt(command);
const nonceBaseBig = BigInt(nonceBase);
const keyBaseBig = keyBase === null ? null : BigInt(keyBase);

const payloads = new Array(end - start);
for (let i = start; i < end; i++) {
  const cmd = createCommand(nonceBaseBig + BigInt(i), commandBig, [0n, 0n, 0n, 0n]);
  payloads[i - start] = sign(cmd, keyFor({ i, keyCount, adminKey, keyBase: keyBaseBig }));
}

parentPort.postMessage({ start, payloads });
