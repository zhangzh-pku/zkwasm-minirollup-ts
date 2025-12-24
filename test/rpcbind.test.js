import assert from 'node:assert';
import { fork } from 'node:child_process';
import { once } from 'node:events';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { test, before, after } from 'node:test';

let serverProcess;
let rpc;
const requests = [];
const pendingRequests = [];

function waitForRequest() {
  return new Promise((resolve) => {
    pendingRequests.push(resolve);
  });
}

before(async () => {
  const dirname = path.dirname(fileURLToPath(import.meta.url));
  serverProcess = fork(path.join(dirname, 'rpc-server.mjs'), [], {
    stdio: ['ignore', 'ignore', 'ignore', 'ipc'],
    execArgv: [],
  });

  const port = await new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error('Timed out waiting for RPC server'));
    }, 5000);

    serverProcess.on('message', (message) => {
      if (message?.type === 'request') {
        requests.push(message.data);
        const resolver = pendingRequests.shift();
        if (resolver) {
          resolver(message.data);
        }
      }
      if (message?.type === 'ready') {
        clearTimeout(timeout);
        resolve(message.port);
      }
    });

    serverProcess.once('exit', (code) => {
      clearTimeout(timeout);
      reject(new Error(`RPC server exited early with code ${code}`));
    });
  });

  process.env.MERKLE_SERVER = `http://127.0.0.1:${port}`;
  rpc = await import('../src/bootstrap/rpcbind.js');
});

after(async () => {
  if (serverProcess) {
    serverProcess.send({ type: 'close' });
    await once(serverProcess, 'exit');
  }
});

test('get_leaf uses HTTP RPC and returns result', async () => {
  const root = new Uint8Array([1, 2, 3, 4]);
  const requestPromise = waitForRequest();
  const result = rpc.get_leaf(root, 42n);
  const request = await requestPromise;
  assert.deepStrictEqual(result, { leaf: '42' });
  assert.strictEqual(request.method, 'get_leaf');
});

test('get_record maps results to BigInt array', async () => {
  const hash = new Uint8Array([1, 2, 3, 4]);
  const requestPromise = waitForRequest();
  const result = rpc.get_record(hash);
  const request = await requestPromise;
  assert.deepStrictEqual(result, [7n, 8n]);
  assert.strictEqual(request.method, 'get_record');
});
