import assert from 'node:assert';
import { test, before } from 'node:test';

let rpc;

before(async () => {
  // The sandbox forbids opening sockets; use an in-process mock merkle RPC.
  process.env.MERKLE_RPC_MODE = 'mock';
  rpc = await import('../src/bootstrap/rpcbind.js');
});

test('get_leaf uses HTTP RPC and returns result', async () => {
  const root = new Uint8Array([1, 2, 3, 4]);
  const result = rpc.get_leaf(root, 42n);
  assert.deepStrictEqual(result, { leaf: '42' });
});

test('get_record maps results to BigInt array', async () => {
  const hash = new Uint8Array([1, 2, 3, 4]);
  const result = rpc.get_record(hash);
  assert.deepStrictEqual(result, [7n, 8n]);
});

test('trace-only writes use overlay and record trace', async () => {
  globalThis.__MERKLE_TRACE_ONLY_WRITES = true;
  globalThis.__MERKLE_TRACE = {};
  globalThis.__MERKLE_TRACE_OVERLAY = {
    leaves: new Map(),
    records: new Map(),
  };

  const root = new Uint8Array(32).fill(1);
  const data = new Uint8Array(32).fill(2);

  const nextRoot = rpc.update_leaf(root, 42n, data);
  assert.deepStrictEqual(nextRoot, Array.from(root));
  assert.deepStrictEqual(globalThis.__MERKLE_TRACE_OVERLAY.leaves.get('42'), Array.from(data));
  assert.deepStrictEqual(globalThis.__MERKLE_TRACE.writes, [{ index: '42', data: Array.from(data) }]);

  const leaf = rpc.get_leaf(root, 42n);
  assert.deepStrictEqual(leaf, Array.from(data));
  assert.deepStrictEqual(globalThis.__MERKLE_TRACE.reads, ['42']);

  const hash = new Uint8Array(32).fill(3);
  const record = [7n, 8n];
  const ok = rpc.update_record(hash, record);
  assert.strictEqual(ok, true);
  const hashKey = Buffer.from(hash).toString('hex');
  assert.deepStrictEqual(globalThis.__MERKLE_TRACE_OVERLAY.records.get(hashKey), record);

  const record2 = rpc.get_record(hash);
  assert.deepStrictEqual(record2, record);
  assert.ok(Array.isArray(globalThis.__MERKLE_TRACE.getRecords));

  globalThis.__MERKLE_TRACE_ONLY_WRITES = undefined;
  globalThis.__MERKLE_TRACE = undefined;
  globalThis.__MERKLE_TRACE_OVERLAY = undefined;
});

test('rpcbind mock covers remaining exported RPC helpers', async () => {
  globalThis.__MERKLE_TRACE_ONLY_WRITES = false;
  globalThis.__MERKLE_TRACE = {};
  globalThis.__MERKLE_TRACE_OVERLAY = { leaves: {}, records: {} };
  globalThis.__MERKLE_SESSION = 's1';

  const root = new Uint8Array(32).fill(9);
  const data = new Uint8Array(32).fill(10);
  const hash = new Uint8Array(32).fill(11);

  assert.deepStrictEqual(rpc.update_leaf(root, 1n, data), { ok: true });
  assert.deepStrictEqual(rpc.update_record(hash, [1n, 2n]), { ok: true });
  assert.deepStrictEqual(rpc.apply_txs(root, [{ writes: [], updateRecords: [] }]), { ok: true });
  assert.deepStrictEqual(rpc.apply_txs_final(root, [{ writes: [], updateRecords: [] }]), { ok: true });
  assert.deepStrictEqual(await rpc.apply_txs_async(root, [{ writes: [], updateRecords: [] }]), { ok: true });
  assert.deepStrictEqual(await rpc.apply_txs_final_async(root, [{ writes: [], updateRecords: [] }]), { ok: true });

  assert.deepStrictEqual(rpc.begin_session(), { ok: true });
  assert.deepStrictEqual(rpc.drop_session('s1'), { ok: true });
  assert.deepStrictEqual(rpc.reset_session('s1'), { ok: true });
  assert.deepStrictEqual(rpc.commit_session('s1'), { ok: true });
  assert.strictEqual(rpc.ping(), true);

  globalThis.__MERKLE_TRACE_ONLY_WRITES = undefined;
  globalThis.__MERKLE_TRACE = undefined;
  globalThis.__MERKLE_TRACE_OVERLAY = undefined;
  globalThis.__MERKLE_SESSION = undefined;
});
