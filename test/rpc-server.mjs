import http from 'node:http';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createRequire } from 'node:module';

const FORCE_STATUS = (() => {
  const raw = process.env.RPC_SERVER_FORCE_STATUS;
  if (!raw) return null;
  const n = Number.parseInt(raw, 10);
  return Number.isFinite(n) ? n : null;
})();
const ERROR_ON_METHOD = process.env.RPC_SERVER_ERROR_ON_METHOD ?? null;
const ERROR_ON_INDEX = process.env.RPC_SERVER_ERROR_ON_INDEX ?? null;
const BACKEND = process.env.RPC_SERVER_BACKEND ?? 'stub';

const require = createRequire(import.meta.url);
let merkleNative;
let merkleOpened = false;

function bytes32FromJson(value, name) {
  if (!Array.isArray(value) || value.length !== 32) {
    throw new Error(`${name} must be number[32]`);
  }
  const buf = Buffer.alloc(32);
  for (let i = 0; i < 32; i++) {
    const n = Number(value[i] ?? 0);
    if (!Number.isFinite(n) || n < 0 || n > 255) {
      throw new Error(`${name}[${i}] must be byte`);
    }
    buf[i] = n;
  }
  return buf;
}

function ensureMerkleOpen() {
  if (BACKEND !== 'native') return;
  if (merkleOpened) return;
  const addonPath = path.join(
    path.dirname(new URL(import.meta.url).pathname),
    '..',
    'native',
    'merkle-native',
    'merkle_native.node',
  );
  merkleNative = require(addonPath);
  const dbDir =
    process.env.RPC_SERVER_MERKLE_DB ??
    fs.mkdtempSync(path.join(os.tmpdir(), `zkwasm-rpc-server-${process.pid}-`));
  fs.mkdirSync(dbDir, { recursive: true });
  merkleNative.open(dbDir);
  merkleOpened = true;
}

const server = http.createServer((req, res) => {
  let body = '';
  req.on('data', (chunk) => {
    body += chunk.toString();
  });
  req.on('end', () => {
    let parsed;
    try {
      parsed = JSON.parse(body);
    } catch (e) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          error: `invalid json: ${e instanceof Error ? e.message : String(e)}`,
        }),
      );
      return;
    }
    if (process.send) {
      process.send({ type: 'request', data: parsed });
    }
    if (FORCE_STATUS) {
      res.writeHead(FORCE_STATUS, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: `forced status ${FORCE_STATUS}` }));
      return;
    }

    try {
      ensureMerkleOpen();
    } catch (e) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          jsonrpc: '2.0',
          id: parsed.id,
          error: { message: e instanceof Error ? e.message : String(e) },
        }),
      );
      return;
    }

    try {
      let result;
      switch (parsed.method) {
        case 'ping':
          result = true;
          break;
        case 'get_leaf':
          if (
            ERROR_ON_METHOD === 'get_leaf' &&
            ERROR_ON_INDEX &&
            parsed?.params?.index === ERROR_ON_INDEX
          ) {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(
              JSON.stringify({ jsonrpc: '2.0', id: parsed.id, error: { message: 'bad leaf' } }),
            );
            return;
          }
          if (BACKEND === 'native') {
            const root = bytes32FromJson(parsed?.params?.root, 'root');
            const index = String(parsed?.params?.index ?? '');
            const session =
              typeof parsed?.params?.session === 'string' ? parsed.params.session : null;
            const leaf = merkleNative.getLeaf(root, index, session);
            result = Array.from(leaf);
          } else {
            result = { leaf: parsed.params.index };
          }
          break;
        case 'update_leaf':
          if (BACKEND === 'native') {
            const root = bytes32FromJson(parsed?.params?.root, 'root');
            const index = String(parsed?.params?.index ?? '');
            const data = bytes32FromJson(parsed?.params?.data, 'data');
            const session =
              typeof parsed?.params?.session === 'string' ? parsed.params.session : null;
            const next = merkleNative.updateLeaf(root, index, data, session);
            result = Array.from(next);
          } else {
            result = { ok: true };
          }
          break;
        case 'get_record':
          if (BACKEND === 'native') {
            const hash = bytes32FromJson(parsed?.params?.hash, 'hash');
            const session =
              typeof parsed?.params?.session === 'string' ? parsed.params.session : null;
            result = merkleNative.getRecord(hash, session);
          } else {
            result = ['7', '8'];
          }
          break;
        case 'update_record':
          if (BACKEND === 'native') {
            const hash = bytes32FromJson(parsed?.params?.hash, 'hash');
            const data = Array.isArray(parsed?.params?.data) ? parsed.params.data : [];
            const session =
              typeof parsed?.params?.session === 'string' ? parsed.params.session : null;
            merkleNative.updateRecord(hash, data, session);
            result = null;
          } else {
            result = { ok: true };
          }
          break;
        case 'apply_txs':
          if (BACKEND === 'native') {
            const root = bytes32FromJson(parsed?.params?.root, 'root');
            const session =
              typeof parsed?.params?.session === 'string' ? parsed.params.session : null;
            const txsIn = Array.isArray(parsed?.params?.txs) ? parsed.params.txs : [];
            const txs = txsIn.map((tx) => ({
              writes: Array.isArray(tx?.writes)
                ? tx.writes.map((w) => ({
                    index: String(w?.index ?? ''),
                    data: bytes32FromJson(w?.data, 'write.data'),
                  }))
                : [],
              updateRecords: Array.isArray(tx?.updateRecords ?? tx?.update_records)
                ? (tx.updateRecords ?? tx.update_records).map((r) => ({
                    hash: bytes32FromJson(r?.hash, 'updateRecord.hash'),
                    data: Array.isArray(r?.data) ? r.data : [],
                  }))
                : [],
            }));
            const roots = merkleNative.applyTxs(root, txs, session);
            result = roots.map((r) => Array.from(r));
          } else {
            result = { ok: true };
          }
          break;
        case 'apply_txs_final':
          if (BACKEND === 'native') {
            const root = bytes32FromJson(parsed?.params?.root, 'root');
            const session =
              typeof parsed?.params?.session === 'string' ? parsed.params.session : null;
            const txsIn = Array.isArray(parsed?.params?.txs) ? parsed.params.txs : [];
            const txs = txsIn.map((tx) => ({
              writes: Array.isArray(tx?.writes)
                ? tx.writes.map((w) => ({
                    index: String(w?.index ?? ''),
                    data: bytes32FromJson(w?.data, 'write.data'),
                  }))
                : [],
              updateRecords: Array.isArray(tx?.updateRecords ?? tx?.update_records)
                ? (tx.updateRecords ?? tx.update_records).map((r) => ({
                    hash: bytes32FromJson(r?.hash, 'updateRecord.hash'),
                    data: Array.isArray(r?.data) ? r.data : [],
                  }))
                : [],
            }));
            const out = merkleNative.applyTxsFinal(root, txs, session);
            result = Array.from(out);
          } else {
            result = { ok: true };
          }
          break;
        case 'begin_session':
          result = BACKEND === 'native' ? merkleNative.beginSession() : { ok: true };
          break;
        case 'drop_session':
          if (BACKEND === 'native') {
            result = merkleNative.dropSession(String(parsed?.params?.session ?? ''));
          } else {
            result = { ok: true };
          }
          break;
        case 'reset_session':
          if (BACKEND === 'native') {
            result = merkleNative.resetSession(String(parsed?.params?.session ?? ''));
          } else {
            result = { ok: true };
          }
          break;
        case 'commit_session':
          if (BACKEND === 'native') {
            const r = merkleNative.commitSession(String(parsed?.params?.session ?? ''));
            result = { merkle_records: r?.merkleRecords ?? 0, data_records: r?.dataRecords ?? 0 };
          } else {
            result = { ok: true };
          }
          break;
        default:
          if (ERROR_ON_METHOD && parsed.method === ERROR_ON_METHOD) {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(
              JSON.stringify({
                jsonrpc: '2.0',
                id: parsed.id,
                error: { message: `bad method ${ERROR_ON_METHOD}` },
              }),
            );
            return;
          }
          result = { ok: true };
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ jsonrpc: '2.0', id: parsed.id, result }));
    } catch (e) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          jsonrpc: '2.0',
          id: parsed?.id,
          error: { message: e instanceof Error ? e.message : String(e) },
        }),
      );
    }
  });
});

server.listen(0, '127.0.0.1', () => {
  const { port } = server.address();
  if (process.send) {
    process.send({ type: 'ready', port });
  }
});

process.on('message', (message) => {
  if (message?.type === 'close') {
    server.close(() => {
      process.exit(0);
    });
  }
});
