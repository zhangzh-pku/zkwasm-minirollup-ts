import requestMerkleData from './syncrpc.cjs';
let url = 'http://127.0.0.1:3030';
import dotenv from 'dotenv';
import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
dotenv.config();

// Load environment variables from .env file
//
if (process.env.MERKLE_SERVER) {
  url = process.env.MERKLE_SERVER;
}

console.log("rpc bind merkle server:", url);
const MERKLE_RPC_MODE = process.env.MERKLE_RPC_MODE ?? 'syncproc';
const require = createRequire(import.meta.url);
let syncFetch;
const __dirname = path.dirname(fileURLToPath(import.meta.url));

let merkleNative;
let merkleNativeOpened = false;
const MERKLE_NATIVE_ADDON_PATH = path.join(
  __dirname,
  '../../native/merkle-native/merkle_native.node',
);

function getMerkleDbUri() {
  return process.env.MERKLE_DB_URI ?? process.env.MERKLE_DB_PATH ?? process.env.MERKLE_DB ?? '';
}

function ensureMerkleNativeOpen() {
  if (merkleNativeOpened) return;
  const uri = getMerkleDbUri();
  if (!uri) {
    throw new Error('MERKLE_DB_URI is required when MERKLE_RPC_MODE=native');
  }
  if (!merkleNative) {
    try {
      merkleNative = require(MERKLE_NATIVE_ADDON_PATH);
    } catch (e) {
      throw new Error(
        `failed to load merkle native addon at ${MERKLE_NATIVE_ADDON_PATH}: ${e instanceof Error ? e.message : String(e)}`,
      );
    }
  }
  merkleNative.open(uri);
  merkleNativeOpened = true;
}

function bytes32ToBuffer(value, name) {
  if (Buffer.isBuffer(value)) {
    if (value.length !== 32) throw new Error(`${name} expected 32 bytes, got ${value.length}`);
    return value;
  }
  if (value instanceof Uint8Array) {
    if (value.length !== 32) throw new Error(`${name} expected 32 bytes, got ${value.length}`);
    return Buffer.from(value);
  }
  if (Array.isArray(value)) {
    if (value.length !== 32) throw new Error(`${name} expected 32 bytes, got ${value.length}`);
    return Buffer.from(value);
  }
  throw new Error(`${name} must be Uint8Array/Buffer/number[32], got ${typeof value}`);
}

function normalizeTxsForNative(txs) {
  if (!Array.isArray(txs)) return [];
  return txs.map((tx) => ({
    writes: Array.isArray(tx?.writes)
      ? tx.writes.map((w) => ({
          index: w.index,
          data: bytes32ToBuffer(w.data, 'write.data'),
        }))
      : [],
    updateRecords: Array.isArray(tx?.updateRecords)
      ? tx.updateRecords.map((r) => ({
          hash: bytes32ToBuffer(r.hash, 'updateRecord.hash'),
          data: r.data,
        }))
      : [],
  }));
}

function requestMerkle(requestData) {
  if (MERKLE_RPC_MODE === 'mock') {
    let result;
    switch (requestData?.method) {
      case 'ping':
        result = true;
        break;
      case 'get_leaf':
        result = { leaf: requestData?.params?.index };
        break;
      case 'get_record':
        result = ['7', '8'];
        break;
      default:
        result = { ok: true };
    }
    return JSON.stringify({ jsonrpc: '2.0', id: requestData?.id, result });
  }
  if (MERKLE_RPC_MODE === 'http') {
    if (!syncFetch) {
      syncFetch = require('sync-fetch');
    }
    const resp = syncFetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(requestData),
    });
    if (!resp.ok) {
      throw new Error(`merkle rpc failed: ${resp.status}`);
    }
    return resp.text();
  }
  return requestMerkleData(requestData);
}

async function requestMerkleAsync(requestData) {
  if (MERKLE_RPC_MODE === 'mock') {
    return JSON.parse(requestMerkle(requestData));
  }
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(requestData),
  });
  if (!resp.ok) {
    throw new Error(`merkle rpc failed: ${resp.status}`);
  }
  return await resp.json();
}

function getMerkleTrace() {
  const trace = globalThis.__MERKLE_TRACE;
  if (!trace || typeof trace !== 'object') return null;
  return trace;
}

function getMerkleSession() {
  const session = globalThis.__MERKLE_SESSION;
  if (typeof session !== 'string' || session.length === 0) return null;
  return session;
}

function traceOnlyWritesEnabled() {
  return globalThis.__MERKLE_TRACE_ONLY_WRITES === true;
}

function getMerkleOverlay() {
  const overlay = globalThis.__MERKLE_TRACE_OVERLAY;
  if (!overlay || typeof overlay !== 'object') return null;
  return overlay;
}

function overlayGet(overlay, kind, key) {
  if (!overlay) return undefined;
  const store = overlay[kind];
  if (!store) return undefined;
  if (store instanceof Map) return store.get(key);
  return store[key];
}

function overlaySet(overlay, kind, key, value) {
  if (!overlay) return;
  const store = overlay[kind];
  if (store instanceof Map) {
    store.set(key, value);
  } else if (store && typeof store === 'object') {
    store[key] = value;
  }
}

function withSession(params) {
  const session = getMerkleSession();
  if (!session) return params;
  return { ...params, session };
}

function hash2array(hash) {
  if (Array.isArray(hash)) return hash;
  if (hash instanceof Uint8Array) return Array.from(hash);
  const hasharray = [];
  for (let v of hash) {
      hasharray.push(v);
  }
  return hasharray;
}

function bigintArray2array(hash) {
  const hasharray = [];
  for (let v of hash) {
      hasharray.push(v.toString());
  }
  return hasharray;
}

function bytes32ToKey(value) {
  if (Buffer.isBuffer(value)) return value.toString('hex');
  if (value instanceof Uint8Array) return Buffer.from(value).toString('hex');
  if (Array.isArray(value)) return Buffer.from(value).toString('hex');
  return String(value);
}

function async_get_leaf(root, index) {
  if (MERKLE_RPC_MODE === 'native') {
    ensureMerkleNativeOpen();
    const leaf = merkleNative.getLeaf(
      bytes32ToBuffer(root, 'root'),
      index.toString(),
      getMerkleSession(),
    );
    return leaf;
  }
  let roothash = hash2array(root);
  const requestData = {
    jsonrpc: '2.0',
    method: 'get_leaf',
    params: withSession({root: roothash, index: index.toString()}),
    id: 1
  };
  //console.log("get leaf", root);
  let data = requestMerkle(requestData);
  const response = JSON.parse(data);
  if (response.error==undefined) {
    //console.log(jsonResponse);
    return response.result;
  } else {
    console.error('Failed to fetch:', response.error);
    throw("Failed to get leaf");
  }
}

export function get_leaf(root, index) {
  const overlay = getMerkleOverlay();
  const indexKey = index.toString();
  const overlayLeaf = overlayGet(overlay, "leaves", indexKey);
  if (overlayLeaf !== undefined) {
    const trace = getMerkleTrace();
    if (trace) {
      if (!Array.isArray(trace.reads)) trace.reads = [];
      trace.reads.push(indexKey);
    }
    return overlayLeaf;
  }
  let data = async_get_leaf(root, index);
  const trace = getMerkleTrace();
  if (trace) {
    if (!Array.isArray(trace.reads)) trace.reads = [];
    trace.reads.push(indexKey);
  }
  return data;
}

function async_update_leaf(root, index, data) {
  if (MERKLE_RPC_MODE === 'native') {
    ensureMerkleNativeOpen();
    const nextRoot = merkleNative.updateLeaf(
      bytes32ToBuffer(root, 'root'),
      index.toString(),
      bytes32ToBuffer(data, 'data'),
      getMerkleSession(),
    );
    return nextRoot;
  }
  let roothash = hash2array(root);
  let datahash = hash2array(data);
  const requestData = {
    jsonrpc: '2.0',
    method: 'update_leaf',
    params: withSession({root: roothash, index: index.toString(), data: datahash}),
    id: 2
  };
  //console.log("get leaf", root);
  let responseStr = requestMerkle(requestData);
  const response = JSON.parse(responseStr);
  if (response.error==undefined) {
    //console.log(jsonResponse);
    return response.result;
  } else {
    console.error('Failed to fetch:', response.error);
    throw("Failed to update leaf");
  }

}
export function update_leaf(root, index, data) {
  const trace = getMerkleTrace();
  const indexKey = index.toString();
  const writeData = MERKLE_RPC_MODE === 'native' ? data : hash2array(data);

  let r;
  if (traceOnlyWritesEnabled()) {
    const overlay = getMerkleOverlay();
    overlaySet(overlay, "leaves", indexKey, writeData);
    r = MERKLE_RPC_MODE === 'native' ? root : hash2array(root);
  } else {
    r = async_update_leaf(root, index, data);
  }
  if (trace) {
    if (!Array.isArray(trace.writes)) trace.writes = [];
    trace.writes.push({
      index: indexKey,
      data: writeData,
    });
  }
  return r;
}

function async_update_record(hash, data) {
  if (MERKLE_RPC_MODE === 'native') {
    ensureMerkleNativeOpen();
    merkleNative.updateRecord(
      bytes32ToBuffer(hash, 'hash'),
      bigintArray2array(data),
      getMerkleSession(),
    );
    return null;
  }
  let roothash = hash2array(hash);
  let datavec = bigintArray2array(data);
  const requestData = {
    jsonrpc: '2.0',
    method: 'update_record',
    params: withSession({hash: roothash, data: datavec}),
    id: 3
  };
  let responseStr = requestMerkle(requestData);
  const response = JSON.parse(responseStr);
  if (response.error==undefined) {
    return response.result;
  } else {
    console.log(response);
    console.error('Failed to fetch:', response.error);
    throw("Failed to update_record");
  }
}

export function update_record(hash, data) {
  const trace = getMerkleTrace();
  const hashKey = bytes32ToKey(hash);
  const hashArray = trace ? hash2array(hash) : null;
  const dataArray = trace ? bigintArray2array(data) : null;
  let r;
  if (traceOnlyWritesEnabled()) {
    const overlay = getMerkleOverlay();
    overlaySet(overlay, "records", hashKey, data);
    r = true;
  } else {
    r = async_update_record(hash, data);
  }
  if (trace) {
    if (!Array.isArray(trace.updateRecords)) trace.updateRecords = [];
    trace.updateRecords.push({
      hash: hashArray,
      data: dataArray,
    });
  }
  return r;
}

function async_apply_txs(root, txs) {
  if (MERKLE_RPC_MODE === 'native') {
    ensureMerkleNativeOpen();
    const roots = merkleNative.applyTxs(
      bytes32ToBuffer(root, 'root'),
      normalizeTxsForNative(txs),
      getMerkleSession(),
    );
    return roots.map((r) => hash2array(r));
  }
  let roothash = hash2array(root);
  const requestData = {
    jsonrpc: '2.0',
    method: 'apply_txs',
    params: withSession({ root: roothash, txs }),
    id: 9
  };
  let responseStr = requestMerkle(requestData);
  const response = JSON.parse(responseStr);
  if (response.error==undefined) {
    return response.result;
  } else {
    console.error('Failed to apply_txs:', response.error);
    throw("Failed to apply_txs");
  }
}

export function apply_txs(root, txs) {
  const start = performance.now();
  let r = async_apply_txs(root, txs);
  const end = performance.now();
  let lag = end - start;
  //console.log("bench-log: apply_txs", lag);
  return r;
}

function async_apply_txs_final(root, txs) {
  if (MERKLE_RPC_MODE === 'native') {
    ensureMerkleNativeOpen();
    const finalRoot = merkleNative.applyTxsFinal(
      bytes32ToBuffer(root, 'root'),
      normalizeTxsForNative(txs),
      getMerkleSession(),
    );
    return hash2array(finalRoot);
  }
  let roothash = hash2array(root);
  const requestData = {
    jsonrpc: '2.0',
    method: 'apply_txs_final',
    params: withSession({ root: roothash, txs }),
    id: 10,
  };
  let responseStr = requestMerkle(requestData);
  const response = JSON.parse(responseStr);
  if (response.error==undefined) {
    return response.result;
  } else {
    console.error('Failed to apply_txs_final:', response.error);
    throw("Failed to apply_txs_final");
  }
}

export function apply_txs_final(root, txs) {
  const start = performance.now();
  let r = async_apply_txs_final(root, txs);
  const end = performance.now();
  let lag = end - start;
  //console.log("bench-log: apply_txs_final", lag);
  return r;
}

export async function apply_txs_async(root, txs) {
  if (MERKLE_RPC_MODE === 'native') {
    ensureMerkleNativeOpen();
    const roots = merkleNative.applyTxs(
      bytes32ToBuffer(root, 'root'),
      normalizeTxsForNative(txs),
      getMerkleSession(),
    );
    return roots.map((r) => hash2array(r));
  }
  let roothash = hash2array(root);
  const requestData = {
    jsonrpc: '2.0',
    method: 'apply_txs',
    params: withSession({ root: roothash, txs }),
    id: 11,
  };
  const response = await requestMerkleAsync(requestData);
  if (response.error == undefined) {
    return response.result;
  } else {
    console.error('Failed to apply_txs_async:', response.error);
    throw new Error('Failed to apply_txs_async');
  }
}

export async function apply_txs_final_async(root, txs) {
  if (MERKLE_RPC_MODE === 'native') {
    ensureMerkleNativeOpen();
    const finalRoot = merkleNative.applyTxsFinal(
      bytes32ToBuffer(root, 'root'),
      normalizeTxsForNative(txs),
      getMerkleSession(),
    );
    return hash2array(finalRoot);
  }
  let roothash = hash2array(root);
  const requestData = {
    jsonrpc: '2.0',
    method: 'apply_txs_final',
    params: withSession({ root: roothash, txs }),
    id: 12,
  };
  const response = await requestMerkleAsync(requestData);
  if (response.error == undefined) {
    return response.result;
  } else {
    console.error('Failed to apply_txs_final_async:', response.error);
    throw new Error('Failed to apply_txs_final_async');
  }
}

function async_get_record(hash) {
  if (MERKLE_RPC_MODE === 'native') {
    ensureMerkleNativeOpen();
    const out = merkleNative.getRecord(bytes32ToBuffer(hash, 'hash'), getMerkleSession());
    return out.map((x) => BigInt(x));
  }
  let hasharray = hash2array(hash);
  const requestData = {
    jsonrpc: '2.0',
    method: 'get_record',
    params: withSession({hash: hasharray}),
    id: 4
  };

  let responseStr = requestMerkle(requestData);
  const response = JSON.parse(responseStr);
  if (response.error==undefined) {
    let result = response.result.map((x)=>{return BigInt(x)});
    return result;
  } else {
    console.log("get_record");
    console.error('Failed to fetch:', response.statusText);
    throw("Failed to get_record");
  }
}

export function get_record(hash) {
  const overlay = getMerkleOverlay();
  const trace = getMerkleTrace();
  const hashKey = bytes32ToKey(hash);
  const hashArray = trace ? hash2array(hash) : null;
  const overlayRecord = overlayGet(overlay, "records", hashKey);
  if (overlayRecord !== undefined) {
    if (trace) {
      if (!Array.isArray(trace.getRecords)) trace.getRecords = [];
      trace.getRecords.push({
        hash: hashArray,
      });
    }
    return overlayRecord;
  }
  let r = async_get_record(hash);
  if (trace) {
    if (!Array.isArray(trace.getRecords)) trace.getRecords = [];
    trace.getRecords.push({
      hash: hashArray,
    });
  }
  return r;

}

export function begin_session() {
  if (MERKLE_RPC_MODE === 'native') {
    ensureMerkleNativeOpen();
    return merkleNative.beginSession();
  }
  const requestData = {
    jsonrpc: '2.0',
    method: 'begin_session',
    params: {},
    id: 5,
  };
  const responseStr = requestMerkle(requestData);
  const response = JSON.parse(responseStr);
  if (response.error == undefined) {
    return response.result;
  } else {
    console.error('Failed to begin_session:', response.error);
    throw new Error('Failed to begin_session');
  }
}

export function drop_session(session) {
  if (MERKLE_RPC_MODE === 'native') {
    ensureMerkleNativeOpen();
    return merkleNative.dropSession(session);
  }
  const requestData = {
    jsonrpc: '2.0',
    method: 'drop_session',
    params: { session },
    id: 6,
  };
  const responseStr = requestMerkle(requestData);
  const response = JSON.parse(responseStr);
  if (response.error == undefined) {
    return response.result;
  } else {
    console.error('Failed to drop_session:', response.error);
    throw new Error('Failed to drop_session');
  }
}

export function reset_session(session) {
  if (MERKLE_RPC_MODE === 'native') {
    ensureMerkleNativeOpen();
    return merkleNative.resetSession(session);
  }
  const requestData = {
    jsonrpc: '2.0',
    method: 'reset_session',
    params: { session },
    id: 7,
  };
  const responseStr = requestMerkle(requestData);
  const response = JSON.parse(responseStr);
  if (response.error == undefined) {
    return response.result;
  } else {
    console.error('Failed to reset_session:', response.error);
    throw new Error('Failed to reset_session');
  }
}

export function commit_session(session) {
  if (MERKLE_RPC_MODE === 'native') {
    ensureMerkleNativeOpen();
    const result = merkleNative.commitSession(session);
    return {
      merkle_records: result?.merkleRecords ?? 0,
      data_records: result?.dataRecords ?? 0,
    };
  }
  const requestData = {
    jsonrpc: '2.0',
    method: 'commit_session',
    params: { session },
    id: 8,
  };
  const responseStr = requestMerkle(requestData);
  const response = JSON.parse(responseStr);
  if (response.error == undefined) {
    return response.result;
  } else {
    console.error('Failed to commit_session:', response.error);
    throw new Error('Failed to commit_session');
  }
}

export function ping() {
  if (MERKLE_RPC_MODE === 'native') {
    ensureMerkleNativeOpen();
    return merkleNative.ping();
  }
  if (MERKLE_RPC_MODE === 'mock') {
    return true;
  }
  const requestData = {
    jsonrpc: '2.0',
    method: 'ping',
    params: {},
    id: 123,
  };
  const responseStr = requestMerkle(requestData);
  const response = JSON.parse(responseStr);
  if (response.error == undefined) {
    return response.result;
  }
  throw new Error(`Failed to ping merkle service: ${JSON.stringify(response.error)}`);
}
