import requestMerkleData from './syncrpc.cjs';
let url = 'http://127.0.0.1:3030';
import dotenv from 'dotenv';
import { createRequire } from 'node:module';
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

function requestMerkle(requestData) {
  if (MERKLE_RPC_MODE === 'mock') {
    let result;
    switch (requestData?.method) {
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

function async_get_leaf(root, index) {
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
  const start = performance.now();
  let data = async_get_leaf(root, index);
  const end = performance.now();
  let lag = end - start;
  //console.log("bench-log: get_leaf", lag);
  const trace = getMerkleTrace();
  if (trace) {
    if (!Array.isArray(trace.reads)) trace.reads = [];
    trace.reads.push(index.toString());
  }
  return data;
}

function async_update_leaf(root, index, data) {
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
  const start = performance.now();
  let r;
  if (traceOnlyWritesEnabled()) {
    const overlay = getMerkleOverlay();
    overlaySet(overlay, "leaves", index.toString(), hash2array(data));
    r = hash2array(root);
  } else {
    r = async_update_leaf(root, index, data);
  }
  const end = performance.now();
  let lag = end - start;
  //console.log("bench-log: update_leaf", lag);
  const trace = getMerkleTrace();
  if (trace) {
    if (!Array.isArray(trace.writes)) trace.writes = [];
    trace.writes.push({
      index: index.toString(),
      data: hash2array(data),
    });
  }
  return r;
}

function async_update_record(hash, data) {
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
  const start = performance.now();
  let r;
  if (traceOnlyWritesEnabled()) {
    const overlay = getMerkleOverlay();
    overlaySet(overlay, "records", hash2array(hash).join(","), data);
    r = true;
  } else {
    r = async_update_record(hash, data);
  }
  const end = performance.now();
  let lag = end - start;
  //console.log("bench-log: update_record", lag);
  const trace = getMerkleTrace();
  if (trace) {
    if (!Array.isArray(trace.updateRecords)) trace.updateRecords = [];
    trace.updateRecords.push({
      hash: hash2array(hash),
      data: bigintArray2array(data),
    });
  }
  return r;
}

function async_apply_txs(root, txs) {
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

function async_get_record(hash) {
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
  const overlayRecord = overlayGet(overlay, "records", hash2array(hash).join(","));
  if (overlayRecord !== undefined) {
    const trace = getMerkleTrace();
    if (trace) {
      if (!Array.isArray(trace.getRecords)) trace.getRecords = [];
      trace.getRecords.push({
        hash: hash2array(hash),
      });
    }
    return overlayRecord;
  }
  const start = performance.now();
  let r = async_get_record(hash);
  const end = performance.now();
  let lag = end - start;
  //console.log("bench-log: update_record", lag);
  const trace = getMerkleTrace();
  if (trace) {
    if (!Array.isArray(trace.getRecords)) trace.getRecords = [];
    trace.getRecords.push({
      hash: hash2array(hash),
    });
  }
  return r;

}

export function begin_session() {
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
