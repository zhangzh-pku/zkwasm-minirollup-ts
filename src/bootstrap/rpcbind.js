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
    params: {root: roothash, index: index.toString()},
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
    params: {root: roothash, index: index.toString(), data: datahash},
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
  let r = async_update_leaf(root, index, data);
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
    params: {hash: roothash, data: datavec},
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
  let r = async_update_record(hash, data);
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

function async_get_record(hash) {
  let hasharray = hash2array(hash);
  const requestData = {
    jsonrpc: '2.0',
    method: 'get_record',
    params: {hash: hasharray},
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
