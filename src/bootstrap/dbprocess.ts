import axios from 'axios';
import http from 'node:http';
import https from 'node:https';
import { URL } from 'node:url';
let url = 'http://127.0.0.1:3030';
if (process.env.MERKLE_SERVER) {
  url = process.env.MERKLE_SERVER;
}

export class MerkleServiceRpc {
  private baseUrl: string;
  private instance;
  private agent: http.Agent | https.Agent;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl;
    const parsed = new URL(baseUrl);
    const keepAlive = process.env.MERKLE_RPC_KEEPALIVE !== '0';
    const maxSocketsRaw = Number.parseInt(process.env.MERKLE_RPC_MAX_SOCKETS ?? "1", 10);
    const maxSockets = Number.isFinite(maxSocketsRaw) && maxSocketsRaw > 0 ? maxSocketsRaw : 1;
    this.agent =
      parsed.protocol === 'https:'
        ? new https.Agent({ keepAlive, maxSockets })
        : new http.Agent({ keepAlive, maxSockets });
    this.instance = axios.create({
      baseURL: this.baseUrl,
      headers: { 'Content-Type': 'application/json' },
      httpAgent: this.agent,
      httpsAgent: this.agent,
    });
  }

  public async queryDB(request: any): Promise<any> {
    try {
      const response = await this.instance.post("", request);
      if (response.status === 200) {
        return response.data
      } else {
        throw Error("SendTransactionError");
      }
    } catch (error) {
      if (error instanceof Error) {
        throw error
      } else {
        throw Error("UnknownDBError");
      }
    }
  }
}

let serviceRpc = new MerkleServiceRpc(url);
import * as readline from 'readline';

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

// Listen for the 'line' event to handle user input
rl.on('line', async (input: string) => {
  // console.log(`Received: ${input}`);
  // You can handle different inputs here
  if (input.startsWith("====")) {
    let param = JSON.parse(input.slice(4));
    try {
      let data = await serviceRpc.queryDB(param);
      console.log(JSON.stringify(data));
    } catch (e) {
      let obj = {
        error:(e as Error).message
      };
      console.log(JSON.stringify(obj))
    }
  }
});

// Handle the 'close' event when the interface is closed
rl.on('close', () => {
  console.log('>> Interface closed.');
  process.exit(0);
});
