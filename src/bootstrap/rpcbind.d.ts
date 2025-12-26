export function get_leaf(root: Uint8Array | number[], index: bigint): number[];
export function update_leaf(
  root: Uint8Array | number[],
  index: bigint,
  data: Uint8Array | number[],
): number[];

export function get_record(hash: Uint8Array | number[]): bigint[];
export function update_record(hash: Uint8Array | number[], data: bigint[]): unknown;

export function apply_txs(
  root: Uint8Array | number[],
  txs: Array<{
    writes: Array<{ index: string; data: Uint8Array | number[] }>;
    updateRecords: Array<{ hash: Uint8Array | number[]; data: string[] }>;
  }>,
): number[][];

export function apply_txs_final(
  root: Uint8Array | number[],
  txs: Array<{
    writes: Array<{ index: string; data: Uint8Array | number[] }>;
    updateRecords: Array<{ hash: Uint8Array | number[]; data: string[] }>;
  }>,
): number[];

export function apply_txs_async(
  root: Uint8Array | number[],
  txs: Array<{
    writes: Array<{ index: string; data: Uint8Array | number[] }>;
    updateRecords: Array<{ hash: Uint8Array | number[]; data: string[] }>;
  }>,
): Promise<number[][]>;

export function apply_txs_final_async(
  root: Uint8Array | number[],
  txs: Array<{
    writes: Array<{ index: string; data: Uint8Array | number[] }>;
    updateRecords: Array<{ hash: Uint8Array | number[]; data: string[] }>;
  }>,
): Promise<number[]>;

export function begin_session(): string;
export function drop_session(session: string): boolean;
export function reset_session(session: string): boolean;
export function commit_session(session: string): { merkle_records: number; data_records: number };
