export function get_leaf(root: Uint8Array | number[], index: bigint): number[];
export function update_leaf(
  root: Uint8Array | number[],
  index: bigint,
  data: Uint8Array | number[],
): number[];

export function get_record(hash: Uint8Array | number[]): bigint[];
export function update_record(hash: Uint8Array | number[], data: bigint[]): unknown;

export function begin_session(): string;
export function drop_session(session: string): boolean;
export function reset_session(session: string): boolean;
export function commit_session(session: string): { merkle_records: number; data_records: number };

