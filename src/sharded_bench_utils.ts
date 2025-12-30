type Env = Record<string, string | undefined>;

function replaceAll(haystack: string, needle: string, replacement: string): string {
  return haystack.split(needle).join(replacement);
}

export function resolveMerkleDbUriBase(env: Env): string {
  return env.MERKLE_DB_URI ?? env.MERKLE_DB_PATH ?? env.MERKLE_DB ?? "";
}

export function merkleDbUriForShard(opts: {
  base: string;
  image: string;
  shardId: number;
  shardCount: number;
}): string {
  const { base, image, shardId, shardCount } = opts;
  if (!base) return "";

  const hasShardPlaceholder = base.includes("{shardId}") || base.includes("{shard}");
  let uri = base;
  uri = replaceAll(uri, "{image}", image);
  uri = replaceAll(uri, "{shardId}", String(shardId));
  uri = replaceAll(uri, "{shard}", String(shardId));

  if (!hasShardPlaceholder && shardCount > 1) {
    uri = `${uri}_shard${shardId}`;
  }
  return uri;
}
