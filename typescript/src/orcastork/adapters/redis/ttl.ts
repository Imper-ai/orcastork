/**
 * Sliding-TTL helper shared by the Redis adapters, plus the client surface they are typed against.
 *
 * Every per-session key — the store hashes (`dp`/`added`/`upd`), the revision/epoch counters, the
 * inbox stream, the `mint`/`complete` lock counters — carries a long TTL that is refreshed on each
 * write. An active session therefore never expires, while a finished one is reclaimed, bounding
 * Redis growth. The TTL (24h by default) is far larger than the second-scale fencing and recovery
 * windows, so it never interferes with lease takeover, epoch monotonicity, or completion reads.
 *
 * The client is described **structurally**, one small interface per command family: any node-redis
 * client instance (v5 or v6, RESP2 or RESP3, with whatever modules a deployment configured)
 * satisfies them, and nothing in this package is a value imported from the optional peer — so the
 * adapters type-check and load even where `redis` is not installed.
 *
 * @module
 */

/** The default sliding lifetime of a session's Redis state: 24 hours. */
export const DEFAULT_STATE_TTL_MS = 86_400_000;

/** How the adapters run their Lua scripts (`EVAL`, with the keys and arguments split). */
export interface RedisEvalCommand {
  /**
   * Run a Lua script against `keys`/`arguments`.
   *
   * Every script in this family returns a Lua integer, which reaches the caller as a number.
   */
  eval(script: string, options: { keys: string[]; arguments: string[] }): Promise<unknown>;
}

/** How the adapters slide a key's TTL (`PEXPIRE`). */
export interface RedisExpireCommand {
  pExpire(key: string, ms: number): Promise<unknown>;
}

/** Refresh the TTL on each key in one round-trip (`PEXPIRE` on a missing key is a no-op). */
export const slideTtl = async (redis: RedisExpireCommand, ttlMs: number, ...keys: string[]): Promise<void> => {
  // node-redis writes every command issued within one tick into a single flush, which is what the
  // Python adapter's non-transactional pipeline buys: the refresh costs one round-trip, not one
  // per key.
  await Promise.all(keys.map((key) => redis.pExpire(key, ttlMs)));
};
