/**
 * Redis adapters (optional peer `redis`) — Lua-CAS store, Streams inbox, fencing lock, cooldown
 * gate, token-bucket rate limiter.
 *
 * Each adapter takes a node-redis client. They are typed against the commands they use rather than
 * against the client class, so any v5/v6 instance fits and nothing here imports the peer as a
 * value — the modules load, and type-check, wherever `redis` is not installed.
 *
 * Every key name, Lua script and JSON payload is byte-identical to the Python package's, so a
 * TypeScript worker and a Python worker can share one deployment and one keyspace.
 *
 * @module
 */

export type { RedisGateClient } from './gate.js';
export { RedisCooldownGate } from './gate.js';
export type {
  RedisInboxClient,
  RedisPendingEntry,
  RedisStreamMessage,
  RedisStreamsInboxOptions,
  RedisSubscriberClient,
} from './inbox.js';
export { RedisStreamsInbox } from './inbox.js';
export type { RedisLockClient, RedisSessionLockOptions } from './lock.js';
export { DEFAULT_TTL_MS, RedisSessionLock } from './lock.js';
export type { RedisRateLimiterClient } from './rate_limiter.js';
export { RedisRateLimiter } from './rate_limiter.js';
export type { RedisDataPointStoreOptions, RedisStoreClient } from './store.js';
export { RedisDataPointStore } from './store.js';
export type { RedisEvalCommand, RedisExpireCommand } from './ttl.js';
export { DEFAULT_STATE_TTL_MS } from './ttl.js';
