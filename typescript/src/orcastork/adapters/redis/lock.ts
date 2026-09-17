/**
 * Redis `SessionLock` — a `PX` liveness lease whose value is a monotonic minted epoch.
 *
 * The lock value is the minted epoch, so acquire/release/renew are compare-by-epoch (Lua) — a stale
 * holder cannot release or extend a successor's lease. The epoch counter (`mint:{session}`) only
 * ever increases and advances only when a lease is actually granted, so a non-zero epoch means the
 * session was started at some point; lease expiry is Redis's own TTL.
 *
 * @module
 */

import { LockHeldError, StaleEpochError } from '../../exceptions.js';
import type { Epoch, SessionId } from '../../ids.js';
import { Epoch as toEpoch } from '../../ids.js';
import type { SessionLock } from '../../ports/session_lock.js';
import type { RedisEvalCommand, RedisExpireCommand } from './ttl.js';
import { DEFAULT_STATE_TTL_MS, slideTtl } from './ttl.js';

/** How long a lease stays live without a renew (Python's `DEFAULT_TTL_MS`). */
export const DEFAULT_TTL_MS = 30_000;

/** The commands this lock needs. */
export interface RedisLockClient extends RedisEvalCommand, RedisExpireCommand {
  get(key: string): Promise<string | null>;
  exists(key: string): Promise<number>;
  del(key: string): Promise<number>;
}

// KEYS[1]=lock key, KEYS[2]=mint key, ARGV[1]=lease TTL ms. Minting and taking the lease must be one
// atomic step, not INCR-then-SET: between those two calls the mint reads as advanced while no lease is
// held, which is indistinguishable from a started session whose owner died — and a caller that treats a
// minted epoch as "already started" (the manager's resume guard) would act on a session mid-acquire.
// Returns the minted epoch, or 0 when a live lease already holds the lock.
const ACQUIRE = `if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
local epoch = redis.call('INCR', KEYS[2])
redis.call('SET', KEYS[1], epoch, 'PX', ARGV[1])
return epoch`;

const RELEASE = `if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0`;

const RENEW = `if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) end
return 0`;

// KEYS[1]=mint key, KEYS[2]=complete key, ARGV[1]=epoch. Compare against the live epoch and set the
// completion flag in one atomic script — a writer below the current epoch (a fenced predecessor) loses.
const COMPLETE = `if tonumber(redis.call('GET', KEYS[1]) or '0') > tonumber(ARGV[1]) then return 0 end
redis.call('SET', KEYS[2], '1')
return 1`;

/** How a {@link RedisSessionLock} is configured. */
export interface RedisSessionLockOptions {
  /** Lease lifetime; it must exceed the longest single unrenewed await in the completion tail. */
  readonly ttlMs?: number;

  /** The sliding lifetime of the mint/complete counters. */
  readonly stateTtlMs?: number;
}

/** Ownership of a session: the lease, the fencing epoch it mints, and the completion flag. */
export class RedisSessionLock implements SessionLock {
  private readonly redis: RedisLockClient;
  private readonly ttlMs: number;
  private readonly stateTtlMs: number;

  public constructor(redis: RedisLockClient, options: RedisSessionLockOptions = {}) {
    this.redis = redis;
    this.ttlMs = options.ttlMs ?? DEFAULT_TTL_MS;
    // The lease (`lock` key) lives for ttlMs; the mint/complete counters outlive it by stateTtlMs
    // so fencing stays monotonic and completion stays readable well beyond any recovery window.
    this.stateTtlMs = options.stateTtlMs ?? DEFAULT_STATE_TTL_MS;
  }

  private static lockKey(sessionId: SessionId): string {
    return `lock:${sessionId}`;
  }

  private static mintKey(sessionId: SessionId): string {
    return `mint:${sessionId}`;
  }

  private static completeKey(sessionId: SessionId): string {
    return `complete:${sessionId}`;
  }

  public async acquire(sessionId: SessionId): Promise<Epoch> {
    const epoch = Number(
      await this.redis.eval(ACQUIRE, {
        keys: [RedisSessionLock.lockKey(sessionId), RedisSessionLock.mintKey(sessionId)],
        arguments: [String(this.ttlMs)],
      }),
    );
    if (epoch === 0) {
      throw new LockHeldError(`session ${sessionId} is already locked`);
    }
    await slideTtl(this.redis, this.stateTtlMs, RedisSessionLock.mintKey(sessionId));
    return toEpoch(epoch);
  }

  public async renew(sessionId: SessionId, options: { readonly epoch: Epoch }): Promise<void> {
    const extended = Number(
      await this.redis.eval(RENEW, {
        keys: [RedisSessionLock.lockKey(sessionId)],
        arguments: [String(options.epoch), String(this.ttlMs)],
      }),
    );
    if (extended === 0) {
      throw new StaleEpochError(`epoch ${options.epoch} does not hold the lock for session ${sessionId}`);
    }
  }

  public async release(sessionId: SessionId, options: { readonly epoch: Epoch }): Promise<void> {
    await this.redis.eval(RELEASE, {
      keys: [RedisSessionLock.lockKey(sessionId)],
      arguments: [String(options.epoch)],
    });
  }

  public async currentEpoch(sessionId: SessionId): Promise<Epoch> {
    const minted = await this.redis.get(RedisSessionLock.mintKey(sessionId));
    return toEpoch(minted === null ? 0 : Number(minted));
  }

  public async isHeld(sessionId: SessionId): Promise<boolean> {
    return (await this.redis.exists(RedisSessionLock.lockKey(sessionId))) > 0;
  }

  public async markComplete(sessionId: SessionId, options: { readonly epoch: Epoch }): Promise<void> {
    const marked = Number(
      await this.redis.eval(COMPLETE, {
        keys: [RedisSessionLock.mintKey(sessionId), RedisSessionLock.completeKey(sessionId)],
        arguments: [String(options.epoch)],
      }),
    );
    if (marked === 0) {
      throw new StaleEpochError(`epoch ${options.epoch} is stale for session ${sessionId}`);
    }
    await slideTtl(
      this.redis,
      this.stateTtlMs,
      RedisSessionLock.mintKey(sessionId),
      RedisSessionLock.completeKey(sessionId),
    );
  }

  public async isComplete(sessionId: SessionId): Promise<boolean> {
    return (await this.redis.exists(RedisSessionLock.completeKey(sessionId))) > 0;
  }

  public async clearComplete(sessionId: SessionId): Promise<void> {
    await this.redis.del(RedisSessionLock.completeKey(sessionId));
  }
}
