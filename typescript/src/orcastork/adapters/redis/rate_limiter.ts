/**
 * Redis `RateLimiter` — an atomic Lua token bucket shared by every pod of the fleet.
 *
 * The bucket state (`tokens` + `refill_ms`) lives in a hash keyed `ratelimit:{key}`, and one Lua
 * script refills, takes a token, and reports the wait — atomically, so concurrent sessions across
 * pods can never overdraw the bucket. The script computes the refill from a caller-supplied now-ms
 * taken off the **injected clock** (wall time, comparable across pods), so tests drive it
 * deterministically with a `FakeClock`; waiting also goes through the injected clock. The key
 * expires once the bucket would be full again — an absent key reads as a full bucket, so the
 * expiry is semantically lossless and state stays bounded.
 *
 * @module
 */

import { z } from 'zod';
import type { Clock } from '../../clock.js';
import type { RateLimiter } from '../../ports/rate_limiter.js';
import type { TokenBucketOptions } from '../memory/rate_limiter.js';
import { tokenBucketBounds } from '../memory/rate_limiter.js';
import type { RedisEvalCommand } from './ttl.js';

/** Milliseconds in the second `ratePerSecond` is expressed in. */
const MS_PER_SECOND = 1000;

/**
 * The same bounds the in-memory limiter enforces.
 *
 * The two adapters share the "waits, never fails" contract, so they must reject the same
 * misconfiguration identically — the shape is imported rather than restated so they cannot drift.
 */
const tokenBucketSchema = z.object(tokenBucketBounds);

/** The one command this limiter needs. */
export type RedisRateLimiterClient = RedisEvalCommand;

// KEYS[1] = bucket hash; ARGV = now_ms, rate_per_ms, burst, ttl_ms. Returns 0 (token taken,
// proceed) or the ms until the next token becomes available. A negative elapsed (another
// pod's later write, clock skew) is clamped so the bucket never refills backwards.
const ACQUIRE_SCRIPT = `
local now_ms = tonumber(ARGV[1])
local rate_per_ms = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])
local state = redis.call('HMGET', KEYS[1], 'tokens', 'refill_ms')
local tokens = tonumber(state[1])
local refill_ms = tonumber(state[2])
if tokens == nil then
  tokens = burst
  refill_ms = now_ms
end
local elapsed = math.max(now_ms - refill_ms, 0)
tokens = math.min(burst, tokens + elapsed * rate_per_ms)
local wait_ms = 0
if tokens >= 1 then
  tokens = tokens - 1
else
  wait_ms = math.ceil((1 - tokens) / rate_per_ms)
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'refill_ms', now_ms)
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return wait_ms
`;

/** Fleet pacing shared by every pod that talks to the same Redis. */
export class RedisRateLimiter implements RateLimiter {
  private readonly redis: RedisRateLimiterClient;
  private readonly clock: Clock;
  private readonly ratePerMs: number;
  private readonly burst: number;
  private readonly ttlMs: number;

  public constructor(redis: RedisRateLimiterClient, clock: Clock, options: TokenBucketOptions) {
    // Fail fast on a misconfigured limiter rather than dividing by zero (or never refilling) on the
    // first contended acquire — the guard the in-memory limiter applies, from the same schema.
    const validated = tokenBucketSchema.parse(options);
    this.redis = redis;
    this.clock = clock;
    this.ratePerMs = validated.ratePerSecond / MS_PER_SECOND;
    this.burst = validated.burst;
    // Once a full refill's worth of time has passed, the stored state equals the absent-key
    // default (a full bucket), so expiring then loses nothing and bounds Redis growth.
    this.ttlMs = Math.ceil(this.burst / this.ratePerMs);
  }

  public async acquire(key: string): Promise<void> {
    // Re-run the script after every sleep: another session may have taken the token this one
    // slept for, in which case the script reports the next wait.
    for (;;) {
      const nowMs = this.clock.now().getTime();
      const waitMs = Number(
        await this.redis.eval(ACQUIRE_SCRIPT, {
          keys: [`ratelimit:${key}`],
          arguments: [String(nowMs), String(this.ratePerMs), String(this.burst), String(this.ttlMs)],
        }),
      );
      if (waitMs === 0) {
        return;
      }
      await this.clock.sleep(waitMs);
    }
  }
}
