/**
 * In-memory `RateLimiter` — a classic token bucket per key on the injected clock.
 *
 * Single-process only (the Redis adapter coordinates across pods); deterministic under a
 * `FakeClock` because both the refill arithmetic (`monotonic`) and the wait (`sleep`) go through
 * the injected clock.
 *
 * @module
 */

import { z } from 'zod';
import type { Clock } from '../../clock.js';
import type { RateLimiter } from '../../ports/rate_limiter.js';

/** Milliseconds in the second `ratePerSecond` is expressed in. */
const MS_PER_SECOND = 1000;

/**
 * The bounds a token bucket must satisfy, whichever backend runs it.
 *
 * Exported so the Redis limiter rejects the same misconfiguration identically: the two adapters
 * share the "waits, never fails" contract, and a zero/negative rate would never refill (hang)
 * while a sub-1 burst would divide by zero on the first contended acquire.
 */
export const tokenBucketBounds = {
  ratePerSecond: z.number().positive(),
  burst: z.number().int().min(1),
};

const tokenBucketSchema = z.object(tokenBucketBounds);

/** How a token bucket is paced: its steady-state rate and how much of it may be spent at once. */
export interface TokenBucketOptions {
  /** Tokens accrued per second; the steady-state pace of the key. */
  readonly ratePerSecond: number;

  /** Bucket capacity — the most actions that may fire back to back after an idle period. */
  readonly burst: number;
}

/** One key's bucket: how many tokens it holds and when that was last computed. */
interface Bucket {
  tokens: number;

  /** Monotonic time (ms) of the last refill computation. */
  refilledAt: number;
}

/** Fleet pacing for a single process — the test and local-run substrate. */
export class InMemoryRateLimiter implements RateLimiter {
  private readonly clock: Clock;
  private readonly ratePerSecond: number;
  private readonly burst: number;
  private readonly buckets = new Map<string, Bucket>();

  public constructor(clock: Clock, options: TokenBucketOptions) {
    // Fail fast on a misconfigured limiter rather than dividing by zero (or never refilling) on the
    // first contended acquire.
    const validated = tokenBucketSchema.parse(options);
    this.clock = clock;
    this.ratePerSecond = validated.ratePerSecond;
    this.burst = validated.burst;
  }

  public async acquire(key: string): Promise<void> {
    // Re-check after every sleep: a concurrent waiter may have taken the token this one slept for.
    for (;;) {
      const now = this.clock.monotonic();
      const bucket = this.bucketFor(key, now);
      const accrued = ((now - bucket.refilledAt) / MS_PER_SECOND) * this.ratePerSecond;
      bucket.tokens = Math.min(this.burst, bucket.tokens + accrued);
      bucket.refilledAt = now;
      if (bucket.tokens >= 1) {
        bucket.tokens -= 1;
        return;
      }
      await this.clock.sleep(((1 - bucket.tokens) / this.ratePerSecond) * MS_PER_SECOND);
    }
  }

  private bucketFor(key: string, now: number): Bucket {
    const known = this.buckets.get(key);
    if (known !== undefined) {
      return known;
    }
    const created: Bucket = { tokens: this.burst, refilledAt: now };
    this.buckets.set(key, created);
    return created;
  }
}
