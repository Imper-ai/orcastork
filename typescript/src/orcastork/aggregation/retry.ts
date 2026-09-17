/**
 * Bounded retry with jittered backoff, then dead-letter.
 *
 * An aggregator gets bounded retries (e.g. on an OCC version conflict) with a jittered exponential
 * backoff; after `maxAttempts` it is dead-lettered (the failure surfaces as
 * {@link AggregatorDeadLetteredError}) rather than wedging the session. The jitter is seeded for
 * reproducibility, and the sleeper is injected so tests run instantly.
 *
 * @module
 */

import { crc32 } from 'node:zlib';
import { z } from 'zod';
import { AggregatorDeadLetteredError } from '../exceptions.js';

/** Attempts a retry policy allows when it does not say otherwise. */
const DEFAULT_MAX_ATTEMPTS = 5;

/** The first backoff window, doubled per attempt (Python's `0.05` seconds). */
const DEFAULT_BASE_DELAY_MS = 50;

/** Multiplicative jitter applied to every backoff window, ±20%. */
const DEFAULT_JITTER = 0.2;

/** Bounded relaunch-on-failure with a jittered exponential backoff (`baseDelayMs * 2**attempt`). */
export interface RetryPolicy {
  /** How many attempts the failure budget holds, the first run included. */
  readonly maxAttempts: number;

  /** The first window, in milliseconds; attempt *n* waits `baseDelayMs * 2**n`. */
  readonly baseDelayMs: number;

  /** Multiplicative jitter, ±`jitter`, applied to each window. */
  readonly jitter: number;
}

/** What {@link RetryPolicy} is built from; every field has a default. */
export interface RetryPolicyInit {
  readonly maxAttempts?: number;
  readonly baseDelayMs?: number;
  readonly jitter?: number;
}

/**
 * The bounds a retry policy must satisfy, whether it is being built or accepted as a field.
 *
 * Exported so a policy that *holds* a retry policy validates it against the same bounds instead of
 * restating them.
 */
export const retryPolicyBounds = {
  maxAttempts: z.number().int().min(1),
  baseDelayMs: z.number().min(0),
  jitter: z.number().min(0).max(1),
};

const retryPolicySchema = z.object({
  maxAttempts: retryPolicyBounds.maxAttempts.default(DEFAULT_MAX_ATTEMPTS),
  baseDelayMs: retryPolicyBounds.baseDelayMs.default(DEFAULT_BASE_DELAY_MS),
  jitter: retryPolicyBounds.jitter.default(DEFAULT_JITTER),
});

/**
 * Build a {@link RetryPolicy}, failing fast on a misconfigured one.
 *
 * A wrong value here would otherwise dead-letter immediately (`maxAttempts` below 1 makes the loop
 * run zero times) or produce a negative, degenerate backoff schedule — both far from where they
 * were written. Validation pays at the trust boundary: a policy is authored once, by hand.
 */
export const RetryPolicy = (init: RetryPolicyInit = {}): RetryPolicy => Object.freeze(retryPolicySchema.parse(init));

/** A stable (cross-process) jitter seed derived from e.g. `(sessionId, operatorId)`. */
export const seedFor = (...parts: string[]): number => crc32(parts.join('|'));

/**
 * A deterministic pseudo-random source for one backoff schedule.
 *
 * Python seeds `random.Random(seed)`; JavaScript's `Math.random` cannot be seeded, so the package
 * carries this one small generator instead. Only the *property* matters — the same seed yields the
 * same schedule, different seeds spread two aggregators' retries apart — and nothing about a
 * backoff window is ever compared across languages or processes.
 */
const seededRandom = (seed: number): (() => number) => {
  let state = seed >>> 0;
  return (): number => {
    state = (state + 0x6d2b79f5) >>> 0;
    let mixed = Math.imul(state ^ (state >>> 15), state | 1);
    mixed ^= mixed + Math.imul(mixed ^ (mixed >>> 7), mixed | 61);
    return ((mixed ^ (mixed >>> 14)) >>> 0) / 4_294_967_296;
  };
};

/** Which schedule to draw — the seed that makes it reproducible. */
export interface BackoffOptions {
  readonly seed: number;
}

/** The jittered exponential backoff schedule, in milliseconds — deterministic for a given seed. */
export const backoffDelays = (policy: RetryPolicy, options: BackoffOptions): readonly number[] => {
  const random = seededRandom(options.seed);
  const delays: number[] = [];
  for (let attempt = 0; attempt < policy.maxAttempts; attempt += 1) {
    // `uniform(-jitter, jitter)`: a draw is taken even at zero jitter, so the schedule's shape does
    // not depend on whether jitter is configured.
    const jitter = -policy.jitter + random() * 2 * policy.jitter;
    delays.push(policy.baseDelayMs * 2 ** attempt * (1 + jitter));
  }
  return Object.freeze(delays);
};

/** Notified after each attempt with its 1-based number and the error it raised (`undefined` on success). */
export type OnAttempt = (attempt: number, error: unknown) => Promise<void> | void;

/** How {@link runWithRetry} paces and reports the attempts it makes. */
export interface RunWithRetryOptions {
  readonly policy: RetryPolicy;

  /** Seeds the jitter, so one caller's schedule is reproducible and two callers' differ. */
  readonly seed: number;

  /**
   * How a backoff window is waited out. Required, unlike Python's `asyncio.sleep` default: the
   * injected `Clock` is the only time source in core, and a hidden real-time sleep here would be a
   * window no test could fast-forward.
   */
  readonly sleep: (ms: number) => Promise<void>;

  /** The caller's hook for auditing each attempt. */
  readonly onAttempt?: OnAttempt;
}

/**
 * Run `operation`, retrying on any failure with jittered backoff; dead-letter after N.
 *
 * `onAttempt` (if given) is notified after every attempt with its 1-based number and the error it
 * raised (`undefined` on success).
 */
export const runWithRetry = async (operation: () => Promise<void>, options: RunWithRetryOptions): Promise<void> => {
  const { policy, seed, sleep, onAttempt } = options;
  const delays = backoffDelays(policy, { seed });
  let lastError: unknown;
  for (let attempt = 0; attempt < policy.maxAttempts; attempt += 1) {
    let failed = false;
    let attemptError: unknown;
    try {
      await operation();
    } catch (error) {
      // Aggregator retry boundary — bounded, then dead-lettered.
      failed = true;
      attemptError = error;
      lastError = error;
    }
    // The hook runs outside the try/catch so a failing hook propagates rather than being mistaken
    // for an operation failure — which would replay an already-successful operation's side effects.
    if (onAttempt !== undefined) {
      await onAttempt(attempt + 1, attemptError);
    }
    if (!failed) {
      return;
    }
    if (attempt < policy.maxAttempts - 1) {
      await sleep(delays[attempt] ?? 0);
    }
  }
  throw new AggregatorDeadLetteredError(`dead-lettered after ${policy.maxAttempts} attempts`, { cause: lastError });
};
