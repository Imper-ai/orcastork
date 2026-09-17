/**
 * Bounded retry with jittered backoff, then dead-letter.
 *
 * The policy's bounds, the schedule the seed pins, and the attempt loop's contract with its hook —
 * everything about `aggregation/retry.ts` that does not need a session to run.
 */

import { describe, expect, it } from 'vitest';
import { ZodError } from 'zod';
import type { RetryPolicyInit } from '../src/orcastork/aggregation/index.js';
import { backoffDelays, RetryPolicy, runWithRetry, seedFor } from '../src/orcastork/aggregation/index.js';
import {
  AggregatorDeadLetteredError,
  OptimisticConcurrencyError,
  StaleEpochError,
} from '../src/orcastork/exceptions.js';

/** Tests never wait out a backoff window; the schedule itself is asserted separately. */
const noSleep = async (): Promise<void> => undefined;

describe('RetryPolicy', () => {
  it('defaults to five attempts on a 50 ms base with ±20% jitter, frozen', () => {
    const policy = RetryPolicy();

    expect(policy).toEqual({ maxAttempts: 5, baseDelayMs: 50, jitter: 0.2 });
    expect(Object.isFrozen(policy)).toBe(true);
  });

  it.each<{ readonly init: RetryPolicyInit; readonly field: string }>([
    { init: { maxAttempts: 0 }, field: 'maxAttempts' },
    { init: { maxAttempts: -1 }, field: 'maxAttempts' },
    { init: { baseDelayMs: -100 }, field: 'baseDelayMs' },
    { init: { baseDelayMs: -1_000 }, field: 'baseDelayMs' },
    { init: { jitter: 1.5 }, field: 'jitter' },
    { init: { jitter: -0.1 }, field: 'jitter' },
  ])('rejects a misconfigured $field at construction', ({ init, field }) => {
    // A misconfigured policy must fail fast where it is written rather than dead-lettering
    // immediately (maxAttempts < 1 makes the loop run zero times) or producing a degenerate
    // schedule. The error names the offending field.
    expect(() => RetryPolicy(init)).toThrow(ZodError);
    expect(() => RetryPolicy(init)).toThrow(new RegExp(field));
  });

  it.each<{ readonly label: string; readonly init: RetryPolicyInit }>([
    { label: 'the single-attempt boundary', init: { maxAttempts: 1 } },
    { label: 'an all-zero schedule', init: { baseDelayMs: 0 } },
    { label: 'no jitter at all', init: { jitter: 0 } },
    { label: 'full jitter, the inclusive upper bound', init: { jitter: 1 } },
  ])('accepts $label', ({ init }) => {
    expect(() => RetryPolicy(init)).not.toThrow();
  });
});

describe('the backoff schedule', () => {
  it('is exactly the un-jittered exponential when jitter is off', () => {
    const policy = RetryPolicy({ maxAttempts: 4, baseDelayMs: 50, jitter: 0 });

    expect(backoffDelays(policy, { seed: 1 })).toEqual([0, 1, 2, 3].map((attempt) => 50 * 2 ** attempt));
  });

  it('is all zeros for a zero base delay, however the jitter is armed', () => {
    const policy = RetryPolicy({ maxAttempts: 5, baseDelayMs: 0, jitter: 0.5 });

    expect(backoffDelays(policy, { seed: 1 })).toEqual([0, 0, 0, 0, 0]);
  });

  it('is jittered, deterministic for a seed, and bounded by the jitter', () => {
    const policy = RetryPolicy({ maxAttempts: 5, baseDelayMs: 50, jitter: 0.2 });
    const delays = backoffDelays(policy, { seed: 42 });

    expect(new Set(delays).size).toBeGreaterThan(1); // jittered — not a fixed schedule
    expect(delays).toEqual(backoffDelays(policy, { seed: 42 })); // deterministic for a seed
    expect(delays).not.toEqual([0, 1, 2, 3, 4].map((attempt) => 50 * 2 ** attempt));
    delays.forEach((delay, attempt) => {
      expect(delay).toBeGreaterThanOrEqual(0.8 * 50 * 2 ** attempt);
      expect(delay).toBeLessThanOrEqual(1.2 * 50 * 2 ** attempt);
    });
  });

  it('spreads two seeds apart, so two aggregators do not relaunch in lockstep', () => {
    const policy = RetryPolicy({ maxAttempts: 3, baseDelayMs: 50, jitter: 0.2 });

    expect(backoffDelays(policy, { seed: seedFor('session', 'a') })).not.toEqual(
      backoffDelays(policy, { seed: seedFor('session', 'b') }),
    );
  });
});

describe('seedFor', () => {
  it('derives a stable seed from its parts — the same one Python derives', () => {
    // CRC32 over `'|'.join(parts)`, so a session resumed by a worker of either language draws the
    // same schedule.
    expect(seedFor('s', 'op')).toBe(3_074_770_702);
    expect(seedFor('s', 'op')).toBe(seedFor('s', 'op'));
    expect(seedFor('s', 'op')).not.toBe(seedFor('s', 'other'));
  });
});

describe('runWithRetry', () => {
  it('retries a failing operation until it succeeds', async () => {
    let attempts = 0;
    const succeedsOnThird = async (): Promise<void> => {
      attempts += 1;
      if (attempts < 3) {
        throw new OptimisticConcurrencyError('version conflict');
      }
    };

    await runWithRetry(succeedsOnThird, {
      policy: RetryPolicy({ maxAttempts: 3, baseDelayMs: 0 }),
      seed: 1,
      sleep: noSleep,
    });

    expect(attempts).toBe(3);
  });

  it('dead-letters once the attempts are exhausted, keeping the last failure as the cause', async () => {
    const conflict = new OptimisticConcurrencyError('version conflict');
    const alwaysConflicts = async (): Promise<void> => {
      throw conflict;
    };

    const error = await runWithRetry(alwaysConflicts, {
      policy: RetryPolicy({ maxAttempts: 3, baseDelayMs: 0 }),
      seed: 1,
      sleep: noSleep,
    }).catch((reason: unknown) => reason);

    expect(error).toBeInstanceOf(AggregatorDeadLetteredError);
    expect((error as Error).message).toContain('3 attempts');
    expect((error as Error).cause).toBe(conflict);
  });

  it('waits out one backoff window between attempts, and none after the last', async () => {
    const slept: number[] = [];
    const policy = RetryPolicy({ maxAttempts: 3, baseDelayMs: 50, jitter: 0 });
    const alwaysFails = async (): Promise<void> => {
      throw new OptimisticConcurrencyError('version conflict');
    };

    await expect(
      runWithRetry(alwaysFails, {
        policy,
        seed: 1,
        sleep: async (ms: number): Promise<void> => {
          slept.push(ms);
        },
      }),
    ).rejects.toBeInstanceOf(AggregatorDeadLetteredError);

    expect(slept).toEqual(backoffDelays(policy, { seed: 1 }).slice(0, 2));
  });

  it('dead-letters on the first failure at the single-attempt boundary, without sleeping', async () => {
    // maxAttempts=1: the `attempt < maxAttempts - 1` guard is `0 < 0` == false, so a failing
    // operation dead-letters on the FIRST failure without ever sleeping the backoff.
    const slept: number[] = [];
    const alwaysFails = async (): Promise<void> => {
      throw new Error('boom');
    };

    await expect(
      runWithRetry(alwaysFails, {
        policy: RetryPolicy({ maxAttempts: 1 }),
        seed: 1,
        sleep: async (ms: number): Promise<void> => {
          slept.push(ms);
        },
      }),
    ).rejects.toBeInstanceOf(AggregatorDeadLetteredError);

    expect(slept).toEqual([]);
  });

  it('notifies the hook after every attempt with its 1-based number and that attempt failure', async () => {
    const seen: { readonly attempt: number; readonly failed: boolean }[] = [];
    let attempts = 0;
    const succeedsOnSecond = async (): Promise<void> => {
      attempts += 1;
      if (attempts < 2) {
        throw new OptimisticConcurrencyError('version conflict');
      }
    };

    await runWithRetry(succeedsOnSecond, {
      policy: RetryPolicy({ maxAttempts: 3, baseDelayMs: 0 }),
      seed: 1,
      sleep: noSleep,
      onAttempt: async (attempt, error) => {
        seen.push({ attempt, failed: error !== undefined });
      },
    });

    expect(seen).toEqual([
      { attempt: 1, failed: true },
      { attempt: 2, failed: false },
    ]);
  });

  it('lets a hook that fails on the successful attempt propagate, without replaying it', async () => {
    // The hook runs OUTSIDE the operation's try/catch and before the success return. A hook that
    // raises on the SUCCESS attempt (e.g. a per-attempt lease renew that finds the epoch fenced)
    // must propagate unchanged — not be swallowed, not be counted as an operation failure, and
    // above all not replay an already-successful operation (which would double-apply
    // non-idempotent effects).
    let operationCalls = 0;
    const succeedsOnce = async (): Promise<void> => {
      operationCalls += 1;
    };

    await expect(
      runWithRetry(succeedsOnce, {
        policy: RetryPolicy({ maxAttempts: 3, baseDelayMs: 0 }),
        seed: 1,
        sleep: noSleep,
        onAttempt: async (_attempt, error) => {
          if (error === undefined) {
            throw new StaleEpochError('lease fenced on the success attempt');
          }
        },
      }),
    ).rejects.toBeInstanceOf(StaleEpochError);

    expect(operationCalls).toBe(1);
  });

  it('surfaces a failing hook as the hook failure, never as a dead-letter', async () => {
    // A hook failure is the caller's concern, distinct from operation failure: it must not be
    // mistaken for an exhausted-retry dead-letter.
    const alwaysSucceeds = async (): Promise<void> => undefined;

    const error = await runWithRetry(alwaysSucceeds, {
      policy: RetryPolicy({ maxAttempts: 2, baseDelayMs: 0 }),
      seed: 1,
      sleep: noSleep,
      onAttempt: async () => {
        throw new StaleEpochError('fenced');
      },
    }).catch((reason: unknown) => reason);

    expect(error).toBeInstanceOf(StaleEpochError);
    expect(error).not.toBeInstanceOf(AggregatorDeadLetteredError);
  });
});
