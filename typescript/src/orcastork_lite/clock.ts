/**
 * Injected time source.
 *
 * All time-dependent logic (debounce windows, timeouts, retry backoff) reads the clock through
 * the {@link Clock} interface so tests can drive a deterministic fake instead of the wall clock.
 * Core code never reads `Date.now()` / `performance.now()` or calls `setTimeout` directly.
 *
 * Durations are **milliseconds** throughout (Python's `float` seconds); a parameter or field
 * carrying one ends in `Ms`.
 *
 * @module
 */

import { OrcastorkLiteError } from './exceptions.js';

/**
 * A sleep given up on because the caller's `AbortSignal` fired.
 *
 * It lives here rather than in `exceptions.ts` because it is part of the {@link Clock} contract
 * itself: an implementation that holds a real timer owes its caller this rejection, and a caller
 * that abandons a wait must be able to recognise its own abort rather than a genuine fault. Its
 * own copy, not a shared one: `orcastork_lite` never imports `orcastork`.
 */
export class SleepAbortedError extends OrcastorkLiteError {}

/** A source of wall-clock timestamps, a monotonic counter, and an awaitable sleep. */
export interface Clock {
  /** Current UTC time (DataPoint observation timestamps). */
  now(): Date;

  /** Monotonic milliseconds (debounce / backoff arithmetic). */
  monotonic(): number;

  /**
   * Wait `ms` of monotonic time — the engine's only wait.
   *
   * `signal` is how a caller that raced this sleep and lost gives it up: the implementation drops
   * whatever it is holding and rejects with {@link SleepAbortedError}. Python cancels the task it
   * raced; without the signal a lost race here leaves a real timer pending for the window's full
   * width. A clock that advances time synchronously has nothing pending and may ignore it.
   */
  sleep(ms: number, signal?: AbortSignal): Promise<void>;
}

/** Real clock — the production default. */
export class SystemClock implements Clock {
  public now(): Date {
    return new Date();
  }

  public monotonic(): number {
    return performance.now();
  }

  public sleep(ms: number, signal?: AbortSignal): Promise<void> {
    // The one place in the package a real timer is allowed: every other wait goes through a
    // `Clock`, which is exactly what makes a test able to replace this with fast-forwarded time.
    return new Promise<void>((resolve, reject) => {
      if (signal?.aborted === true) {
        reject(new SleepAbortedError('the sleep was abandoned before its timer was armed'));
        return;
      }
      const timer = setTimeout(resolve, ms);
      signal?.addEventListener(
        'abort',
        () => {
          clearTimeout(timer);
          reject(new SleepAbortedError('the sleep was abandoned'));
        },
        { once: true },
      );
    });
  }
}
