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

/** A source of wall-clock timestamps, a monotonic counter, and an awaitable sleep. */
export interface Clock {
  /** Current UTC time (DataPoint observation timestamps). */
  now(): Date;

  /** Monotonic milliseconds (debounce / backoff arithmetic). */
  monotonic(): number;

  /** Wait `ms` of monotonic time — the engine's only wait. */
  sleep(ms: number): Promise<void>;
}

/** Real clock — the production default. */
export class SystemClock implements Clock {
  public now(): Date {
    return new Date();
  }

  public monotonic(): number {
    return performance.now();
  }

  public sleep(ms: number): Promise<void> {
    // The one place in the package a real timer is allowed: every other wait goes through a
    // `Clock`, which is exactly what makes a test able to replace this with fast-forwarded time.
    return new Promise<void>((resolve) => {
      setTimeout(resolve, ms);
    });
  }
}
