/**
 * Injected time source.
 *
 * All time-dependent logic (debounce windows, timeouts, lock TTLs, backoff) reads the clock
 * through the {@link Clock} interface so tests can drive a deterministic `FakeClock` instead of
 * the wall clock. Core code must never read `Date.now()` / `performance.now()` or call
 * `setTimeout` directly.
 *
 * Durations are **milliseconds** throughout the port (Python's `float` seconds); a parameter or
 * field carrying one ends in `Ms`.
 *
 * @module
 */

/** A source of wall-clock timestamps, a monotonic counter, and an awaitable sleep. */
export interface Clock {
  /** Current UTC time (used for DataPoint timestamps, audit). */
  now(): Date;

  /** Monotonic milliseconds (used for debounce/timeout/backoff arithmetic). */
  monotonic(): number;

  /** Wait `ms` of monotonic time (the engine's only wait — never a bare `setTimeout`). */
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
