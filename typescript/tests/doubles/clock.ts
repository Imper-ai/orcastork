/**
 * `FakeClock` — a deterministic {@link Clock}.
 *
 * Time only moves when a test advances it, so debounce windows, timeouts, lock TTLs and backoff
 * are exercised without any wall-clock dependency.
 *
 * It satisfies both packages' `Clock` interfaces. They are identical by construction and
 * structurally interchangeable, so one double serves both suites — without either package ever
 * importing the other.
 *
 * @module
 */

import type { Clock } from '../../src/orcastork/clock.js';
import type { Clock as LiteClock } from '../../src/orcastork_lite/clock.js';

/** The instant a `FakeClock` starts at unless a test says otherwise. */
export const FAKE_CLOCK_START = new Date('2026-01-01T00:00:00.000Z');

/** A clock that moves only when a test moves it. */
export class FakeClock implements Clock, LiteClock {
  private current: Date;
  private elapsedMs = 0;

  public constructor(start: Date = FAKE_CLOCK_START) {
    this.current = new Date(start.getTime());
  }

  /** A copy, not the instance: `Date` is mutable, and a caller must not be able to move the clock. */
  public now(): Date {
    return new Date(this.current.getTime());
  }

  public monotonic(): number {
    return this.elapsedMs;
  }

  /** Move both the wall clock and the monotonic counter forward by `ms`. */
  public advance(ms: number): void {
    this.current = new Date(this.current.getTime() + ms);
    this.elapsedMs += ms;
  }

  /** Deterministic sleep: fast-forward the clock instead of waiting on the wall clock. */
  public async sleep(ms: number): Promise<void> {
    this.advance(ms);
    // Yield once, so tasks running concurrently with the sleeper interleave — the counterpart of
    // the Python double's `await asyncio.sleep(0)`.
    await new Promise<void>((resolve) => {
      setImmediate(resolve);
    });
  }
}
