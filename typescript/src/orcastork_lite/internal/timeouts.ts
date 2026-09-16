/**
 * Bounding a real await — the port of Python's `asyncio.wait_for`.
 *
 * A **timeout** is not a window. Debounce, retry backoff and the session deadline are arithmetic
 * on the injected `Clock`, so a test can fast-forward them; a timeout bounds something that is
 * genuinely in flight (an operator run, a capability activation, an event publish) and must
 * therefore run on real time, exactly as `wait_for` does. That is what lets a test drive a
 * `FakeClock` through a session while a 20 ms bound still fires on a stub that really hangs.
 *
 * One difference from Python is unavoidable: `wait_for` cancels the coroutine it was waiting on,
 * and a JavaScript promise cannot be cancelled. {@link withTimeout} stops *waiting*; whatever it
 * was waiting on keeps running unless the caller also aborts it (the orchestrator aborts the
 * context's `AbortSignal`, and a test stub ties its sleep to that signal).
 *
 * @module
 */

import { OrcastorkLiteError } from '../exceptions.js';

/** A bounded await did not finish inside its bound. The counterpart of Python's `TimeoutError`. */
export class OperationTimeoutError extends OrcastorkLiteError {}

/** The largest delay Node's timers accept; beyond it a `setTimeout` fires immediately instead. */
const MAX_TIMER_MS = 2_147_483_647;

/**
 * Await `operation`, rejecting with {@link OperationTimeoutError} if it takes longer than `ms`.
 *
 * The timer is `unref`'d and always cleared, so a bound that never fires cannot hold the process
 * open and a finished operation leaves nothing behind. A bound that is not a usable timer delay
 * (infinite, or longer than a timer can express) is treated as no bound at all rather than as a
 * timer that fires at once.
 */
export const withTimeout = async <T>(operation: PromiseLike<T>, ms: number): Promise<T> => {
  if (!Number.isFinite(ms) || ms > MAX_TIMER_MS) {
    return await operation;
  }
  let timer: ReturnType<typeof setTimeout> | undefined;
  const expiry = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => {
      reject(new OperationTimeoutError(`operation timed out after ${ms} ms`));
    }, ms);
    timer.unref();
  });
  try {
    // `race` subscribes to both, so a rejection arriving after the bound has fired is delivered to
    // a handler that ignores it rather than surfacing as an unhandled rejection.
    return await Promise.race([operation, expiry]);
  } finally {
    clearTimeout(timer);
  }
};
