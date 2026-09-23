/**
 * The lite package's own `Clock` — its own copy, never the full package's.
 *
 * Only the behaviour that is not simply "reads the wall clock" is pinned here: a sleep that a
 * caller abandons must drop its timer, which is what keeps the scheduler's window races from
 * leaving one armed per pass.
 */

import { describe, expect, it } from 'vitest';
import { SleepAbortedError, SystemClock } from '../../src/orcastork_lite/clock.js';
import { FakeClock } from '../doubles/clock.js';

describe('an abandoned lite sleep', () => {
  it('gives up its timer the moment the signal aborts, and rejects with a lite framework error', async () => {
    // A caller that races a window and loses aborts the loser. A real timer would otherwise stay
    // armed for the window's full width — a minute-long window would hold the worker open for a
    // minute after the wait it belonged to had already ended.
    const clock = new SystemClock();
    const window = new AbortController();

    const sleeping = clock.sleep(60_000, window.signal);
    const rejection = expect(sleeping).rejects.toBeInstanceOf(SleepAbortedError);
    window.abort();

    await rejection; // promptly: the assertion resolves without the 60 s window elapsing
    await expect(clock.sleep(60_000, AbortSignal.abort())).rejects.toBeInstanceOf(SleepAbortedError);
  });

  it('is ignored by the FakeClock, which has no timer to give up', async () => {
    // The fake advances time synchronously, so nothing is ever pending; honouring the signal could
    // only turn an already-finished wait into a spurious rejection.
    const clock = new FakeClock();

    await expect(clock.sleep(60_000, AbortSignal.abort())).resolves.toBeUndefined();
    expect(clock.monotonic()).toBe(60_000);
  });
});
