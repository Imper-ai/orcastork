import { describe, expect, it } from 'vitest';
import { SystemClock } from '../src/orcastork/clock.js';

describe('SystemClock', () => {
  it('reports wall-clock time as a Date', () => {
    const clock = new SystemClock();

    expect(clock.now()).toBeInstanceOf(Date);
    expect(clock.now().toISOString()).toMatch(/Z$/);
  });

  it('reports a monotonic counter in milliseconds that never goes backwards', () => {
    const clock = new SystemClock();

    const first = clock.monotonic();
    const second = clock.monotonic();

    expect(second).toBeGreaterThanOrEqual(first);
  });

  it('resolves its sleep', async () => {
    const clock = new SystemClock();

    await expect(clock.sleep(0)).resolves.toBeUndefined();
  });
});
