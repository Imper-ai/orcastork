import { describe, expect, it } from 'vitest';
import { FAKE_CLOCK_START, FakeClock } from './clock.js';

describe('FakeClock', () => {
  it('starts at a fixed instant, so no test depends on the day it runs', () => {
    expect(new FakeClock().now().toISOString()).toBe('2026-01-01T00:00:00.000Z');
    expect(new FakeClock().now()).toEqual(FAKE_CLOCK_START);
  });

  it('starts wherever a test asks it to', () => {
    const clock = new FakeClock(new Date('2030-06-15T10:30:00.000Z'));

    expect(clock.now().toISOString()).toBe('2030-06-15T10:30:00.000Z');
  });

  it('starts its monotonic counter at zero', () => {
    expect(new FakeClock().monotonic()).toBe(0);
  });

  it('does not move on its own', () => {
    const clock = new FakeClock();
    const first = clock.now();

    expect(clock.now()).toEqual(first);
    expect(clock.monotonic()).toBe(0);
  });

  it('moves both the wall clock and the monotonic counter when advanced', () => {
    const clock = new FakeClock();

    clock.advance(1_500);

    expect(clock.now().toISOString()).toBe('2026-01-01T00:00:01.500Z');
    expect(clock.monotonic()).toBe(1_500);
  });

  it('hands out a copy, so a caller cannot move the clock by mutating what it read', () => {
    const clock = new FakeClock();

    clock.now().setUTCFullYear(1999);

    expect(clock.now().toISOString()).toBe('2026-01-01T00:00:00.000Z');
  });

  it('fast-forwards instead of waiting when it sleeps', async () => {
    const clock = new FakeClock();

    await clock.sleep(30_000);

    expect(clock.monotonic()).toBe(30_000);
    expect(clock.now().toISOString()).toBe('2026-01-01T00:00:30.000Z');
  });

  it('yields once while sleeping, so a task running concurrently with the sleeper interleaves', async () => {
    const clock = new FakeClock();
    const order: string[] = [];

    const sleeper = (async (): Promise<void> => {
      order.push('sleeper:before');
      await clock.sleep(1_000);
      order.push('sleeper:after');
    })();
    const other = (async (): Promise<void> => {
      order.push('other:before');
      await Promise.resolve();
      order.push('other:after');
    })();
    await Promise.all([sleeper, other]);

    expect(order).toEqual(['sleeper:before', 'other:before', 'other:after', 'sleeper:after']);
  });
});
