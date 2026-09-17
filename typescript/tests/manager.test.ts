/**
 * MGR — the session manager's contracts.
 *
 * Only the pieces that do not need the manager itself are here yet: the `CooldownGate` contract,
 * which Python binds in this same file (`TestInMemoryCooldownGate`) because the gate is what backs
 * the manager's scheduling gate. Everything in Python's `tests/test_manager.py` that drives a
 * `SessionOrchestrationManager` lands with the manager port — ADD to this file rather than
 * recreating it.
 */

import { describe, expect, it } from 'vitest';
import { InMemoryCooldownGate } from '../src/orcastork/adapters/memory/index.js';
import { FakeClock } from './doubles/clock.js';
import { describeCooldownGateConformance } from './doubles/conformance/cooldown_gate.js';

describeCooldownGateConformance({
  name: 'InMemoryCooldownGate',
  create: () => {
    const clock = new FakeClock();
    return Promise.resolve({
      gate: new InMemoryCooldownGate(clock),
      advanceTime: (ms: number) => {
        clock.advance(ms);
        return Promise.resolve();
      },
    });
  },
});

describe('the in-memory cooldown gate', () => {
  it('holds a half-open window, so the exact expiry tie admits', async () => {
    // The window is [T, T+C): at exactly now === expiresAt the cooldown has lapsed and a new start
    // is admitted (and re-arms); one tick earlier it is still closed. With a FakeClock this exact
    // equality is reachable, so the off-by-one boundary must have a defined, tested outcome.
    const clock = new FakeClock();
    const gate = new InMemoryCooldownGate(clock);

    expect(await gate.tryAcquire('tie', 60_000)).toBe(true); // arms expiresAt = T0 + 60_000
    clock.advance(59_999);
    expect(await gate.tryAcquire('tie', 60_000)).toBe(false); // 59_999 < 60_000 — still inside the closed front
    clock.advance(1); // now === expiresAt exactly
    expect(await gate.tryAcquire('tie', 60_000)).toBe(true); // now === expiresAt admits (strict `<`, not `<=`)
  });
});
