/**
 * In-memory `CooldownGate` — per-key expiry decided against the injected clock.
 *
 * @module
 */

import type { Clock } from '../../clock.js';
import type { CooldownGate } from '../../ports/cooldown_gate.js';

/** Check-and-arm in one step, with the armed windows held in process memory. */
export class InMemoryCooldownGate implements CooldownGate {
  private readonly clock: Clock;
  private readonly expiresAt = new Map<string, number>();

  public constructor(clock: Clock) {
    this.clock = clock;
  }

  public async tryAcquire(key: string, cooldownMs: number): Promise<boolean> {
    const now = this.clock.monotonic();
    const armedUntil = this.expiresAt.get(key);
    if (armedUntil !== undefined && now < armedUntil) {
      return false;
    }
    this.expiresAt.set(key, now + cooldownMs);
    return true;
  }
}
