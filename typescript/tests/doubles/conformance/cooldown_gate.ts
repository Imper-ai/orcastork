/**
 * CNF — the `CooldownGate` contract: atomic check-and-arm, durable per-key expiry.
 *
 * @module
 */

import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import type { CooldownGate } from '../../../src/orcastork/ports/index.js';
import type { ConformanceBinding, ConformanceHarness } from './shared.js';
import { DEFAULT_COOLDOWN_MS, pastWindow } from './shared.js';

/** The gate under contract, plus the harness controls. */
export interface CooldownGateHarness extends ConformanceHarness {
  readonly gate: CooldownGate;

  /**
   * The cooldown width this contract arms; {@link DEFAULT_COOLDOWN_MS} by default.
   *
   * A backend whose expiry runs on a real server's clock cannot be fast-forwarded, so its binding
   * names a short window and waits it out; every wait below is a multiple of this width, so the
   * contract is the same either way.
   */
  readonly cooldownMs?: number;
}

/** One cooldown-gate adapter bound to the contract. */
export type CooldownGateBinding = ConformanceBinding<CooldownGateHarness>;

/** Run the whole `CooldownGate` contract against one adapter. */
export const describeCooldownGateConformance = (binding: CooldownGateBinding): void => {
  describe(binding.name, () => {
    let harness: CooldownGateHarness;
    let gate: CooldownGate;
    let cooldownMs: number;

    beforeEach(async () => {
      harness = await binding.create();
      gate = harness.gate;
      cooldownMs = harness.cooldownMs ?? DEFAULT_COOLDOWN_MS;
    });

    afterEach(async () => {
      await harness.close?.();
    });

    it('lets the first acquire win and arms the cooldown in the same step', async () => {
      expect(await gate.tryAcquire('k', cooldownMs)).toBe(true);
      expect(await gate.tryAcquire('k', cooldownMs)).toBe(false); // armed by the first call — no check-then-act gap
    });

    it('reopens once the cooldown has elapsed', async () => {
      expect(await gate.tryAcquire('k', cooldownMs)).toBe(true);
      await harness.advanceTime(pastWindow(cooldownMs));
      expect(await gate.tryAcquire('k', cooldownMs)).toBe(true);
    });

    it('keeps distinct keys independent', async () => {
      expect(await gate.tryAcquire('k1', cooldownMs)).toBe(true);
      expect(await gate.tryAcquire('k2', cooldownMs)).toBe(true);
    });
  });
};
