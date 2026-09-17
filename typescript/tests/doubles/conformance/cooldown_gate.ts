/**
 * CNF — the `CooldownGate` contract: atomic check-and-arm, durable per-key expiry.
 *
 * @module
 */

import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import type { CooldownGate } from '../../../src/orcastork/ports/index.js';
import type { ConformanceBinding, ConformanceHarness } from './shared.js';

/** The gate under contract, plus the harness controls. */
export interface CooldownGateHarness extends ConformanceHarness {
  readonly gate: CooldownGate;
}

/** One cooldown-gate adapter bound to the contract. */
export type CooldownGateBinding = ConformanceBinding<CooldownGateHarness>;

/** Run the whole `CooldownGate` contract against one adapter. */
export const describeCooldownGateConformance = (binding: CooldownGateBinding): void => {
  describe(binding.name, () => {
    let harness: CooldownGateHarness;
    let gate: CooldownGate;

    beforeEach(async () => {
      harness = await binding.create();
      gate = harness.gate;
    });

    afterEach(async () => {
      await harness.close?.();
    });

    it('lets the first acquire win and arms the cooldown in the same step', async () => {
      expect(await gate.tryAcquire('k', 60_000)).toBe(true);
      expect(await gate.tryAcquire('k', 60_000)).toBe(false); // armed by the first call — no check-then-act gap
    });

    it('reopens once the cooldown has elapsed', async () => {
      expect(await gate.tryAcquire('k', 60_000)).toBe(true);
      await harness.advanceTime(61_000);
      expect(await gate.tryAcquire('k', 60_000)).toBe(true);
    });

    it('keeps distinct keys independent', async () => {
      expect(await gate.tryAcquire('k1', 60_000)).toBe(true);
      expect(await gate.tryAcquire('k2', 60_000)).toBe(true);
    });
  });
};
