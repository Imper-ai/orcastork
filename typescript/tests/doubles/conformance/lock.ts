/**
 * CNF — the `SessionLock` contract: TTL lease (liveness), monotonic epoch (correctness), completion.
 *
 * Every time-dependent contract moves the backend's own clock through the harness, never the wall
 * clock, so the lease arithmetic is deterministic on every adapter.
 *
 * @module
 */

import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { LockHeldError, StaleEpochError } from '../../../src/orcastork/exceptions.js';
import type { SessionLock } from '../../../src/orcastork/ports/index.js';
import type { ConformanceBinding, ConformanceHarness } from './shared.js';
import { DEFAULT_LEASE_MS, OTHER_SID, pastWindow, SID, withinWindow } from './shared.js';

/** The lock under contract, plus the harness controls. */
export interface LockHarness extends ConformanceHarness {
  readonly lock: SessionLock;

  /**
   * The lease TTL the adapter under test was configured with; {@link DEFAULT_LEASE_MS} by default.
   *
   * A backend that expires leases on a real server's clock cannot be fast-forwarded, so its binding
   * configures a short lease and waits it out; every advance below is a multiple of this width, so
   * the contract is the same either way.
   */
  readonly ttlMs?: number;
}

/** One session-lock adapter bound to the contract. */
export type LockBinding = ConformanceBinding<LockHarness>;

/** Run the whole `SessionLock` contract against one adapter. */
export const describeLockConformance = (binding: LockBinding): void => {
  describe(binding.name, () => {
    let harness: LockHarness;
    let lock: SessionLock;
    let ttlMs: number;

    beforeEach(async () => {
      harness = await binding.create();
      lock = harness.lock;
      ttlMs = harness.ttlMs ?? DEFAULT_LEASE_MS;
    });

    afterEach(async () => {
      await harness.close?.();
    });

    it('grants the lock and mints a higher epoch on acquire', async () => {
      const first = await lock.acquire(SID);
      expect(await lock.isHeld(SID)).toBe(true);
      expect(first).toBeGreaterThan(0);
    });

    it('expires the lease after its TTL, letting a successor take over', async () => {
      await lock.acquire(SID);
      await harness.advanceTime(pastWindow(ttlMs));
      expect(await lock.isHeld(SID)).toBe(false);
      const second = await lock.acquire(SID); // a successor can take over
      expect(second).toBeGreaterThan(1);
    });

    it('extends the lease on renew', async () => {
      const epoch = await lock.acquire(SID);
      await harness.advanceTime(withinWindow(ttlMs));
      await lock.renew(SID, { epoch });
      await harness.advanceTime(withinWindow(ttlMs)); // more than a full TTL in total, but renewed midway
      expect(await lock.isHeld(SID)).toBe(true);
    });

    it('mints strictly increasing epochs across grants', async () => {
      const epochs: number[] = [];
      for (let grant = 0; grant < 3; grant += 1) {
        epochs.push(await lock.acquire(SID));
        await harness.advanceTime(pastWindow(ttlMs)); // let the lease expire so the next acquire succeeds
      }
      expect(epochs).toEqual([...epochs].sort((left, right) => left - right));
      expect(new Set(epochs).size).toBe(3);
    });

    it('excludes a second holder while the lease is live', async () => {
      await lock.acquire(SID);
      await expect(lock.acquire(SID)).rejects.toThrow(LockHeldError);
    });

    it('advances the mint only on a successful acquire', async () => {
      // A minted epoch means the lock was held at some point, so epoch 0 means "never started" — the
      // only signal separating a never-started session from one whose owner died (both show no live
      // lease). Callers rely on that (the manager refuses to resume an unstarted session), which a
      // mint advanced by a *failed* acquire would break: an acquire in progress would read as a
      // session already started.
      expect(await lock.currentEpoch(SID)).toBe(0);
      const epoch = await lock.acquire(SID);
      await expect(lock.acquire(SID)).rejects.toThrow(LockHeldError);
      expect(await lock.currentEpoch(SID)).toBe(epoch);
    });

    it('frees the lock for reuse on release', async () => {
      const epoch = await lock.acquire(SID);
      await lock.release(SID, { epoch });
      expect(await lock.isHeld(SID)).toBe(false);
      expect(await lock.acquire(SID)).toBeGreaterThan(epoch);
    });

    it('marks completion idempotently', async () => {
      expect(await lock.isComplete(SID)).toBe(false);
      const epoch = await lock.acquire(SID);
      await lock.markComplete(SID, { epoch });
      expect(await lock.isComplete(SID)).toBe(true);
      await lock.markComplete(SID, { epoch }); // re-marking under the same epoch is a no-op
      expect(await lock.isComplete(SID)).toBe(true);
    });

    it('keeps completion per session', async () => {
      const epoch = await lock.acquire(SID);
      await lock.markComplete(SID, { epoch });
      expect(await lock.isComplete(OTHER_SID)).toBe(false);
    });

    it('fences completion against a takeover', async () => {
      const stale = await lock.acquire(SID);
      await harness.advanceTime(pastWindow(ttlMs)); // the predecessor's lease expires
      await lock.acquire(SID); // a successor takes over, minting a higher epoch
      // The fenced predecessor cannot finalize.
      await expect(lock.markComplete(SID, { epoch: stale })).rejects.toThrow(StaleEpochError);
      expect(await lock.isComplete(SID)).toBe(false);
      await lock.markComplete(SID, { epoch: await lock.currentEpoch(SID) }); // the current holder marks it
      expect(await lock.isComplete(SID)).toBe(true);
    });

    it('re-opens a completed session on clearComplete, keeping the fence intact', async () => {
      // markComplete → isComplete true; clearComplete → isComplete false (re-openable). The epoch
      // counter is NOT reset: a subsequent acquire mints a strictly higher epoch so the re-open
      // write is fenced against any stale predecessor.
      const epoch = await lock.acquire(SID);
      await lock.markComplete(SID, { epoch });
      expect(await lock.isComplete(SID)).toBe(true);
      await lock.release(SID, { epoch });

      await lock.clearComplete(SID);
      expect(await lock.isComplete(SID)).toBe(false);

      // The next acquire must still mint a strictly higher epoch — fencing is intact after clear.
      expect(await lock.acquire(SID)).toBeGreaterThan(epoch);
    });

    it('clears completion idempotently', async () => {
      // Clearing a session that was never completed (or already cleared) is a no-op.
      await lock.clearComplete(SID);
      expect(await lock.isComplete(SID)).toBe(false); // no error, still false
    });
  });
};
