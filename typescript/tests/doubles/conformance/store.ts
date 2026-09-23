/**
 * CNF — the `DataPointStore` contract: keyed merge, monotonic revisions, effect marks, fenced meta.
 *
 * Written entirely against the port, so every store adapter is behaviourally interchangeable.
 *
 * @module
 */

import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { StaleEpochError } from '../../../src/orcastork/exceptions.js';
import { Epoch, Revision } from '../../../src/orcastork/ids.js';
import type { DataPointStore } from '../../../src/orcastork/ports/index.js';
import { EffectClaim } from '../../../src/orcastork/ports/index.js';
import { EmailDataPoint, observed, personalEmail, T0, TriggerDataPoint, workEmail } from '../datapoints.js';
import type { ConformanceBinding, ConformanceHarness } from './shared.js';
import { OP, OTHER_SID, SID, T1, T2 } from './shared.js';

/** The store under contract, plus the harness controls. */
export interface StoreHarness extends ConformanceHarness {
  readonly store: DataPointStore;
}

/** One store adapter bound to the contract. */
export type StoreBinding = ConformanceBinding<StoreHarness>;

/** Run the whole `DataPointStore` contract against one adapter. */
export const describeStoreConformance = (binding: StoreBinding): void => {
  describe(binding.name, () => {
    let harness: StoreHarness;
    let store: DataPointStore;

    beforeEach(async () => {
      harness = await binding.create();
      store = harness.store;
    });

    afterEach(async () => {
      await harness.close?.();
    });

    it('stores a DataPoint and hands it back in the snapshot', async () => {
      const point = workEmail('a@work.example');
      await store.write(SID, [point], { epoch: Epoch(1) });
      expect((await store.snapshot(SID)).all().some((stored) => stored.equals(point))).toBe(true);
    });

    it('merges a re-observed identity in place, bumping lastRetrieved and keeping the size', async () => {
      await store.write(SID, [workEmail('a@work.example', { last: T0 })], { epoch: Epoch(1) });
      await store.write(SID, [workEmail('a@work.example', { last: T2 })], { epoch: Epoch(1) });
      const snapshot = await store.snapshot(SID);
      expect(snapshot.size).toBe(1);
      expect(snapshot.all()[0]?.lastRetrieved).toEqual(T2);
    });

    it('advances the revision on every mutating write', async () => {
      const first = await store.write(SID, [workEmail('a@work.example')], { epoch: Epoch(1) });
      const second = await store.write(SID, [personalEmail('p@home.example')], { epoch: Epoch(1) });
      expect(second).toBeGreaterThan(first);
    });

    it('rejects a write from a stale epoch', async () => {
      await store.write(SID, [workEmail('a@work.example')], { epoch: Epoch(2) });
      await expect(store.write(SID, [personalEmail('p@home.example')], { epoch: Epoch(1) })).rejects.toThrow(
        StaleEpochError,
      );
    });

    it('returns every leaf when queried by an abstract intermediate', async () => {
      await store.write(SID, [workEmail('w@e.example'), personalEmail('p@e.example')], { epoch: Epoch(1) });
      expect((await store.snapshot(SID)).ofType(EmailDataPoint)).toHaveLength(2);
    });

    it('lets a new value of an existing type coexist with the old one', async () => {
      await store.write(SID, [workEmail('a@e.example'), workEmail('b@e.example')], { epoch: Epoch(1) });
      expect((await store.snapshot(SID)).all()).toHaveLength(2);
    });

    it('advances the revision on a re-observation and surfaces it as updated', async () => {
      const first = await store.write(SID, [workEmail('a@e.example', { last: T0 })], { epoch: Epoch(1) });
      const second = await store.write(SID, [workEmail('a@e.example', { last: T2 })], { epoch: Epoch(1) });
      expect(second).toBeGreaterThan(first);
      const change = await store.changeSetSince(SID, first);
      expect(change.updated.some((point) => point.type === 'work_email')).toBe(true);
      expect(change.added).toHaveLength(0);
    });

    it('fails a lower-epoch CAS without a partial write', async () => {
      await store.write(SID, [workEmail('a@e.example')], { epoch: Epoch(2) });
      await expect(store.write(SID, [personalEmail('b@e.example')], { epoch: Epoch(1) })).rejects.toThrow(
        StaleEpochError,
      );
      const values = (await store.snapshot(SID)).all().map((point) => point.value);
      expect(values).not.toContain('b@e.example');
    });

    it('splits a change set into added and updated', async () => {
      const base = await store.write(SID, [workEmail('a@e.example', { last: T0 })], { epoch: Epoch(1) });
      await store.write(SID, [workEmail('a@e.example', { last: T2 }), personalEmail('p@e.example')], {
        epoch: Epoch(1),
      });
      const change = await store.changeSetSince(SID, base);
      expect(new Set(change.added.map((point) => point.type))).toEqual(new Set(['personal_email']));
      expect(new Set(change.updated.map((point) => point.type))).toEqual(new Set(['work_email']));
    });

    it('stores an ephemeral DataPoint live and flags it', async () => {
      await store.write(SID, [observed(TriggerDataPoint, 'go', { by: OP })], { epoch: Epoch(1) });
      const stored = (await store.snapshot(SID)).all();
      expect(stored).toHaveLength(1);
      expect(stored[0]?.isEphemeral).toBe(true);
    });

    it('starts empty at the baseline revision', async () => {
      expect((await store.snapshot(SID)).all()).toHaveLength(0);
      expect(await store.revision(SID)).toBe(0);
    });

    it('acquires a claim, then reports a same-epoch duplicate as pending', async () => {
      expect(await store.claimEffect(SID, 'op:send-otp', { epoch: Epoch(1), reclaimStale: false })).toBe(
        EffectClaim.ACQUIRED,
      );
      expect(await store.getEffectState(SID, 'op:send-otp')).toBe('pending:1');
      const duplicate = await store.claimEffect(SID, 'op:send-otp', { epoch: Epoch(1), reclaimStale: false });
      expect(duplicate).toBe(EffectClaim.PENDING_SAME_EPOCH);
      expect(await store.getEffectState(SID, 'op:send-otp')).toBe('pending:1'); // the duplicate changed nothing
    });

    it('transitions a pending mark to committed, idempotently', async () => {
      await store.claimEffect(SID, 'op:send-otp', { epoch: Epoch(1), reclaimStale: false });
      await store.commitEffect(SID, 'op:send-otp', { epoch: Epoch(1) });
      expect(await store.getEffectState(SID, 'op:send-otp')).toBe('committed');
      await store.commitEffect(SID, 'op:send-otp', { epoch: Epoch(1) }); // re-committing is a safe no-op
      expect(await store.getEffectState(SID, 'op:send-otp')).toBe('committed');
      const claim = await store.claimEffect(SID, 'op:send-otp', { epoch: Epoch(1), reclaimStale: false });
      expect(claim).toBe(EffectClaim.ALREADY_COMMITTED);
    });

    it('keeps a committed effect across an epoch takeover', async () => {
      await store.claimEffect(SID, 'op:send-otp', { epoch: Epoch(1), reclaimStale: false });
      await store.commitEffect(SID, 'op:send-otp', { epoch: Epoch(1) });
      const successor = await store.claimEffect(SID, 'op:send-otp', { epoch: Epoch(2), reclaimStale: false });
      expect(successor).toBe(EffectClaim.ALREADY_COMMITTED); // a resume never re-fires a committed effect
    });

    it('reports or reclaims a stale pending mark per the flag', async () => {
      await store.claimEffect(SID, 'op:send-otp', { epoch: Epoch(1), reclaimStale: false });
      const observedClaim = await store.claimEffect(SID, 'op:send-otp', { epoch: Epoch(2), reclaimStale: false });
      expect(observedClaim).toBe(EffectClaim.PENDING_STALE_EPOCH);
      expect(await store.getEffectState(SID, 'op:send-otp')).toBe('pending:1'); // reporting leaves the mark intact
      const reclaimed = await store.claimEffect(SID, 'op:send-otp', { epoch: Epoch(2), reclaimStale: true });
      expect(reclaimed).toBe(EffectClaim.ACQUIRED);
      expect(await store.getEffectState(SID, 'op:send-otp')).toBe('pending:2'); // ownership moved to the reclaimer
    });

    it('reverts only this epoch own pending mark', async () => {
      // The owner releases its claim, freeing the key for a same-epoch re-claim.
      await store.claimEffect(SID, 'op:send-otp', { epoch: Epoch(1), reclaimStale: false });
      await store.revertEffect(SID, 'op:send-otp', { epoch: Epoch(1) });
      expect(await store.getEffectState(SID, 'op:send-otp')).toBeNull();
      expect(await store.claimEffect(SID, 'op:send-otp', { epoch: Epoch(1), reclaimStale: false })).toBe(
        EffectClaim.ACQUIRED,
      );
      // A non-owning (higher-epoch) revert leaves the predecessor's mark in place — the unknown
      // outcome stays visible for the recovery policy, never silently cleared.
      await store.revertEffect(SID, 'op:send-otp', { epoch: Epoch(2) });
      expect(await store.getEffectState(SID, 'op:send-otp')).toBe('pending:1');
    });

    it('never reverts a committed mark', async () => {
      await store.claimEffect(SID, 'op:send-otp', { epoch: Epoch(1), reclaimStale: false });
      await store.commitEffect(SID, 'op:send-otp', { epoch: Epoch(1) });
      await store.revertEffect(SID, 'op:send-otp', { epoch: Epoch(1) });
      expect(await store.getEffectState(SID, 'op:send-otp')).toBe('committed'); // the effect DID run
    });

    it('guards all three effect operations by epoch', async () => {
      await store.claimEffect(SID, 'op:send-otp', { epoch: Epoch(2), reclaimStale: false });
      await store.write(SID, [workEmail('a@work.example')], { epoch: Epoch(2) });
      await expect(store.claimEffect(SID, 'op:other', { epoch: Epoch(1), reclaimStale: false })).rejects.toThrow(
        StaleEpochError,
      );
      await expect(store.commitEffect(SID, 'op:send-otp', { epoch: Epoch(1) })).rejects.toThrow(StaleEpochError);
      await expect(store.revertEffect(SID, 'op:send-otp', { epoch: Epoch(1) })).rejects.toThrow(StaleEpochError);
      expect(await store.getEffectState(SID, 'op:other')).toBeNull(); // the rejected claim left nothing
      expect(await store.getEffectState(SID, 'op:send-otp')).toBe('pending:2'); // untouched by the fenced calls
    });

    it('keeps effect marks per session', async () => {
      expect(await store.claimEffect(SID, 'op:send-otp', { epoch: Epoch(1), reclaimStale: false })).toBe(
        EffectClaim.ACQUIRED,
      );
      expect(await store.claimEffect(OTHER_SID, 'op:send-otp', { epoch: Epoch(1), reclaimStale: false })).toBe(
        EffectClaim.ACQUIRED,
      );
      await store.commitEffect(SID, 'op:send-otp', { epoch: Epoch(1) });
      expect(await store.getEffectState(OTHER_SID, 'op:send-otp')).toBe('pending:1'); // the commit never crossed over
    });

    it('keeps distinct effect keys independent', async () => {
      expect(await store.claimEffect(SID, 'op-a:notify', { epoch: Epoch(1), reclaimStale: false })).toBe(
        EffectClaim.ACQUIRED,
      );
      expect(await store.claimEffect(SID, 'op-b:notify', { epoch: Epoch(1), reclaimStale: false })).toBe(
        EffectClaim.ACQUIRED,
      );
    });

    it('round-trips the session deadline, absent until set', async () => {
      expect(await store.getSessionDeadline(SID)).toBeNull();
      await store.setSessionDeadline(SID, T1, { epoch: Epoch(1) });
      expect(await store.getSessionDeadline(SID)).toEqual(T1);
    });

    it('guards the session deadline write by epoch', async () => {
      await store.write(SID, [workEmail('a@work.example')], { epoch: Epoch(2) });
      await expect(store.setSessionDeadline(SID, T1, { epoch: Epoch(1) })).rejects.toThrow(StaleEpochError);
      expect(await store.getSessionDeadline(SID)).toBeNull(); // the rejected write left nothing
    });

    it('keeps session deadlines per session', async () => {
      await store.setSessionDeadline(SID, T1, { epoch: Epoch(1) });
      expect(await store.getSessionDeadline(OTHER_SID)).toBeNull();
    });

    it('round-trips the flow fingerprint, absent until set, and rewrites it on drift', async () => {
      expect(await store.getFlowFingerprint(SID)).toBeNull();
      await store.setFlowFingerprint(SID, 'fp-original', { epoch: Epoch(1) });
      expect(await store.getFlowFingerprint(SID)).toBe('fp-original');
      await store.setFlowFingerprint(SID, 'fp-drifted', { epoch: Epoch(2) }); // drift rewrites to the latest flow
      expect(await store.getFlowFingerprint(SID)).toBe('fp-drifted');
    });

    it('guards the flow fingerprint write by epoch', async () => {
      await store.write(SID, [workEmail('a@work.example')], { epoch: Epoch(2) });
      await expect(store.setFlowFingerprint(SID, 'fp-stale', { epoch: Epoch(1) })).rejects.toThrow(StaleEpochError);
      expect(await store.getFlowFingerprint(SID)).toBeNull(); // the rejected write left nothing
    });

    it('keeps flow fingerprints per session', async () => {
      await store.setFlowFingerprint(SID, 'fp-original', { epoch: Epoch(1) });
      expect(await store.getFlowFingerprint(OTHER_SID)).toBeNull();
    });

    it('allocates revisions in applyResolved exactly as write does', async () => {
      const first = await store.write(SID, [workEmail('a@e.example')], { epoch: Epoch(1) });
      const second = await store.applyResolved(SID, {
        added: [personalEmail('p@e.example')],
        updated: [],
        epoch: Epoch(1),
      });
      const third = await store.write(SID, [workEmail('b@e.example')], { epoch: Epoch(1) });
      expect(second).toBe(first + 1); // one revision per non-empty batch, same allocator as write
      expect(third).toBe(second + 1); // and write keeps allocating from the same sequence afterwards
      const values = new Set((await store.snapshot(SID)).all().map((point) => point.value));
      expect(values).toEqual(new Set(['a@e.example', 'p@e.example', 'b@e.example']));
    });

    it('stamps added and updated in applyResolved exactly as write does', async () => {
      const base = await store.write(SID, [workEmail('a@e.example', { last: T0 })], { epoch: Epoch(1) });
      const merged = workEmail('a@e.example', { last: T2 }); // the caller resolved the merge: timestamps final
      const applied = await store.applyResolved(SID, {
        added: [personalEmail('p@e.example')],
        updated: [merged],
        epoch: Epoch(1),
      });
      const change = await store.changeSetSince(SID, base);
      expect(new Set(change.added.map((point) => point.type))).toEqual(new Set(['personal_email']));
      expect(new Set(change.updated.map((point) => point.type))).toEqual(new Set(['work_email']));
      // Both were stamped at the batch revision, so nothing reads as added after it.
      expect((await store.changeSetSince(SID, applied)).added).toHaveLength(0);
      const snapshot = await store.snapshot(SID);
      expect(snapshot.size).toBe(2); // the update merged, never duplicated
      expect(snapshot.all().find((point) => point.type === 'work_email')?.lastRetrieved).toEqual(T2);
    });

    it('rejects a stale-epoch applyResolved with no partial write', async () => {
      const before = await store.write(SID, [workEmail('a@e.example', { last: T0 })], { epoch: Epoch(2) });
      await expect(
        store.applyResolved(SID, {
          added: [personalEmail('p@e.example')],
          updated: [workEmail('a@e.example', { last: T2 })],
          epoch: Epoch(1),
        }),
      ).rejects.toThrow(StaleEpochError);
      const snapshot = await store.snapshot(SID);
      const values = new Set(snapshot.all().map((point) => point.value));
      expect(values).toEqual(new Set(['a@e.example'])); // the added identity never landed
      expect(snapshot.all()[0]?.lastRetrieved).toEqual(T0); // the update never landed either
      expect(await store.revision(SID)).toBe(before); // no revision was burned by the rejected batch
    });

    it('keeps the revision on an empty applyResolved batch and still fences', async () => {
      const current = await store.write(SID, [workEmail('a@e.example')], { epoch: Epoch(2) });
      expect(await store.applyResolved(SID, { added: [], updated: [], epoch: Epoch(2) })).toBe(current);
      expect(await store.revision(SID)).toBe(current); // an all-no-op batch allocates nothing
      // Fencing still applies on the empty path, like write.
      await expect(store.applyResolved(SID, { added: [], updated: [], epoch: Epoch(1) })).rejects.toThrow(
        StaleEpochError,
      );
    });

    it('advances the fence from an effect claim, for every later guarded write', async () => {
      // A session whose FIRST mutation is an effect claim must still fence lower-epoch writers:
      // every guarded write records the highest accepted epoch (ownership is the lock's job; the
      // recorded fence is what completes stale-writer rejection).
      await store.claimEffect(SID, 'op:send-otp', { epoch: Epoch(5), reclaimStale: false });
      await expect(store.setWatermark(SID, OP, Revision(1), { epoch: Epoch(4) })).rejects.toThrow(StaleEpochError);
      await expect(store.setSessionDeadline(SID, T1, { epoch: Epoch(4) })).rejects.toThrow(StaleEpochError);
      expect(await store.getWatermark(SID, OP)).toBeNull(); // the fenced writes left nothing
      expect(await store.getSessionDeadline(SID)).toBeNull();
    });

    it('advances the fence from a fingerprint write, for every later guarded write', async () => {
      await store.setFlowFingerprint(SID, 'fp-first', { epoch: Epoch(5) });
      await expect(store.setSessionDeadline(SID, T1, { epoch: Epoch(4) })).rejects.toThrow(StaleEpochError);
      await expect(store.write(SID, [workEmail('a@e.example')], { epoch: Epoch(4) })).rejects.toThrow(StaleEpochError);
      expect((await store.snapshot(SID)).all()).toHaveLength(0); // the fenced write left nothing
    });
  });
};
