/**
 * CNF — the `DurableStore` contract: OCC, table routing, set-add, contribution markers, fencing.
 *
 * Written entirely against the port, so every durable-store adapter is behaviourally
 * interchangeable.
 *
 * @module
 */

import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { OptimisticConcurrencyError, StaleEpochError } from '../../../src/orcastork/exceptions.js';
import { Epoch, OperatorId } from '../../../src/orcastork/ids.js';
import type { DurableStore } from '../../../src/orcastork/ports/index.js';
import type { ConformanceBinding, ConformanceHarness } from './shared.js';
import { OP, SID, TBL } from './shared.js';

/** The durable store under contract, plus the harness controls. */
export interface DurableStoreHarness extends ConformanceHarness {
  readonly durable: DurableStore;
}

/** One durable-store adapter bound to the contract. */
export type DurableStoreBinding = ConformanceBinding<DurableStoreHarness>;

/** Run the whole `DurableStore` contract against one adapter. */
export const describeDurableStoreConformance = (binding: DurableStoreBinding): void => {
  describe(binding.name, () => {
    let harness: DurableStoreHarness;
    let durable: DurableStore;

    beforeEach(async () => {
      harness = await binding.create();
      durable = harness.durable;
    });

    afterEach(async () => {
      await harness.close?.();
    });

    it('inserts a document at version 1 and reads it back', async () => {
      expect(await durable.upsert(TBL, 'k', { a: 1 }, { expectedVersion: 0, epoch: Epoch(1) })).toBe(1);
      const document = await durable.read(TBL, 'k');
      expect(document?.version).toBe(1);
      expect(document?.document).toEqual({ a: 1 });
    });

    it('accepts an update that carries the version it read', async () => {
      await durable.upsert(TBL, 'k', { a: 1 }, { expectedVersion: 0, epoch: Epoch(1) });
      expect(await durable.upsert(TBL, 'k', { a: 2 }, { expectedVersion: 1, epoch: Epoch(1) })).toBe(2);
    });

    it('conflicts on a stale version', async () => {
      await durable.upsert(TBL, 'k', { a: 1 }, { expectedVersion: 0, epoch: Epoch(1) });
      await expect(durable.upsert(TBL, 'k', { a: 9 }, { expectedVersion: 0, epoch: Epoch(1) })).rejects.toThrow(
        OptimisticConcurrencyError,
      );
    });

    it('routes the destination by table', async () => {
      // The same key in two different tables is two independent documents (destination by table).
      await durable.upsert('reports', 'k', { a: 1 }, { expectedVersion: 0, epoch: Epoch(1) });
      await durable.upsert('profiles', 'k', { a: 2 }, { expectedVersion: 0, epoch: Epoch(1) });
      expect((await durable.read('reports', 'k'))?.document).toEqual({ a: 1 });
      expect((await durable.read('profiles', 'k'))?.document).toEqual({ a: 2 });
    });

    it('adds a set member idempotently', async () => {
      await durable.addToSet(TBL, 'agg', 'sessions', 'session-1', { epoch: Epoch(1) });
      expect(await durable.addToSet(TBL, 'agg', 'sessions', 'session-1', { epoch: Epoch(1) })).toBe(1);
    });

    it('marks a contribution exactly once', async () => {
      expect(await durable.markContribution(SID, OP, { epoch: Epoch(1) })).toBe(true);
      expect(await durable.markContribution(SID, OP, { epoch: Epoch(1) })).toBe(false);
      expect(await durable.isContributionMarked(SID, OP)).toBe(true);
    });

    it('rejects an upsert from a stale epoch', async () => {
      await durable.upsert(TBL, 'k', { a: 1 }, { expectedVersion: 0, epoch: Epoch(2) });
      await expect(durable.upsert(TBL, 'k', { a: 2 }, { expectedVersion: 1, epoch: Epoch(1) })).rejects.toThrow(
        StaleEpochError,
      );
    });

    it('fences a contribution from a superseded predecessor', async () => {
      // A fenced predecessor (lower epoch) cannot record a contribution after a higher epoch took
      // over. The stale call uses a different operator that never marked, so it is the epoch fence
      // — not the at-most-once dedup — that rejects it.
      expect(await durable.markContribution(SID, OP, { epoch: Epoch(2) })).toBe(true);
      await expect(durable.markContribution(SID, OperatorId('cnf-fenced-op'), { epoch: Epoch(1) })).rejects.toThrow(
        StaleEpochError,
      );
    });

    it('grows the set cardinality it reports as distinct members arrive', async () => {
      // The dedup half (a re-added member keeps the size) is locked elsewhere; this locks the
      // counting half: a distinct member grows the set and the returned cardinality reflects the
      // true set size (an aggregator reads this to know it is the Nth contributor). A regression
      // that overwrote the set, returned a stale length, or always returned 1 would still pass the
      // idempotency test but fail here.
      expect(await durable.addToSet(TBL, 'agg', 'sessions', 'v1', { epoch: Epoch(1) })).toBe(1); // absent → first
      expect(await durable.addToSet(TBL, 'agg', 'sessions', 'v2', { epoch: Epoch(1) })).toBe(2); // union grows
      expect(await durable.addToSet(TBL, 'agg', 'sessions', 'v1', { epoch: Epoch(1) })).toBe(2); // no regrow
    });

    it('fences a set-add from a stale epoch and leaves the set untouched', async () => {
      // addToSet is a mutating write path, so it must be epoch-fenced exactly like upsert: a
      // superseded predecessor must not be able to mutate a set member after a higher epoch wrote
      // the same key. Without this, a fenced writer could split-brain the durable aggregate.
      await durable.addToSet(TBL, 'k', 'sessions', 'v1', { epoch: Epoch(2) });
      await expect(durable.addToSet(TBL, 'k', 'sessions', 'v2', { epoch: Epoch(1) })).rejects.toThrow(StaleEpochError);
      // The rejected write left the set untouched (still a single member).
      expect(await durable.addToSet(TBL, 'k', 'sessions', 'v1', { epoch: Epoch(2) })).toBe(1);
    });

    it('shares one fence between set-add and upsert on the same key', async () => {
      // addToSet and upsert mutate the same durable (table, key) — they must share one epoch fence
      // so neither can be superseded behind the other's back. A prior addToSet at epoch 2 fences a
      // lower-epoch upsert to the same key, and vice-versa: the two paths advance and consult one
      // fence per (table, key), not two independent ones. (This pins the cross-adapter decision:
      // the in-memory adapter fences per (table, key) and Mongo must match, so a set-doc write and
      // a versioned-doc write to the same key cannot diverge.)
      await durable.addToSet(TBL, 'k', 'sessions', 'v1', { epoch: Epoch(2) });
      await expect(durable.upsert(TBL, 'k', { a: 1 }, { expectedVersion: 0, epoch: Epoch(1) })).rejects.toThrow(
        StaleEpochError,
      );
      await durable.upsert(TBL, 'other', { a: 1 }, { expectedVersion: 0, epoch: Epoch(2) });
      await expect(durable.addToSet(TBL, 'other', 'sessions', 'v2', { epoch: Epoch(1) })).rejects.toThrow(
        StaleEpochError,
      );
    });

    it('spans distinct fields of one key with a single fence', async () => {
      // The fence scope is the whole (table, key), not (table, key, field): once a higher epoch has
      // mutated ANY set on a key, a lower-epoch write to a DIFFERENT field of that key is still
      // superseded. This is the stricter, split-brain-free semantics both adapters must agree on —
      // a per-field fence would let a fenced predecessor keep writing to sibling fields undetected.
      await durable.addToSet(TBL, 'agg', 'sessions', 'v1', { epoch: Epoch(2) });
      await expect(durable.addToSet(TBL, 'agg', 'namespaces', 'namespace-1', { epoch: Epoch(1) })).rejects.toThrow(
        StaleEpochError,
      );
    });

    it('advances the fencing epoch on an accepted write and fences strictly lower only', async () => {
      // Monotonic fencing epoch through a SUCCESSFUL versioned update: the stored epoch must move
      // forward on every accepted write, then reject any strictly-newer-was-seen writer while still
      // admitting a same-epoch resume (the boundary is >, not >=).
      expect(await durable.upsert(TBL, 'k', { a: 1 }, { expectedVersion: 0, epoch: Epoch(2) })).toBe(1);
      expect(await durable.upsert(TBL, 'k', { a: 2 }, { expectedVersion: 1, epoch: Epoch(3) })).toBe(2); // → 3
      await expect(
        // strictly lower than the bumped epoch → fenced
        durable.upsert(TBL, 'k', { a: 3 }, { expectedVersion: 2, epoch: Epoch(2) }),
      ).rejects.toThrow(StaleEpochError);
      // The equal-epoch boundary still succeeds: a same-epoch resume after the advance is not stale.
      expect(await durable.upsert(TBL, 'k', { a: 4 }, { expectedVersion: 2, epoch: Epoch(3) })).toBe(3);
    });

    it('re-marks under a higher epoch as a stable false that still advances the fence', async () => {
      // Re-marking an already-contributed operator is a stable False regardless of a forward epoch
      // move (at-most-once dedup, NOT a fence rejection). The higher-epoch re-mark still advances
      // the session contribution fence, so a later lower-epoch marker for a DIFFERENT operator is
      // then rejected as stale — proving the fence tracks the highest epoch seen across attempts.
      expect(await durable.markContribution(SID, OP, { epoch: Epoch(1) })).toBe(true);
      expect(await durable.markContribution(SID, OP, { epoch: Epoch(2) })).toBe(false); // dedup, not stale
      await expect(
        // the epoch-2 re-mark advanced the session fence past 1
        durable.markContribution(SID, OperatorId('cnf-other-op'), { epoch: Epoch(1) }),
      ).rejects.toThrow(StaleEpochError);
    });

    it('round-trips the status metadata beside the document', async () => {
      // status/updatedAt ride the record (next to version/epoch), not the business document.
      const stamp = new Date('2026-01-01T12:00:00.000Z');
      await durable.upsert(
        TBL,
        'k',
        { a: 1 },
        { expectedVersion: 0, epoch: Epoch(1), status: 'in_progress', updatedAt: stamp },
      );
      const document = await durable.read(TBL, 'k');
      expect(document?.document).toEqual({ a: 1 }); // the curated doc is untouched by stamping
      expect(document?.status).toBe('in_progress');
      expect(document?.updatedAt).toEqual(stamp);
    });

    it('leaves the status metadata absent when the write supplied none', async () => {
      await durable.upsert(TBL, 'k', { a: 1 }, { expectedVersion: 0, epoch: Epoch(1) });
      const document = await durable.read(TBL, 'k');
      expect(document?.status).toBeNull();
      expect(document?.updatedAt).toBeNull();
    });

    it('re-stamps the status on the update path', async () => {
      // The interim→final transition is a SECOND upsert to the same key (the version-guarded UPDATE
      // path, distinct from the first INSERT). The finalize re-run must flip status to 'final' and
      // re-stamp updatedAt — verified on every backend, since in-memory collapses insert/update into
      // one assignment and so cannot catch a backend-specific update-path regression.
      const first = new Date('2026-01-01T09:00:00.000Z');
      const second = new Date('2026-01-01T10:30:00.000Z');
      await durable.upsert(
        TBL,
        'k',
        { a: 1 },
        { expectedVersion: 0, epoch: Epoch(1), status: 'in_progress', updatedAt: first },
      );
      await durable.upsert(
        TBL,
        'k',
        { a: 2 },
        { expectedVersion: 1, epoch: Epoch(1), status: 'final', updatedAt: second },
      );
      const document = await durable.read(TBL, 'k');
      expect(document?.version).toBe(2);
      expect(document?.document).toEqual({ a: 2 });
      expect(document?.status).toBe('final');
      expect(document?.updatedAt).toEqual(second); // re-stamped on the update path
    });

    it('writes no versioned document for a set-add', async () => {
      // addToSet is membership-only: it writes a separate set record, never a versioned doc, so a
      // set-shaped aggregate exposes no status/updatedAt — only upsert stamps the status envelope.
      await durable.addToSet(TBL, 'agg', 'sessions', 'session-1', { epoch: Epoch(1) });
      expect(await durable.read(TBL, 'agg')).toBeNull();
    });

    it('never disturbs a versioned document through a set-add on the same key', async () => {
      // A per-key curated doc plus a contributor set on the SAME key share the (table, key) epoch
      // fence, but addToSet must never rewrite the versioned doc's document/version/status/updatedAt.
      const stamp = new Date('2026-01-01T12:00:00.000Z');
      await durable.upsert(
        TBL,
        'k',
        { a: 1 },
        { expectedVersion: 0, epoch: Epoch(1), status: 'in_progress', updatedAt: stamp },
      );
      await durable.addToSet(TBL, 'k', 'sessions', 'v1', { epoch: Epoch(1) });
      const document = await durable.read(TBL, 'k');
      expect(document?.document).toEqual({ a: 1 });
      expect(document?.version).toBe(1);
      expect(document?.status).toBe('in_progress');
      expect(document?.updatedAt).toEqual(stamp);
    });
  });
};
