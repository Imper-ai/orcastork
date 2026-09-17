/**
 * AGG — aggregation phase: idempotency, OCC, contribution markers, dead-letter, backoff.
 *
 * The `DurableStore` port contract runs here against the in-memory adapter (and against Mongo in
 * that adapter's suite). The aggregation-phase behaviours of the Python original — everything that
 * needs the orchestrator to run a session — land with the engine port.
 */

import { describe, expect, it } from 'vitest';
import { InMemoryDataPointStore, InMemoryDurableStore } from '../src/orcastork/adapters/memory/index.js';
import { AggregateStatus, AggregationHelpers } from '../src/orcastork/aggregation/index.js';
import { DataPointView } from '../src/orcastork/datapoints/index.js';
import { OptimisticConcurrencyError, StaleEpochError } from '../src/orcastork/exceptions.js';
import type { SessionId } from '../src/orcastork/ids.js';
import { Epoch as toEpoch, OperatorId as toOperatorId, SessionId as toSessionId } from '../src/orcastork/ids.js';
import { CapabilityView, EffectGuard, InvocationDelta, OperatorContext } from '../src/orcastork/operators/index.js';
import { FakeClock } from './doubles/clock.js';
import { describeDurableStoreConformance } from './doubles/conformance/durable_store.js';
import { makeAggregator } from './doubles/operators.js';

describeDurableStoreConformance({
  name: 'InMemoryDurableStore',
  create: () =>
    Promise.resolve({
      durable: new InMemoryDurableStore(),
      // The durable store keeps no expiring state, so nothing in its contract waits on a clock.
      advanceTime: () => Promise.resolve(),
    }),
});

describe('AggregationHelpers', () => {
  const SID: SessionId = toSessionId('agg-session');
  const PROFILE = toOperatorId('profile');

  const helperOver = (
    durable: InMemoryDurableStore,
    options: { readonly epoch?: number; readonly isFinal?: boolean; readonly clock?: FakeClock } = {},
  ): AggregationHelpers =>
    new AggregationHelpers(durable, {
      sessionId: SID,
      operatorId: PROFILE,
      epoch: toEpoch(options.epoch ?? 1),
      clock: options.clock ?? new FakeClock(),
      isFinal: options.isFinal ?? true,
    });

  it('does not lose an update: an OCC upsert reads the current version and writes guarded by it', async () => {
    const durable = new InMemoryDurableStore();
    const sessionA = new AggregationHelpers(durable, {
      sessionId: toSessionId('a'),
      operatorId: PROFILE,
      epoch: toEpoch(1),
      clock: new FakeClock(),
      isFinal: true,
    });
    const sessionB = new AggregationHelpers(durable, {
      sessionId: toSessionId('b'),
      operatorId: PROFILE,
      epoch: toEpoch(1),
      clock: new FakeClock(),
      isFinal: true,
    });

    await sessionA.upsert('profiles', 'profile', { a: 1 }); // version 1
    await sessionB.upsert('profiles', 'profile', { a: 1, b: 2 }); // reads v1, writes v2 — no lost update

    const document = await durable.read('profiles', 'profile');
    expect(document?.version).toBe(2);
    expect(document?.document).toEqual({ a: 1, b: 2 });

    // A stale-version write conflicts rather than clobbering.
    await expect(
      durable.upsert('profiles', 'profile', {}, { expectedVersion: 1, epoch: toEpoch(1) }),
    ).rejects.toBeInstanceOf(OptimisticConcurrencyError);
  });

  it('forwards its bound epoch through addToSet and returns the set size', async () => {
    // The helper is the aggregator-facing set-cardinality surface, and it injects its bound epoch so
    // the write fences exactly like upsert. A predecessor bound to a lower epoch must be rejected
    // once a higher epoch has mutated the same (table, key) — proving the helper threads its own
    // epoch through, not a hardcoded or defaulted value — while a fresh add returns the true size.
    const durable = new InMemoryDurableStore();
    const helper = helperOver(durable, { epoch: 1 });

    expect(await helper.addToSet('profiles', 'profile', 'sessions', 'session-1')).toBe(1); // size, not a count
    expect(await helper.addToSet('profiles', 'profile', 'sessions', 'session-2')).toBe(2); // the union grows

    await durable.addToSet('profiles', 'profile', 'sessions', 'session-3', { epoch: toEpoch(2) }); // takeover

    await expect(helper.addToSet('profiles', 'profile', 'sessions', 'session-4')).rejects.toBeInstanceOf(
      StaleEpochError,
    );
  });

  it('dedups markContribution and stamps it under the bound epoch', async () => {
    // markContribution must forward the bound (sessionId, operatorId, epoch): the first call newly
    // marks, the second is a stable false (at-most-once dedup), and the mark advances the session's
    // contribution fence to the bound epoch — so a later lower-epoch marker for a DIFFERENT operator
    // is rejected as stale. That last assertion is what pins the *epoch* (not just the ids) through.
    const durable = new InMemoryDurableStore();
    const helper = helperOver(durable, { epoch: 2 });

    expect(await helper.markContribution()).toBe(true); // newly recorded under the bound (session, operator)
    expect(await helper.markContribution()).toBe(false); // already recorded — at-most-once
    expect(await durable.isContributionMarked(SID, PROFILE)).toBe(true); // stamped for the bound operator

    await expect(durable.markContribution(SID, toOperatorId('other'), { epoch: toEpoch(1) })).rejects.toBeInstanceOf(
      StaleEpochError,
    );
  });

  it('stamps status and updatedAt, and the finalize pass re-stamps both', async () => {
    const durable = new InMemoryDurableStore();
    const clock = new FakeClock();

    await helperOver(durable, { clock, isFinal: false }).upsert('reports', 'k', { score: 1 });
    const interim = await durable.read('reports', 'k');
    expect(interim?.document).toEqual({ score: 1 });
    expect(interim?.status).toBe(AggregateStatus.IN_PROGRESS);
    expect(interim?.updatedAt).toEqual(clock.now());
    const interimStamp = interim?.updatedAt;

    clock.advance(60_000); // the finalize pass re-stamps updatedAt, so it must move forward
    await helperOver(durable, { clock, isFinal: true }).upsert('reports', 'k', { score: 2 });

    const finalized = await durable.read('reports', 'k');
    expect(finalized?.status).toBe(AggregateStatus.FINAL);
    expect(finalized?.updatedAt).toEqual(clock.now());
    expect(finalized?.updatedAt).not.toEqual(interimStamp);
  });

  it('refuses a non-final write over an already-final record, keeping its version', async () => {
    // `final` is terminal for an aggregator's output. A resumed/reopened epoch re-runs an
    // `interimRefresh` aggregator during gathering while its finalize pass is skipped as
    // already-contributed — so without this guard the record is walked back to `in_progress` with
    // nothing left to restore it, and every consumer that waits for `final` reads a finished
    // session as having produced nothing.
    const durable = new InMemoryDurableStore();
    const clock = new FakeClock();
    const finalVersion = await helperOver(durable, { clock, isFinal: true }).upsert('reports', 'k', { score: 2 });

    clock.advance(60_000);
    const returned = await helperOver(durable, { clock, isFinal: false }).upsert('reports', 'k', { score: 1 });

    expect(returned).toBe(finalVersion); // the record's current version, because nothing was written
    const stored = await durable.read('reports', 'k');
    expect(stored?.status).toBe(AggregateStatus.FINAL);
    expect(stored?.document).toEqual({ score: 2 });
    expect(stored?.version).toBe(finalVersion);
  });
});

describe('the aggregator declarations', () => {
  it('defaults ctx.isFinal to false', () => {
    const ctx = new OperatorContext({
      sessionId: toSessionId('ctx-session'),
      epoch: toEpoch(1),
      store: new DataPointView(),
      capabilities: new CapabilityView(),
      delta: InvocationDelta({ added: [], updated: [], newlyAvailableCaps: [], isFirstInvocation: true }),
      effects: new EffectGuard(new InMemoryDataPointStore(), {
        sessionId: toSessionId('ctx-session'),
        operatorId: toOperatorId('ctx-op'),
        epoch: toEpoch(1),
      }),
      signal: new AbortController().signal,
    });

    expect(ctx.isFinal).toBe(false);
    expect(ctx.aggregation).toBeNull();
  });

  it('defaults interimRefresh to false and lets an aggregator set it', () => {
    expect(makeAggregator('plain_agg', { rerunOnNewData: false }).interimRefresh).toBe(false);
    expect(makeAggregator('live_agg', { rerunOnNewData: true, interimRefresh: true }).interimRefresh).toBe(true);
  });
});
