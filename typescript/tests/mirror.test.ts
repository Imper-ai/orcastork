/**
 * MIR — `SessionStateMirror`: sole-mutator local state with exact store parity.
 *
 * The mirror must reproduce the store's keyed-merge and change-set semantics EXACTLY (it replaces
 * the store on every hot-loop read), so the core tests here are parity tables: the same adversarial
 * batch sequences applied to a mirror and to the in-memory store directly must yield identical
 * snapshots, revisions and change-set answers at every revision. The orchestrator-level tests pin
 * the read budget: one full store read per run (the rehydrate), plus one change-set read per
 * distinct pre-rehydration watermark on resume — never per pass.
 */

import { describe, expect, it } from 'vitest';
import { InMemoryDataPointStore } from '../src/orcastork/adapters/memory/index.js';
import type { AnyDataPoint, DataPointView } from '../src/orcastork/datapoints/index.js';
import { identityKey, MergeKind } from '../src/orcastork/datapoints/index.js';
import { StaleEpochError, StateMirrorError } from '../src/orcastork/exceptions.js';
import type { Revision } from '../src/orcastork/ids.js';
import { Epoch, NamespaceId, OperatorId, SessionId, Revision as toRevision } from '../src/orcastork/ids.js';
import type { OperatorContext } from '../src/orcastork/operators/index.js';
import { Orchestrator, SessionStatus } from '../src/orcastork/orchestrator/index.js';
import { SessionStateMirror } from '../src/orcastork/orchestrator/mirror.js';
import type { ApplyResolvedOptions, ChangeSet } from '../src/orcastork/ports/index.js';
import { buildInMemoryRuntime, OrchestratorRuntime } from '../src/orcastork/runtime.js';
import { FakeClock } from './doubles/clock.js';
import {
  ChatAnswerDataPoint,
  DEFAULT_OP,
  EmailDataPoint,
  GeoDataPoint,
  IpDataPoint,
  ip,
  observed,
  personalEmail,
  RiskDataPoint,
  risk,
  T0,
  workEmail,
} from './doubles/datapoints.js';
import { makeOperator } from './doubles/operators.js';

const SID = SessionId('mir-session');
const NAMESPACE = NamespaceId('mir-namespace');
const EPOCH = Epoch(1);
const HOUR_MS = 60 * 60 * 1000;
const T1 = new Date(T0.getTime() + HOUR_MS);
const T2 = new Date(T0.getTime() + 2 * HOUR_MS);

const geo = (value: Record<string, number>, last: Date = T0): GeoDataPoint =>
  observed(GeoDataPoint, value, { by: DEFAULT_OP, first: T0, last });

/** Full observable state per identity: provenance + both timestamps. */
const stateOf = (view: DataPointView): Record<string, string> =>
  Object.fromEntries(
    view
      .all()
      .map((dataPoint) => [
        identityKey(dataPoint),
        `${dataPoint.retrievedBy}|${dataPoint.firstRetrieved.toISOString()}|${dataPoint.lastRetrieved.toISOString()}`,
      ]),
  );

/** Change-set rows by identity, carrying the merged payload's `lastRetrieved`. */
const idsOf = (points: readonly AnyDataPoint[]): Record<string, string> =>
  Object.fromEntries(points.map((dataPoint) => [identityKey(dataPoint), dataPoint.lastRetrieved.toISOString()]));

/** In-memory store counting full reads and resolved applies (pins the mirror's I/O budget). */
class CountingStore extends InMemoryDataPointStore {
  public snapshotReads = 0;
  public revisionReads = 0;
  public changeSetReads = 0;
  public applyCalls = 0;

  public override async snapshot(sessionId: SessionId): Promise<DataPointView> {
    this.snapshotReads += 1;
    return await super.snapshot(sessionId);
  }

  public override async revision(sessionId: SessionId): Promise<Revision> {
    this.revisionReads += 1;
    return await super.revision(sessionId);
  }

  public override async changeSetSince(sessionId: SessionId, since: Revision): Promise<ChangeSet> {
    this.changeSetReads += 1;
    return await super.changeSetSince(sessionId, since);
  }

  public override async applyResolved(sessionId: SessionId, options: ApplyResolvedOptions): Promise<Revision> {
    this.applyCalls += 1;
    return await super.applyResolved(sessionId, options);
  }
}

// Adversarial merge sequences: re-observations (newer, older, equal), intra-batch duplicates,
// mixed add+update batches, value coexistence, and a non-scalar (object) identity.
const PARITY_SEQUENCES: Readonly<Record<string, readonly (readonly AnyDataPoint[])[]>> = {
  newer_reobservation: [[workEmail('a@e.example', { last: T0 })], [workEmail('a@e.example', { last: T2 })]],
  older_reobservation_is_noop: [[workEmail('a@e.example', { last: T2 })], [workEmail('a@e.example', { last: T0 })]],
  equal_reobservation_is_noop: [[workEmail('a@e.example', { last: T1 })], [workEmail('a@e.example', { last: T1 })]],
  intra_batch_duplicate_add: [[workEmail('a@e.example', { last: T0 }), workEmail('a@e.example', { last: T2 })]],
  mixed_add_and_update_batch: [
    [workEmail('a@e.example', { last: T0 })],
    [workEmail('a@e.example', { last: T2 }), personalEmail('p@e.example', { last: T0 })],
    [ip('203.0.113.1', { last: T1 })],
  ],
  same_type_new_value_coexists: [
    [workEmail('a@e.example', { last: T0 }), workEmail('b@e.example', { last: T0 })],
    [workEmail('a@e.example', { last: T1 })],
  ],
  unhashable_dict_identity: [
    [geo({ lat: 1.0, lon: 2.0 })],
    [geo({ lon: 2.0, lat: 1.0 }, T2)], // key-order-insensitive identity → an update
    [geo({ lat: 3.0, lon: 4.0 })],
  ],
  interleaved_updates_across_batches: [
    [workEmail('a@e.example', { last: T0 }), ip('203.0.113.1', { last: T0 })],
    [ip('203.0.113.1', { last: T2 })],
    [workEmail('a@e.example', { last: T1 }), ip('203.0.113.1', { last: T1 })],
  ],
};

describe('SessionStateMirror', () => {
  for (const [name, batches] of Object.entries(PARITY_SEQUENCES)) {
    it(`merges exactly like the store under adversarial sequences: ${name}`, async () => {
      const reference = new InMemoryDataPointStore(); // the store's own keyed-merge is the contract
      const backing = new InMemoryDataPointStore();
      const mirror = new SessionStateMirror(backing, SID);
      await mirror.rehydrate();

      for (const batch of batches) {
        await reference.write(SID, batch, { epoch: EPOCH });
        await mirror.write(batch, { epoch: EPOCH });
      }

      expect(await mirror.revision(SID)).toBe(await reference.revision(SID));
      expect(stateOf(await mirror.snapshot(SID))).toEqual(stateOf(await reference.snapshot(SID)));
      // Written through.
      expect(stateOf(await backing.snapshot(SID))).toEqual(stateOf(await reference.snapshot(SID)));
      for (let revision = 0; revision <= (await reference.revision(SID)); revision += 1) {
        const expected = await reference.changeSetSince(SID, toRevision(revision));
        const local = await mirror.changeSetSince(SID, toRevision(revision));
        const durable = await backing.changeSetSince(SID, toRevision(revision));
        expect(idsOf(local.added)).toEqual(idsOf(expected.added));
        expect(idsOf(durable.added)).toEqual(idsOf(expected.added));
        expect(idsOf(local.updated)).toEqual(idsOf(expected.updated));
        expect(idsOf(durable.updated)).toEqual(idsOf(expected.updated));
      }
    });
  }

  it('answers pre-rehydration change sets exactly once it has been primed', async () => {
    const store = new InMemoryDataPointStore();
    await store.write(SID, [workEmail('a@e.example', { last: T0 })], { epoch: EPOCH }); // rev 1
    await store.write(SID, [ip('203.0.113.1', { last: T0 })], { epoch: EPOCH }); // rev 2
    await store.write(SID, [workEmail('a@e.example', { last: T1 })], { epoch: EPOCH }); // rev 3 — an update

    const mirror = new SessionStateMirror(store, SID);
    await mirror.rehydrate();
    for (let revision = 0; revision < 4; revision += 1) {
      await mirror.primeChangeBaseline(toRevision(revision)); // 3 is the rehydration point → no-op
    }

    await mirror.write([risk(0.5)], { epoch: EPOCH }); // rev 4 — a local add on top of rehydrated state
    // rev 5 — updates a pre-rehydration identity.
    await mirror.write([ip('203.0.113.1', { last: T2 })], { epoch: EPOCH });

    expect(await mirror.revision(SID)).toBe(await store.revision(SID));
    for (let revision = 0; revision <= (await store.revision(SID)); revision += 1) {
      const expected = await store.changeSetSince(SID, toRevision(revision));
      const actual = await mirror.changeSetSince(SID, toRevision(revision));
      expect(idsOf(actual.added), `added diverged at revision ${revision}`).toEqual(idsOf(expected.added));
      expect(idsOf(actual.updated), `updated diverged at revision ${revision}`).toEqual(idsOf(expected.updated));
    }
  });

  it('does no store I/O for local reads once it has rehydrated', async () => {
    const store = new CountingStore();
    const first = await store.write(SID, [workEmail('a@e.example', { last: T0 })], { epoch: EPOCH });
    await store.write(SID, [ip('203.0.113.1', { last: T0 })], { epoch: EPOCH });
    store.snapshotReads = 0;
    store.revisionReads = 0;
    store.changeSetReads = 0;

    const mirror = new SessionStateMirror(store, SID);
    await mirror.rehydrate();
    await mirror.primeChangeBaseline(first);
    for (let index = 0; index < 5; index += 1) {
      await mirror.write([risk(index)], { epoch: EPOCH });
      await mirror.snapshot(SID);
      await mirror.revision(SID);
      await mirror.changeSetSince(SID, first); // pre-rehydration → served from the primed baseline
      await mirror.changeSetSince(SID, toRevision(2)); // rehydration point → served from local stamps
    }

    expect(store.snapshotReads).toBe(1); // the rehydrate
    expect(store.revisionReads).toBe(1); // likewise
    expect(store.changeSetReads).toBe(1); // the one primed baseline; never re-read per query
  });

  it('propagates a stale epoch before the local copy is touched', async () => {
    const store = new InMemoryDataPointStore();
    await store.write(SID, [workEmail('a@e.example')], { epoch: Epoch(2) });
    const mirror = new SessionStateMirror(store, SID);
    await mirror.rehydrate();

    await expect(mirror.write([personalEmail('p@e.example')], { epoch: Epoch(1) })).rejects.toBeInstanceOf(
      StaleEpochError,
    );

    // Local copy untouched.
    expect(new Set((await mirror.snapshot(SID)).all().map((dataPoint) => dataPoint.value))).toEqual(
      new Set(['a@e.example']),
    );
    expect(await mirror.revision(SID)).toBe(1);
    // Store untouched (atomic).
    expect(new Set((await store.snapshot(SID)).all().map((dataPoint) => dataPoint.value))).toEqual(
      new Set(['a@e.example']),
    );
  });

  it('raises StateMirrorError on every contract violation', async () => {
    const store = new InMemoryDataPointStore();
    const unhydrated = new SessionStateMirror(store, SID);
    await expect(unhydrated.snapshot(SID)).rejects.toBeInstanceOf(StateMirrorError);
    await expect(unhydrated.write([workEmail()], { epoch: EPOCH })).rejects.toBeInstanceOf(StateMirrorError);

    // Revision 1, so revision 0 predates rehydration.
    await store.write(SID, [workEmail()], { epoch: EPOCH });
    const mirror = new SessionStateMirror(store, SID);
    await mirror.rehydrate();
    // A mirror serves exactly one session.
    await expect(mirror.snapshot(SessionId('mir-other-session'))).rejects.toBeInstanceOf(StateMirrorError);
    // Pre-rehydration and never primed → loud, not wrong.
    await expect(mirror.changeSetSince(SID, toRevision(0))).rejects.toBeInstanceOf(StateMirrorError);
  });

  it('splits write outcomes into added and updated per presented point', async () => {
    const mirror = new SessionStateMirror(new InMemoryDataPointStore(), SID);
    await mirror.rehydrate();

    const first = await mirror.write([workEmail('a@e.example', { last: T0 })], { epoch: EPOCH });
    expect(first.outcomes.map((outcome) => outcome.kind)).toEqual([MergeKind.ADDED]);

    const second = await mirror.write(
      [
        workEmail('a@e.example', { last: T2 }), // existing identity → updated
        personalEmail('p@e.example', { last: T0 }), // new identity → added
        personalEmail('p@e.example', { last: T0 }), // intra-batch re-observation → updated
      ],
      { epoch: EPOCH },
    );
    expect(second.outcomes.map((outcome) => outcome.kind)).toEqual([
      MergeKind.UPDATED,
      MergeKind.ADDED,
      MergeKind.UPDATED,
    ]);

    // An older re-observation merges into the existing entry (an UPDATED presentation) without
    // changing anything durably — the revision stays where it was.
    const third = await mirror.write([workEmail('a@e.example', { last: T0 })], { epoch: EPOCH });
    expect(third.outcomes.map((outcome) => outcome.kind)).toEqual([MergeKind.UPDATED]);
    expect(third.revision).toBe(second.revision);
  });

  it('lets an orchestrator run read the store once, not per pass', async () => {
    const clock = new FakeClock();
    const store = new CountingStore();
    const runtime = OrchestratorRuntime({ ...buildInMemoryRuntime(clock), store });
    let next = 0;
    // A bounded self-cycle (multiple passes + reruns) plus a multi-emission producer: plenty of
    // merges and re-plans, all of which must be served by the mirror, not store re-reads.
    const selfCycle = makeOperator('selfloop', {
      produces: [IpDataPoint],
      dependsOn: [IpDataPoint],
      rerunOnNewData: true,
      maxCycles: 3,
      debounceMs: 1_000,
      emitFactory: () => {
        next += 1;
        return [ip(`203.0.113.${next}`)];
      },
    });
    const chatty = makeOperator('chatty', {
      dependsOn: [EmailDataPoint],
      produces: [RiskDataPoint],
      emits: [RiskDataPoint.emit(0.1), RiskDataPoint.emit(0.2), RiskDataPoint.emit(0.3)],
    });

    const result = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [selfCycle, chatty],
      seed: [workEmail(), ip('seed')],
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    // The loop genuinely iterated.
    expect(result.operatorRuns.get(OperatorId('selfloop')) ?? 0).toBeGreaterThanOrEqual(2);
    expect(store.snapshotReads).toBe(1); // ONE full read per run — the mirror rehydrate
    expect(store.revisionReads).toBe(1); // likewise; never re-read per pass or per merge
    expect(store.changeSetReads).toBe(0); // a fresh session computes every delta locally
    expect((await store.snapshot(SID)).ofType(RiskDataPoint)).toHaveLength(3); // the work still all landed
  });

  it('primes each pre-rehydration watermark exactly once on resume', async () => {
    const clock = new FakeClock();
    const store = new CountingStore();
    const runtime = OrchestratorRuntime({ ...buildInMemoryRuntime(clock), store });
    // A predecessor ran the watcher (watermark at revision 1), then merged one more relevant
    // DataPoint (revision 2) and died; its lease expires.
    const epoch = await runtime.lock.acquire(SID);
    const seenByWatcher = await runtime.store.write(SID, [ip('203.0.113.1')], { epoch });
    await runtime.store.setWatermark(SID, OperatorId('watcher'), seenByWatcher, { epoch });
    await runtime.store.write(SID, [ip('203.0.113.2')], { epoch });
    clock.advance(31_000);

    const deltas: (readonly string[])[] = [];
    const watcher = makeOperator('watcher', {
      dependsOn: [IpDataPoint],
      produces: [ChatAnswerDataPoint],
      rerunOnNewData: true,
      debounceMs: 0,
      emitFactory: (ctx: OperatorContext) => {
        deltas.push([...ctx.delta.added].map((dataPoint) => String(dataPoint.value)).sort());
        return [ChatAnswerDataPoint.emit('seen')];
      },
    });
    // Count only the resumed run.
    store.snapshotReads = 0;
    store.revisionReads = 0;
    store.changeSetReads = 0;

    const result = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [watcher],
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect([...result.operatorRuns]).toEqual([[OperatorId('watcher'), 1]]);
    // The rerun's delta matches what the store itself would answer.
    expect(deltas).toEqual([['203.0.113.2']]);
    expect(store.snapshotReads).toBe(1); // the rehydrate
    expect(store.revisionReads).toBe(1);
    expect(store.changeSetReads).toBe(1); // exactly one primed baseline for the rehydrated watermark
  });
});
