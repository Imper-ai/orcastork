/**
 * REPLAY — the session-replay harness over archived raw DataPoints.
 *
 * A recorded session's `ArchivedDataPoint`s are reconstructed and re-run through a flow on a fresh
 * in-memory runtime: same flow → same durable output (regression testing); different flow → new
 * aggregates re-derived from the raw signals without re-running collection (the design's
 * reprocessing promise, made executable).
 */

import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod';
import { InMemoryCapabilityCatalog } from '../src/orcastork/adapters/memory/index.js';
import { ArchivedDataPoint } from '../src/orcastork/archive/index.js';
import { ReplayError, UnknownDataPointTypeError } from '../src/orcastork/exceptions.js';
import type { FlowDefinitionInit } from '../src/orcastork/flow.js';
import { FlowDefinition } from '../src/orcastork/flow.js';
import type { OperatorRef } from '../src/orcastork/ids.js';
import { CapabilityId, Epoch, NamespaceId, OperatorId, SessionId } from '../src/orcastork/ids.js';
import type { OperatorClass, OperatorContext } from '../src/orcastork/operators/index.js';
import type { OrchestratorOptions } from '../src/orcastork/orchestrator/index.js';
import { SessionStatus } from '../src/orcastork/orchestrator/index.js';
import { buildInMemoryRuntime } from '../src/orcastork/runtime.js';
import { makeCapability } from './doubles/capabilities.js';
import { FakeClock } from './doubles/clock.js';
import { EmailDataPoint, RiskDataPoint, risk, T0, WorkEmailDataPoint, workEmail } from './doubles/datapoints.js';
import { makeAggregator, makeOperator } from './doubles/operators.js';

/**
 * Every orchestrator `replay.ts` spawned, captured by subclassing the class it imports.
 *
 * The port of Python's `monkeypatch.setattr(replay_module, 'Orchestrator', ...)`: a module's import
 * binding cannot be reassigned in ESM, so the module the replay imports is mocked with a subclass
 * that records each instance and otherwise behaves exactly like the real one.
 */
const capture = vi.hoisted(() => ({ spawned: [] as { readonly emissionQueueSize: number }[] }));

vi.mock('../src/orcastork/orchestrator/index.js', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/orcastork/orchestrator/index.js')>();
  class CapturingOrchestrator extends actual.Orchestrator {
    public constructor(options: OrchestratorOptions) {
      super(options);
      capture.spawned.push(this);
    }
  }
  return { ...actual, Orchestrator: CapturingOrchestrator };
});

const { Orchestrator } = await import('../src/orcastork/orchestrator/index.js');
const { replaySession } = await import('../src/orcastork/replay.js');

const NAMESPACE = NamespaceId('replay-namespace');
const SID = SessionId('replay-session');
const ORIGINAL_COLLECTOR = OperatorId('original_collector');

const flowOf = (
  operators: readonly OperatorClass[] = [],
  extras: Omit<FlowDefinitionInit, 'name' | 'operators'> = {},
): FlowDefinition => new FlowDefinition({ name: 'replay-flow', operators, ...extras });

const writeRiskCount = async (ctx: OperatorContext): Promise<void> => {
  expect(ctx.aggregation).not.toBeNull();
  await ctx.aggregation?.upsert('reports', 'report', { risk_count: ctx.store.ofType(RiskDataPoint).length });
};

const archivedEmail = (
  value = 'alice@work.example',
  options: { readonly first?: Date; readonly last?: Date; readonly by?: OperatorRef } = {},
): ArchivedDataPoint =>
  ArchivedDataPoint.fromDataPoint(
    workEmail(value, {
      first: options.first ?? T0,
      last: options.last ?? T0,
      by: options.by ?? ORIGINAL_COLLECTOR,
    }),
    { sessionId: SID, namespaceId: NAMESPACE, epoch: Epoch(1) },
  );

describe('replaySession', () => {
  it('reproduces the original durable output under the same flow', async () => {
    // Original session: collect → score → aggregate, live-archiving as it goes.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const scorer = makeOperator('scorer', {
      dependsOn: [EmailDataPoint],
      produces: [RiskDataPoint],
      emits: [risk(0.8)],
    });
    const report = makeAggregator('report', { dependsOn: [RiskDataPoint], onAggregate: writeRiskCount });
    const original = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [scorer, report],
      seed: [workEmail()],
    }).run();
    expect(original.status).toBe(SessionStatus.COMPLETED);
    const originalDocument = await runtime.durable.read('reports', 'report');
    expect(originalDocument).not.toBeNull();
    const archived = await runtime.archive.read(SID);
    expect(new Set(archived.map((entry) => entry.type))).toEqual(new Set(['work_email', 'risk']));

    const replayed = await replaySession(archived, { flow: flowOf([scorer, report]), clock: new FakeClock() });

    expect(replayed.result.status).toBe(SessionStatus.COMPLETED);
    const replayedDocument = await replayed.runtime.durable.read('reports', 'report');
    expect(replayedDocument).not.toBeNull();
    expect(replayedDocument?.document).toEqual(originalDocument?.document);
    // Every archived identity is back in the replayed store (the flow may add more on top).
    const replayedIdentities = replayed.dataPoints.map((dataPoint) => [dataPoint.type, dataPoint.value]);
    for (const entry of archived) {
      expect(replayedIdentities).toContainEqual([entry.type, entry.value]);
    }
  });

  it('re-derives a new aggregate without re-running collection', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const collectRuns = { count: 0 };

    const collector = makeOperator('collector', {
      dependsOn: [EmailDataPoint],
      produces: [RiskDataPoint],
      emitFactory: () => {
        collectRuns.count += 1;
        return [risk(0.4), risk(0.9)];
      },
    });
    const original = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [collector],
      seed: [workEmail()],
    }).run();
    expect(original.status).toBe(SessionStatus.COMPLETED);
    expect(collectRuns.count).toBe(1);
    const archived = await runtime.archive.read(SID);

    // A what-if flow: an aggregator that did not exist when the session ran; the collector is NOT
    // part of the replay flow — its raw signals arrive via the reconstructed seed.
    const newReport = makeAggregator('max_risk_report', {
      dependsOn: [RiskDataPoint],
      onAggregate: async (ctx) => {
        const risks = ctx.store.ofType(RiskDataPoint);
        await ctx.aggregation?.upsert('reports', 'max_risk', {
          max_risk: Math.max(...risks.map((dataPoint) => dataPoint.value)),
        });
      },
    });
    const replayed = await replaySession(archived, { flow: flowOf([newReport]) });

    expect(replayed.result.status).toBe(SessionStatus.COMPLETED);
    expect(collectRuns.count).toBe(1); // collection was not re-run
    expect(replayed.result.operatorRuns.has(OperatorId('collector'))).toBe(false);
    const document = await replayed.runtime.durable.read('reports', 'max_risk');
    expect(document).not.toBeNull();
    expect(document?.document).toEqual({ max_risk: 0.9 });
  });

  it('preserves the archived timestamps and provenance', async () => {
    const first = new Date('2026-03-01T00:00:00.000Z');
    const last = new Date('2026-03-02T00:00:00.000Z');
    const entry = archivedEmail('alice@work.example', { first, last });

    const replayed = await replaySession([entry], { flow: flowOf() });

    const emails = replayed.dataPoints.filter(
      (dataPoint): dataPoint is WorkEmailDataPoint => dataPoint instanceof WorkEmailDataPoint,
    );
    expect(emails).toHaveLength(1);
    expect(emails[0]?.firstRetrieved).toEqual(first);
    expect(emails[0]?.lastRetrieved).toEqual(last);
    expect(emails[0]?.retrievedBy).toBe(ORIGINAL_COLLECTOR);
  });

  it('propagates an unknown archived type', async () => {
    // The leaf this entry was archived under no longer ships — a meaningful replay failure.
    const entry = new ArchivedDataPoint({
      sessionId: SID,
      namespaceId: NAMESPACE,
      type: 'vanished_signal',
      value: 'whatever',
      retrievedBy: OperatorId('old_operator'),
      firstRetrieved: T0,
      lastRetrieved: T0,
      isPii: false,
      epoch: Epoch(1),
    });

    await expect(replaySession([entry], { flow: flowOf() })).rejects.toThrow(UnknownDataPointTypeError);
  });

  it('derives the session and namespace ids from the entries', async () => {
    const replayed = await replaySession([archivedEmail()], { flow: flowOf() });

    // The replayed state lives under the archived session id, and the audit trail carries the
    // archived namespace id — both were derived, not supplied.
    expect((await replayed.runtime.store.snapshot(SID)).all()).toEqual(replayed.dataPoints);
    expect(replayed.dataPoints).toHaveLength(1);
    const auditEntries = await replayed.runtime.audit.replay(SID);
    expect(auditEntries.length).toBeGreaterThan(0);
    expect(auditEntries.every((entry) => entry.namespaceId === NAMESPACE)).toBe(true);
  });

  it('refuses an empty archive without explicit ids', async () => {
    await expect(replaySession([], { flow: flowOf() })).rejects.toThrow(ReplayError);
    await expect(replaySession([], { flow: flowOf() })).rejects.toThrow(/sessionId and namespaceId/);
  });

  it('threads the flow emission-queue size to the orchestrator', async () => {
    capture.spawned.length = 0;

    const replayed = await replaySession([archivedEmail()], { flow: flowOf([], { emissionQueueSize: 1 }) });

    expect(replayed.result.status).toBe(SessionStatus.COMPLETED);
    expect(capture.spawned).toHaveLength(1);
    expect(capture.spawned[0]?.emissionQueueSize).toBe(1); // the flow-level bound reached the replay spawn
  });

  it('auto-permits the flow capabilities with empty credentials', async () => {
    // The default (no catalog) replay path must permit every flow capability and hand the activator
    // EMPTY credentials — a replayed flow reprocesses already-arrived data and must never reach for
    // a live backend. The standard suite only ever replays capability-free flows, so the loop over
    // the flow's capabilities runs on a non-empty set for the first time.
    const netcap = makeCapability('netcap', { dependsOn: [EmailDataPoint] });
    const consumer = makeOperator('consumer', {
      dependsOn: [EmailDataPoint],
      requires: [netcap],
      produces: [RiskDataPoint],
      emits: [risk(0.3)],
    });

    const replayed = await replaySession([archivedEmail()], {
      flow: flowOf([consumer], { capabilities: [netcap] }),
    });

    expect(replayed.result.status).toBe(SessionStatus.COMPLETED);
    // the required capability became available
    expect(replayed.result.operatorRuns.has(OperatorId('consumer'))).toBe(true);
    // the emission landed
    expect(
      replayed.dataPoints.some((dataPoint) => dataPoint instanceof RiskDataPoint && dataPoint.value === 0.3),
    ).toBe(true);
    // auto-permitted AND activated with empty credentials (no live backend)
    expect(netcap.activations).toEqual([{}]);
  });

  it('runs an empty session when the archive is empty but the ids are explicit', async () => {
    // The 0/empty boundary: entries is empty but both ids are supplied, so the ReplayError guard is
    // NOT tripped and the orchestrator runs to a well-defined empty completion over zero seed data.
    const replayed = await replaySession([], { flow: flowOf(), sessionId: SID, namespaceId: NAMESPACE });

    expect(replayed.result.status).toBe(SessionStatus.COMPLETED);
    expect(replayed.dataPoints).toEqual([]); // nothing was seeded, nothing was derived
    // The replayed durable state lives under the explicitly-supplied ids (not derived from the first
    // entry, which would be undefined on an empty archive).
    expect((await replayed.runtime.store.snapshot(SID)).all()).toEqual([]);
    expect(await replayed.runtime.lock.isComplete(SID)).toBe(true);
  });

  it('lets a caller-supplied restrictive catalog override the auto-permit', async () => {
    // When the caller injects a catalog, the auto-permit-all block is skipped and the supplied
    // catalog governs availability exactly as in a live run. A catalog that permits no capability
    // leaves the required netcap unavailable, so its dependent operator never launches.
    const netcap = makeCapability('netcap', { dependsOn: [EmailDataPoint] });
    const consumer = makeOperator('consumer', {
      dependsOn: [EmailDataPoint],
      requires: [netcap],
      produces: [RiskDataPoint],
      emits: [risk(0.3)],
    });
    const flow = flowOf([consumer], { capabilities: [netcap] });

    const forbidden = await replaySession([archivedEmail()], {
      flow,
      catalog: new InMemoryCapabilityCatalog({ permitted: [[NAMESPACE, []]] }),
    });

    // capability forbidden → never launched
    expect(forbidden.result.operatorRuns.has(OperatorId('consumer'))).toBe(false);
    // its emission is absent
    expect(forbidden.dataPoints.some((dataPoint) => dataPoint instanceof RiskDataPoint)).toBe(false);
    expect(netcap.activations).toEqual([]); // never activated under the restrictive catalog

    // Contrast: the default (auto-permit) run of the same flow DOES launch the consumer.
    const permitted = await replaySession([archivedEmail()], { flow });
    expect(permitted.result.operatorRuns.has(OperatorId('consumer'))).toBe(true);
  });

  it('threads a caller-supplied permissive catalog credentials through to activation', async () => {
    // The caller-governed branch must thread the injected catalog's credentials through to
    // activation (distinct from the auto-permit branch, which always supplies empty credentials).
    const netcap = makeCapability('netcap', { dependsOn: [EmailDataPoint] });
    const consumer = makeOperator('consumer', {
      dependsOn: [EmailDataPoint],
      requires: [netcap],
      produces: [RiskDataPoint],
      emits: [risk(0.3)],
    });
    const catalog = new InMemoryCapabilityCatalog({
      permitted: [[NAMESPACE, [CapabilityId('netcap')]]],
      credentials: [{ namespaceId: NAMESPACE, capabilityId: CapabilityId('netcap'), credentials: { token: 'live' } }],
    });

    const replayed = await replaySession([archivedEmail()], {
      flow: flowOf([consumer], { capabilities: [netcap] }),
      catalog,
    });

    expect(replayed.result.operatorRuns.has(OperatorId('consumer'))).toBe(true);
    // the injected catalog's credentials reached activation
    expect(netcap.activations).toEqual([{ token: 'live' }]);
  });

  it('surfaces the raw validation error of a schema-drifted value', async () => {
    // A still-registered leaf ('risk' binds a number) whose archived value drifted to an object hits
    // parseDataPoint's "known type, malformed payload" branch and re-raises the bare ZodError — it
    // propagates out of replaySession uncaught (NOT wrapped as UnknownDataPointTypeError/ReplayError,
    // which the doc comment's throws section names). This pins the currently-undocumented leak so a
    // future contract change is a deliberate, visible decision.
    const entry = new ArchivedDataPoint({
      sessionId: SID,
      namespaceId: NAMESPACE,
      type: 'risk',
      value: { x: 1 }, // schema drift: 'risk' binds a number, not an object
      retrievedBy: OperatorId('old_scorer'),
      firstRetrieved: T0,
      lastRetrieved: T0,
      isPii: false,
      epoch: Epoch(1),
    });

    await expect(replaySession([entry], { flow: flowOf() })).rejects.toThrow(z.ZodError);
    // And it is NOT mapped onto either documented replay exception.
    const raised = await replaySession([entry], { flow: flowOf() }).catch((error: unknown) => error);
    expect(raised).not.toBeInstanceOf(UnknownDataPointTypeError);
    expect(raised).not.toBeInstanceOf(ReplayError);
  });

  it('is equivalent to the original run and idempotent under re-replay', async () => {
    // SYSTEMIC: replay-equivalence + replay-of-a-replay idempotency. Replaying an archived session
    // under the SAME flow must reproduce the original run's durable documents, and replaying the
    // replay's own archive again must produce byte-identical durable output (a stable fixpoint).
    const runtime = buildInMemoryRuntime(new FakeClock());
    const scorer = makeOperator('scorer', {
      dependsOn: [EmailDataPoint],
      produces: [RiskDataPoint],
      emits: [risk(0.8)],
    });
    const report = makeAggregator('report', { dependsOn: [RiskDataPoint], onAggregate: writeRiskCount });
    const flow = flowOf([scorer, report]);
    const original = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [scorer, report],
      seed: [workEmail()],
    }).run();
    expect(original.status).toBe(SessionStatus.COMPLETED);
    const originalDocument = await runtime.durable.read('reports', 'report');
    expect(originalDocument).not.toBeNull();
    const archived = await runtime.archive.read(SID);

    const firstReplay = await replaySession(archived, { flow, clock: new FakeClock() });
    const firstDoc = await firstReplay.runtime.durable.read('reports', 'report');
    expect(firstDoc).not.toBeNull();
    expect(firstDoc?.document).toEqual(originalDocument?.document); // replay-equivalence

    // Re-replay the FIRST replay's own archive: a second derivation must land on the same document.
    const reArchived = await firstReplay.runtime.archive.read(SID);
    const secondReplay = await replaySession(reArchived, { flow, clock: new FakeClock() });
    const secondDoc = await secondReplay.runtime.durable.read('reports', 'report');
    expect(secondDoc).not.toBeNull();
    expect(secondDoc?.document).toEqual(firstDoc?.document); // idempotent under re-replay
  });
});
