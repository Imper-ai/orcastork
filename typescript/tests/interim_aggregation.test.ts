/**
 * Live interim aggregation — an opt-in aggregator scheduled during gathering.
 *
 * An `interimRefresh` aggregator rejoins the gather set so a reader polling the durable record sees
 * a live view while the session is still running, and the finalize pass remains the authoritative
 * one. The two invariants proven end to end here: `final` is terminal for an aggregator's output
 * (nothing an interim refold does afterwards may walk it back), and the coalescing window an interim
 * aggregator arms is never charged to the completion tail — but is still waited out before an
 * unbounded idle wait.
 */

import { describe, expect, it } from 'vitest';
import { InMemoryCapabilityCatalog, InMemorySessionLock } from '../src/orcastork/adapters/memory/index.js';
import type { AggregationHelpers } from '../src/orcastork/aggregation/index.js';
import { AggregateStatus } from '../src/orcastork/aggregation/index.js';
import { CompletionTailTimeoutError, StaleEpochError } from '../src/orcastork/exceptions.js';
import { FlowDefinition } from '../src/orcastork/flow.js';
import type { SessionId } from '../src/orcastork/ids.js';
import { NamespaceId, OperatorId, SessionId as toSessionId } from '../src/orcastork/ids.js';
import { Deferred } from '../src/orcastork/internal/deferred.js';
import { SessionOrchestrationManager } from '../src/orcastork/manager/index.js';
import type { OperatorContext } from '../src/orcastork/operators/index.js';
import { Orchestrator, SessionStatus } from '../src/orcastork/orchestrator/index.js';
import { buildInMemoryRuntime, OrchestratorRuntime } from '../src/orcastork/runtime.js';
import { FakeClock } from './doubles/clock.js';
import {
  ChatAnswerDataPoint,
  chatAnswer,
  IpDataPoint,
  ip,
  RiskDataPoint,
  risk,
  TriggerDataPoint,
} from './doubles/datapoints.js';
import { makeAggregator, makeOperator } from './doubles/operators.js';

const NAMESPACE = NamespaceId('namespace');
const REPORTS = 'reports';

/** In-memory lock whose renew always reports a takeover (models being fenced mid-run). */
class FenceOnRenewLock extends InMemorySessionLock {
  public override async renew(): Promise<void> {
    throw new StaleEpochError('a higher epoch took over');
  }
}

/** The helpers an aggregator context always carries — the port of Python's `assert` on it. */
const aggregationOf = (ctx: OperatorContext): AggregationHelpers => {
  if (ctx.aggregation === null) {
    throw new Error('an aggregator context always carries its aggregation helpers');
  }
  return ctx.aggregation;
};

const writeScore = async (ctx: OperatorContext): Promise<void> => {
  const count = ctx.store.ofType(RiskDataPoint).length;
  await aggregationOf(ctx).upsert(REPORTS, String(ctx.sessionId), { risk_count: count, is_final: ctx.isFinal });
};

const writeCounts = async (ctx: OperatorContext): Promise<void> => {
  const risks = ctx.store.ofType(RiskDataPoint).length;
  const chats = ctx.store.ofType(ChatAnswerDataPoint).length;
  await aggregationOf(ctx).upsert(REPORTS, String(ctx.sessionId), { total: risks + chats, is_final: ctx.isFinal });
};

describe('an interim-refresh aggregator', () => {
  it('writes an in_progress document during gathering', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const report = makeAggregator('report', {
      dependsOn: [RiskDataPoint],
      rerunOnNewData: true,
      interimRefresh: true,
      onAggregate: writeScore,
    });
    // parkAfterMs: 0 makes the session park immediately once it reaches the inbox wait, so the test
    // does not spend real wall-clock time waiting for the session deadline.
    const flow = new FlowDefinition({
      name: 'live',
      operators: [report],
      completesWhen: ChatAnswerDataPoint,
      parkAfterMs: 0,
    });
    const sid = toSessionId('s1');
    const manager = new SessionOrchestrationManager(runtime);

    // No ChatAnswer yet → completesWhen unsatisfied → the session parks, but the interim aggregator
    // ran during gathering and wrote an in_progress document.
    await manager.startSession({ sessionId: sid, namespaceId: NAMESPACE, flow, seed: [risk(0.4)] });

    const document = await runtime.durable.read(REPORTS, String(sid));
    expect(document?.document.risk_count).toBe(1);
    expect(document?.status).toBe(AggregateStatus.IN_PROGRESS);
    // The orchestrated gather pass binds ctx.isFinal=false end to end.
    expect(document?.document.is_final).toBe(false);
    expect(await runtime.durable.isContributionMarked(sid, OperatorId('report'))).toBe(false);
  });

  it('flips to final and marks its contribution once the finalize pass runs', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const report = makeAggregator('report', {
      dependsOn: [RiskDataPoint],
      rerunOnNewData: true,
      interimRefresh: true,
      onAggregate: writeScore,
    });
    const flow = new FlowDefinition({
      name: 'live',
      operators: [report],
      completesWhen: ChatAnswerDataPoint,
      parkAfterMs: 0,
    });
    const sid = toSessionId('s2');
    const manager = new SessionOrchestrationManager(runtime);

    await manager.startSession({ sessionId: sid, namespaceId: NAMESPACE, flow, seed: [risk(0.4)] });
    expect((await runtime.durable.read(REPORTS, String(sid)))?.status).toBe(AggregateStatus.IN_PROGRESS);

    // The completing DataPoint arrives → the session resumes, satisfies completesWhen, and the
    // finalize pass re-runs the aggregator authoritatively.
    await manager.deliver({ sessionId: sid, namespaceId: NAMESPACE, flow, dataPoint: chatAnswer('done') });

    const document = await runtime.durable.read(REPORTS, String(sid));
    expect(document?.status).toBe(AggregateStatus.FINAL);
    expect(document?.document.is_final).toBe(true);
    expect(await runtime.durable.isContributionMarked(sid, OperatorId('report'))).toBe(true);
  });

  it('re-runs as new data arrives', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const report = makeAggregator('report', {
      dependsOn: [RiskDataPoint],
      rerunOnNewData: true,
      interimRefresh: true,
      onAggregate: writeScore,
    });
    const flow = new FlowDefinition({
      name: 'live',
      operators: [report],
      completesWhen: ChatAnswerDataPoint,
      parkAfterMs: 0,
    });
    const sid = toSessionId('s3');
    const manager = new SessionOrchestrationManager(runtime);

    await manager.startSession({ sessionId: sid, namespaceId: NAMESPACE, flow, seed: [risk(0.1)] });
    expect((await runtime.durable.read(REPORTS, String(sid)))?.document.risk_count).toBe(1);

    await manager.deliver({ sessionId: sid, namespaceId: NAMESPACE, flow, dataPoint: risk(0.9) });

    const refreshed = await runtime.durable.read(REPORTS, String(sid));
    expect(refreshed?.document.risk_count).toBe(2);
    expect(refreshed?.status).toBe(AggregateStatus.IN_PROGRESS);
  });

  it('is re-run by a `uses` type without that type gating its readiness', async () => {
    // `uses` is a rerun trigger, NOT a readiness requirement: the aggregator is ready off its
    // dependsOn (RiskDataPoint) even though the used type (ChatAnswer) is absent, and a later
    // ChatAnswer ADD re-fires the interim aggregator anyway.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const report = makeAggregator('report', {
      dependsOn: [RiskDataPoint],
      uses: [ChatAnswerDataPoint],
      rerunOnNewData: true,
      interimRefresh: true,
      onAggregate: writeCounts,
    });
    // completesWhen is a type never delivered → the session parks and stays interim.
    const flow = new FlowDefinition({
      name: 'live',
      operators: [report],
      completesWhen: TriggerDataPoint,
      parkAfterMs: 0,
    });
    const sid = toSessionId('uses');
    const manager = new SessionOrchestrationManager(runtime);

    // Ready off RiskDataPoint alone (ChatAnswer, a `uses` type, is absent and does NOT block it).
    await manager.startSession({ sessionId: sid, namespaceId: NAMESPACE, flow, seed: [risk(0.4)] });
    const first = await runtime.durable.read(REPORTS, String(sid));
    expect(first?.document.total).toBe(1);
    expect(first?.status).toBe(AggregateStatus.IN_PROGRESS);

    // A `uses` type arrives → the interim aggregator RE-RUNS and folds it, though ChatAnswer is
    // neither a dependency nor a readiness gate.
    await manager.deliver({ sessionId: sid, namespaceId: NAMESPACE, flow, dataPoint: chatAnswer('hi') });

    const refreshed = await runtime.durable.read(REPORTS, String(sid));
    expect(refreshed?.document.total).toBe(2);
    expect(refreshed?.status).toBe(AggregateStatus.IN_PROGRESS);
  });

  it('is not re-run by undeclared, non-dependency data', async () => {
    // Control: with the same shape but ChatAnswer in neither dependsOn nor uses, its arrival is not
    // rerun-worthy, so the interim document is not refreshed.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const report = makeAggregator('report', {
      dependsOn: [RiskDataPoint],
      rerunOnNewData: true,
      interimRefresh: true,
      onAggregate: writeCounts,
    });
    const flow = new FlowDefinition({
      name: 'live',
      operators: [report],
      completesWhen: TriggerDataPoint,
      parkAfterMs: 0,
    });
    const sid = toSessionId('no-uses');
    const manager = new SessionOrchestrationManager(runtime);

    await manager.startSession({ sessionId: sid, namespaceId: NAMESPACE, flow, seed: [risk(0.4)] });
    await manager.deliver({ sessionId: sid, namespaceId: NAMESPACE, flow, dataPoint: chatAnswer('hi') });

    // The ChatAnswer never triggered a rerun.
    expect((await runtime.durable.read(REPORTS, String(sid)))?.document.total).toBe(1);
  });

  it('does not run during gathering when it is a plain aggregator', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    // interimRefresh defaults to false — the aggregator must NOT run until the finalize phase.
    const report = makeAggregator('report', { dependsOn: [RiskDataPoint], onAggregate: writeScore });
    const flow = new FlowDefinition({
      name: 'batch',
      operators: [report],
      completesWhen: ChatAnswerDataPoint,
      parkAfterMs: 0,
    });
    const sid = toSessionId('s4');
    const manager = new SessionOrchestrationManager(runtime);

    await manager.startSession({ sessionId: sid, namespaceId: NAMESPACE, flow, seed: [risk(0.4)] });

    expect(await runtime.durable.read(REPORTS, String(sid))).toBeNull(); // nothing written until completion
  });

  it('treats a failing interim run as best-effort', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    let calls = 0;
    const flaky = async (ctx: OperatorContext): Promise<void> => {
      calls += 1;
      if (!ctx.isFinal) {
        throw new Error('interim boom'); // interim runs raise; the finalize pass succeeds
      }
      await aggregationOf(ctx).upsert(REPORTS, String(ctx.sessionId), { ok: true });
    };

    const report = makeAggregator('report', {
      dependsOn: [RiskDataPoint],
      rerunOnNewData: true,
      interimRefresh: true,
      onAggregate: flaky,
    });
    const flow = new FlowDefinition({
      name: 'live',
      operators: [report],
      completesWhen: ChatAnswerDataPoint,
      parkAfterMs: 0,
    });
    const sid = toSessionId('s5');
    const manager = new SessionOrchestrationManager(runtime);

    // Interim run raises; fault isolation swallows it without dead-lettering or wedging the session.
    await manager.startSession({ sessionId: sid, namespaceId: NAMESPACE, flow, seed: [risk(0.4)] });
    const result = await manager.deliver({
      sessionId: sid,
      namespaceId: NAMESPACE,
      flow,
      dataPoint: chatAnswer('done'),
    });

    expect(result?.deadLetters).toEqual([]); // an interim failure never dead-letters
    expect(calls).toBeGreaterThan(0);
    // Finalize still ran and committed.
    expect((await runtime.durable.read(REPORTS, String(sid)))?.status).toBe(AggregateStatus.FINAL);
  });

  it('still finalizes after a resume that followed an interim write', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const report = makeAggregator('report', {
      dependsOn: [RiskDataPoint],
      rerunOnNewData: true,
      interimRefresh: true,
      onAggregate: writeScore,
    });
    const flow = new FlowDefinition({
      name: 'live',
      operators: [report],
      completesWhen: ChatAnswerDataPoint,
      parkAfterMs: 0,
    });
    const sid = toSessionId('s6');
    const manager = new SessionOrchestrationManager(runtime);

    await manager.startSession({ sessionId: sid, namespaceId: NAMESPACE, flow, seed: [risk(0.4)] });
    const result = await manager.deliver({
      sessionId: sid,
      namespaceId: NAMESPACE,
      flow,
      dataPoint: chatAnswer('done'),
    });

    expect(result?.status).toBe(SessionStatus.COMPLETED);
    expect((await runtime.durable.read(REPORTS, String(sid)))?.status).toBe(AggregateStatus.FINAL);
  });

  it('ends a superseded run on its interim write, without finalizing', async () => {
    // A fenced run (a higher epoch took over mid-gather) ends SUPERSEDED: the interim write that
    // already landed in the durable store is the last record, contribution is never marked, and the
    // finalize pass never ran — the successor (a higher epoch) is responsible for completing.
    const clock = new FakeClock();
    const runtime = OrchestratorRuntime({ ...buildInMemoryRuntime(clock), lock: new FenceOnRenewLock(clock) });
    const sid = toSessionId('s7');
    const report = makeAggregator('report', {
      dependsOn: [RiskDataPoint],
      rerunOnNewData: true,
      interimRefresh: true,
      onAggregate: writeScore,
    });
    // A self-cycling operator with a long debounce forces a loop sleep that crosses the renew
    // interval, triggering the fenced renew after the interim aggregator has already written.
    const ticker = makeOperator('ticker', {
      dependsOn: [RiskDataPoint],
      rerunOnNewData: true,
      debounceMs: 20_000,
    });

    const result = await new Orchestrator({
      sessionId: sid,
      namespaceId: NAMESPACE,
      runtime,
      operators: [report, ticker],
      seed: [risk(0.4)],
      completesWhen: ChatAnswerDataPoint,
      sessionDeadlineMs: 300_000,
    }).run();

    expect(result.status).toBe(SessionStatus.SUPERSEDED); // fenced mid-run → a clean stop, not a raise
    // The finalize pass never ran: contribution was not marked and the durable document stays
    // in_progress (the interim write the fenced run already landed is its last word; a successor
    // overwrites at a higher epoch when it finalizes).
    expect(await runtime.durable.isContributionMarked(sid, OperatorId('report'))).toBe(false);
    expect((await runtime.durable.read(REPORTS, String(sid)))?.status).toBe(AggregateStatus.IN_PROGRESS);
  });

  it('does not block quiescence once it has caught up', async () => {
    // An interim aggregator with rerunOnNewData that has caught up (no new data) must let the
    // session quiesce and park — it must NOT spin indefinitely or block startSession.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const report = makeAggregator('report', {
      dependsOn: [RiskDataPoint],
      rerunOnNewData: true,
      interimRefresh: true,
      onAggregate: writeScore,
    });
    const flow = new FlowDefinition({
      name: 'live',
      operators: [report],
      completesWhen: ChatAnswerDataPoint,
      parkAfterMs: 0,
    });
    const sid = toSessionId('s8');
    const manager = new SessionOrchestrationManager(runtime);

    // startSession must return (not hang): after the aggregator processes the seed data and finds no
    // further changes, the session reaches the inbox wait and parks immediately.
    const result = await manager.startSession({ sessionId: sid, namespaceId: NAMESPACE, flow, seed: [risk(0.4)] });

    expect(result.status).toBe(SessionStatus.PARKED); // parked, not hung
    // The interim write happened.
    expect((await runtime.durable.read(REPORTS, String(sid)))?.status).toBe(AggregateStatus.IN_PROGRESS);
  });

  it('writes nothing in either phase when the namespace gates it', async () => {
    // An interimRefresh aggregator the namespace does not permit must be dropped from BOTH the
    // gather set and the aggregation set: no in_progress write during gathering and no final write
    // at completion.
    const catalog = new InMemoryCapabilityCatalog({ permittedOperators: [[NAMESPACE, [OperatorId('kept')]]] });
    const runtime = buildInMemoryRuntime(new FakeClock(), { catalog });
    const kept = makeOperator('kept', {
      dependsOn: [RiskDataPoint],
      produces: [ChatAnswerDataPoint],
      emits: [chatAnswer('done')],
    });
    const report = makeAggregator('report', {
      dependsOn: [RiskDataPoint],
      rerunOnNewData: true,
      interimRefresh: true,
      onAggregate: writeScore,
    });
    const flow = new FlowDefinition({
      name: 'gated',
      operators: [kept, report],
      completesWhen: ChatAnswerDataPoint,
      parkAfterMs: 0,
    });
    const sid = toSessionId('s10');
    const manager = new SessionOrchestrationManager(runtime);

    const result = await manager.startSession({ sessionId: sid, namespaceId: NAMESPACE, flow, seed: [risk(0.4)] });

    // 'kept' satisfies completesWhen; the session finishes.
    expect(result.status).toBe(SessionStatus.COMPLETED);
    // The gated report never wrote, interim or final.
    expect(await runtime.durable.read(REPORTS, String(sid))).toBeNull();
    expect(await runtime.durable.isContributionMarked(sid, OperatorId('report'))).toBe(false);
  });

  it('never blocks or dead-letters when every interim run fails', async () => {
    // An interim aggregator that raises on EVERY interim run must still let the session park (its
    // watermark advances on failure, so it does not loop) and must never dead-letter — only the
    // final pass has finality.
    const runtime = buildInMemoryRuntime(new FakeClock());
    let interimAttempts = 0;
    const flaky = async (ctx: OperatorContext): Promise<void> => {
      if (!ctx.isFinal) {
        interimAttempts += 1;
        throw new Error('interim boom');
      }
      await aggregationOf(ctx).upsert(REPORTS, String(ctx.sessionId), { ok: true });
    };

    const report = makeAggregator('report', {
      dependsOn: [RiskDataPoint],
      rerunOnNewData: true,
      interimRefresh: true,
      onAggregate: flaky,
    });
    const flow = new FlowDefinition({
      name: 'flaky',
      operators: [report],
      completesWhen: ChatAnswerDataPoint,
      parkAfterMs: 0,
    });
    const sid = toSessionId('s11');
    const manager = new SessionOrchestrationManager(runtime);

    const first = await manager.startSession({ sessionId: sid, namespaceId: NAMESPACE, flow, seed: [risk(0.1)] });
    expect(first.status).toBe(SessionStatus.PARKED);
    expect(first.deadLetters).toEqual([]);

    const second = await manager.deliver({ sessionId: sid, namespaceId: NAMESPACE, flow, dataPoint: risk(0.9) });
    expect(second?.status).toBe(SessionStatus.PARKED);
    expect(second?.deadLetters).toEqual([]);
    expect(interimAttempts).toBeGreaterThanOrEqual(2); // reran on the new data, each failing — never looping

    const final = await manager.deliver({
      sessionId: sid,
      namespaceId: NAMESPACE,
      flow,
      dataPoint: chatAnswer('done'),
    });
    expect(final?.status).toBe(SessionStatus.COMPLETED);
    expect(final?.deadLetters).toEqual([]);
    expect((await runtime.durable.read(REPORTS, String(sid)))?.status).toBe(AggregateStatus.FINAL);
  });
});

/**
 * Lets a run finish its finalize, then fences it at `markComplete`.
 *
 * The shape of a predecessor that lost its lease in the completion tail after its finalize pass had
 * already committed `final`. That tail runs unrenewed, so a durable write stalling there for longer
 * than the lock's TTL produces exactly this: a live owner fenced by a supervisor that read the
 * session as orphaned.
 */
class FenceOnCompleteLock extends InMemorySessionLock {
  public override async markComplete(): Promise<void> {
    throw new StaleEpochError('a higher epoch took over');
  }
}

/**
 * The lock's TTL, in milliseconds.
 *
 * It is decided against the injected clock, so the production failure — a lease lapsing under a live
 * owner because the completion tail runs unrenewed — is reproducible on virtual time.
 */
const LOCK_TTL_MS = 30_000;

describe('`final` is terminal for an aggregator output', () => {
  it('never downgrades an already-final record on a resumed epoch', async () => {
    // The failure this pins: epoch 1 finalized (status `final`, contribution marked) but was fenced
    // in its completion tail before marking the session complete. The successor rehydrated, its
    // interimRefresh aggregator re-ran during gather and overwrote `final` with `in_progress`, and
    // its finalize pass was then SKIPPED as already-contributed — so the record was stranded at
    // `in_progress` for good. Every consumer polls for `final`, so a complete session read as "no
    // data at all".
    const clock = new FakeClock();
    const runtime = OrchestratorRuntime({ ...buildInMemoryRuntime(clock), lock: new FenceOnCompleteLock(clock) });
    const sid = toSessionId('s-downgrade');
    const report = makeAggregator('report', {
      dependsOn: [RiskDataPoint],
      rerunOnNewData: true,
      interimRefresh: true,
      onAggregate: writeScore,
    });

    const fenced = await new Orchestrator({
      sessionId: sid,
      namespaceId: NAMESPACE,
      runtime,
      operators: [report],
      seed: [risk(0.4), chatAnswer('done')],
      completesWhen: ChatAnswerDataPoint,
      sessionDeadlineMs: 300_000,
    }).run();

    // Precondition: the predecessor DID finalize, and only its completion mark was lost.
    expect(fenced.status).toBe(SessionStatus.SUPERSEDED);
    expect(await runtime.durable.isContributionMarked(sid, OperatorId('report'))).toBe(true);
    expect((await runtime.durable.read(REPORTS, String(sid)))?.status).toBe(AggregateStatus.FINAL);

    // The successor: fresh data makes the interim aggregator re-run during gather, and the finalize
    // pass is skipped because the contribution is already marked.
    const resumed = await new Orchestrator({
      sessionId: sid,
      namespaceId: NAMESPACE,
      runtime: OrchestratorRuntime({ ...runtime, lock: new InMemorySessionLock(clock) }),
      operators: [report],
      seed: [risk(0.9)],
      completesWhen: ChatAnswerDataPoint,
      sessionDeadlineMs: 300_000,
    }).run();

    expect(resumed.status).toBe(SessionStatus.COMPLETED);
    // `final` is terminal for an aggregator's output: an interim refresh that lands afterwards must
    // not be able to walk it back, whatever the scheduling.
    expect((await runtime.durable.read(REPORTS, String(sid)))?.status).toBe(AggregateStatus.FINAL);
  });

  it('cannot be stranded by a lease lost in the completion tail', async () => {
    // The whole failure, end to end, on the real mechanism rather than a stand-in fence: the tail
    // stalls past the lock's TTL, so the lease lapses while its owner is still working; the
    // supervisor reads the session as genuinely orphaned and resumes it at a higher epoch, fencing
    // the original mid-finalize. That takeover is tolerated — the tail being unrenewed is a known,
    // documented gap — but it must not walk the record back. In production it did: the successor's
    // interim pass overwrote `final` with `in_progress` and skipped its own finalize as
    // already-contributed, stranding a complete session where every consumer polls for `final` and
    // so reads it as holding no data at all.
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const sid = toSessionId('s-tail-stall');
    const manager = new SessionOrchestrationManager(runtime);
    const report = makeAggregator('report', {
      dependsOn: [RiskDataPoint],
      rerunOnNewData: true,
      interimRefresh: true,
      onAggregate: writeScore,
    });
    const flow = new FlowDefinition({ name: 'live', operators: [report], completesWhen: ChatAnswerDataPoint });

    const tailBlocked = new Deferred<void>();
    const releaseTail = new Deferred<void>();
    const inbox = runtime.inbox as { pendingCount(sessionId: SessionId): Promise<number> };
    const realPendingCount = inbox.pendingCount.bind(inbox);
    let stallsLeft = 1;
    inbox.pendingCount = async (sessionId: SessionId): Promise<number> => {
      // Only the FIRST caller stalls: the successor's own tail must run normally, or it would wedge
      // too.
      if (stallsLeft > 0) {
        stallsLeft -= 1;
        tailBlocked.resolve();
        await releaseTail.promise;
      }
      return await realPendingCount(sessionId);
    };

    const stalled = new Orchestrator({
      sessionId: sid,
      namespaceId: NAMESPACE,
      runtime,
      operators: [report],
      seed: [risk(0.4), chatAnswer('done')],
      completesWhen: ChatAnswerDataPoint,
      sessionDeadlineMs: 300_000,
    }).run();
    await tailBlocked.promise;

    // Its finalize already committed; only the completion mark is still outstanding.
    expect((await runtime.durable.read(REPORTS, String(sid)))?.status).toBe(AggregateStatus.FINAL);

    // Carry the clock past the TTL. Nothing renews in the tail, so the lease lapses under a LIVE
    // owner — this is the gap itself, asserted rather than described.
    clock.advance(LOCK_TTL_MS + 1000);
    expect(await runtime.lock.isHeld(sid)).toBe(false);
    expect(await manager.isOrphaned(sid)).toBe(true);

    // A late deliver lands while the owner is stalled — the shape that produced this in production,
    // where participant info arrived during the run. With the lock free and the session not
    // complete, the supervisor resumes it at a higher epoch, and the successor's gather sees the new
    // data, so its interimRefresh aggregator re-runs (the write that used to walk `final` back).
    const resumed = await manager.deliver({ sessionId: sid, namespaceId: NAMESPACE, flow, dataPoint: risk(0.9) });
    expect(resumed?.status).toBe(SessionStatus.COMPLETED);
    expect(Number(resumed?.epoch)).toBeGreaterThan(1); // the takeover really happened
    // The successor ran the aggregator during gather but NOT as its finalize: the contribution was
    // already marked by the fenced predecessor, so the aggregation phase skipped it. Its last word
    // was an interim write.
    expect(await runtime.durable.isContributionMarked(sid, OperatorId('report'))).toBe(true);

    releaseTail.resolve();
    expect((await stalled).status).toBe(SessionStatus.SUPERSEDED); // fenced by the successor, as designed

    expect((await runtime.durable.read(REPORTS, String(sid)))?.status).toBe(AggregateStatus.FINAL);
  });

  it('fails the run instead of holding the epoch when the completion tail hangs', async () => {
    // The tail is the one stretch the gathering loop's renewals do not cover, and the lock's TTL is
    // specified to exceed the longest single unrenewed await. A durable flush that blocks
    // indefinitely broke that promise silently: the lease lapsed under a live owner and a supervisor
    // resumed the session on top of it, mid-finalize. Bounding the step keeps the lease honest — the
    // run fails, the epoch is released, and the successor re-drives from durable state.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const sid = toSessionId('s-hung-tail');
    const hung = new Deferred<void>(); // never resolved: models a write that simply never returns
    const inbox = runtime.inbox as { pendingCount(sessionId: SessionId): Promise<number> };
    inbox.pendingCount = async (): Promise<number> => {
      await hung.promise;
      return 0;
    };
    const report = makeAggregator('report', {
      dependsOn: [RiskDataPoint],
      rerunOnNewData: true,
      interimRefresh: true,
      onAggregate: writeScore,
    });

    await expect(
      new Orchestrator({
        sessionId: sid,
        namespaceId: NAMESPACE,
        runtime,
        operators: [report],
        seed: [risk(0.4), chatAnswer('done')],
        completesWhen: ChatAnswerDataPoint,
        sessionDeadlineMs: 300_000,
        operationTimeoutMs: 50,
      }).run(),
    ).rejects.toBeInstanceOf(CompletionTailTimeoutError);

    // The epoch is released even though the tail blew up, so a successor can take the session over
    // immediately rather than waiting out the TTL.
    expect(await runtime.lock.isHeld(sid)).toBe(false);
    // And it is NOT marked complete: the run did not finish, so the session stays re-drivable.
    expect(await runtime.lock.isComplete(sid)).toBe(false);
  });
});

/**
 * The coalescing window an interim aggregator arms.
 *
 * Its exact width is irrelevant to the behaviour under test — what matters is that the completion
 * tail no longer scales with it.
 */
const INTERIM_WINDOW_MS = 250;

/** Note whether each fold was the authoritative one, then delegate the durable write. */
const recordFold = async (
  folds: boolean[],
  write: (ctx: OperatorContext) => Promise<void>,
  ctx: OperatorContext,
): Promise<void> => {
  folds.push(ctx.isFinal);
  await write(ctx);
};

const writeTotals = async (ctx: OperatorContext): Promise<void> => {
  const total = ctx.store.ofType(RiskDataPoint).length + ctx.store.ofType(IpDataPoint).length;
  await aggregationOf(ctx).upsert(REPORTS, String(ctx.sessionId), { total, is_final: ctx.isFinal });
};

describe('an interim coalescing window', () => {
  it('is not charged to the completion tail once the condition is satisfied', async () => {
    // The tail is the one stretch where an interim refold can buy nothing: with the completion
    // condition already satisfied and nothing left running, the next thing that happens is the
    // finalize pass, which rewrites the same document. Waiting the window out there produced a
    // second `in_progress` write that `final` overwrote in the same instant — no reader could ever
    // observe it — while charging the window's full width to every session that completes.
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const folds: boolean[] = [];
    const report = makeAggregator('report', {
      dependsOn: [RiskDataPoint],
      rerunOnNewData: true,
      interimRefresh: true,
      debounceMs: INTERIM_WINDOW_MS,
      onAggregate: async (ctx) => await recordFold(folds, writeScore, ctx),
    });
    // One late detector: ready off the already-present completing DataPoint, and its emission is
    // what arms the aggregator's window with nothing else left to run.
    const late = makeOperator('late', {
      dependsOn: [ChatAnswerDataPoint],
      produces: [RiskDataPoint],
      emits: [risk(0.9)],
    });
    const sid = toSessionId('s-tail-charge');

    const result = await new Orchestrator({
      sessionId: sid,
      namespaceId: NAMESPACE,
      runtime,
      operators: [report, late],
      seed: [risk(0.4), chatAnswer('done')],
      completesWhen: ChatAnswerDataPoint,
      sessionDeadlineMs: 300_000,
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    // FakeClock advances only where the gathering loop itself waits, so this IS the simulated time
    // the session spent in its tail — and it must not scale with the window.
    expect(clock.monotonic(), 'the completion tail waited out the interim window').toBe(0);
    expect(folds, 'the tail ran an interim refold the finalize immediately overwrote').toEqual([false, true]);
    // Nothing is lost by abandoning the refold: the finalize folds the late Risk authoritatively.
    const document = await runtime.durable.read(REPORTS, String(sid));
    expect(document?.status).toBe(AggregateStatus.FINAL);
    expect(document?.document.risk_count).toBe(2);
  });

  it('still collapses a burst arriving while work runs', async () => {
    // The coalescing itself is the point of the window and must survive: without it every arrival
    // forces a full rebuild-reseal-rewrite on the event loop, competing with operators still on the
    // critical path. A self-cycling ticker delivers its arrivals in separate gather passes with work
    // still in flight — exactly the burst the window exists to collapse.
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const refoldedAt: number[] = [];
    const noteRefold = async (ctx: OperatorContext): Promise<void> => {
      if (!ctx.isFinal) {
        refoldedAt.push(clock.monotonic());
      }
      await writeScore(ctx);
    };

    const report = makeAggregator('report', {
      dependsOn: [IpDataPoint],
      rerunOnNewData: true,
      interimRefresh: true,
      debounceMs: 3000,
      onAggregate: noteRefold,
    });
    let counter = 0;
    const ticker = makeOperator('ticker', {
      dependsOn: [IpDataPoint],
      produces: [IpDataPoint],
      rerunOnNewData: true,
      maxCycles: 6,
      debounceMs: 1000, // one arrival per simulated second, for six seconds
      emitFactory: () => {
        const emitted = [ip(`198.51.100.${counter}`)];
        counter += 1;
        return emitted;
      },
    });
    const sid = toSessionId('s-burst');

    const result = await new Orchestrator({
      sessionId: sid,
      namespaceId: NAMESPACE,
      runtime,
      operators: [report, ticker],
      seed: [ip('198.51.100.254')],
      sessionDeadlineMs: 300_000,
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    const stored = (await runtime.store.snapshot(sid)).ofType(IpDataPoint);
    expect(stored).toHaveLength(7); // seven arrivals, one per simulated second, while the ticker cycles
    // One refold per window, not one per arrival: the burst still collapses while work is in flight.
    // A zero-width window would refold on every one of the seven.
    expect(refoldedAt.slice(0, 2), 'the window stopped collapsing arrivals into one refold').toEqual([0, 3000]);
    // The ticker's last cycle lands at 5.0s and nothing else can run after it, so any later refold
    // is one the imminent finalize would overwrite — and the wait for it is charged to the tail.
    expect(
      Math.max(...refoldedAt),
      'an interim refold ran once the work had drained, ahead of the finalize',
    ).toBeLessThan(5000);
    expect(clock.monotonic(), 'the completion tail waited out the interim window').toBe(5000);
  });

  it('is still waited out before an unsatisfied session goes idle', async () => {
    // The mirror image, and the reason the exemption is conditional rather than blanket: with the
    // completion condition unsatisfied, a would-be-quiescent session is heading for an inbox wait of
    // unbounded length, where the interim document is the only live view a reader gets. Abandoning
    // the window there would trade a bounded tail charge for a stale record over an open-ended wait.
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const folds: boolean[] = [];
    const report = makeAggregator('report', {
      dependsOn: [RiskDataPoint],
      uses: [IpDataPoint],
      rerunOnNewData: true,
      interimRefresh: true,
      debounceMs: 2000,
      onAggregate: async (ctx) => await recordFold(folds, writeTotals, ctx),
    });
    const late = makeOperator('late', {
      dependsOn: [RiskDataPoint],
      produces: [IpDataPoint],
      emits: [ip('198.51.100.7')],
    });
    // completesWhen never arrives, so the session reaches the inbox wait and parks immediately.
    const flow = new FlowDefinition({
      name: 'live',
      operators: [report, late],
      completesWhen: ChatAnswerDataPoint,
      parkAfterMs: 0,
    });
    const sid = toSessionId('s-idle-window');
    const manager = new SessionOrchestrationManager(runtime);

    const result = await manager.startSession({ sessionId: sid, namespaceId: NAMESPACE, flow, seed: [risk(0.4)] });

    expect(result.status).toBe(SessionStatus.PARKED);
    expect(clock.monotonic(), 'the interim window was abandoned before an idle wait').toBe(2000);
    expect(folds, 'the refold that carries the live view never ran').toEqual([false, false]);
    // The refold landed before the session went idle, so a reader polling the record sees the late
    // arrival rather than a snapshot frozen at the first fold.
    const document = await runtime.durable.read(REPORTS, String(sid));
    expect(document?.status).toBe(AggregateStatus.IN_PROGRESS);
    expect(document?.document.total).toBe(2);
  });
});
