/**
 * E2E — cross-cutting end-to-end flows over the in-memory runtime + FakeClock.
 *
 * These layer on the unit/conformance suites (ACC = single happy paths, RECOV = single crash
 * scenarios) to drive the *whole* engine under multi-actor, multi-wave conditions. "Cross-pod"
 * is modelled by two `SessionOrchestrationManager`s sharing one `OrchestratorRuntime` — the
 * store/inbox/lock/durable/audit that real pods would share via Redis + Mongo. The fencing
 * epoch and the durable completion/contribution markers are what keep the pods correct, so
 * each flow asserts the durable end state, not just the in-process result.
 */

import { describe, expect, it } from 'vitest';
import {
  InMemoryCapabilityCatalog,
  InMemoryDataPointStore,
  InMemorySessionLock,
} from '../src/orcastork/adapters/memory/index.js';
import { AuditKind } from '../src/orcastork/audit/index.js';
import type { AnyDataPoint } from '../src/orcastork/datapoints/index.js';
import { DataPointEmission } from '../src/orcastork/datapoints/index.js';
import { StaleEpochError } from '../src/orcastork/exceptions.js';
import type { FlowDefinitionInit } from '../src/orcastork/flow.js';
import { FlowDefinition } from '../src/orcastork/flow.js';
import type { Revision } from '../src/orcastork/ids.js';
import { CapabilityId, NamespaceId, OperatorId, SessionId } from '../src/orcastork/ids.js';
import { Deferred } from '../src/orcastork/internal/deferred.js';
import { SessionOrchestrationManager } from '../src/orcastork/manager/index.js';
import type { ConcreteOperatorClass, OperatorContext } from '../src/orcastork/operators/index.js';
import { Operator, OperatorPolicy, operator } from '../src/orcastork/operators/index.js';
import type { OrchestratorOptions, OrchestratorResult } from '../src/orcastork/orchestrator/index.js';
import { Orchestrator, SessionStatus } from '../src/orcastork/orchestrator/index.js';
import type { ApplyResolvedOptions } from '../src/orcastork/ports/index.js';
import { buildInMemoryRuntime, OrchestratorRuntime as makeRuntime } from '../src/orcastork/runtime.js';
import { makeCapability } from './doubles/capabilities.js';
import { FakeClock } from './doubles/clock.js';
import {
  ChatAnswerDataPoint,
  chatAnswer,
  EmailDataPoint,
  IpDataPoint,
  ip,
  RiskDataPoint,
  risk,
  workEmail,
} from './doubles/datapoints.js';
import { captureLogs } from './doubles/logs.js';
import { makeAggregator, makeOperator } from './doubles/operators.js';
import { TelemetryProbe } from './doubles/otel.js';

const NAMESPACE = NamespaceId('e2e-namespace');

const SECOND_MS = 1_000;

/** The lock TTL default is 30 s, so this is a lease that has certainly lapsed. */
const PAST_TTL_MS = 31 * SECOND_MS;

const flowOf = (
  operators: readonly ConcreteOperatorClass[],
  extras: Omit<FlowDefinitionInit, 'name' | 'operators'> = {},
): FlowDefinition => new FlowDefinition({ name: 'e2e-flow', operators, ...extras });

/** An orchestrator over a session, so a test names only what it varies. */
const orchestrate = (sessionId: SessionId, options: Omit<OrchestratorOptions, 'sessionId' | 'namespaceId'>) =>
  new Orchestrator({ sessionId, namespaceId: NAMESPACE, ...options });

/** A representative aggregator: fold the gathered risks into one durable report. */
const writeRiskCount = async (ctx: OperatorContext): Promise<void> => {
  expect(ctx.aggregation).not.toBeNull();
  await ctx.aggregation?.upsert('reports', 'report', { risk_count: ctx.store.ofType(RiskDataPoint).length });
};

/** A representative aggregator over Ip DataPoints (used by the deadline-straggler flow). */
const writeIpCount = async (ctx: OperatorContext): Promise<void> => {
  expect(ctx.aggregation).not.toBeNull();
  await ctx.aggregation?.upsert('reports', 'report', { ip_count: ctx.store.ofType(IpDataPoint).length });
};

/** Hand the event loop back once — the port of the Python tests' `await asyncio.sleep(0)`. */
const yieldOnce = (): Promise<void> =>
  new Promise<void>((resolve) => {
    setImmediate(resolve);
  });

/**
 * Stay in flight until the run is stopped, then give up quietly.
 *
 * The port of a straggler's `await never.wait()`: an `asyncio.Event` that is never set keeps the
 * task alive until the deadline cancels it. A promise cannot be cancelled, so the run's abort
 * signal is what ends the wait — and it resolves, because the Python operator has nothing left to
 * do once the cancel arrives.
 */
const waitUntilStopped = (signal: AbortSignal): Promise<void> =>
  new Promise<void>((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    signal.addEventListener('abort', () => resolve(), { once: true });
  });

/** A run stopped while it was waiting, the port of the `CancelledError` `asyncio.sleep` raises. */
class RunCancelledError extends Error {}

/**
 * Stay in flight until the run is stopped, then REJECT into the operator.
 *
 * `asyncio.sleep`/`Event.wait` raise `CancelledError` into the coroutine when its task is
 * cancelled, which is what lets an operator emit one last DataPoint as it unwinds. A promise
 * carries no such signal, so the abort on the run's context is turned into a rejection here.
 */
const waitUntilCancelled = (signal: AbortSignal): Promise<never> =>
  new Promise<never>((_resolve, reject) => {
    if (signal.aborted) {
      reject(new RunCancelledError('the run was cancelled'));
      return;
    }
    signal.addEventListener('abort', () => reject(new RunCancelledError('the run was cancelled')), { once: true });
  });

/**
 * Store whose `applyResolved` raises {@link StaleEpochError} on its Nth call.
 *
 * Models a successor minting a higher epoch mid-batch: the very next forwarded merge is fenced by
 * the store, exactly as the real CAS would reject a stale writer's write.
 */
class FenceAfterNApplies extends InMemoryDataPointStore {
  private readonly raiseOnCall: number;
  public applies = 0;

  public constructor(options: { readonly raiseOnCall: number }) {
    super();
    this.raiseOnCall = options.raiseOnCall;
  }

  public override async applyResolved(sessionId: SessionId, options: ApplyResolvedOptions): Promise<Revision> {
    this.applies += 1;
    if (this.applies === this.raiseOnCall) {
      throw new StaleEpochError('a higher epoch took over mid-batch');
    }
    return await super.applyResolved(sessionId, options);
  }
}

/** Store whose `commitEffect` is fenced — a takeover lands before the effect's commit. */
class FenceOnCommitEffectStore extends InMemoryDataPointStore {
  public override async commitEffect(): Promise<void> {
    throw new StaleEpochError('a higher epoch took over before the effect commit landed');
  }
}

/** Lock that grants/renews normally but fences the completion CAS — a takeover just before it. */
class FenceOnMarkCompleteLock extends InMemorySessionLock {
  public override async markComplete(): Promise<void> {
    throw new StaleEpochError('a higher epoch took over before the completion CAS');
  }
}

/**
 * An operator that emits, pushes the fake clock past the session deadline, then blocks forever.
 *
 * Emitting first lets the loop merge the DataPoint; advancing the clock makes the loop's NEXT
 * while-check fail (a deadline hit with this operator still in flight); the wait that only the
 * abort ends keeps the run alive so `drainRemaining` is the thing that stops it as a straggler.
 */
const deadlineStraggler = (
  operatorId: string,
  options: { readonly emits: readonly AnyDataPoint[]; readonly clock: FakeClock; readonly deadlineMs: number },
): ConcreteOperatorClass => {
  const emits = [...options.emits];

  class Straggler extends Operator {
    public static readonly operatorId = OperatorId(operatorId);
    public static readonly policy = OperatorPolicy({ rerunOnNewData: false });
    public static readonly produces = [...new Set(emits.map((dataPoint) => dataPoint.constructor))] as never;

    public async *run(ctx: OperatorContext): AsyncIterable<DataPointEmission> {
      for (const dataPoint of emits) {
        yield new DataPointEmission(dataPoint.constructor as never, dataPoint.value);
      }
      options.clock.advance(options.deadlineMs + SECOND_MS); // the next while-check sees the deadline elapsed
      await waitUntilStopped(ctx.signal); // stay running so only the deadline-cancel can stop us
    }
  }

  operator(Straggler);
  return Straggler;
};

/**
 * A straggler whose ONLY interesting emission is produced *as it is cancelled*.
 *
 * `wakeEmit` is yielded normally — it merely unblocks the loop's queue take so the next
 * while-check can observe the elapsed deadline (without an emission the loop would block on an
 * empty queue forever). `finalEmit` is yielded from the cancellation handler, i.e. only after
 * `drainRemaining` cancels the straggler. That emission is therefore enqueued *after* the loop's
 * last in-loop drain, so the second drain-after-cancel is the only thing that can capture it — the
 * precise path the first drain alone cannot reach.
 */
const cancelEmittingStraggler = (
  operatorId: string,
  options: {
    readonly wakeEmit: AnyDataPoint;
    readonly finalEmit: AnyDataPoint;
    readonly clock: FakeClock;
    readonly deadlineMs: number;
    /** Set when the final emission really was produced from the cancellation handler. */
    readonly emittedWhileCancelling: { cancelled: boolean };
  },
): ConcreteOperatorClass => {
  const { wakeEmit, finalEmit, emittedWhileCancelling } = options;

  class Straggler extends Operator {
    public static readonly operatorId = OperatorId(operatorId);
    public static readonly policy = OperatorPolicy({ rerunOnNewData: false });
    public static readonly produces = [...new Set([wakeEmit.constructor, finalEmit.constructor])] as never;

    public async *run(ctx: OperatorContext): AsyncIterable<DataPointEmission> {
      yield new DataPointEmission(wakeEmit.constructor as never, wakeEmit.value); // wakes the loop; merged in-loop
      options.clock.advance(options.deadlineMs + SECOND_MS); // the next while-check sees the deadline elapsed
      try {
        await waitUntilCancelled(ctx.signal); // stay running so only the deadline-cancel can stop us
      } catch (error) {
        // The cancel arrives from drainRemaining; emit one last DataPoint as we unwind so only the
        // post-cancel second drain can persist it, then let the cancellation finish.
        emittedWhileCancelling.cancelled = true;
        yield new DataPointEmission(finalEmit.constructor as never, finalEmit.value);
        throw error;
      }
    }
  }

  operator(Straggler);
  return Straggler;
};

describe('cross-pod handoff', () => {
  it('reconstructs unfinished work and restores capabilities on the new pod', async () => {
    const sid = SessionId('e2e-handoff');
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock, {
      catalog: new InMemoryCapabilityCatalog({ permitted: [[NAMESPACE, [CapabilityId('netcap')]]] }),
    });
    const collect = makeOperator('collect', { produces: [IpDataPoint], emits: [ip()] });
    const netcap = makeCapability('netcap', { dependsOn: [IpDataPoint] }); // available once an Ip exists
    const score = makeOperator('score', { requires: [netcap], produces: [RiskDataPoint], emits: [risk(0.8)] });
    const report = makeAggregator('report', { dependsOn: [RiskDataPoint], onAggregate: writeRiskCount });
    const operators = [collect, score, report];

    // Pod A ran `collect` (its Ip is persisted, its watermark advanced) then crashed before the rest.
    const epochA = await runtime.lock.acquire(sid);
    await runtime.store.write(sid, [ip()], { epoch: epochA });
    const revision = await runtime.store.revision(sid);
    await runtime.store.setWatermark(sid, OperatorId('collect'), revision, { epoch: epochA });
    clock.advance(PAST_TTL_MS); // Pod A's ownership lease expires → the session is orphaned

    const podB = new SessionOrchestrationManager(runtime);
    expect(await podB.isOrphaned(sid)).toBe(true);
    const result = await podB.resume({
      sessionId: sid,
      namespaceId: NAMESPACE,
      flow: flowOf(operators, { capabilities: [netcap] }),
    });

    expect(result).not.toBeNull();
    expect(result?.epoch).toBe(2); // taken over under a higher epoch
    expect(result?.operatorRuns.has(OperatorId('collect'))).toBe(false); // reconstructed, not re-run
    expect(result?.operatorRuns.get(OperatorId('score'))).toBe(1); // only the unfinished operator ran
    expect(netcap.activations).toHaveLength(1); // capability restored: activated fresh on Pod B
    expect((await runtime.durable.read('reports', 'report'))?.document).toEqual({ risk_count: 1 });
    expect(await runtime.lock.isComplete(sid)).toBe(true);
    // A third pod must not re-drive the now-finished session.
    const third = await new SessionOrchestrationManager(runtime).resume({
      sessionId: sid,
      namespaceId: NAMESPACE,
      flow: flowOf(operators, { capabilities: [netcap] }),
    });
    expect(third).toBeNull();
  });

  it('lets concurrent pods resume one orphan exactly once', async () => {
    const sid = SessionId('e2e-race');
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    let aggregateCalls = 0;

    const scorer = makeOperator('scorer', {
      dependsOn: [EmailDataPoint],
      produces: [RiskDataPoint],
      emits: [risk()],
    });
    const report = makeAggregator('report', {
      dependsOn: [RiskDataPoint],
      onAggregate: async (ctx) => {
        aggregateCalls += 1;
        await writeRiskCount(ctx);
      },
    });
    const flow = flowOf([scorer, report]);

    // A predecessor seeded the session then crashed (lease expires) → orphaned.
    const epochA = await runtime.lock.acquire(sid);
    await runtime.store.write(sid, [workEmail()], { epoch: epochA });
    clock.advance(PAST_TTL_MS);

    // Two pods both notice the orphan and race to resume it.
    const podA = new SessionOrchestrationManager(runtime);
    const podB = new SessionOrchestrationManager(runtime);
    const outcomes = await Promise.allSettled([
      podA.resume({ sessionId: sid, namespaceId: NAMESPACE, flow }),
      podB.resume({ sessionId: sid, namespaceId: NAMESPACE, flow }),
    ]);

    const driven = outcomes
      .filter((outcome) => outcome.status === 'fulfilled')
      .map((outcome) => outcome.value)
      .filter((value): value is OrchestratorResult => value !== null);
    expect(driven).toHaveLength(1); // exactly one pod drove it
    expect(driven[0]?.status).toBe(SessionStatus.COMPLETED);
    expect(aggregateCalls).toBe(1); // the aggregator ran exactly once — no double-drive
    expect((await runtime.durable.read('reports', 'report'))?.document).toEqual({ risk_count: 1 });
  });
});

describe('multi-wave pipelines', () => {
  it('unblocks a multi-wave pipeline through layered capabilities', async () => {
    const sid = SessionId('e2e-layered');
    const runtime = buildInMemoryRuntime(new FakeClock(), {
      catalog: new InMemoryCapabilityCatalog({
        permitted: [[NAMESPACE, [CapabilityId('auth'), CapabilityId('net')]]],
      }),
    });
    const activationOrder: CapabilityId[] = [];
    const auth = makeCapability('auth', { dependsOn: [EmailDataPoint], recordOrder: activationOrder });
    const net = makeCapability('net', { dependsOn: [IpDataPoint], requires: [auth], recordOrder: activationOrder });
    const collect = makeOperator('collect', { requires: [auth], produces: [IpDataPoint], emits: [ip()] });
    const score = makeOperator('score', { requires: [net], produces: [RiskDataPoint], emits: [risk()] });
    const report = makeAggregator('report', { dependsOn: [RiskDataPoint], onAggregate: writeRiskCount });

    const result = await orchestrate(sid, {
      runtime,
      operators: [collect, score, report],
      capabilities: [auth, net],
      seed: [workEmail()],
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(result.operatorRuns.get(OperatorId('collect'))).toBe(1); // wave 1, once `auth` was online
    expect(result.operatorRuns.get(OperatorId('score'))).toBe(1); // wave 2, once the layered `net` was online
    expect(activationOrder).toEqual([CapabilityId('auth'), CapabilityId('net')]); // base before layer
    expect(new Set((await runtime.store.snapshot(sid)).all().map((dataPoint) => dataPoint.type))).toEqual(
      new Set(['work_email', 'ip', 'risk']),
    );
    const activated = (await runtime.audit.replay(sid))
      .filter((entry) => entry.kind === AuditKind.CAPABILITY_ACTIVATED && entry.capability !== null)
      .map((entry) => entry.capability?.capabilityId)
      .sort();
    expect(activated).toEqual(['auth', 'net']); // both activations were audited
  });

  it('isolates a slow and a failing operator without wedging the session', async () => {
    const sid = SessionId('e2e-isolation');
    const runtime = buildInMemoryRuntime(new FakeClock());
    const collector = makeOperator('collector', { produces: [RiskDataPoint], emits: [risk()] });
    // Exceeds the operation timeout.
    const slow = makeOperator('slow', { produces: [IpDataPoint], emits: [ip()], sleepAfterMs: SECOND_MS });
    const failing = makeOperator('failing', {
      produces: [ChatAnswerDataPoint],
      emits: [chatAnswer()],
      raiseError: new Error('boom'),
    });
    const report = makeAggregator('report', { dependsOn: [RiskDataPoint], onAggregate: writeRiskCount });

    const result = await orchestrate(sid, {
      runtime,
      operators: [collector, slow, failing, report],
      operationTimeoutMs: 20,
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED); // neither the timeout nor the failure wedged it
    const types = new Set((await runtime.store.snapshot(sid)).all().map((dataPoint) => dataPoint.type));
    for (const expected of ['risk', 'ip', 'chat_answer']) {
      expect(types.has(expected)).toBe(true); // each operator's emission persisted, even slow/failing ones
    }
    expect((await runtime.durable.read('reports', 'report'))?.document).toEqual({ risk_count: 1 });
  });
});

describe('fencing across a takeover', () => {
  it('stops a fenced predecessor from mutating or completing the session', async () => {
    const sid = SessionId('e2e-fencing');
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const scorer = makeOperator('scorer', {
      dependsOn: [EmailDataPoint],
      produces: [RiskDataPoint],
      emits: [risk()],
    });
    const report = makeAggregator('report', { dependsOn: [RiskDataPoint], onAggregate: writeRiskCount });

    // Predecessor (Pod A) seeds under epoch 1 then stalls; its lease expires.
    const staleEpoch = await runtime.lock.acquire(sid);
    await runtime.store.write(sid, [workEmail()], { epoch: staleEpoch });
    clock.advance(PAST_TTL_MS);

    // Pod B takes over under a higher epoch and finishes the session.
    const resumed = await new SessionOrchestrationManager(runtime).resume({
      sessionId: sid,
      namespaceId: NAMESPACE,
      flow: flowOf([scorer, report]),
    });
    expect(resumed).not.toBeNull();
    expect(resumed?.epoch).toBe(2);
    expect(await runtime.lock.isComplete(sid)).toBe(true);

    // The fenced predecessor (still holding epoch 1) can neither mutate state nor declare it done.
    await expect(runtime.store.write(sid, [ip()], { epoch: staleEpoch })).rejects.toBeInstanceOf(StaleEpochError);
    await expect(runtime.lock.markComplete(sid, { epoch: staleEpoch })).rejects.toBeInstanceOf(StaleEpochError);

    // And a fresh supervisor still sees the session finished — it is not re-driven.
    const notResumed = await new SessionOrchestrationManager(runtime).resume({
      sessionId: sid,
      namespaceId: NAMESPACE,
      flow: flowOf([scorer, report]),
    });
    expect(notResumed).toBeNull();
  });

  it('stops as superseded when the operator-emission merge is fenced by the store', async () => {
    // A successor mints a higher epoch mid-batch; the very next forwarded applyResolved (the
    // operator-emission merge, after the seed merge already landed) is rejected with
    // StaleEpochError. That must unwind the gather loop into a clean SUPERSEDED stop — no durable
    // output, not marked complete, lock released.
    const sid = SessionId('e2e-store-fence-emit');
    const store = new FenceAfterNApplies({ raiseOnCall: 2 }); // 1 = seed merge; 2 = the operator emission
    const runtime = makeRuntime({ ...buildInMemoryRuntime(new FakeClock()), store });
    const scorer = makeOperator('scorer', {
      dependsOn: [EmailDataPoint],
      produces: [RiskDataPoint],
      emits: [risk()],
    });
    const report = makeAggregator('report', { dependsOn: [RiskDataPoint], onAggregate: writeRiskCount });

    const result = await orchestrate(sid, {
      runtime,
      operators: [scorer, report],
      seed: [workEmail()],
    }).run();

    expect(result.status).toBe(SessionStatus.SUPERSEDED); // the fenced merge ended the run cleanly
    expect(await runtime.durable.read('reports', 'report')).toBeNull(); // no durable output was written
    expect(await runtime.lock.isComplete(sid)).toBe(false); // never finalized
    expect(await runtime.lock.isHeld(sid)).toBe(false); // the epoch was released on the fenced path
  });

  it('stops as superseded when the inbox apply is fenced by the store', async () => {
    // The fence lands inside the inbox-apply write path: a takeover rejects the inbox merge, which
    // must also propagate as a clean SUPERSEDED stop.
    const sid = SessionId('e2e-store-fence-inbox');
    const store = new FenceAfterNApplies({ raiseOnCall: 1 }); // the first apply is the inbox entry
    const runtime = makeRuntime({ ...buildInMemoryRuntime(new FakeClock()), store });
    await runtime.inbox.append(sid, chatAnswer('q1'));
    const scorer = makeOperator('scorer', {
      dependsOn: [ChatAnswerDataPoint],
      produces: [RiskDataPoint],
      emits: [risk()],
    });

    const result = await orchestrate(sid, {
      runtime,
      operators: [scorer],
      completesWhen: RiskDataPoint,
    }).run();

    expect(result.status).toBe(SessionStatus.SUPERSEDED); // the inbox-apply fence ended the run cleanly
    expect(await runtime.inbox.pendingCount(sid)).toBe(1); // the entry stays un-acked for the successor
    expect(await runtime.lock.isComplete(sid)).toBe(false);
    expect(await runtime.lock.isHeld(sid)).toBe(false);
  });

  it('stops as superseded when mark_complete loses the CAS', async () => {
    // A higher epoch is minted between the end of aggregation and the completion CAS; markComplete
    // raises StaleEpochError, which the orchestrator must turn into a clean SUPERSEDED stop rather
    // than leaking the rejection as a run failure or a (wrong) COMPLETED.
    const sid = SessionId('e2e-fenced-finalize');
    const clock = new FakeClock();
    const runtime = makeRuntime({ ...buildInMemoryRuntime(clock), lock: new FenceOnMarkCompleteLock(clock) });
    const scorer = makeOperator('scorer', {
      dependsOn: [EmailDataPoint],
      produces: [RiskDataPoint],
      emits: [risk()],
    });
    const report = makeAggregator('report', { dependsOn: [RiskDataPoint], onAggregate: writeRiskCount });

    const result = await orchestrate(sid, {
      runtime,
      operators: [scorer, report],
      seed: [workEmail()],
    }).run();

    expect(result.status).toBe(SessionStatus.SUPERSEDED); // the lost completion CAS is a clean stop
    expect(await runtime.lock.isComplete(sid)).toBe(false); // the predecessor never finalized
    expect(await runtime.lock.isHeld(sid)).toBe(false); // the epoch was released
  });

  it('degrades cleanly when an effect commit is fenced mid-once', async () => {
    // Cross-subsystem cleanup-under-fencing: an operator claims an effect via ctx.once and runs its
    // body, but a takeover mints a higher epoch before the commit lands, so commitEffect(epoch=1)
    // is rejected. EffectGuard deliberately ABSORBS a failed commit (preferring a possibly-skipped
    // effect over a possibly-double-fired one within an epoch) — so the fenced commit degrades to a
    // logged warning, the claim is left as this epoch's pending mark for the successor's RERUN/SKIP
    // recovery policy, the operator is NOT crashed by the cleanup-path failure, and the session runs
    // to its normal disposition rather than a secondary crash masking the unwind.
    const sid = SessionId('e2e-effect-fence');
    const store = new FenceOnCommitEffectStore();
    const runtime = makeRuntime({ ...buildInMemoryRuntime(new FakeClock()), store });
    const effectRan = { fired: false };

    class Sender extends Operator {
      public static readonly operatorId = OperatorId('sender');
      public static readonly policy = OperatorPolicy({ rerunOnNewData: false });
      public static readonly dependsOn = [EmailDataPoint];
      public static readonly produces = [RiskDataPoint];

      public async *run(ctx: OperatorContext): AsyncIterable<DataPointEmission> {
        // The guarded block is a callback here, so what it would have yielded is collected and
        // yielded once the claim has been committed (or, as here, failed to commit).
        const emissions: DataPointEmission[] = [];
        await ctx.once('send-otp', (acquired) => {
          if (acquired) {
            effectRan.fired = true; // the side effect actually ran this attempt
            emissions.push(new DataPointEmission(RiskDataPoint, 0.7));
          }
        });
        yield* emissions;
      }
    }
    operator(Sender);

    const { records, result } = await captureLogs(
      async () => await orchestrate(sid, { runtime, operators: [Sender], seed: [workEmail()] }).run(),
      { level: 'WARNING' },
    );

    expect(result.status).toBe(SessionStatus.COMPLETED); // the absorbed commit failure is no secondary crash
    expect(effectRan.fired).toBe(true); // the effect did run; only its commit was fenced
    expect(result.operatorRuns.get(OperatorId('sender'))).toBe(1); // ran without an injected error
    // The claim is left as this epoch's pending mark — never fabricated 'committed' — so the
    // successor's RERUN/SKIP recovery policy decides what the half-run effect means.
    expect(await store.getEffectState(sid, 'sender:send-otp')).toBe('pending:1');
    const commitWarning = records.find((record) => record.message.includes('commit failed'));
    expect(commitWarning?.fields.effect_key).toBe('sender:send-otp'); // logged, not raised
    expect(await runtime.lock.isHeld(sid)).toBe(false); // the epoch was released regardless
  });
});

describe('inbox-driven sessions', () => {
  it('survives a crash with inbox arrivals and applies each exactly once', async () => {
    const sid = SessionId('e2e-inbox');
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);

    const scorer = makeOperator('scorer', {
      dependsOn: [ChatAnswerDataPoint],
      produces: [RiskDataPoint],
      emitFactory: (ctx) => [risk(ctx.store.ofType(ChatAnswerDataPoint).length)],
    });
    const report = makeAggregator('report', {
      dependsOn: [RiskDataPoint],
      onAggregate: async (ctx) => {
        const latest = ctx.latest(RiskDataPoint);
        await ctx.aggregation?.upsert('reports', 'report', { answers: latest === null ? null : latest.value });
      },
    });

    // Pod A: one inbox entry claimed-but-unacked (in-flight at the crash), one that arrived while
    // it was down.
    await runtime.lock.acquire(sid);
    await runtime.inbox.append(sid, chatAnswer('q1'));
    await runtime.inbox.consume(sid); // claims q1, then the pod dies before applying/acking it
    await runtime.inbox.append(sid, chatAnswer('q2')); // arrives while the pod is offline
    clock.advance(PAST_TTL_MS);

    const resumed = await new SessionOrchestrationManager(runtime).resume({
      sessionId: sid,
      namespaceId: NAMESPACE,
      flow: flowOf([scorer, report]),
    });

    expect(resumed).not.toBeNull();
    const answers = new Set(
      (await runtime.store.snapshot(sid)).ofType(ChatAnswerDataPoint).map((dataPoint) => dataPoint.value),
    );
    expect(answers).toEqual(new Set(['q1', 'q2'])); // in-flight reclaimed, the new one consumed — each once
    expect((await runtime.durable.read('reports', 'report'))?.document).toEqual({ answers: 2 }); // both reflected
    expect(await runtime.inbox.pendingCount(sid)).toBe(0); // both acked
  });

  it('waits for the user’s input, then completes', async () => {
    // A challenge-shaped flow: present a question (operator), wait for the user's answer to arrive
    // on the inbox (no operator holds a connection open), then validate and aggregate.
    const sid = SessionId('e2e-wait');
    const runtime = buildInMemoryRuntime(new FakeClock());
    const questionPresented = new Deferred<void>();

    const triage = makeOperator('triage', {
      dependsOn: [EmailDataPoint],
      produces: [IpDataPoint],
      emitFactory: () => {
        questionPresented.resolve();
        return [IpDataPoint.emit('203.0.113.9')];
      },
    });
    const answerHandler = makeOperator('answer_handler', {
      dependsOn: [ChatAnswerDataPoint],
      produces: [RiskDataPoint],
      emits: [risk(0.9)],
    });
    const written: Record<string, number> = {};
    const reporter = makeAggregator('reporter', {
      dependsOn: [RiskDataPoint],
      onAggregate: async (ctx) => {
        written.risk_count = ctx.store.ofType(RiskDataPoint).length;
      },
    });

    const userAnswers = async (): Promise<void> => {
      await questionPresented.promise;
      for (let step = 0; step < 5; step += 1) {
        await yieldOnce(); // give the session time to reach the inbox wait (not required for correctness)
      }
      await runtime.inbox.append(sid, chatAnswer('it was me'));
    };

    const orchestrator = orchestrate(sid, {
      runtime,
      operators: [triage, answerHandler, reporter],
      seed: [workEmail()],
      completesWhen: RiskDataPoint,
    });
    const [result] = await Promise.all([orchestrator.run(), userAnswers()]);

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(result.operatorRuns.get(OperatorId('answer_handler'))).toBe(1); // woken by the inbox arrival
    expect(written).toEqual({ risk_count: 1 }); // aggregation saw the post-answer state
    expect(await runtime.inbox.pendingCount(sid)).toBe(0); // the user action was applied and acked
  });

  it('recovers a pod killed during an inbox wait through deliver', async () => {
    // Pod A dies while waiting for the user's answer; the answer lands on pod B's ingress.
    // `deliver` appends durably, detects the orphan, and resumes the session on pod B.
    const sid = SessionId('e2e-deliver');
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const answerHandler = makeOperator('answer_handler', {
      dependsOn: [ChatAnswerDataPoint],
      produces: [RiskDataPoint],
      emits: [risk(0.9)],
    });
    const report = makeAggregator('report', { dependsOn: [RiskDataPoint], onAggregate: writeRiskCount });

    // Pod A seeded the session and was waiting on the inbox when it died (lease expires unreleased).
    const epochA = await runtime.lock.acquire(sid);
    await runtime.store.write(sid, [workEmail()], { epoch: epochA });
    clock.advance(PAST_TTL_MS);

    const podB = new SessionOrchestrationManager(runtime);
    const result = await podB.deliver({
      sessionId: sid,
      namespaceId: NAMESPACE,
      dataPoint: chatAnswer('it was me'),
      flow: flowOf([answerHandler, report], { completesWhen: RiskDataPoint }),
    });

    expect(result).not.toBeNull();
    expect(result?.status).toBe(SessionStatus.COMPLETED);
    expect(result?.epoch).toBe(2); // a fresh, higher-epoch orchestrator on pod B
    expect(result?.operatorRuns.get(OperatorId('answer_handler'))).toBe(1); // the answer was processed
    expect((await runtime.durable.read('reports', 'report'))?.document).toEqual({ risk_count: 1 });
    expect(await runtime.inbox.pendingCount(sid)).toBe(0); // applied and acked — no message lost
  });
});

describe('the session deadline', () => {
  it('cancels a straggler after running aggregation', async () => {
    // The deadline elapses while an operator is mid-run: the deadline branch fires, drainRemaining
    // persists the pre-deadline emission and CANCELS the straggler, and the session still runs
    // aggregation and COMPLETES — the deadline is the one signal that may stop a running operator
    // (no other cancellation).
    const sid = SessionId('e2e-deadline-straggler');
    const clock = new FakeClock();
    const probe = new TelemetryProbe();
    const runtime = makeRuntime({ ...buildInMemoryRuntime(clock), telemetry: probe.telemetry });
    const straggler = deadlineStraggler('straggler', {
      emits: [ip('203.0.113.5')],
      clock,
      deadlineMs: 5 * SECOND_MS,
    });
    const report = makeAggregator('report', { dependsOn: [IpDataPoint], onAggregate: writeIpCount });

    const { records, result } = await captureLogs(
      async () =>
        await orchestrate(sid, {
          runtime,
          operators: [straggler, report],
          sessionDeadlineMs: 5 * SECOND_MS,
        }).run(),
      { level: 'WARNING' },
    );

    expect(result.status).toBe(SessionStatus.COMPLETED); // the deadline runs aggregation, not a wedge
    const stored = new Set(
      (await runtime.store.snapshot(sid)).ofType(IpDataPoint).map((dataPoint) => dataPoint.value),
    );
    expect(stored).toEqual(new Set(['203.0.113.5'])); // the pre-wait emission persisted before the cancel
    expect(await probe.counterTotal('session_deadline_hits_total')).toBe(1); // exactly one deadline hit
    expect((await runtime.durable.read('reports', 'report'))?.document).toEqual({ ip_count: 1 }); // aggregation ran
    const deadlineWarning = records.find((record) => record.message.includes('deadline hit'));
    expect(deadlineWarning?.fields.in_flight_operator_ids).toEqual([OperatorId('straggler')]); // the straggler named
    expect(await runtime.lock.isHeld(sid)).toBe(false); // the epoch was released even on the deadline path
  });

  it('captures an emission a straggler makes as it is cancelled', async () => {
    // At-least-once across the deadline-cancel boundary: an emission a straggler enqueues *as it is
    // cancelled* (already past the epoch-guarded merge boundary) must not be dropped.
    // drainRemaining drains once, cancels the stragglers, then drains a SECOND time precisely to
    // catch this. The wake emission is merged in-loop; the final emission exists only because the
    // post-cancel drain ran.
    const sid = SessionId('e2e-post-cancel-drain');
    const clock = new FakeClock();
    const probe = new TelemetryProbe();
    const runtime = makeRuntime({ ...buildInMemoryRuntime(clock), telemetry: probe.telemetry });
    const emittedWhileCancelling = { cancelled: false };
    const straggler = cancelEmittingStraggler('straggler', {
      wakeEmit: ip('203.0.113.1'),
      finalEmit: ip('203.0.113.2'),
      clock,
      deadlineMs: 5 * SECOND_MS,
      emittedWhileCancelling,
    });
    const report = makeAggregator('report', { dependsOn: [IpDataPoint], onAggregate: writeIpCount });

    const result = await orchestrate(sid, {
      runtime,
      operators: [straggler, report],
      sessionDeadlineMs: 5 * SECOND_MS,
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED); // the deadline runs aggregation, not a wedge
    // The final emission really was produced from the cancellation handler, i.e. after the cancel
    // and after the loop's last in-loop drain — so nothing but the second drain could have kept it.
    expect(emittedWhileCancelling.cancelled).toBe(true);
    const stored = new Set(
      (await runtime.store.snapshot(sid)).ofType(IpDataPoint).map((dataPoint) => dataPoint.value),
    );
    expect(stored).toEqual(new Set(['203.0.113.1', '203.0.113.2'])); // BOTH the wake and the cancel-time emission
    expect(await probe.counterTotal('session_deadline_hits_total')).toBe(1); // the branch fired exactly once
    expect((await runtime.durable.read('reports', 'report'))?.document).toEqual({ ip_count: 2 }); // post-drain state
    expect(await runtime.lock.isHeld(sid)).toBe(false); // the epoch was released on the deadline path
  });
});
