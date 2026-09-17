/**
 * AGG — aggregation phase: idempotency, OCC, contribution markers, dead-letter, backoff.
 *
 * The `DurableStore` port contract runs here against the in-memory adapter (and against Mongo in
 * that adapter's suite). Everything else needs the orchestrator to run a whole session: an
 * aggregator runs in the aggregation phase, at most once per session, independently of its peers,
 * with each attempt bounded and the lease renewed between them.
 */

import { describe, expect, it } from 'vitest';
import {
  InMemoryDataPointStore,
  InMemoryDurableStore,
  InMemorySessionLock,
} from '../src/orcastork/adapters/memory/index.js';
import { AggregateStatus, AggregationHelpers, RetryPolicy } from '../src/orcastork/aggregation/index.js';
import type { AuditLogEntry } from '../src/orcastork/audit/index.js';
import { OperatorOutcome } from '../src/orcastork/audit/index.js';
import type { Clock } from '../src/orcastork/clock.js';
import { DataPointView } from '../src/orcastork/datapoints/index.js';
import { OptimisticConcurrencyError, StaleEpochError } from '../src/orcastork/exceptions.js';
import type { Epoch, OperatorId, SessionId } from '../src/orcastork/ids.js';
import {
  Epoch as toEpoch,
  NamespaceId as toNamespaceId,
  OperatorId as toOperatorId,
  SessionId as toSessionId,
} from '../src/orcastork/ids.js';
import { CapabilityView, EffectGuard, InvocationDelta, OperatorContext } from '../src/orcastork/operators/index.js';
import { Orchestrator, SessionStatus } from '../src/orcastork/orchestrator/index.js';
import { buildInMemoryRuntime, OrchestratorRuntime } from '../src/orcastork/runtime.js';
import { FakeClock } from './doubles/clock.js';
import { describeDurableStoreConformance } from './doubles/conformance/durable_store.js';
import { EmailDataPoint, observed, RiskDataPoint, risk, TriggerDataPoint, workEmail } from './doubles/datapoints.js';
import { captureLogs } from './doubles/logs.js';
import { abortableSleep, makeAggregator, makeOperator } from './doubles/operators.js';

const SID: SessionId = toSessionId('agg-session');
const NAMESPACE = toNamespaceId('agg-namespace');

/** Two attempts on a zero-width window: bounded retries with no real sleeping in tests. */
const FAST_RETRY = RetryPolicy({ maxAttempts: 2, baseDelayMs: 0 });

/** The helpers an aggregator context always carries — the port of Python's `assert` on it. */
const aggregationOf = (ctx: OperatorContext): AggregationHelpers => {
  if (ctx.aggregation === null) {
    throw new Error('an aggregator context always carries its aggregation helpers');
  }
  return ctx.aggregation;
};

/** The outcomes one operator's audited runs recorded, in order. */
const aggregatorRuns = (entries: readonly AuditLogEntry[], operatorId: OperatorId): readonly OperatorOutcome[] =>
  entries
    .filter((entry) => entry.operatorId === operatorId)
    .flatMap((entry) => (entry.operator === null ? [] : [entry.operator.outcome]));

/** In-memory lock that counts lease renewals (to observe the per-attempt renew). */
class RenewCountingLock extends InMemorySessionLock {
  public renews = 0;

  public override async renew(sessionId: SessionId, options: { readonly epoch: Epoch }): Promise<void> {
    this.renews += 1;
    await super.renew(sessionId, options);
  }
}

/** In-memory lock that fences after N successful renews (models a takeover mid-aggregation). */
class FenceAfterRenewsLock extends InMemorySessionLock {
  private remaining: number;

  public constructor(clock: Clock, options: { readonly fenceAfter: number }) {
    super(clock);
    this.remaining = options.fenceAfter;
  }

  public override async renew(sessionId: SessionId, options: { readonly epoch: Epoch }): Promise<void> {
    if (this.remaining === 0) {
      throw new StaleEpochError('a higher epoch took over');
    }
    this.remaining -= 1;
    await super.renew(sessionId, options);
  }
}

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

describe('the aggregation phase', () => {
  it('runs an aggregator in the aggregation phase and writes its durable output', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const write = async (ctx: OperatorContext): Promise<void> => {
      await aggregationOf(ctx).upsert('reports', 'report', { risk_count: ctx.store.ofType(RiskDataPoint).length });
    };

    const scorer = makeOperator('s', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    const reporter = makeAggregator('rep', { dependsOn: [RiskDataPoint], onAggregate: write });
    await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [scorer, reporter],
      seed: [workEmail()],
    }).run();

    const document = await runtime.durable.read('reports', 'report');
    expect(document).not.toBeNull();
    expect(document?.document).toEqual({ risk_count: 1 });
  });

  it('makes a redundant re-drive a no-op through the contribution marker', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const calls: number[] = [];
    const write = async (): Promise<void> => {
      calls.push(1);
    };

    const scorer = makeOperator('s', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    const reporter = makeAggregator('rep', { dependsOn: [RiskDataPoint], onAggregate: write });
    for (let drive = 0; drive < 2; drive += 1) {
      // A redundant re-drive of the same session.
      await new Orchestrator({
        sessionId: SID,
        namespaceId: NAMESPACE,
        runtime,
        operators: [scorer, reporter],
        seed: [workEmail()],
      }).run();
    }

    expect(calls).toEqual([1]); // contributed at most once
  });

  it('dead-letters a failing aggregator, flagging its domain while its peers are unaffected', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const boom = async (): Promise<void> => {
      throw new Error('cannot aggregate');
    };
    const healthy = async (ctx: OperatorContext): Promise<void> => {
      await aggregationOf(ctx).upsert('reports', 'healthy', { ok: true });
    };

    const scorer = makeOperator('s', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    const failing = makeAggregator('failing', { dependsOn: [RiskDataPoint], onAggregate: boom });
    const good = makeAggregator('healthy', { dependsOn: [RiskDataPoint], onAggregate: healthy });
    const result = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [scorer, failing, good],
      seed: [workEmail()],
      retryPolicy: FAST_RETRY,
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED); // the session still completes
    // Recorded for re-drive.
    expect(new Set(result.deadLetters.map((dead) => dead.operatorId))).toEqual(new Set([toOperatorId('failing')]));
    expect(await runtime.durable.read('reports', 'healthy')).not.toBeNull(); // the peer is unaffected
  });

  it('adds to a set idempotently, by cardinality rather than by increment', async () => {
    const durable = new InMemoryDurableStore();
    await durable.addToSet('profiles', 'profile', 'sessions', 'session-1', { epoch: toEpoch(1) });
    const size = await durable.addToSet('profiles', 'profile', 'sessions', 'session-1', { epoch: toEpoch(1) }); // again

    expect(size).toBe(1); // set cardinality, never a double-counting increment
  });

  it('skips ephemeral DataPoints when an aggregator folds the store', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const captured: { persisted?: number } = {};
    const write = async (ctx: OperatorContext): Promise<void> => {
      captured.persisted = ctx.store.all().filter((dataPoint) => !dataPoint.isEphemeral).length;
    };

    const trigger = observed(TriggerDataPoint, 'go');
    const reporter = makeAggregator('rep', { dependsOn: [RiskDataPoint], onAggregate: write });
    await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [reporter],
      seed: [risk(), trigger],
    }).run();

    expect(captured.persisted).toBe(1); // the ephemeral Trigger is excluded
  });

  it('keeps aggregators independent, so a failing one does not block a healthy peer', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const boom = async (): Promise<void> => {
      throw new Error('boom');
    };
    const healthy = async (ctx: OperatorContext): Promise<void> => {
      await aggregationOf(ctx).upsert('reports', 'healthy', { ok: true });
    };

    const scorer = makeOperator('s', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    const failing = makeAggregator('failing', { dependsOn: [RiskDataPoint], onAggregate: boom });
    const good = makeAggregator('healthy', { dependsOn: [RiskDataPoint], onAggregate: healthy });
    await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [scorer, failing, good],
      seed: [workEmail()],
      retryPolicy: FAST_RETRY,
    }).run();

    expect(await runtime.durable.read('reports', 'healthy')).not.toBeNull(); // the failing peer did not block it
  });

  it('re-runs only the unfinished aggregator on a resume', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const calls = { a: 0, b: 0 };
    const runA = async (): Promise<void> => {
      calls.a += 1;
    };
    const runB = async (): Promise<void> => {
      calls.b += 1;
    };

    const scorer = makeOperator('s', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    const aggregatorA = makeAggregator('agg_a', { dependsOn: [RiskDataPoint], onAggregate: runA });
    await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [scorer, aggregatorA],
      seed: [workEmail()],
    }).run();
    expect(calls).toEqual({ a: 1, b: 0 });

    // "Resume": agg_a already completed; a newly-added agg_b is the only unfinished one.
    const aggregatorB = makeAggregator('agg_b', { dependsOn: [RiskDataPoint], onAggregate: runB });
    await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [scorer, aggregatorA, aggregatorB],
      seed: [workEmail()],
    }).run();

    expect(calls).toEqual({ a: 1, b: 1 }); // completed aggregator skipped, unfinished one re-driven
  });

  it('never lets an aggregator sink re-trigger gathering', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    let gatheringRuns = 0;

    const scorer = makeOperator('s', {
      dependsOn: [EmailDataPoint],
      produces: [RiskDataPoint],
      emitFactory: () => {
        gatheringRuns += 1;
        return [risk()];
      },
    });
    // Produces nothing → a sink.
    const reporter = makeAggregator('rep', { dependsOn: [RiskDataPoint] });
    await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [scorer, reporter],
      seed: [workEmail()],
    }).run();

    expect(gatheringRuns).toBe(1); // the gathering operator ran once; the sink did not re-trigger it
  });

  it('records a dead-lettered failure for re-drive', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const boom = async (): Promise<void> => {
      throw new Error('boom');
    };

    const scorer = makeOperator('s', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    const failing = makeAggregator('failing', { dependsOn: [RiskDataPoint], onAggregate: boom });
    const result = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [scorer, failing],
      seed: [workEmail()],
      retryPolicy: FAST_RETRY,
    }).run();

    expect(result.deadLetters.map((dead) => dead.operatorId)).toEqual([toOperatorId('failing')]);
  });

  it('audits each retry and then the success that recovered it', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    let attempts = 0;
    const failOnce = async (ctx: OperatorContext): Promise<void> => {
      attempts += 1;
      if (attempts === 1) {
        throw new OptimisticConcurrencyError('version conflict'); // the first attempt loses the OCC race
      }
      await aggregationOf(ctx).upsert('reports', 'report', { ok: true });
    };

    const scorer = makeOperator('scorer', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    const reporter = makeAggregator('reporter', { dependsOn: [RiskDataPoint], onAggregate: failOnce });
    const result = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [scorer, reporter],
      seed: [workEmail()],
      retryPolicy: FAST_RETRY,
    }).run();

    expect(result.deadLetters).toEqual([]); // it recovered on retry
    expect(aggregatorRuns(await runtime.audit.replay(SID), toOperatorId('reporter'))).toEqual([
      OperatorOutcome.FAILED,
      OperatorOutcome.SUCCEEDED,
    ]);
  });

  it('audits the dead-letter that follows exhausted retries', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const alwaysFail = async (): Promise<void> => {
      throw new OptimisticConcurrencyError('version conflict');
    };

    const scorer = makeOperator('scorer', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    const reporter = makeAggregator('reporter', { dependsOn: [RiskDataPoint], onAggregate: alwaysFail });
    const result = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [scorer, reporter],
      seed: [workEmail()],
      retryPolicy: FAST_RETRY,
    }).run();

    // Exhausted retries.
    expect(new Set(result.deadLetters.map((dead) => dead.operatorId))).toEqual(new Set([toOperatorId('reporter')]));
    // Every bounded attempt is audited, then a terminal dead-letter disposition.
    expect(aggregatorRuns(await runtime.audit.replay(SID), toOperatorId('reporter'))).toEqual([
      OperatorOutcome.FAILED,
      OperatorOutcome.FAILED,
      OperatorOutcome.DEAD_LETTERED,
    ]);
  });

  it('runs aggregators concurrently, so a retrying peer does not delay completion', async () => {
    // Aggregators are independent: the healthy one must finish while the failing one is still
    // working through its retry backoff — sequential execution would order it strictly after the
    // last attempt.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const order: string[] = [];
    const boom = async (): Promise<void> => {
      order.push('failing-attempt');
      throw new Error('cannot aggregate');
    };
    const healthy = async (ctx: OperatorContext): Promise<void> => {
      await aggregationOf(ctx).upsert('reports', 'healthy', { ok: true });
      order.push('healthy-done');
    };

    const scorer = makeOperator('s', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    const failing = makeAggregator('failing', { dependsOn: [RiskDataPoint], onAggregate: boom });
    const good = makeAggregator('healthy', { dependsOn: [RiskDataPoint], onAggregate: healthy });
    const result = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [scorer, failing, good],
      seed: [workEmail()],
      retryPolicy: RetryPolicy({ maxAttempts: 2, baseDelayMs: 5000, jitter: 0 }), // a real backoff window
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(new Set(result.deadLetters.map((dead) => dead.operatorId))).toEqual(new Set([toOperatorId('failing')]));
    expect(await runtime.durable.read('reports', 'healthy')).not.toBeNull();
    const lastFailingAttempt = order.lastIndexOf('failing-attempt');
    // Finished while the peer was still retrying.
    expect(order.indexOf('healthy-done')).toBeLessThan(lastFailingAttempt);
  });

  it('bounds a hung aggregator attempt by attempt and then dead-letters it', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const hangs = async (ctx: OperatorContext): Promise<void> => {
      await abortableSleep(5000, ctx.signal); // a durable write that never returns
    };

    const scorer = makeOperator('s', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    const hung = makeAggregator('hung', { dependsOn: [RiskDataPoint], onAggregate: hangs });
    const result = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [scorer, hung],
      seed: [workEmail()],
      operationTimeoutMs: 20, // bounds each aggregation attempt, exactly like the gathering bound
      retryPolicy: FAST_RETRY,
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED); // the session was not hung forever
    expect(new Set(result.deadLetters.map((dead) => dead.operatorId))).toEqual(new Set([toOperatorId('hung')]));
    expect(aggregatorRuns(await runtime.audit.replay(SID), toOperatorId('hung'))).toEqual([
      OperatorOutcome.FAILED,
      OperatorOutcome.FAILED,
      OperatorOutcome.DEAD_LETTERED,
    ]);
  });

  it('lets the per-operator policy timeout bound an aggregator attempt', async () => {
    // The per-operator policy timeout (not just the orchestrator-wide default, which stays at its
    // 30s default here and never fires) is what cuts each attempt short.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const hangs = async (ctx: OperatorContext): Promise<void> => {
      await abortableSleep(5000, ctx.signal);
    };

    const hung = makeAggregator('hung', { dependsOn: [RiskDataPoint], onAggregate: hangs, timeoutMs: 20 });
    const result = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [hung],
      seed: [risk()],
      retryPolicy: FAST_RETRY,
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(new Set(result.deadLetters.map((dead) => dead.operatorId))).toEqual(new Set([toOperatorId('hung')]));
  });

  it('renews the lease between retry attempts', async () => {
    const clock = new FakeClock();
    const counting = new RenewCountingLock(clock);
    const runtime = OrchestratorRuntime({ ...buildInMemoryRuntime(clock), lock: counting });
    const renewsSeen: number[] = [];
    const flaky = async (): Promise<void> => {
      renewsSeen.push(counting.renews);
      if (renewsSeen.length < 3) {
        throw new OptimisticConcurrencyError('version conflict');
      }
    };

    const reporter = makeAggregator('rep', { dependsOn: [RiskDataPoint], onAggregate: flaky });
    const result = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [reporter],
      seed: [risk()],
      retryPolicy: RetryPolicy({ maxAttempts: 3, baseDelayMs: 0 }),
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(result.deadLetters).toEqual([]);
    // Each attempt observed exactly one more renewal than its predecessor: the lease was extended
    // between attempts, not just once at phase start.
    expect(renewsSeen[1]).toBe((renewsSeen[0] ?? 0) + 1);
    expect(renewsSeen[2]).toBe((renewsSeen[1] ?? 0) + 1);
  });

  it('stops cleanly as superseded when the renew between attempts is fenced', async () => {
    // The phase-start renew succeeds; the per-attempt renew reveals a takeover. That must propagate
    // out of the retry loop (the hook runs outside the operation's try/catch) into a clean
    // SUPERSEDED stop — never be retried as if the aggregator itself had failed.
    const clock = new FakeClock();
    const runtime = OrchestratorRuntime({
      ...buildInMemoryRuntime(clock),
      lock: new FenceAfterRenewsLock(clock, { fenceAfter: 1 }),
    });

    const reporter = makeAggregator('rep', { dependsOn: [RiskDataPoint] });
    const result = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [reporter],
      seed: [risk()],
    }).run();

    expect(result.status).toBe(SessionStatus.SUPERSEDED);
    expect(result.deadLetters).toEqual([]); // the takeover is not an aggregator failure
  });

  it('audits a never-ready aggregator as skipped and names what it was missing', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    // RiskDataPoint is never produced, so the reporter never becomes ready: the session still
    // completes, but the unwritten output domain must be visible in the logs and the audit trail.
    const reporter = makeAggregator('reporter', { dependsOn: [RiskDataPoint] });
    const { records, result } = await captureLogs(
      async () =>
        await new Orchestrator({
          sessionId: SID,
          namespaceId: NAMESPACE,
          runtime,
          operators: [reporter],
          seed: [workEmail()],
        }).run(),
    );

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(aggregatorRuns(await runtime.audit.replay(SID), toOperatorId('reporter'))).toEqual([
      OperatorOutcome.SKIPPED,
    ]);
    const skipped = records.find((record) => record.fields.operator_id === toOperatorId('reporter'));
    expect(skipped?.fields.missing_data_points).toEqual(['RiskDataPoint']);
  });
});
