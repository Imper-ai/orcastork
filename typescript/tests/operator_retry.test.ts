/**
 * Gathering-operator retry: loop-scheduled backoff relaunch of failed runs.
 *
 * A gathering operator whose policy declares `retry` is relaunched by the gather loop on a backoff
 * window (the same due-time mechanics as a debounced rerun) instead of being abandoned on its first
 * failure. The loop owns the window — no task ever sleeps the backoff away in-line — so lease
 * renewal, the session deadline, and inbox draining stay live throughout, and quiescence waits for
 * an armed retry rather than breaking to aggregation.
 *
 * `run_with_retry`'s hook contract, `RetryPolicy`'s validation and the backoff schedule need no
 * session and live in `tests/aggregation_retry.test.ts`.
 */

import { describe, expect, it } from 'vitest';
import { RetryPolicy } from '../src/orcastork/aggregation/index.js';
import type { AuditLogEntry, OperatorAuditInfo } from '../src/orcastork/audit/index.js';
import { OperatorOutcome } from '../src/orcastork/audit/index.js';
import type { DataPointEmission } from '../src/orcastork/datapoints/index.js';
import type { OperatorId as OperatorIdType } from '../src/orcastork/ids.js';
import { NamespaceId, OperatorId, SessionId } from '../src/orcastork/ids.js';
import type { OperatorContext } from '../src/orcastork/operators/index.js';
import { Operator, OperatorPolicy, operator } from '../src/orcastork/operators/index.js';
import { Orchestrator, SessionStatus } from '../src/orcastork/orchestrator/index.js';
import { buildInMemoryRuntime } from '../src/orcastork/runtime.js';
import { FakeClock } from './doubles/clock.js';
import { EmailDataPoint, IpDataPoint, ip, RiskDataPoint, risk, workEmail } from './doubles/datapoints.js';
import { captureLogs } from './doubles/logs.js';
import type { Emitted } from './doubles/operators.js';
import { abortableSleep, makeAggregator, makeOperator } from './doubles/operators.js';

const SID = SessionId('retry-session');
const NAMESPACE = NamespaceId('retry-namespace');

const SECOND_MS = 1_000;

/** Every audited run of `operatorId`, in order — the port of Python's `_operator_runs`. */
const operatorRuns = (entries: readonly AuditLogEntry[], operatorId: OperatorIdType): readonly OperatorAuditInfo[] =>
  entries
    .filter((entry) => entry.operator !== null && entry.operatorId === operatorId)
    .map((entry) => entry.operator as OperatorAuditInfo);

describe('a gathering operator with a retry policy', () => {
  it('relaunches a failed operator until it succeeds', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    let attempts = 0;

    function* flaky(): Iterable<Emitted> {
      // Emitted on every attempt — the keyed-merge makes the retry idempotent.
      yield RiskDataPoint.emit(0.5);
      attempts += 1;
      if (attempts < 3) {
        throw new Error('transient blip');
      }
    }

    const flakyOperator = makeOperator('flaky', {
      dependsOn: [EmailDataPoint],
      produces: [RiskDataPoint],
      emitFactory: flaky,
      rerunOnNewData: false, // never reruns on data — the retry must relaunch it anyway
      retry: RetryPolicy({ maxAttempts: 3, baseDelayMs: 0 }),
    });
    const result = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [flakyOperator],
      seed: [workEmail()],
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(result.operatorRuns.get(OperatorId('flaky'))).toBe(3); // two failures + the recovering attempt
    const runs = operatorRuns(await runtime.audit.replay(SID), OperatorId('flaky'));
    expect(runs.map((run) => run.attempt)).toEqual([1, 2, 3]); // every attempt is audited with its number
    expect(runs.map((run) => run.outcome)).toEqual([
      OperatorOutcome.FAILED,
      OperatorOutcome.FAILED,
      OperatorOutcome.SUCCEEDED,
    ]);
    // The failed attempts' emissions were merged and re-emitted idempotently: one identity, not three.
    expect((await runtime.store.snapshot(SID)).ofType(RiskDataPoint)).toHaveLength(1);
  });

  it('keeps the single-failure behaviour when no retry policy is declared', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const failer = makeOperator('failer', { dependsOn: [EmailDataPoint], raiseError: new Error('boom') });

    const { records, result } = await captureLogs(
      async () =>
        new Orchestrator({
          sessionId: SID,
          namespaceId: NAMESPACE,
          runtime,
          operators: [failer],
          seed: [workEmail()],
        }).run(),
      { level: 'ERROR' },
    );

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(result.operatorRuns.get(OperatorId('failer'))).toBe(1); // abandoned after one attempt, as before
    const runs = operatorRuns(await runtime.audit.replay(SID), OperatorId('failer'));
    expect(runs.map((run) => [run.outcome, run.attempt])).toEqual([[OperatorOutcome.FAILED, 1]]);
    const failure = records.find((record) => record.fields.operator_id === OperatorId('failer'));
    expect(failure?.fields.error).toBeInstanceOf(Error); // still an ERROR carrying the failure, not a retry WARNING
  });

  it('waits out the backoff window before relaunching', async () => {
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const runTimes: number[] = [];

    const recordThenRecover = (): readonly Emitted[] => {
      runTimes.push(clock.monotonic());
      if (runTimes.length === 1) {
        throw new Error('transient blip');
      }
      return [risk()];
    };

    const flakyOperator = makeOperator('flaky', {
      dependsOn: [EmailDataPoint],
      produces: [RiskDataPoint],
      emitFactory: recordThenRecover,
      // Deterministic 8 s first backoff.
      retry: RetryPolicy({ maxAttempts: 2, baseDelayMs: 8 * SECOND_MS, jitter: 0 }),
    });
    const result = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [flakyOperator],
      seed: [workEmail()],
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(result.operatorRuns.get(OperatorId('flaky'))).toBe(2);
    const [first, second] = runTimes;
    expect((second ?? 0) - (first ?? 0)).toBeGreaterThanOrEqual(8 * SECOND_MS); // the full window was waited out
  });

  it('proceeds without wedging once the retries are exhausted', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const hopeless = makeOperator('hopeless', {
      dependsOn: [EmailDataPoint],
      raiseError: new Error('persistent failure'),
      retry: RetryPolicy({ maxAttempts: 2, baseDelayMs: 0 }),
    });
    const healthy = makeOperator('healthy', {
      dependsOn: [EmailDataPoint],
      produces: [RiskDataPoint],
      emits: [risk()],
    });

    const { records, result } = await captureLogs(async () =>
      new Orchestrator({
        sessionId: SID,
        namespaceId: NAMESPACE,
        runtime,
        operators: [hopeless, healthy],
        seed: [workEmail()],
      }).run(),
    );

    expect(result.status).toBe(SessionStatus.COMPLETED); // no wedge
    expect(result.operatorRuns.get(OperatorId('hopeless'))).toBe(2); // bounded by maxAttempts
    const runs = operatorRuns(await runtime.audit.replay(SID), OperatorId('hopeless'));
    expect(runs.map((run) => [run.outcome, run.attempt])).toEqual([
      [OperatorOutcome.FAILED, 1],
      [OperatorOutcome.FAILED, 2],
    ]);
    expect((await runtime.store.snapshot(SID)).ofType(RiskDataPoint)).toHaveLength(1); // the scheduler proceeded
    const exhausted = records.filter(
      (record) =>
        record.fields.operator_id === OperatorId('hopeless') && record.message.includes('no retry remaining'),
    );
    expect(exhausted).toHaveLength(1);
    expect(exhausted[0]?.level).toBe('WARNING');
  });

  it('waits for an armed retry before declaring quiescence and aggregating', async () => {
    // When the failure is consumed, nothing is running and nothing else is due: the loop must wait
    // out the armed retry (via the injected clock) rather than declare quiescence and aggregate.
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    let attempts = 0;
    const seen: { riskCount?: number } = {};

    const flaky = (): readonly Emitted[] => {
      attempts += 1;
      if (attempts === 1) {
        throw new Error('transient blip');
      }
      return [risk()];
    };

    const flakyOperator = makeOperator('flaky', {
      dependsOn: [EmailDataPoint],
      produces: [RiskDataPoint],
      emitFactory: flaky,
      retry: RetryPolicy({ maxAttempts: 2, baseDelayMs: 5 * SECOND_MS, jitter: 0 }),
    });
    const reporter = makeAggregator('rep', {
      dependsOn: [RiskDataPoint],
      onAggregate: async (ctx) => {
        seen.riskCount = ctx.store.ofType(RiskDataPoint).length;
      },
    });
    const result = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [flakyOperator, reporter],
      seed: [workEmail()],
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(seen).toEqual({ riskCount: 1 }); // aggregation only ran after the due retry produced its output
    expect(clock.monotonic()).toBeGreaterThanOrEqual(5 * SECOND_MS); // the backoff window was genuinely waited out
  });

  it('relaunches an armed retry exactly once even as new data arrives', async () => {
    // A rerunOnNewData operator fails attempt 1 and arms a retry. While the backoff window is armed,
    // a peer merges a new IpDataPoint it depends on. When the window comes due the operator must
    // relaunch EXACTLY once via the armed-retry path — the rerun/debounce path stands aside — and
    // the keyed-merge collapses the repeated emission to a single identity (no racing double).
    const runtime = buildInMemoryRuntime(new FakeClock());
    let attempts = 0;

    function* flaky(): Iterable<Emitted> {
      // Emitted every attempt; keyed-merge makes the relaunch idempotent.
      yield RiskDataPoint.emit(0.5);
      attempts += 1;
      if (attempts === 1) {
        throw new Error('transient blip');
      }
    }

    // A peer turns the seed email into a fresh Ip (new relevant data) for the flaky operator.
    const peer = makeOperator('peer', {
      dependsOn: [EmailDataPoint],
      produces: [IpDataPoint],
      emits: [ip('203.0.113.9')],
    });
    const flakyOperator = makeOperator('flaky', {
      dependsOn: [IpDataPoint],
      produces: [RiskDataPoint],
      emitFactory: flaky,
      rerunOnNewData: true,
      debounceMs: 0,
      retry: RetryPolicy({ maxAttempts: 2, baseDelayMs: 5 * SECOND_MS, jitter: 0 }), // deterministic window
    });
    const result = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [peer, flakyOperator],
      seed: [workEmail()],
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(result.operatorRuns.get(OperatorId('flaky'))).toBe(2); // exactly one relaunch, not a second racing one
    const runs = operatorRuns(await runtime.audit.replay(SID), OperatorId('flaky'));
    expect(runs.map((run) => run.outcome)).toEqual([OperatorOutcome.FAILED, OperatorOutcome.SUCCEEDED]);
    expect((await runtime.store.snapshot(SID)).ofType(RiskDataPoint)).toHaveLength(1); // de-duplicated identity
  });

  it('makes an armed retry terminal once the cycle breaker has tripped', async () => {
    // A self-cycle operator with BOTH a retry policy (maxAttempts > maxCycles) and a persistent
    // failure: its retried runs consume breaker budget; once the breaker trips (maxCycles), the
    // retry must become terminal even with attempts remaining — otherwise it ping-pongs forever.
    const runtime = buildInMemoryRuntime(new FakeClock());

    function* emitThenFail(): Iterable<Emitted> {
      yield IpDataPoint.emit('203.0.113.1');
      throw new Error('persistent failure');
    }

    const selfCycle = makeOperator('cyclic', {
      dependsOn: [IpDataPoint],
      produces: [IpDataPoint],
      rerunOnNewData: true,
      maxCycles: 2,
      emitFactory: emitThenFail,
      retry: RetryPolicy({ maxAttempts: 5, baseDelayMs: 0 }), // far more attempts than maxCycles
    });

    const { records, result } = await captureLogs(async () =>
      new Orchestrator({
        sessionId: SID,
        namespaceId: NAMESPACE,
        runtime,
        operators: [selfCycle],
        seed: [ip('seed')],
      }).run(),
    );

    expect(result.status).toBe(SessionStatus.COMPLETED); // bounded, not looping forever
    expect(result.operatorRuns.get(OperatorId('cyclic')) ?? 0).toBeLessThanOrEqual(2); // bounded by maxCycles
    const terminal = records.filter(
      (record) => record.fields.operator_id === OperatorId('cyclic') && record.message.includes('no retry remaining'),
    );
    expect(terminal.length).toBeGreaterThan(0);
    expect(terminal.at(-1)?.level).toBe('WARNING'); // logged terminal, not retry-scheduled
  });

  it('arms a retry on a timeout failure and relaunches it', async () => {
    // A per-operator timeout that fires on attempt 1 must be treated exactly like an exception
    // failure: the bound raises, the fault boundary records it as a FAILED attempt, and that arms a
    // retry. Attempt 2 (fast this time) succeeds and persists its emission. The sleep here is a REAL
    // sleep so the REAL timeout actually fires.
    const runtime = buildInMemoryRuntime(new FakeClock());
    let attempts = 0;

    class Timed extends Operator {
      public static readonly operatorId = OperatorId('timed');
      public static readonly policy = OperatorPolicy({
        rerunOnNewData: false,
        timeoutMs: 20,
        retry: RetryPolicy({ maxAttempts: 2, baseDelayMs: 0 }),
      });
      public static readonly dependsOn = [EmailDataPoint];
      public static readonly produces = [RiskDataPoint];

      public async *run(ctx: OperatorContext): AsyncIterable<DataPointEmission> {
        yield RiskDataPoint.emit(0.7);
        attempts += 1;
        if (attempts === 1) {
          await abortableSleep(500, ctx.signal); // blows the 20 ms policy timeout on the first attempt only
        }
      }
    }
    operator(Timed);

    const result = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [Timed],
      seed: [workEmail()],
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(result.operatorRuns.get(OperatorId('timed'))).toBe(2);
    const runs = operatorRuns(await runtime.audit.replay(SID), OperatorId('timed'));
    expect(runs.map((run) => [run.outcome, run.attempt])).toEqual([
      [OperatorOutcome.FAILED, 1],
      [OperatorOutcome.SUCCEEDED, 2],
    ]);
    expect((await runtime.store.snapshot(SID)).ofType(RiskDataPoint)).toHaveLength(1); // recovering emission persisted
  });
});

describe('a dead-lettered aggregator that already wrote durable output', () => {
  it('keeps its upsert and re-drives idempotently on resume', async () => {
    // An aggregator upserts a durable doc on each attempt, then raises; retries exhaust → dead-letter.
    // markContribution runs only on a fully successful run, so the dead-lettered aggregator is NOT
    // marked — and its durable write survives the dead-letter. On resume it is re-launched (still
    // unmarked) and, because upsert re-reads the OCC version each attempt, the re-drive is
    // idempotent: the final document is correct, not double-counted or conflicting.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const scorer = makeOperator('s', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    // The same aggregator identity must be re-driven across both runs (the contribution marker keys
    // on it), so one class is reused; a counter makes session 1's attempts raise and session 2's
    // succeed.
    let raiseUntil = 0;

    const aggregator = makeAggregator('agg', {
      dependsOn: [RiskDataPoint],
      onAggregate: async (ctx) => {
        expect(ctx.aggregation).not.toBeNull();
        // OCC upsert re-reads the current version each attempt, so re-running over a surviving doc
        // cannot double-count or conflict — the value is recomputed from the (unchanged) snapshot.
        await ctx.aggregation?.upsert('reports', 'x', { risk_count: ctx.store.ofType(RiskDataPoint).length });
        raiseUntil += 1;
        if (raiseUntil <= 2) {
          // Session 1's two attempts raise → dead-letter; session 2 succeeds.
          throw new Error('boom after the durable write');
        }
      },
    });

    const first = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [scorer, aggregator],
      seed: [workEmail()],
      retryPolicy: RetryPolicy({ maxAttempts: 2, baseDelayMs: 0 }),
    }).run();

    expect(first.status).toBe(SessionStatus.COMPLETED);
    expect(new Set(first.deadLetters.map((dead) => dead.operatorId))).toEqual(new Set([OperatorId('agg')]));
    expect(await runtime.durable.isContributionMarked(SID, OperatorId('agg'))).toBe(false); // never marked
    const survived = await runtime.durable.read('reports', 'x');
    expect(survived).not.toBeNull();
    expect(survived?.document).toEqual({ risk_count: 1 }); // the durable write survived

    // Resume: the same (now unmarked) aggregator re-runs (not skipped); its OCC upsert re-reads the
    // surviving version — no version conflict — leaving the document correct.
    const second = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [scorer, aggregator],
      seed: [workEmail()],
    }).run();

    expect(second.status).toBe(SessionStatus.COMPLETED);
    expect(second.deadLetters).toEqual([]); // the re-drive succeeded this time
    expect(await runtime.durable.isContributionMarked(SID, OperatorId('agg'))).toBe(true); // now marked
    const final = await runtime.durable.read('reports', 'x');
    expect(final).not.toBeNull();
    expect(final?.document).toEqual({ risk_count: 1 }); // idempotent: not double-counted
  });

  it('keeps the set cardinality across its addToSet, the dead-letter and the resume', async () => {
    // The addToSet variant: each attempt adds the same set member then raises → dead-letter. Because
    // addToSet is set-keyed (not an increment), the dead-lettered attempts plus the resume re-drive
    // must leave the set at exactly the intended cardinality, never a duplicated/double-counted value.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const scorer = makeOperator('s', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    let raiseUntil = 0;
    const cardinality: { size?: number | undefined } = {};

    const aggregator = makeAggregator('agg', {
      dependsOn: [RiskDataPoint],
      onAggregate: async (ctx) => {
        expect(ctx.aggregation).not.toBeNull();
        cardinality.size = await ctx.aggregation?.addToSet('profiles', 'profile', 'sessions', String(SID));
        raiseUntil += 1;
        if (raiseUntil <= 2) {
          // Session 1's two attempts raise → dead-letter; session 2 succeeds.
          throw new Error('boom after the set-add');
        }
      },
    });

    const first = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [scorer, aggregator],
      seed: [workEmail()],
      retryPolicy: RetryPolicy({ maxAttempts: 2, baseDelayMs: 0 }),
    }).run();
    expect(new Set(first.deadLetters.map((dead) => dead.operatorId))).toEqual(new Set([OperatorId('agg')]));

    await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [scorer, aggregator],
      seed: [workEmail()],
    }).run();

    // Exactly the intended single member despite retry + dead-letter + resume.
    expect(cardinality).toEqual({ size: 1 });
  });
});
