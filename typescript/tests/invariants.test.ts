/**
 * INV — cross-cutting invariants, as parametrized example tables over adversarial inputs.
 *
 * No property-based testing library is used; each invariant is exercised over a small hand-picked
 * set of sequences/interleavings.
 */

import { describe, expect, it } from 'vitest';
import {
  InMemoryAuditSink,
  InMemoryDataPointStore,
  InMemoryDurableStore,
  InMemoryInbox,
} from '../src/orcastork/adapters/memory/index.js';
import { RetryPolicy } from '../src/orcastork/aggregation/index.js';
import { AuditKind, AuditLogEntry } from '../src/orcastork/audit/index.js';
import { computeAvailable } from '../src/orcastork/capabilities/index.js';
import type { AnyDataPoint, DataPointClass, DataPointEmission } from '../src/orcastork/datapoints/index.js';
import { StaleEpochError } from '../src/orcastork/exceptions.js';
import type { CapabilityId, SessionId as SessionIdType } from '../src/orcastork/ids.js';
import { Epoch, NamespaceId, OperatorId, SessionId, CapabilityId as toCapabilityId } from '../src/orcastork/ids.js';
import type { ConcreteOperatorClass, OperatorContext } from '../src/orcastork/operators/index.js';
import { Operator, OperatorPolicy, operator } from '../src/orcastork/operators/index.js';
import { Orchestrator, SessionStatus } from '../src/orcastork/orchestrator/index.js';
import { buildInMemoryRuntime, OrchestratorRuntime } from '../src/orcastork/runtime.js';
import { makeCapability } from './doubles/capabilities.js';
import { FakeClock } from './doubles/clock.js';
import {
  ChatAnswerDataPoint,
  chatAnswer,
  EmailDataPoint,
  GeoDataPoint,
  IpDataPoint,
  ip,
  personalEmail,
  RiskDataPoint,
  risk,
  WorkEmailDataPoint,
  workEmail,
} from './doubles/datapoints.js';
import { captureLogs } from './doubles/logs.js';
import type { Emitted } from './doubles/operators.js';
import { makeAggregator, makeOperator } from './doubles/operators.js';
import { TelemetryProbe } from './doubles/otel.js';

const SID = SessionId('inv-session');
const NAMESPACE = NamespaceId('inv-namespace');

const SECOND_MS = 1_000;

describe('INV-01 dedup is idempotent under reordering', () => {
  it.each([[[0, 1, 2]], [[2, 1, 0]], [[1, 0, 2]], [[0, 0, 1, 2, 1]], [[2, 2, 2, 0, 1]]])(
    'collapses the writes of %j to the same three identities',
    async (order) => {
      const points: readonly AnyDataPoint[] = [
        workEmail('a@e.example'),
        personalEmail('p@e.example'),
        ip('203.0.113.1'),
      ];
      const store = new InMemoryDataPointStore();
      for (const index of order) {
        await store.write(SID, [points[index] as AnyDataPoint], { epoch: Epoch(1) });
      }
      const identities = new Set(
        (await store.snapshot(SID)).all().map((dataPoint) => `${dataPoint.type}:${String(dataPoint.value)}`),
      );
      expect(identities).toEqual(new Set(['work_email:a@e.example', 'personal_email:p@e.example', 'ip:203.0.113.1']));
    },
  );
});

describe('INV-02 at-least-once delivery does not change the output', () => {
  it.each([1, 2, 5])('collapses %i deliveries of the same action to one identity', async (deliveries) => {
    const runtime = buildInMemoryRuntime(new FakeClock());

    for (let index = 0; index < deliveries; index += 1) {
      // The same user action delivered N times.
      await runtime.inbox.append(SID, chatAnswer('same-answer'));
    }
    const reporter = makeAggregator('rep', {
      dependsOn: [ChatAnswerDataPoint],
      onAggregate: async (ctx) => {
        expect(ctx.aggregation).not.toBeNull();
        await ctx.aggregation?.upsert('reports', 'report', { answers: ctx.store.ofType(ChatAnswerDataPoint).length });
      },
    });
    await new Orchestrator({ sessionId: SID, namespaceId: NAMESPACE, runtime, operators: [reporter] }).run();

    const document = await runtime.durable.read('reports', 'report');
    expect(document).not.toBeNull();
    expect(document?.document).toEqual({ answers: 1 }); // deliveries collapse to one identity
  });
});

describe('INV-03 fencing rejects a superseded epoch', () => {
  it.each([[[1, 2, 1]], [[2, 1]], [[1, 1, 2, 1]], [[2, 2, 1]]])(
    'never lets the interleaving %j land a write from a superseded epoch',
    async (interleaving) => {
      const store = new InMemoryDataPointStore();
      let highest = 0;
      for (const [position, epoch] of interleaving.entries()) {
        if (epoch < highest) {
          await expect(store.write(SID, [ip(`${position}`)], { epoch: Epoch(epoch) })).rejects.toThrow(
            StaleEpochError,
          );
        } else {
          await store.write(SID, [ip(`${position}`)], { epoch: Epoch(epoch) });
          highest = epoch;
        }
      }
    },
  );
});

describe('INV-04 availability is monotonic', () => {
  it.each<{ readonly label: string; readonly additions: readonly DataPointClass<AnyDataPoint>[] }>([
    { label: 'work_email', additions: [WorkEmailDataPoint] },
    { label: 'ip then work_email', additions: [IpDataPoint, WorkEmailDataPoint] },
    { label: 'work_email, ip, geo', additions: [WorkEmailDataPoint, IpDataPoint, GeoDataPoint] },
    { label: 'geo twice then work_email', additions: [GeoDataPoint, GeoDataPoint, WorkEmailDataPoint] },
  ])('only ever adds capabilities as $label arrive', ({ additions }) => {
    const emailCap = makeCapability('email_cap', { dependsOn: [WorkEmailDataPoint] });
    const ipCap = makeCapability('ip_cap', { dependsOn: [IpDataPoint] });
    const registered = new Map([
      [emailCap.capabilityId, emailCap],
      [ipCap.capabilityId, ipCap],
    ]);
    const permitted = new Set([toCapabilityId('email_cap'), toCapabilityId('ip_cap')]);

    const present = new Set<DataPointClass<AnyDataPoint>>();
    let previous: ReadonlySet<CapabilityId> = new Set();
    for (const added of additions) {
      present.add(added);
      const available = computeAvailable({ registered, permitted, presentTypes: present });
      // Adding DataPoints only ever adds capabilities.
      expect([...previous].every((id) => available.has(id))).toBe(true);
      previous = available;
    }
  });
});

describe('INV-05 the revision strictly increases', () => {
  it.each([1, 3, 5])('grows over %i writes', async (count) => {
    const store = new InMemoryDataPointStore();
    let last = 0;
    for (let index = 0; index < count; index += 1) {
      const revision = await store.write(SID, [ip(`203.0.113.${index}`)], { epoch: Epoch(1) });
      expect(revision).toBeGreaterThan(last);
      last = revision;
    }
  });
});

describe('INV-06 a running operator is never cancelled', () => {
  it.each([0, 1, 3])('lets the operator finish with %i extra arrivals in flight', async (extraArrivals) => {
    const runtime = buildInMemoryRuntime(new FakeClock());

    const emitThree = (): readonly Emitted[] => [ip('a'), ip('b'), ip('c')];

    const multi = makeOperator('multi', {
      dependsOn: [EmailDataPoint],
      produces: [IpDataPoint],
      emitFactory: emitThree,
    });
    for (let index = 0; index < extraArrivals; index += 1) {
      // New data arriving must not cancel the running operator.
      await runtime.inbox.append(SID, chatAnswer(`arrival-${index}`));
    }
    await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [multi],
      seed: [workEmail()],
    }).run();

    const emitted = new Set((await runtime.store.snapshot(SID)).ofType(IpDataPoint).map((dp) => dp.value));
    expect(emitted).toEqual(new Set(['a', 'b', 'c'])); // every emission landed → the operator ran to completion
  });
});

describe('INV-07 a session always completes', () => {
  it.each<{ readonly flow: string }>([{ flow: 'simple' }, { flow: 'failing_aggregator' }])(
    'never wedges the $flow flow',
    async ({ flow }) => {
      const runtime = buildInMemoryRuntime(new FakeClock());
      const operators: ConcreteOperatorClass[] =
        flow === 'simple'
          ? [makeOperator('s', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] })]
          : [
              makeAggregator('failing', {
                dependsOn: [EmailDataPoint],
                onAggregate: async () => {
                  throw new Error('permanently failing');
                },
              }),
            ];

      const result = await new Orchestrator({
        sessionId: SID,
        namespaceId: NAMESPACE,
        runtime,
        operators,
        seed: [workEmail()],
        retryPolicy: RetryPolicy({ maxAttempts: 2, baseDelayMs: 0 }),
      }).run();

      expect(result.status).toBe(SessionStatus.COMPLETED); // never wedges
    },
  );
});

describe('INV-08 every write path honours the epoch', () => {
  it('fences the store, the durable upsert, the audit append and the inbox ack alike', async () => {
    // Store CAS
    const store = new InMemoryDataPointStore();
    await store.write(SID, [workEmail()], { epoch: Epoch(2) });
    await expect(store.write(SID, [ip()], { epoch: Epoch(1) })).rejects.toThrow(StaleEpochError);

    // Durable OCC upsert
    const durable = new InMemoryDurableStore();
    await durable.upsert('docs', 'k', {}, { expectedVersion: 0, epoch: Epoch(2) });
    await expect(durable.upsert('docs', 'k', { x: 1 }, { expectedVersion: 1, epoch: Epoch(1) })).rejects.toThrow(
      StaleEpochError,
    );

    // Audit append
    const audit = new InMemoryAuditSink();
    await audit.append(
      AuditLogEntry({
        sessionId: SID,
        epoch: Epoch(2),
        timestamp: workEmail().firstRetrieved,
        kind: AuditKind.DATA_POINT_ADDED,
      }),
    );
    await expect(
      audit.append(
        AuditLogEntry({
          sessionId: SID,
          epoch: Epoch(1),
          timestamp: workEmail().firstRetrieved,
          kind: AuditKind.DATA_POINT_ADDED,
        }),
      ),
    ).rejects.toThrow(StaleEpochError);

    // Inbox ack
    const inbox = new InMemoryInbox();
    const first = await inbox.append(SID, workEmail());
    const second = await inbox.append(SID, personalEmail());
    await inbox.consume(SID);
    await inbox.ack(SID, first, { epoch: Epoch(2) });
    await expect(inbox.ack(SID, second, { epoch: Epoch(1) })).rejects.toThrow(StaleEpochError);
  });
});

/**
 * Inbox whose FIRST `waitForEntry` makes an entry land in the SAME pass the park window elapses.
 *
 * The orchestrator's first inbox wait creates this waiter; when it runs it appends an entry to
 * itself and returns at once (entries now pending). Meanwhile the wait's park-bounded sleep
 * fast-forwards the fake clock to exactly `parkAt`. Both complete in the same pass, so the NEXT
 * `while (!settled)` check sees a settled waiter and resolves the tie as 'data beats parking' — the
 * wakeup wins over the just-elapsed park window.
 */
class TieInbox extends InMemoryInbox {
  private tied = false;

  public override async waitForEntry(sessionId: SessionIdType): Promise<void> {
    if (!this.tied) {
      this.tied = true;
      await this.append(sessionId, chatAnswer('arrived-at-the-tie')); // an entry lands as the wait begins
    }
    await super.waitForEntry(sessionId);
  }
}

describe('INV-09 an inbox wakeup at the park tie resumes instead of parking', () => {
  it('resolves the tie as a wakeup and processes the entry', async () => {
    // The exact tie: an inbox entry is pending in the same pass that `now >= parkAt`. The loop checks
    // the settled waiter first, so the wakeup wins — the session must resume and process the entry
    // rather than PARK a session that actually had input ready. parkAfter is kept under the lease
    // renew interval so the clock fast-forward to parkAt does not trip a renew fence.
    const clock = new FakeClock();
    const probe = new TelemetryProbe();
    const parkAfterMs = 5 * SECOND_MS;
    const runtime = OrchestratorRuntime({
      ...buildInMemoryRuntime(clock),
      inbox: new TieInbox(),
      telemetry: probe.telemetry,
    });
    const scorer = makeOperator('scorer', {
      dependsOn: [ChatAnswerDataPoint],
      produces: [RiskDataPoint],
      emits: [risk()],
    });

    const result = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [scorer],
      completesWhen: RiskDataPoint, // only the inbox answer can satisfy this
      sessionDeadlineMs: 300 * SECOND_MS,
      parkAfterMs,
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED); // resumed on the wakeup, NOT parked
    expect(result.operatorRuns.get(OperatorId('scorer'))).toBe(1); // the tie-arriving entry was processed
    const answers = new Set(
      (await runtime.store.snapshot(SID)).ofType(ChatAnswerDataPoint).map((dataPoint) => dataPoint.value),
    );
    expect(answers).toEqual(new Set(['arrived-at-the-tie']));
    // The park window genuinely elapsed — a real tie, not an early wakeup.
    expect(clock.monotonic()).toBeGreaterThanOrEqual(parkAfterMs);
    const waitSpans = probe.spans('session.inbox_wait');
    expect(waitSpans.length).toBeGreaterThan(0);
    // The tie resolved as a wakeup, not 'parked'.
    expect(waitSpans.at(-1)?.attributes.outcome).toBe('wakeup');
  });
});

/** Store whose `revertEffect` is fenced — a takeover lands before the failing body's revert. */
class FenceOnRevertEffectStore extends InMemoryDataPointStore {
  public override async revertEffect(): Promise<void> {
    throw new StaleEpochError('a higher epoch took over before the effect revert landed');
  }
}

describe('INV-10 a fenced effect revert degrades and preserves the original failure', () => {
  it('absorbs the fenced revert, keeps the pending mark and re-raises the body error', async () => {
    // ctx.once's body throws, so EffectGuard reverts the claim during the unwind — but the revert is
    // fenced by a takeover. EffectGuard ABSORBS the failed revert (a logged warning, leaving the mark
    // pending for the successor's recovery policy) and re-raises the ORIGINAL body exception — the
    // cleanup-path failure under fencing must not mask the real error nor crash the operator with a
    // secondary exception. The operator's failure is isolated, so the session still reaches its
    // normal disposition.
    const runtime = OrchestratorRuntime({
      ...buildInMemoryRuntime(new FakeClock()),
      store: new FenceOnRevertEffectStore(),
    });
    const seenAcquired: { value?: boolean } = {};

    class FailingSender extends Operator {
      public static readonly operatorId = OperatorId('failing_sender');
      public static readonly policy = OperatorPolicy({ rerunOnNewData: false });
      public static readonly dependsOn = [EmailDataPoint];
      public static readonly produces = [RiskDataPoint];

      public async *run(ctx: OperatorContext): AsyncIterable<DataPointEmission> {
        await ctx.once('send-otp', (acquired) => {
          seenAcquired.value = acquired;
          if (acquired) {
            throw new Error('effect body blew up'); // forces the revert on the unwind
          }
        });
        yield RiskDataPoint.emit(0.1); // unreachable; keeps this an async generator
      }
    }
    operator(FailingSender);

    const { records, result } = await captureLogs(
      async () =>
        new Orchestrator({
          sessionId: SID,
          namespaceId: NAMESPACE,
          runtime,
          operators: [FailingSender],
          seed: [workEmail()],
        }).run(),
      { level: 'WARNING' },
    );

    expect(result.status).toBe(SessionStatus.COMPLETED); // the operator's failure and the fenced revert are isolated
    expect(seenAcquired.value).toBe(true); // the claim was acquired, so the revert path was taken on the throw
    // The fenced revert is absorbed (logged), leaving the claim as this epoch's pending mark for the
    // successor's RERUN/SKIP recovery — never deleted under a stale epoch, never a secondary crash.
    expect(await runtime.store.getEffectState(SID, 'failing_sender:send-otp')).toBe('pending:1');
    // Degraded, not raised as a new error.
    expect(records.some((record) => record.message.includes('revert failed'))).toBe(true);
    // The ORIGINAL body failure is what was logged as the operator failure (its emissions persisted).
    expect(records.some((record) => record.fields.operator_id === OperatorId('failing_sender'))).toBe(true);
  });
});

/**
 * A representative aggregator whose durable write straddles an injected await (a read→write gap).
 *
 * Half the cohort throws (dead-letters); half writes a durable doc. The injected `clock.sleep`
 * between observing and writing forces a real interleaving of the concurrently-gathered aggregator
 * runs, stressing the 'single loop, no locking' claim for shared orchestrator state (`deadLetters` /
 * `runs`).
 */
const slowDurableAggregator = (
  operatorId: string,
  options: { readonly table: string; readonly key: string; readonly deadLetter: boolean; readonly clock: FakeClock },
): ConcreteOperatorClass =>
  makeAggregator(operatorId, {
    dependsOn: [RiskDataPoint],
    onAggregate: async (ctx) => {
      await options.clock.sleep(0); // yields control — peers' mutations can interleave here
      if (options.deadLetter) {
        throw new Error(`${operatorId} cannot aggregate`);
      }
      expect(ctx.aggregation).not.toBeNull();
      await ctx.aggregation?.upsert(options.table, options.key, { op: operatorId });
    },
  });

describe('INV-11 concurrent aggregators keep exact dead-letter and run counts', () => {
  it('drops neither a dead-letter nor a run count under interleaving', async () => {
    // The 'single event loop, no locking' claim for shared orchestrator state, stress-asserted: 20
    // aggregators run concurrently (each yielding mid-aggregate so they genuinely interleave), half
    // dead-lettering and half succeeding. Every mutation of the dead-letter list / run counts is one
    // non-awaiting step, so NO interleaving may drop a dead-letter or a run count.
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const scorer = makeOperator('scorer', {
      dependsOn: [EmailDataPoint],
      produces: [RiskDataPoint],
      emits: [risk()],
    });
    const aggregators = Array.from({ length: 20 }, (_unused, index) =>
      slowDurableAggregator(`agg-${index}`, {
        table: 'reports',
        key: `doc-${index}`,
        deadLetter: index % 2 === 0,
        clock,
      }),
    );

    const result = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [scorer, ...aggregators],
      seed: [workEmail()],
      retryPolicy: RetryPolicy({ maxAttempts: 1, baseDelayMs: 0 }), // one shot: a failure dead-letters at once
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    const deadIds = new Set(result.deadLetters.map((dead) => dead.operatorId));
    const expectedDead = new Set(
      Array.from({ length: 20 }, (_unused, index) => index)
        .filter((index) => index % 2 === 0)
        .map((index) => OperatorId(`agg-${index}`)),
    );
    expect(deadIds).toEqual(expectedDead); // exactly the 10
    expect(result.deadLetters).toHaveLength(10); // no dead-letter dropped or double-counted
    const succeeded = Array.from({ length: 20 }, (_unused, index) => index).filter((index) => index % 2 === 1);
    expect(new Set(succeeded.map((index) => result.operatorRuns.get(OperatorId(`agg-${index}`)) ?? 0))).toEqual(
      new Set([1]),
    ); // each ran once
    for (const index of succeeded) {
      const document = await runtime.durable.read('reports', `doc-${index}`);
      expect(document).not.toBeNull();
      expect(document?.document).toEqual({ op: `agg-${index}` }); // every success persisted
    }
  });
});
