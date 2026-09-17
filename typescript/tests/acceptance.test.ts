/**
 * ACC — Tier-0 walking-skeleton acceptance tests (in-memory + FakeClock).
 *
 * These are the headline end-to-end behaviours that prove the engine exists. ACC-04 (kill +
 * resume) lands with the manager; ACC-06 (aggregator dead-letter) with the aggregation retry.
 */

import { describe, expect, it } from 'vitest';
import {
  InMemoryCapabilityCatalog,
  InMemoryDataPointArchive,
  InMemorySessionLock,
} from '../src/orcastork/adapters/memory/index.js';
import { RetryPolicy } from '../src/orcastork/aggregation/index.js';
import { AuditKind } from '../src/orcastork/audit/index.js';
import { StaleEpochError } from '../src/orcastork/exceptions.js';
import { FlowDefinition } from '../src/orcastork/flow.js';
import { CapabilityId, NamespaceId, OperatorId, SessionId } from '../src/orcastork/ids.js';
import { SessionOrchestrationManager } from '../src/orcastork/manager/index.js';
import type { OperatorContext } from '../src/orcastork/operators/index.js';
import type { OrchestratorOptions } from '../src/orcastork/orchestrator/index.js';
import { Orchestrator, SessionStatus } from '../src/orcastork/orchestrator/index.js';
import { buildInMemoryRuntime, OrchestratorRuntime as makeRuntime } from '../src/orcastork/runtime.js';
import { makeCapability } from './doubles/capabilities.js';
import { FakeClock } from './doubles/clock.js';
import {
  ChatAnswerDataPoint,
  chatAnswer,
  EmailDataPoint,
  IpDataPoint,
  ip,
  observed,
  RiskDataPoint,
  risk,
  workEmail,
} from './doubles/datapoints.js';
import { captureLogs } from './doubles/logs.js';
import { makeAggregator, makeOperator } from './doubles/operators.js';

const SID = SessionId('acc-session');
const NAMESPACE = NamespaceId('acc-namespace');

const SECOND_MS = 1_000;

/** The lock TTL default is 30 s, so this is a lease that has certainly lapsed. */
const PAST_TTL_MS = 31 * SECOND_MS;

/** An orchestrator over the shared session/namespace, so a test names only what it varies. */
const orchestrate = (options: Omit<OrchestratorOptions, 'sessionId' | 'namespaceId'>): Orchestrator =>
  new Orchestrator({ sessionId: SID, namespaceId: NAMESPACE, ...options });

/** Archive whose `flush` raises — a fault on the `run()`-finally cleanup path. */
class FlushFailingArchive extends InMemoryDataPointArchive {
  public override async flush(): Promise<number> {
    throw new Error('archive flush exploded');
  }
}

/** Lock whose renew always reports a takeover — drives the run into a SUPERSEDED stop. */
class FenceOnRenewLock extends InMemorySessionLock {
  public override async renew(): Promise<void> {
    throw new StaleEpochError('a higher epoch took over');
  }
}

describe('the walking skeleton', () => {
  it('seeds, runs an operator, quiesces, aggregates and completes', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const writeReport = async (ctx: OperatorContext): Promise<void> => {
      expect(ctx.aggregation).not.toBeNull();
      await ctx.aggregation?.upsert('reports', 'risk-report', {
        risk_count: ctx.store.ofType(RiskDataPoint).length,
      });
    };
    const scorer = makeOperator('scorer', {
      dependsOn: [EmailDataPoint],
      produces: [RiskDataPoint],
      emits: [risk(0.9)],
    });
    const reporter = makeAggregator('risk_report', { dependsOn: [RiskDataPoint], onAggregate: writeReport });

    const result = await orchestrate({ runtime, operators: [scorer, reporter], seed: [workEmail()] }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(result.operatorRuns.get(OperatorId('scorer'))).toBe(1); // ran exactly once
    expect((await runtime.durable.read('reports', 'risk-report'))?.document).toEqual({ risk_count: 1 }); // durable
    expect(await runtime.lock.isHeld(SID)).toBe(false); // epoch released
    expect(new Set((await runtime.audit.replay(SID)).map((entry) => entry.kind))).toContain(
      AuditKind.DATA_POINT_ADDED,
    );
  });

  it('unblocks an operator when its capability comes online', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock(), {
      catalog: new InMemoryCapabilityCatalog({ permitted: [[NAMESPACE, [CapabilityId('netcap')]]] }),
    });
    const seeder = makeOperator('seeder', { produces: [IpDataPoint], emits: [ip()] });
    const netcap = makeCapability('netcap', { dependsOn: [IpDataPoint] }); // available once an Ip exists
    const consumer = makeOperator('netconsumer', {
      requires: [netcap],
      produces: [RiskDataPoint],
      emits: [risk()],
    });

    const result = await orchestrate({ runtime, operators: [seeder, consumer], capabilities: [netcap] }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(result.operatorRuns.get(OperatorId('netconsumer'))).toBe(1); // unblocked once netcap came online
    expect(new Set((await runtime.store.snapshot(SID)).all().map((dataPoint) => dataPoint.type))).toEqual(
      new Set(['ip', 'risk']),
    );
  });

  it('reflects an inbox DataPoint in the aggregate', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const scorer = makeOperator('scorer', {
      dependsOn: [ChatAnswerDataPoint],
      produces: [RiskDataPoint],
      rerunOnNewData: true,
      emitFactory: (ctx) => [
        observed(RiskDataPoint, ctx.store.ofType(ChatAnswerDataPoint).length, { by: OperatorId('scorer') }),
      ],
    });
    const reporter = makeAggregator('risk_report', {
      dependsOn: [RiskDataPoint],
      onAggregate: async (ctx) => {
        const latest = ctx.latest(RiskDataPoint);
        await ctx.aggregation?.upsert('reports', 'risk', { score: latest === null ? null : latest.value });
      },
    });
    // A user action arrives via the durable inbox.
    await runtime.inbox.append(SID, chatAnswer('second'));

    const result = await orchestrate({
      runtime,
      operators: [scorer, reporter],
      seed: [chatAnswer('first')],
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    // Both the seed and the inbox answer are reflected.
    expect((await runtime.durable.read('reports', 'risk'))?.document).toEqual({ score: 2 });
  });

  it('converges a bounded cycle and halts it at the breaker', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    let ipCounter = 0;
    let riskCounter = 0;
    const opA = makeOperator('cyc_a', {
      produces: [IpDataPoint],
      dependsOn: [RiskDataPoint],
      rerunOnNewData: true,
      maxCycles: 2,
      emitFactory: () => {
        const value = `ip-${ipCounter}`;
        ipCounter += 1;
        return [observed(IpDataPoint, value, { by: OperatorId('cyc_a') })];
      },
    });
    const opB = makeOperator('cyc_b', {
      produces: [RiskDataPoint],
      dependsOn: [IpDataPoint],
      rerunOnNewData: true,
      maxCycles: 2,
      emitFactory: () => {
        const value = riskCounter;
        riskCounter += 1;
        return [observed(RiskDataPoint, value, { by: OperatorId('cyc_b') })];
      },
    });

    const result = await orchestrate({ runtime, operators: [opA, opB], seed: [ip('seed')] }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED); // the loop terminated (no wedge)
    // Bounded by the circuit-breaker cap.
    expect(result.operatorRuns.get(OperatorId('cyc_a')) ?? 0).toBeGreaterThanOrEqual(1);
    expect(result.operatorRuns.get(OperatorId('cyc_a')) ?? 0).toBeLessThanOrEqual(2);
    expect(result.operatorRuns.get(OperatorId('cyc_b')) ?? 0).toBeGreaterThanOrEqual(1);
    expect(result.operatorRuns.get(OperatorId('cyc_b')) ?? 0).toBeLessThanOrEqual(2);
  });

  it('dead-letters a failing aggregator but still completes the session', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const scorer = makeOperator('scorer', {
      dependsOn: [EmailDataPoint],
      produces: [RiskDataPoint],
      emits: [risk()],
    });
    const failing = makeAggregator('failing_report', {
      dependsOn: [RiskDataPoint],
      onAggregate: async () => {
        throw new Error('cannot aggregate');
      },
    });
    const healthyReport = makeAggregator('healthy_report', {
      dependsOn: [RiskDataPoint],
      onAggregate: async (ctx) => {
        expect(ctx.aggregation).not.toBeNull();
        await ctx.aggregation?.upsert('reports', 'healthy-report', { ok: true });
      },
    });

    const result = await orchestrate({
      runtime,
      operators: [scorer, failing, healthyReport],
      seed: [workEmail()],
      retryPolicy: RetryPolicy({ maxAttempts: 2, baseDelayMs: 0 }),
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED); // reaches COMPLETED despite the failure
    expect(result.deadLetters.some((dead) => dead.operatorId === OperatorId('failing_report'))).toBe(true);
    expect(await runtime.durable.read('reports', 'healthy-report')).not.toBeNull(); // other aggregator unaffected
  });

  it('yields output identical to a clean run when killed mid-gathering and resumed', async () => {
    const writeReport = async (ctx: OperatorContext): Promise<void> => {
      expect(ctx.aggregation).not.toBeNull();
      await ctx.aggregation?.upsert('reports', 'risk-report', {
        risk_count: ctx.store.ofType(RiskDataPoint).length,
      });
    };

    // Same operator classes drive both runs (the registry holds one definition).
    const scorer = makeOperator('scorer', {
      dependsOn: [EmailDataPoint],
      produces: [RiskDataPoint],
      emits: [risk(0.9)],
    });
    const reporter = makeAggregator('risk_report', { dependsOn: [RiskDataPoint], onAggregate: writeReport });
    const operators = [scorer, reporter];

    // Baseline: a clean, uninterrupted run.
    const clean = buildInMemoryRuntime(new FakeClock());
    await orchestrate({ runtime: clean, operators, seed: [workEmail()] }).run();
    const cleanReport = await clean.durable.read('reports', 'risk-report');

    // Crash mid-gathering: a predecessor held epoch 1 and wrote the seed, then died (lock expires).
    const crashClock = new FakeClock();
    const crashed = buildInMemoryRuntime(crashClock);
    const predecessorEpoch = await crashed.lock.acquire(SID);
    await crashed.store.write(SID, [workEmail()], { epoch: predecessorEpoch });
    crashClock.advance(PAST_TTL_MS);
    const manager = new SessionOrchestrationManager(crashed);

    const resumed = await manager.resume({
      sessionId: SID,
      namespaceId: NAMESPACE,
      flow: new FlowDefinition({ name: 'acc-flow', operators }),
    });
    const resumedReport = await crashed.durable.read('reports', 'risk-report');

    expect(resumed).not.toBeNull();
    expect(resumed?.epoch).toBe(2); // resumed under a higher epoch
    expect(cleanReport).not.toBeNull();
    expect(resumedReport).not.toBeNull();
    expect(cleanReport?.document).toEqual(resumedReport?.document); // identical durable output
  });

  it('still releases the epoch when the cleanup archive flush fails', async () => {
    // The run()-finally flushes the write-behind archive, then releases the lock — a nested
    // try/finally whose whole point is that a flush raising still lets lock.release run. A stranded
    // epoch would block recovery for the full lease TTL. With archive.flush raising, the exception
    // surfaces (it is not silently swallowed) but the epoch MUST still be released.
    const runtime = makeRuntime({ ...buildInMemoryRuntime(new FakeClock()), archive: new FlushFailingArchive() });
    const scorer = makeOperator('scorer', {
      dependsOn: [EmailDataPoint],
      produces: [RiskDataPoint],
      emits: [risk()],
    });

    // The flush fault is not masked away.
    await expect(orchestrate({ runtime, operators: [scorer], seed: [workEmail()] }).run()).rejects.toThrow(
      'archive flush exploded',
    );
    expect(await runtime.lock.isHeld(SID)).toBe(false); // released even though the archive flush raised
    // The audit is a separate, independently durable write path, so the trail of the run that failed
    // its archive flush is intact — the durable record of a failure must survive the failure.
    expect(new Set((await runtime.audit.replay(SID)).map((entry) => entry.kind))).toContain(
      AuditKind.DATA_POINT_ADDED,
    );
  });

  it('still releases the epoch when a flush fails during a superseded stop', async () => {
    // Error-during-error on the cleanup path: the run is ALREADY ending SUPERSEDED (a fenced renew)
    // when archive.flush ALSO raises in the finally. The lock must still be released — a held epoch
    // on a superseded predecessor would strand the session against its own successor for the lease
    // TTL.
    const clock = new FakeClock();
    const runtime = makeRuntime({
      ...buildInMemoryRuntime(clock),
      lock: new FenceOnRenewLock(clock),
      archive: new FlushFailingArchive(),
    });
    const selfCycle = makeOperator('selfloop', {
      produces: [IpDataPoint],
      dependsOn: [IpDataPoint],
      rerunOnNewData: true,
      maxCycles: 3,
      debounceMs: 20 * SECOND_MS, // forces a loop sleep crossing the renew interval → fenced renew
      emitFactory: () => [ip('looped')],
    });

    const { records } = await captureLogs(
      async () => {
        await expect(orchestrate({ runtime, operators: [selfCycle], seed: [ip('seed')] }).run()).rejects.toThrow(
          'archive flush exploded',
        );
      },
      { level: 'WARNING' },
    );

    // The SUPERSEDED stop was reached.
    expect(records.some((record) => record.message.includes('fenced by a higher epoch'))).toBe(true);
    expect(await runtime.lock.isHeld(SID)).toBe(false); // release ran despite the flush fault
  });
});
