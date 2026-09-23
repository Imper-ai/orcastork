/**
 * RECOV — end-to-end crash-recovery scenarios (in-memory + a simulated kill switch).
 */

import { describe, expect, it } from 'vitest';
import { InMemoryDataPointStore, InMemoryInbox } from '../src/orcastork/adapters/memory/index.js';
import { RetryPolicy } from '../src/orcastork/aggregation/index.js';
import { parseDataPoint } from '../src/orcastork/datapoints/index.js';
import { StaleEpochError } from '../src/orcastork/exceptions.js';
import type { FlowDefinitionInit } from '../src/orcastork/flow.js';
import { FlowDefinition } from '../src/orcastork/flow.js';
import type { Epoch } from '../src/orcastork/ids.js';
import { NamespaceId, OperatorId, SessionId } from '../src/orcastork/ids.js';
import { SessionOrchestrationManager } from '../src/orcastork/manager/index.js';
import type { ConcreteOperatorClass, OperatorClass } from '../src/orcastork/operators/index.js';
import { Orchestrator, SessionStatus } from '../src/orcastork/orchestrator/index.js';
import type { OrchestratorRuntime } from '../src/orcastork/runtime.js';
import { buildInMemoryRuntime } from '../src/orcastork/runtime.js';
import { FakeClock } from './doubles/clock.js';
import {
  ChatAnswerDataPoint,
  chatAnswer,
  EmailDataPoint,
  IpDataPoint,
  ip,
  RiskDataPoint,
  risk,
  T0,
  workEmail,
} from './doubles/datapoints.js';
import type { MakeOperatorOptions } from './doubles/operators.js';
import { makeAggregator, makeOperator } from './doubles/operators.js';

const SID = SessionId('recov-session');
const NAMESPACE = NamespaceId('recov-namespace');

/** The in-memory lock's TTL is 30 s, so this is a predecessor lease that has certainly lapsed. */
const PAST_TTL_MS = 31_000;

const flowOf = (
  operators: readonly OperatorClass[],
  extras: Omit<FlowDefinitionInit, 'name' | 'operators'> = {},
): FlowDefinition => new FlowDefinition({ name: 'recov-flow', operators, ...extras });

/** A predecessor held an epoch and wrote the seed, then died; its lock then expires. */
const crashAfterSeed = async (runtime: OrchestratorRuntime, clock: FakeClock): Promise<Epoch> => {
  const epoch = await runtime.lock.acquire(SID);
  await runtime.store.write(SID, [workEmail()], { epoch });
  clock.advance(PAST_TTL_MS); // ownership lock TTL expires
  return epoch;
};

/** The DataPoint types the session holds, as the Python assertions read them. */
const storedTypes = async (runtime: OrchestratorRuntime): Promise<ReadonlySet<string>> =>
  new Set((await runtime.store.snapshot(SID)).all().map((dataPoint) => dataPoint.type));

/** The stub every scenario re-drives; only what it depends on varies. */
const scorerOn = (options: MakeOperatorOptions): ConcreteOperatorClass => makeOperator('scorer', options);

describe('crash recovery', () => {
  it('isolates an operator failure', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const boom = makeOperator('boom', { produces: [IpDataPoint], emits: [ip()], raiseError: new Error('x') });
    const healthy = makeOperator('healthy', { produces: [RiskDataPoint], emits: [risk()] });
    const result = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [boom, healthy],
    }).run();

    const types = await storedTypes(runtime);
    expect(result.status).toBe(SessionStatus.COMPLETED); // scheduler proceeded
    expect(types.has('ip')).toBe(true); // failed op's emission persisted
    expect(types.has('risk')).toBe(true); // peer ran
  });

  it('redelivers inbox entries on resume', async () => {
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const manager = new SessionOrchestrationManager(runtime);
    const epoch = await runtime.lock.acquire(SID);
    await runtime.store.write(SID, [workEmail()], { epoch });
    await runtime.inbox.append(SID, chatAnswer('pending')); // arrived but never processed
    clock.advance(PAST_TTL_MS);

    const scorer = scorerOn({ dependsOn: [ChatAnswerDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    await manager.resume({ sessionId: SID, namespaceId: NAMESPACE, flow: flowOf([scorer]) });

    const types = await storedTypes(runtime);
    expect(types.has('chat_answer')).toBe(true); // the inbox entry was not lost — redelivered on resume
    expect(types.has('risk')).toBe(true); // and the operator it unblocked ran
  });

  it('rejects a stale orchestrator’s writes', async () => {
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const manager = new SessionOrchestrationManager(runtime);
    const staleEpoch = await crashAfterSeed(runtime, clock);
    const scorer = scorerOn({ dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    await manager.resume({ sessionId: SID, namespaceId: NAMESPACE, flow: flowOf([scorer]) });

    // A fenced predecessor.
    await expect(runtime.store.write(SID, [ip()], { epoch: staleEpoch })).rejects.toThrow(StaleEpochError);
  });

  it('re-runs only the unfinished aggregators on resume', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const manager = new SessionOrchestrationManager(runtime);
    let calls = 0;

    const scorer = scorerOn({ dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    const reporter = makeAggregator('rep', {
      dependsOn: [RiskDataPoint],
      onAggregate: async (ctx) => {
        calls += 1;
        expect(ctx.aggregation).not.toBeNull();
        await ctx.aggregation?.upsert('reports', 'report', { v: 1 });
      },
    });
    await manager.startSession({
      sessionId: SID,
      namespaceId: NAMESPACE,
      flow: flowOf([scorer, reporter]),
      seed: [workEmail()],
    });
    expect(calls).toBe(1);

    // A redundant resume (same session) re-runs nothing — the aggregator's contribution is marked.
    await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [scorer, reporter],
    }).run();
    expect(calls).toBe(1);
  });

  it('completes with a dead-letter flag when an aggregator fails', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const scorer = scorerOn({ dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    const failing = makeAggregator('failing', {
      dependsOn: [RiskDataPoint],
      onAggregate: async () => {
        throw new Error('cannot aggregate');
      },
    });

    const result = await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [scorer, failing],
      seed: [workEmail()],
      retryPolicy: RetryPolicy({ maxAttempts: 2, baseDelayMs: 0 }),
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(new Set(result.deadLetters.map((dead) => dead.operatorId))).toEqual(new Set([OperatorId('failing')]));
  });

  it('keeps the audit when the live store is lost', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const scorer = scorerOn({ dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [scorer],
      seed: [workEmail()],
    }).run();

    expect((await runtime.audit.replay(SID)).length).toBeGreaterThan(0); // the audit is durable
    // Losing the live store loses the live session, but the audit is independent of it.
    expect((await new InMemoryDataPointStore().snapshot(SID)).all()).toHaveLength(0);
    expect((await runtime.audit.replay(SID)).length).toBeGreaterThan(0);
  });

  it('reaches the same final state on resume as on a clean run', async () => {
    const scorer = scorerOn({ dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    const operators = [scorer];

    const clean = buildInMemoryRuntime(new FakeClock());
    await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime: clean,
      operators,
      seed: [workEmail()],
    }).run();
    const cleanTypes = new Set((await clean.store.snapshot(SID)).all().map((dataPoint) => dataPoint.type));

    const crashClock = new FakeClock();
    const crashed = buildInMemoryRuntime(crashClock);
    await crashAfterSeed(crashed, crashClock);
    await new SessionOrchestrationManager(crashed).resume({
      sessionId: SID,
      namespaceId: NAMESPACE,
      flow: flowOf(operators),
    });
    const resumedTypes = new Set((await crashed.store.snapshot(SID)).all().map((dataPoint) => dataPoint.type));

    expect(cleanTypes).toEqual(resumedTypes); // identical readiness/availability ⇒ identical final state
  });

  it('ignores unknown fields when reading a record written by newer code', () => {
    // Rolling deploy: a record written by newer code carries a field this reader does not know.
    const raw = {
      type: 'work_email',
      value: 'a@e.example',
      retrieved_by: 'op',
      first_retrieved: T0,
      last_retrieved: T0,
      field_from_a_newer_version: 'ignored',
    };
    const restored = parseDataPoint(raw);
    expect(restored.type).toBe('work_email');
    expect('field_from_a_newer_version' in restored).toBe(false);
  });

  it('reclaims a claimed-but-unapplied inbox entry on resume', async () => {
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const manager = new SessionOrchestrationManager(runtime);
    const epoch = await runtime.lock.acquire(SID);
    await runtime.store.write(SID, [workEmail()], { epoch });
    await runtime.inbox.append(SID, chatAnswer('in-flight'));
    await runtime.inbox.consume(SID); // predecessor CLAIMED it, then died before applying/acking
    clock.advance(PAST_TTL_MS); // ownership lock TTL expires

    const scorer = scorerOn({ dependsOn: [ChatAnswerDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    await manager.resume({ sessionId: SID, namespaceId: NAMESPACE, flow: flowOf([scorer]) });

    const types = await storedTypes(runtime);
    expect(types.has('chat_answer')).toBe(true); // reclaimed on resume, not lost
    expect(types.has('risk')).toBe(true); // and the operator it unblocked ran
  });

  it('quarantines a poison inbox entry on resume instead of crash-looping', async () => {
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const manager = new SessionOrchestrationManager(runtime);
    const epoch = await runtime.lock.acquire(SID);
    await runtime.store.write(SID, [workEmail()], { epoch });
    const inbox = runtime.inbox;
    expect(inbox).toBeInstanceOf(InMemoryInbox);
    // A producer wrote bytes no deployment can decode (poison == bad wire payload); the
    // predecessor claimed the entry, then died before disposing of it.
    const poisonId = await (inbox as InMemoryInbox).appendSerialized(SID, 'not-json{');
    await inbox.consume(SID);
    clock.advance(PAST_TTL_MS); // ownership lock TTL expires

    const scorer = scorerOn({ dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    const result = await manager.resume({ sessionId: SID, namespaceId: NAMESPACE, flow: flowOf([scorer]) });

    expect(result).not.toBeNull();
    expect(result?.status).toBe(SessionStatus.COMPLETED); // the resume disposed of the poison, never crashed
    const quarantined = await runtime.inbox.quarantined(SID);
    expect(quarantined).toHaveLength(1);
    expect(quarantined[0]?.entryId).toBe(poisonId);
    expect(await runtime.inbox.pendingCount(SID)).toBe(0); // nothing left for the next resume to re-present
    expect((await storedTypes(runtime)).has('risk')).toBe(true); // the session still progressed
  });
});
