/**
 * MGR — SessionOrchestrationManager: spawn, orphan-resume, fencing, gate, catalog, flow drift.
 *
 * Also binds the `CooldownGate` contract, which Python binds in this same file
 * (`TestInMemoryCooldownGate`) because the gate is what backs the manager's scheduling gate.
 */

import { describe, expect, it, vi } from 'vitest';
import {
  InMemoryCapabilityCatalog,
  InMemoryCooldownGate,
  InMemoryDataPointStore,
  InMemorySessionLock,
} from '../src/orcastork/adapters/memory/index.js';
import type { AuditLogEntry, OperatorAuditInfo } from '../src/orcastork/audit/index.js';
import { AuditKind, OperatorOutcome } from '../src/orcastork/audit/index.js';
import type { AnyDataPoint } from '../src/orcastork/datapoints/index.js';
import { LockHeldError, SchedulingGateBlockedError, StaleEpochError } from '../src/orcastork/exceptions.js';
import type { FlowDefinitionInit } from '../src/orcastork/flow.js';
import { FlowDefinition } from '../src/orcastork/flow.js';
import type { Epoch, OperatorId, Revision } from '../src/orcastork/ids.js';
import { CapabilityId, NamespaceId, SessionId, OperatorId as toOperatorId } from '../src/orcastork/ids.js';
import { Deferred } from '../src/orcastork/internal/deferred.js';
import type { Redrive, ResumeOptions, SessionOrchestrationManagerOptions } from '../src/orcastork/manager/index.js';
import { SchedulingGate, SessionOrchestrationManager } from '../src/orcastork/manager/index.js';
import type { ConcreteOperatorClass, OperatorClass, OperatorContext } from '../src/orcastork/operators/index.js';
import type { OrchestratorOptions, OrchestratorResult } from '../src/orcastork/orchestrator/index.js';
import { SessionStatus } from '../src/orcastork/orchestrator/index.js';
import type { ApplyResolvedOptions } from '../src/orcastork/ports/index.js';
import type { OrchestratorRuntime } from '../src/orcastork/runtime.js';
import { buildInMemoryRuntime, OrchestratorRuntime as makeRuntime } from '../src/orcastork/runtime.js';
import { makeCapability } from './doubles/capabilities.js';
import { FakeClock } from './doubles/clock.js';
import { describeCooldownGateConformance } from './doubles/conformance/cooldown_gate.js';
import {
  ChatAnswerDataPoint,
  chatAnswer,
  EmailDataPoint,
  IpDataPoint,
  ip,
  observed,
  RiskDataPoint,
  risk,
  TriggerDataPoint,
  workEmail,
} from './doubles/datapoints.js';
import { captureLogs } from './doubles/logs.js';
import { makeAggregator, makeOperator } from './doubles/operators.js';

/**
 * Every orchestrator the manager spawned, captured by subclassing the class it imports.
 *
 * The port of Python's `monkeypatch.setattr(manager_module, 'Orchestrator', ...)`: a module's import
 * binding cannot be reassigned in ESM, so the module the manager imports is mocked with a subclass
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

const SID = SessionId('mgr-session');
const NAMESPACE = NamespaceId('mgr-namespace');
const SECOND_MS = 1_000;

/** The lock TTL default is 30 s, so this is a lease that has certainly lapsed. */
const PAST_TTL_MS = 31 * SECOND_MS;

/** A minimal ephemeral DataPoint for tests that need a late-ephemeral deliver. */
const trigger = (): TriggerDataPoint => observed(TriggerDataPoint, 'late-signal');

const flowOf = (
  operators: readonly OperatorClass[] = [],
  extras: Omit<FlowDefinitionInit, 'name' | 'operators'> = {},
): FlowDefinition => new FlowDefinition({ name: 'mgr-flow', operators, ...extras });

const scorer = (): ConcreteOperatorClass =>
  makeOperator('scorer', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });

/** A predecessor held epoch 1 and wrote the seed, then died; its lock then expires. */
const simulateCrashedPredecessor = async (runtime: OrchestratorRuntime, clock: FakeClock): Promise<Epoch> => {
  const epoch = await runtime.lock.acquire(SID);
  await runtime.store.write(SID, [workEmail()], { epoch });
  clock.advance(PAST_TTL_MS);
  return epoch;
};

/** Hand the event loop back once — the port of the Python tests' `await asyncio.sleep(0)`. */
const yieldOnce = (): Promise<void> =>
  new Promise<void>((resolve) => {
    setImmediate(resolve);
  });

/** Every operator run recorded in the trail, by operator id (the last one wins, as Python's dict does). */
const runsByOperator = (entries: readonly AuditLogEntry[]): ReadonlyMap<OperatorId, OperatorAuditInfo> => {
  const runs = new Map<OperatorId, OperatorAuditInfo>();
  for (const entry of entries) {
    if (entry.operator !== null && entry.operatorId !== null) {
      runs.set(entry.operatorId, entry.operator);
    }
  }
  return runs;
};

/**
 * A manager whose spawn and deferred recheck a test can drive.
 *
 * The port of Python's `manager._spawn_and_drain = ...` monkeypatch and its reach into
 * `manager._takeover_locks` / `manager._recheck_tasks`.
 */
class ProbeManager extends SessionOrchestrationManager {
  /** Stands in for the whole spawn when set, exactly as the Python stub does. */
  public blockingSpawn: (() => Promise<void>) | null = null;

  public constructor(runtime: OrchestratorRuntime, options: SessionOrchestrationManagerOptions = {}) {
    super(runtime, options);
  }

  public override async spawnAndDrain(
    sessionId: SessionId,
    namespaceId: NamespaceId,
    flow: FlowDefinition,
    seed: readonly AnyDataPoint[],
    freshDeadline = false,
  ): Promise<OrchestratorResult> {
    if (this.blockingSpawn !== null) {
      await this.blockingSpawn();
      // The stub never produces a result; the callers under test only observe that they got here.
      return undefined as unknown as OrchestratorResult;
    }
    return await super.spawnAndDrain(sessionId, namespaceId, flow, seed, freshDeadline);
  }

  /** How many per-session takeover entries this process currently holds. */
  public get takeoverEntryCount(): number {
    return this.takeoverLocks.size;
  }

  /** Schedule one deferred recheck directly, as Python calls `_schedule_recheck`. */
  public scheduleRecheckNow(options: ResumeOptions & { readonly redrive: Redrive }): void {
    this.scheduleRecheck(options);
  }
}

describeCooldownGateConformance({
  name: 'InMemoryCooldownGate',
  create: () => {
    const clock = new FakeClock();
    return Promise.resolve({
      gate: new InMemoryCooldownGate(clock),
      advanceTime: (ms: number) => {
        clock.advance(ms);
        return Promise.resolve();
      },
    });
  },
});

describe('the in-memory cooldown gate', () => {
  it('holds a half-open window, so the exact expiry tie admits', async () => {
    // The window is [T, T+C): at exactly now === expiresAt the cooldown has lapsed and a new start
    // is admitted (and re-arms); one tick earlier it is still closed. With a FakeClock this exact
    // equality is reachable, so the off-by-one boundary must have a defined, tested outcome.
    const clock = new FakeClock();
    const gate = new InMemoryCooldownGate(clock);

    expect(await gate.tryAcquire('tie', 60_000)).toBe(true); // arms expiresAt = T0 + 60_000
    clock.advance(59_999);
    expect(await gate.tryAcquire('tie', 60_000)).toBe(false); // 59_999 < 60_000 — still inside the closed front
    clock.advance(1); // now === expiresAt exactly
    expect(await gate.tryAcquire('tie', 60_000)).toBe(true); // now === expiresAt admits (strict `<`, not `<=`)
  });
});

describe('spawning and resuming', () => {
  it('spawns a new session with epoch 1', async () => {
    const manager = new SessionOrchestrationManager(buildInMemoryRuntime(new FakeClock()));

    const result = await manager.startSession({
      sessionId: SID,
      namespaceId: NAMESPACE,
      flow: flowOf([scorer()]),
      seed: [workEmail()],
    });

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(result.epoch).toBe(1);
  });

  it('resumes an orphan with a higher epoch', async () => {
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const manager = new SessionOrchestrationManager(runtime);
    await simulateCrashedPredecessor(runtime, clock);
    expect(await manager.isOrphaned(SID)).toBe(true);

    const result = await manager.resume({ sessionId: SID, namespaceId: NAMESPACE, flow: flowOf([scorer()]) });

    expect(result).not.toBeNull();
    expect(result?.epoch).toBe(2);
    expect(result?.status).toBe(SessionStatus.COMPLETED);
  });

  it('does not let a second resume enter the spawn while one is in flight', async () => {
    // A racing resume must not build an orchestrator only to discover it lost. The distributed lock
    // decides ownership, but it is acquired INSIDE the spawn. Without the per-session takeover guard
    // each racer first constructed an orchestrator and activated the whole capability set, learning
    // only afterwards that it had lost — so one takeover's Mongo/Redis/HTTP clients and secret
    // loading were paid once per racer. Four racers in 61 ms OOM-killed a 1Gi pod. Blocking inside
    // the spawn is what makes the overlap deterministic; without it the in-memory runtime finishes
    // the first resume before the second is scheduled and no race exists to observe.
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const manager = new ProbeManager(runtime);
    await simulateCrashedPredecessor(runtime, clock);
    const flow = flowOf([scorer()]);

    let entered = 0;
    const release = new Deferred<void>();
    manager.blockingSpawn = async () => {
      entered += 1;
      await release.promise;
    };

    const first = manager.resume({ sessionId: SID, namespaceId: NAMESPACE, flow });
    await yieldOnce(); // let the first reach the spawn and block there
    const second = manager.resume({ sessionId: SID, namespaceId: NAMESPACE, flow });
    await yieldOnce();

    // The property that prevents the OOM: no two resumes construct at the same time. Once the winner
    // finishes, a queued caller proceeding is correct — in production it then sees the session
    // complete and stands down, which the stub spawn here cannot reproduce.
    expect(entered).toBe(1);

    release.resolve();
    await Promise.all([first, second]);
  });

  it('keeps the takeover entry alive for the next caller while one resume is queued', async () => {
    // A lock is unlocked between the release and the woken waiter re-acquiring, so evicting the
    // per-session entry on "not locked" drops it while a caller is still queued. The next resume
    // then finds nothing mapped, mints a second lock, and enters the spawn alongside the queued one
    // — the concurrent full-orchestrator build (clients + secret loading, seconds long) this guard
    // exists to collapse. A name+email burst to a parked session overlapping the redelivery recheck
    // reaches three concurrent same-session resumes on routine traffic.
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const manager = new ProbeManager(runtime);
    await simulateCrashedPredecessor(runtime, clock);
    const flow = flowOf([scorer()]);

    let entered = 0;
    let inSpawn = 0;
    let peakInSpawn = 0;
    const winnerGate = new Deferred<void>();
    const openGate = new Deferred<void>();
    manager.blockingSpawn = async () => {
      entered += 1;
      const gate = entered === 1 ? winnerGate : openGate;
      inSpawn += 1;
      peakInSpawn = Math.max(peakInSpawn, inSpawn);
      await gate.promise;
      inSpawn -= 1;
    };

    const winner = manager.resume({ sessionId: SID, namespaceId: NAMESPACE, flow });
    await yieldOnce(); // the winner reaches the spawn and blocks there
    const queued = manager.resume({ sessionId: SID, namespaceId: NAMESPACE, flow });
    await yieldOnce(); // the second resume is now queued on the guard
    winnerGate.resolve();
    await yieldOnce(); // the winner returns and releases; the queued caller re-acquires and spawns
    const late = manager.resume({ sessionId: SID, namespaceId: NAMESPACE, flow });
    await yieldOnce();

    expect(peakInSpawn).toBe(1); // two resumes never build an orchestrator at once

    openGate.resolve();
    await Promise.all([winner, queued, late]);
    // Still evicted once nobody holds or waits, so a long-lived process keeps no lock per session touched.
    expect(manager.takeoverEntryCount).toBe(0);
  });

  it('rehydrates and re-drives on resume', async () => {
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const manager = new SessionOrchestrationManager(runtime);
    await simulateCrashedPredecessor(runtime, clock);

    const reporter = makeAggregator('rep', {
      dependsOn: [RiskDataPoint],
      onAggregate: async (ctx: OperatorContext) => {
        await ctx.aggregation?.upsert('reports', 'report', { risk_count: ctx.store.ofType(RiskDataPoint).length });
      },
    });
    await manager.resume({ sessionId: SID, namespaceId: NAMESPACE, flow: flowOf([scorer(), reporter]) });

    const document = await runtime.durable.read('reports', 'report');
    expect(document).not.toBeNull();
    expect(document?.document).toEqual({ risk_count: 1 }); // re-driven from persisted seed
  });

  it('rejects the stale predecessor writes after a resume', async () => {
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const manager = new SessionOrchestrationManager(runtime);
    const staleEpoch = await simulateCrashedPredecessor(runtime, clock);

    const result = await manager.resume({ sessionId: SID, namespaceId: NAMESPACE, flow: flowOf([scorer()]) });

    expect(result?.epoch).toBe(2);
    // The fenced predecessor cannot write.
    await expect(runtime.store.write(SID, [ip()], { epoch: staleEpoch })).rejects.toThrow(StaleEpochError);
  });

  it('re-activates capabilities fresh on every grant', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock(), {
      catalog: new InMemoryCapabilityCatalog({ permitted: [[NAMESPACE, [CapabilityId('netcap')]]] }),
    });
    const netcap = makeCapability('netcap', { dependsOn: [IpDataPoint] });
    const seeder = makeOperator('seeder', { produces: [IpDataPoint], emits: [ip()] });
    const consumer = makeOperator('consumer', {
      requires: [netcap],
      produces: [RiskDataPoint],
      emits: [risk()],
    });
    const operators = [seeder, consumer];

    await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators,
      capabilities: [netcap],
    }).run();
    const afterFirst = netcap.activations.length;
    // A fresh orchestrator on the same session re-activates the capability from credentials.
    await new Orchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators,
      capabilities: [netcap],
    }).run();

    expect(netcap.activations).toHaveLength(afterFirst + 1);
  });

  it('never resumes a completed session', async () => {
    const manager = new SessionOrchestrationManager(buildInMemoryRuntime(new FakeClock()));
    const flow = flowOf([scorer()]);
    await manager.startSession({ sessionId: SID, namespaceId: NAMESPACE, flow, seed: [workEmail()] });

    expect(await manager.resume({ sessionId: SID, namespaceId: NAMESPACE, flow })).toBeNull();
  });

  it('mints a strictly increasing epoch across grants', async () => {
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const manager = new SessionOrchestrationManager(runtime);
    const firstEpoch = await simulateCrashedPredecessor(runtime, clock); // grant 1

    const result = await manager.resume({ sessionId: SID, namespaceId: NAMESPACE, flow: flowOf([scorer()]) }); // grant 2

    expect(firstEpoch).toBe(1);
    expect(result?.epoch).toBe(2);
  });

  it('admits exactly one winner among concurrent grants', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());

    const outcomes = await Promise.allSettled([runtime.lock.acquire(SID), runtime.lock.acquire(SID)]);

    const epochs = outcomes.filter((outcome) => outcome.status === 'fulfilled');
    const conflicts = outcomes.filter(
      (outcome) => outcome.status === 'rejected' && outcome.reason instanceof LockHeldError,
    );
    expect(epochs).toHaveLength(1); // atomic mint → exactly one successor wins
    expect(conflicts).toHaveLength(1);
  });

  it('makes completion visible to a second manager', async () => {
    // Durable completion: a peer supervisor over the same backends sees the session finished.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const podA = new SessionOrchestrationManager(runtime);
    const podB = new SessionOrchestrationManager(runtime); // a separate supervisor sharing the same infra
    const flow = flowOf([scorer()]);
    await podA.startSession({ sessionId: SID, namespaceId: NAMESPACE, flow, seed: [workEmail()] });

    // The ownership lock is free again, yet podB must not treat the finished session as orphaned.
    expect(await podB.isOrphaned(SID)).toBe(false);
    expect(await podB.resume({ sessionId: SID, namespaceId: NAMESPACE, flow })).toBeNull();
  });

  it('stands down without raising when a resume loses the acquire race', async () => {
    /** Models the check/acquire window: isHeld reports free while the lease is in fact live. */
    class StaleIsHeldLock extends InMemorySessionLock {
      public override async isHeld(): Promise<boolean> {
        return false;
      }
    }

    const clock = new FakeClock();
    const lock = new StaleIsHeldLock(clock);
    const runtime = makeRuntime({ ...buildInMemoryRuntime(clock), lock });
    await lock.acquire(SID); // a concurrent resumer actually holds the lease

    const result = await new SessionOrchestrationManager(runtime).resume({
      sessionId: SID,
      namespaceId: NAMESPACE,
      flow: flowOf([scorer()]),
    });

    expect(result).toBeNull(); // lost the race → stand down; the winner's higher epoch fences us
  });

  it('does not call a never-started session orphaned', async () => {
    // Orphaned means "was started and its owner died". A session with no minted epoch was never
    // started, so there is no owner to have died and nothing persisted to rehydrate — reporting it
    // orphaned invites a resume that would spawn it from an empty store.
    const manager = new SessionOrchestrationManager(buildInMemoryRuntime(new FakeClock()));

    expect(await manager.isOrphaned(SID)).toBe(false);
  });

  it('stands down when resuming a never-started session', async () => {
    // Resume rehydrates from the persisted store; with no epoch ever minted there is nothing to
    // rehydrate, so spawning here would create a session whose only state is whatever the inbox
    // happens to hold.
    const runtime = buildInMemoryRuntime(new FakeClock());

    const result = await new SessionOrchestrationManager(runtime).resume({
      sessionId: SID,
      namespaceId: NAMESPACE,
      flow: flowOf([scorer()]),
    });

    expect(result).toBeNull();
    expect(await runtime.lock.currentEpoch(SID)).toBe(0); // stood down without minting
  });

  it('sees an operator gating change between grants on resume', async () => {
    const catalog = new InMemoryCapabilityCatalog({ permittedOperators: [[NAMESPACE, []]] }); // everything gated at first
    const runtime = buildInMemoryRuntime(new FakeClock(), { catalog });
    const manager = new SessionOrchestrationManager(runtime);
    const answerHandler = makeOperator('answer_handler', {
      dependsOn: [EmailDataPoint],
      produces: [RiskDataPoint],
      emits: [risk()],
    });
    const flow = flowOf([answerHandler], { completesWhen: RiskDataPoint, parkAfterMs: 20 * SECOND_MS });

    const parked = await manager.startSession({
      sessionId: SID,
      namespaceId: NAMESPACE,
      flow,
      seed: [workEmail()],
    });
    expect(parked.status).toBe(SessionStatus.PARKED); // gated → nothing could produce the risk; the session idled
    expect([...parked.operatorRuns]).toEqual([]);

    // Namespace config change between grants.
    catalog.setPermittedOperators(NAMESPACE, [toOperatorId('answer_handler')]);
    const result = await manager.resume({ sessionId: SID, namespaceId: NAMESPACE, flow });

    expect(result?.status).toBe(SessionStatus.COMPLETED);
    // The resumed grant sees the new gating.
    expect(result?.operatorRuns.get(toOperatorId('answer_handler'))).toBe(1);
    expect(await runtime.lock.isComplete(SID)).toBe(true);
  });
});

describe('the scheduling gate', () => {
  it('enforces the cooldown per key', async () => {
    const clock = new FakeClock();
    const gate = new SchedulingGate(new InMemoryCooldownGate(clock), { cooldownMs: 100 * SECOND_MS });

    expect(await gate.tryStart('device-1')).toBe(true); // the winning start arms the cooldown atomically
    expect(await gate.tryStart('device-1')).toBe(false); // within cooldown
    clock.advance(100 * SECOND_MS);
    expect(await gate.tryStart('device-1')).toBe(true); // cooldown elapsed
  });

  it('enforces the cooldown on startSession', async () => {
    const clock = new FakeClock();
    const gate = new SchedulingGate(new InMemoryCooldownGate(clock), { cooldownMs: 100 * SECOND_MS });
    const manager = new SessionOrchestrationManager(buildInMemoryRuntime(clock), { schedulingGate: gate });
    const flow = flowOf([scorer()]);

    const first = await manager.startSession({
      sessionId: SessionId('s1'),
      namespaceId: NAMESPACE,
      flow,
      seed: [workEmail()],
    });
    expect(first.status).toBe(SessionStatus.COMPLETED);

    // A second start for the same gate key (defaulting to the namespace) is refused while cooling down.
    await expect(
      manager.startSession({ sessionId: SessionId('s2'), namespaceId: NAMESPACE, flow, seed: [workEmail()] }),
    ).rejects.toThrow(SchedulingGateBlockedError);

    clock.advance(100 * SECOND_MS); // cooldown elapses
    const third = await manager.startSession({
      sessionId: SessionId('s3'),
      namespaceId: NAMESPACE,
      flow,
      seed: [workEmail()],
    });
    expect(third.status).toBe(SessionStatus.COMPLETED);
  });

  it('keeps distinct gate keys on independent cooldowns', async () => {
    const clock = new FakeClock();
    const gate = new SchedulingGate(new InMemoryCooldownGate(clock), { cooldownMs: 100 * SECOND_MS });
    const manager = new SessionOrchestrationManager(buildInMemoryRuntime(clock), { schedulingGate: gate });
    const flow = flowOf([scorer()]);

    await manager.startSession({
      sessionId: SessionId('d1'),
      namespaceId: NAMESPACE,
      flow,
      seed: [workEmail()],
      gateKey: 'device-1',
    });
    // 'device-1' is cooling down, but 'device-2' is an independent key and may start immediately.
    const second = await manager.startSession({
      sessionId: SessionId('d2'),
      namespaceId: NAMESPACE,
      flow,
      seed: [workEmail()],
      gateKey: 'device-2',
    });
    expect(second.status).toBe(SessionStatus.COMPLETED);

    await expect(
      manager.startSession({
        sessionId: SessionId('d3'),
        namespaceId: NAMESPACE,
        flow,
        seed: [workEmail()],
        gateKey: 'device-1',
      }),
    ).rejects.toThrow(SchedulingGateBlockedError);
  });

  it('exempts resume and deliver from the start cooldown', async () => {
    // The SchedulingGate decides when a NEW session may start; resume/deliver are recovery paths for
    // EXISTING sessions and must be exempt. A start arms the namespace cooldown, but an orphaned
    // session for that same namespace must still resume, and a parked one must still accept
    // delivery, well within the cooldown window. A regression that gated resume/deliver would
    // deadlock crash recovery behind the cooldown — so both must succeed despite the gate being
    // armed and still cooling down.
    const clock = new FakeClock();
    const gate = new SchedulingGate(new InMemoryCooldownGate(clock), { cooldownMs: 100 * SECOND_MS });
    const runtime = buildInMemoryRuntime(clock);
    const manager = new SessionOrchestrationManager(runtime, { schedulingGate: gate });
    const scorerFlow = flowOf([scorer()]); // one registered scorer reused across the start and the resume

    // A start for NAMESPACE arms the cooldown for the default gate key (the namespace); a second
    // start is now fenced.
    await manager.startSession({ sessionId: SessionId('starter'), namespaceId: NAMESPACE, flow: scorerFlow });
    await expect(
      manager.startSession({ sessionId: SessionId('blocked'), namespaceId: NAMESPACE, flow: scorerFlow }),
    ).rejects.toThrow(SchedulingGateBlockedError);

    // An orphaned session under the SAME namespace: a predecessor wrote the seed then died; its lock
    // lapses (31 s) but the 100 s cooldown is still active.
    const orphan = SessionId('orphan-under-cooldown');
    const orphanEpoch = await runtime.lock.acquire(orphan);
    await runtime.store.write(orphan, [workEmail()], { epoch: orphanEpoch });
    clock.advance(PAST_TTL_MS); // lock TTL expires; still 31 s ≪ 100 s cooldown

    const resumed = await manager.resume({ sessionId: orphan, namespaceId: NAMESPACE, flow: scorerFlow });
    expect(resumed).not.toBeNull(); // recovery is not fenced by the start cooldown
    expect(resumed?.status).toBe(SessionStatus.COMPLETED);

    // A parked session under the SAME namespace accepts a late delivery and resumes despite the live
    // cooldown.
    const parked = SessionId('parked-under-cooldown');
    const answerHandler = makeOperator('answer_handler', {
      dependsOn: [ChatAnswerDataPoint],
      produces: [RiskDataPoint],
      emits: [risk(0.9)],
    });
    const parkingFlow = flowOf([answerHandler], { completesWhen: RiskDataPoint, parkAfterMs: 20 * SECOND_MS });
    // Start under an independent gate key so the start itself isn't fenced; the deliver below still
    // uses NAMESPACE and must bypass the (still-armed) namespace cooldown regardless.
    const parkedResult = await manager.startSession({
      sessionId: parked,
      namespaceId: NAMESPACE,
      flow: parkingFlow,
      seed: [workEmail()],
      gateKey: 'parked-device',
    });
    expect(parkedResult.status).toBe(SessionStatus.PARKED); // holds no lock, not complete — the deliver/orphan branch
    expect(await manager.isOrphaned(parked)).toBe(true);

    const delivered = await manager.deliver({
      sessionId: parked,
      namespaceId: NAMESPACE,
      dataPoint: chatAnswer('it was me'),
      flow: parkingFlow,
    });
    expect(delivered).not.toBeNull(); // deliver-driven resume is not fenced by the start cooldown either
    expect(delivered?.status).toBe(SessionStatus.COMPLETED);
  });

  it('logs a warning when the gate blocks a start', async () => {
    const clock = new FakeClock();
    const gate = new SchedulingGate(new InMemoryCooldownGate(clock), { cooldownMs: 100 * SECOND_MS });
    const manager = new SessionOrchestrationManager(buildInMemoryRuntime(clock), { schedulingGate: gate });
    const flow = flowOf([scorer()]);
    await manager.startSession({ sessionId: SessionId('s1'), namespaceId: NAMESPACE, flow, seed: [workEmail()] });

    const { records } = await captureLogs(async () => {
      await expect(
        manager.startSession({ sessionId: SessionId('s2'), namespaceId: NAMESPACE, flow, seed: [workEmail()] }),
      ).rejects.toThrow(SchedulingGateBlockedError);
    });

    const blocked = records.filter(
      (record) => record.message === 'Session start blocked by the scheduling gate cool-down',
    );
    expect(blocked).toHaveLength(1);
    expect(blocked[0]?.fields.session_id).toBe(SessionId('s2'));
    expect(blocked[0]?.fields.gate_key).toBe(String(NAMESPACE));
  });
});

describe('delivery', () => {
  it('appends to a held session and rechecks after the lease', async () => {
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const answerHandler = makeOperator('answer_handler', {
      dependsOn: [ChatAnswerDataPoint],
      produces: [RiskDataPoint],
      emits: [risk(0.9)],
    });
    const manager = new SessionOrchestrationManager(runtime);
    await runtime.lock.acquire(SID); // a live orchestrator owns the session — and then silently dies

    const result = await manager.deliver({
      sessionId: SID,
      namespaceId: NAMESPACE,
      dataPoint: chatAnswer('answer'),
      flow: flowOf([answerHandler], { completesWhen: RiskDataPoint }),
    });

    expect(result).toBeNull(); // left to the (presumed live) owner
    expect(await runtime.inbox.pendingCount(SID)).toBe(1); // but the entry is already durable
    expect(manager.recheckTasks.size).toBe(1);
    await Promise.all(manager.recheckTasks); // the recheck fires after the lease TTL
    expect(await runtime.lock.isComplete(SID)).toBe(true); // the dead holder's session was resumed and completed
    expect(await runtime.inbox.pendingCount(SID)).toBe(0); // the delivery-window message was not lost
  });

  it('appends an ephemeral late deliver to a completed session but never re-drives it', async () => {
    // A late deliver to a completed session with no re-open requested is not re-driven: no re-open,
    // no recheck. (This is the default path; ephemerality is irrelevant — reopenIfComplete gates a
    // re-open.)
    const runtime = buildInMemoryRuntime(new FakeClock());
    const manager = new SessionOrchestrationManager(runtime);
    const flow = flowOf([scorer()]);
    await manager.startSession({ sessionId: SID, namespaceId: NAMESPACE, flow, seed: [workEmail()] });

    const result = await manager.deliver({
      sessionId: SID,
      namespaceId: NAMESPACE,
      dataPoint: trigger(),
      flow,
    });

    expect(result).toBeNull(); // no re-open requested -> the late entry is left to expire
    expect(manager.recheckTasks.size).toBe(0); // nothing is scheduled to re-drive a finished session
    expect(await runtime.lock.isComplete(SID)).toBe(true); // still complete — no re-open
  });

  it('appends before discarding a late deliver to a completed session', async () => {
    // deliver is crash-safe ingress: the durable inbox append always happens FIRST, *then* the
    // completed-session-without-reopen branch returns null. A late delivery (no re-open requested) to
    // a finished session leaves its entry durably appended and never processed (it expires with the
    // session state). A regression that moved the isComplete check ahead of the append would silently
    // drop this ingress write; pinning the pending count proves the append-first ordering.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const manager = new SessionOrchestrationManager(runtime);
    const flow = flowOf([scorer()]);
    await manager.startSession({ sessionId: SID, namespaceId: NAMESPACE, flow, seed: [workEmail()] });
    expect(await runtime.lock.isComplete(SID)).toBe(true); // the session finished
    expect(await runtime.inbox.pendingCount(SID)).toBe(0); // nothing in the inbox before the late delivery

    const result = await manager.deliver({ sessionId: SID, namespaceId: NAMESPACE, dataPoint: trigger(), flow });

    expect(result).toBeNull(); // no re-open requested -> the entry is never processed
    expect(await runtime.inbox.pendingCount(SID)).toBe(1); // but the append already landed durably, append-first
  });

  it('preserves an entry delivered before the first start without spawning', async () => {
    // A deliver that arrives before the session's first start must not become the session's creator:
    // the durable append still happens (delivery is never lost), but the spawn is left to
    // startSession, which is the only caller that carries the seed.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const manager = new SessionOrchestrationManager(runtime);

    const result = await manager.deliver({
      sessionId: SID,
      namespaceId: NAMESPACE,
      dataPoint: trigger(),
      flow: flowOf([scorer()]),
    });

    expect(result).toBeNull();
    expect(await runtime.inbox.pendingCount(SID)).toBe(1); // held for the real start to drain
    expect(await runtime.lock.currentEpoch(SID)).toBe(0);
  });

  it('does not strand the seed when a deliver races ahead of startSession', async () => {
    // The collection websocket spawns startSession as a task and begins receiving frames
    // immediately, so a client frame's deliver can reach its free-lock check before startSession
    // acquires. A deliver that spawned the session there would own the lock with an empty seed,
    // startSession would lose the acquire race, and the seed the flow depends on would never be
    // applied — the session then runs to its deadline collecting nothing it needs and finalizes no
    // result.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const manager = new SessionOrchestrationManager(runtime);
    const reporter = makeAggregator('rep', {
      dependsOn: [RiskDataPoint],
      onAggregate: async (ctx: OperatorContext) => {
        await ctx.aggregation?.upsert('reports', 'report', { risk_count: ctx.store.ofType(RiskDataPoint).length });
      },
    });
    const flow = flowOf([scorer(), reporter]);

    await manager.deliver({ sessionId: SID, namespaceId: NAMESPACE, dataPoint: trigger(), flow });
    const result = await manager.startSession({
      sessionId: SID,
      namespaceId: NAMESPACE,
      flow,
      seed: [workEmail()],
    });

    expect(result.epoch).toBe(1); // startSession owns the session, not the deliver that arrived first
    expect(result.status).toBe(SessionStatus.COMPLETED);
    const document = await runtime.durable.read('reports', 'report');
    expect(document?.document).toEqual({ risk_count: 1 }); // the seed was applied
  });

  it('resumes a parked session and folds the late answer in', async () => {
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const manager = new SessionOrchestrationManager(runtime);
    const answerHandler = makeOperator('answer_handler', {
      dependsOn: [ChatAnswerDataPoint],
      produces: [RiskDataPoint],
      emits: [risk(0.9)],
    });
    const flow = flowOf([answerHandler], { completesWhen: RiskDataPoint, parkAfterMs: 20 * SECOND_MS });

    const parked = await manager.startSession({
      sessionId: SID,
      namespaceId: NAMESPACE,
      flow,
      seed: [workEmail()],
    });
    expect(parked.status).toBe(SessionStatus.PARKED); // parkAfterMs threads through to the orchestrator
    expect(await manager.isOrphaned(SID)).toBe(true); // no lock + not complete — exactly what deliver resumes
    expect(manager.recheckTasks.size).toBe(0); // nothing else is scheduled to re-drive it

    clock.advance(100 * SECOND_MS); // the user answers much later, well within the persisted session budget
    const result = await manager.deliver({
      sessionId: SID,
      namespaceId: NAMESPACE,
      dataPoint: chatAnswer('it was me'),
      flow,
    });

    expect(result?.status).toBe(SessionStatus.COMPLETED);
    expect(result?.epoch).toBe(2); // a fresh, higher-epoch orchestrator resumed the parked session
    expect(result?.operatorRuns.get(toOperatorId('answer_handler'))).toBe(1); // the late answer was folded in
    expect(await runtime.lock.isComplete(SID)).toBe(true); // finalized this time — no further resume re-drives it
    expect(await runtime.inbox.pendingCount(SID)).toBe(0); // applied and acked — no message lost
  });

  it('logs the delivery dispositions', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const manager = new SessionOrchestrationManager(runtime);
    const flow = flowOf([scorer()]);
    await manager.startSession({ sessionId: SID, namespaceId: NAMESPACE, flow, seed: [workEmail()] });

    const { records } = await captureLogs(
      async () => {
        await manager.deliver({ sessionId: SID, namespaceId: NAMESPACE, dataPoint: trigger(), flow });
      },
      { level: 'DEBUG' },
    );

    expect(records.map((record) => record.message)).toContain('DataPoint delivered to the session inbox');
    // A late deliver to a finished session with no re-open requested is left to expire — that
    // disposition must be visible in logs.
    const ignored = records.filter((record) =>
      record.message.startsWith('Delivery to a completed session without a re-open request'),
    );
    expect(ignored).toHaveLength(1);
    expect(ignored[0]?.level).toBe('INFO');
    expect(ignored[0]?.fields.data_point_type).toBe('trigger');
  });

  it('logs an orphan resume at INFO', async () => {
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    await simulateCrashedPredecessor(runtime, clock);
    const manager = new SessionOrchestrationManager(runtime);

    const { records, result } = await captureLogs(
      async () => await manager.resume({ sessionId: SID, namespaceId: NAMESPACE, flow: flowOf([scorer()]) }),
      { level: 'INFO' },
    );

    expect(result?.status).toBe(SessionStatus.COMPLETED);
    const resumed = records.filter(
      (record) => record.message === 'Resuming orphaned session with a fresh higher-epoch orchestrator',
    );
    expect(resumed).toHaveLength(1);
    expect(resumed[0]?.fields).toEqual({
      logger_name: 'orcastork.manager.manager',
      session_id: SID,
      namespace_id: NAMESPACE,
      flow_name: 'mgr-flow',
    });
  });

  it('logs a failing deferred recheck without propagating it', async () => {
    class FailingResumeManager extends ProbeManager {
      public override async resume(_options: ResumeOptions): Promise<OrchestratorResult | null> {
        // An unexpected fault inside the fire-and-forget recheck.
        throw new Error('store unreachable');
      }
    }

    const manager = new FailingResumeManager(buildInMemoryRuntime(new FakeClock()));
    manager.scheduleRecheckNow({
      sessionId: SID,
      namespaceId: NAMESPACE,
      flow: flowOf([scorer()]),
      redrive: async (options) => await manager.resume(options),
    });

    const { records } = await captureLogs(
      async () => {
        // The swallowed failure must not surface here.
        await Promise.all(manager.recheckTasks);
      },
      { level: 'ERROR' },
    );

    const failed = records.filter((record) => record.message.startsWith('Deferred recheck failed'));
    expect(failed).toHaveLength(1);
    expect(failed[0]?.fields.error).toBeInstanceOf(Error); // logged with the error, not just the ids
    expect(failed[0]?.fields.session_id).toBe(SID);
    expect(failed[0]?.fields.namespace_id).toBe(NAMESPACE);
  });

  it('does not lose a straggler delivered to a held session that then completes', async () => {
    // `deliver` leaves a held session to its live owner and schedules a deferred re-drive — but that
    // re-drive is `resume`, and `resume` refuses a session that has since COMPLETED. So an entry
    // appended while the owner still held the lock is dropped the moment that owner finishes: the
    // recheck fires, sees the completion flag, and stands down. Nothing else re-drives it, the caller
    // was already told the delivery succeeded, and every stand-down on the path logs at DEBUG —
    // which is off in production.
    //
    // Real shape: a participant's name/email delivered just as their collection run is finishing.
    // The record keeps its `final` status, so nothing looks broken; the late identity is simply never
    // folded.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const manager = new SessionOrchestrationManager(runtime);
    const flow = flowOf([scorer()]);
    const epoch = await runtime.lock.acquire(SID); // a live owner is mid-run

    const result = await manager.deliver({
      sessionId: SID,
      namespaceId: NAMESPACE,
      dataPoint: workEmail(),
      flow,
      reopenIfComplete: true,
    });

    expect(result).toBeNull(); // left to the owner
    expect(await runtime.inbox.pendingCount(SID)).toBe(1); // durably appended

    // The owner finishes: marks complete, then releases — exactly what a completing collection run does.
    await runtime.lock.markComplete(SID, { epoch });
    await runtime.lock.release(SID, { epoch });

    await Promise.all(manager.recheckTasks);

    // The caller was told this was delivered, so it has to actually land.
    expect(await runtime.inbox.pendingCount(SID)).toBe(0);
  });
});

describe('flow-level tuning', () => {
  it('lets the flow session deadline override the orchestrator default', async () => {
    const clock = new FakeClock();
    const manager = new SessionOrchestrationManager(buildInMemoryRuntime(clock));
    const flow = flowOf([], { completesWhen: ChatAnswerDataPoint, sessionDeadlineMs: 30 * SECOND_MS }); // the answer never arrives

    const result = await manager.startSession({
      sessionId: SID,
      namespaceId: NAMESPACE,
      flow,
      seed: [workEmail()],
    });

    expect(result.status).toBe(SessionStatus.COMPLETED);
    // Bounded by the flow-level deadline, not the 300 s default.
    expect(clock.monotonic()).toBeGreaterThanOrEqual(30 * SECOND_MS);
    expect(clock.monotonic()).toBeLessThan(300 * SECOND_MS);
  });

  it('lets the flow operation timeout override the orchestrator default', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const manager = new SessionOrchestrationManager(runtime);
    const slow = makeOperator('slow', {
      produces: [IpDataPoint],
      emits: [ip('slow-ip')],
      sleepAfterMs: 5 * SECOND_MS,
    });
    const fast = makeOperator('fast', { produces: [RiskDataPoint], emits: [risk()] });

    const result = await manager.startSession({
      sessionId: SID,
      namespaceId: NAMESPACE,
      flow: flowOf([slow, fast], { operationTimeoutMs: 20 }),
    });

    expect(result.status).toBe(SessionStatus.COMPLETED);
    const runs = runsByOperator(await runtime.audit.replay(SID));
    // Cut by the flow-level bound, not 30 s.
    expect(runs.get(toOperatorId('slow'))?.outcome).toBe(OperatorOutcome.FAILED);
    expect(runs.get(toOperatorId('fast'))?.outcome).toBe(OperatorOutcome.SUCCEEDED);
  });

  it('reaches the spawned orchestrator with the flow emission-queue size', async () => {
    capture.spawned.length = 0;
    const runtime = buildInMemoryRuntime(new FakeClock());
    const manager = new SessionOrchestrationManager(runtime);
    const burst = makeOperator('burst', {
      dependsOn: [EmailDataPoint],
      produces: [IpDataPoint],
      emits: [1, 2, 3, 4, 5].map((n) => ip(`203.0.113.${n}`)),
    });

    const result = await manager.startSession({
      sessionId: SID,
      namespaceId: NAMESPACE,
      flow: flowOf([burst], { emissionQueueSize: 1 }),
      seed: [workEmail()],
    });

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(capture.spawned).toHaveLength(1);
    expect(capture.spawned[0]?.emissionQueueSize).toBe(1); // the flow-level bound reached the spawn
    const stored = new Set((await runtime.store.snapshot(SID)).ofType(IpDataPoint).map((dp) => dp.value));
    // The tight bound backpressured, never dropped.
    expect(stored).toEqual(new Set([1, 2, 3, 4, 5].map((n) => `203.0.113.${n}`)));
  });

  it('lets the flow max-inbox-deliveries override the orchestrator default', async () => {
    /** Rejects applies containing the marked value (a persistently unappliable inbox entry). */
    class RejectingStore extends InMemoryDataPointStore {
      public override async applyResolved(sessionId: SessionId, options: ApplyResolvedOptions): Promise<Revision> {
        if ([...options.added, ...options.updated].some((dataPoint) => dataPoint.value === 'merge-bomb')) {
          throw new Error('store rejected the write');
        }
        return await super.applyResolved(sessionId, options);
      }
    }

    const clock = new FakeClock();
    const runtime = makeRuntime({ ...buildInMemoryRuntime(clock), store: new RejectingStore() });
    const manager = new SessionOrchestrationManager(runtime);
    await runtime.inbox.append(SID, chatAnswer('merge-bomb'));
    const flow = flowOf([], {
      completesWhen: ChatAnswerDataPoint,
      sessionDeadlineMs: 30 * SECOND_MS,
      maxInboxDeliveries: 2,
    });

    const result = await manager.startSession({
      sessionId: SID,
      namespaceId: NAMESPACE,
      flow,
      seed: [workEmail()],
    });

    expect(result.status).toBe(SessionStatus.COMPLETED);
    const quarantined = await runtime.inbox.quarantined(SID);
    expect(quarantined).toHaveLength(1);
    expect(quarantined[0]?.deliveryCount).toBe(2); // quarantined at the flow-level cap, not the default 5
  });
});

describe('flow drift', () => {
  it('emits no drift audit when the same flow resumes', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const manager = new SessionOrchestrationManager(runtime);
    const answerHandler = makeOperator('answer_handler', {
      dependsOn: [ChatAnswerDataPoint],
      produces: [RiskDataPoint],
      emits: [risk()],
    });
    const flow = flowOf([answerHandler], { completesWhen: RiskDataPoint, parkAfterMs: 20 * SECOND_MS });

    const parked = await manager.startSession({
      sessionId: SID,
      namespaceId: NAMESPACE,
      flow,
      seed: [workEmail()],
    });
    expect(parked.status).toBe(SessionStatus.PARKED);
    const resumed = await manager.resume({ sessionId: SID, namespaceId: NAMESPACE, flow });

    expect(resumed?.status).toBe(SessionStatus.PARKED); // same flow → re-parked quietly
    const drift = (await runtime.audit.replay(SID)).filter((entry) => entry.kind === AuditKind.FLOW_DRIFT_DETECTED);
    expect(drift).toEqual([]);
    expect(await runtime.store.getFlowFingerprint(SID)).toBe(flow.fingerprint()); // persisted at the first spawn
  });

  it('audits a changed flow once, then stays quiet', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const manager = new SessionOrchestrationManager(runtime);
    const answerHandler = makeOperator('answer_handler', {
      dependsOn: [ChatAnswerDataPoint],
      produces: [RiskDataPoint],
      emits: [risk()],
    });
    const lateAddition = makeOperator('late_addition', {
      dependsOn: [IpDataPoint],
      produces: [RiskDataPoint],
      emits: [risk()],
    });
    const original = flowOf([answerHandler], { completesWhen: RiskDataPoint, parkAfterMs: 20 * SECOND_MS });
    const changed = flowOf([answerHandler, lateAddition], {
      completesWhen: RiskDataPoint,
      parkAfterMs: 20 * SECOND_MS,
    });

    const parked = await manager.startSession({
      sessionId: SID,
      namespaceId: NAMESPACE,
      flow: original,
      seed: [workEmail()],
    });
    expect(parked.status).toBe(SessionStatus.PARKED);

    const firstResume = await manager.resume({ sessionId: SID, namespaceId: NAMESPACE, flow: changed });
    expect(firstResume?.status).toBe(SessionStatus.PARKED); // drift never blocks the session
    const drifts = (await runtime.audit.replay(SID)).filter((entry) => entry.kind === AuditKind.FLOW_DRIFT_DETECTED);
    expect(drifts).toHaveLength(1);
    expect(drifts[0]?.flow).not.toBeNull();
    expect(drifts[0]?.flow?.flowName).toBe('mgr-flow');
    expect(drifts[0]?.flow?.storedFingerprint).toBe(original.fingerprint());
    expect(drifts[0]?.flow?.currentFingerprint).toBe(changed.fingerprint());
    expect(await runtime.store.getFlowFingerprint(SID)).toBe(changed.fingerprint()); // rebaselined immediately

    const secondResume = await manager.resume({ sessionId: SID, namespaceId: NAMESPACE, flow: changed });
    expect(secondResume?.status).toBe(SessionStatus.PARKED);
    const afterRebaseline = (await runtime.audit.replay(SID)).filter(
      (entry) => entry.kind === AuditKind.FLOW_DRIFT_DETECTED,
    );
    expect(afterRebaseline).toHaveLength(1); // the same changed flow resumes quietly after the rebaseline

    const completed = await manager.deliver({
      sessionId: SID,
      namespaceId: NAMESPACE,
      dataPoint: chatAnswer('finally'),
      flow: changed,
    });
    expect(completed?.status).toBe(SessionStatus.COMPLETED); // the drifted flow still completes
  });
});

describe('the catalog', () => {
  it('makes a revocation visible to the next grant', async () => {
    const catalog = new InMemoryCapabilityCatalog({ permitted: [[NAMESPACE, [CapabilityId('netcap')]]] });
    const runtime = buildInMemoryRuntime(new FakeClock(), { catalog });
    const manager = new SessionOrchestrationManager(runtime);
    const netcap = makeCapability('netcap', { dependsOn: [IpDataPoint] });
    const seeder = makeOperator('seeder', { produces: [IpDataPoint], emits: [ip()] });
    const consumer = makeOperator('consumer', { requires: [netcap], produces: [RiskDataPoint], emits: [risk()] });
    const flow = flowOf([seeder, consumer], { capabilities: [netcap] });

    const first = await manager.startSession({ sessionId: SessionId('s1'), namespaceId: NAMESPACE, flow });
    expect(first.operatorRuns.get(toOperatorId('consumer'))).toBe(1); // netcap permitted → consumer ran

    catalog.setPermitted(NAMESPACE, []); // revoke
    const second = await manager.startSession({ sessionId: SessionId('s2'), namespaceId: NAMESPACE, flow });

    // Revocation visible to the new grant.
    expect(second.operatorRuns.has(toOperatorId('consumer'))).toBe(false);
  });
});

describe('re-opening a completed session', () => {
  it('re-runs the aggregator for a late non-ephemeral deliver', async () => {
    // A non-ephemeral DataPoint (e.g. NAME/WORK_EMAIL) delivered AFTER a session completes must
    // trigger a re-open so the aggregator re-runs and the durable result reflects the late data.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const manager = new SessionOrchestrationManager(runtime);
    const reporter = makeAggregator('rep', {
      dependsOn: [EmailDataPoint],
      onAggregate: async (ctx: OperatorContext) => {
        const emails = ctx.store
          .ofType(EmailDataPoint)
          .map((dataPoint) => dataPoint.value)
          .sort();
        await ctx.aggregation?.upsert('reports', 'report', { emails });
      },
    });
    const flow = flowOf([reporter], { completesWhen: EmailDataPoint });

    // Start and complete the session with one email.
    const first = await manager.startSession({
      sessionId: SID,
      namespaceId: NAMESPACE,
      flow,
      seed: [workEmail()],
    });
    expect(first.status).toBe(SessionStatus.COMPLETED);
    const beforeDoc = await runtime.durable.read('reports', 'report');
    expect(beforeDoc?.document).toEqual({ emails: ['alice@work.example'] });

    // Deliver a second email AFTER completion with the re-open intent — must re-open and re-aggregate.
    const result = await manager.deliver({
      sessionId: SID,
      namespaceId: NAMESPACE,
      dataPoint: workEmail('late@work.example'),
      flow,
      reopenIfComplete: true,
    });

    expect(result?.status).toBe(SessionStatus.COMPLETED);
    expect(result?.epoch).toBe(2); // a fresh higher-epoch orchestrator ran the re-open
    const afterDoc = await runtime.durable.read('reports', 'report');
    expect([...((afterDoc?.document.emails ?? []) as readonly string[])].sort()).toEqual([
      'alice@work.example',
      'late@work.example',
    ]);
    expect(await runtime.lock.isComplete(SID)).toBe(true); // still complete after the re-open finishes
  });

  it('does not re-open for a late ephemeral deliver without the flag', async () => {
    // An ephemeral late deliver WITHOUT reopenIfComplete must NOT re-open — completing the flag
    // matrix alongside the ephemeral-plus-flag and non-ephemeral-without-flag cases.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const manager = new SessionOrchestrationManager(runtime);
    const flow = flowOf([scorer()], { completesWhen: EmailDataPoint });
    await manager.startSession({ sessionId: SID, namespaceId: NAMESPACE, flow, seed: [workEmail()] });
    expect(await runtime.lock.isComplete(SID)).toBe(true);

    const result = await manager.deliver({ sessionId: SID, namespaceId: NAMESPACE, dataPoint: trigger(), flow });

    expect(result).toBeNull(); // ephemeral late data is discarded, no re-open
    expect(await runtime.lock.isComplete(SID)).toBe(true); // still complete — ephemeral did not re-open
  });

  it('lets exactly one of two concurrent re-opens win, with no corruption', async () => {
    // Two concurrent re-open races: exactly one wins the acquire. The loser stands down via
    // LockHeldError (same as resume's concurrent-grant contract) — no corruption, no partial state.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const manager = new SessionOrchestrationManager(runtime);
    const reporter = makeAggregator('rep', {
      dependsOn: [EmailDataPoint],
      onAggregate: async (ctx: OperatorContext) => {
        await ctx.aggregation?.upsert('reports', 'report', { count: 1 });
      },
    });
    const flow = flowOf([reporter], { completesWhen: EmailDataPoint });
    await manager.startSession({ sessionId: SID, namespaceId: NAMESPACE, flow, seed: [workEmail()] });

    // Race two reopens simultaneously.
    const results = await Promise.allSettled([
      manager.reopen({ sessionId: SID, namespaceId: NAMESPACE, flow }),
      manager.reopen({ sessionId: SID, namespaceId: NAMESPACE, flow }),
    ]);

    // Neither should raise — one wins (returns a result), the other stands down (returns null).
    expect(results.filter((outcome) => outcome.status === 'rejected')).toEqual([]);
    const values = results.map((outcome) => (outcome.status === 'fulfilled' ? outcome.value : undefined));
    expect(values.filter((value) => value !== null && value !== undefined)).toHaveLength(1); // exactly one winner
    expect(values.filter((value) => value === null)).toHaveLength(1);
    expect(await runtime.lock.isComplete(SID)).toBe(true); // completed after the winner's re-aggregation
  });

  it('re-aggregates on a re-open whose original deadline has elapsed', async () => {
    // The original deadline is blown long before the late participant data arrives. A fresh deadline
    // is granted on re-open so the aggregation phase has a live budget, not a negative one that would
    // fire spurious deadline-hit telemetry and skip aggregation.
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const manager = new SessionOrchestrationManager(runtime);
    const reporter = makeAggregator('rep', {
      dependsOn: [EmailDataPoint],
      onAggregate: async (ctx: OperatorContext) => {
        const emails = ctx.store
          .ofType(EmailDataPoint)
          .map((dataPoint) => dataPoint.value)
          .sort();
        await ctx.aggregation?.upsert('reports', 'report', { emails });
      },
    });
    // Use a very short deadline (1 s) to simulate a budgeted flow.
    const flow = flowOf([reporter], { completesWhen: EmailDataPoint, sessionDeadlineMs: SECOND_MS });

    await manager.startSession({ sessionId: SID, namespaceId: NAMESPACE, flow, seed: [workEmail()] });
    expect(await runtime.lock.isComplete(SID)).toBe(true);

    // Advance past the original deadline — any resume rehydrating the old deadline would get a
    // non-positive budget and the gather loop would fire deadline-hit telemetry.
    clock.advance(300 * SECOND_MS); // well past the 1 s budget

    // A late re-open with a fresh deadline must still re-aggregate successfully.
    const result = await manager.deliver({
      sessionId: SID,
      namespaceId: NAMESPACE,
      dataPoint: workEmail('late@work.example'),
      flow,
      reopenIfComplete: true,
    });

    expect(result?.status).toBe(SessionStatus.COMPLETED);
    const afterDoc = await runtime.durable.read('reports', 'report');
    // The late data landed in the result.
    expect((afterDoc?.document.emails ?? []) as readonly string[]).toContain('late@work.example');
  });

  it('re-opens and folds a late ephemeral attribute delivered with the flag', async () => {
    // REGRESSION (production): NAME/WORK_EMAIL are EPHEMERAL leaves, yet the aggregator folds them
    // into the durable result. A late deliver of an EPHEMERAL attribute WITH reopenIfComplete must
    // re-open and re-aggregate. Ephemerality must NOT gate the re-open — the caller's explicit intent
    // does. (A prior ephemeral-based gate silently dropped exactly these attributes.)
    const runtime = buildInMemoryRuntime(new FakeClock());
    const manager = new SessionOrchestrationManager(runtime);
    const reporter = makeAggregator('rep', {
      dependsOn: [EmailDataPoint],
      onAggregate: async (ctx: OperatorContext) => {
        const signals = ctx.store
          .ofType(TriggerDataPoint)
          .map((dataPoint) => dataPoint.value)
          .sort();
        await ctx.aggregation?.upsert('reports', 'report', { signals });
      },
    });
    const flow = flowOf([reporter], { completesWhen: EmailDataPoint });

    const first = await manager.startSession({
      sessionId: SID,
      namespaceId: NAMESPACE,
      flow,
      seed: [workEmail()],
    });
    expect(first.status).toBe(SessionStatus.COMPLETED);
    const beforeDoc = await runtime.durable.read('reports', 'report');
    expect(beforeDoc?.document).toEqual({ signals: [] });

    const late = trigger();
    expect(late.isEphemeral).toBe(true); // mirrors the real ephemeral NAME/WORK_EMAIL leaves
    const result = await manager.deliver({
      sessionId: SID,
      namespaceId: NAMESPACE,
      dataPoint: late,
      flow,
      reopenIfComplete: true,
    });

    expect(result?.status).toBe(SessionStatus.COMPLETED);
    expect(result?.epoch).toBe(2); // a fresh higher-epoch orchestrator ran the re-open
    const afterDoc = await runtime.durable.read('reports', 'report');
    expect(afterDoc?.document).toEqual({ signals: ['late-signal'] }); // the late EPHEMERAL attribute was folded in
    expect(await runtime.lock.isComplete(SID)).toBe(true);
  });

  it('never re-opens a late deliver that carries no flag', async () => {
    // The discriminator is the caller's reopenIfComplete intent, NOT ephemerality. A late deliver of
    // a NON-ephemeral DataPoint WITHOUT the flag must append-and-expire, never re-open — proving the
    // flag, not the data point's ephemerality, drives the re-open.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const manager = new SessionOrchestrationManager(runtime);
    const flow = flowOf([scorer()], { completesWhen: EmailDataPoint });
    await manager.startSession({ sessionId: SID, namespaceId: NAMESPACE, flow, seed: [workEmail()] });
    expect(await runtime.lock.isComplete(SID)).toBe(true);

    // workEmail() is NON-ephemeral, yet without the flag it must not re-open.
    const result = await manager.deliver({
      sessionId: SID,
      namespaceId: NAMESPACE,
      dataPoint: workEmail('late@work.example'),
      flow,
    });

    expect(result).toBeNull(); // no reopen requested -> appended and left to expire
    expect(await runtime.lock.isComplete(SID)).toBe(true);
  });

  it('defers a held-and-complete deliver to the holder with no reopen recheck', async () => {
    // Flush-window race, new model: a late result-affecting deliver can land while the JUST-completed
    // orchestrator still holds its lease (it drains its own inbox BEFORE releasing the epoch).
    // Because the holder owns the inbox while it holds the epoch, the deliver must NOT schedule a
    // deliver-side reopen-recheck — reopen() stands down on the held lock and defers to the holder,
    // which folds the late data in-run. A recheck here would spawn a redundant re-aggregation (the
    // latency + timeline pollution this change removes). Once the lock frees, a genuinely free-lock
    // deliver still reopens and folds.
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const manager = new SessionOrchestrationManager(runtime);
    const reporter = makeAggregator('rep', {
      dependsOn: [EmailDataPoint],
      onAggregate: async (ctx: OperatorContext) => {
        const emails = ctx.store
          .ofType(EmailDataPoint)
          .map((dataPoint) => dataPoint.value)
          .sort();
        await ctx.aggregation?.upsert('reports', 'report', { emails });
      },
    });
    const flow = flowOf([reporter], { completesWhen: EmailDataPoint });
    await manager.startSession({ sessionId: SID, namespaceId: NAMESPACE, flow, seed: [workEmail()] });
    expect(await runtime.lock.isComplete(SID)).toBe(true);

    // Simulate the completing orchestrator still holding its lease during the post-completion flush window.
    await runtime.lock.acquire(SID);
    expect(await runtime.lock.isHeld(SID)).toBe(true);

    // Late deliver with the re-open intent onto the held+complete session: reopen() stands down on
    // the held lock, returning null. Crucially, NO deliver-side reopen-recheck is scheduled.
    const result = await manager.deliver({
      sessionId: SID,
      namespaceId: NAMESPACE,
      dataPoint: workEmail('late@work.example'),
      flow,
      reopenIfComplete: true,
    });

    expect(result).toBeNull();
    expect(manager.recheckTasks.size).toBe(0); // the holder drains it in-run; no deliver-side recheck
    expect(await runtime.inbox.pendingCount(SID)).toBe(1); // appended durably, awaiting the holder's own drain

    // Once the holder releases (lease expires here), a genuinely free-lock reopen deliver still folds
    // the late data — the free-lock spawn path is intact; only the held-lock recheck hack is gone.
    clock.advance(PAST_TTL_MS); // the holder's lease expires; the lock is now free
    expect(await runtime.lock.isHeld(SID)).toBe(false);
    const foldedResult = await manager.deliver({
      sessionId: SID,
      namespaceId: NAMESPACE,
      dataPoint: workEmail('late@work.example'),
      flow,
      reopenIfComplete: true,
    });

    expect(foldedResult?.status).toBe(SessionStatus.COMPLETED);
    expect(foldedResult?.epoch).toBe(3); // a fresh higher-epoch orchestrator (epoch 1 start, epoch 2 stub-holder)
    const folded = await runtime.durable.read('reports', 'report');
    expect([...((folded?.document.emails ?? []) as readonly string[])].sort()).toEqual([
      'alice@work.example',
      'late@work.example',
    ]);
    expect(await runtime.inbox.pendingCount(SID)).toBe(0); // both stragglers drained by the free-lock reopen
    expect(await runtime.lock.isComplete(SID)).toBe(true);
  });
});

describe('the post-run backstop', () => {
  it('re-spawns for a straggler that landed in the check-release gap', async () => {
    // The completing orchestrator reads the pending count (W2) and then, in a SEPARATE step,
    // releases the lock. A cross-pod deliver that appends a straggler in the [W2, release] gap is
    // invisible to W2, so the orchestrator's own pre-release view is clean — nothing in-run can catch
    // it. The manager holds no epoch and reads the inbox AFTER the run returns (post-release), so its
    // pending-count probe DOES observe the straggler and must re-spawn to fold it rather than orphan
    // it. This reproduces the exact NAME/WORK_EMAIL late-deliver orphan: without the post-release
    // probe the straggler is durably in the inbox but nothing ever drains it.
    const clock = new FakeClock();
    // Unique session id + durable table + operator id so the test is hermetic under any ordering.
    const sid = SessionId('mgr-check-release-gap-session');
    const injected = { done: false };

    /**
     * Injects the straggler during `release` — the single-event-loop interleaving point of the [W2,
     * release] gap: it lands strictly after the completing run's pending-count read (W2) yet
     * at/before the actual release, so the run returns a clean COMPLETED but the inbox is durably
     * non-empty once released.
     */
    class StragglerInjectingLock extends InMemorySessionLock {
      public inject: (() => Promise<void>) | null = null;

      public override async release(sessionId: SessionId, options: { readonly epoch: Epoch }): Promise<void> {
        if (sessionId === sid && !injected.done) {
          injected.done = true;
          await this.inject?.();
        }
        await super.release(sessionId, options);
      }
    }

    const lock = new StragglerInjectingLock(clock);
    const runtime = makeRuntime({ ...buildInMemoryRuntime(clock), lock });
    lock.inject = async () => {
      await runtime.inbox.append(sid, workEmail('late@work.example'));
    };
    const manager = new SessionOrchestrationManager(runtime);
    const reporter = makeAggregator('gap_reporter', {
      dependsOn: [EmailDataPoint],
      onAggregate: async (ctx: OperatorContext) => {
        const emails = ctx.store
          .ofType(EmailDataPoint)
          .map((dataPoint) => dataPoint.value)
          .sort();
        await ctx.aggregation?.upsert('gap_reports', 'report', { emails });
      },
    });
    const flow = flowOf([reporter], { completesWhen: EmailDataPoint });

    const result = await manager.startSession({
      sessionId: sid,
      namespaceId: NAMESPACE,
      flow,
      seed: [workEmail()],
    });

    expect(result.status).toBe(SessionStatus.COMPLETED);
    // The post-release probe re-spawned and folded the straggler: the durable doc carries BOTH values
    // and the inbox is fully drained. Without the fix the straggler would sit orphaned in the inbox
    // and the doc would hold only the seed email.
    const doc = await runtime.durable.read('gap_reports', 'report');
    expect([...((doc?.document.emails ?? []) as readonly string[])].sort()).toEqual([
      'alice@work.example',
      'late@work.example',
    ]);
    expect(await runtime.inbox.pendingCount(sid)).toBe(0); // fully drained by the backstop
  });
});
