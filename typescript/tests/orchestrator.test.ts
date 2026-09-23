/**
 * ORCH — orchestrator lifecycle, sole-mutator, timeouts, fault isolation, breaker arming.
 *
 * Also covers POLICY-03 (no cancellation) and POLICY-06 (stopped only by timeout/deadline) via the
 * timeout/exception isolation tests.
 */

import { describe, expect, it } from 'vitest';
import {
  InMemoryAuditSink,
  InMemoryCapabilityCatalog,
  InMemoryDataPointStore,
  InMemoryInbox,
  InMemorySessionLock,
} from '../src/orcastork/adapters/memory/index.js';
import { RetryPolicy } from '../src/orcastork/aggregation/index.js';
import type { NamespaceCipherProvider, ValueCipher } from '../src/orcastork/archive/index.js';
import type { AuditLogEntry, OperatorAuditInfo } from '../src/orcastork/audit/index.js';
import { AuditKind, OperatorOutcome } from '../src/orcastork/audit/index.js';
import type { CapabilityContext } from '../src/orcastork/capabilities/index.js';
import { Capability, capability } from '../src/orcastork/capabilities/index.js';
import type { AnyDataPoint, DataPointView } from '../src/orcastork/datapoints/index.js';
import { DataPointEmission } from '../src/orcastork/datapoints/index.js';
import { StaleEpochError } from '../src/orcastork/exceptions.js';
import { FlowIdentity } from '../src/orcastork/flow.js';
import type { Revision } from '../src/orcastork/ids.js';
import { CapabilityId, Epoch, NamespaceId, OperatorId, SessionId } from '../src/orcastork/ids.js';
import { Deferred } from '../src/orcastork/internal/deferred.js';
import type { BoundedQueue } from '../src/orcastork/internal/index.js';
import { BoundedQueue as Queue } from '../src/orcastork/internal/index.js';
import type { OperatorContext } from '../src/orcastork/operators/index.js';
import { Operator, OperatorPolicy, operator, RerunOn } from '../src/orcastork/operators/index.js';
import type { OrchestratorOptions, Signal } from '../src/orcastork/orchestrator/index.js';
import { Orchestrator, SessionStatus } from '../src/orcastork/orchestrator/index.js';
import { SessionStateMirror } from '../src/orcastork/orchestrator/mirror.js';
import type { ApplyResolvedOptions } from '../src/orcastork/ports/index.js';
import { buildInMemoryRuntime, OrchestratorRuntime } from '../src/orcastork/runtime.js';
import { allOf } from '../src/orcastork/scheduling/index.js';
import { makeCapability } from './doubles/capabilities.js';
import { ReversingCipher } from './doubles/cipher.js';
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
import { captureLogs } from './doubles/logs.js';
import { abortableSleep, makeAggregator, makeOperator } from './doubles/operators.js';
import { TelemetryProbe } from './doubles/otel.js';

const SID = SessionId('orch-session');
const NAMESPACE = NamespaceId('orch-namespace');

/**
 * A `NamespaceCipherProvider` that hands the same cipher back for every namespace (a real
 * per-namespace provider in production resolves a distinct key per namespace).
 */
class SingleNamespaceCipherProvider implements NamespaceCipherProvider {
  private readonly cipher: ValueCipher;

  public constructor(cipher: ValueCipher) {
    this.cipher = cipher;
  }

  public async forNamespace(): Promise<ValueCipher> {
    return this.cipher;
  }
}

/** In-memory lock that counts lease renewals (to assert the orchestrator keeps ownership alive). */
class RenewCountingLock extends InMemorySessionLock {
  public renews = 0;

  public override async renew(sessionId: SessionId, options: { readonly epoch: Epoch }): Promise<void> {
    this.renews += 1;
    await super.renew(sessionId, options);
  }
}

/** In-memory lock whose renew always reports a takeover (models being fenced mid-run). */
class FenceOnRenewLock extends InMemorySessionLock {
  public override async renew(): Promise<void> {
    throw new StaleEpochError('a higher epoch took over');
  }
}

/** Queue recording the highest depth it ever held (to pin the backpressure bound). */
class HighWaterQueue<T> extends Queue<T> {
  public highWater = 0;

  public override async put(item: T): Promise<boolean> {
    const accepted = await super.put(item);
    this.highWater = Math.max(this.highWater, this.size);
    return accepted;
  }
}

/**
 * Orchestrator whose emission queue and mirror are observable — the port of Python's
 * `_ProbeQueueOrchestrator` plus its reach into `_mirror._entries`.
 */
class ProbeOrchestrator extends Orchestrator {
  public probe: HighWaterQueue<Signal> | null = null;

  public override buildEmissionQueue(): BoundedQueue<Signal> {
    const queue = new HighWaterQueue<Signal>(this.emissionQueueSize);
    this.probe = queue;
    return queue;
  }

  /** How many identities the sole-mutator mirror holds. */
  public get mirroredIdentities(): number {
    return this.mirror.size;
  }
}

const SECOND_MS = 1_000;

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

/** Hand the event loop back once — the port of the Python helpers' `await asyncio.sleep(0)`. */
const yieldOnce = (): Promise<void> =>
  new Promise<void>((resolve) => {
    setImmediate(resolve);
  });

/** Poll `predicate` on the event loop until it holds, bounded by a short REAL deadline. */
const until = async (predicate: () => Promise<boolean> | boolean, complaint: string): Promise<void> => {
  const deadline = Date.now() + 1_000;
  while (Date.now() < deadline) {
    if (await predicate()) {
      return;
    }
    await yieldOnce();
  }
  throw new Error(complaint);
};

/** The DataPoint types the session holds, as the Python assertions read them. */
const storedTypes = async (runtime: OrchestratorRuntime): Promise<ReadonlySet<string>> =>
  new Set((await runtime.store.snapshot(SID)).all().map((dataPoint) => dataPoint.type));

/** An orchestrator over the shared session/namespace, so a test names only what it varies. */
const orchestrate = (options: Omit<OrchestratorOptions, 'sessionId' | 'namespaceId'>): Orchestrator =>
  new Orchestrator({ sessionId: SID, namespaceId: NAMESPACE, ...options });

/** In-memory store that counts hot-loop reads and applies (per-batch, not per-emission, cost). */
class CountingStore extends InMemoryDataPointStore {
  public snapshots = 0;
  public watermarkReads = 0;
  public applies = 0;

  public override async snapshot(sessionId: SessionId): Promise<DataPointView> {
    this.snapshots += 1;
    return await super.snapshot(sessionId);
  }

  public override async getWatermark(sessionId: SessionId, operatorId: OperatorId): Promise<Revision | null> {
    this.watermarkReads += 1;
    return await super.getWatermark(sessionId, operatorId);
  }

  public override async applyResolved(sessionId: SessionId, options: ApplyResolvedOptions): Promise<Revision> {
    this.applies += 1;
    return await super.applyResolved(sessionId, options);
  }
}

/** In-memory store that rejects applies containing a marked value (a persistently bad apply). */
class RejectingStore extends InMemoryDataPointStore {
  public override async applyResolved(sessionId: SessionId, options: ApplyResolvedOptions): Promise<Revision> {
    if ([...options.added, ...options.updated].some((dataPoint) => dataPoint.value === 'merge-bomb')) {
      throw new Error('store rejected the write');
    }
    return await super.applyResolved(sessionId, options);
  }
}

/** Audit sink whose DataPoint-added appends always fail (a broken post-apply bookkeeping sink). */
class FailingAuditSink extends InMemoryAuditSink {
  public override async append(entry: AuditLogEntry): Promise<void> {
    if (entry.kind === AuditKind.DATA_POINT_ADDED) {
      throw new Error('audit sink down');
    }
    await super.append(entry);
  }
}

/** Audit sink whose batched DataPoint-added append fails (a transient sink fault mid-batch). */
class BatchAuditFailingSink extends InMemoryAuditSink {
  public override async appendMany(entries: readonly AuditLogEntry[]): Promise<void> {
    if (entries.some((entry) => entry.kind === AuditKind.DATA_POINT_ADDED)) {
      throw new Error('audit sink down mid-batch');
    }
    await super.appendMany(entries);
  }
}

/** A run stopped while it was sleeping, the port of the `CancelledError` `asyncio.sleep` raises. */
class RunCancelledError extends Error {}

/**
 * A REAL sleep that REJECTS the moment the run is cut short.
 *
 * `asyncio.sleep` raises `CancelledError` into the coroutine when its task is cancelled, which is
 * what lets an operator distinguish "the wait finished" from "the wait was taken away". A promise
 * carries no such signal, so the abort on the run's context is turned into a rejection here — and
 * the test operators that must not reach their post-sleep yield depend on exactly that.
 */
const cancellableSleep = (ms: number, signal: AbortSignal): Promise<void> =>
  new Promise<void>((resolve, reject) => {
    if (signal.aborted) {
      reject(new RunCancelledError('the run was cancelled'));
      return;
    }
    const timer = setTimeout(resolve, ms);
    timer.unref();
    signal.addEventListener(
      'abort',
      () => {
        clearTimeout(timer);
        reject(new RunCancelledError('the run was cancelled'));
      },
      { once: true },
    );
  });

describe('Orchestrator', () => {
  it('completes its lifecycle and releases the epoch', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const op = makeOperator('op', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    const result = await orchestrate({ runtime, operators: [op], seed: [workEmail()] }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(result.epoch).toBe(1); // first epoch minted on grant
    expect(await runtime.lock.isHeld(SID)).toBe(false); // epoch released at completion
  });

  it('runs aggregation once gathering is quiescent', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const seen: { riskCount?: number } = {};
    const op = makeOperator('op', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    const reporter = makeAggregator('rep', {
      dependsOn: [RiskDataPoint],
      onAggregate: async (ctx) => {
        seen.riskCount = ctx.store.ofType(RiskDataPoint).length;
      },
    });

    await orchestrate({ runtime, operators: [op, reporter], seed: [workEmail()] }).run();

    expect(seen.riskCount).toBe(1); // the aggregator observed the fully-gathered state
  });

  it('applies every emission as the sole mutator', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const first = makeOperator('o1', { produces: [IpDataPoint], emits: [ip()] });
    const second = makeOperator('o2', { produces: [RiskDataPoint], emits: [risk()] });

    await orchestrate({ runtime, operators: [first, second], seed: [workEmail()] }).run();

    expect(await storedTypes(runtime)).toEqual(new Set(['work_email', 'ip', 'risk']));
  });

  it('persists a timed-out operator’s emissions and proceeds', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const slow = makeOperator('slow', { produces: [IpDataPoint], emits: [ip('slow-ip')], sleepAfterMs: 5_000 });
    const fast = makeOperator('fast', { produces: [RiskDataPoint], emits: [risk()] });

    const result = await orchestrate({ runtime, operators: [slow, fast], operationTimeoutMs: 20 }).run();

    const types = await storedTypes(runtime);
    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(types.has('ip')).toBe(true); // the slow operator's pre-timeout emission persisted
    expect(types.has('risk')).toBe(true); // the scheduler proceeded with the other operator
  });

  it('lets a policy timeout carry a slow operator past the global default', async () => {
    // A wrapped streaming collector legitimately runs past the orchestrator-wide bound: its policy
    // timeout raises the per-run limit, so it finishes (and its post-sleep emission lands) where the
    // global default alone would have cancelled it mid-run.
    const runtime = buildInMemoryRuntime(new FakeClock());

    class Streaming extends Operator {
      public static readonly operatorId = OperatorId('streaming');
      public static readonly policy = OperatorPolicy({ rerunOnNewData: false, timeoutMs: 5_000 });
      public static readonly produces = [IpDataPoint];

      public async *run(ctx: OperatorContext): AsyncIterable<DataPointEmission> {
        // Longer than the global bound — only the policy override allows this.
        await abortableSleep(60, ctx.signal);
        yield IpDataPoint.emit('survived');
      }
    }
    operator(Streaming);

    const result = await orchestrate({ runtime, operators: [Streaming], operationTimeoutMs: 20 }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    const stored = (await runtime.store.snapshot(SID)).ofType(IpDataPoint);
    // The post-sleep emission proves the run was not cut short.
    expect(stored.map((dataPoint) => dataPoint.value)).toEqual(['survived']);
    const runs = runsByOperator(await runtime.audit.replay(SID));
    expect(runs.get(OperatorId('streaming'))?.outcome).toBe(OperatorOutcome.SUCCEEDED);
  });

  it('lets a policy timeout cut an operator shorter than the global default', async () => {
    // The override works in both directions: a scoring operator that must answer in well under the
    // global bound is timed out by its own (shorter) policy timeout, and the scheduler proceeds.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const slow = makeOperator('slow', {
      produces: [IpDataPoint],
      emits: [ip('slow-ip')],
      sleepAfterMs: 500,
      timeoutMs: 20,
    });
    const fast = makeOperator('fast', { produces: [RiskDataPoint], emits: [risk()] });

    const result = await orchestrate({ runtime, operators: [slow, fast] }).run();

    const types = await storedTypes(runtime);
    expect(result.status).toBe(SessionStatus.COMPLETED);
    // The pre-timeout emission persisted; the scheduler proceeded.
    expect(types.has('ip')).toBe(true);
    expect(types.has('risk')).toBe(true);
    const runs = runsByOperator(await runtime.audit.replay(SID));
    // Cut off well before the 30 s global default.
    expect(runs.get(OperatorId('slow'))?.outcome).toBe(OperatorOutcome.FAILED);
    expect(runs.get(OperatorId('fast'))?.outcome).toBe(OperatorOutcome.SUCCEEDED);
  });

  it('runs aggregation and completes when the session deadline is already spent', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const seen: { riskCount?: number } = {};
    const reporter = makeAggregator('rep', {
      dependsOn: [RiskDataPoint],
      onAggregate: async (ctx) => {
        seen.riskCount = ctx.store.ofType(RiskDataPoint).length;
      },
    });

    const result = await orchestrate({
      runtime,
      operators: [reporter],
      seed: [risk()],
      sessionDeadlineMs: 0, // deadline already reached → straight to aggregation
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(seen.riskCount).toBe(1);
  });

  it('persists a throwing operator’s emissions and proceeds', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const boom = makeOperator('boom', {
      produces: [IpDataPoint],
      emits: [ip('boom-ip')],
      raiseError: new Error('boom'),
    });
    const fast = makeOperator('fast', { produces: [RiskDataPoint], emits: [risk()] });

    const { records, result } = await captureLogs(
      async () => await orchestrate({ runtime, operators: [boom, fast] }).run(),
      { level: 'ERROR' },
    );

    const types = await storedTypes(runtime);
    expect(result.status).toBe(SessionStatus.COMPLETED); // no wedge
    expect(types.has('ip')).toBe(true);
    expect(types.has('risk')).toBe(true);
    const failure = records.find((record) => record.fields.operator_id === OperatorId('boom'));
    // The log carries the error itself, not just the operator id.
    const logged = failure?.fields.error;
    expect(logged).toBeInstanceOf(Error);
    expect((logged as Error).message).toContain('boom');
  });

  it('needs no in-band control DataPoint to decide quiescence', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const op = makeOperator('op', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });

    await orchestrate({ runtime, operators: [op], seed: [workEmail()] }).run();

    // Only real DataPoints exist — quiescence is computed directly, not signalled by a marker.
    expect(await storedTypes(runtime)).toEqual(new Set(['work_email', 'risk']));
  });

  it('never writes durable output itself', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const op = makeOperator('op', { produces: [RiskDataPoint], emits: [risk()] }); // no aggregator in the flow

    await orchestrate({ runtime, operators: [op] }).run();

    expect((await runtime.store.snapshot(SID)).all()).toHaveLength(1); // gathering wrote to the live store
    expect(await runtime.durable.read('reports', 'risk')).toBeNull(); // no aggregator ran → curated store empty
  });

  it('stamps every write with the run’s epoch', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const op = makeOperator('op', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });

    const result = await orchestrate({ runtime, operators: [op], seed: [workEmail()] }).run();

    const entries = await runtime.audit.replay(SID);
    expect(entries.length).toBeGreaterThan(0);
    expect(entries.every((entry) => entry.epoch === result.epoch)).toBe(true);
  });

  it('arms the circuit breaker from the graph re-check', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    let counter = 0;
    const selfCycle = makeOperator('selfloop', {
      produces: [IpDataPoint],
      dependsOn: [IpDataPoint],
      rerunOnNewData: true,
      maxCycles: 3,
      emitFactory: () => {
        const value = `ip-${counter}`;
        counter += 1;
        return [ip(value)];
      },
    });

    const result = await orchestrate({ runtime, operators: [selfCycle], seed: [ip('seed')] }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED); // the self-cycle did not run forever
    // Bounded by the armed circuit-breaker.
    expect(result.operatorRuns.get(OperatorId('selfloop')) ?? 0).toBeLessThanOrEqual(3);
  });

  it('treats completion as terminal — nothing re-runs after it', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const op = makeOperator('op', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    const reporter = makeAggregator('rep', { dependsOn: [RiskDataPoint] });

    const result = await orchestrate({ runtime, operators: [op, reporter], seed: [workEmail()] }).run();

    expect(result.operatorRuns.get(OperatorId('op'))).toBe(1); // no post-completion re-runs
    expect(result.operatorRuns.get(OperatorId('rep'))).toBe(1);
  });

  it('stamps emission provenance and observation time', async () => {
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    // The operator emits value-only via the public `Leaf.emit(value)` API — no provenance plumbing.
    const op = makeOperator('emitter', { produces: [RiskDataPoint], emits: [RiskDataPoint.emit(0.7)] });

    await orchestrate({ runtime, operators: [op] }).run();

    const stored = (await runtime.store.snapshot(SID)).ofType(RiskDataPoint);
    expect(stored).toHaveLength(1);
    expect(stored[0]?.value).toBe(0.7);
    // Provenance stamped by the orchestrator, not the operator.
    expect(stored[0]?.retrievedBy).toBe(OperatorId('emitter'));
    // Observation time stamped too.
    expect(stored[0]?.firstRetrieved.getTime()).toBe(clock.now().getTime());
    expect(stored[0]?.lastRetrieved.getTime()).toBe(clock.now().getTime());
  });

  it('isolates a malformed emission instead of aborting the session', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    // The bad value only fails when the orchestrator finalizes the emission into a DataPoint (zod
    // validation); that must be isolated to its operator like any other fault, not abort the session.
    const bad = makeOperator('bad', {
      produces: [RiskDataPoint],
      emits: [new DataPointEmission(RiskDataPoint, 'not-a-number' as unknown as number)],
    });
    const good = makeOperator('good', { produces: [IpDataPoint], emits: [ip()] });

    const result = await orchestrate({ runtime, operators: [bad, good] }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED); // the malformed emission did not wedge the session
    expect(result.operatorRuns.get(OperatorId('good'))).toBe(1); // the healthy operator still ran
    expect(await storedTypes(runtime)).toEqual(new Set(['ip'])); // only the valid one persisted
  });

  it('pipelines an emission to its consumer while the producer is still running', async () => {
    // Eager streaming: an emission is merged and its consumer launched while the producer is still
    // running. The producer here only finishes *after* the consumer runs (it blocks on a deferred the
    // consumer settles), so reaching its post-gate emission proves the two overlapped — a
    // phase/gather scheduler would block the whole wave on the producer and never start the consumer.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const consumerRan = new Deferred<void>();

    class PipelineProducer extends Operator {
      public static readonly operatorId = OperatorId('pipeline_producer');
      public static readonly policy = OperatorPolicy({ rerunOnNewData: false });
      public static readonly dependsOn = [EmailDataPoint]; // ready from the seed
      public static readonly produces = [IpDataPoint, RiskDataPoint];

      public async *run(): AsyncIterable<DataPointEmission> {
        yield IpDataPoint.emit('203.0.113.7'); // the consumer's dependency
        await consumerRan.promise; // released only by the consumer running concurrently
        yield RiskDataPoint.emit(0.123); // post-gate marker — reached only if the consumer ran first
      }
    }
    operator(PipelineProducer);

    class PipelineConsumer extends Operator {
      public static readonly operatorId = OperatorId('pipeline_consumer');
      public static readonly policy = OperatorPolicy({ rerunOnNewData: false });
      public static readonly dependsOn = [IpDataPoint]; // not ready until the producer emits
      public static readonly produces = [ChatAnswerDataPoint];

      public async *run(): AsyncIterable<DataPointEmission> {
        consumerRan.resolve(); // unblock the still-running producer
        yield ChatAnswerDataPoint.emit('done');
      }
    }
    operator(PipelineConsumer);

    const result = await orchestrate({
      runtime,
      operators: [PipelineProducer, PipelineConsumer],
      seed: [workEmail()],
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect([...result.operatorRuns].sort()).toEqual([
      [OperatorId('pipeline_consumer'), 1],
      [OperatorId('pipeline_producer'), 1],
    ]);
    const byType = new Map(
      (await runtime.store.snapshot(SID)).all().map((dataPoint) => [dataPoint.type, dataPoint.value]),
    );
    // The producer reached its post-gate emission → the consumer ran while it waited.
    expect(byType.get('risk')).toBe(0.123);
    expect(byType.get('chat_answer')).toBe('done');
  });

  it('reruns for relevant data merged while it was running', async () => {
    // Watermark correctness under streaming: a run's watermark must advance to the revision its
    // launch snapshot observed, not the live revision at completion. Here a relevant DataPoint (Ip
    // 'b') is merged *while* the watcher is still running; a live-revision watermark would swallow it
    // and the watcher would never rerun. (The rerun also proves own-emissions don't re-trigger it.)
    const runtime = buildInMemoryRuntime(new FakeClock());
    const watcherStarted = new Deferred<void>();
    const secondIpMerged = new Deferred<void>();
    const seen: (readonly string[])[] = [];

    class Watcher extends Operator {
      public static readonly operatorId = OperatorId('watcher');
      // Rerun immediately on a new Ip.
      public static readonly policy = OperatorPolicy({ rerunOnNewData: true, debounceMs: 0 });
      public static readonly dependsOn = [IpDataPoint];
      public static readonly produces = [ChatAnswerDataPoint];

      public async *run(ctx: OperatorContext): AsyncIterable<DataPointEmission> {
        seen.push(
          ctx.store
            .ofType(IpDataPoint)
            .map((dataPoint) => dataPoint.value)
            .sort(),
        );
        if (!watcherStarted.settled) {
          watcherStarted.resolve(); // release the feeder to emit the second Ip
          await secondIpMerged.promise; // stay running until that Ip has actually been merged
        }
        yield ChatAnswerDataPoint.emit('seen'); // nothing depends on this — must NOT re-trigger the watcher
      }
    }
    operator(Watcher);

    class Feeder extends Operator {
      public static readonly operatorId = OperatorId('feeder');
      public static readonly policy = OperatorPolicy({ rerunOnNewData: false });
      public static readonly dependsOn = [EmailDataPoint];
      public static readonly produces = [IpDataPoint];

      public async *run(): AsyncIterable<DataPointEmission> {
        yield IpDataPoint.emit('a');
        await watcherStarted.promise; // emit the second Ip only once the watcher is mid-run
        yield IpDataPoint.emit('b');
      }
    }
    operator(Feeder);

    const releaseOnceSecondIpIsMerged = async (): Promise<void> => {
      await until(
        async () => (await runtime.store.snapshot(SID)).ofType(IpDataPoint).some((dp) => dp.value === 'b'),
        'timed out waiting for IpDataPoint(value="b") to merge',
      );
      secondIpMerged.resolve();
    };

    const orchestrator = orchestrate({ runtime, operators: [Watcher, Feeder], seed: [workEmail()] });
    const [result] = await Promise.all([orchestrator.run(), releaseOnceSecondIpIsMerged()]);

    expect(result.operatorRuns.get(OperatorId('watcher'))).toBe(2); // reran exactly once, for the mid-run Ip
    expect(seen).toEqual([['a'], ['a', 'b']]); // the rerun observed the Ip that landed while it was running
    const watcherRuns = (await runtime.audit.replay(SID))
      .filter((entry) => entry.operator !== null && entry.operatorId === OperatorId('watcher'))
      .map((entry) => entry.operator as OperatorAuditInfo);
    // The audit records each rerun by its run number.
    expect(watcherRuns.map((run) => run.runCount)).toEqual([1, 2]);
    expect(watcherRuns.every((run) => run.outcome === OperatorOutcome.SUCCEEDED)).toBe(true);
  });

  /**
   * Run a watcher/feeder pair where the feeder makes a second Ip observation mid-watcher-run.
   *
   * The second observation lands 5 s after the first: with the same value it merges as a
   * freshness-only update (`delta.updated`), with a different value as a new identity
   * (`delta.added`). Returns how many times the watcher ran.
   */
  const watcherRunsAfterSecondObservation = async (
    clock: FakeClock,
    rerunOn: RerunOn,
    secondValue: string,
  ): Promise<number> => {
    const runtime = buildInMemoryRuntime(clock);
    const watcherStarted = new Deferred<void>();
    const secondMerged = new Deferred<void>();
    const observedAtStart = clock.now();

    class Watcher extends Operator {
      public static readonly operatorId = OperatorId('watcher');
      public static readonly policy = OperatorPolicy({ rerunOnNewData: true, rerunOn, debounceMs: 0 });
      public static readonly dependsOn = [IpDataPoint];
      public static readonly produces = [ChatAnswerDataPoint];

      public async *run(): AsyncIterable<DataPointEmission> {
        if (!watcherStarted.settled) {
          watcherStarted.resolve(); // release the feeder to make its second observation
          await secondMerged.promise; // stay running until that observation has actually been merged
        }
        yield ChatAnswerDataPoint.emit('seen'); // nothing depends on this — must NOT re-trigger the watcher
      }
    }
    operator(Watcher);

    class Feeder extends Operator {
      public static readonly operatorId = OperatorId('feeder');
      public static readonly policy = OperatorPolicy({ rerunOnNewData: false });
      public static readonly dependsOn = [EmailDataPoint];
      public static readonly produces = [IpDataPoint];

      public async *run(): AsyncIterable<DataPointEmission> {
        yield IpDataPoint.emit('a');
        await watcherStarted.promise; // observe again only once the watcher is mid-run
        clock.advance(5 * SECOND_MS); // a later observation time, so an identical value still lands as an update
        yield IpDataPoint.emit(secondValue);
      }
    }
    operator(Feeder);

    const releaseOnceSecondObservationIsMerged = async (): Promise<void> => {
      await until(
        async () =>
          (await runtime.store.snapshot(SID))
            .ofType(IpDataPoint)
            .some((dataPoint) => dataPoint.lastRetrieved.getTime() > observedAtStart.getTime()),
        'timed out waiting for the second Ip observation to merge',
      );
      secondMerged.resolve();
    };

    const orchestrator = orchestrate({ runtime, operators: [Watcher, Feeder], seed: [workEmail()] });
    const [result] = await Promise.all([orchestrator.run(), releaseOnceSecondObservationIsMerged()]);
    expect(result.status).toBe(SessionStatus.COMPLETED);
    return result.operatorRuns.get(OperatorId('watcher')) ?? 0;
  };

  it('does not rerun an ADDED_ONLY watcher for a freshness-only re-observation', async () => {
    // The same (type, value) re-observed with a bumped lastRetrieved is a freshness-only update: an
    // ADDED_ONLY watcher must not rerun for it.
    expect(await watcherRunsAfterSecondObservation(new FakeClock(), RerunOn.ADDED_ONLY, 'a')).toBe(1);
  });

  it('reruns an ADDED_OR_UPDATED watcher for a freshness-only re-observation', async () => {
    // The default keeps today's behavior: a freshness-only update still re-triggers the watcher.
    expect(await watcherRunsAfterSecondObservation(new FakeClock(), RerunOn.ADDED_OR_UPDATED, 'a')).toBe(2);
  });

  it('still reruns an ADDED_ONLY watcher for a genuinely new value', async () => {
    // ADDED_ONLY narrows reruns to new identities — it must not suppress a genuinely new value.
    expect(await watcherRunsAfterSecondObservation(new FakeClock(), RerunOn.ADDED_ONLY, 'b')).toBe(2);
  });

  it('audits operator runs with their success and failure outcomes', async () => {
    // Every operator run is recorded with its outcome — including a run that emits nothing or raises,
    // which leaves no DATA_POINT_ADDED trace and so was previously invisible in the audit log.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const producer = makeOperator('producer', {
      dependsOn: [EmailDataPoint],
      produces: [RiskDataPoint],
      emits: [risk()],
    });
    const failer = makeOperator('failer', { dependsOn: [RiskDataPoint], raiseError: new Error('boom') });

    await orchestrate({ runtime, operators: [producer, failer], seed: [workEmail()] }).run();

    const runs = runsByOperator(await runtime.audit.replay(SID));
    expect(runs.get(OperatorId('producer'))?.outcome).toBe(OperatorOutcome.SUCCEEDED);
    expect(runs.get(OperatorId('failer'))?.outcome).toBe(OperatorOutcome.FAILED);
    expect(runs.get(OperatorId('failer'))?.error ?? '').toContain('boom'); // the failure reason is captured
  });

  it('audits a capability invocation with its parameters redacted', async () => {
    const catalog = new InMemoryCapabilityCatalog({ permitted: [[NAMESPACE, [CapabilityId('idp')]]] });
    const runtime = buildInMemoryRuntime(new FakeClock(), { catalog });
    const performed: { readonly action: string; readonly parameters: Record<string, string> }[] = [];

    class Idp extends Capability {
      public static readonly capabilityId = CapabilityId('idp');
      public static readonly dependsOn = [EmailDataPoint];

      public async activate(_ctx: CapabilityContext): Promise<void> {
        return;
      }

      /** A real, typed action method. */
      public async sendChallenge(args: { readonly userId: string; readonly email: string }): Promise<string> {
        performed.push({ action: 'sendChallenge', parameters: { ...args } });
        return 'challenge-sent';
      }
    }
    capability(Idp);

    class Caller extends Operator {
      public static readonly operatorId = OperatorId('caller');
      public static readonly policy = OperatorPolicy({ rerunOnNewData: false });
      public static readonly dependsOn = [EmailDataPoint];
      public static readonly requires = [Idp];
      public static readonly produces = [ChatAnswerDataPoint];

      public async *run(ctx: OperatorContext): AsyncIterable<DataPointEmission> {
        // Typed; `requires` gating means it is always available here.
        const idp = ctx.capabilities.require(Idp);
        const answer = await idp.sendChallenge({ userId: 'u-1', email: 'alice@work.example' }); // direct, typed call
        yield ChatAnswerDataPoint.emit(answer);
      }
    }
    operator(Caller);

    await orchestrate({ runtime, operators: [Caller], capabilities: [Idp], seed: [workEmail()] }).run();

    // The real arguments reach the capability; only the audit log redacts them.
    expect(performed).toEqual([
      { action: 'sendChallenge', parameters: { userId: 'u-1', email: 'alice@work.example' } },
    ]);
    const invoked = (await runtime.audit.replay(SID)).filter((entry) => entry.kind === AuditKind.CAPABILITY_INVOKED);
    expect(invoked).toHaveLength(1);
    const invocation = invoked[0]?.capability;
    expect(invocation).not.toBeNull();
    expect(invocation?.capabilityId).toBe(CapabilityId('idp'));
    expect(invocation?.action).toBe('sendChallenge');
    // Parameter keys are recorded; their values are redacted, so PII never lands in the audit log.
    expect(invocation?.parameters).toEqual({ userId: '<redacted>', email: '<redacted>' });
    expect(JSON.stringify(invocation?.parameters)).not.toContain('alice@work.example');
  });

  it('seals a PII audit value under the namespace cipher', async () => {
    // With a real per-namespace provider, a PII value is sealed under the namespace key rather than
    // discarded, so an operator can recover it from the audit with the key — but it is never stored
    // in clear.
    const cipher = new ReversingCipher();
    const runtime = OrchestratorRuntime({
      ...buildInMemoryRuntime(new FakeClock()),
      cipherProvider: new SingleNamespaceCipherProvider(cipher),
    });
    const collector = makeOperator('collect', { produces: [IpDataPoint], emits: [ip('203.0.113.9')] });

    const result = await orchestrate({ runtime, operators: [collector] }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    const points = (await runtime.audit.replay(SID))
      .filter((entry) => entry.kind === AuditKind.DATA_POINT_ADDED)
      .map((entry) => entry.dataPoint);
    const ipSummary = points.find((info) => info?.dataPointType === 'ip')?.summary ?? '';
    expect(ipSummary).not.toBe('<redacted>');
    expect(ipSummary).not.toContain('203.0.113.9'); // ciphertext at rest, never the plaintext value
    expect(cipher.decrypt(ipSummary)).toBe('203.0.113.9'); // recoverable with the key
  });

  it('redacts a PII audit value without a real cipher', async () => {
    // The default passthrough provider must keep the historical behavior: PII stays redacted, never
    // written in clear by an identity "encrypt".
    const runtime = buildInMemoryRuntime(new FakeClock());
    const collector = makeOperator('collect', { produces: [IpDataPoint], emits: [ip('203.0.113.9')] });

    await orchestrate({ runtime, operators: [collector] }).run();

    const points = (await runtime.audit.replay(SID))
      .filter((entry) => entry.kind === AuditKind.DATA_POINT_ADDED)
      .map((entry) => entry.dataPoint);
    expect(points.find((info) => info?.dataPointType === 'ip')?.summary).toBe('<redacted>');
  });

  it('renews the lease while a long session runs', async () => {
    // A session that runs longer than the lock TTL must renew its lease, or a supervisor would treat
    // the still-working orchestrator as orphaned and take it over. A bounded self-cycle with a long
    // debounce makes the gather loop fast-forward the clock past the renew interval between reruns.
    const clock = new FakeClock();
    const counting = new RenewCountingLock(clock);
    const runtime = OrchestratorRuntime({ ...buildInMemoryRuntime(clock), lock: counting });
    let counter = 0;
    const selfCycle = makeOperator('selfloop', {
      produces: [IpDataPoint],
      dependsOn: [IpDataPoint],
      rerunOnNewData: true,
      maxCycles: 3,
      debounceMs: 20 * SECOND_MS, // each rerun fast-forwards the clock 20 s (> the 10 s renew interval)
      emitFactory: () => {
        const value = `ip-${counter}`;
        counter += 1;
        return [ip(value)];
      },
    });

    const result = await orchestrate({ runtime, operators: [selfCycle], seed: [ip('seed')] }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(counting.renews).toBeGreaterThanOrEqual(1); // the lease was renewed while the session worked
  });

  it('stops cleanly as SUPERSEDED when a renew reveals a higher epoch', async () => {
    // A renew that reveals a higher epoch took over ends the run as SUPERSEDED rather than raising:
    // the successor now owns the session and re-drives any unfinished work idempotently.
    const clock = new FakeClock();
    const runtime = OrchestratorRuntime({ ...buildInMemoryRuntime(clock), lock: new FenceOnRenewLock(clock) });
    let counter = 0;
    const selfCycle = makeOperator('selfloop', {
      produces: [IpDataPoint],
      dependsOn: [IpDataPoint],
      rerunOnNewData: true,
      maxCycles: 3,
      debounceMs: 20 * SECOND_MS, // forces a loop sleep that crosses the renew interval
      emitFactory: () => {
        const value = `ip-${counter}`;
        counter += 1;
        return [ip(value)];
      },
    });

    const result = await orchestrate({ runtime, operators: [selfCycle], seed: [ip('seed')] }).run();

    expect(result.status).toBe(SessionStatus.SUPERSEDED); // fenced mid-run → clean stop, not a raise
  });

  it('merges an undeclared emission and logs the mismatch once per operator and type', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    // Declares only Risk but emits two IPs: both must merge (data is never dropped); the mismatch is
    // reported once per (operator, type), not per emission.
    const sneaky = makeOperator('sneaky', {
      produces: [RiskDataPoint],
      emits: [risk(), ip('198.51.100.1'), ip('198.51.100.2')],
    });

    const { records, result } = await captureLogs(
      async () => await orchestrate({ runtime, operators: [sneaky] }).run(),
      { level: 'ERROR' },
    );

    expect(result.status).toBe(SessionStatus.COMPLETED);
    const stored = (await runtime.store.snapshot(SID)).ofType(IpDataPoint);
    // Merged anyway.
    expect(new Set(stored.map((dataPoint) => dataPoint.value))).toEqual(new Set(['198.51.100.1', '198.51.100.2']));
    const undeclared = records.filter((record) => record.fields.data_point_type === 'ip');
    expect(undeclared).toHaveLength(1); // one ERROR per operator-and-type, no spam
    expect(undeclared[0]?.fields.operator_id).toBe(OperatorId('sneaky'));
    expect(undeclared[0]?.fields.declared_produces).toEqual(['RiskDataPoint']);
  });

  it('does not flag a leaf of a declared abstract produces type', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    // Declaring the abstract intermediate covers every leaf in its substitution group.
    const emitter = makeOperator('emitter', {
      produces: [EmailDataPoint],
      emits: [workEmail('bob@work.example')],
    });

    const { records, result } = await captureLogs(
      async () => await orchestrate({ runtime, operators: [emitter] }).run(),
      { level: 'ERROR' },
    );

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(records).toEqual([]); // a leaf of a declared abstract type is a declared emission
  });

  it('gives each emission its actual observation time', async () => {
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const op = makeOperator('op', {
      produces: [IpDataPoint, RiskDataPoint],
      emitFactory: function* () {
        yield IpDataPoint.emit('198.51.100.1');
        clock.advance(5 * SECOND_MS); // the operator keeps running; later yields happen later
        yield RiskDataPoint.emit(0.5);
      },
    });

    await orchestrate({ runtime, operators: [op] }).run();

    const view = await runtime.store.snapshot(SID);
    const first = view.ofType(IpDataPoint)[0];
    const second = view.ofType(RiskDataPoint)[0];
    expect((second?.firstRetrieved.getTime() ?? 0) - (first?.firstRetrieved.getTime() ?? 0)).toBe(5 * SECOND_MS);
  });

  it('completes without waiting when the completion type is already present', async () => {
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const op = makeOperator('op', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });

    const result = await orchestrate({
      runtime,
      operators: [op],
      seed: [workEmail()],
      completesWhen: RiskDataPoint,
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    // The condition was met by gathering itself — no inbox wait happened.
    expect(clock.monotonic()).toBeLessThan(SECOND_MS);
  });

  it('bounds an unsatisfied wait by the session deadline and then aggregates', async () => {
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const seen: { emails?: number } = {};
    const reporter = makeAggregator('rep', {
      dependsOn: [EmailDataPoint],
      onAggregate: async (ctx) => {
        seen.emails = ctx.store.ofType(EmailDataPoint).length;
      },
    });

    const result = await orchestrate({
      runtime,
      operators: [reporter],
      seed: [workEmail()],
      completesWhen: ChatAnswerDataPoint, // never arrives; the inbox stays empty
      sessionDeadlineMs: 30 * SECOND_MS,
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED); // the deadline bounds the wait; aggregation still ran
    expect(seen).toEqual({ emails: 1 });
    expect(clock.monotonic()).toBeGreaterThanOrEqual(30 * SECOND_MS); // it genuinely waited out its deadline
  });

  it('renews the lease while waiting on the inbox', async () => {
    const clock = new FakeClock();
    const counting = new RenewCountingLock(clock);
    const runtime = OrchestratorRuntime({ ...buildInMemoryRuntime(clock), lock: counting });

    const result = await orchestrate({
      runtime,
      operators: [],
      seed: [workEmail()],
      completesWhen: ChatAnswerDataPoint,
      sessionDeadlineMs: 25 * SECOND_MS, // spans two 10 s renew intervals while waiting
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(counting.renews).toBeGreaterThanOrEqual(2); // ownership was kept alive across the whole wait
  });

  it('stops cleanly as SUPERSEDED when a renew during the wait is fenced', async () => {
    const clock = new FakeClock();
    const runtime = OrchestratorRuntime({ ...buildInMemoryRuntime(clock), lock: new FenceOnRenewLock(clock) });

    const result = await orchestrate({
      runtime,
      operators: [],
      seed: [workEmail()],
      completesWhen: ChatAnswerDataPoint,
      sessionDeadlineMs: 60 * SECOND_MS,
    }).run();

    // The successor owns the session; the waiter stood down.
    expect(result.status).toBe(SessionStatus.SUPERSEDED);
  });

  it('replans per drained batch, not per emission', async () => {
    const store = new CountingStore();
    const runtime = OrchestratorRuntime({ ...buildInMemoryRuntime(new FakeClock()), store });
    const chatty = makeOperator('chatty', {
      produces: [IpDataPoint],
      emits: [1, 2, 3, 4, 5].map((n) => ip(`198.51.100.${n}`)),
    });

    const result = await orchestrate({ runtime, operators: [chatty] }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect((await runtime.store.snapshot(SID)).ofType(IpDataPoint)).toHaveLength(5); // one snapshot read by the test
    expect(store.snapshots).toBeLessThanOrEqual(5); // drained in batches — far fewer re-plans than emissions
  });

  it('applies an emission burst once but keeps per-event granularity', async () => {
    const store = new CountingStore();
    const runtime = OrchestratorRuntime({ ...buildInMemoryRuntime(new FakeClock()), store });
    const chatty = makeOperator('chatty', {
      produces: [IpDataPoint],
      emits: [1, 2, 3, 4, 5].map((n) => ip(`198.51.100.${n}`)),
    });

    const result = await orchestrate({ runtime, operators: [chatty] }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(store.applies).toBe(1); // the five-emission burst was merged in ONE mirror/store apply
    const added = (await runtime.audit.replay(SID)).filter((entry) => entry.kind === AuditKind.DATA_POINT_ADDED);
    expect(added).toHaveLength(5); // batching the write never collapses per-event audit granularity
    expect(await runtime.archive.read(SID)).toHaveLength(5); // nor the per-DataPoint archive documents
    expect((await runtime.store.snapshot(SID)).ofType(IpDataPoint)).toHaveLength(5);
  });

  it('applies a pending inbox batch in one store write', async () => {
    const store = new CountingStore();
    const runtime = OrchestratorRuntime({ ...buildInMemoryRuntime(new FakeClock()), store });
    for (let index = 0; index < 3; index += 1) {
      await runtime.inbox.append(SID, chatAnswer(`answer-${index}`));
    }

    const result = await orchestrate({
      runtime,
      operators: [],
      completesWhen: ChatAnswerDataPoint,
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(store.applies).toBe(1); // the whole drained inbox batch landed in ONE apply
    expect(await runtime.inbox.pendingCount(SID)).toBe(0); // every entry was still acked individually
    const added = (await runtime.audit.replay(SID)).filter((entry) => entry.kind === AuditKind.DATA_POINT_ADDED);
    expect(added).toHaveLength(3); // per-entry audit granularity intact
    expect(
      new Set((await runtime.store.snapshot(SID)).ofType(ChatAnswerDataPoint).map((dataPoint) => dataPoint.value)),
    ).toEqual(new Set(['answer-0', 'answer-1', 'answer-2']));
  });

  it('backpressures a burst on the bounded queue without losing emissions', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const burst = makeOperator('burst', {
      produces: [IpDataPoint],
      emits: [1, 2, 3, 4, 5].map((n) => ip(`198.51.100.${n}`)),
    });
    const orchestrator = new ProbeOrchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [burst],
      emissionQueueSize: 1,
    });

    const result = await orchestrator.run();

    // The suspended emitter resumed every time the loop drained.
    expect(result.status).toBe(SessionStatus.COMPLETED);
    const stored = new Set(
      (await runtime.store.snapshot(SID)).ofType(IpDataPoint).map((dataPoint) => dataPoint.value),
    );
    // Backpressure, not loss — every emission landed.
    expect(stored).toEqual(new Set([1, 2, 3, 4, 5].map((n) => `198.51.100.${n}`)));
    expect(orchestrator.probe?.capacity).toBe(1);
    expect(orchestrator.probe?.highWater ?? 0).toBeLessThanOrEqual(1); // the queue never exceeded its bound
    const runs = runsByOperator(await runtime.audit.replay(SID));
    // The completion signal was not lost.
    expect(runs.get(OperatorId('burst'))?.outcome).toBe(OperatorOutcome.SUCCEEDED);
  });

  it('bounds the emission queue by default', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const op = makeOperator('op', { produces: [RiskDataPoint], emits: [risk()] });
    const orchestrator = orchestrate({ runtime, operators: [op] });

    expect(orchestrator.buildEmissionQueue().capacity).toBe(1024); // bounded out of the box, never unbounded
  });

  it('quarantines a poison inbox entry and still completes the session', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const inbox = runtime.inbox;
    expect(inbox).toBeInstanceOf(InMemoryInbox);
    // A payload no deploy can parse.
    const poisonId = await (inbox as InMemoryInbox).appendSerialized(SID, 'not-json{');
    const op = makeOperator('op', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });

    const { records, result } = await captureLogs(
      async () => await orchestrate({ runtime, operators: [op], seed: [workEmail()] }).run(),
    );

    expect(result.status).toBe(SessionStatus.COMPLETED); // the malformed payload did not wedge the session
    expect(result.operatorRuns.get(OperatorId('op'))).toBe(1); // the healthy flow still ran
    const quarantined = await runtime.inbox.quarantined(SID);
    expect(quarantined).toHaveLength(1);
    expect(quarantined[0]?.entryId).toBe(poisonId);
    expect(await runtime.inbox.pendingCount(SID)).toBe(0); // nothing left to crash-loop a resume
    const audited = (await runtime.audit.replay(SID)).filter(
      (entry) => entry.kind === AuditKind.INBOX_ENTRY_QUARANTINED,
    );
    expect(audited).toHaveLength(1);
    expect(audited[0]?.inbox?.entryId).toBe(poisonId);
    // The quarantine is epoch-stamped like every other event.
    expect(audited[0]?.epoch).toBe(result.epoch);
    expect(records.some((record) => record.fields.entry_id === poisonId)).toBe(true); // WARNING logged for ops
  });

  it('acks an inbox entry whose post-apply audit failed', async () => {
    // The ack gates on the durable STORE apply alone: once that landed, a failing audit append is
    // logged and absorbed — redelivering an already-applied entry would burn its delivery budget and
    // duplicate audit without fixing anything.
    const runtime = OrchestratorRuntime({ ...buildInMemoryRuntime(new FakeClock()), audit: new FailingAuditSink() });
    await runtime.inbox.append(SID, chatAnswer('applied'));

    const { records, result } = await captureLogs(
      async () =>
        await orchestrate({
          runtime,
          operators: [],
          completesWhen: ChatAnswerDataPoint,
          sessionDeadlineMs: 30 * SECOND_MS,
        }).run(),
    );

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(
      new Set((await runtime.store.snapshot(SID)).ofType(ChatAnswerDataPoint).map((dataPoint) => dataPoint.value)),
    ).toEqual(new Set(['applied']));
    expect(await runtime.inbox.pendingCount(SID)).toBe(0); // acked despite the audit failure — no redelivery
    expect(await runtime.inbox.quarantined(SID)).toEqual([]); // no delivery budget burned toward quarantine
    const error = records.find((record) => record.level === 'ERROR');
    expect(error?.message).toContain('bookkeeping');
  });

  it('acks every entry of a batch whose post-apply audit failed', async () => {
    // The batched-merge fallback is reserved for STORE apply failures; a bookkeeping failure after a
    // successful batch apply must not push the entries onto the per-entry redelivery path.
    const runtime = OrchestratorRuntime({ ...buildInMemoryRuntime(new FakeClock()), audit: new FailingAuditSink() });
    for (let index = 0; index < 3; index += 1) {
      await runtime.inbox.append(SID, chatAnswer(`answer-${index}`));
    }

    const { records, result } = await captureLogs(
      async () =>
        await orchestrate({
          runtime,
          operators: [],
          completesWhen: ChatAnswerDataPoint,
          sessionDeadlineMs: 30 * SECOND_MS,
        }).run(),
    );

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(
      new Set((await runtime.store.snapshot(SID)).ofType(ChatAnswerDataPoint).map((dataPoint) => dataPoint.value)),
    ).toEqual(new Set(['answer-0', 'answer-1', 'answer-2']));
    expect(await runtime.inbox.pendingCount(SID)).toBe(0); // the whole batch was acked
    expect(await runtime.inbox.quarantined(SID)).toEqual([]);
    expect(records.some((record) => record.level === 'ERROR')).toBe(true);
  });

  it('quarantines an unappliable inbox entry at the delivery cap', async () => {
    // A valid entry whose merge keeps failing is redelivered (the fault may be transient), but only
    // up to maxInboxDeliveries — past the cap it is quarantined so it cannot grind forever.
    const runtime = OrchestratorRuntime({ ...buildInMemoryRuntime(new FakeClock()), store: new RejectingStore() });
    await runtime.inbox.append(SID, chatAnswer('merge-bomb'));

    const result = await orchestrate({
      runtime,
      operators: [],
      seed: [workEmail()],
      completesWhen: ChatAnswerDataPoint, // keeps the loop draining instead of exiting on the first pass
      sessionDeadlineMs: 30 * SECOND_MS,
      maxInboxDeliveries: 3,
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    const quarantined = await runtime.inbox.quarantined(SID);
    expect(quarantined).toHaveLength(1);
    expect(quarantined[0]?.deliveryCount).toBe(3); // redelivered up to the cap, then quarantined
    expect(quarantined[0]?.reason).toContain('store rejected the write');
    expect(await runtime.inbox.pendingCount(SID)).toBe(0);
    // The apply never landed.
    expect((await runtime.store.snapshot(SID)).ofType(ChatAnswerDataPoint)).toEqual([]);
    const audited = (await runtime.audit.replay(SID)).filter(
      (entry) => entry.kind === AuditKind.INBOX_ENTRY_QUARANTINED,
    );
    expect(audited).toHaveLength(1);
    expect(audited[0]?.inbox?.deliveryCount).toBe(3);
  });

  it('reads watermarks once per gather, not per iteration', async () => {
    const store = new CountingStore();
    const runtime = OrchestratorRuntime({ ...buildInMemoryRuntime(new FakeClock()), store });
    let counter = 0;
    // A bounded self-cycle reruns several times, so the loop iterates well over once per operator.
    const selfCycle = makeOperator('selfloop', {
      produces: [IpDataPoint],
      dependsOn: [IpDataPoint],
      rerunOnNewData: true,
      maxCycles: 3,
      debounceMs: SECOND_MS,
      emitFactory: () => {
        const value = `ip-${counter}`;
        counter += 1;
        return [ip(value)];
      },
    });
    const other = makeOperator('other', {
      dependsOn: [EmailDataPoint],
      produces: [RiskDataPoint],
      emits: [risk()],
    });

    const result = await orchestrate({ runtime, operators: [selfCycle, other], seed: [ip('seed')] }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(store.watermarkReads).toBe(2); // exactly one rehydration read per gathering operator
  });

  it('parks once an idle wait exceeds parkAfterMs', async () => {
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const aggregated = { ran: false };
    const reporter = makeAggregator('rep', {
      dependsOn: [EmailDataPoint],
      onAggregate: async () => {
        aggregated.ran = true;
      },
    });

    const result = await orchestrate({
      runtime,
      operators: [reporter],
      seed: [workEmail()],
      completesWhen: ChatAnswerDataPoint, // never arrives — the wait stays idle
      sessionDeadlineMs: 300 * SECOND_MS,
      parkAfterMs: 30 * SECOND_MS,
    }).run();

    expect(result.status).toBe(SessionStatus.PARKED);
    expect(aggregated).toEqual({ ran: false }); // parking skips aggregation entirely
    expect(await runtime.lock.isComplete(SID)).toBe(false); // not finalized — a deliver/resume re-drives it
    expect(await runtime.lock.isHeld(SID)).toBe(false); // the epoch was released; the pod holds nothing
    expect(clock.monotonic()).toBe(30 * SECOND_MS); // parked at the idle window, far before the 300 s deadline
    const parked = (await runtime.audit.replay(SID)).filter((entry) => entry.kind === AuditKind.SESSION_PARKED);
    expect(parked).toHaveLength(1);
    expect(parked[0]?.epoch).toBe(result.epoch); // the park is epoch-stamped like every other event
  });

  it('waits out the deadline exactly as before when parking is disabled', async () => {
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);

    const result = await orchestrate({
      runtime,
      operators: [],
      seed: [workEmail()],
      completesWhen: ChatAnswerDataPoint,
      sessionDeadlineMs: 30 * SECOND_MS,
      parkAfterMs: null, // parking disabled — the deadline alone bounds the wait
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(clock.monotonic()).toBeGreaterThanOrEqual(30 * SECOND_MS); // it genuinely waited out its deadline
    expect(await runtime.lock.isComplete(SID)).toBe(true);
    expect((await runtime.audit.replay(SID)).some((entry) => entry.kind === AuditKind.SESSION_PARKED)).toBe(false);
  });

  it('treats an armed retry backoff as progress, never as idleness that parks', async () => {
    // The 20 s retry backoff dwarfs parkAfterMs, yet the session must not park during it: armed retry
    // windows ride the nextDueInMs sleep path, and the idle window only starts once the session
    // actually reaches the inbox wait (here at t=20, parking at t=20+30).
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const flaky = makeOperator('flaky', {
      dependsOn: [EmailDataPoint],
      raiseError: new Error('boom'),
      retry: RetryPolicy({ maxAttempts: 2, baseDelayMs: 20 * SECOND_MS, jitter: 0 }),
    });

    const result = await orchestrate({
      runtime,
      operators: [flaky],
      seed: [workEmail()],
      completesWhen: ChatAnswerDataPoint, // never arrives — the wait branch follows the retries
      sessionDeadlineMs: 300 * SECOND_MS,
      parkAfterMs: 30 * SECOND_MS,
    }).run();

    expect(result.status).toBe(SessionStatus.PARKED);
    expect(result.operatorRuns.get(OperatorId('flaky'))).toBe(2); // the armed retry ran out its backoff un-parked
    expect(clock.monotonic()).toBe(50 * SECOND_MS); // 20 s backoff (progress) + the full 30 s idle window
  });

  it('persists no fingerprint without a flow identity', async () => {
    // A directly-constructed orchestrator (tests, spikes) carries no flow identity — drift detection
    // is skipped entirely and nothing is written to the session meta.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const op = makeOperator('op', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });

    await orchestrate({ runtime, operators: [op], seed: [workEmail()] }).run();

    expect(await runtime.store.getFlowFingerprint(SID)).toBeNull();
  });

  it('persists the flow fingerprint on a first spawn without reporting drift', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const op = makeOperator('op', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });

    await orchestrate({
      runtime,
      operators: [op],
      seed: [workEmail()],
      flowIdentity: FlowIdentity({ name: 'orch-flow', fingerprint: 'fp-1' }),
    }).run();

    expect(await runtime.store.getFlowFingerprint(SID)).toBe('fp-1'); // absent → persisted, no drift event
    expect((await runtime.audit.replay(SID)).some((entry) => entry.kind === AuditKind.FLOW_DRIFT_DETECTED)).toBe(
      false,
    );
  });

  it('never runs a namespace-gated operator, even with its inputs present', async () => {
    const catalog = new InMemoryCapabilityCatalog({ permittedOperators: [[NAMESPACE, [OperatorId('kept')]]] });
    const runtime = buildInMemoryRuntime(new FakeClock(), { catalog });
    const kept = makeOperator('kept', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });
    const gated = makeOperator('gated', { dependsOn: [EmailDataPoint], produces: [IpDataPoint], emits: [ip()] });

    const { records, result } = await captureLogs(
      async () => await orchestrate({ runtime, operators: [kept, gated], seed: [workEmail()] }).run(),
      { level: 'INFO' },
    );

    expect(result.status).toBe(SessionStatus.COMPLETED); // the gated operator does not stall quiescence
    // `gated` never ran although its input was present.
    expect([...result.operatorRuns]).toEqual([[OperatorId('kept'), 1]]);
    expect(await storedTypes(runtime)).toEqual(new Set(['work_email', 'risk']));
    const exclusion = records.find((record) => record.fields.excluded_operator_ids !== undefined);
    expect(exclusion?.level).toBe('INFO');
    expect(exclusion?.fields.excluded_operator_ids).toEqual([OperatorId('gated')]);
    // No graph-stall warning either.
    expect(records.some((record) => record.level === 'WARNING')).toBe(false);
  });

  it('is unaffected by operator gating configured for another namespace', async () => {
    // Our namespace has no operator restriction configured (null) — behavior is exactly as before.
    const catalog = new InMemoryCapabilityCatalog({
      permittedOperators: [[NamespaceId('some-other-namespace'), []]],
    });
    const runtime = buildInMemoryRuntime(new FakeClock(), { catalog });
    const op = makeOperator('op', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });

    const result = await orchestrate({ runtime, operators: [op], seed: [workEmail()] }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect([...result.operatorRuns]).toEqual([[OperatorId('op'), 1]]);
  });

  it('runs nothing and still completes when the namespace permits no operator', async () => {
    // An empty set is the explicit deny-everything configuration (distinct from null).
    const catalog = new InMemoryCapabilityCatalog({ permittedOperators: [[NAMESPACE, []]] });
    const runtime = buildInMemoryRuntime(new FakeClock(), { catalog });
    const op = makeOperator('op', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint], emits: [risk()] });

    const result = await orchestrate({ runtime, operators: [op], seed: [workEmail()] }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED); // nothing ran, but the session terminated cleanly
    expect([...result.operatorRuns]).toEqual([]);
    expect(await storedTypes(runtime)).toEqual(new Set(['work_email']));
    expect(await runtime.lock.isComplete(SID)).toBe(true);
  });

  it('skips a gated aggregator without wedging the session', async () => {
    const catalog = new InMemoryCapabilityCatalog({ permittedOperators: [[NAMESPACE, [OperatorId('scorer')]]] });
    const runtime = buildInMemoryRuntime(new FakeClock(), { catalog });
    const scorer = makeOperator('scorer', {
      dependsOn: [EmailDataPoint],
      produces: [RiskDataPoint],
      emits: [risk()],
    });
    const reporter = makeAggregator('rep', {
      dependsOn: [RiskDataPoint],
      onAggregate: async (ctx) => {
        await ctx.aggregation?.upsert('reports', 'report', { riskCount: ctx.store.ofType(RiskDataPoint).length });
      },
    });

    const result = await orchestrate({ runtime, operators: [scorer, reporter], seed: [workEmail()] }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED); // the gated aggregator did not wedge aggregation
    expect([...result.operatorRuns]).toEqual([[OperatorId('scorer'), 1]]);
    expect(result.deadLetters).toEqual([]);
    expect(await runtime.durable.read('reports', 'report')).toBeNull(); // its output domain was not written
  });

  it('continues a shrinking deadline budget across a resume', async () => {
    // The wall-clock deadline persisted at the first gather is rehydrated on resume: the second run
    // gets the REMAINING 40 s of the 100 s budget, not a fresh 100 s window.
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const build = (parkAfterMs: number | null, seed: readonly AnyDataPoint[]): Orchestrator =>
      orchestrate({
        runtime,
        operators: [],
        seed,
        completesWhen: ChatAnswerDataPoint, // never arrives — only the deadline can end the wait
        sessionDeadlineMs: 100 * SECOND_MS,
        parkAfterMs,
      });

    const first = await build(10 * SECOND_MS, [workEmail()]).run();
    expect(first.status).toBe(SessionStatus.PARKED); // 10 s of the budget spent waiting
    clock.advance(50 * SECOND_MS); // 50 more seconds pass while the session sits parked

    const resumedAt = clock.monotonic();
    const second = await build(null, []).run();

    expect(second.status).toBe(SessionStatus.COMPLETED); // the deadline, not a park, ended the resume
    expect(clock.monotonic() - resumedAt).toBe(40 * SECOND_MS); // 100 s budget - 10 s waited - 50 s parked
  });

  it('aggregates immediately when a resume starts past the stored deadline', async () => {
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const seen: { emails?: number } = {};
    const reporter = makeAggregator('rep', {
      dependsOn: [EmailDataPoint],
      onAggregate: async (ctx) => {
        seen.emails = ctx.store.ofType(EmailDataPoint).length;
      },
    });
    const build = (seed: readonly AnyDataPoint[]): Orchestrator =>
      orchestrate({
        runtime,
        operators: [reporter],
        seed,
        completesWhen: ChatAnswerDataPoint,
        sessionDeadlineMs: 100 * SECOND_MS,
        parkAfterMs: 10 * SECOND_MS,
      });

    const first = await build([workEmail()]).run();
    expect(first.status).toBe(SessionStatus.PARKED);
    expect(seen).toEqual({});
    clock.advance(150 * SECOND_MS); // the stored wall-clock deadline passes while the session is parked

    const resumedAt = clock.monotonic();
    const second = await build([]).run();

    expect(second.status).toBe(SessionStatus.COMPLETED);
    expect(seen).toEqual({ emails: 1 }); // aggregation ran over the rehydrated state
    expect(clock.monotonic()).toBe(resumedAt); // no gathering wait at all — the budget was already spent
  });

  it('makes an armed retry terminal once the breaker trips', async () => {
    // A self-cycle operator declares BOTH a circuit breaker (maxCycles) and a retry policy, and fails
    // on every attempt. `recordCompletion` records the breaker run BEFORE `maybeScheduleRetry`, so the
    // attempt that trips the breaker must make the retry terminal even with attempts remaining: the
    // operator is permanently skipped in `plan`, so an armed retry could never launch — arming one
    // would leave `failedAttempts` dangling and the session waiting on a relaunch that never comes due.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const flakyCycle = makeOperator('flakyloop', {
      produces: [IpDataPoint],
      dependsOn: [IpDataPoint],
      rerunOnNewData: true,
      maxCycles: 2, // breaker trips on the 2nd run
      retry: RetryPolicy({ maxAttempts: 5, baseDelayMs: 20 * SECOND_MS, jitter: 0 }), // attempts remain
      raiseError: new Error('boom'),
    });

    const { records, result } = await captureLogs(
      async () => await orchestrate({ runtime, operators: [flakyCycle], seed: [ip('seed')] }).run(),
      { level: 'WARNING' },
    );

    expect(result.status).toBe(SessionStatus.COMPLETED); // the loop reached aggregation deterministically
    expect(result.operatorRuns.get(OperatorId('flakyloop'))).toBe(2); // bounded by maxCycles, not maxAttempts
    const scheduled = records.filter((record) => record.message.includes('a retry is scheduled'));
    const terminal = records.filter((record) => record.message.includes('no retry remaining'));
    // Only the FIRST failure arms a retry (breaker count 1 < cap 2). The SECOND failure trips the
    // breaker (`recordRun` runs before `maybeScheduleRetry`), so its retry is foreclosed and recorded
    // as terminal — no third relaunch is ever armed, so `failedAttempts` leaves nothing dangling for
    // quiescence to wait on.
    expect(scheduled).toHaveLength(1);
    expect(terminal).toHaveLength(1);
  });

  it('restarts the idle park clock on a non-satisfying inbox arrival', async () => {
    // The completion condition needs BOTH a chat answer and an Ip. The session enters the inbox wait,
    // idles partway, then a chat answer (activity, but not enough to satisfy allOf) arrives at t=20.
    // The loop re-plans, finds the condition unsatisfied, and re-enters the wait with a FRESH parkAt:
    // parking is measured from continuous idleness, so the session parks a full parkAfterMs AFTER the
    // arrival (t=20+30=50), not at the original t=30.
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);

    const deliverAPartialArrivalAtT20 = async (): Promise<void> => {
      await until(
        () => clock.monotonic() >= 20 * SECOND_MS,
        'the fake clock never reached t=20 while the session waited',
      );
      await runtime.inbox.append(SID, chatAnswer('partial')); // activity, but allOf still needs an Ip
    };

    const orchestrator = orchestrate({
      runtime,
      operators: [],
      seed: [workEmail()],
      completesWhen: allOf(ChatAnswerDataPoint, IpDataPoint), // the Ip never arrives
      sessionDeadlineMs: 300 * SECOND_MS,
      parkAfterMs: 30 * SECOND_MS,
    });
    const [result] = await Promise.all([orchestrator.run(), deliverAPartialArrivalAtT20()]);

    expect(result.status).toBe(SessionStatus.PARKED); // never satisfied → eventually parks
    const folded = new Set(
      (await runtime.store.snapshot(SID)).ofType(ChatAnswerDataPoint).map((dataPoint) => dataPoint.value),
    );
    expect(folded).toEqual(new Set(['partial'])); // the arrival was folded into the session before re-parking
    // The park fired a full 30 s window measured from the t=20 arrival, NOT from the original wait
    // start — an inbox arrival is activity, so only continuous idleness can park the session.
    expect(clock.monotonic()).toBe(50 * SECOND_MS);
  });

  it('reruns an already-run operator when a capability becomes available', async () => {
    // A newly-available capability is a first-class rerun trigger, distinct from new data: the watcher
    // depends only on Email (present from the seed) and does NOT depend on Ip. It runs once while the
    // capability is offline (the capability's own Ip dependency has not arrived). A feeder then emits
    // Ip mid-run, the capability comes online, and the watcher reruns SOLELY because
    // delta.newlyAvailableCaps is non-empty — the Ip itself is not a depended-on type, so the data
    // branch could never have triggered.
    const catalog = new InMemoryCapabilityCatalog({ permitted: [[NAMESPACE, [CapabilityId('geo_lookup')]]] });
    const runtime = buildInMemoryRuntime(new FakeClock(), { catalog });
    const geoLookup = makeCapability('geo_lookup', { dependsOn: [IpDataPoint] }); // offline until an Ip lands
    const watcherStarted = new Deferred<void>();
    const ipMerged = new Deferred<void>();
    const deltas: { readonly caps: readonly CapabilityId[]; readonly added: readonly string[] }[] = [];

    class CapWatcher extends Operator {
      public static readonly operatorId = OperatorId('cap_watcher');
      public static readonly policy = OperatorPolicy({ rerunOnNewData: true, debounceMs: 0 });
      public static readonly dependsOn = [EmailDataPoint]; // ready from the seed; does NOT depend on Ip
      public static readonly produces = [ChatAnswerDataPoint];

      public async *run(ctx: OperatorContext): AsyncIterable<DataPointEmission> {
        deltas.push({
          caps: [...ctx.delta.newlyAvailableCaps],
          added: [...ctx.delta.added].map((dataPoint) => dataPoint.type),
        });
        if (!watcherStarted.settled) {
          watcherStarted.resolve(); // release the feeder to emit the Ip that brings the capability online
          await ipMerged.promise; // stay running until that Ip has actually been merged
        }
        yield ChatAnswerDataPoint.emit('seen'); // nothing depends on this — must not re-trigger the watcher
      }
    }
    operator(CapWatcher);

    class CapFeeder extends Operator {
      public static readonly operatorId = OperatorId('cap_feeder');
      public static readonly policy = OperatorPolicy({ rerunOnNewData: false });
      public static readonly dependsOn = [EmailDataPoint];
      public static readonly produces = [IpDataPoint];

      public async *run(): AsyncIterable<DataPointEmission> {
        await watcherStarted.promise; // emit the Ip only once the watcher is mid-run
        yield IpDataPoint.emit('203.0.113.7');
      }
    }
    operator(CapFeeder);

    const releaseOnceIpIsMerged = async (): Promise<void> => {
      await until(
        async () => (await runtime.store.snapshot(SID)).ofType(IpDataPoint).length > 0,
        'timed out waiting for the Ip to merge',
      );
      ipMerged.resolve();
    };

    const orchestrator = orchestrate({
      runtime,
      operators: [CapWatcher, CapFeeder],
      capabilities: [geoLookup],
      seed: [workEmail()],
    });
    const [result] = await Promise.all([orchestrator.run(), releaseOnceIpIsMerged()]);

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(result.operatorRuns.get(OperatorId('cap_watcher'))).toBe(2); // reran once the capability came online
    expect(deltas[0]?.caps).toEqual([]); // the capability was offline at the first run
    expect(deltas[1]?.caps).toContain(CapabilityId('geo_lookup')); // the capability drove the rerun
    // The Ip is NOT a watcher dependency, so even though it appears in delta.added it cannot have
    // triggered the rerun via the data branch — only the newly-available capability could.
    expect(deltas[1]?.added).not.toContain('work_email');
  });

  it('re-observes a pre-existing identity presented twice in one batch', async () => {
    // The intra-batch duplicate-of-a-PRE-EXISTING-identity branch (`key in updated`) must reproduce
    // the store's keyed-merge: a row established before the batch, then presented twice within one
    // later batch, stays a SINGLE updated row carrying its final (max) lastRetrieved — never two rows,
    // never a stale stamp.
    const t1 = new Date(T0.getTime() + 5 * SECOND_MS);
    const t2 = new Date(T0.getTime() + 10 * SECOND_MS);
    const epoch = Epoch(1);
    const mirrorStore = new InMemoryDataPointStore();
    const reference = new InMemoryDataPointStore();
    const mirror = new SessionStateMirror(mirrorStore, SID);
    await mirror.rehydrate();

    // batch1 establishes the identity at T0 in both the mirror and the reference store.
    await mirror.write([workEmail(undefined, { last: T0 })], { epoch });
    await reference.write(SID, [workEmail(undefined, { last: T0 })], { epoch });
    const revisionAfterBatch1 = await mirror.revision(SID);

    // batch2 presents the SAME pre-existing identity twice: the first occurrence takes the
    // existing-entry branch into `updated`, the second takes the `key in updated` branch and
    // re-observes again.
    const batch2 = [workEmail(undefined, { last: t1 }), workEmail(undefined, { last: t2 })];
    const mirrorResult = await mirror.write(batch2, { epoch });
    await reference.write(SID, batch2, { epoch });

    // Exact parity with the store reference: snapshot, revision, and the change-set since batch1.
    const snapshotOf = (view: DataPointView): readonly string[] =>
      view
        .all()
        .map((dataPoint) => `${String(dataPoint.value)}|${dataPoint.lastRetrieved.toISOString()}`)
        .sort();
    expect(snapshotOf(await mirror.snapshot(SID))).toEqual(snapshotOf(await reference.snapshot(SID)));
    expect(await mirror.revision(SID)).toBe(await reference.revision(SID));
    const mirrorChanges = await mirror.changeSetSince(SID, revisionAfterBatch1);
    const referenceChanges = await reference.changeSetSince(SID, revisionAfterBatch1);
    expect(mirrorChanges.updated.map((dataPoint) => dataPoint.value)).toEqual(
      referenceChanges.updated.map((dataPoint) => dataPoint.value),
    );
    expect(mirrorChanges.added.map((dataPoint) => dataPoint.value)).toEqual(
      referenceChanges.added.map((dataPoint) => dataPoint.value),
    );

    // The single surviving row carries the final (max) timestamp, and the batch produced one row.
    const entries = (await mirror.snapshot(SID)).all();
    expect(entries).toHaveLength(1);
    expect(entries[0]?.lastRetrieved.getTime()).toBe(t2.getTime());
    expect(mirrorResult.outcomes).toHaveLength(2); // two presented DataPoints, both resolved as UPDATED
    expect(mirrorResult.outcomes.every((outcome) => outcome.kind === 'updated')).toBe(true);
  });

  it('lets the deadline win the tie against an equal park window', async () => {
    // parkAt == deadline (parkAfterMs == sessionDeadlineMs): the deadline check precedes the park
    // check in `waitForInbox`, so the deadline wins the exact tie — the session aggregates and
    // COMPLETEs and never parks, despite parkAfterMs being set. This ordering is load-bearing for a
    // flow whose shrinking budget drops to exactly parkAfterMs.
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const aggregated = { ran: false };
    const reporter = makeAggregator('rep', {
      dependsOn: [EmailDataPoint],
      onAggregate: async () => {
        aggregated.ran = true;
      },
    });

    const result = await orchestrate({
      runtime,
      operators: [reporter],
      seed: [workEmail()],
      completesWhen: ChatAnswerDataPoint, // never arrives — only the deadline/park can end the wait
      sessionDeadlineMs: 30 * SECOND_MS,
      parkAfterMs: 30 * SECOND_MS, // exactly equal to the deadline → the tie must resolve to the deadline
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED); // the deadline won the tie, not parking
    expect(aggregated).toEqual({ ran: true }); // aggregation ran (parking would have skipped it)
    expect(clock.monotonic()).toBe(30 * SECOND_MS); // the wait ended exactly at the tie instant
    expect((await runtime.audit.replay(SID)).some((entry) => entry.kind === AuditKind.SESSION_PARKED)).toBe(false);
  });

  it('lets the deadline win when the park window exceeds it', async () => {
    // parkAfterMs > sessionDeadlineMs can never park: the deadline always elapses first, so the
    // session completes at the deadline regardless of the larger park window.
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);

    const result = await orchestrate({
      runtime,
      operators: [],
      seed: [workEmail()],
      completesWhen: ChatAnswerDataPoint,
      sessionDeadlineMs: 30 * SECOND_MS,
      parkAfterMs: 60 * SECOND_MS, // larger than the deadline → unreachable, the deadline bounds the wait
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(clock.monotonic()).toBe(30 * SECOND_MS); // ended at the deadline, never at the (later) park window
    expect((await runtime.audit.replay(SID)).some((entry) => entry.kind === AuditKind.SESSION_PARKED)).toBe(false);
  });

  it('does not silently unbound the queue when emissionQueueSize is zero', async () => {
    // A queue built with a non-positive bound would be UNBOUNDED, defeating backpressure entirely and
    // letting a runaway streamer grow the heap without limit. A non-positive size must be coerced to
    // the bounded default rather than becoming an immortal-buffer footgun.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const op = makeOperator('op', { produces: [RiskDataPoint], emits: [risk()] });
    const orchestrator = orchestrate({ runtime, operators: [op], emissionQueueSize: 0 });

    const queue = orchestrator.buildEmissionQueue();
    expect(queue.capacity).toBeGreaterThan(0); // bounded, never the unbounded sentinel
    expect(queue.capacity).toBe(1024); // coerced to the bounded default
  });

  it('keeps a burst bounded even when emissionQueueSize is zero', async () => {
    // End to end: a multi-emission burst under emissionQueueSize=0 must NOT let the queue grow to the
    // full burst size (which an unbounded queue would). The high-water mark proves backpressure held.
    const runtime = buildInMemoryRuntime(new FakeClock());
    const values = Array.from({ length: 20 }, (_unused, index) => `198.51.100.${index + 1}`);
    const burst = makeOperator('burst', { produces: [IpDataPoint], emits: values.map((value) => ip(value)) });
    const orchestrator = new ProbeOrchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [burst],
      emissionQueueSize: 0,
    });

    const result = await orchestrator.run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    const stored = new Set(
      (await runtime.store.snapshot(SID)).ofType(IpDataPoint).map((dataPoint) => dataPoint.value),
    );
    expect(stored).toEqual(new Set(values)); // backpressure, not loss — every emission landed
    expect(orchestrator.probe?.capacity ?? 0).toBeGreaterThan(0); // coerced to a bounded queue
    // The queue never exceeds its own bound — the genuine backpressure invariant. Unbounded, it would
    // have grown to hold the full burst at once; bounded, it never can.
    expect(orchestrator.probe?.highWater ?? 0).toBeLessThanOrEqual(orchestrator.probe?.capacity ?? 0);
  });

  it('cancels an in-flight operator at the session deadline and re-drains', async () => {
    // The deadline fires while an operator is genuinely mid-stream. The loop exits its while-condition
    // with the straggler still in `running`, logs the deadline hit naming it, and `drainRemaining`
    // persists the already-queued emissions, cancels the straggler (the deadline is the ONE signal
    // allowed to stop a running operator), then drains ONCE MORE to capture the emission the run
    // enqueues as it is cancelled.
    const clock = new FakeClock();
    const probe = new TelemetryProbe();
    const runtime = buildInMemoryRuntime(clock, { telemetry: probe.telemetry });
    const ipEmitted = new Deferred<void>();
    const wakeLoop = new Deferred<void>();

    class Straggler extends Operator {
      public static readonly operatorId = OperatorId('straggler');
      // A large per-op timeout so only the session deadline (not the per-op timeout) can stop it.
      public static readonly policy = OperatorPolicy({ rerunOnNewData: false, timeoutMs: 3_600 * SECOND_MS });
      public static readonly dependsOn = [EmailDataPoint];
      public static readonly produces = [IpDataPoint];

      public async *run(ctx: OperatorContext): AsyncIterable<DataPointEmission> {
        yield IpDataPoint.emit('before-deadline'); // a pre-deadline emission that must persist
        ipEmitted.resolve();
        try {
          await cancellableSleep(3_600 * SECOND_MS, ctx.signal); // keeps the run in flight until cancelled
        } catch (error) {
          yield IpDataPoint.emit('on-cancel'); // a final emission enqueued right as the run is cancelled
          throw error;
        }
      }
    }
    operator(Straggler);

    class Ticker extends Operator {
      // Wakes the gather loop (blocked on the queue) after the clock has been advanced past the
      // deadline, so the loop re-checks the deadline with the straggler still running.
      public static readonly operatorId = OperatorId('ticker');
      public static readonly policy = OperatorPolicy({ rerunOnNewData: false });
      public static readonly dependsOn = [EmailDataPoint];
      public static readonly produces = [RiskDataPoint];

      public async *run(): AsyncIterable<DataPointEmission> {
        await wakeLoop.promise;
        yield RiskDataPoint.emit(0.5);
      }
    }
    operator(Ticker);

    const tripTheDeadlineMidRun = async (): Promise<void> => {
      await until(
        async () =>
          ipEmitted.settled &&
          (await runtime.store.snapshot(SID)).ofType(IpDataPoint).some((dp) => dp.value === 'before-deadline'),
        'the straggler never emitted before the deadline was tripped',
      );
      clock.advance(20 * SECOND_MS); // push monotonic past the 10 s deadline while the straggler runs
      wakeLoop.resolve(); // wake the loop so it re-checks the (now elapsed) deadline
    };

    const orchestrator = orchestrate({
      runtime,
      operators: [Straggler, Ticker],
      seed: [workEmail()],
      operationTimeoutMs: 3_600 * SECOND_MS, // the global default must not pre-empt either operator
      sessionDeadlineMs: 10 * SECOND_MS,
    });
    const { records, result } = await captureLogs(
      async () => {
        const [outcome] = await Promise.all([orchestrator.run(), tripTheDeadlineMidRun()]);
        return outcome;
      },
      { level: 'WARNING' },
    );

    expect(result.status).toBe(SessionStatus.COMPLETED); // aggregation still ran after the deadline-cancel
    const merged = new Set((await runtime.store.snapshot(SID)).ofType(IpDataPoint).map((dp) => dp.value));
    expect(merged.has('before-deadline')).toBe(true); // the pre-deadline emission persisted
    expect(merged.has('on-cancel')).toBe(true); // the post-cancel final emission was captured by the second drain
    expect(await probe.counter('session_deadline_hits_total')).toBe(1);
    const hit = records.find((record) => record.message.includes('Session deadline hit'));
    // The running operator is named.
    expect(hit?.fields.in_flight_operator_ids).toContain(String(OperatorId('straggler')));
  });

  it('cuts a running operator short at the deadline instead of awaiting its end', async () => {
    // POLICY-03/06: the deadline is the ONE signal allowed to stop a running operator. An operator
    // that would emit again after a long sleep is cancelled mid-sleep — its pre-deadline emission
    // persists, but the post-sleep emission never lands, proving the run was cut short rather than
    // awaited to completion.
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const preEmitted = new Deferred<void>();
    const wakeLoop = new Deferred<void>();

    class LongRunner extends Operator {
      public static readonly operatorId = OperatorId('long_runner');
      public static readonly policy = OperatorPolicy({ rerunOnNewData: false, timeoutMs: 3_600 * SECOND_MS });
      public static readonly dependsOn = [EmailDataPoint];
      public static readonly produces = [IpDataPoint];

      public async *run(ctx: OperatorContext): AsyncIterable<DataPointEmission> {
        yield IpDataPoint.emit('pre-deadline');
        preEmitted.resolve();
        await cancellableSleep(3_600 * SECOND_MS, ctx.signal); // the deadline must cancel this, not wait it out
        yield IpDataPoint.emit('post-sleep'); // unreachable once cancelled
      }
    }
    operator(LongRunner);

    class Ticker extends Operator {
      public static readonly operatorId = OperatorId('ticker');
      public static readonly policy = OperatorPolicy({ rerunOnNewData: false });
      public static readonly dependsOn = [EmailDataPoint];
      public static readonly produces = [RiskDataPoint];

      public async *run(): AsyncIterable<DataPointEmission> {
        await wakeLoop.promise;
        yield RiskDataPoint.emit(0.5);
      }
    }
    operator(Ticker);

    const tripTheDeadlineMidRun = async (): Promise<void> => {
      await until(
        async () =>
          preEmitted.settled &&
          (await runtime.store.snapshot(SID)).ofType(IpDataPoint).some((dp) => dp.value === 'pre-deadline'),
        'the long-runner never emitted before the deadline was tripped',
      );
      clock.advance(20 * SECOND_MS);
      wakeLoop.resolve();
    };

    const { records, result } = await captureLogs(
      async () => {
        const [outcome] = await Promise.all([
          orchestrate({
            runtime,
            operators: [LongRunner, Ticker],
            seed: [workEmail()],
            operationTimeoutMs: 3_600 * SECOND_MS,
            sessionDeadlineMs: 10 * SECOND_MS,
          }).run(),
          tripTheDeadlineMidRun(),
        ]);
        return outcome;
      },
      { level: 'WARNING' },
    );

    expect(result.status).toBe(SessionStatus.COMPLETED); // aggregation still runs after the deadline cancel
    const merged = new Set((await runtime.store.snapshot(SID)).ofType(IpDataPoint).map((dp) => dp.value));
    expect(merged.has('pre-deadline')).toBe(true); // the emission produced before the deadline persisted
    expect(merged.has('post-sleep')).toBe(false); // cancelled mid-sleep, never reaching its later yield
    const hit = records.find((record) => record.message.includes('Session deadline hit'));
    expect(hit?.fields.in_flight_operator_ids).toContain(String(OperatorId('long_runner')));
  });

  it('lets a wakeup in the same pass as an elapsed park window win', async () => {
    // 'Data beats parking': the wait loop checks whether the waiter settled at the TOP, before
    // re-checking parkAt. When the renew timer advances now to exactly parkAt on the same pass the
    // inbox waiter resolves, the next iteration sees the wakeup and returns it — the elapsed park
    // window does not win the tie.
    const clock = new FakeClock();
    const probe = new TelemetryProbe();
    const runtime = buildInMemoryRuntime(clock, { telemetry: probe.telemetry });

    const deliverExactlyAtTheParkInstant = async (): Promise<void> => {
      // parkAfterMs is 30 s; append the moment the fake clock reaches it (== parkAt), inside the
      // timer's own yield, so the waiter resolves in the same race the park window elapses.
      await until(() => clock.monotonic() >= 30 * SECOND_MS, 'the fake clock never reached the park instant');
      await runtime.inbox.append(SID, chatAnswer('arrived'));
    };

    const orchestrator = orchestrate({
      runtime,
      operators: [],
      seed: [workEmail()],
      completesWhen: ChatAnswerDataPoint, // satisfied by the arrival → the wakeup ends the run
      sessionDeadlineMs: 300 * SECOND_MS, // far away — only the park-vs-wakeup tie is under test
      parkAfterMs: 30 * SECOND_MS,
    });
    const [result] = await Promise.all([orchestrator.run(), deliverExactlyAtTheParkInstant()]);

    expect(result.status).toBe(SessionStatus.COMPLETED); // the wakeup won; parking would have left it PARKED
    expect(
      new Set((await runtime.store.snapshot(SID)).ofType(ChatAnswerDataPoint).map((dataPoint) => dataPoint.value)),
    ).toEqual(new Set(['arrived']));
    expect((await runtime.audit.replay(SID)).some((entry) => entry.kind === AuditKind.SESSION_PARKED)).toBe(false);
    // The wait span's own outcome attribute records what ended the wait: a wakeup, not a park.
    const waitSpans = probe.spans('session.inbox_wait');
    expect(waitSpans.length).toBeGreaterThan(0); // the session did enter an inbox wait
    expect(waitSpans[waitSpans.length - 1]?.attributes.outcome).toBe('wakeup');
  });

  it('never lets the merge counter outrun the audit trail when the append fails', async () => {
    // README invariant: metrics ride the audit seam so they can never disagree. When the batched audit
    // append fails after a successful store apply (a transient sink fault), the
    // data_points_merged_total counter must NOT advance for entries whose audit row never committed —
    // otherwise the metric and the replayed audit trail disagree, exactly what the seam prevents.
    const probe = new TelemetryProbe();
    const runtime = OrchestratorRuntime({
      ...buildInMemoryRuntime(new FakeClock(), { telemetry: probe.telemetry }),
      audit: new BatchAuditFailingSink(),
    });
    for (let index = 0; index < 3; index += 1) {
      await runtime.inbox.append(SID, chatAnswer(`answer-${index}`)); // a 3-entry batch → one appendMany
    }

    const result = await captureLogs(
      async () =>
        await orchestrate({
          runtime,
          operators: [],
          completesWhen: ChatAnswerDataPoint,
          sessionDeadlineMs: 30 * SECOND_MS,
        }).run(),
    );

    // The bookkeeping failure is absorbed, the session finishes.
    expect(result.result.status).toBe(SessionStatus.COMPLETED);
    const committedAdded = (await runtime.audit.replay(SID)).filter(
      (entry) => entry.kind === AuditKind.DATA_POINT_ADDED,
    ).length;
    // The counter must equal the number of DATA_POINT_ADDED rows that actually committed — they ride
    // the same seam, so a failed append leaves neither the row nor the count behind.
    expect(await probe.counterTotal('data_points_merged_total')).toBe(committedAdded);
  });

  it('bounds the store and the mirror of a chatty self-cycle by the cycle cap', async () => {
    // S2 (unbounded-growth bound beyond emissionQueueSize): a chatty self-cycle that emits a NEW
    // distinct identity every cycle must not grow the session's in-memory state without limit. The
    // circuit breaker caps the cycle at maxCycles, so both the durable store and the sole-mutator
    // mirror hold at most one identity per cycle plus the seed — bounded by the cap, not by unbounded
    // distinct identities.
    const runtime = buildInMemoryRuntime(new FakeClock());
    let counter = 0;
    const chattyCycle = makeOperator('chattyloop', {
      produces: [IpDataPoint],
      dependsOn: [IpDataPoint],
      rerunOnNewData: true,
      maxCycles: 3,
      emitFactory: () => {
        const value = `ip-${counter}`; // a brand-new identity each cycle
        counter += 1;
        return [ip(value)];
      },
    });
    const orchestrator = new ProbeOrchestrator({
      sessionId: SID,
      namespaceId: NAMESPACE,
      runtime,
      operators: [chattyCycle],
      seed: [ip('seed')],
    });

    const result = await orchestrator.run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    const runs = result.operatorRuns.get(OperatorId('chattyloop')) ?? 0;
    expect(runs).toBeLessThanOrEqual(3); // bounded by the armed circuit breaker
    const storedIps = (await runtime.store.snapshot(SID)).ofType(IpDataPoint);
    // Each run adds exactly one new identity; with the seed that is runs + 1, and never more — the cap
    // bounds the distinct-identity growth, so the store cannot accumulate without limit.
    expect(storedIps).toHaveLength(runs + 1);
    expect(storedIps.length).toBeLessThanOrEqual(4); // maxCycles (3) distinct emissions + the seed
    // The sole-mutator mirror holds the identical bounded set — it never diverges from the store.
    expect(orchestrator.mirroredIdentities).toBe(storedIps.length);
  });

  // S3 (systematic clock-edge table): evaluate the deadline-vs-park boundary at -epsilon / exact tie /
  // +epsilon around sessionDeadlineMs=30 s. The wait checks the deadline BEFORE parkAt, so the exact
  // tie and the over-shoot both resolve to the deadline (COMPLETED); only a strictly-earlier park
  // window parks.
  const parkBoundaryCases: readonly {
    readonly parkAfterMs: number;
    readonly expectedStatus: SessionStatus;
    readonly expectedMonotonic: number;
  }[] = [
    { parkAfterMs: 29 * SECOND_MS, expectedStatus: SessionStatus.PARKED, expectedMonotonic: 29 * SECOND_MS },
    { parkAfterMs: 30 * SECOND_MS, expectedStatus: SessionStatus.COMPLETED, expectedMonotonic: 30 * SECOND_MS },
    { parkAfterMs: 31 * SECOND_MS, expectedStatus: SessionStatus.COMPLETED, expectedMonotonic: 30 * SECOND_MS },
  ];
  for (const boundary of parkBoundaryCases) {
    it(`resolves the park-vs-deadline boundary at parkAfterMs=${boundary.parkAfterMs}`, async () => {
      const clock = new FakeClock();
      const runtime = buildInMemoryRuntime(clock);

      const result = await orchestrate({
        runtime,
        operators: [],
        seed: [workEmail()],
        completesWhen: ChatAnswerDataPoint, // never arrives — only the deadline or the park ends the wait
        sessionDeadlineMs: 30 * SECOND_MS,
        parkAfterMs: boundary.parkAfterMs,
      }).run();

      expect(result.status).toBe(boundary.expectedStatus);
      expect(clock.monotonic()).toBe(boundary.expectedMonotonic);
      const parked = (await runtime.audit.replay(SID)).filter((entry) => entry.kind === AuditKind.SESSION_PARKED);
      // The audit trail agrees with the verdict.
      expect(parked.length > 0).toBe(boundary.expectedStatus === SessionStatus.PARKED);
    });
  }

  it('drains a late inbox entry before releasing, on the same epoch', async () => {
    // A late /participant deliver that lands during the completion flush must be drained in-run on the
    // SAME epoch (no re-spawn): after aggregating + flushing the orchestrator re-checks the inbox and,
    // finding a pending entry, loops back to gather + aggregate before releasing the lock.
    const runtime = buildInMemoryRuntime(new FakeClock());
    // Unique session id + durable table + operator id so the test is hermetic under any ordering (the
    // shared 'rep'/'reports' names are reused by many other tests).
    const sid = SessionId('orch-drain-late-session');
    const seenEmails: (readonly string[])[] = [];
    const injected = { done: false };

    const reporter = makeAggregator('drain_late_reporter', {
      dependsOn: [EmailDataPoint],
      onAggregate: async (ctx) => {
        const emails = ctx.store
          .ofType(EmailDataPoint)
          .map((dataPoint) => dataPoint.value)
          .sort();
        seenEmails.push(emails);
        await ctx.aggregation?.upsert('drain_late_reports', 'report', { emails });
        // Simulate a /participant deliver landing during the FIRST completion pass (mid-aggregate),
        // i.e. inside the flush window: append a late attribute to the inbox exactly once.
        if (!injected.done) {
          injected.done = true;
          await runtime.inbox.append(sid, workEmail('late@work.example'));
        }
      },
    });

    const result = await new Orchestrator({
      sessionId: sid,
      namespaceId: NAMESPACE,
      runtime,
      operators: [reporter],
      completesWhen: EmailDataPoint,
      seed: [workEmail()],
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(result.epoch).toBe(1); // SAME run/epoch — the late entry was drained in-run, NOT via a re-spawn
    const doc = await runtime.durable.read('drain_late_reports', 'report');
    expect(doc).not.toBeNull();
    const writtenEmails = (doc?.document.emails ?? []) as readonly string[];
    expect([...writtenEmails].sort()).toEqual(['alice@work.example', 'late@work.example']);
    expect(await runtime.inbox.pendingCount(sid)).toBe(0); // fully drained before release
    expect(seenEmails).toHaveLength(2); // the aggregator ran twice in the one run: initial + re-drive
  });

  it('records an operator constructor failure instead of silently dropping it', async () => {
    // An operator whose constructor throws (e.g. config validation) must be recorded as a FAILED run —
    // logged + audited — not dropped before it can enqueue completion (which would wedge it 'in
    // flight' with no trace). A second healthy operator must still complete the session.
    const runtime = buildInMemoryRuntime(new FakeClock());

    class BadInit extends Operator {
      public static readonly operatorId = OperatorId('bad_init');
      public static readonly policy = OperatorPolicy({ rerunOnNewData: false });
      public static readonly dependsOn = [EmailDataPoint];
      public static readonly produces = [RiskDataPoint];

      public constructor() {
        super();
        throw new Error('construction blew up');
      }

      public async *run(): AsyncIterable<DataPointEmission> {
        yield RiskDataPoint.emit(0.5);
      }
    }
    operator(BadInit);

    class Healthy extends Operator {
      public static readonly operatorId = OperatorId('healthy');
      public static readonly policy = OperatorPolicy({ rerunOnNewData: false });
      public static readonly dependsOn = [EmailDataPoint];
      public static readonly produces = [IpDataPoint];

      public async *run(): AsyncIterable<DataPointEmission> {
        yield IpDataPoint.emit('203.0.113.9');
      }
    }
    operator(Healthy);

    const { records, result } = await captureLogs(
      async () => await orchestrate({ runtime, operators: [BadInit, Healthy], seed: [workEmail()] }).run(),
      { level: 'ERROR' },
    );

    expect(result.status).toBe(SessionStatus.COMPLETED); // the healthy operator still finishes the session
    expect(result.operatorRuns.get(OperatorId('bad_init'))).toBe(1); // the construction failure counts as a run
    const runs = runsByOperator(await runtime.audit.replay(SID));
    // Audited as FAILED, not absent.
    expect(runs.get(OperatorId('bad_init'))?.outcome).toBe(OperatorOutcome.FAILED);
    // The audit records each run's own time (invoked → completed) for profiling — present even on failure.
    expect(runs.get(OperatorId('healthy'))?.runSeconds).not.toBeNull();
    expect(runs.get(OperatorId('bad_init'))?.runSeconds).not.toBeNull();
    const failure = records.find((record) => record.fields.operator_id === OperatorId('bad_init'));
    expect(failure?.fields.error).toBeInstanceOf(Error); // logged with the error, not swallowed
    expect((await runtime.store.snapshot(SID)).ofType(IpDataPoint).length).toBeGreaterThan(0);
  });
});
