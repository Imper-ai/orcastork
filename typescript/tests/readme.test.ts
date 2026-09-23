/**
 * README — every runnable example in `README.md`, executed.
 *
 * The README is the package's front door, and a front door that does not compile is worse than no
 * README at all. Each block below is the README's code verbatim, with two differences and no
 * others: the imports point at `../src` instead of at `orcastork` / `orcastork/lite`, and what the
 * README prints is asserted here on the same values. Change one and change the other.
 *
 * The blocks under `documented, not executed` are the snippets whose point is the *wiring* — a
 * Redis client, a Mongo database, an OTel SDK, a replay of a production archive. They are declared
 * and never called, so `tsc` and biome hold them to the same standard as the rest without the suite
 * needing any of that infrastructure.
 *
 * @module
 */

import type { MeterProvider, TracerProvider } from '@opentelemetry/api';
import { MongoClient } from 'mongodb';
import { createClient } from 'redis';
import { describe, expect, it } from 'vitest';
import { z } from 'zod';
import {
  InMemoryCapabilityCatalog,
  InMemoryDataPointStore,
  InMemoryDurableStore,
  InMemoryRateLimiter,
} from '../src/orcastork/adapters/memory/index.js';
import { MongoAuditSink, MongoDataPointArchive, MongoDurableStore } from '../src/orcastork/adapters/mongo/index.js';
import {
  RedisCooldownGate,
  RedisDataPointStore,
  RedisRateLimiter,
  RedisSessionLock,
  RedisStreamsInbox,
} from '../src/orcastork/adapters/redis/index.js';
import type {
  ArchivedDataPoint,
  CapabilityCatalog,
  Clock,
  DataPointArchive,
  DataPointEmission,
} from '../src/orcastork/index.js';
import {
  AggregationHelpers,
  Aggregator,
  abstractDataPoint,
  aggregator,
  allOf,
  anyOf,
  attachOtelLogBridge,
  BaseDataPoint,
  buildInMemoryRuntime,
  Capability,
  CapabilityActivator,
  type CapabilityContext,
  CapabilityId,
  CapabilityView,
  capability,
  DataPointView,
  dataPointType,
  describeSession,
  EffectGuard,
  Epoch,
  FlowDefinition,
  InvocationDelta,
  NamespaceId,
  Operator,
  OperatorContext,
  OperatorId,
  OperatorPolicy,
  Orchestrator,
  OrchestratorRuntime,
  operator,
  renderText,
  replaySession,
  SchedulingGate,
  SessionId,
  SessionOrchestrationManager,
  SessionStatus,
  SystemClock,
  Telemetry,
} from '../src/orcastork/index.js';
import { InMemorySessionEventSink } from '../src/orcastork_lite/adapters/memory.js';
import type {
  OperatorContext as LiteContext,
  DataPointEmission as LiteEmission,
} from '../src/orcastork_lite/index.js';
import {
  buildRuntime,
  DataPoint,
  NamespaceId as LiteNamespaceId,
  Operator as LiteOperator,
  OperatorId as LiteOperatorId,
  OperatorPolicy as LiteOperatorPolicy,
  Orchestrator as LiteOrchestrator,
  SessionId as LiteSessionId,
} from '../src/orcastork_lite/index.js';
import { FakeClock } from './doubles/clock.js';

// --- Quickstart ----------------------------------------------------------------------------------

// 1. The data. Identity is (type, value), so re-observing a value merges instead of duplicating.
@dataPointType('url', { pii: false, ephemeral: false }, { value: z.string() })
class UrlDataPoint extends BaseDataPoint<string> {}

@dataPointType('title', { pii: false, ephemeral: false }, { value: z.string() })
class TitleDataPoint extends BaseDataPoint<string> {}

// 2. The work. It runs because a URL exists, not because anything called it.
@operator
class TitleFetcher extends Operator {
  static readonly operatorId = OperatorId('title_fetcher');
  static readonly policy = OperatorPolicy({ rerunOnNewData: true });
  static readonly dependsOn = [UrlDataPoint];
  static readonly produces = [TitleDataPoint];

  public async *run(ctx: OperatorContext): AsyncIterable<DataPointEmission> {
    for (const url of ctx.store.ofType(UrlDataPoint)) {
      yield TitleDataPoint.emit(`Title of ${url.value}`);
    }
  }
}

// 3. The output. Aggregators are the only writers of curated durable state.
@aggregator
class TitleReport extends Aggregator {
  static readonly operatorId = OperatorId('title_report');
  static readonly policy = OperatorPolicy({ rerunOnNewData: false });
  static readonly dependsOn = [TitleDataPoint];

  public async aggregate(ctx: OperatorContext): Promise<void> {
    const titles = ctx.store
      .ofType(TitleDataPoint)
      .map((title) => title.value)
      .sort();
    await ctx.aggregation?.upsert('reports', 'titles', { titles });
  }
}

describe('the README quickstart', () => {
  it('seeds a URL, fetches a title and writes the durable report', async () => {
    const runtime = buildInMemoryRuntime();
    const manager = new SessionOrchestrationManager(runtime);
    const flow = new FlowDefinition({ name: 'titles', operators: [TitleFetcher, TitleReport] });

    const now = new Date();
    const result = await manager.startSession({
      sessionId: SessionId('run-1'),
      namespaceId: NamespaceId('default'),
      flow,
      seed: [
        new UrlDataPoint({
          value: 'https://example.com',
          retrievedBy: OperatorId('seed'),
          firstRetrieved: now,
          lastRetrieved: now,
        }),
      ],
    });

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(Object.fromEntries(result.operatorRuns)).toEqual({ title_fetcher: 1, title_report: 1 });
    const record = await runtime.durable.read('reports', 'titles');
    expect(record?.document).toEqual({ titles: ['Title of https://example.com'] });
  });
});

// --- Looking for something smaller: the orcastork/lite quickstart --------------------------------

// 1. The data. Identity is (class, value) — the class is the type; there is no discriminator.
class Url extends DataPoint<string> {}

class Title extends DataPoint<string> {}

// 2. The work. It runs because a Url exists, not because anything called it.
class LiteTitleFetcher extends LiteOperator {
  static readonly operatorId = LiteOperatorId('title_fetcher');
  static readonly policy = LiteOperatorPolicy({ rerunOnNewData: true });
  static readonly dependsOn = [Url];
  static readonly produces = [Title];

  public async *run(ctx: LiteContext): AsyncIterable<LiteEmission> {
    for (const url of ctx.delta.added) {
      if (url instanceof Url) {
        yield Title.emit(`Title of ${url.value}`);
      }
    }
  }
}

describe('the README lite quickstart', () => {
  it('runs the operator once and returns the gathered DataPoints', async () => {
    const now = new Date();
    const seed = [
      new Url({
        value: 'https://example.com',
        retrievedBy: LiteOperatorId('seed'),
        firstRetrieved: now,
        lastRetrieved: now,
      }),
    ];
    const result = await new LiteOrchestrator({
      sessionId: LiteSessionId('run-1'),
      namespaceId: LiteNamespaceId('default'),
      runtime: buildRuntime(),
      operators: [LiteTitleFetcher],
      seed,
    }).run();

    expect(Object.fromEntries(result.operatorRuns)).toEqual({ title_fetcher: 1 });
    expect(result.dataPoints.ofType(Title).map((title) => title.value)).toEqual(['Title of https://example.com']);
  });
});

// --- Reference guide: the risk-report flow --------------------------------------------------------

@dataPointType('email', { pii: true, ephemeral: false }, { value: z.string() })
class EmailDataPoint extends BaseDataPoint<string> {}

@dataPointType('risk_score', { pii: false, ephemeral: false }, { value: z.number() })
class RiskScoreDataPoint extends BaseDataPoint<number> {}

@dataPointType('breach_count', { pii: false, ephemeral: false }, { value: z.number() })
class BreachCountDataPoint extends BaseDataPoint<number> {}

@operator
class RiskScorer extends Operator {
  static readonly operatorId = OperatorId('risk_scorer');
  static readonly policy = OperatorPolicy({ rerunOnNewData: false });
  static readonly dependsOn = [EmailDataPoint];
  static readonly produces = [RiskScoreDataPoint];

  public async *run(ctx: OperatorContext): AsyncIterable<DataPointEmission> {
    for (const email of ctx.store.ofType(EmailDataPoint)) {
      yield RiskScoreDataPoint.emit(email.value.endsWith('@acme.test') ? 0.9 : 0.1);
    }
  }
}

/** The real client the example stands in for; a stub here, so the README needs no network. */
class BreachIntelClient {
  public constructor(private readonly apiKey: string) {}

  public async lookup(email: string): Promise<number> {
    return this.apiKey.length > 0 && email.includes('@') ? 3 : 0;
  }
}

@capability
class BreachIntelCapability extends Capability {
  static readonly capabilityId = CapabilityId('breach_intel'); // unique registry key
  static readonly dependsOn = [EmailDataPoint]; // only available once an Email exists

  private client!: BreachIntelClient;

  public async activate(ctx: CapabilityContext): Promise<void> {
    // Build the real client from catalog-supplied credentials. Called at most once per session,
    // the first moment this capability becomes available.
    this.client = new BreachIntelClient(ctx.credentials.api_key as string);
  }

  /** An action — a public async method, so it is audited, paced and traced at the seam. */
  public async breachCount(email: string): Promise<number> {
    return await this._lookup(email);
  }

  /** Underscored helper — not an audited action. */
  public async _lookup(email: string): Promise<number> {
    return await this.client.lookup(email);
  }
}

@operator
class BreachChecker extends Operator {
  static readonly operatorId = OperatorId('breach_checker');
  static readonly policy = OperatorPolicy({ rerunOnNewData: false });
  static readonly dependsOn = [EmailDataPoint];
  static readonly requires = [BreachIntelCapability];
  static readonly produces = [BreachCountDataPoint];

  public async *run(ctx: OperatorContext): AsyncIterable<DataPointEmission> {
    const intel = ctx.capabilities.require(BreachIntelCapability);
    for (const email of ctx.store.ofType(EmailDataPoint)) {
      yield BreachCountDataPoint.emit(await intel.breachCount(email.value));
    }
  }
}

@aggregator
class RiskReportAggregator extends Aggregator {
  static readonly operatorId = OperatorId('risk_report');
  static readonly policy = OperatorPolicy({ rerunOnNewData: false });
  static readonly dependsOn = [RiskScoreDataPoint];

  public async aggregate(ctx: OperatorContext): Promise<void> {
    const scores = ctx.store.ofType(RiskScoreDataPoint);
    const peak = scores.reduce((highest, score) => Math.max(highest, score.value), 0);
    await ctx.aggregation?.upsert('risk-reports', 'risk-report', { peak_risk: peak, count: scores.length });
  }
}

const NAMESPACE = NamespaceId('acme');

const email = (value = 'alice@acme.test', at = new Date()): EmailDataPoint =>
  new EmailDataPoint({ value, retrievedBy: OperatorId('seed'), firstRetrieved: at, lastRetrieved: at });

describe('the README reference guide', () => {
  it('wires a catalog, a flow and a session', async () => {
    // A catalog says which capabilities/operators each namespace may use and holds the credentials.
    const catalog = new InMemoryCapabilityCatalog({
      permitted: [[NAMESPACE, [CapabilityId('breach_intel')]]],
      credentials: [
        { namespaceId: NAMESPACE, capabilityId: CapabilityId('breach_intel'), credentials: { api_key: 'secret' } },
      ],
    });
    const runtime = buildInMemoryRuntime(new FakeClock(), { catalog });

    const flow = new FlowDefinition({
      name: 'risk-report',
      operators: [RiskScorer, BreachChecker, RiskReportAggregator],
      capabilities: [BreachIntelCapability],
    });

    const manager = new SessionOrchestrationManager(runtime);
    const result = await manager.startSession({
      sessionId: SessionId('order-4711'),
      namespaceId: NAMESPACE,
      flow,
      seed: [email()],
    });

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(result.deadLetters).toEqual([]);
    expect((await runtime.durable.read('risk-reports', 'risk-report'))?.document).toEqual({
      peak_risk: 0.9,
      count: 1,
    });
  });

  it('drives an operator directly with a hand-built context', async () => {
    const observed = email('alice@acme.test', new Date('2026-01-01T00:00:00.000Z'));
    const ctx = new OperatorContext({
      sessionId: SessionId('t'),
      epoch: Epoch(1),
      store: new DataPointView([observed]),
      capabilities: new CapabilityView(), // empty — no caps needed here
      delta: InvocationDelta({
        added: [observed],
        updated: [],
        newlyAvailableCaps: [],
        isFirstInvocation: true,
      }),
      effects: new EffectGuard(new InMemoryDataPointStore(), {
        sessionId: SessionId('t'),
        operatorId: OperatorId('risk_scorer'),
        epoch: Epoch(1),
      }),
      signal: new AbortController().signal,
    });

    const emitted: DataPointEmission[] = [];
    for await (const emission of new RiskScorer().run(ctx)) {
      emitted.push(emission);
    }

    expect(emitted.map((emission) => [emission.leafType, emission.value])).toEqual([[RiskScoreDataPoint, 0.9]]);
  });

  it('runs a whole in-memory session and asserts the store', async () => {
    const clock = new FakeClock();
    const runtime = buildInMemoryRuntime(clock);
    const seed = email('alice@acme.test', clock.now());

    const result = await new Orchestrator({
      sessionId: SessionId('s'),
      namespaceId: NamespaceId('o'),
      runtime,
      operators: [RiskScorer],
      seed: [seed],
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(result.operatorRuns.get(OperatorId('risk_scorer'))).toBe(1);
    const scores = (await runtime.store.snapshot(SessionId('s'))).ofType(RiskScoreDataPoint);
    expect(scores.map((score) => score.value)).toEqual([0.9]);
  });

  it('activates a capability only once its dependency is present', async () => {
    const catalog = new InMemoryCapabilityCatalog({
      permitted: [[NAMESPACE, [CapabilityId('breach_intel')]]],
      credentials: [
        { namespaceId: NAMESPACE, capabilityId: CapabilityId('breach_intel'), credentials: { api_key: 'secret' } },
      ],
    });
    const activator = new CapabilityActivator(
      [[BreachIntelCapability.capabilityId, BreachIntelCapability]],
      catalog,
      NAMESPACE,
      new FakeClock(),
    );

    const noEmail = await activator.refresh(new DataPointView([]));
    expect(noEmail.isAvailable(CapabilityId('breach_intel'))).toBe(false);

    const withEmail = await activator.refresh(new DataPointView([email()]));
    expect(withEmail.isAvailable(CapabilityId('breach_intel'))).toBe(true);
    expect(withEmail.resolve(BreachIntelCapability)).not.toBeNull();
  });

  it('drives an aggregator directly over an in-memory durable store', async () => {
    const durable = new InMemoryDurableStore();
    const clock = new FakeClock();
    const at = clock.now();
    const scores = [0.2, 0.9].map(
      (value) =>
        new RiskScoreDataPoint({
          value,
          retrievedBy: OperatorId('risk_scorer'),
          firstRetrieved: at,
          lastRetrieved: at,
        }),
    );
    const ctx = new OperatorContext({
      sessionId: SessionId('s'),
      epoch: Epoch(1),
      store: new DataPointView(scores),
      capabilities: new CapabilityView(),
      delta: InvocationDelta({ added: scores, updated: [], newlyAvailableCaps: [], isFirstInvocation: true }),
      effects: new EffectGuard(new InMemoryDataPointStore(), {
        sessionId: SessionId('s'),
        operatorId: OperatorId('risk_report'),
        epoch: Epoch(1),
      }),
      signal: new AbortController().signal,
      aggregation: new AggregationHelpers(durable, {
        sessionId: SessionId('s'),
        operatorId: OperatorId('risk_report'),
        epoch: Epoch(1),
        clock,
        isFinal: true,
      }),
    });

    await new RiskReportAggregator().aggregate(ctx);

    expect((await durable.read('risk-reports', 'risk-report'))?.document).toEqual({ peak_risk: 0.9, count: 2 });
  });

  it('describes a session read-only', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const flow = new FlowDefinition({ name: 'risk-report', operators: [RiskScorer, RiskReportAggregator] });
    await new SessionOrchestrationManager(runtime).startSession({
      sessionId: SessionId('verification-123'),
      namespaceId: NAMESPACE,
      flow,
      seed: [email()],
    });

    const description = await describeSession(runtime, {
      sessionId: SessionId('verification-123'),
      namespaceId: NAMESPACE,
      flow,
    });

    expect(renderText(description)).toContain('session verification-123 (flow risk-report): complete');
  });

  it('follows a lite session through its event sink', async () => {
    const events = new InMemorySessionEventSink();
    const now = new Date();
    await new LiteOrchestrator({
      sessionId: LiteSessionId('run-1'),
      namespaceId: LiteNamespaceId('default'),
      runtime: buildRuntime(undefined, { events }),
      operators: [LiteTitleFetcher],
      seed: [
        new Url({
          value: 'https://example.com',
          retrievedBy: LiteOperatorId('seed'),
          firstRetrieved: now,
          lastRetrieved: now,
        }),
      ],
    }).run();

    expect(events.events.map((event) => event.kind)).toContain('session_completed');
  });
});

// --- Reference guide: substitution groups, completion conditions, delivery -----------------------

@abstractDataPoint({ pii: true, ephemeral: false }, { value: z.string() })
abstract class ContactDataPoint extends BaseDataPoint<string> {} // an intermediate, not a union member

@dataPointType('work_email')
class WorkEmailDataPoint extends ContactDataPoint {} // a leaf in the group; config is inherited

@dataPointType('challenge_outcome', { pii: false, ephemeral: false }, { value: z.string() })
class ChallengeOutcomeDataPoint extends BaseDataPoint<string> {}

describe('the README reference guide, continued', () => {
  it('lets a leaf inherit its intermediate declaration and satisfy a base-typed dependency', () => {
    const at = new Date();
    const work = new WorkEmailDataPoint({
      value: 'alice@acme.test',
      retrievedBy: OperatorId('seed'),
      firstRetrieved: at,
      lastRetrieved: at,
    });

    expect(work.config).toEqual({ pii: true, ephemeral: false, auditEveryEmission: true });
    expect(new DataPointView([work]).ofType(ContactDataPoint)).toEqual([work]); // subtype substitution
  });

  it('declares a long-lived flow with a completion condition, parking and a deadline', () => {
    const flow = new FlowDefinition({
      name: 'challenge',
      operators: [RiskScorer],
      completesWhen: ChallengeOutcomeDataPoint, // a bare class is shorthand for TypePresent
      parkAfterMs: 120_000, // idle 2 minutes on the inbox wait → park (release the process)
      sessionDeadlineMs: 3_600_000, // ONE persisted wall-clock budget across parks/crashes/resumes
    });

    // The tuning knobs are runtime behavior, not graph shape, so they stay out of the fingerprint;
    // the completion condition is graph shape, so it goes in.
    const tuned = new FlowDefinition({
      name: 'challenge',
      operators: [RiskScorer],
      completesWhen: ChallengeOutcomeDataPoint,
      retryPolicy: null,
      parkAfterMs: null,
      operationTimeoutMs: null,
      sessionDeadlineMs: null,
      maxInboxDeliveries: null,
      emissionQueueSize: null,
    });
    expect(tuned.fingerprint()).toBe(flow.fingerprint());
    expect(allOf(ChallengeOutcomeDataPoint, anyOf(RiskScoreDataPoint)).isSatisfied(new DataPointView([]))).toBe(false);
  });

  it('delivers mid-session input through the crash-safe front door', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const manager = new SessionOrchestrationManager(runtime);
    const flow = new FlowDefinition({ name: 'challenge', operators: [RiskScorer] });
    const sessionId = SessionId('challenge-1');
    const namespaceId = NAMESPACE;
    const at = new Date();
    const chatAnswer = new ChallengeOutcomeDataPoint({
      value: 'approved',
      retrievedBy: OperatorId('ingress'),
      firstRetrieved: at,
      lastRetrieved: at,
    });

    // No session has been started, so there is nothing to resume: the entry is appended durably and
    // waits for whoever drives the session next.
    const redriven = await manager.deliver({ sessionId, namespaceId, flow, dataPoint: chatAnswer });

    expect(redriven).toBeNull();
    expect(await runtime.inbox.pendingCount(sessionId)).toBe(1);
  });
});

// --- documented, not executed --------------------------------------------------------------------
// The wiring snippets: they need a Redis, a Mongo, an OTel SDK or a production archive, so they are
// held to `tsc` and biome rather than run.

/** README: guarding a non-idempotent side effect with `ctx.once`. */
const sendTheOtp = async (ctx: OperatorContext, otp: { send(to: string): Promise<void> }): Promise<void> => {
  const address = ctx.latest(EmailDataPoint);
  await ctx.once('send-otp', async (acquired) => {
    if (acquired && address !== null) {
      await otp.send(address.value);
    }
  });
};

/** README: replaying an archived session onto a newer flow. */
const replayProduction = async (
  productionArchive: DataPointArchive,
  sessionId: SessionId,
  newFlow: FlowDefinition,
): Promise<readonly ArchivedDataPoint[]> => {
  const archived = await productionArchive.read(sessionId);
  const replay = await replaySession(archived, { flow: newFlow });

  if (replay.result.status !== SessionStatus.COMPLETED) {
    throw new Error('the replayed flow did not complete');
  }
  await replay.runtime.durable.read('risk-reports', 'risk-report');
  return archived;
};

/** README: a production runtime on Redis + Mongo, and the manager's cross-pod scheduling gate. */
const buildProductionRuntime = async (catalog: CapabilityCatalog): Promise<SessionOrchestrationManager> => {
  const redis = await createClient({ url: 'redis://localhost:6379' }).connect();
  const database = new MongoClient('mongodb://localhost:27017').db('orcastork');

  const clock = new SystemClock();
  const runtime = OrchestratorRuntime({
    clock,
    store: new RedisDataPointStore(redis),
    inbox: new RedisStreamsInbox(redis),
    // ttlMs must exceed the longest unrenewed await — see "the completion tail is bounded".
    lock: new RedisSessionLock(redis, { ttlMs: 60_000 }),
    audit: new MongoAuditSink(database),
    archive: new MongoDataPointArchive(database, { retentionMs: 30 * 86_400_000 }),
    durable: new MongoDurableStore(database),
    catalog,
    rateLimiter: new RedisRateLimiter(redis, clock, { ratePerSecond: 10, burst: 20 }),
  });
  return new SessionOrchestrationManager(runtime, {
    schedulingGate: new SchedulingGate(new RedisCooldownGate(redis), { cooldownMs: 60_000 }),
  });
};

/** README: fleet pacing in a single process, on the injected clock. */
const buildPacedRuntime = (clock: Clock): OrchestratorRuntime =>
  buildInMemoryRuntime(clock, { rateLimiter: new InMemoryRateLimiter(clock, { ratePerSecond: 5, burst: 10 }) });

/** README: wiring telemetry and the log bridge. */
const wireTelemetry = (
  tracerProvider: TracerProvider,
  meterProvider: MeterProvider,
  clock: Clock,
): OrchestratorRuntime => {
  const runtime = buildInMemoryRuntime(clock, { telemetry: new Telemetry({ tracerProvider, meterProvider }) });
  attachOtelLogBridge({ level: 'INFO', moduleFilter: 'orcastork' }); // once per process
  return runtime;
};

describe('the README wiring snippets', () => {
  it('compiles, which is the whole assertion — they need infrastructure to run', () => {
    // Declared above so `tsc` and biome hold them to the same standard as an executed example;
    // named here so nothing is an unused local and the file still exports nothing.
    for (const snippet of [sendTheOtp, replayProduction, buildProductionRuntime, buildPacedRuntime, wireTelemetry]) {
      expect(typeof snippet).toBe('function');
    }
  });
});
