/** End-to-end sessions on the in-memory runtime, driven by `FakeClock`. */

import { describe, expect, it } from 'vitest';
import { InMemorySessionEventSink } from '../../src/orcastork_lite/adapters/memory.js';
import { CapabilityId } from '../../src/orcastork_lite/ids.js';
import type {
  AnyDataPoint,
  ConcreteOperatorClass,
  DataPointClass,
  DataPointEmission,
  DataPointView,
  LogFields,
  Logger,
  OperatorContext,
  SessionEvent,
  SessionEventSink,
  SessionResult,
} from '../../src/orcastork_lite/index.js';
import {
  buildRuntime,
  DataPoint,
  DuplicateIdError,
  getLogger,
  InMemoryCapabilityCatalog,
  InvalidOperatorError,
  NamespaceId,
  Operator,
  OperatorId,
  OperatorPolicy,
  Orchestrator,
  RerunOn,
  RetryPolicy,
  SESSION_EVENT_KIND,
  setLogger,
  UnboundedCycleError,
} from '../../src/orcastork_lite/index.js';
import { Deferred } from '../../src/orcastork_lite/internal/deferred.js';
import { FakeClock } from '../doubles/clock.js';
import {
  abortableSleep,
  dp,
  Email,
  Flag,
  Ip,
  makeCapability,
  makeOperator,
  NAMESPACE,
  Risk,
  runSession,
  SESSION,
  WorkEmail,
} from './fixtures.js';

const SECOND = 1_000;
const MINUTE = 60 * SECOND;

/** Code-unit order, so a list of values sorts the same way on every host — Python's `sorted`. */
const ascending = (left: unknown, right: unknown): number => {
  const [first, second] = [String(left), String(right)];
  if (first === second) {
    return 0;
  }
  return first < second ? -1 : 1;
};

/** The values of one DataPoint type, sorted — the counterpart of the Python suite's `_values`. */
const values = (dataPoints: DataPointView, leaf: DataPointClass): readonly unknown[] =>
  [...dataPoints.ofType(leaf)].map((point) => point.value).sort(ascending);

/** A result's maps as plain objects, so a test reads like the Python `== {'a': 1}` it came from. */
const runsOf = (result: SessionResult): Record<string, number> => Object.fromEntries(result.operatorRuns);

const failuresOf = (result: SessionResult): Record<string, string> => Object.fromEntries(result.failures);

/** `now + delta`, the counterpart of Python's `now - timedelta(...)`. */
const shifted = (at: Date, deltaMs: number): Date => new Date(at.getTime() + deltaMs);

/** Hand the event loop back once — Python's `await asyncio.sleep(0)` in a test double. */
const yieldToLoop = (): Promise<void> =>
  new Promise<void>((resolve) => {
    setImmediate(resolve);
  });

describe('Orchestrator', () => {
  it('runs a chain of operators once each, in data order', async () => {
    const clock = new FakeClock();
    const order: string[] = [];
    const emitter = (name: string, emission: DataPointEmission) => (): readonly DataPointEmission[] => {
      order.push(name);
      return [emission];
    };

    const a = makeOperator('a', { dependsOn: [Flag], produces: [Ip], emitFactory: emitter('a', Ip.emit('1.1.1.1')) });
    const b = makeOperator('b', { dependsOn: [Ip], produces: [Risk], emitFactory: emitter('b', Risk.emit(0.9)) });
    const c = makeOperator('c', {
      dependsOn: [Risk],
      produces: [WorkEmail],
      emitFactory: emitter('c', WorkEmail.emit('w')),
    });

    const result = await runSession(clock, [c, b, a], { seed: [dp(Flag, true, clock.now())] });

    expect(order).toEqual(['a', 'b', 'c']);
    expect(runsOf(result)).toEqual({ a: 1, b: 1, c: 1 });
    expect(failuresOf(result)).toEqual({});
    expect(result.dataPoints.presentTypes()).toEqual(new Set([Flag, Ip, Risk, WorkEmail]));
    const emails = result.dataPoints.ofType(Email);
    expect(emails).toHaveLength(1);
    expect(emails[0]?.retrievedBy).toBe('c');
    expect(emails[0]?.firstRetrieved).toEqual(clock.now());
  });

  it('never runs an operator whose dependency never arrives, and still completes', async () => {
    const clock = new FakeClock();
    const orphan = makeOperator('orphan', { dependsOn: [Risk], emits: [Ip.emit('x')] });

    const result = await runSession(clock, [orphan], { seed: [dp(Flag, true, clock.now())] });

    expect(runsOf(result)).toEqual({});
    expect(result.dataPoints.size).toBe(1);
  });

  it('reruns with only the new delta, and a `uses` type triggers the rerun', async () => {
    const clock = new FakeClock();
    const seen: OperatorContext[] = [];
    const producer = makeOperator('producer', { dependsOn: [Flag], produces: [Risk], emits: [Risk.emit(0.5)] });
    const folder = makeOperator('folder', { dependsOn: [Flag], uses: [Risk], rerunOnNewData: true, seen });
    const once = makeOperator('once', { dependsOn: [Flag], uses: [Risk], rerunOnNewData: false });

    const result = await runSession(clock, [producer, folder, once], { seed: [dp(Flag, true, clock.now())] });

    expect(runsOf(result)).toEqual({ producer: 1, folder: 2, once: 1 });
    const [first, second] = seen;
    expect(first?.delta.isFirstInvocation).toBe(true);
    expect(identities(first?.delta.added)).toEqual(identities([dp(Flag, true, clock.now())]));
    expect(second?.delta.isFirstInvocation).toBe(false);
    expect(identities(second?.delta.added)).toEqual(identities([dp(Risk, 0.5, clock.now())]));
    expect(second?.delta.updated.size).toBe(0);
    // The rerun sees the store that triggered it.
    expect(second?.store.ofType(Risk).length ?? 0).toBeGreaterThan(0);
  });

  it('coalesces the arrivals inside a debounce window into one rerun', async () => {
    const clock = new FakeClock();
    const seen: OperatorContext[] = [];
    const firstHop = makeOperator('first_hop', { dependsOn: [Flag], produces: [Ip], emits: [Ip.emit('1.1.1.1')] });
    const secondHop = makeOperator('second_hop', { dependsOn: [Ip], produces: [Risk], emits: [Risk.emit(0.1)] });
    const folder = makeOperator('folder', {
      dependsOn: [Flag],
      uses: [Ip, Risk],
      rerunOnNewData: true,
      debounceMs: 5 * SECOND,
      seen,
    });

    const result = await runSession(clock, [firstHop, secondHop, folder], { seed: [dp(Flag, true, clock.now())] });

    // First run at seed; the Ip arrival arms a 5s window, the Risk arrival lands inside it, one
    // rerun folds both.
    expect(result.operatorRuns.get(OperatorId('folder'))).toBe(2);
    expect(identities(seen[1]?.delta.added)).toEqual(
      identities([dp(Ip, '1.1.1.1', clock.now()), dp(Risk, 0.1, clock.now())]),
    );
    expect(clock.monotonic()).toBe(5 * SECOND); // waited out on the injected clock, not abandoned
  });

  it.each([
    { rerunOn: RerunOn.ADDED_OR_UPDATED, expectedRuns: 2 },
    { rerunOn: RerunOn.ADDED_ONLY, expectedRuns: 1 },
  ])('reruns a freshness-only re-observation only under $rerunOn', async ({ rerunOn, expectedRuns }) => {
    const clock = new FakeClock();
    const earlier = shifted(clock.now(), -MINUTE);
    const reobserver = makeOperator('reobserver', { dependsOn: [Flag], produces: [Ip], emits: [Ip.emit('1.1.1.1')] });
    const watcher = makeOperator('watcher', { dependsOn: [Ip], rerunOnNewData: true, rerunOn });

    const result = await runSession(clock, [reobserver, watcher], {
      seed: [dp(Flag, true, earlier), dp(Ip, '1.1.1.1', earlier)],
    });

    expect(result.operatorRuns.get(OperatorId('watcher'))).toBe(expectedRuns);
    const [ip] = result.dataPoints.ofType(Ip);
    // Merged, not duplicated.
    expect(ip?.firstRetrieved).toEqual(earlier);
    expect(ip?.lastRetrieved).toEqual(clock.now());
  });

  it('isolates a failing operator: its emissions are kept and its peers run', async () => {
    const clock = new FakeClock();
    const broken = makeOperator('broken', {
      dependsOn: [Flag],
      produces: [Ip],
      emits: [Ip.emit('kept')],
      raiseError: new Error('boom'),
    });
    const peer = makeOperator('peer', { dependsOn: [Ip], produces: [Risk], emits: [Risk.emit(1)] });

    const result = await runSession(clock, [broken, peer], { seed: [dp(Flag, true, clock.now())] });

    expect(failuresOf(result)).toEqual({ broken: 'boom' });
    expect(runsOf(result)).toEqual({ broken: 1, peer: 1 });
    expect(values(result.dataPoints, Ip)).toEqual(['kept']);
    expect(values(result.dataPoints, Risk)).toEqual([1]);
  });

  it('isolates a timed-out operator exactly like any other failure', async () => {
    const clock = new FakeClock();
    const slow = makeOperator('slow', { dependsOn: [Flag], sleepAfterMs: 5 * SECOND, timeoutMs: 20 });
    const globalSlow = makeOperator('global_slow', { dependsOn: [Flag], sleepAfterMs: 5 * SECOND });

    const result = await runSession(clock, [slow, globalSlow], {
      seed: [dp(Flag, true, clock.now())],
      operationTimeoutMs: 20,
    });

    expect(new Set(result.failures.keys())).toEqual(new Set([OperatorId('slow'), OperatorId('global_slow')]));
  });

  it('relaunches a failed run on backoff with the same delta, then succeeds', async () => {
    const clock = new FakeClock();
    const seen: OperatorContext[] = [];
    const flaky = makeOperator('flaky', {
      dependsOn: [Flag],
      produces: [Ip],
      emits: [Ip.emit('1.1.1.1')],
      failFirst: 2,
      retry: RetryPolicy({ maxAttempts: 5, baseDelayMs: SECOND, jitter: 0 }),
      seen,
    });

    const result = await runSession(clock, [flaky], { seed: [dp(Flag, true, clock.now())] });

    expect(runsOf(result)).toEqual({ flaky: 3 });
    expect(failuresOf(result)).toEqual({});
    // The watermark never advanced while retrying.
    expect(seen.map((ctx) => ctx.delta.isFirstInvocation)).toEqual([true, true, true]);
    // The two backoff windows, waited out on the injected clock.
    expect(clock.monotonic()).toBe(SECOND + 2 * SECOND);
    expect(values(result.dataPoints, Ip)).toEqual(['1.1.1.1']);
  });

  it('treats an exhausted retry budget as terminal', async () => {
    const clock = new FakeClock();
    const doomed = makeOperator('doomed', {
      dependsOn: [Flag],
      raiseError: new Error('always'),
      retry: RetryPolicy({ maxAttempts: 3, baseDelayMs: SECOND, jitter: 0 }),
    });

    const result = await runSession(clock, [doomed], { seed: [dp(Flag, true, clock.now())] });

    expect(runsOf(result)).toEqual({ doomed: 3 });
    expect(failuresOf(result)).toEqual({ doomed: 'always' });
  });

  it('bounds a self-feeding operator by maxCycles, and rejects an unbounded cycle', async () => {
    const clock = new FakeClock();
    const nextIp = (ctx: OperatorContext): readonly DataPointEmission[] => [
      Ip.emit(`10.0.0.${ctx.store.ofType(Ip).length + 1}`),
    ];
    const looper = makeOperator('looper', {
      dependsOn: [Ip],
      produces: [Ip],
      rerunOnNewData: true,
      maxCycles: 3,
      emitFactory: nextIp,
    });

    const result = await runSession(clock, [looper], { seed: [dp(Ip, '10.0.0.1', clock.now())] });

    expect(runsOf(result)).toEqual({ looper: 3 });
    expect(values(result.dataPoints, Ip)).toEqual(['10.0.0.1', '10.0.0.2', '10.0.0.3', '10.0.0.4']);

    const unbounded = makeOperator('loop', { dependsOn: [Ip], produces: [Ip], rerunOnNewData: true });
    await expect(runSession(clock, [unbounded])).rejects.toThrow(UnboundedCycleError);
  });

  it('makes a retry terminal once the circuit breaker has tripped', async () => {
    const clock = new FakeClock();
    const looper = makeOperator('looper', {
      dependsOn: [Ip],
      produces: [Ip],
      maxCycles: 2,
      raiseError: new Error('x'),
      retry: RetryPolicy({ maxAttempts: 10, baseDelayMs: 0, jitter: 0 }),
    });

    const result = await runSession(clock, [looper], { seed: [dp(Ip, '1', clock.now())] });

    expect(runsOf(result)).toEqual({ looper: 2 });
    expect(result.failures.has(OperatorId('looper'))).toBe(true);
  });

  it('prunes the operators nothing consumes', async () => {
    const clock = new FakeClock();
    const needed = makeOperator('needed', { dependsOn: [Flag], produces: [Ip], emits: [Ip.emit('1')] });
    const upstream = makeOperator('upstream', { dependsOn: [Ip], produces: [Risk], emits: [Risk.emit(0.2)] });
    const dead = makeOperator('dead', { dependsOn: [Flag], produces: [WorkEmail], emits: [WorkEmail.emit('w')] });
    const sink = makeOperator('sink', { dependsOn: [Risk], consumes: [Risk] });

    const result = await runSession(clock, [needed, upstream, dead, sink], { seed: [dp(Flag, true, clock.now())] });

    expect(new Set(result.operatorRuns.keys())).toEqual(
      new Set([OperatorId('needed'), OperatorId('upstream'), OperatorId('sink')]),
    );
    expect(result.dataPoints.ofType(WorkEmail)).toHaveLength(0);
  });

  it.each([
    { name: 'unrestricted', permitted: null, expected: ['x', 'y'] },
    { name: 'gated', permitted: ['x'], expected: ['x'] },
    { name: 'run-nothing', permitted: [], expected: [] },
  ])('gates which operators a namespace may run ($name)', async ({ permitted, expected }) => {
    const clock = new FakeClock();
    const x = makeOperator('x', { dependsOn: [Flag] });
    const y = makeOperator('y', { dependsOn: [Flag] });
    const catalog = new InMemoryCapabilityCatalog();
    if (permitted !== null) {
      catalog.setPermittedOperators(NAMESPACE, permitted.map(OperatorId));
    }

    const result = await runSession(clock, [x, y], { seed: [dp(Flag, true, clock.now())], catalog });
    expect([...result.operatorRuns.keys()].sort(ascending)).toEqual(expected);

    // Gating is per namespace.
    const other = await runSession(clock, [x, y], {
      seed: [dp(Flag, true, clock.now())],
      catalog,
      namespaceId: NamespaceId('other'),
    });
    expect([...other.operatorRuns.keys()].sort(ascending)).toEqual(['x', 'y']);
  });

  it('runs an operator requiring a capability only once it is activated from credentials', async () => {
    const clock = new FakeClock();
    const cap = makeCapability('intel', { dependsOn: [Email] });
    const seen: OperatorContext[] = [];
    const checker = makeOperator('checker', {
      dependsOn: [Email],
      requires: [cap],
      produces: [Risk],
      emits: [Risk.emit(0.7)],
      seen,
    });
    const catalog = new InMemoryCapabilityCatalog({
      permitted: [[NAMESPACE, [CapabilityId('intel')]]],
      credentials: [{ namespaceId: NAMESPACE, capabilityId: CapabilityId('intel'), credentials: { token: 'secret' } }],
    });
    const seed = [dp(WorkEmail, 'a@x', clock.now())];

    const result = await runSession(clock, [checker], { capabilities: [cap], seed, catalog });

    expect(runsOf(result)).toEqual({ checker: 1 });
    expect(seen[0]?.capabilities.availableIds()).toEqual(new Set([CapabilityId('intel')]));
    expect(await seen[0]?.capabilities.require(cap).token()).toBe('secret');

    const notPermitted = await runSession(clock, [checker], { capabilities: [cap], seed });
    expect(runsOf(notPermitted)).toEqual({});
  });

  it('reruns a rerun-eligible operator when a capability becomes available', async () => {
    const clock = new FakeClock();
    const cap = makeCapability('late', { dependsOn: [Ip] });
    const unlock = makeOperator('unlock', { dependsOn: [Flag], produces: [Ip], emits: [Ip.emit('1')] });
    const seen: OperatorContext[] = [];
    const watcher = makeOperator('watcher', { dependsOn: [Flag], rerunOnNewData: true, seen });
    const catalog = new InMemoryCapabilityCatalog({ permitted: [[NAMESPACE, [CapabilityId('late')]]] });

    const result = await runSession(clock, [unlock, watcher], {
      capabilities: [cap],
      seed: [dp(Flag, true, clock.now())],
      catalog,
    });

    expect(result.operatorRuns.get(OperatorId('watcher'))).toBe(2);
    expect(seen[1]?.delta.newlyAvailableCaps).toEqual(new Set([CapabilityId('late')]));
  });

  it('rejects duplicate ids and an operator with no policy', async () => {
    const clock = new FakeClock();
    await expect(runSession(clock, [makeOperator('dup'), makeOperator('dup')])).rejects.toThrow(
      /duplicate operatorId/,
    );
    await expect(runSession(clock, [makeOperator('dup'), makeOperator('dup')])).rejects.toThrow(DuplicateIdError);
    const duplicateCapabilities = { capabilities: [makeCapability('dup'), makeCapability('dup')] };
    await expect(runSession(clock, [], duplicateCapabilities)).rejects.toThrow(/duplicate capabilityId/);
    await expect(runSession(clock, [], duplicateCapabilities)).rejects.toThrow(DuplicateIdError);

    /** Python rejects this at class-definition time; TypeScript's own check is the type it fails. */
    class NoPolicy extends Operator {
      public static readonly operatorId = OperatorId('no_policy');

      public async *run(_ctx: OperatorContext): AsyncIterable<DataPointEmission> {
        yield Ip.emit('x');
      }
    }

    // The cast is how a test reaches the runtime check, which exists for the untyped JavaScript
    // caller the compiler cannot reach.
    await expect(runSession(clock, [NoPolicy as unknown as ConcreteOperatorClass])).rejects.toThrow(
      InvalidOperatorError,
    );

    expect(OperatorPolicy({ rerunOnNewData: false }).retry).toBeNull();
  });

  it('merges an undeclared emission anyway, and logs the mismatch once', async () => {
    const clock = new FakeClock();
    const records: string[] = [];
    const recording: Logger = {
      debug: () => undefined,
      info: () => undefined,
      warning: () => undefined,
      error: (message: string, fields?: LogFields) => records.push(`${message} ${JSON.stringify(fields ?? {})}`),
    };
    const previous = getLogger();
    setLogger(recording);
    let result: SessionResult;
    try {
      const sneaky = makeOperator('sneaky', { dependsOn: [Flag], emits: [Ip.emit('1'), Ip.emit('2')] });
      result = await runSession(clock, [sneaky], { seed: [dp(Flag, true, clock.now())] });
    } finally {
      setLogger(previous);
    }

    expect(values(result.dataPoints, Ip)).toEqual(['1', '2']);
    expect(records.filter((record) => record.includes('produces declaration'))).toHaveLength(1);
  });

  it('fails only its own operator when an emission has no stable identity', async () => {
    const clock = new FakeClock();
    // A `bigint` is this port's counterpart of Python's `bytearray`: a value the identity
    // encoding refuses rather than let it collide with another.
    class Raw extends DataPoint<unknown> {}

    const careless = makeOperator('careless', { dependsOn: [Flag], produces: [Raw], emits: [Raw.emit(1n)] });
    const peer = makeOperator('peer', { dependsOn: [Flag], produces: [Ip], emits: [Ip.emit('1')] });

    const result = await runSession(clock, [careless, peer], { seed: [dp(Flag, true, clock.now())] });

    expect(result.failures.get(OperatorId('careless'))).toContain('bigint');
    expect(runsOf(result)).toEqual({ careless: 1, peer: 1 });
    expect(values(result.dataPoints, Ip)).toEqual(['1']);
  });

  it('bounds a flow that keeps re-triggering itself by the session deadline', async () => {
    const clock = new FakeClock();
    const nextIp = (ctx: OperatorContext): readonly DataPointEmission[] => [
      Ip.emit(`10.0.0.${ctx.store.ofType(Ip).length + 1}`),
    ];
    const looper = makeOperator('looper', {
      dependsOn: [Ip],
      produces: [Ip],
      rerunOnNewData: true,
      maxCycles: 1_000,
      debounceMs: SECOND,
      emitFactory: nextIp,
    });

    const result = await new Orchestrator({
      sessionId: SESSION,
      namespaceId: NAMESPACE,
      runtime: buildRuntime(clock),
      operators: [looper],
      seed: [dp(Ip, '10.0.0.1', clock.now())],
      sessionDeadlineMs: 5 * SECOND,
    }).run();

    expect(result.deadlineHit).toBe(true);
    // Runs at t=0..4; the pass at t=5 is refused by the deadline.
    expect(runsOf(result)).toEqual({ looper: 5 });
    // The last window was clipped to the deadline, never past it.
    expect(clock.monotonic()).toBe(5 * SECOND);
    expect(failuresOf(result)).toEqual({});
  });

  it('cancels the in-flight operators at the deadline but keeps their emissions', async () => {
    const clock = new FakeClock();
    const longRun = (): readonly DataPointEmission[] => {
      clock.advance(10 * SECOND); // this run alone eats the whole budget
      return [Risk.emit(0.5)];
    };
    const hog = makeOperator('hog', { dependsOn: [Flag], produces: [Risk], emitFactory: longRun });
    const slow = makeOperator('slow', {
      dependsOn: [Flag],
      produces: [Ip],
      emits: [Ip.emit('early')],
      sleepAfterMs: 5 * SECOND,
    });
    const events = new InMemorySessionEventSink();

    const result = await new Orchestrator({
      sessionId: SESSION,
      namespaceId: NAMESPACE,
      runtime: buildRuntime(clock, { events }),
      operators: [hog, slow],
      seed: [dp(Flag, true, clock.now())],
      sessionDeadlineMs: 5 * SECOND,
    }).run();

    expect(result.deadlineHit).toBe(true);
    expect(runsOf(result)).toEqual({ hog: 1 }); // slow never finished a run
    expect(result.failures.get(OperatorId('slow'))).toContain('deadline');
    expect(values(result.dataPoints, Ip)).toEqual(['early']);
    expect(values(result.dataPoints, Risk)).toEqual([0.5]);
    expect(cancelledOperators(events)).toEqual(['slow']);
  });

  it('runs an unbounded session to quiescence', async () => {
    const clock = new FakeClock();
    const a = makeOperator('a', { dependsOn: [Flag], produces: [Ip], emits: [Ip.emit('1')] });

    const result = await new Orchestrator({
      sessionId: SESSION,
      namespaceId: NAMESPACE,
      runtime: buildRuntime(clock),
      operators: [a],
      seed: [dp(Flag, true, clock.now())],
      sessionDeadlineMs: null,
    }).run();

    expect(result.deadlineHit).toBe(false);
    expect(runsOf(result)).toEqual({ a: 1 });
  });

  it('publishes every change to the event sink, in order', async () => {
    const clock = new FakeClock();
    const cap = makeCapability('intel', { dependsOn: [Flag] });
    const earlier = shifted(clock.now(), -MINUTE);
    const producer = makeOperator('producer', {
      dependsOn: [Flag],
      requires: [cap],
      produces: [Ip],
      emits: [Ip.emit('1'), Ip.emit('seeded')],
    });
    const broken = makeOperator('broken', { dependsOn: [Flag], raiseError: new Error('boom') });
    const events = new InMemorySessionEventSink();
    const catalog = new InMemoryCapabilityCatalog({ permitted: [[NAMESPACE, [CapabilityId('intel')]]] });

    const result = await new Orchestrator({
      sessionId: SESSION,
      namespaceId: NAMESPACE,
      runtime: buildRuntime(clock, { catalog, events }),
      operators: [producer, broken],
      capabilities: [cap],
      seed: [dp(Flag, true, earlier), dp(Ip, 'seeded', earlier)],
    }).run();

    const kinds = events.events.map((event) => event.kind);
    // The seed, before anything runs.
    expect(kinds.slice(0, 2)).toEqual([SESSION_EVENT_KIND.DATA_POINT_MERGED, SESSION_EVENT_KIND.DATA_POINT_MERGED]);
    expect(kinds[2]).toBe(SESSION_EVENT_KIND.CAPABILITY_ACTIVATED);
    expect(kinds.at(-1)).toBe(SESSION_EVENT_KIND.SESSION_COMPLETED);
    expect(events.events.every((event) => event.sessionId === SESSION && event.namespaceId === NAMESPACE)).toBe(true);

    const merged = events.events.filter((event) => event.kind === SESSION_EVENT_KIND.DATA_POINT_MERGED);
    expect(merged.map((event) => [event.dataPointType, event.value, event.merge])).toEqual([
      ['Flag', true, 'added'],
      ['Ip', 'seeded', 'added'],
      ['Ip', '1', 'added'],
      ['Ip', 'seeded', 'updated'],
    ]);
    // An update keeps the first observer.
    expect(merged.at(-1)?.retrievedBy).toBe('seed');
    expect(merged.at(-1)?.revision).toBe(2);

    const runs = events.events
      .filter((event) => event.kind === SESSION_EVENT_KIND.OPERATOR_RUN_COMPLETED)
      .map((event) => ({ operatorId: event.operatorId, outcome: event.outcome, error: event.error }))
      .sort((left, right) => ascending(left.operatorId, right.operatorId));
    expect(runs).toEqual([
      { operatorId: 'broken', outcome: 'failed', error: 'boom' },
      { operatorId: 'producer', outcome: 'succeeded', error: null },
    ]);

    const activated = events.events.filter((event) => event.kind === SESSION_EVENT_KIND.CAPABILITY_ACTIVATED);
    expect(activated.map((event) => [event.capabilityId, event.outcome])).toEqual([['intel', 'activated']]);

    const completed = events.events.at(-1);
    expect(completed?.kind).toBe(SESSION_EVENT_KIND.SESSION_COMPLETED);
    if (completed?.kind === SESSION_EVENT_KIND.SESSION_COMPLETED) {
      expect(completed.operatorRuns).toEqual(result.operatorRuns);
      expect(completed.failures).toEqual(result.failures);
      expect(completed.deadlineHit).toBe(false);
    }
  });

  it('never lets a failing event sink stop the session', async () => {
    const clock = new FakeClock();

    class Exploding implements SessionEventSink {
      public publish(_event: SessionEvent): Promise<void> {
        return Promise.reject(new Error('redis is down'));
      }
    }

    const a = makeOperator('a', { dependsOn: [Flag], produces: [Ip], emits: [Ip.emit('1')] });
    const result = await new Orchestrator({
      sessionId: SESSION,
      namespaceId: NAMESPACE,
      runtime: buildRuntime(clock, { events: new Exploding() }),
      operators: [a],
      seed: [dp(Flag, true, clock.now())],
    }).run();

    expect(runsOf(result)).toEqual({ a: 1 });
    expect(values(result.dataPoints, Ip)).toEqual(['1']);
  });

  it('makes an armed retry terminal when the operator loses readiness', async () => {
    const clock = new FakeClock();
    const cap = makeCapability('intel');
    const catalog = new InMemoryCapabilityCatalog({ permitted: [[NAMESPACE, [CapabilityId('intel')]]] });
    const revoke = (): readonly DataPointEmission[] => {
      // The namespace drops the capability while the retry window is armed.
      catalog.setPermitted(NAMESPACE, []);
      return [];
    };
    const flaky = makeOperator('flaky', {
      dependsOn: [Flag],
      requires: [cap],
      raiseError: new Error('boom'),
      retry: RetryPolicy({ maxAttempts: 3, baseDelayMs: SECOND, jitter: 0 }),
    });
    const revoker = makeOperator('revoker', { dependsOn: [Flag], emitFactory: revoke });
    const events = new InMemorySessionEventSink();

    const result = await new Orchestrator({
      sessionId: SESSION,
      namespaceId: NAMESPACE,
      runtime: buildRuntime(clock, { catalog, events }),
      operators: [flaky, revoker],
      capabilities: [cap],
      seed: [dp(Flag, true, clock.now())],
    }).run();

    // The relaunch can never run, so the failure it was retrying is terminal — not silently
    // forgotten.
    expect(runsOf(result)).toEqual({ flaky: 1, revoker: 1 });
    expect(failuresOf(result)).toEqual({ flaky: 'boom' });
    const outcomes = events.events
      .filter((event) => event.kind === SESSION_EVENT_KIND.OPERATOR_RUN_COMPLETED)
      .filter((event) => event.operatorId === OperatorId('flaky'))
      .map((event) => event.outcome);
    expect(outcomes).toEqual(['retrying', 'failed']);
    expect(clock.monotonic()).toBe(0); // nothing waited out a window that could never fire
  });

  it('bounds a hanging event sink by the publish timeout', async () => {
    const clock = new FakeClock();

    /** A connection that never answers. */
    class Stalled implements SessionEventSink {
      public attempts = 0;

      public publish(_event: SessionEvent): Promise<void> {
        this.attempts += 1;
        return new Promise<void>((resolve) => {
          setTimeout(resolve, 5 * SECOND).unref();
        });
      }
    }

    const sink = new Stalled();
    const a = makeOperator('a', { dependsOn: [Flag], produces: [Ip], emits: [Ip.emit('1')] });
    const result = await new Orchestrator({
      sessionId: SESSION,
      namespaceId: NAMESPACE,
      runtime: buildRuntime(clock, { events: sink }),
      operators: [a],
      seed: [dp(Flag, true, clock.now())],
      publishTimeoutMs: 10,
    }).run();

    expect(runsOf(result)).toEqual({ a: 1 });
    expect(values(result.dataPoints, Ip)).toEqual(['1']);
    // Seed merge, emission merge, run completed, session completed — each bounded.
    expect(sink.attempts).toBe(4);
  });

  it('keeps the session deadline hard while operators are running', async () => {
    // Nothing emits after the seed and nothing advances the fake clock, so the deadline check
    // between passes alone could never fire: only the clipped run timeout can end this session.
    const clock = new FakeClock();
    const sleeper = makeOperator('sleeper', { dependsOn: [Flag], sleepAfterMs: 5 * SECOND });
    const quick = makeOperator('quick', { dependsOn: [Flag], produces: [Ip], emits: [Ip.emit('1')] });
    const events = new InMemorySessionEventSink();

    const result = await new Orchestrator({
      sessionId: SESSION,
      namespaceId: NAMESPACE,
      runtime: buildRuntime(clock, { events }),
      operators: [sleeper, quick],
      seed: [dp(Flag, true, clock.now())],
      sessionDeadlineMs: 50,
    }).run();

    expect(result.deadlineHit).toBe(true);
    expect(runsOf(result)).toEqual({ quick: 1 }); // the sleeper never finished a run of its own
    expect(result.failures.get(OperatorId('sleeper'))).toContain('deadline');
    expect(values(result.dataPoints, Ip)).toEqual(['1']);
    const cancelled = events.events
      .filter((event) => event.kind === SESSION_EVENT_KIND.OPERATOR_RUN_COMPLETED)
      .filter((event) => event.outcome === 'cancelled')
      .map((event) => [event.operatorId, event.error]);
    expect(cancelled).toEqual([['sleeper', result.failures.get(OperatorId('sleeper'))]]);
  });

  it('fires an armed window while a silent operator is still running', async () => {
    // The blocker never emits and outlives the session, so the only way the rerunner's window can
    // come due is for the loop to honour it while the blocker is in flight. The 0.3s deadline
    // (which clips the blocker's run) is what ends the session; without that, the window is
    // starved until then.
    const clock = new FakeClock();
    const blocker = makeOperator('blocker', { dependsOn: [Flag], sleepAfterMs: 5 * SECOND });
    const trigger = makeOperator('trigger', { dependsOn: [Flag], produces: [Ip], emits: [Ip.emit('second')] });
    const seen: OperatorContext[] = [];
    const rerunner = makeOperator('rerunner', {
      dependsOn: [Flag],
      uses: [Ip],
      rerunOnNewData: true,
      debounceMs: 200,
      seen,
    });

    const result = await new Orchestrator({
      sessionId: SESSION,
      namespaceId: NAMESPACE,
      runtime: buildRuntime(clock),
      operators: [blocker, trigger, rerunner],
      seed: [dp(Flag, true, clock.now())],
      sessionDeadlineMs: 300,
    }).run();

    expect(result.operatorRuns.get(OperatorId('rerunner'))).toBe(2); // the debounced rerun happened
    expect(identities(seen[1]?.delta.added)).toEqual(identities([dp(Ip, 'second', clock.now())]));
    // The window was fast-forwarded on the injected clock, not on the blocker's exit.
    expect(clock.monotonic()).toBe(200);
    expect(result.deadlineHit).toBe(true);
    expect(result.failures.get(OperatorId('blocker'))).toContain('deadline');
  });

  it('records an operator that finishes during the last pass instead of cancelling it', async () => {
    const clock = new FakeClock();

    class Ready extends DataPoint<boolean> {}

    const peerMayFinish = new Deferred<void>();

    class SlowA extends Operator {
      public static readonly operatorId = OperatorId('slow_a');
      public static readonly policy = OperatorPolicy({ rerunOnNewData: false });
      public static readonly dependsOn: readonly DataPointClass[] = [Flag];
      public static readonly produces: readonly DataPointClass[] = [Ip];

      public async *run(ctx: OperatorContext): AsyncIterable<DataPointEmission> {
        yield Ip.emit('A');
        await abortableSleep(5 * SECOND, ctx.signal); // still in flight when the deadline passes
      }
    }

    class QuickB extends Operator {
      public static readonly operatorId = OperatorId('quick_b');
      public static readonly policy = OperatorPolicy({ rerunOnNewData: false });
      public static readonly dependsOn: readonly DataPointClass[] = [Flag];
      public static readonly produces: readonly DataPointClass[] = [Ready];

      public async *run(_ctx: OperatorContext): AsyncIterable<DataPointEmission> {
        await peerMayFinish.promise;
        yield Ready.emit(true);
      }
    }

    class GatingSink extends InMemorySessionEventSink {
      public override async publish(event: SessionEvent): Promise<void> {
        await super.publish(event);
        if (event.kind === SESSION_EVENT_KIND.DATA_POINT_MERGED && event.dataPointType === 'Ip') {
          // While the loop is publishing A's merge, B finishes; then the deadline passes.
          peerMayFinish.resolve(undefined);
          for (let turn = 0; turn < 5; turn += 1) {
            await yieldToLoop();
          }
          clock.advance(SECOND);
        }
      }
    }

    const events = new GatingSink();
    const result = await new Orchestrator({
      sessionId: SESSION,
      namespaceId: NAMESPACE,
      runtime: buildRuntime(clock, { events }),
      operators: [SlowA, QuickB],
      seed: [dp(Flag, true, clock.now())],
      sessionDeadlineMs: 200,
    }).run();

    expect(result.deadlineHit).toBe(true);
    expect(result.dataPoints.presentTypes()).toEqual(new Set([Flag, Ip, Ready]));
    // B finished; it must be counted, not swept up as in flight.
    expect(runsOf(result)).toEqual({ quick_b: 1 });
    expect(new Set(result.failures.keys())).toEqual(new Set([OperatorId('slow_a')]));
    const outcomes = events.events
      .filter((event) => event.kind === SESSION_EVENT_KIND.OPERATOR_RUN_COMPLETED)
      .map((event) => `${event.operatorId}:${event.outcome}`);
    expect(outcomes).toContain('quick_b:succeeded');
    expect(outcomes).not.toContain('quick_b:cancelled');
  });
});

/**
 * DataPoints compared the way Python compares them: by identity, timestamps excluded.
 *
 * A Python `frozenset` of DataPoints compares through `__eq__`, which is the `(class, value)`
 * identity — so a freshened sighting still equals the one the test built. The order is normalized
 * too, because a `frozenset` has none.
 */
const identities = (dataPoints: Iterable<AnyDataPoint> | undefined): readonly string[] =>
  [...(dataPoints ?? [])].map((dataPoint) => dataPoint.identity).sort(ascending);

/** The operators an event sink saw cancelled at the deadline, in publication order. */
const cancelledOperators = (events: InMemorySessionEventSink): readonly string[] =>
  events.events
    .filter((event) => event.kind === SESSION_EVENT_KIND.OPERATOR_RUN_COMPLETED)
    .filter((event) => event.outcome === 'cancelled')
    .map((event) => String(event.operatorId));
