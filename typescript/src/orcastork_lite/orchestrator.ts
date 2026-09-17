/**
 * The session-scoped `Orchestrator` — the gathering loop, and nothing else.
 *
 * It owns one session: seed, then run every ready operator concurrently, merging each emission the
 * instant it is produced and re-evaluating readiness, so a downstream operator starts as soon as
 * its input lands. Operators are never cancelled by new data; reruns are coalesced on a debounce
 * window and failed runs are relaunched on a backoff window, both scheduled by the loop on the
 * injected clock (never an in-task sleep). When nothing is running and no window is armed, the
 * session is quiescent: the gathered DataPoints are returned.
 *
 * The whole session is bounded by `sessionDeadlineMs` on the same clock: the deadline is checked
 * between passes, caps every window the loop waits out, and caps every run launched — a run's
 * timeout is clipped to the budget left, so no operator outlives the deadline — and when it passes
 * the in-flight operators are cancelled (their already-streamed emissions are kept), so a flow that
 * keeps re-triggering itself cannot hold a process forever.
 *
 * The loop is the **sole writer** of the session state: operators only stream emissions onto a
 * queue. It publishes every change — a merge, a run outcome, an activation, the completion — to the
 * runtime's `SessionEventSink` so a consumer can act on a DataPoint the moment it lands. An
 * operator that raises or times out is isolated — its already-emitted DataPoints stay, the failure
 * is logged and reported in the result, and the scheduler proceeds.
 *
 * @module
 */

import { z } from 'zod';
import type { CapabilityView, ConcreteCapabilityClass } from './capabilities.js';
import { CapabilityActivator } from './capabilities.js';
import type { AnyDataPoint, DataPointClass, DataPointEmission } from './datapoints.js';
import { DataPointView } from './datapoints.js';
import type { ActivationOutcome, RunOutcome, SessionEvent, SessionEventBase } from './events.js';
import { CapabilityActivated, DataPointMerged, OperatorRunCompleted, SessionCompleted } from './events.js';
import { DuplicateIdError, OrcastorkLiteError } from './exceptions.js';
import { backwardReachable, buildEdges, cycleCaps, validateAcyclicOrBounded } from './graph.js';
import type { CapabilityId, NamespaceId, OperatorId, SessionId } from './ids.js';
import { BoundedQueue } from './internal/bounded_queue.js';
import { OperationTimeoutError, withTimeout } from './internal/timeouts.js';
import { getLogger } from './logging.js';
import type { ConcreteOperatorClass, InvocationDelta, OperatorClass } from './operators.js';
import { OperatorContext, RerunOn, validateOperatorDeclaration } from './operators.js';
import type { Runtime } from './runtime.js';
import { backoffDelays, CircuitBreaker, DebounceController, isReady, seedFor } from './scheduling.js';
import type { MergeOutcome } from './state.js';
import { SessionState } from './state.js';

/** How long one operator run — or one capability activation — may take by default. */
export const DEFAULT_OPERATION_TIMEOUT_MS = 30_000;

/** How long a whole session may gather by default; `null` on the orchestrator means unbounded. */
export const DEFAULT_SESSION_DEADLINE_MS = 300_000;

/**
 * How long one event publish may take.
 *
 * Short on purpose: an event is published per merge and per run outcome, on the gathering loop, so
 * a stalled sink costs this much per event. Dropping the view beats holding the session that far.
 */
export const DEFAULT_PUBLISH_TIMEOUT_MS = 5_000;

const CANCELLED_AT_DEADLINE = 'cancelled: the session deadline passed while the operator was running';

/** What a session gathered — the value `run()` resolves to. */
export interface SessionResult {
  /** Every finished attempt, per operator, retries included. */
  readonly operatorRuns: ReadonlyMap<OperatorId, number>;

  /** The session's final DataPoint set. */
  readonly dataPoints: DataPointView;

  /** Last error of each operator that failed with no retry left, or was cancelled. */
  readonly failures: ReadonlyMap<OperatorId, string>;

  /** The session deadline ended gathering before it went quiescent. */
  readonly deadlineHit: boolean;
}

/** What a {@link SessionResult} is built from; the two maps are copied. */
export type SessionResultInit = SessionResult;

const sessionResultSchema = z.object({
  operatorRuns: z.map(z.string(), z.number().int()),
  dataPoints: z.instanceof(DataPointView),
  failures: z.map(z.string(), z.string()),
  deadlineHit: z.boolean(),
});

/**
 * Build a {@link SessionResult}.
 *
 * Validated and frozen because it is the one thing the engine hands back across its public
 * surface: a caller reads it long after the session that produced it has gone.
 */
export const SessionResult = (init: SessionResultInit): SessionResult => {
  sessionResultSchema.parse(init);
  return Object.freeze({
    operatorRuns: new Map(init.operatorRuns),
    dataPoints: init.dataPoints,
    failures: new Map(init.failures),
    deadlineHit: init.deadlineHit,
  });
};

// The loop's own signals and records are plain objects, not validated models: the loop is their
// only constructor, so validation would only check the engine against itself, on every emission.

/** One DataPoint an operator streamed, already stamped with its provenance. */
interface Emitted {
  readonly kind: 'emitted';
  readonly operatorId: OperatorId;
  readonly dataPoint: AnyDataPoint;
}

/** One run that ended, however it ended. */
interface Completed {
  readonly kind: 'completed';
  readonly operatorId: OperatorId;

  /** What the run threw, or `null` when it finished cleanly. */
  readonly error: unknown;

  /**
   * The revision the launch snapshot saw — this run's watermark.
   *
   * Carried on the signal rather than looked up when it lands: the watermark must be what *this*
   * run observed, and the live state has moved on by the time the loop records the completion.
   */
  readonly observedRevision: number;

  /**
   * The run's timeout was clipped to the session budget and it hit that clipped bound: the
   * deadline passed while it ran, which is a cancellation at the deadline, not an operator timeout.
   */
  readonly cutByDeadline: boolean;
}

/** Everything an operator task can tell the loop. */
type Signal = Emitted | Completed;

/**
 * One launched run's cancellation handle — what Python gets from `asyncio.Task.cancel()`.
 *
 * A promise cannot be cancelled, so stopping a run is two things the loop must do itself: abort
 * the context's signal (anything the operator tied to it gives up) and return the async generator
 * (it stops at its next yield point). Already-streamed emissions are on the queue and are kept.
 */
class RunHandle {
  private readonly controller = new AbortController();
  private iterator: AsyncIterator<DataPointEmission> | null = null;
  private isCancelled = false;

  /** Handed to the operator on its context; aborted when the run is cut short. */
  public get signal(): AbortSignal {
    return this.controller.signal;
  }

  /** Whether the deadline cancelled this run, which is the loop's to report, not the run's. */
  public get cancelled(): boolean {
    return this.isCancelled;
  }

  /** Remember the generator being drained, so stopping the run can end it. */
  public track(iterator: AsyncIterator<DataPointEmission>): void {
    this.iterator = iterator;
  }

  /** Tell the run to stop working; it still reports how it ended. */
  public stop(): void {
    this.controller.abort();
    const iterator = this.iterator;
    this.iterator = null;
    // A generator that has already finished has nothing to stop, and one that throws on the way
    // out is a fault of the operator, not of the loop that is shutting it down.
    void iterator?.return?.()?.catch(() => undefined);
  }

  /** The deadline taking the run away: it stops, and reports nothing — the loop reports it. */
  public cancel(): void {
    this.isCancelled = true;
    this.stop();
  }
}

/** A run the loop is waiting on. */
interface Running {
  /** Settles when the run has finished reporting; it never rejects. */
  readonly promise: Promise<void>;
  readonly handle: RunHandle;
}

/** What a relaunch owes the failure it is retrying. */
interface ArmedRetry {
  /** The error to report if the relaunch can never run. */
  readonly error: unknown;

  /** The watermark the failed run observed, which the retry deliberately left un-advanced. */
  readonly observedRevision: number;

  readonly attempts: number;
}

/**
 * What a launched run is given of the pass that launched it.
 *
 * Every field is a snapshot on purpose: the run must see the store, the capabilities, the revision
 * and the budget as they were when it was launched, not as they are when it finishes.
 */
interface LaunchSnapshot {
  readonly view: DataPointView;
  readonly capabilities: CapabilityView;
  readonly observedRevision: number;

  /** Milliseconds of session budget left at launch; `null` when the session is unbounded. */
  readonly remainingMs: number | null;
}

/** One operator the loop is about to launch. */
interface Runnable {
  readonly operator: ConcreteOperatorClass;
  readonly delta: InvocationDelta;
  readonly isRetry: boolean;
}

/** What one planning pass decided. */
interface Plan {
  readonly runnable: readonly Runnable[];

  /** Milliseconds until the soonest armed-but-not-due window, else `null`. */
  readonly nextDueInMs: number | null;
}

/**
 * The gathering loop's own scratch state — Python's locals in `_gather`, bundled.
 *
 * It lives exactly as long as one `run()`, which is why it is not on the orchestrator: a second
 * `run()` must start from an empty queue and no armed windows, not from the last one's remains.
 */
interface GatherState {
  readonly queue: BoundedQueue<Signal>;
  readonly debounce: DebounceController;

  /** Retries reuse the same due-time mechanics as debounced reruns: the loop owns the backoff. */
  readonly retries: DebounceController;
  readonly running: Map<OperatorId, Running>;

  /** The monotonic instant gathering must stop at; `null` → unbounded. */
  readonly deadlineAt: number | null;
}

/** What the orchestrator is built from. */
export interface OrchestratorOptions {
  readonly sessionId: SessionId;
  readonly namespaceId: NamespaceId;

  /** The injected bundle: the clock, the capability catalog, the event sink. */
  readonly runtime: Runtime;

  /** The operator classes to schedule; a fresh instance is constructed per run. */
  readonly operators: Iterable<ConcreteOperatorClass>;

  /** The capability classes available to this session, subject to namespace permission. */
  readonly capabilities?: Iterable<ConcreteCapabilityClass>;

  /** DataPoints the session starts from, merged before the first pass. */
  readonly seed?: Iterable<AnyDataPoint>;

  /** Bound on one operator run and one capability activation, unless a policy overrides it. */
  readonly operationTimeoutMs?: number;

  /** Bound on the whole session, on the injected clock; `null` is unbounded. */
  readonly sessionDeadlineMs?: number | null;

  /** Bound on one event publish, so a stalled sink cannot hold the loop. */
  readonly publishTimeoutMs?: number;
}

/** Runs one session: schedules the operators by data readiness until nothing can run any more. */
export class Orchestrator {
  private readonly sessionId: SessionId;
  private readonly namespaceId: NamespaceId;
  private readonly runtime: Runtime;
  private operators: readonly ConcreteOperatorClass[];
  private readonly capabilities: readonly ConcreteCapabilityClass[];
  private readonly seed: readonly AnyDataPoint[];
  private readonly operationTimeoutMs: number;
  private readonly sessionDeadlineMs: number | null;
  private readonly publishTimeoutMs: number;
  private readonly operatorsById: ReadonlyMap<OperatorId, ConcreteOperatorClass>;
  private readonly breaker: CircuitBreaker;
  private readonly state = new SessionState();
  private readonly runs = new Map<OperatorId, number>();
  private readonly failures = new Map<OperatorId, string>();

  /** Consecutive failures of the current retry sequence, per operator. */
  private readonly failedAttempts = new Map<OperatorId, number>();

  /** The failure each armed retry window stands for. */
  private readonly armedRetries = new Map<OperatorId, ArmedRetry>();
  private readonly prevCaps = new Map<OperatorId, ReadonlySet<CapabilityId>>();

  /** Which (operator, DataPoint type) mismatches have already been reported. */
  private readonly undeclaredEmissions = new Map<OperatorId, Set<DataPointClass>>();
  private readonly publishedCaps = new Set<CapabilityId>();
  private deadlineHit = false;

  public constructor(options: OrchestratorOptions) {
    this.sessionId = options.sessionId;
    this.namespaceId = options.namespaceId;
    this.runtime = options.runtime;
    this.operators = [...options.operators];
    this.capabilities = [...(options.capabilities ?? [])];
    this.seed = [...(options.seed ?? [])];
    this.operationTimeoutMs = options.operationTimeoutMs ?? DEFAULT_OPERATION_TIMEOUT_MS;
    // `null` is a meaningful value here (unbounded), so only an absent option takes the default.
    this.sessionDeadlineMs =
      options.sessionDeadlineMs === undefined ? DEFAULT_SESSION_DEADLINE_MS : options.sessionDeadlineMs;
    this.publishTimeoutMs = options.publishTimeoutMs ?? DEFAULT_PUBLISH_TIMEOUT_MS;
    // Python rejects a policy-less operator in `__init_subclass__`, at class-definition time;
    // without a registration hook, the classes entering the framework is the equivalent moment.
    for (const operator of this.operators) {
      validateOperatorDeclaration(operator);
    }
    this.checkUniqueIds();
    this.pruneToConsumedClosure();
    this.operatorsById = new Map(this.operators.map((operator) => [operator.operatorId, operator]));
    // Fail fast on an unbounded cycle rather than looping to the deadline.
    const edges = buildEdges(this.operators, this.capabilities);
    validateAcyclicOrBounded(edges);
    this.breaker = new CircuitBreaker(cycleCaps(edges));
  }

  /** Gather until the session is quiescent (or the deadline passes), then report what it holds. */
  public async run(): Promise<SessionResult> {
    await this.excludeUnpermittedOperators();
    const activator = new CapabilityActivator(
      this.capabilities.map((capability) => [capability.capabilityId, capability] as const),
      this.runtime.catalog,
      this.namespaceId,
      { activationTimeoutMs: this.operationTimeoutMs },
    );
    await this.merge(this.seed);
    await this.gather(activator);
    getLogger().info('Session completed', {
      sessionId: this.sessionId,
      operatorRuns: [...this.runs.values()].reduce((total, count) => total + count, 0),
      failures: [...this.failures.keys()].sort(),
      deadlineHit: this.deadlineHit,
    });
    await this.publish(
      SessionCompleted({
        ...this.eventBase(),
        deadlineHit: this.deadlineHit,
        operatorRuns: new Map(this.runs),
        failures: new Map(this.failures),
      }),
    );
    return SessionResult({
      operatorRuns: new Map(this.runs),
      dataPoints: this.state.view(),
      failures: new Map(this.failures),
      deadlineHit: this.deadlineHit,
    });
  }

  private checkUniqueIds(): void {
    const kinds = [
      { kind: 'operatorId', ids: this.operators.map((operator) => String(operator.operatorId)) },
      { kind: 'capabilityId', ids: this.capabilities.map((capability) => String(capability.capabilityId)) },
    ];
    for (const { kind, ids } of kinds) {
      const duplicates = [...new Set(ids.filter((id) => ids.indexOf(id) !== ids.lastIndexOf(id)))].sort();
      if (duplicates.length > 0) {
        throw new DuplicateIdError(`duplicate ${kind}: ${duplicates.join(', ')}`);
      }
    }
  }

  /**
   * Run only the operators whose output is (transitively) needed by a `consumes` declaration.
   *
   * Sinks are every declared `consumes` type plus the declaring operators' own gate inputs; an
   * operator outside the backward-reachable closure of those sinks produces data nothing considers,
   * so it never runs. No `consumes` anywhere leaves every operator in place.
   */
  private pruneToConsumedClosure(): void {
    const sinkOperators = this.operators.filter((operator) => (operator.consumes ?? []).length > 0);
    if (sinkOperators.length === 0) {
      return;
    }
    const sinks = new Set<DataPointClass>();
    for (const operator of sinkOperators) {
      for (const sink of [...(operator.consumes ?? []), ...(operator.dependsOn ?? [])]) {
        sinks.add(sink);
      }
    }
    const kept = backwardReachable(this.operators, this.capabilities, sinks);
    const isSink = new Set<ConcreteOperatorClass>(sinkOperators);
    const pruned = this.operators.filter((operator) => !kept.has(operator) && !isSink.has(operator));
    if (pruned.length === 0) {
      return;
    }
    const isPruned = new Set<ConcreteOperatorClass>(pruned);
    this.operators = this.operators.filter((operator) => !isPruned.has(operator));
    getLogger().info('Pruned operators whose output nothing consumes', {
      sessionId: this.sessionId,
      pruned: pruned.map((operator) => String(operator.operatorId)).sort(),
    });
  }

  /** Per-namespace operator gating: the catalog decides, `null` means unrestricted. */
  private async excludeUnpermittedOperators(): Promise<void> {
    const permitted = await this.runtime.catalog.permittedOperators(this.namespaceId);
    if (permitted === null) {
      return;
    }
    const excluded = this.operators
      .filter((operator) => !permitted.has(operator.operatorId))
      .map((operator) => String(operator.operatorId))
      .sort();
    if (excluded.length === 0) {
      return;
    }
    this.operators = this.operators.filter((operator) => permitted.has(operator.operatorId));
    getLogger().info('Operators excluded: not permitted for the namespace', {
      sessionId: this.sessionId,
      namespaceId: this.namespaceId,
      excludedOperatorIds: excluded,
    });
  }

  private async gather(activator: CapabilityActivator): Promise<void> {
    const clock = this.runtime.clock;
    const state: GatherState = {
      // Unbounded, as `asyncio.Queue()` is: an emission is merged by the very next pass, so the
      // queue is a hand-off and not a buffer, and backpressure here would stall the producer for
      // as long as the loop spends publishing.
      queue: new BoundedQueue<Signal>(),
      debounce: new DebounceController(clock),
      retries: new DebounceController(clock),
      running: new Map<OperatorId, Running>(),
      deadlineAt: this.sessionDeadlineMs === null ? null : clock.monotonic() + this.sessionDeadlineMs,
    };
    try {
      while (!this.deadlineHit && (state.deadlineAt === null || clock.monotonic() < state.deadlineAt)) {
        const remainingMs = state.deadlineAt === null ? null : state.deadlineAt - clock.monotonic();
        const view = this.state.view();
        const observedRevision = this.state.revision;
        const capabilities = await this.refreshCapabilities(activator, view);
        const plan = await this.plan(view, capabilities, state);
        for (const runnable of plan.runnable) {
          this.launch(runnable, { view, capabilities, observedRevision, remainingMs }, state);
        }
        // Milliseconds until the soonest armed window, never past the deadline; `null` when
        // nothing is armed.
        let waitMs: number | null = null;
        if (plan.nextDueInMs !== null) {
          const untilDeadline = state.deadlineAt === null ? plan.nextDueInMs : state.deadlineAt - clock.monotonic();
          waitMs = Math.max(0, Math.min(plan.nextDueInMs, untilDeadline));
        }
        if (state.running.size > 0) {
          // Wait for the first signal — or for the soonest window to come due, whichever is first:
          // a due rerun or retry must not be starved by an in-flight operator that stays quiet.
          // Then drain everything already queued behind the signal and re-plan for the whole batch.
          const first = await this.nextSignal(state.queue, waitMs);
          if (first === null) {
            continue; // a window came due: re-plan so it launches
          }
          await this.consume([first, ...drainSignals(state.queue)], state);
          continue;
        }
        if (waitMs !== null) {
          await clock.sleep(waitMs); // nothing running: fast-forward to the soonest armed window
          continue;
        }
        return; // nothing running, nothing armed → quiescent
      }
      // Falling out of the loop condition is exactly a deadline hit (already flagged when a clipped
      // run reported it; otherwise the clock crossed the deadline between passes).
      this.deadlineHit = true;
      getLogger().warning('Session deadline hit; gathering stopped and in-flight operators are cancelled', {
        sessionId: this.sessionId,
        sessionDeadlineMs: this.sessionDeadlineMs,
        inFlightOperatorIds: [...state.running.keys()].sort(),
      });
    } finally {
      await this.drainRemaining(state);
      state.queue.close();
    }
  }

  /** Start one run and record it as in flight; the loop never waits for it here. */
  private launch(runnable: Runnable, snapshot: LaunchSnapshot, state: GatherState): void {
    const operatorId = runnable.operator.operatorId;
    getLogger().debug('Launching operator', {
      sessionId: this.sessionId,
      operatorId,
      isRetry: runnable.isRetry,
    });
    // Recorded at launch: it must reflect what this run saw, not the live state at completion.
    this.prevCaps.set(operatorId, snapshot.capabilities.availableIds());
    const handle = new RunHandle();
    const promise = this.runOne(runnable.operator, runnable.delta, snapshot, handle, state.queue).catch(
      (error: unknown) => {
        // `runOne` is itself the fault boundary, so reaching here is an engine bug; it is logged
        // rather than left as an unhandled rejection that would take the whole process down.
        getLogger().error('Operator run failed outside its fault boundary', {
          sessionId: this.sessionId,
          operatorId,
          error: failureText(error),
        });
      },
    );
    state.running.set(operatorId, { promise, handle });
    if (runnable.isRetry) {
      state.retries.clear(operatorId);
      this.armedRetries.delete(operatorId);
    } else {
      state.debounce.clear(operatorId);
    }
  }

  /**
   * The next queued signal, or `null` when an armed window comes due first.
   *
   * The wait runs on the injected clock, so under a fake clock this is the same fast-forward the
   * idle branch performs — it just no longer requires nothing to be running. Abandoning the wait is
   * safe: {@link BoundedQueue.whenNotEmpty} takes nothing off the queue, so a signal that lands
   * after the window won stays queued for the next pass.
   *
   * It begins by yielding the event loop once. Python's operator tasks run their whole
   * non-suspending burst before the loop task is resumed, so a fast operator's emissions *and* its
   * completion arrive as one batch; JavaScript hands the loop its continuation on the first `put`
   * instead, which would split that burst across passes — merging one emission at a time, and
   * letting a window fire against a view a producer was one microtask away from completing.
   */
  private async nextSignal(queue: BoundedQueue<Signal>, waitMs: number | null): Promise<Signal | null> {
    await yieldToEventLoop();
    const queued = queue.getNowait();
    if (queued !== undefined) {
      return queued;
    }
    if (waitMs === null) {
      return (await queue.get()) ?? null;
    }
    // The loser of the race is abandoned, not left running: Python cancels the task it raced, and
    // without the abort a signal that beat the window would leave a real timer pending for the
    // window's full width on every pass.
    const window = new AbortController();
    try {
      await Promise.race([queue.whenNotEmpty(), this.runtime.clock.sleep(waitMs, window.signal)]);
    } finally {
      window.abort();
    }
    return queue.getNowait() ?? null;
  }

  private async drainRemaining(state: GatherState): Promise<void> {
    // A quiescent exit reaches here with both empty (a no-op). A deadline hit — or an unexpected
    // error — reaches here with operators in flight. Anything already queued is applied first,
    // completions included: an operator that finished during the last pass's awaits is a finished
    // run, not an in-flight one, and must be recorded as such rather than swept up as cancelled.
    // That is also why the event loop is yielded first: a run whose last `put` is still in flight
    // has finished, and Python would already have seen it queued.
    await yieldToEventLoop();
    await this.consume(drainSignals(state.queue), state);
    // Only what is genuinely still running is then cancelled — the deadline is the one signal
    // allowed to stop a running operator — and whatever it managed to emit is kept.
    for (const record of state.running.values()) {
      record.handle.cancel();
    }
    if (state.running.size > 0) {
      await Promise.all([...state.running.values()].map((record) => record.promise));
    }
    // A task can enqueue a final emission right as it is cancelled; drain once more so it is not
    // dropped.
    await this.consume(drainSignals(state.queue), state);
    for (const operatorId of [...state.running.keys()].sort()) {
      this.failures.set(operatorId, CANCELLED_AT_DEADLINE);
      await this.publish(
        OperatorRunCompleted({
          ...this.eventBase(),
          operatorId,
          outcome: 'cancelled',
          attempt: (this.failedAttempts.get(operatorId) ?? 0) + 1,
          error: CANCELLED_AT_DEADLINE,
        }),
      );
    }
    state.running.clear();
  }

  private async plan(view: DataPointView, capabilities: CapabilityView, state: GatherState): Promise<Plan> {
    const presentTypes = view.presentTypes();
    const availableCapabilityTypes = capabilities.availableTypes();
    const runnable: Runnable[] = [];
    let soonestDue: number | null = null;
    const track = (dueAt: number | null): void => {
      if (dueAt !== null) {
        soonestDue = soonestDue === null ? dueAt : Math.min(soonestDue, dueAt);
      }
    };

    for (const operator of this.operators) {
      const operatorId = operator.operatorId;
      if (state.running.has(operatorId) || this.breaker.isTripped(operatorId)) {
        continue;
      }
      const ready = isReady(operator, { presentTypes, availableCapabilityTypes });
      if (state.retries.isScheduled(operatorId)) {
        // An armed retry owns this operator's next launch: it relaunches even with no new data and
        // its watermark is deliberately stale, so the rerun path must stand aside until it fires.
        if (!ready) {
          // Readiness is only ever lost to a capability revocation, so the relaunch can never run:
          // the failure it stood for is terminal now, not silently forgotten with the window.
          state.retries.clear(operatorId);
          const armed = this.armedRetries.get(operatorId);
          this.armedRetries.delete(operatorId);
          if (armed !== undefined) {
            await this.recordTerminalFailure(operatorId, armed.error, armed.observedRevision, armed.attempts);
          }
          continue;
        }
        if (state.retries.isDue(operatorId)) {
          runnable.push({ operator, delta: this.deltaFor(operator, capabilities), isRetry: true });
        } else {
          track(state.retries.dueAt(operatorId));
        }
        continue;
      }
      if (!ready) {
        continue;
      }
      if (!this.state.hasRun(operatorId)) {
        runnable.push({ operator, delta: this.deltaFor(operator, capabilities), isRetry: false });
        continue;
      }
      if (!operator.policy.rerunOnNewData) {
        continue;
      }
      const delta = this.deltaFor(operator, capabilities);
      if (!hasRelevantNewData(operator, delta)) {
        continue;
      }
      // Rerun-eligible: arm a coalescing window (arrivals within it collapse into one rerun) and
      // launch only once it is due. An armed window holds the loop open until then.
      if (!state.debounce.isScheduled(operatorId)) {
        state.debounce.schedule(operatorId, { windowMs: operator.policy.debounceMs });
      }
      if (state.debounce.isDue(operatorId)) {
        runnable.push({ operator, delta, isRetry: false });
      } else {
        track(state.debounce.dueAt(operatorId));
      }
    }
    const nextDueInMs = soonestDue === null ? null : Math.max(0, soonestDue - this.runtime.clock.monotonic());
    return { runnable, nextDueInMs };
  }

  private deltaFor(operator: OperatorClass, capabilities: CapabilityView): InvocationDelta {
    return this.state.deltaFor(operator.operatorId, {
      previousCaps: this.prevCaps.get(operator.operatorId) ?? new Set<CapabilityId>(),
      availableCaps: capabilities.availableIds(),
    });
  }

  private async runOne(
    operator: ConcreteOperatorClass,
    delta: InvocationDelta,
    snapshot: LaunchSnapshot,
    handle: RunHandle,
    queue: BoundedQueue<Signal>,
  ): Promise<void> {
    const context = new OperatorContext({
      sessionId: this.sessionId,
      namespaceId: this.namespaceId,
      store: snapshot.view,
      capabilities: snapshot.capabilities,
      delta,
      signal: handle.signal,
    });
    // A run never outlives the session: its timeout is clipped to the budget left, so the deadline
    // is hard even while the loop is blocked waiting on this run's signals.
    const operatorTimeoutMs = this.timeoutFor(operator);
    const clipped = snapshot.remainingMs !== null && snapshot.remainingMs < operatorTimeoutMs;
    const boundMs = clipped && snapshot.remainingMs !== null ? snapshot.remainingMs : operatorTimeoutMs;
    let error: unknown = null;
    try {
      await withTimeout(this.drain(operator, context, handle, queue), boundMs);
    } catch (thrown) {
      // Operator fault-isolation boundary — never wedge the session.
      error = thrown ?? new OrcastorkLiteError('the operator run failed without an error value');
      // `withTimeout` stops waiting; only this stops the operator, which Python's `wait_for` does
      // by cancelling the coroutine it was waiting on.
      handle.stop();
    }
    if (handle.cancelled) {
      // The deadline took this run away: the loop reports it, so reporting it here too would
      // count a run that never finished.
      return;
    }
    await queue.put({
      kind: 'completed',
      operatorId: operator.operatorId,
      error,
      observedRevision: snapshot.observedRevision,
      cutByDeadline: clipped && error instanceof OperationTimeoutError,
    });
  }

  /**
   * Stream one run's emissions onto the queue as they are produced.
   *
   * Finalizing stamps provenance and can throw on a malformed value; it happens inside the fault
   * boundary so a bad emission is isolated to this operator. Each emission is queued as produced,
   * so the loop merges it while this operator is still running.
   */
  private async drain(
    operator: ConcreteOperatorClass,
    context: OperatorContext,
    handle: RunHandle,
    queue: BoundedQueue<Signal>,
  ): Promise<void> {
    // Constructed inside the boundary too: a constructor that throws is a FAILED run, not a lost
    // task.
    const instance = new operator();
    const iterator = instance.run(context)[Symbol.asyncIterator]();
    handle.track(iterator);
    for (;;) {
      const next = await iterator.next();
      if (next.done === true) {
        return;
      }
      const finalized = next.value.finalize({
        retrievedBy: operator.operatorId,
        at: this.runtime.clock.now(),
      });
      await queue.put({ kind: 'emitted', operatorId: operator.operatorId, dataPoint: finalized });
    }
  }

  private timeoutFor(operator: OperatorClass): number {
    return operator.policy.timeoutMs ?? this.operationTimeoutMs;
  }

  private async refreshCapabilities(activator: CapabilityActivator, view: DataPointView): Promise<CapabilityView> {
    const capabilities = await activator.refresh(view);
    const outcomes: readonly { readonly capabilityId: CapabilityId; readonly outcome: ActivationOutcome }[] = [
      ...[...activator.activatedIds()].sort().map((capabilityId) => ({ capabilityId, outcome: 'activated' as const })),
      ...[...activator.failedIds()].sort().map((capabilityId) => ({ capabilityId, outcome: 'failed' as const })),
    ];
    for (const { capabilityId, outcome } of outcomes) {
      if (this.publishedCaps.has(capabilityId)) {
        continue;
      }
      this.publishedCaps.add(capabilityId);
      await this.publish(CapabilityActivated({ ...this.eventBase(), capabilityId, outcome }));
    }
    return capabilities;
  }

  private async consume(signals: readonly Signal[], state: GatherState): Promise<void> {
    // ONE merge for every emission in the batch (arrival order preserved), then the completion
    // bookkeeping — an operator's emissions always precede its completion on the queue anyway.
    await this.mergeEmissions(signals.filter((signal): signal is Emitted => signal.kind === 'emitted'));
    for (const signal of signals) {
      if (signal.kind === 'completed') {
        await this.recordCompletion(signal, state);
      }
    }
  }

  private async merge(dataPoints: readonly AnyDataPoint[]): Promise<void> {
    await this.publishMerge(this.state.merge(dataPoints));
  }

  private async mergeEmissions(emitted: readonly Emitted[]): Promise<void> {
    if (emitted.length === 0) {
      return;
    }
    for (const emission of emitted) {
      this.checkEmissionDeclared(emission.operatorId, emission.dataPoint);
    }
    await this.publishMerge(this.state.merge(emitted.map((emission) => emission.dataPoint)));
  }

  private async publishMerge(outcome: MergeOutcome): Promise<void> {
    const batches = [
      { merge: 'added' as const, dataPoints: outcome.added },
      { merge: 'updated' as const, dataPoints: outcome.updated },
    ];
    for (const { merge, dataPoints } of batches) {
      for (const dataPoint of dataPoints) {
        await this.publish(
          DataPointMerged({
            ...this.eventBase(),
            dataPointType: dataPoint.constructor.name,
            value: dataPoint.value,
            retrievedBy: dataPoint.retrievedBy,
            merge,
            revision: outcome.revision,
          }),
        );
      }
    }
  }

  private async recordCompletion(signal: Completed, state: GatherState): Promise<void> {
    const operatorId = signal.operatorId;
    state.running.delete(operatorId);
    if (signal.cutByDeadline) {
      // Not a run that finished, and not this operator's fault: the session's budget ran out under
      // it. Reported exactly like a run cancelled by the deadline check, and the loop exits on the
      // flag.
      this.deadlineHit = true;
      this.failures.set(operatorId, CANCELLED_AT_DEADLINE);
      await this.publishRun(operatorId, 'cancelled', (this.failedAttempts.get(operatorId) ?? 0) + 1, signal.error);
      return;
    }
    this.runs.set(operatorId, (this.runs.get(operatorId) ?? 0) + 1);
    // Every attempt counts toward the breaker, so a failing cycle operator cannot loop between the
    // retry scheduler and the cycle forever.
    this.breaker.recordRun(operatorId);
    const attempts = (this.failedAttempts.get(operatorId) ?? 0) + 1;
    if (signal.error === null) {
      this.failedAttempts.delete(operatorId);
      this.state.advanceWatermark(operatorId, signal.observedRevision);
      await this.publishRun(operatorId, 'succeeded', attempts, null);
      return;
    }
    this.failedAttempts.set(operatorId, attempts);
    if (this.scheduleRetry(operatorId, attempts, state)) {
      getLogger().warning('Operator run failed; a retry is scheduled on backoff', {
        operatorId,
        attempt: attempts,
        error: failureText(signal.error),
      });
      // A retried failure keeps its old watermark: the relaunch must re-present the same delta. Its
      // emissions are already merged, and re-emitting them is idempotent (keyed-merge).
      this.armedRetries.set(operatorId, {
        error: signal.error,
        observedRevision: signal.observedRevision,
        attempts,
      });
      await this.publishRun(operatorId, 'retrying', attempts, signal.error);
      return;
    }
    await this.recordTerminalFailure(operatorId, signal.error, signal.observedRevision, attempts);
  }

  private async recordTerminalFailure(
    operatorId: OperatorId,
    error: unknown,
    observedRevision: number,
    attempts: number,
  ): Promise<void> {
    // Terminal — a later data-driven rerun starts a fresh attempt sequence.
    this.failedAttempts.delete(operatorId);
    this.failures.set(operatorId, failureText(error));
    getLogger().error('Operator run failed; its emissions were kept and the scheduler is proceeding', {
      operatorId,
      attempt: attempts,
      error: failureText(error),
    });
    // Advance to the revision the failed run *observed*, not the live one: DataPoints merged while
    // it ran were never seen by it, so they must still count as new data and be able to trigger a
    // rerun.
    this.state.advanceWatermark(operatorId, observedRevision);
    await this.publishRun(operatorId, 'failed', attempts, error);
  }

  private async publishRun(
    operatorId: OperatorId,
    outcome: RunOutcome,
    attempt: number,
    error: unknown,
  ): Promise<void> {
    // A run cut at the deadline reports the deadline, not the bound that happened to fire.
    const reported = outcome === 'cancelled' ? CANCELLED_AT_DEADLINE : failureText(error);
    const message = error === null ? null : reported;
    await this.publish(OperatorRunCompleted({ ...this.eventBase(), operatorId, outcome, attempt, error: message }));
  }

  /** Arm a loop-scheduled relaunch of a failed run; `false` when the failure is terminal. */
  private scheduleRetry(operatorId: OperatorId, attempts: number, state: GatherState): boolean {
    const policy = this.operatorsById.get(operatorId)?.policy.retry ?? null;
    // A tripped breaker is terminal even with attempts remaining — an armed retry could never
    // launch.
    if (policy === null || attempts >= policy.maxAttempts || this.breaker.isTripped(operatorId)) {
      return false;
    }
    const delayMs = backoffDelays(policy, { seed: seedFor(this.sessionId, operatorId) })[attempts - 1];
    state.retries.schedule(operatorId, { windowMs: delayMs ?? null });
    return true;
  }

  private checkEmissionDeclared(operatorId: OperatorId, dataPoint: AnyDataPoint): void {
    // Cycle detection and pruning reason from `produces`, so an undeclared emission silently
    // invalidates them. The DataPoint is still merged — data is never dropped — but the mismatch is
    // logged as an ERROR once per operator-and-type so the declaration gets fixed at the source.
    const declared = this.operatorsById.get(operatorId)?.produces ?? [];
    if (declared.some((declaredType) => dataPoint instanceof declaredType)) {
      return;
    }
    const dataPointType = dataPoint.constructor as DataPointClass;
    const reported = this.undeclaredEmissions.get(operatorId) ?? new Set<DataPointClass>();
    this.undeclaredEmissions.set(operatorId, reported);
    if (reported.has(dataPointType)) {
      return;
    }
    reported.add(dataPointType);
    getLogger().error('Operator emitted a DataPoint type missing from its produces declaration; merging it anyway', {
      operatorId,
      dataPointType: dataPointType.name,
      declaredProduces: declared.map((declaredType) => declaredType.name).sort(),
    });
  }

  private eventBase(): SessionEventBase {
    return { sessionId: this.sessionId, namespaceId: this.namespaceId, at: this.runtime.clock.now() };
  }

  private async publish(event: SessionEvent): Promise<void> {
    // The sink is a view of the session, not part of it: a publish that throws OR hangs is logged
    // and the event dropped, never allowed to stop the loop. The bound matters as much as the
    // catch — a stalled connection throws nothing, and the session deadline is only checked between
    // passes.
    try {
      await withTimeout(this.runtime.events.publish(event), this.publishTimeoutMs);
    } catch (error) {
      // Sink fault-isolation boundary.
      getLogger().warning('Session event publish failed or timed out; the event was dropped', {
        sessionId: this.sessionId,
        eventKind: event.kind,
        publishTimeoutMs: this.publishTimeoutMs,
        error: failureText(error),
      });
    }
  }
}

/** Everything the queue holds right now, taken without waiting. */
const drainSignals = (queue: BoundedQueue<Signal>): readonly Signal[] => {
  const signals: Signal[] = [];
  for (;;) {
    const signal = queue.getNowait();
    if (signal === undefined) {
      return signals;
    }
    signals.push(signal);
  }
};

/**
 * Whether new data (or a new capability) is worth rerunning `operator` for.
 *
 * A newly-available capability always warrants a rerun. Otherwise new data must match a type the
 * operator consumes (`dependsOn` or `uses`, subtype-aware); under `ADDED_ONLY` a freshness-only
 * re-observation does not count. An operator consuming what it produces is a genuine cycle whose
 * own emissions re-trigger it — which is why the graph requires it to declare `maxCycles`.
 */
const hasRelevantNewData = (operator: OperatorClass, delta: InvocationDelta): boolean => {
  if (delta.newlyAvailableCaps.size > 0) {
    return true;
  }
  const newData =
    operator.policy.rerunOn === RerunOn.ADDED_ONLY ? delta.added : new Set([...delta.added, ...delta.updated]);
  const triggers: readonly DataPointClass[] = [...(operator.dependsOn ?? []), ...(operator.uses ?? [])];
  return [...newData].some((dataPoint) => triggers.some((trigger) => dataPoint instanceof trigger));
};

/** What a failure reads as in the result and on the wire — Python's `str(exc)`. */
const failureText = (error: unknown): string => (error instanceof Error ? error.message : String(error));

/**
 * Hand the event loop back once — the counterpart of `asyncio.sleep(0)` for a *task*, not a timer.
 *
 * `setImmediate` runs after the pending microtasks and after any expired timer, which is exactly
 * the point at which Python's loop has let every runnable task take its turn.
 */
const yieldToEventLoop = (): Promise<void> =>
  new Promise<void>((resolve) => {
    setImmediate(resolve);
  });
