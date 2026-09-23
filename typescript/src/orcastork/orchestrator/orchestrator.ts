/**
 * The session-scoped Orchestrator.
 *
 * It owns one session: acquire ownership (minting the fencing epoch), seed, then run the
 * **gathering** loop. The loop is *eager*, not phased: every ready operator is launched as a
 * concurrent task, and the orchestrator drains the available signals off a shared queue each pass —
 * merging each emission the instant it is produced and re-evaluating readiness, so a downstream
 * operator starts as soon as its input lands rather than waiting for a whole batch to finish.
 * Operators are never cancelled by new data; reruns are coalesced on a debounce window. When
 * nothing is running and no operator can become ready (graph-aware quiescence), it runs the
 * **aggregation** phase, records the session complete durably (so a supervisor never re-drives a
 * finished session), and returns. A flow that expects mid-session input declares `completesWhen` —
 * a DataPoint type or a declarative `CompletionCondition` AST (`allOf`/`anyOf`/`TypePresent`):
 * until it is satisfied, the would-be-quiescent session instead **waits on the inbox**
 * (event-driven, lease kept alive, bounded by the session deadline) and resumes the loop when input
 * arrives. A wait that stays idle past `parkAfterMs` instead **parks** the session: the run returns
 * PARKED without aggregating or marking complete, releasing the pod, the subscription and the lease
 * — the durable inbox plus the supervisor's resume-on-deliver re-drive it when input finally lands.
 * The session deadline is persisted in wall-clock terms at the first gather and rehydrated by every
 * later run, so a parked (or crash-looping) session consumes one shrinking budget rather than a
 * fresh full window per process. The epoch is always released and the audit/archive always flushed,
 * even on an unexpected error (`try/finally`).
 *
 * It is the **sole mutator** of the store: every write (seed, emissions, inbox entries, watermarks)
 * is epoch-guarded and applied on the single gathering loop — operators only stream emissions onto
 * the queue, so no two writers ever touch the store at once. Being the sole mutator, it keeps a
 * local {@link SessionStateMirror} of the session's DataPoint state — rehydrated once per run, read
 * locally on every pass, written through to the store per merge batch — so the hot loop never
 * re-reads the store it alone writes. Aggregators write the curated durable outputs, and the
 * orchestrator additionally **live-archives** every non-ephemeral DataPoint through the same
 * epoch-guarded write-behind buffer (the second durable write path) — so both persistence
 * boundaries are framework-owned. Each DataPoint-added and capability-activation is audited
 * per-event off the hot path. An operator that raises or times out is isolated — its
 * already-emitted DataPoints persist, the failure is logged, and the scheduler proceeds (no wedge).
 * A policy that declares `retry` relaunches the failed operator on a loop-scheduled backoff window
 * (like a debounced rerun) — never an in-task sleep, so lease renewal, the session deadline, and
 * inbox draining stay live throughout the backoff. Inbox entries get the same isolation: an
 * undecodable (poison) entry is quarantined on sight, and a valid entry whose apply keeps failing
 * is redelivered up to a bounded cap and then quarantined — one bad message never crash-loops the
 * session.
 *
 * @module
 */

import type { Span } from '@opentelemetry/api';
import { SpanStatusCode } from '@opentelemetry/api';
import { AggregationHelpers } from '../aggregation/helpers.js';
import type { RetryPolicy } from '../aggregation/retry.js';
import { backoffDelays, RetryPolicy as makeRetryPolicy, runWithRetry, seedFor } from '../aggregation/retry.js';
import type { ValueCipher } from '../archive/cipher.js';
import { NullCipher } from '../archive/cipher.js';
import { ArchivedDataPoint } from '../archive/index.js';
import type { AuditLogEntry } from '../audit/index.js';
import {
  AuditKind,
  CapabilityAuditInfo,
  DataPointAuditInfo,
  FlowAuditInfo,
  InboxAuditInfo,
  AuditLogEntry as makeAuditLogEntry,
  OperatorAuditInfo,
  OperatorOutcome,
} from '../audit/index.js';
import { CapabilityActivator } from '../capabilities/availability.js';
import type { CapabilityClass, ConcreteCapabilityClass } from '../capabilities/base.js';
import type { AnyDataPoint, DataPointClass, DataPointEmission, DataPointView } from '../datapoints/index.js';
import { isSubclass, MergeKind } from '../datapoints/index.js';
import { AggregatorDeadLetteredError, CompletionTailTimeoutError, StaleEpochError } from '../exceptions.js';
import type { FlowIdentity } from '../flow.js';
import {
  backwardReachable,
  buildGraph,
  CircuitBreaker,
  findCycles,
  validateAcyclicOrBounded,
} from '../graph/index.js';
import type { CapabilityId, Epoch, NamespaceId, OperatorId, Revision, SessionId } from '../ids.js';
import { BoundedQueue } from '../internal/bounded_queue.js';
import { Deferred } from '../internal/deferred.js';
import { OperationTimeoutError, withTimeout } from '../internal/timeouts.js';
import { getLogger } from '../logging.js';
import { LOGGER_NAME_FIELD } from '../logging_bridge.js';
import type { AggregatorStatics, ConcreteOperatorClass, InvocationDelta, OperatorClass } from '../operators/index.js';
import {
  Aggregator,
  type CapabilityView,
  EffectGuard,
  Operator,
  OperatorContext,
  RerunOn,
} from '../operators/index.js';
import type { DeliveredInboxEntry } from '../ports/change_set.js';
import { InboxEntry, PoisonInboxEntry } from '../ports/change_set.js';
import type { OrchestratorRuntime } from '../runtime.js';
import type { CompletionCondition, CompletionItem, ReadinessGap } from '../scheduling/index.js';
import {
  DebounceController,
  isReady,
  normalizeCompletion,
  operatorDelta,
  reachablePending,
  readinessGap,
  referencedTypes,
  rerunEligible,
  windowDefersToFinalize,
} from '../scheduling/index.js';
import { withSpan } from '../telemetry.js';
import type { MirrorWriteResult } from './mirror.js';
import { SessionStateMirror } from './mirror.js';

/** The module a log record names as its origin, so the OTel bridge can filter on it. */
const LOGGER_NAME = 'orcastork.orchestrator.orchestrator';

/** How long one operator run — or one completion-tail step — may take by default. */
export const DEFAULT_OPERATION_TIMEOUT_MS = 30_000;

/** How long a whole session may gather by default, as a persisted wall-clock budget. */
export const DEFAULT_SESSION_DEADLINE_MS = 300_000;

/**
 * How often the orchestrator renews its ownership lease while working.
 *
 * Must be shorter than the lock adapter's TTL — and the TTL should exceed the longest single
 * operator run the loop can be blocked on without renewing: the global `operationTimeoutMs`, raised
 * by any larger per-operator `OperatorPolicy.timeoutMs` override. A renew that finds a higher epoch
 * raises and is turned into a clean SUPERSEDED stop.
 */
export const DEFAULT_LEASE_RENEW_INTERVAL_MS = 10_000;

/**
 * How many deliveries a valid-but-unappliable inbox entry gets before it is quarantined instead of
 * redelivered: enough for a transient store fault to clear, small enough that a persistently bad
 * entry cannot grind against the store for the session's whole lifetime.
 */
export const DEFAULT_MAX_INBOX_DELIVERIES = 5;

/**
 * Upper bound on queued-but-not-yet-merged emissions: large enough that a healthy flow never
 * touches it, small enough that a runaway streaming operator cannot grow the heap without limit
 * (the founding Reactive Streams lesson — backpressure, not unbounded buffering).
 */
export const DEFAULT_EMISSION_QUEUE_SIZE = 1024;

/** How a session run ended. */
export const SessionStatus = {
  COMPLETED: 'completed',

  /** A higher epoch took over mid-run; this orchestrator stopped without finalizing. */
  SUPERSEDED: 'superseded',

  /** Idle past `parkAfterMs` while waiting on the inbox; not finalized — a deliver/resume re-drives it. */
  PARKED: 'parked',
} as const;

/** One of the three {@link SessionStatus} values; the strings reach the audit trail and metrics. */
export type SessionStatus = (typeof SessionStatus)[keyof typeof SessionStatus];

/** An aggregator whose bounded retries were exhausted; carried out for a manual re-drive. */
export interface DeadLetter {
  readonly operatorId: OperatorId;

  readonly reason: string;
}

/** What one `run()` produced. */
export interface OrchestratorResult {
  readonly status: SessionStatus;

  readonly epoch: Epoch;

  /** Finished runs per operator, retries and reruns included. */
  readonly operatorRuns: ReadonlyMap<OperatorId, number>;

  /** Dead-lettered aggregator failures, for manual re-drive. */
  readonly deadLetters: readonly DeadLetter[];
}

// The loop's own signals and records are plain objects, not validated models: the loop is their
// only constructor, so validation would only check the engine against itself, on every emission.

/** One DataPoint presented at the sole-mutator merge point. */
interface Emitted {
  readonly kind: 'emitted';

  /**
   * `null` when the merge is not an operator emission (the seed, an inbox entry) — the same
   * merge-item shape carries every DataPoint through the sole-mutator merge point.
   */
  readonly operatorId: OperatorId | null;

  /** Already finalized inside the fault-isolation boundary ({@link Orchestrator.runOne}). */
  readonly dataPoint: AnyDataPoint;
}

/** One run that ended, however it ended. */
interface Completed {
  readonly kind: 'completed';

  readonly operatorId: OperatorId;

  /** Set if the operator raised/timed out; its prior emissions are already persisted. */
  readonly error: unknown;

  /** The operator's own run time in **seconds** (invoked → completed), for the audit/profile. */
  readonly runSeconds: number;
}

/** Everything an operator task can tell the loop. */
export type Signal = Emitted | Completed;

/**
 * One launched run's cancellation handle — what Python gets from `asyncio.Task.cancel()`.
 *
 * A promise cannot be cancelled, so stopping a run is three things the loop must do itself: abort
 * the context's signal (anything the operator tied to it gives up), return the async generator (it
 * stops at its next yield point), and release a hand-off blocked on a full emission queue — a task
 * parked in `put` must give up exactly like one parked in its own awaits. Already-queued emissions
 * stay queued and are still merged.
 */
class RunHandle {
  private readonly controller = new AbortController();
  private readonly stopped = new Deferred<void>();
  private iterator: AsyncIterator<DataPointEmission> | null = null;
  private isCancelled = false;

  /** Handed to the operator on its context; aborted when the run is cut short. */
  public get signal(): AbortSignal {
    return this.controller.signal;
  }

  /** Whether the deadline cancelled this run, which the loop reports (as Python's task death does). */
  public get cancelled(): boolean {
    return this.isCancelled;
  }

  /** Settles once the run has been told to stop; raced against a blocked emission hand-off. */
  public get whenStopped(): Promise<void> {
    return this.stopped.promise;
  }

  /** Remember the generator being drained, so stopping the run can end it. */
  public track(iterator: AsyncIterator<DataPointEmission>): void {
    this.iterator = iterator;
  }

  /** Tell the run to stop working; it still reports how it ended. */
  public stop(): void {
    this.controller.abort();
    this.stopped.resolve();
    const iterator = this.iterator;
    this.iterator = null;
    // A generator that has already finished has nothing to stop, and one that throws on the way out
    // is a fault of the operator, not of the loop that is shutting it down.
    void iterator?.return?.()?.catch(() => undefined);
  }

  /** The deadline taking the run away: it stops, and reports nothing. */
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

  /** Store revision the launch snapshot saw — becomes this run's watermark. */
  readonly observedRevision: Revision;
}

/** One operator the loop is about to launch. */
interface Runnable {
  readonly operator: ConcreteOperatorClass;

  /** Computed once here and threaded into the run (no recompute). */
  readonly delta: InvocationDelta;

  readonly isRerun: boolean;

  /** A due relaunch of a failed run (consumes the armed retry, not the debounce). */
  readonly isRetry: boolean;
}

/** What one inbox wait decided. */
interface InboxWait {
  /** Updated renew bookkeeping — the wait renews the lease on the loop's cadence. */
  readonly lastRenew: number;

  /** The idle window (`parkAfterMs`) elapsed before any input arrived. */
  readonly parked: boolean;
}

/** What one planning pass decided. */
interface GatherPlan {
  /** Operators to launch now (first-runs + due reruns/retries, never already-running). */
  readonly runnable: readonly Runnable[];

  /** For the graph-aware quiescence check. */
  readonly notYetRun: readonly OperatorClass[];

  /** Milliseconds until the soonest armed-but-not-due rerun/retry, else `null`. */
  readonly nextDueInMs: number | null;

  /** DataPoint types in the snapshot (quiescence check). */
  readonly presentTypes: ReadonlySet<DataPointClass<AnyDataPoint>>;

  /** Available capability types (quiescence check). */
  readonly availableTypes: ReadonlySet<CapabilityClass>;
}

/** Everything the orchestrator is built from — the port of `__init__`'s keyword arguments. */
export interface OrchestratorOptions {
  readonly sessionId: SessionId;

  readonly namespaceId: NamespaceId;

  /** The injected bundle of ports, the clock and the telemetry. */
  readonly runtime: OrchestratorRuntime;

  /** The operator classes to schedule; a fresh instance is constructed per run. */
  readonly operators: Iterable<ConcreteOperatorClass>;

  /** The capability classes available to this session, subject to namespace permission. */
  readonly capabilities?: Iterable<ConcreteCapabilityClass>;

  /** DataPoints the session starts from, merged before the first pass. */
  readonly seed?: Iterable<AnyDataPoint>;

  /** A DataPoint type or a declarative condition; `null`/omitted means "quiescence completes". */
  readonly completesWhen?: CompletionItem | null;

  /** Bound on one operator run, one aggregator attempt and one completion-tail step. */
  readonly operationTimeoutMs?: number | null;

  /** The whole session's wall-clock budget, persisted at the first gather. */
  readonly sessionDeadlineMs?: number | null;

  /** How long a continuously idle inbox wait may last before the session parks; `null` disables it. */
  readonly parkAfterMs?: number | null;

  readonly leaseRenewIntervalMs?: number;

  readonly maxInboxDeliveries?: number | null;

  /** Non-positive is coerced to the bounded default — the queue is never unbounded. */
  readonly emissionQueueSize?: number | null;

  /** The aggregation-phase retry schedule; also the capability activation cool-off. */
  readonly retryPolicy?: RetryPolicy | null;

  /** Backs resume drift detection; a directly-constructed orchestrator carries none. */
  readonly flowIdentity?: FlowIdentity | null;

  /** Discard the persisted deadline and write a fresh full budget (a re-open for late data). */
  readonly freshDeadline?: boolean;
}

/** Runs one session: schedules operators by data readiness, then aggregates and finalizes. */
export class Orchestrator {
  private readonly sessionId: SessionId;
  private readonly namespaceId: NamespaceId;
  private readonly runtime: OrchestratorRuntime;

  /**
   * Resolved once at run start from `runtime.cipherProvider`; seals PII audit values under the
   * namespace key. `null` (or a {@link NullCipher}) → PII stays `<redacted>` rather than written in
   * clear.
   */
  private auditCipher: ValueCipher | null = null;
  private readonly operators: readonly ConcreteOperatorClass[];
  private readonly capabilities: readonly ConcreteCapabilityClass[];
  private readonly seed: readonly AnyDataPoint[];
  private readonly completesWhen: CompletionCondition | null;
  private readonly operationTimeoutMs: number;
  private readonly sessionDeadlineMs: number;
  private readonly parkAfterMs: number | null;
  private readonly leaseRenewIntervalMs: number;
  private readonly maxInboxDeliveries: number;
  private readonly retryPolicy: RetryPolicy;
  private readonly flowIdentity: FlowIdentity | null;

  /** type → merges suppressed from the per-emission trail, reported as one row each before release. */
  private readonly coalescedAudits = new Map<string, number>();

  /** type → merges that re-observed an existing identity, reported as one row each. */
  private readonly reobservedAudits = new Map<string, number>();

  /**
   * When true, the persisted deadline is discarded and a fresh full budget is set at the first
   * gather. Used for re-opens: the original deadline is almost certainly blown, but gathering is a
   * no-op (the completion condition is already satisfied) and the aggregation phase needs a live
   * budget.
   */
  private freshDeadline: boolean;
  private readonly deadLetters: DeadLetter[] = [];

  /**
   * Aggregators normally run only in the aggregation phase; an `interimRefresh` aggregator ALSO
   * joins the gather set so the operator scheduler (readiness + rerun-on-new-data) drives live
   * interim writes. It stays in {@link Orchestrator.aggregators} too, for the authoritative
   * finalize pass.
   */
  private gathering: readonly ConcreteOperatorClass[];
  private aggregators: readonly ConcreteOperatorClass[];
  private readonly operatorsById: ReadonlyMap<OperatorId, ConcreteOperatorClass>;
  /** Which (operator, DataPoint type) mismatches have already been reported. */
  private readonly undeclaredEmissions = new Map<OperatorId, Set<string>>();
  private readonly registeredCaps: ReadonlyMap<CapabilityId, ConcreteCapabilityClass>;
  private readonly runs = new Map<OperatorId, number>();

  /** Consecutive failures of the current retry sequence, per operator. */
  private readonly failedAttempts = new Map<OperatorId, number>();
  private readonly prevCaps = new Map<OperatorId, ReadonlySet<CapabilityId>>();

  /** Rehydrated once per gather (the orchestrator is the session's sole watermark writer). */
  private watermarks = new Map<OperatorId, Revision | null>();
  private readonly activatedSeen = new Set<CapabilityId>();

  /**
   * The sole-mutator local state (rehydrated in `run`): every keyed-merge resolves here and every
   * hot-loop read is served here, so the store is written once per batch and fully read once per
   * run instead of once per pass.
   *
   * `protected` rather than private for the same reason Python names it `_mirror`: a test that pins
   * the bounded-growth invariant reads the mirrored identity count through a subclass.
   */
  protected readonly mirror: SessionStateMirror;

  /**
   * The resolved bound on queued-but-not-yet-merged emissions.
   *
   * Public because it is the one tuning knob a caller cannot read back off the options it passed: a
   * non-positive request is coerced to {@link DEFAULT_EMISSION_QUEUE_SIZE}, and a spawn built by the
   * manager or a replay is asserted against this to prove the flow-level bound reached it.
   */
  public readonly emissionQueueSize: number;

  public constructor(options: OrchestratorOptions) {
    this.sessionId = options.sessionId;
    this.namespaceId = options.namespaceId;
    this.runtime = options.runtime;
    this.operators = [...options.operators];
    this.capabilities = [...(options.capabilities ?? [])];
    this.seed = [...(options.seed ?? [])];
    this.completesWhen = normalizeCompletion(options.completesWhen ?? null);
    // `null` means "use the orchestrator default", so a FlowDefinition can leave tuning unset
    // without this module's defaults leaking into the flow layer.
    this.operationTimeoutMs = options.operationTimeoutMs ?? DEFAULT_OPERATION_TIMEOUT_MS;
    this.sessionDeadlineMs = options.sessionDeadlineMs ?? DEFAULT_SESSION_DEADLINE_MS;
    this.parkAfterMs = options.parkAfterMs ?? null;
    this.leaseRenewIntervalMs = options.leaseRenewIntervalMs ?? DEFAULT_LEASE_RENEW_INTERVAL_MS;
    this.maxInboxDeliveries = options.maxInboxDeliveries ?? DEFAULT_MAX_INBOX_DELIVERIES;
    // A non-positive size means an unbounded queue, which silently defeats backpressure — coerce it
    // to the bounded default so the queue is never the immortal-buffer footgun.
    const requestedQueueSize = options.emissionQueueSize ?? null;
    this.emissionQueueSize =
      requestedQueueSize === null || requestedQueueSize <= 0 ? DEFAULT_EMISSION_QUEUE_SIZE : requestedQueueSize;
    this.retryPolicy = options.retryPolicy ?? makeRetryPolicy();
    this.flowIdentity = options.flowIdentity ?? null;
    this.freshDeadline = options.freshDeadline ?? false;
    this.gathering = this.operators.filter(
      (operator) => !isSubclass(operator, Aggregator) || interimRefreshOf(operator),
    );
    this.aggregators = this.operators.filter((operator) => isSubclass(operator, Aggregator));
    this.mirror = new SessionStateMirror(this.runtime.store, this.sessionId);
    this.pruneToConsumedClosure();
    this.operatorsById = new Map(this.operators.map((operator) => [operator.operatorId, operator]));
    this.registeredCaps = new Map(this.capabilities.map((capability) => [capability.capabilityId, capability]));
    // Fail fast on an unbounded cycle rather than busy-looping to the session deadline.
    validateAcyclicOrBounded(buildGraph(this.gathering, this.capabilities));
  }

  /**
   * Run only the operators whose output is (transitively) consumed by an aggregator.
   *
   * Opt-in: when an aggregator declares `consumes` (the DataPoint types it folds/persists), the
   * gather set is restricted to the backward-reachable closure of those sinks — plus the
   * aggregators' own gate inputs and the completion condition — so an operator producing data
   * nothing considers never runs (and can't hold the session open). No declared `consumes` (or a
   * completion condition whose types can't be introspected) leaves every operator in place, so
   * existing flows are unaffected. Aggregators are sinks and are always kept.
   */
  private pruneToConsumedClosure(): void {
    const declared = new Set<DataPointClass<AnyDataPoint>>();
    for (const aggregator of this.aggregators) {
      for (const consumed of consumesOf(aggregator)) {
        declared.add(consumed);
      }
    }
    if (declared.size === 0) {
      return;
    }
    const completionTypes = referencedTypes(this.completesWhen);
    if (completionTypes === null) {
      return; // opaque completion condition — cannot prove a completion producer is safe to drop
    }
    const sinks = new Set<DataPointClass<AnyDataPoint>>(declared);
    for (const aggregator of this.aggregators) {
      for (const gateInput of aggregator.dependsOn ?? []) {
        sinks.add(gateInput);
      }
    }
    for (const completionType of completionTypes) {
      sinks.add(completionType);
    }
    const runnable = this.gathering.filter((operator) => !isSubclass(operator, Aggregator));
    const kept = backwardReachable(runnable, this.capabilities, sinks);
    const pruned = runnable.filter((operator) => !kept.has(operator));
    if (pruned.length === 0) {
      return;
    }
    this.gathering = this.gathering.filter((operator) => isSubclass(operator, Aggregator) || kept.has(operator));
    getLogger().info('Pruned operators whose output no aggregator consumes (backward-reachable closure)', {
      [LOGGER_NAME_FIELD]: LOGGER_NAME,
      session_id: this.sessionId,
      pruned: pruned.map((operator) => String(operator.operatorId)).sort(),
      kept: runnable
        .filter((operator) => kept.has(operator))
        .map((operator) => String(operator.operatorId))
        .sort(),
    });
  }

  /** Acquire the epoch, gather, aggregate, finalize — and always release what it took. */
  public async run(): Promise<OrchestratorResult> {
    // The root span covers everything ownership-scoped — acquire, gather, aggregation, the final
    // flushes — so one trace shows the whole run. Per-session identifiers are fine as span
    // attributes (each span stands alone; only metric attributes create series).
    return await withSpan(this.runtime.telemetry.tracer, 'session.run', async (span) => await this.runOwned(span), {
      attributes: { session_id: this.sessionId, namespace_id: this.namespaceId },
    });
  }

  private async runOwned(span: Span): Promise<OrchestratorResult> {
    const epoch = await this.runtime.lock.acquire(this.sessionId);
    span.setAttribute('epoch', epoch);
    getLogger().info('Session run started after acquiring the fencing epoch', {
      [LOGGER_NAME_FIELD]: LOGGER_NAME,
      session_id: this.sessionId,
      namespace_id: this.namespaceId,
      epoch,
      flow_name: this.flowIdentity === null ? null : this.flowIdentity.name,
    });
    this.auditCipher = await this.resolveAuditCipher();
    // Set once the loop's own flush ran, so the finally-flush stays a crash-path backstop.
    let flushed = false;
    try {
      await this.excludeUnpermittedOperators();
      await this.detectFlowDrift(epoch);

      const activator = new CapabilityActivator(
        this.registeredCaps,
        this.runtime.catalog,
        this.namespaceId,
        this.runtime.clock,
        {
          // Audit every capability invocation at the view's seam; the hook carries this run's epoch.
          onInvoke: async (capabilityId, action, parameters): Promise<void> => {
            await this.auditCapabilityInvocation(capabilityId, action, parameters, epoch);
          },
          activationRetry: this.retryPolicy,
          // A terminal activation failure means a whole dependent subgraph silently disappeared for
          // the session — that must be visible in the audit trail, mirroring `auditCapability`.
          onTerminalFailure: async (capabilityId, error): Promise<void> => {
            await this.runtime.audit.append(
              makeAuditLogEntry({
                sessionId: this.sessionId,
                namespaceId: this.namespaceId,
                epoch,
                timestamp: this.runtime.clock.now(),
                kind: AuditKind.CAPABILITY_ACTIVATION_FAILED,
                capability: CapabilityAuditInfo({ capabilityId, error: failureText(error) }),
              }),
            );
          },
          rateLimiter: this.runtime.rateLimiter,
          telemetry: this.runtime.telemetry,
        },
      );
      // Rehydrate the sole-mutator mirror before the first merge: from here on every DataPoint read
      // and keyed-merge is local, written through to the store per batch.
      await this.mirror.rehydrate();
      if (this.seed.length > 0) {
        getLogger().debug('Seeding the store with initial data points before the first gather', {
          [LOGGER_NAME_FIELD]: LOGGER_NAME,
          session_id: this.sessionId,
          seed_types: [...new Set(this.seed.map((dataPoint) => dataPoint.type))].sort(),
        });
        await this.merge(this.seed, epoch);
      }
      const breaker = this.buildCircuitBreaker();
      // Drain-before-release: while it holds the epoch the orchestrator owns the inbox, so after
      // aggregating + flushing it re-checks the inbox and, if a deliver landed during the flush
      // window, loops back to gather + aggregate on the SAME epoch (fresh drain budget) instead of
      // releasing. It marks complete + releases only once the inbox is empty at that point. The
      // happy path is one iteration; a late deliver folds in-run rather than waiting for a re-spawn.
      for (;;) {
        const gatherStarted = this.runtime.clock.monotonic();
        const parked = await withSpan(
          this.runtime.telemetry.tracer,
          'session.gather',
          async () => await this.gather(epoch, activator, breaker),
        );
        this.runtime.telemetry.sessionGatherSeconds.record(
          secondsSince(gatherStarted, this.runtime.clock.monotonic()),
        );
        if (parked) {
          // Parked: no aggregation and no completion mark, so the session stays resumable — a
          // supervisor sees a not-complete, unlocked session and re-drives it on delivery. The audit
          // entries are appended here so the trail shows the park.
          //
          // The coalesced counts are appended here too: they live only on this instance and the
          // resume that follows starts them at zero, so a park — which releases cleanly, unlike a
          // crash — must commit them or the trail permanently under-counts the merges it stood in
          // for. Appending clears the counters, so a run that parks cannot re-report them.
          await this.appendCoalescedAudits(epoch);
          await this.auditSessionParked(epoch);
          getLogger().info('Session parked: idle waiting exceeded park_after; releasing the pod until input arrives', {
            [LOGGER_NAME_FIELD]: LOGGER_NAME,
            session_id: this.sessionId,
            park_after_ms: this.parkAfterMs,
          });
          this.countSession(SessionStatus.PARKED, span);
          return this.result(SessionStatus.PARKED, epoch);
        }
        const aggregationStarted = this.runtime.clock.monotonic();
        await withSpan(this.runtime.telemetry.tracer, 'session.aggregate', async () => {
          await this.aggregate(epoch, activator);
        });
        this.runtime.telemetry.sessionAggregationSeconds.record(
          secondsSince(aggregationStarted, this.runtime.clock.monotonic()),
        );
        // Flush the write-behind archive BEFORE the inbox check (moved out of the finally): a
        // deliver racing the check must be re-drivable off durable state, and every durable write is
        // complete before the drained-before-release decision is made.
        await this.appendCoalescedAudits(epoch);
        await this.boundedTailStep(this.flushWriteBehind(), 'write-behind flush');
        flushed = true;
        const pending = await this.boundedTailStep(
          this.runtime.inbox.pendingCount(this.sessionId),
          'inbox pending-count re-check',
        );
        if (pending > 0) {
          // A late deliver landed during aggregate/flush. Re-drive on the SAME epoch: clear each
          // aggregator's contribution so it re-folds the new data, then loop back to gather.
          await this.prepareRedrive();
          continue;
        }
        // Mark complete before releasing the epoch. The flag is co-located with the epoch counter on
        // the lock, so this is an atomic compare-and-set against the live epoch: a fenced
        // predecessor cannot finalize, and a successor (even on another pod) reads it to skip a
        // finished session.
        await this.boundedTailStep(this.runtime.lock.markComplete(this.sessionId, { epoch }), 'mark complete');
        getLogger().info('Session completed; durable outputs written and session marked complete', {
          [LOGGER_NAME_FIELD]: LOGGER_NAME,
          session_id: this.sessionId,
          epoch,
          operator_runs: [...this.runs.values()].reduce((total, count) => total + count, 0),
          dead_letters: this.deadLetters.length,
        });
        this.countSession(SessionStatus.COMPLETED, span);
        return this.result(SessionStatus.COMPLETED, epoch);
      }
    } catch (error) {
      if (!(error instanceof StaleEpochError)) {
        throw error;
      }
      // A higher epoch took over while we were working (a renew, store write, or audit/durable write
      // was fenced). The successor now owns the session; stop without finalizing. No corruption is
      // possible — every fenced write was already rejected — so this is a clean exit, not a failure.
      // The successor re-drives any unfinished work idempotently.
      getLogger().warning('Orchestrator fenced by a higher epoch; stopping (the successor owns the session)', {
        [LOGGER_NAME_FIELD]: LOGGER_NAME,
        session_id: this.sessionId,
        epoch,
      });
      this.countSession(SessionStatus.SUPERSEDED, span);
      return this.result(SessionStatus.SUPERSEDED, epoch);
    } finally {
      // Always release the epoch, even if a flush raises — a held epoch would otherwise block
      // recovery until the lease TTL expired. The loop flushes the write-behind archive itself on
      // the completion path; this backstops the park/crash/error paths where that flush never ran. A
      // crash before it completes is safe: its buffer is durable and replayed on resume, and the
      // release below still runs.
      //
      // The check→release window is non-atomic and spans two separate ops (the completion-path
      // pendingCount read, then this release): a cross-pod deliver appending a straggler in that gap
      // is invisible to a read taken while we still hold the epoch, so it cannot be caught in-run.
      // The manager's post-release pendingCount probe (holding no epoch, reading after `run()`
      // returns) catches it and re-spawns to drain — that probe subsumes any pre-release flag.
      // Residual: a pod crash in the microscopic [release, manager-probe] window is a double-failure
      // that needs a host-level reopen-capable re-drive scanner (out of scope here); a post-probe
      // deliver is already covered by deliver-spawns-on-free (reopen on a free lock).
      try {
        if (!flushed) {
          await this.boundedTailStep(this.flushWriteBehind(), 'write-behind flush (backstop)');
        }
      } finally {
        await this.runtime.lock.release(this.sessionId, { epoch });
      }
    }
  }

  private result(status: SessionStatus, epoch: Epoch): OrchestratorResult {
    return Object.freeze({
      status,
      epoch,
      operatorRuns: new Map(this.runs),
      deadLetters: Object.freeze([...this.deadLetters]),
    });
  }

  /**
   * One row per type per reason, carrying how many merges it stood in for.
   *
   * Two reasons collapse into counts rather than rows: a type that opted out of per-emission
   * auditing ("merged"), and a merge that only re-observed an already-audited identity
   * ("re-observed"). They are labelled distinctly so the trail says which happened.
   *
   * Written on every path that releases the epoch cleanly — completion and park alike — so the trail
   * accounts for every DataPoint. Appending clears the counters and an empty tally appends nothing,
   * so a second call on the same run is a no-op. In-memory until then: a run that dies before
   * appending loses the counts, not the DataPoints, which are durable in the store either way — the
   * trade this coalescing exists to make.
   */
  private async appendCoalescedAudits(epoch: Epoch): Promise<void> {
    const counted: { readonly dataPointType: string; readonly count: number; readonly what: string }[] = [
      ...sortedTally(this.coalescedAudits).map(({ key, count }) => ({
        dataPointType: key,
        count,
        what: 'merged',
      })),
      ...sortedTally(this.reobservedAudits).map(({ key, count }) => ({
        dataPointType: key,
        count,
        what: 're-observed',
      })),
    ];
    if (counted.length === 0) {
      return;
    }
    const entries = counted.map(({ dataPointType, count, what }) =>
      makeAuditLogEntry({
        sessionId: this.sessionId,
        namespaceId: this.namespaceId,
        epoch,
        timestamp: this.runtime.clock.now(),
        operatorId: null,
        kind: AuditKind.DATA_POINTS_COALESCED,
        dataPoint: DataPointAuditInfo({ dataPointType, summary: `${count} ${what}` }),
      }),
    );
    this.coalescedAudits.clear();
    this.reobservedAudits.clear();
    await this.appendAuditEntries(entries);
  }

  /**
   * Flush the write-behind archive. Idempotent — a repeat flush is a no-op.
   *
   * The audit is not flushed: its appends are durable where `replay` reads them, so the trail needs
   * nothing from the tail.
   */
  private async flushWriteBehind(): Promise<void> {
    const archiveEntries = await this.runtime.archive.flush(this.sessionId);
    this.runtime.telemetry.archiveFlushEntries.record(archiveEntries);
  }

  private async prepareRedrive(): Promise<void> {
    // A late inbox entry arrived during aggregate/flush. Re-drive on the SAME epoch: clear each
    // aggregator's contribution marker so it re-runs and folds the new data, and grant a fresh drain
    // budget so a re-drive after a deadline-hit completion isn't instantly starved (which would spin).
    for (const aggregator of this.aggregators) {
      await this.runtime.durable.clearContribution(this.sessionId, aggregator.operatorId);
    }
    this.freshDeadline = true; // honored by `remainingSessionBudget` on the next gather pass
  }

  /**
   * Apply per-namespace operator gating for this run, mirroring the capability semantics.
   *
   * The catalog decides; a config change is visible to the next grant, never to a run in flight.
   * Only the operator lists the loop works from are filtered, so an excluded operator never launches
   * and never counts for readiness, quiescence or graph-stall warnings. The ctor-validated full
   * graph is left alone: removing nodes cannot create a cycle, so the unbounded-cycle validation
   * done at construction still holds. Aggregators are operators too — a namespace that gates one
   * accepts the missing-output disposition a never-ready aggregator already has (its durable domain
   * simply isn't written).
   */
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
    this.gathering = this.gathering.filter((operator) => permitted.has(operator.operatorId));
    this.aggregators = this.aggregators.filter((operator) => permitted.has(operator.operatorId));
    getLogger().info('Operators excluded from this run: not permitted for the namespace', {
      [LOGGER_NAME_FIELD]: LOGGER_NAME,
      session_id: this.sessionId,
      namespace_id: this.namespaceId,
      excluded_operator_ids: excluded,
    });
  }

  /**
   * Compare this spawn's flow fingerprint against the one persisted for the session.
   *
   * Drift is *detected*, never pinned: the run continues under the current flow, because the
   * idempotent dataflow model (keyed-merge, watermarks, contribution markers) tolerates a changed
   * operator set far better than a replay-based engine would — the WARNING and the audit entry make
   * the change visible instead of blocking the session. The current fingerprint is persisted right
   * away, so later resumes with the same changed flow stay quiet. A directly-constructed
   * orchestrator with no flow identity skips detection.
   */
  private async detectFlowDrift(epoch: Epoch): Promise<void> {
    if (this.flowIdentity === null) {
      return;
    }
    const stored = await this.runtime.store.getFlowFingerprint(this.sessionId);
    const current = this.flowIdentity.fingerprint;
    if (stored === current) {
      return;
    }
    if (stored !== null) {
      getLogger().warning('Flow definition drift detected on spawn; continuing under the current flow', {
        [LOGGER_NAME_FIELD]: LOGGER_NAME,
        session_id: this.sessionId,
        flow_name: this.flowIdentity.name,
        stored_fingerprint: stored,
        current_fingerprint: current,
      });
      await this.runtime.audit.append(
        makeAuditLogEntry({
          sessionId: this.sessionId,
          namespaceId: this.namespaceId,
          epoch,
          timestamp: this.runtime.clock.now(),
          kind: AuditKind.FLOW_DRIFT_DETECTED,
          flow: FlowAuditInfo({
            flowName: this.flowIdentity.name,
            storedFingerprint: stored,
            currentFingerprint: current,
          }),
        }),
      );
    }
    await this.runtime.store.setFlowFingerprint(this.sessionId, current, { epoch });
  }

  private buildCircuitBreaker(): CircuitBreaker {
    const caps = new Map<OperatorId, number>();
    for (const cycle of findCycles(buildGraph(this.gathering, this.capabilities))) {
      for (const node of cycle) {
        if (isSubclass(node, Operator)) {
          const operator = node as OperatorClass;
          if (operator.policy.maxCycles !== null) {
            caps.set(operator.operatorId, operator.policy.maxCycles);
          }
        }
      }
    }
    return new CircuitBreaker(caps);
  }

  /**
   * The bounded signal queue between operator tasks and the gathering loop.
   *
   * Bounding it is the backpressure boundary: {@link Orchestrator.drain} awaits its puts, so a full
   * queue suspends the emitting operator until the loop drains, instead of letting a runaway
   * streamer grow the heap without limit. The deliberate consequences:
   *
   * - A suspended emitter's per-operation timeout keeps ticking — that timeout is the bound on an
   *   operator whose loop genuinely cannot drain (a wedged consumer never turns into an unbounded
   *   buffer OR an immortal producer).
   * - `completed` puts are awaited too, so a completion signal can be delayed by a full queue but
   *   never lost — the loop always learns every run's disposition.
   * - On a deadline-cancel, a task blocked in `put` gives up exactly like one blocked in its own
   *   awaits: emissions already merged are kept, the rest die with the run.
   *
   * @internal Overridable (and callable) exactly as Python's `_build_emission_queue` is: a test
   * substitutes an instrumented queue of the same bound, or reads the bound off the queue a spawn
   * would build. It is not part of the surface an embedding flow uses.
   */
  public buildEmissionQueue(): BoundedQueue<Signal> {
    return new BoundedQueue<Signal>(this.emissionQueueSize);
  }

  /** Run the gathering loop; `true` means the session parked instead of going quiescent. */
  private async gather(epoch: Epoch, activator: CapabilityActivator, breaker: CircuitBreaker): Promise<boolean> {
    const clock = this.runtime.clock;
    const debounce = new DebounceController(clock);
    // Retries reuse the same due-time mechanics as debounced reruns: the loop (not the failed task)
    // owns the backoff window, so nothing sleeps in-line past the lock TTL.
    const retries = new DebounceController(clock);
    // The loop keeps its monotonic arithmetic, but the budget it counts down is the REMAINING share
    // of the persisted wall-clock deadline — an exhausted budget never enters the loop and goes
    // straight to aggregation, the same disposition as an in-loop deadline hit.
    const deadline = clock.monotonic() + (await this.remainingSessionBudget(epoch));
    const queue = this.buildEmissionQueue();
    const running = new Map<OperatorId, Running>();
    let lastRenew = clock.monotonic();
    // Rehydrate each operator's watermark once (this is how a resume knows what already ran). The
    // orchestrator is the session's sole watermark writer while it holds the epoch, so the cache
    // stays authoritative and the hot loop never re-reads watermarks from the store.
    this.watermarks = new Map();
    for (const operator of this.gathering) {
      this.watermarks.set(
        operator.operatorId,
        await this.runtime.store.getWatermark(this.sessionId, operator.operatorId),
      );
    }
    // A rehydrated watermark can predate the mirror's snapshot (a resume); the added/updated split
    // for those revisions lives only in the store, so each distinct one is primed here once and
    // every later delta read stays local.
    const marks = [...new Set([...this.watermarks.values()].filter((mark): mark is Revision => mark !== null))].sort(
      (left, right) => left - right,
    );
    for (const watermark of marks) {
      await this.mirror.primeChangeBaseline(watermark);
    }
    let parked = false;
    let quiescent = false;
    try {
      while (clock.monotonic() < deadline) {
        lastRenew = await this.renewLeaseIfDue(epoch, lastRenew);
        await this.drainInbox(epoch);
        const view = await this.mirror.snapshot(this.sessionId);
        const observed = await this.mirror.revision(this.sessionId); // the revision `view` reflects
        const capabilities = await this.refreshCapabilities(activator, view, epoch);
        // Evaluated once per pass and shared with the plan on purpose: "may an interim window be
        // abandoned" and "may the loop break to aggregation" are the same question asked twice, and
        // a pass that answered them against different views could drop a refold and then go on to
        // wait rather than finalize.
        const completionSatisfied = this.completesWhen === null || this.completesWhen.isSatisfied(view);
        const plan = await this.plan(view, capabilities, breaker, debounce, retries, running, {
          completionSatisfied,
        });
        for (const runnable of plan.runnable) {
          getLogger().debug('Launching operator for the current gathering iteration', {
            [LOGGER_NAME_FIELD]: LOGGER_NAME,
            session_id: this.sessionId,
            operator_id: runnable.operator.operatorId,
            is_rerun: runnable.isRerun,
            is_retry: runnable.isRetry,
          });
          // Record prevCaps + the observed revision at launch: both must reflect what this run saw,
          // and by completion the live store/capability set will have moved on.
          this.prevCaps.set(runnable.operator.operatorId, capabilities.availableIds());
          const handle = new RunHandle();
          const promise = this.runOne(runnable.operator, runnable.delta, epoch, view, capabilities, queue, handle);
          running.set(runnable.operator.operatorId, { promise, handle, observedRevision: observed });
          if (runnable.isRetry) {
            retries.clear(runnable.operator.operatorId); // consume the armed retry
          } else if (runnable.isRerun) {
            debounce.clear(runnable.operator.operatorId); // consume the coalesced rerun
            this.runtime.telemetry.operatorRerunsTotal.add(1, { operator_id: runnable.operator.operatorId });
          }
        }
        if (running.size > 0) {
          // Block on the first available signal — an emission to merge or an operator's completion —
          // then drain every signal already queued behind it, and re-plan once for the whole batch.
          // Nothing is delayed (each drained signal was produced before the re-plan), so eagerness is
          // intact; planning once on the superset launches everything any individual signal would
          // have, without a snapshot per emission. No wall-clock timeout: operators always finish (a
          // per-op timeout bounds them), and a debounce window is only fast-forwarded (via the
          // injected clock) once nothing runs.
          const first = await this.nextSignal(queue);
          const signals = first === null ? drainSignals(queue) : [first, ...drainSignals(queue)];
          await this.consumeBatch(signals, epoch, breaker, retries, running);
          continue;
        }
        if (plan.nextDueInMs !== null) {
          await clock.sleep(plan.nextDueInMs); // fast-forward to the soonest rerun window
          continue;
        }
        if (completionSatisfied) {
          this.warnIfStalled(plan);
          getLogger().debug('Gathering quiescent; proceeding to aggregation', {
            [LOGGER_NAME_FIELD]: LOGGER_NAME,
            session_id: this.sessionId,
          });
          quiescent = true;
          break; // nothing runnable, nothing pending, and no further wait declared → aggregate
        }
        // The flow declares a completion condition that is not satisfied yet, and nothing can make
        // progress from within the session — wait for mid-session input (a user action, a webhook)
        // to arrive on the inbox, bounded by the session deadline, then re-plan. The idle window
        // restarts here on every wait: an inbox arrival is activity, so only *continuous* idleness
        // can park the session.
        const wait = await this.waitForInbox(epoch, deadline, lastRenew, this.parkAt());
        lastRenew = wait.lastRenew;
        if (wait.parked) {
          parked = true;
          break;
        }
      }
      if (!quiescent && !parked) {
        // Falling out of the loop condition (never via a break) is exactly a deadline hit —
        // including a resume whose persisted budget was already spent and never entered the loop.
        this.runtime.telemetry.sessionDeadlineHitsTotal.add(1);
        getLogger().warning('Session deadline hit; gathering stopped and any in-flight operators will be cancelled', {
          [LOGGER_NAME_FIELD]: LOGGER_NAME,
          session_id: this.sessionId,
          in_flight_operator_ids: [...running.keys()].map(String).sort(),
        });
      }
    } finally {
      await this.drainRemaining(queue, running, epoch);
      queue.close();
    }
    return parked;
  }

  /**
   * The next queued signal, or `null` when the queue closed under the loop.
   *
   * It begins by yielding the event loop once. Python's operator tasks run their whole
   * non-suspending burst before the gathering task is resumed, so a fast operator's emissions *and*
   * its completion arrive as one batch; JavaScript hands the loop its continuation on the first
   * `put` instead, which would split that burst across passes — merging one emission at a time, and
   * costing a store apply and a re-plan per emission where Python pays one per burst.
   */
  private async nextSignal(queue: BoundedQueue<Signal>): Promise<Signal | null> {
    await yieldToEventLoop();
    const queued = queue.getNowait();
    if (queued !== undefined) {
      return queued;
    }
    return (await queue.get()) ?? null;
  }

  private warnIfStalled(plan: GatherPlan): void {
    // Defensive invariant guard: with nothing running and nothing due, no pending operator should
    // still have a producible-input path — if one does, the dependency graph and the scheduler
    // disagree, and the operators named here silently never ran.
    const stuck = reachablePending(plan.notYetRun, {
      presentTypes: plan.presentTypes,
      availableCapabilityTypes: plan.availableTypes,
    });
    if (stuck.size > 0) {
      getLogger().warning(
        'Gathering stopped while pending operators could still become ready; check the dependency graph',
        {
          [LOGGER_NAME_FIELD]: LOGGER_NAME,
          session_id: this.sessionId,
          stuck_operator_ids: [...stuck].map((operator) => String(operator.operatorId)).sort(),
        },
      );
    }
  }

  /**
   * Block until the inbox signals a new entry; returns the renew bookkeeping + park verdict.
   *
   * Event-driven, not polling: one long-lived push subscription (`Inbox.waitForEntry`) is raced
   * against a renew-cadence timer on the injected clock, so the lease stays alive across an
   * arbitrarily long wait and tests stay deterministic. The wait ends at the earliest of: an inbox
   * wakeup (re-plan), the session deadline (the gather loop exits and aggregation runs, exactly like
   * a deadline hit with operators in flight), or `parkAt` (the session parks — holding a task, a
   * subscription and a renewing lease for an hours-long human-in-the-loop wait would waste the pod).
   * A wakeup landing in the same pass as an elapsed park window wins: data beats parking. A fenced
   * renew raises `StaleEpochError` out of here, which `run` turns into a clean SUPERSEDED stop.
   */
  private async waitForInbox(
    epoch: Epoch,
    deadline: number,
    lastRenew: number,
    parkAt: number | null,
  ): Promise<InboxWait> {
    getLogger().debug('Waiting on the inbox for mid-session input', {
      [LOGGER_NAME_FIELD]: LOGGER_NAME,
      session_id: this.sessionId,
    });
    // A promise cannot be cancelled, so the wait is given up through an `AbortSignal` instead: the
    // loop tracks whether it has resolved, and every exit path aborts, which is what Python's
    // `finally: waiter.cancel()` does. The signal is not a nicety — the Redis waiter holds a
    // dedicated pub/sub connection it releases only when its own await ends, so a deadline or a park
    // that merely walked away would strand one connection per session. Its rejection is captured
    // here so an adapter failure after the wait ended never surfaces as an unhandled rejection.
    let settled = false;
    let failure: unknown = null;
    const abandon = new AbortController();
    const waiter = this.runtime.inbox
      .waitForEntry(this.sessionId, abandon.signal)
      .then(() => {
        settled = true;
      })
      .catch((error: unknown) => {
        settled = true;
        failure = error ?? new Error('the inbox wait failed without an error value');
      });
    let renewedAt = lastRenew;
    // The span makes the (potentially very long) wait visible in the trace; its outcome attribute
    // says what ended it. A fenced renew or a failed waiter raises through it.
    try {
      return await withSpan(this.runtime.telemetry.tracer, 'session.inbox_wait', async (span) => {
        while (!settled) {
          const now = this.runtime.clock.monotonic();
          if (now >= deadline) {
            span.setAttribute('outcome', 'deadline');
            return { lastRenew: renewedAt, parked: false };
          }
          if (parkAt !== null && now >= parkAt) {
            span.setAttribute('outcome', 'parked');
            return { lastRenew: renewedAt, parked: true };
          }
          const nextRenewIn = Math.max(this.leaseRenewIntervalMs - (now - renewedAt), 0);
          let bound = Math.min(nextRenewIn, deadline - now);
          if (parkAt !== null) {
            bound = Math.min(bound, parkAt - now);
          }
          // The loser of the race is abandoned, not left running: Python cancels the task it raced,
          // and without the abort a wakeup that beat the window would leave a real timer pending for
          // the window's full width on every pass of a long wait.
          const window = new AbortController();
          try {
            await Promise.race([waiter, this.runtime.clock.sleep(bound, window.signal)]);
          } finally {
            window.abort();
          }
          renewedAt = await this.renewLeaseIfDue(epoch, renewedAt);
        }
        // Propagate adapter errors — a failed wait must not be mistaken for a wakeup.
        if (failure !== null) {
          throw failure;
        }
        span.setAttribute('outcome', 'wakeup');
        getLogger().debug('Inbox wakeup received; resuming the gathering loop', {
          [LOGGER_NAME_FIELD]: LOGGER_NAME,
          session_id: this.sessionId,
        });
        return { lastRenew: renewedAt, parked: false };
      });
    } finally {
      abandon.abort();
    }
  }

  private parkAt(): number | null {
    // Parking is measured only from entering an inbox wait (continuous idleness). Armed
    // retry/debounce windows go through the `nextDueInMs` sleep path instead — they are scheduled
    // progress, never idleness, so they can never park the session.
    return this.parkAfterMs === null ? null : this.runtime.clock.monotonic() + this.parkAfterMs;
  }

  /**
   * Milliseconds left of the session's persisted wall-clock deadline (the first gather persists it).
   *
   * Persisting the deadline (rather than restarting `sessionDeadlineMs` per process) is what gives a
   * repeatedly-parked or crash-looping session one overall budget: every resume continues a
   * shrinking window, and a resume after the deadline passed gets a non-positive budget — straight
   * to aggregation.
   *
   * When `freshDeadline` is set (a re-open for late data), the persisted deadline is discarded and a
   * new full budget is written. The original deadline is almost certainly elapsed (a budgeted run
   * finishes in seconds, and late participant data arrives much later), so rehydrating it would give
   * a non-positive budget, which would log a spurious deadline hit and skip the gather loop.
   * Gathering IS already done (the completion condition is satisfied in the rehydrated store), so
   * the gather loop exits immediately on quiescence, not on the deadline — the fresh budget is
   * needed only to ensure the aggregation phase has a live lease window, not to enable any actual
   * gathering.
   */
  private async remainingSessionBudget(epoch: Epoch): Promise<number> {
    const stored = await this.runtime.store.getSessionDeadline(this.sessionId);
    if (stored === null || this.freshDeadline) {
      const deadline = new Date(this.runtime.clock.now().getTime() + this.sessionDeadlineMs);
      await this.runtime.store.setSessionDeadline(this.sessionId, deadline, { epoch });
      return this.sessionDeadlineMs;
    }
    return stored.getTime() - this.runtime.clock.now().getTime();
  }

  private async consumeBatch(
    signals: readonly Signal[],
    epoch: Epoch,
    breaker: CircuitBreaker,
    retries: DebounceController,
    running: Map<OperatorId, Running>,
  ): Promise<void> {
    // ONE mirror/store apply for every emission in the drained batch (arrival order preserved, so
    // the merge resolution is the one per-signal merging would produce), then the completion
    // bookkeeping. Processing completions after the merge cannot change any outcome: a completion
    // never reads the live snapshot — its watermark is the launch-observed revision — and an
    // operator's own emissions always precede its completion on the queue anyway.
    await this.mergeEmissions(
      signals.filter((signal): signal is Emitted => signal.kind === 'emitted'),
      epoch,
    );
    for (const signal of signals) {
      if (signal.kind === 'completed') {
        await this.recordCompletion(signal, epoch, breaker, retries, running);
      }
    }
  }

  private async recordCompletion(
    signal: Completed,
    epoch: Epoch,
    breaker: CircuitBreaker,
    retries: DebounceController,
    running: Map<OperatorId, Running>,
  ): Promise<void> {
    const record = running.get(signal.operatorId);
    running.delete(signal.operatorId);
    this.runs.set(signal.operatorId, (this.runs.get(signal.operatorId) ?? 0) + 1);
    // Every attempt counts toward the breaker: it bounds cycle iterations, and a retried run on a
    // cycle still consumes that budget — otherwise a failing cycle operator could loop between the
    // retry scheduler and the cycle forever.
    breaker.recordRun(signal.operatorId);
    const attempt = (this.failedAttempts.get(signal.operatorId) ?? 0) + 1;
    let retrying = false;
    if (signal.error === null) {
      this.failedAttempts.delete(signal.operatorId);
      getLogger().debug('Operator run succeeded', {
        [LOGGER_NAME_FIELD]: LOGGER_NAME,
        session_id: this.sessionId,
        operator_id: signal.operatorId,
        run_count: this.runs.get(signal.operatorId) ?? 0,
      });
    } else {
      this.failedAttempts.set(signal.operatorId, attempt);
      retrying = this.maybeScheduleRetry(signal.operatorId, breaker, retries);
      if (!retrying) {
        // Terminal — a later data-driven rerun starts a fresh attempt sequence.
        this.failedAttempts.delete(signal.operatorId);
      }
      this.logRunFailure(signal.operatorId, signal.error, attempt, retrying);
    }
    const outcome = signal.error === null ? OperatorOutcome.SUCCEEDED : OperatorOutcome.FAILED;
    await this.auditOperatorRun(signal.operatorId, epoch, {
      outcome,
      runCount: this.runs.get(signal.operatorId) ?? 0,
      attempt,
      runSeconds: signal.runSeconds,
      error: signal.error,
    });
    if (retrying) {
      // A retried failure keeps its old watermark: the failed attempt never durably processed its
      // delta, so the relaunch must re-present the same changes. Its emissions are already merged —
      // the keyed-merge makes re-emitting them idempotent — and the armed retry (not the rerun path,
      // which stands aside while one is armed) owns the relaunch, so the stale watermark cannot
      // double-trigger.
      return;
    }
    if (record === undefined) {
      // Unreachable: a launch registers its record before the run can put anything on the queue. It
      // is checked rather than asserted because the watermark is the one thing this method writes,
      // and writing the live revision instead of the launch-observed one would silently swallow data.
      return;
    }
    // Advance to the revision this run *observed* (its launch snapshot), not the live one: DataPoints
    // merged by concurrent operators or the inbox while it ran were never seen by it, so they must
    // still count as new data and be able to trigger a rerun.
    await this.advanceWatermark(signal.operatorId, record.observedRevision, epoch);
  }

  /** Arm a loop-scheduled relaunch of a failed run; `false` when the failure is terminal. */
  private maybeScheduleRetry(operatorId: OperatorId, breaker: CircuitBreaker, retries: DebounceController): boolean {
    const policy = this.operatorsById.get(operatorId)?.policy.retry ?? null;
    const failed = this.failedAttempts.get(operatorId) ?? 0;
    // A tripped breaker is terminal even with attempts remaining — an armed retry could never launch.
    if (policy === null || failed >= policy.maxAttempts || breaker.isTripped(operatorId)) {
      return false;
    }
    const delayMs = backoffDelays(policy, { seed: seedFor(this.sessionId, operatorId) })[failed - 1] ?? 0;
    retries.schedule(operatorId, { windowMs: delayMs });
    this.runtime.telemetry.operatorRetriesTotal.add(1, { operator_id: operatorId });
    return true;
  }

  private logRunFailure(operatorId: OperatorId, error: unknown, attempt: number, retrying: boolean): void {
    if (retrying) {
      getLogger().warning('Operator run failed; a retry is scheduled on backoff', {
        [LOGGER_NAME_FIELD]: LOGGER_NAME,
        operator_id: operatorId,
        attempt,
        error,
      });
      return;
    }
    if ((this.operatorsById.get(operatorId)?.policy.retry ?? null) !== null) {
      getLogger().warning('Operator run failed with no retry remaining; the scheduler is proceeding without it', {
        [LOGGER_NAME_FIELD]: LOGGER_NAME,
        operator_id: operatorId,
        attempt,
        error,
      });
      return;
    }
    getLogger().error('Operator run failed; its emissions were persisted and the scheduler is proceeding', {
      [LOGGER_NAME_FIELD]: LOGGER_NAME,
      operator_id: operatorId,
      error,
    });
  }

  private async drainRemaining(
    queue: BoundedQueue<Signal>,
    running: Map<OperatorId, Running>,
    epoch: Epoch,
  ): Promise<void> {
    // Normal quiescent exit reaches here with both already empty (a no-op). The other exit is the
    // session deadline with operators still in flight: persist whatever was already emitted, then
    // cancel the stragglers — the deadline is the one signal allowed to stop a running operator.
    await this.drainQueue(queue, epoch);
    for (const record of running.values()) {
      record.handle.cancel();
    }
    if (running.size > 0) {
      await Promise.all([...running.values()].map((record) => record.promise));
    }
    // Drain once more: a run can enqueue a final emission right as it is cancelled, and that signal
    // (already past the epoch-guarded merge boundary) would otherwise be dropped.
    await this.drainQueue(queue, epoch);
  }

  private async drainQueue(queue: BoundedQueue<Signal>, epoch: Epoch): Promise<void> {
    const emitted = drainSignals(queue).filter((signal): signal is Emitted => signal.kind === 'emitted');
    await this.mergeEmissions(emitted, epoch);
  }

  private async plan(
    view: DataPointView,
    capabilities: CapabilityView,
    breaker: CircuitBreaker,
    debounce: DebounceController,
    retries: DebounceController,
    running: Map<OperatorId, Running>,
    proximity: { readonly completionSatisfied: boolean },
  ): Promise<GatherPlan> {
    const presentTypes = new Set<DataPointClass<AnyDataPoint>>(
      view.all().map((dataPoint) => dataPoint.constructor as DataPointClass<AnyDataPoint>),
    );
    const availableTypes = capabilities.availableTypes();
    const runnable: Runnable[] = [];
    const notYetRun: OperatorClass[] = [];
    let soonestDue: number | null = null;
    const track = (dueAt: number | null): void => {
      if (dueAt !== null) {
        soonestDue = soonestDue === null ? dueAt : Math.min(soonestDue, dueAt);
      }
    };
    for (const operator of this.gathering) {
      const operatorId = operator.operatorId;
      if (running.has(operatorId)) {
        continue; // in flight — neither launch a second instance nor count it as pending-unstarted
      }
      if (breaker.isTripped(operatorId)) {
        continue;
      }
      // "Has run" spans this session's history (a persisted watermark, rehydrated into the cache at
      // gather start), so on resume a completed operator is not re-run unless its policy makes it
      // rerun-eligible.
      const ran = this.runs.has(operatorId) || (this.watermarks.get(operatorId) ?? null) !== null;
      if (!ran) {
        notYetRun.push(operator);
      }
      if (!isReady(operator, { presentTypes, availableCapabilityTypes: availableTypes })) {
        continue;
      }
      if (retries.isScheduled(operatorId)) {
        // An armed retry owns this operator's next launch: it relaunches even with no new data and
        // even when the policy never reruns on data (its watermark is deliberately stale), and
        // quiescence must wait for it to come due rather than break to aggregation.
        if (retries.isDue(operatorId)) {
          const delta = await this.deltaFor(operator, capabilities);
          runnable.push({ operator, delta, isRerun: false, isRetry: true });
        } else {
          track(retries.dueAt(operatorId));
        }
        continue;
      }
      if (!ran) {
        const delta = await this.deltaFor(operator, capabilities);
        runnable.push({ operator, delta, isRerun: false, isRetry: false }); // first run is immediate
        continue;
      }
      if (!operator.policy.rerunOnNewData) {
        continue; // ran already and never reruns
      }
      const delta = await this.deltaFor(operator, capabilities);
      const hasNew = hasRelevantNewData(operator, delta);
      if (!rerunEligible(operator.policy, { hasRelevantNewData: hasNew })) {
        continue;
      }
      // Rerun-eligible: arm a coalescing window (arrivals within it collapse into one rerun) and
      // launch only once it is due.
      if (!debounce.isScheduled(operatorId)) {
        debounce.schedule(operatorId, { windowMs: operator.policy.debounceMs });
      }
      if (debounce.isDue(operatorId)) {
        runnable.push({ operator, delta, isRerun: true, isRetry: false });
      } else if (!windowDefersToFinalize(operator, proximity)) {
        // An armed window normally holds gathering open until it comes due — otherwise the coalescing
        // would be advisory, and a burst arriving with nothing else running would be abandoned rather
        // than collapsed. The one exemption is an interim refold the imminent finalize pass would
        // immediately overwrite: it stays armed and launchable (a later pass with work still in
        // flight runs it once it is due), it just no longer keeps the loop waiting on a write nothing
        // can read.
        track(debounce.dueAt(operatorId));
      }
    }
    const nextDueInMs = soonestDue === null ? null : Math.max(0, soonestDue - this.runtime.clock.monotonic());
    return { runnable, notYetRun, nextDueInMs, presentTypes, availableTypes };
  }

  private async deltaFor(operator: OperatorClass, capabilities: CapabilityView): Promise<InvocationDelta> {
    return await operatorDelta(this.mirror, this.sessionId, {
      watermark: this.watermarks.get(operator.operatorId) ?? null,
      availableCaps: capabilities.availableIds(),
      previousCaps: this.prevCaps.get(operator.operatorId) ?? new Set<CapabilityId>(),
    });
  }

  private async runOne(
    operator: ConcreteOperatorClass,
    delta: InvocationDelta,
    epoch: Epoch,
    view: DataPointView,
    capabilities: CapabilityView,
    queue: BoundedQueue<Signal>,
    handle: RunHandle,
  ): Promise<void> {
    const aggregation = isSubclass(operator, Aggregator)
      ? new AggregationHelpers(this.runtime.durable, {
          sessionId: this.sessionId,
          operatorId: operator.operatorId,
          epoch,
          clock: this.runtime.clock,
          isFinal: false,
        })
      : null;
    const context = new OperatorContext({
      sessionId: this.sessionId,
      epoch,
      store: view,
      capabilities,
      delta,
      effects: this.effectGuard(operator.operatorId, epoch),
      signal: handle.signal,
      aggregation,
      isFinal: false,
    });
    let error: unknown = null;
    const started = this.runtime.clock.monotonic();
    await withSpan(
      this.runtime.telemetry.tracer,
      `operator.run ${operator.operatorId}`,
      async (span) => {
        try {
          await withTimeout(this.drain(operator, context, handle, queue), this.timeoutFor(operator));
        } catch (thrown) {
          // Operator fault-isolation boundary — never wedge the session.
          error = thrown ?? new Error('the operator run failed without an error value');
          // `withTimeout` stops waiting; only this stops the operator, which Python's `wait_for` does
          // by cancelling the coroutine it was waiting on.
          handle.stop();
        }
        const outcome = error !== null ? OperatorOutcome.FAILED : OperatorOutcome.SUCCEEDED;
        span.setAttribute('outcome', outcome);
        if (error !== null) {
          // The failure is isolated (it never propagates out of this run), so the span is marked
          // failed explicitly — `withSpan` will see no exception.
          span.recordException(error instanceof Error ? error : String(error));
          span.setStatus({ code: SpanStatusCode.ERROR, message: failureText(error) });
        }
      },
      { attributes: { operator_id: operator.operatorId } },
    );
    if (handle.cancelled) {
      // The deadline took this run away: Python's cancelled task never reaches its bookkeeping
      // either, so the run is neither counted nor audited — only what it emitted is kept.
      return;
    }
    const runSeconds = secondsSince(started, this.runtime.clock.monotonic());
    const outcome = error !== null ? OperatorOutcome.FAILED : OperatorOutcome.SUCCEEDED;
    this.observeRunSeconds(operator.operatorId, runSeconds, outcome);
    await queue.put({ kind: 'completed', operatorId: operator.operatorId, error, runSeconds });
  }

  /**
   * Stream one run's emissions onto the queue as they are produced.
   *
   * Provenance and observation time are stamped here, inside the fault-isolation boundary: operators
   * emit value-only, so finalizing constructs the DataPoint and can throw on a malformed value —
   * that must be isolated to this operator, not propagate out and wedge the session. Each finalized
   * emission is streamed onto the queue as it is produced, so the loop can merge it and start
   * downstream operators while this one is still running. The observation time is taken per
   * emission, so a long-running operator's later yields carry their actual time.
   */
  private async drain(
    operator: ConcreteOperatorClass,
    context: OperatorContext,
    handle: RunHandle,
    queue: BoundedQueue<Signal>,
  ): Promise<void> {
    // Construct inside the boundary too: an operator constructor that throws (e.g. config
    // validation) must be recorded as a FAILED run — not drop the task before it enqueues its
    // completion, which would leave it wedged in `running` with no log/audit/breaker.
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
      // A hand-off parked on a full queue is abandoned when the run is stopped, exactly as a task
      // blocked in `put` dies on cancellation: what is already queued is kept, this emission is not.
      await Promise.race([
        queue.put({ kind: 'emitted', operatorId: operator.operatorId, dataPoint: finalized }),
        handle.whenStopped,
      ]);
    }
  }

  private timeoutFor(operator: OperatorClass): number {
    // A wrapped streaming collector legitimately runs far longer than a quick scoring operator, so
    // the policy may override the orchestrator-wide bound in either direction.
    return operator.policy.timeoutMs ?? this.operationTimeoutMs;
  }

  private effectGuard(operatorId: OperatorId, epoch: Epoch): EffectGuard {
    // Bound to the LIVE store (not the launch snapshot): an effect mark must be durable and
    // epoch-fenced the instant the operator claims it, even while the run is still streaming.
    return new EffectGuard(this.runtime.store, { sessionId: this.sessionId, operatorId, epoch });
  }

  private async aggregate(epoch: Epoch, activator: CapabilityActivator): Promise<void> {
    await this.runtime.lock.renew(this.sessionId, { epoch }); // hold ownership across the aggregation phase
    const view = await this.mirror.snapshot(this.sessionId);
    const capabilities = await this.refreshCapabilities(activator, view, epoch);
    const presentTypes = new Set<DataPointClass<AnyDataPoint>>(
      view.all().map((dataPoint) => dataPoint.constructor as DataPointClass<AnyDataPoint>),
    );
    const availableTypes = capabilities.availableTypes();
    const launchable: ConcreteOperatorClass[] = [];
    for (const aggregator of this.aggregators) {
      const gap = readinessGap(aggregator, { presentTypes, availableCapabilityTypes: availableTypes });
      if (!gap.isReady) {
        // A never-ready aggregator means a whole output domain was not written — that must be
        // visible in the logs and the audit trail, not just silently absent.
        await this.reportSkippedAggregator(aggregator, epoch, gap);
        continue;
      }
      // Resume skips already-completed aggregators: the contribution marker makes re-drives idempotent.
      if (await this.runtime.durable.isContributionMarked(this.sessionId, aggregator.operatorId)) {
        // Logged because it is the one path that completes a session having run NO finalize this
        // epoch: whatever the predecessor committed is the record's last word, so when a record looks
        // wrong on a resumed session this is the first thing to check.
        getLogger().debug('Skipping an aggregator that already contributed; its predecessor output stands', {
          [LOGGER_NAME_FIELD]: LOGGER_NAME,
          session_id: this.sessionId,
          operator_id: aggregator.operatorId,
          epoch,
        });
        continue;
      }
      launchable.push(aggregator);
    }
    getLogger().debug('Aggregation phase started; launching ready aggregators concurrently', {
      [LOGGER_NAME_FIELD]: LOGGER_NAME,
      session_id: this.sessionId,
      aggregator_ids: launchable.map((aggregator) => String(aggregator.operatorId)).sort(),
    });
    // Aggregators are independent by design, so they run concurrently — one waiting out a retry
    // backoff must not delay its peers. The `deadLetters`/`runs` mutations need no locking:
    // everything shares the single event loop and each mutation is one non-awaiting step.
    const results = await Promise.allSettled(
      launchable.map(async (aggregator) => await this.runAggregator(aggregator, epoch, view, capabilities)),
    );
    // Failures are collected (never lost to sibling cancellation, and every sibling ran to its own
    // disposition); a fencing signal wins the re-raise so `run()` makes its clean SUPERSEDED stop.
    const failures = results
      .filter((result): result is PromiseRejectedResult => result.status === 'rejected')
      .map((result) => result.reason as unknown);
    if (failures.length > 0) {
      const fenced = failures.find((failure) => failure instanceof StaleEpochError);
      throw fenced ?? failures[0];
    }
  }

  private async reportSkippedAggregator(
    aggregator: ConcreteOperatorClass,
    epoch: Epoch,
    gap: ReadinessGap,
  ): Promise<void> {
    const missingData = gap.missingDataPoints.map((required) => required.name);
    const missingCaps = gap.missingCapabilities.map((required) => required.name);
    getLogger().warning('Aggregator skipped: its dependencies never materialized; no durable output was written', {
      [LOGGER_NAME_FIELD]: LOGGER_NAME,
      operator_id: aggregator.operatorId,
      missing_data_points: missingData,
      missing_capabilities: missingCaps,
    });
    await this.auditOperatorRun(aggregator.operatorId, epoch, {
      outcome: OperatorOutcome.SKIPPED,
      runCount: 0,
      error: `dependencies never materialized: ${[...missingData, ...missingCaps].join(', ')}`,
    });
  }

  /**
   * Run one aggregator with bounded, jittered-backoff retries; dead-letter when they are exhausted.
   *
   * A method (rather than a closure inside the loop) gives `attempt`/`afterAttempt` a clean
   * per-aggregator scope. Backoff sleeps go through the injected clock, so retry timing is
   * deterministic in tests and uses no wall clock. Each attempt is timeout-bounded, and the lease is
   * renewed after every attempt.
   */
  private async runAggregator(
    aggregator: ConcreteOperatorClass,
    epoch: Epoch,
    view: DataPointView,
    capabilities: CapabilityView,
  ): Promise<void> {
    const delta = await operatorDelta(this.mirror, this.sessionId, {
      // Aggregators never advance a watermark, so their delta is always first-invocation.
      watermark: null,
      availableCaps: capabilities.availableIds(),
      previousCaps: new Set<CapabilityId>(),
    });
    const helpers = new AggregationHelpers(this.runtime.durable, {
      sessionId: this.sessionId,
      operatorId: aggregator.operatorId,
      epoch,
      clock: this.runtime.clock,
      isFinal: true,
    });
    const effects = this.effectGuard(aggregator.operatorId, epoch);
    // One context per attempt, differing only in its cancellation signal: an attempt that timed out
    // has had its signal aborted, and the relaunch that follows must not inherit it.
    const contextFor = (handle: RunHandle): OperatorContext =>
      new OperatorContext({
        sessionId: this.sessionId,
        epoch,
        store: view,
        capabilities,
        delta,
        effects,
        signal: handle.signal,
        aggregation: helpers,
        isFinal: true,
      });

    const drive = async (handle: RunHandle): Promise<void> => {
      // A fresh instance per attempt (it re-reads the OCC version).
      const iterator = new aggregator().run(contextFor(handle))[Symbol.asyncIterator]();
      handle.track(iterator);
      for (;;) {
        const next = await iterator.next();
        if (next.done === true) {
          return;
        }
      }
    };

    let attemptNumbers = 0; // one span per attempt, carrying the ordinal `runWithRetry` drives
    let lastRunSeconds = 0; // this attempt's own run time, shared from `attempt` to its after-hook audit

    const attempt = async (): Promise<void> => {
      // A hung attempt (e.g. a durable write that never returns) must dead-letter, not hang the
      // session forever: each attempt is bounded by the aggregator's policy timeout, falling back to
      // the orchestrator's operation timeout (the gathering bound does not otherwise apply here).
      const started = this.runtime.clock.monotonic();
      attemptNumbers += 1;
      await withSpan(
        this.runtime.telemetry.tracer,
        `aggregator.run ${aggregator.operatorId}`,
        async (span) => {
          const handle = new RunHandle();
          try {
            await withTimeout(drive(handle), this.timeoutFor(aggregator));
          } catch (error) {
            // The throw propagates through `withSpan`, which records it and marks the span failed;
            // only the outcome attribute is set here.
            handle.stop();
            lastRunSeconds = secondsSince(started, this.runtime.clock.monotonic());
            span.setAttribute('outcome', OperatorOutcome.FAILED);
            this.observeRunSeconds(aggregator.operatorId, lastRunSeconds, OperatorOutcome.FAILED);
            throw error;
          }
          lastRunSeconds = secondsSince(started, this.runtime.clock.monotonic());
          span.setAttribute('outcome', OperatorOutcome.SUCCEEDED);
          this.observeRunSeconds(aggregator.operatorId, lastRunSeconds, OperatorOutcome.SUCCEEDED);
        },
        { attributes: { operator_id: aggregator.operatorId, attempt: attemptNumbers } },
      );
    };

    const afterAttempt = async (attemptNumber: number, error: unknown): Promise<void> => {
      const failed = error !== undefined;
      const outcome = failed ? OperatorOutcome.FAILED : OperatorOutcome.SUCCEEDED;
      if (failed && attemptNumber < this.retryPolicy.maxAttempts) {
        // `runWithRetry` will relaunch after this hook — the same retry semantics the gather loop
        // counts when it arms a backoff window.
        this.runtime.telemetry.operatorRetriesTotal.add(1, { operator_id: aggregator.operatorId });
      }
      await this.auditOperatorRun(aggregator.operatorId, epoch, {
        outcome,
        attempt: attemptNumber,
        runSeconds: lastRunSeconds,
        error: failed ? error : null,
      });
      // Renew the lease after every attempt: a slow aggregator plus backoff could otherwise outlive
      // the lock TTL. This hook deliberately runs outside the operation's try/catch (see
      // `runWithRetry`), so a fenced renew raises StaleEpochError out of the retry loop and `run`
      // turns it into the clean SUPERSEDED stop instead of mistaking the takeover for an aggregator
      // failure. Concurrent aggregators renewing together is a harmless repeated extend.
      await this.runtime.lock.renew(this.sessionId, { epoch });
    };

    try {
      await runWithRetry(attempt, {
        policy: this.retryPolicy,
        seed: seedFor(this.sessionId, aggregator.operatorId),
        sleep: async (ms: number): Promise<void> => {
          await this.runtime.clock.sleep(ms);
        },
        onAttempt: afterAttempt,
      });
    } catch (error) {
      if (!(error instanceof AggregatorDeadLetteredError)) {
        throw error;
      }
      // Bounded retries exhausted → dead-letter + alert; the session still COMPLETES with this
      // failure recorded in `result.deadLetters`; other aggregators are unaffected. The per-attempt
      // failures were already audited; this records the terminal disposition.
      this.deadLetters.push({ operatorId: aggregator.operatorId, reason: failureText(error) });
      this.runtime.telemetry.aggregatorDeadLettersTotal.add(1, { operator_id: aggregator.operatorId });
      await this.auditOperatorRun(aggregator.operatorId, epoch, {
        outcome: OperatorOutcome.DEAD_LETTERED,
        attempt: this.retryPolicy.maxAttempts,
        error,
      });
      getLogger().warning(
        'Aggregator dead-lettered after exhausting retries; its output domain is flagged for re-drive',
        {
          [LOGGER_NAME_FIELD]: LOGGER_NAME,
          operator_id: aggregator.operatorId,
          error,
        },
      );
      return;
    }
    await this.runtime.durable.markContribution(this.sessionId, aggregator.operatorId, { epoch });
    this.runs.set(aggregator.operatorId, (this.runs.get(aggregator.operatorId) ?? 0) + 1);
    getLogger().debug('Aggregator completed and its contribution marked idempotently', {
      [LOGGER_NAME_FIELD]: LOGGER_NAME,
      session_id: this.sessionId,
      operator_id: aggregator.operatorId,
    });
  }

  private async drainInbox(epoch: Epoch): Promise<void> {
    // Reclaim first: re-present entries a predecessor claimed but never applied (crash recovery),
    // then consume never-delivered entries. Keyed-merge makes redelivery idempotent.
    const entries: DeliveredInboxEntry[] = [
      ...(await this.runtime.inbox.reclaim(this.sessionId)),
      ...(await this.runtime.inbox.consume(this.sessionId)),
    ];
    for (const entry of entries) {
      if (entry instanceof PoisonInboxEntry) {
        // Redelivery can never fix malformed wire bytes — quarantine on first sight, or the entry
        // would be re-presented on every pass and every resume until the session TTL.
        await this.quarantineInboxEntry(entry.entryId, entry.error, entry.deliveryCount, epoch);
      }
    }
    const valid = entries.filter((entry): entry is InboxEntry => entry instanceof InboxEntry);
    if (valid.length <= 1) {
      // A single entry needs no batch path — and keeps exactly one apply attempt per delivery.
      for (const entry of valid) {
        await this.applyInboxEntry(entry, epoch);
      }
      return;
    }
    // Apply the whole drained batch in ONE mirror/store write, then ack per entry, each only AFTER
    // its DataPoint is durably applied. At-least-once is preserved: a crash between the batched apply
    // and any individual ack just redelivers those entries, and the keyed-merge makes the re-apply
    // idempotent — so batching the merge ahead of the acks loses nothing.
    try {
      await this.applyInboxDataPoints(
        valid.map((entry) => entry.dataPoint),
        epoch,
      );
    } catch (error) {
      if (error instanceof StaleEpochError) {
        throw error; // fencing is correctness: a takeover must stop this orchestrator, never be absorbed
      }
      // The batch's STORE apply failed as a unit and nothing landed (the apply is atomic). Fall back
      // to per-entry applies so one unappliable entry burns only its own delivery budget — its
      // healthy batch-mates are applied and acked rather than redelivered alongside it.
      for (const entry of valid) {
        await this.applyInboxEntry(entry, epoch);
      }
      return;
    }
    for (const entry of valid) {
      await this.runtime.inbox.ack(this.sessionId, entry.entryId, { epoch });
      this.runtime.telemetry.inboxEntriesTotal.add(1, { disposition: 'applied' });
    }
  }

  /**
   * Durably apply delivered inbox DataPoints; only a store-apply failure propagates.
   *
   * The ack decision gates on the durable STORE apply alone. Once that landed, a failure in the
   * post-apply bookkeeping (audit append, archive append, telemetry) must not leave the entries
   * un-acked: redelivering an already-durably-applied entry burns its delivery budget and duplicates
   * the audit trail without fixing anything, so bookkeeping failures are logged and absorbed. A
   * fencing signal still propagates from everywhere.
   */
  private async applyInboxDataPoints(dataPoints: readonly AnyDataPoint[], epoch: Epoch): Promise<void> {
    const emitted: Emitted[] = dataPoints.map((dataPoint) => ({ kind: 'emitted', operatorId: null, dataPoint }));
    const result = await this.mirror.write(
      emitted.map((signal) => signal.dataPoint),
      { epoch },
    );
    getLogger().debug('Inbox data points applied', {
      [LOGGER_NAME_FIELD]: LOGGER_NAME,
      session_id: this.sessionId,
      count: dataPoints.length,
      data_point_types: [...new Set(dataPoints.map((dataPoint) => dataPoint.type))].sort(),
    });
    try {
      await this.mergeBookkeeping(emitted, result, epoch);
    } catch (error) {
      if (error instanceof StaleEpochError) {
        throw error;
      }
      getLogger().error('Inbox post-apply bookkeeping failed; acking the durably applied entries anyway', {
        [LOGGER_NAME_FIELD]: LOGGER_NAME,
        session_id: this.sessionId,
        data_point_types: [...new Set(dataPoints.map((dataPoint) => dataPoint.type))].sort(),
        error,
      });
    }
  }

  /**
   * Apply one delivered entry under the per-entry isolation policy.
   *
   * One bad entry must never wedge the session or crash-loop a resume — only a fencing signal
   * (`StaleEpochError`) may stop the orchestrator from here.
   */
  private async applyInboxEntry(entry: InboxEntry, epoch: Epoch): Promise<void> {
    try {
      await this.applyInboxDataPoints([entry.dataPoint], epoch); // durable apply first
    } catch (error) {
      if (error instanceof StaleEpochError) {
        throw error; // fencing is correctness: a takeover must stop this orchestrator, never be absorbed
      }
      // The apply may succeed on a later delivery (e.g. a transient store fault), so leave the entry
      // un-acked for redelivery — but only up to the delivery cap, past which it is quarantined
      // rather than ground against the store forever.
      if (entry.deliveryCount >= this.maxInboxDeliveries) {
        await this.quarantineInboxEntry(entry.entryId, failureText(error), entry.deliveryCount, epoch);
        return;
      }
      getLogger().warning('Inbox entry apply failed; leaving it un-acked for redelivery', {
        [LOGGER_NAME_FIELD]: LOGGER_NAME,
        session_id: this.sessionId,
        entry_id: entry.entryId,
        delivery_count: entry.deliveryCount,
        error,
      });
      this.runtime.telemetry.inboxEntriesTotal.add(1, { disposition: 'redelivered' });
      return;
    }
    await this.runtime.inbox.ack(this.sessionId, entry.entryId, { epoch }); // ack after the apply
    this.runtime.telemetry.inboxEntriesTotal.add(1, { disposition: 'applied' });
  }

  private async quarantineInboxEntry(
    entryId: string,
    reason: string,
    deliveryCount: number,
    epoch: Epoch,
  ): Promise<void> {
    await this.runtime.inbox.quarantine(this.sessionId, entryId, { reason, epoch });
    this.runtime.telemetry.inboxEntriesTotal.add(1, { disposition: 'quarantined' });
    getLogger().warning('Inbox entry quarantined; it will not be redelivered', {
      [LOGGER_NAME_FIELD]: LOGGER_NAME,
      session_id: this.sessionId,
      entry_id: entryId,
      reason,
      delivery_count: deliveryCount,
    });
    await this.runtime.audit.append(
      makeAuditLogEntry({
        sessionId: this.sessionId,
        namespaceId: this.namespaceId,
        epoch,
        timestamp: this.runtime.clock.now(),
        kind: AuditKind.INBOX_ENTRY_QUARANTINED,
        inbox: InboxAuditInfo({ entryId, reason, deliveryCount }),
      }),
    );
  }

  private async merge(
    dataPoints: readonly AnyDataPoint[],
    epoch: Epoch,
    operatorId: OperatorId | null = null,
  ): Promise<void> {
    await this.mergeEmissions(
      dataPoints.map((dataPoint) => ({ kind: 'emitted', operatorId, dataPoint })),
      epoch,
    );
  }

  /**
   * Merge a batch through the mirror (one store apply), then do the per-DataPoint bookkeeping.
   *
   * Per-event granularity is preserved end to end: one audit entry and one archive entry per
   * DataPoint, batched onto the sinks in arrival order. Non-ephemeral DataPoints ride the
   * epoch-guarded write-behind archive buffer (the second durable write path); ephemeral ones are
   * never archived — the same rule aggregation follows.
   */
  private async mergeEmissions(emitted: readonly Emitted[], epoch: Epoch): Promise<void> {
    if (emitted.length === 0) {
      return;
    }
    const result = await this.mirror.write(
      emitted.map((signal) => signal.dataPoint),
      { epoch },
    );
    await this.mergeBookkeeping(emitted, result, epoch);
  }

  /** The per-DataPoint bookkeeping that follows a durable apply (audit, archive, telemetry). */
  private async mergeBookkeeping(emitted: readonly Emitted[], result: MirrorWriteResult, epoch: Epoch): Promise<void> {
    const auditEntries: AuditLogEntry[] = [];
    const archiveEntries: ArchivedDataPoint[] = [];
    const mergeKinds: string[] = []; // one per DataPoint, counted only once the audit row actually lands
    for (const [index, signal] of emitted.entries()) {
      const outcome = result.outcomes[index];
      if (outcome === undefined) {
        continue;
      }
      if (signal.operatorId !== null) {
        this.checkEmissionDeclared(signal.operatorId, signal.dataPoint);
      }
      mergeKinds.push(outcome.kind);
      if (!signal.dataPoint.auditsEveryEmission) {
        // A high-volume type: counted here and reported as one coalesced row, so hundreds of merges
        // cost one audit write instead of hundreds.
        bump(this.coalescedAudits, signal.dataPoint.type);
      } else if (outcome.kind === MergeKind.UPDATED) {
        // The identity already existed and only its `lastRetrieved` moved. Operators that rerun on
        // new data re-emit their whole output every pass — a converter can run hundreds of times in
        // one session — so a row each records the same value over and over. The value is already in
        // the store, its latest sighting is on the DataPoint, and the count is reported below; what a
        // per-row trail would add is the timing of each individual re-sighting, at ~99% of the
        // trail's volume.
        bump(this.reobservedAudits, signal.dataPoint.type);
      } else {
        auditEntries.push(this.dataPointAuditEntry(signal.dataPoint, signal.operatorId, epoch));
      }
      if (!signal.dataPoint.isEphemeral) {
        archiveEntries.push(
          ArchivedDataPoint.fromDataPoint(signal.dataPoint, {
            sessionId: this.sessionId,
            namespaceId: this.namespaceId,
            epoch,
          }),
        );
      }
    }
    await this.appendAuditEntries(auditEntries);
    // One count per DataPoint presented at the sole-mutator merge point, emitted only AFTER the audit
    // append succeeded — the count rides the audit seam, so a failed append leaves neither the row
    // nor the count behind and the metric can never disagree with the replayed trail.
    for (const kind of mergeKinds) {
      this.runtime.telemetry.dataPointsMergedTotal.add(1, { kind });
    }
    await this.archiveEntries(archiveEntries);
  }

  private async appendAuditEntries(entries: readonly AuditLogEntry[]): Promise<void> {
    // The batch variant only when more than one entry is in hand; single-event paths keep `append`.
    if (entries.length === 1 && entries[0] !== undefined) {
      await this.runtime.audit.append(entries[0]);
    } else if (entries.length > 0) {
      await this.runtime.audit.appendMany(entries);
    }
  }

  private async archiveEntries(entries: readonly ArchivedDataPoint[]): Promise<void> {
    if (entries.length === 1 && entries[0] !== undefined) {
      await this.runtime.archive.archive(entries[0]);
    } else if (entries.length > 0) {
      await this.runtime.archive.archiveMany(entries);
    }
  }

  private checkEmissionDeclared(operatorId: OperatorId, dataPoint: AnyDataPoint): void {
    // The graph, cycle detection, and quiescence all reason from `produces` declarations, so an
    // undeclared emission silently invalidates the scheduler's reasoning. The DataPoint is still
    // merged — production data is never dropped — but the mismatch is an ERROR (once per
    // operator-and-type, not per emission) so the declaration gets fixed at the source.
    const declared = this.operatorsById.get(operatorId)?.produces ?? [];
    if (declared.some((declaredType) => dataPoint instanceof declaredType)) {
      return;
    }
    const reported = this.undeclaredEmissions.get(operatorId) ?? new Set<string>();
    this.undeclaredEmissions.set(operatorId, reported);
    if (reported.has(dataPoint.type)) {
      return;
    }
    reported.add(dataPoint.type);
    getLogger().error('Operator emitted a DataPoint type missing from its produces declaration; merging it anyway', {
      [LOGGER_NAME_FIELD]: LOGGER_NAME,
      operator_id: operatorId,
      data_point_type: dataPoint.type,
      declared_produces: declared.map((declaredType) => declaredType.name).sort(),
    });
  }

  private async refreshCapabilities(
    activator: CapabilityActivator,
    view: DataPointView,
    epoch: Epoch,
  ): Promise<CapabilityView> {
    const capabilities = await activator.refresh(view);
    for (const capabilityId of activator.activatedIds()) {
      if (this.activatedSeen.has(capabilityId)) {
        continue;
      }
      await this.auditCapability(capabilityId, epoch);
    }
    for (const capabilityId of activator.activatedIds()) {
      this.activatedSeen.add(capabilityId);
    }
    return capabilities;
  }

  /**
   * Run one completion-tail step under the operation timeout.
   *
   * The loop renews its lease as it gathers, but nothing renews across the tail, and the lock's TTL
   * is specified to exceed the longest single unrenewed await. These are the awaits that broke that
   * promise: a slow durable write blocks here for as long as it likes and the lease quietly lapses
   * under a live owner — after which the supervisor resumes the session on top of this one,
   * mid-finalize.
   *
   * Bounding restores the promise instead of working around it (a background keepalive would only
   * make the lease lie). Cancelling a half-done flush is safe: the write-behind archive's buffer is
   * durable and replayed on resume, and the epoch is released either way.
   */
  private async boundedTailStep<T>(step: Promise<T>, description: string): Promise<T> {
    const started = this.runtime.clock.monotonic();
    let result: T;
    try {
      result = await withTimeout(step, this.operationTimeoutMs);
    } catch (error) {
      if (!(error instanceof OperationTimeoutError)) {
        throw error;
      }
      getLogger().error('Completion-tail step timed out; stopping so the lease is not held past its TTL', {
        [LOGGER_NAME_FIELD]: LOGGER_NAME,
        session_id: this.sessionId,
        step: description,
        timeout_ms: this.operationTimeoutMs,
        error,
      });
      throw new CompletionTailTimeoutError(`${description} did not finish within ${this.operationTimeoutMs}ms`, {
        cause: error,
      });
    }
    // Logged on SUCCESS too: without it the only signal a tail step produces is its timeout, so how
    // close a healthy session runs to the budget is invisible until it crosses it.
    getLogger().info(
      'Completion-tail step finished inside its budget; compare elapsed_ms against budget_ms to see how ' +
        'much headroom the session had',
      {
        [LOGGER_NAME_FIELD]: LOGGER_NAME,
        session_id: this.sessionId,
        step: description,
        elapsed_ms: Math.round((this.runtime.clock.monotonic() - started) * 1000) / 1000,
        budget_ms: this.operationTimeoutMs,
      },
    );
    return result;
  }

  private async renewLeaseIfDue(epoch: Epoch, lastRenew: number): Promise<number> {
    // Keep ownership while we work: a session can run longer than the lock's TTL, so renew the lease
    // on a cadence rather than once at acquire. Throttled so a chatty loop (one pass per emission)
    // does not hammer the lock. `renew` raises `StaleEpochError` if a higher epoch has taken over,
    // which `run` turns into a clean SUPERSEDED stop. Returns the (possibly updated) last-renew time.
    const now = this.runtime.clock.monotonic();
    if (now - lastRenew < this.leaseRenewIntervalMs) {
      return lastRenew;
    }
    await this.runtime.lock.renew(this.sessionId, { epoch });
    return now;
  }

  private async advanceWatermark(operatorId: OperatorId, revision: Revision, epoch: Epoch): Promise<void> {
    await this.runtime.store.setWatermark(this.sessionId, operatorId, revision, { epoch });
    this.watermarks.set(operatorId, revision);
  }

  /**
   * Resolve the per-namespace cipher used to seal PII audit values for this run.
   *
   * The provider is injected by the flow; the framework does not know its failure modes (a namespace
   * without a key, a KMS hiccup), so any resolution failure falls back to `null` and PII stays
   * redacted — fail closed, never block the run or leak plaintext.
   */
  private async resolveAuditCipher(): Promise<ValueCipher | null> {
    try {
      return await this.runtime.cipherProvider.forNamespace(this.namespaceId);
    } catch (error) {
      // Provider-defined failures; degrade to redaction, never crash.
      getLogger().warning('Could not resolve per-namespace cipher; PII audit values stay redacted', {
        [LOGGER_NAME_FIELD]: LOGGER_NAME,
        session_id: this.sessionId,
        namespace_id: this.namespaceId,
        error,
      });
      return null;
    }
  }

  private dataPointAuditEntry(dataPoint: AnyDataPoint, operatorId: OperatorId | null, epoch: Epoch): AuditLogEntry {
    let summary = dataPoint.auditSummary();
    if (summary === null) {
      if (dataPoint.isPii) {
        // Seal the value under the namespace key when a real cipher resolved this run; otherwise keep
        // it redacted. A NullCipher (identity) counts as "no cipher" so a passthrough deployment
        // never writes plaintext PII into the audit trail.
        summary =
          this.auditCipher !== null && !(this.auditCipher instanceof NullCipher)
            ? this.auditCipher.encrypt(String(dataPoint.value))
            : '<redacted>';
      } else {
        summary = String(dataPoint.value);
      }
    }
    return makeAuditLogEntry({
      sessionId: this.sessionId,
      namespaceId: this.namespaceId,
      epoch,
      timestamp: this.runtime.clock.now(),
      operatorId,
      kind: AuditKind.DATA_POINT_ADDED,
      dataPoint: DataPointAuditInfo({ dataPointType: dataPoint.type, summary }),
    });
  }

  private async auditSessionParked(epoch: Epoch): Promise<void> {
    await this.runtime.audit.append(
      makeAuditLogEntry({
        sessionId: this.sessionId,
        namespaceId: this.namespaceId,
        epoch,
        timestamp: this.runtime.clock.now(),
        kind: AuditKind.SESSION_PARKED,
      }),
    );
  }

  private async auditCapability(capabilityId: CapabilityId, epoch: Epoch): Promise<void> {
    getLogger().debug('Capability activated for this session; recording the audit entry', {
      [LOGGER_NAME_FIELD]: LOGGER_NAME,
      session_id: this.sessionId,
      capability_id: capabilityId,
    });
    await this.runtime.audit.append(
      makeAuditLogEntry({
        sessionId: this.sessionId,
        namespaceId: this.namespaceId,
        epoch,
        timestamp: this.runtime.clock.now(),
        kind: AuditKind.CAPABILITY_ACTIVATED,
        capability: CapabilityAuditInfo({ capabilityId }),
      }),
    );
  }

  private countSession(status: SessionStatus, span: Span): void {
    // The disposition lands on the run span and the counter at the same seam, so traces and metrics
    // can never disagree on how the session ended.
    span.setAttribute('status', status);
    this.runtime.telemetry.sessionsTotal.add(1, { status });
  }

  private observeRunSeconds(operatorId: OperatorId, runSeconds: number, outcome: OperatorOutcome): void {
    this.runtime.telemetry.operatorRunSeconds.record(runSeconds, { operator_id: operatorId, outcome });
  }

  private async auditOperatorRun(
    operatorId: OperatorId,
    epoch: Epoch,
    detail: {
      readonly outcome: OperatorOutcome;
      readonly runCount?: number;
      readonly attempt?: number;
      readonly runSeconds?: number | null;
      readonly error?: unknown;
    },
  ): Promise<void> {
    // The counter rides the audit seam so metrics and the audit trail can never disagree on how many
    // run dispositions (including SKIPPED and the terminal DEAD_LETTERED record) the session produced.
    this.runtime.telemetry.operatorRunsTotal.add(1, { operator_id: operatorId, outcome: detail.outcome });
    const error = detail.error ?? null;
    await this.runtime.audit.append(
      makeAuditLogEntry({
        sessionId: this.sessionId,
        namespaceId: this.namespaceId,
        epoch,
        timestamp: this.runtime.clock.now(),
        operatorId,
        kind: AuditKind.OPERATOR_INVOKED,
        operator: OperatorAuditInfo({
          outcome: detail.outcome,
          runCount: detail.runCount ?? 1,
          attempt: detail.attempt ?? 1,
          runSeconds: detail.runSeconds ?? null,
          error: error === null ? null : failureText(error),
        }),
      }),
    );
  }

  private async auditCapabilityInvocation(
    capabilityId: CapabilityId,
    action: string,
    parameters: Readonly<Record<string, unknown>>,
    epoch: Epoch,
  ): Promise<void> {
    // Redact every parameter value (keys retained): the engine can't tell which carry PII, so the
    // trail records which action ran on which capability, never the argument values. The log follows
    // the same discipline — keys only. It is emitted inside the capability.action span (the auditor
    // runs within it), so the bridged record correlates with the action's trace.
    getLogger().debug('Capability action invoked', {
      [LOGGER_NAME_FIELD]: LOGGER_NAME,
      session_id: this.sessionId,
      capability_id: capabilityId,
      action,
      parameter_keys: Object.keys(parameters).sort(),
    });
    const redacted = Object.fromEntries(Object.keys(parameters).map((key) => [key, '<redacted>']));
    await this.runtime.audit.append(
      makeAuditLogEntry({
        sessionId: this.sessionId,
        namespaceId: this.namespaceId,
        epoch,
        timestamp: this.runtime.clock.now(),
        kind: AuditKind.CAPABILITY_INVOKED,
        capability: CapabilityAuditInfo({ capabilityId, action, parameters: redacted }),
      }),
    );
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
 * A rerun is warranted by a newly-available capability, or by new data of a type the operator
 * depends on (subtype-aware). Under `RerunOn.ADDED_ONLY` a freshness-only re-observation of an
 * existing `(type, value)` identity (`delta.updated`) is not new data — chatty re-observation must
 * not keep re-triggering a pure value-computation operator. Unrelated types — including the
 * operator's own emissions of types it does NOT consume — don't match, so they never trigger a
 * rerun. An operator that consumes a type it also produces is a genuine cycle whose own emissions DO
 * re-trigger it: that is exactly why the graph requires such an operator to declare a `maxCycles`
 * circuit-breaker bounding the loop.
 */
const hasRelevantNewData = (operator: OperatorClass, delta: InvocationDelta): boolean => {
  if (delta.newlyAvailableCaps.size > 0) {
    return true;
  }
  const newData =
    operator.policy.rerunOn === RerunOn.ADDED_ONLY ? [...delta.added] : [...delta.added, ...delta.updated];
  // Both readiness inputs (`dependsOn`) and read-only inputs (`uses`) are rerun triggers: new data of
  // either re-fires the operator. Only `dependsOn` gates readiness (see `isReady`); `uses` does not.
  const triggers = [...(operator.dependsOn ?? []), ...(operator.uses ?? [])];
  return newData.some((dataPoint) => triggers.some((trigger) => dataPoint instanceof trigger));
};

/** An aggregator's opt-in to joining the gather set, read off a class that may not be one. */
const interimRefreshOf = (operator: ConcreteOperatorClass): boolean =>
  (operator as AggregatorStatics).interimRefresh ?? false;

/** The DataPoint types an aggregator declares it folds; empty for anything that declares none. */
const consumesOf = (operator: ConcreteOperatorClass): readonly DataPointClass<AnyDataPoint>[] =>
  (operator as AggregatorStatics).consumes ?? [];

/** Count one more merge of `key`, the port of `Counter[key] += 1`. */
const bump = (tally: Map<string, number>, key: string): void => {
  tally.set(key, (tally.get(key) ?? 0) + 1);
};

/** The tally in key order — the audit trail's rows must not depend on insertion order. */
const sortedTally = (tally: ReadonlyMap<string, number>): readonly { key: string; count: number }[] =>
  [...tally.entries()]
    .map(([key, count]) => ({ key, count }))
    .sort((left, right) => (left.key === right.key ? 0 : left.key < right.key ? -1 : 1));

/** Milliseconds of monotonic time expressed in the seconds the audit and metrics record. */
const secondsSince = (started: number, ended: number): number => (ended - started) / 1000;

/** What a failure reads as in the audit trail and the dead-letter record — Python's `str(exc)`. */
const failureText = (error: unknown): string => (error instanceof Error ? error.message : String(error));

/**
 * Hand the event loop back once — the counterpart of `asyncio.sleep(0)` for a *task*, not a timer.
 *
 * `setImmediate` runs after the pending microtasks and after any expired timer, which is exactly the
 * point at which Python's loop has let every runnable task take its turn.
 */
const yieldToEventLoop = (): Promise<void> =>
  new Promise<void>((resolve) => {
    setImmediate(resolve);
  });
