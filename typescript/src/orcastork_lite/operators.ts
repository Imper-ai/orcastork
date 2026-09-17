/**
 * `Operator` — the unit of work — plus its scheduling policy and per-invocation context.
 *
 * An operator declares its data dependencies (`dependsOn` in, `produces` out, `uses` as
 * read-only rerun triggers, `consumes` as the sinks it exists to feed), its capability needs
 * (`requires`), and an explicit scheduling `policy` (no default — the author must choose
 * `rerunOnNewData`). It implements one async generator, `run`, that emits DataPoints by
 * yielding them. There is no registry: the classes are handed to the orchestrator directly.
 *
 * @module
 */

import { z } from 'zod';
import type { CapabilityClass, CapabilityView } from './capabilities.js';
import type { AnyDataPoint, DataPointClass, DataPointEmission, DataPointView } from './datapoints.js';
import { InvalidOperatorError } from './exceptions.js';
import type { CapabilityId, NamespaceId, OperatorId, SessionId } from './ids.js';

/**
 * Which delta kinds count as rerun-worthy new data.
 *
 * `ADDED_OR_UPDATED` also reruns on freshness-only re-observations of an existing identity
 * (`delta.updated`); `ADDED_ONLY` ignores those, so chatty re-observation cannot keep
 * re-triggering a pure value-computation operator. A newly-available capability always
 * warrants a rerun, under either mode.
 */
export const RerunOn = {
  ADDED_OR_UPDATED: 'added_or_updated',
  ADDED_ONLY: 'added_only',
} as const;

/** One of the two {@link RerunOn} modes. */
export type RerunOn = (typeof RerunOn)[keyof typeof RerunOn];

/** Attempts a retry policy allows when it does not say otherwise. */
const DEFAULT_MAX_ATTEMPTS = 5;

/** The first backoff window, doubled per attempt (Python's `0.05` seconds). */
const DEFAULT_BASE_DELAY_MS = 50;

/** Multiplicative jitter applied to every backoff window, ±20%. */
const DEFAULT_JITTER = 0.2;

/**
 * Bounded relaunch-on-failure with a jittered exponential backoff (`baseDelayMs * 2**attempt`).
 *
 * The bounds fail fast on a misconfigured policy rather than abandoning the operator on its first
 * failure (`maxAttempts < 1`) or producing a negative or degenerate backoff schedule.
 */
export interface RetryPolicy {
  /** How many attempts the failure budget holds, the first run included. */
  readonly maxAttempts: number;

  /** The first window; attempt *n* waits `baseDelayMs * 2**n`. */
  readonly baseDelayMs: number;

  /** Multiplicative jitter, ±`jitter`, applied to each window. */
  readonly jitter: number;
}

/** What {@link RetryPolicy} is built from; every field has a default. */
export interface RetryPolicyInit {
  readonly maxAttempts?: number;
  readonly baseDelayMs?: number;
  readonly jitter?: number;
}

/** The bounds a retry policy must satisfy, whether it is being built or accepted as a field. */
const retryBounds = {
  maxAttempts: z.number().int().min(1),
  baseDelayMs: z.number().min(0),
  jitter: z.number().min(0).max(1),
};

const retryPolicySchema = z.object({
  maxAttempts: retryBounds.maxAttempts.default(DEFAULT_MAX_ATTEMPTS),
  baseDelayMs: retryBounds.baseDelayMs.default(DEFAULT_BASE_DELAY_MS),
  jitter: retryBounds.jitter.default(DEFAULT_JITTER),
});

/**
 * Build a {@link RetryPolicy}, rejecting a degenerate schedule where it is written.
 *
 * Validation pays at the trust boundary: a policy is authored once, by hand, and a wrong value
 * here would otherwise surface as an operator that is never retried or a window that never ends.
 */
export const RetryPolicy = (init: RetryPolicyInit = {}): RetryPolicy => Object.freeze(retryPolicySchema.parse(init));

/** How an operator is scheduled: whether new data re-triggers it, and every bound on that. */
export interface OperatorPolicy {
  /** NO DEFAULT — the author must decide whether new data re-triggers this operator. */
  readonly rerunOnNewData: boolean;

  /** Consulted only when `rerunOnNewData` is true; inert otherwise (deliberately not an error). */
  readonly rerunOn: RerunOn;

  /** Coalescing window for reruns, in milliseconds; `null` → the scheduler default (zero). */
  readonly debounceMs: number | null;

  /** Circuit-breaker run cap; required to sit on a graph cycle. */
  readonly maxCycles: number | null;

  /** Per-operator override of the orchestrator's operation timeout, in milliseconds. */
  readonly timeoutMs: number | null;

  /**
   * Bounded relaunch-on-failure: the loop relaunches a failed run on a jittered backoff window;
   * `null` abandons the operator after a single failed run.
   */
  readonly retry: RetryPolicy | null;
}

/** What {@link OperatorPolicy} is built from; only `rerunOnNewData` is required. */
export interface OperatorPolicyInit {
  readonly rerunOnNewData: boolean;
  readonly rerunOn?: RerunOn;
  readonly debounceMs?: number | null;
  readonly maxCycles?: number | null;
  readonly timeoutMs?: number | null;
  readonly retry?: RetryPolicy | null;
}

const operatorPolicySchema = z.object({
  rerunOnNewData: z.boolean(),
  rerunOn: z.enum([RerunOn.ADDED_OR_UPDATED, RerunOn.ADDED_ONLY]).optional(),
  debounceMs: z.number().nullish(),
  maxCycles: z.number().int().nullish(),
  timeoutMs: z.number().nullish(),
  retry: z.object(retryBounds).nullish(),
});

/**
 * Build an {@link OperatorPolicy}; an omitted knob is stored as `null` (the scheduler's default).
 *
 * `rerunOnNewData` has no default on purpose: how an operator answers new data is a decision only
 * its author can make, and a default would make the quiet answer the common one.
 */
export const OperatorPolicy = (init: OperatorPolicyInit): OperatorPolicy => {
  operatorPolicySchema.parse(init);
  return Object.freeze({
    rerunOnNewData: init.rerunOnNewData,
    rerunOn: init.rerunOn ?? RerunOn.ADDED_OR_UPDATED,
    debounceMs: init.debounceMs ?? null,
    maxCycles: init.maxCycles ?? null,
    timeoutMs: init.timeoutMs ?? null,
    retry: init.retry ?? null,
  });
};

/**
 * What an operator declares about itself, as `static readonly` fields on the class:
 *
 * ```ts
 * class TitleFetcher extends Operator {
 *   static readonly operatorId = OperatorId('title_fetcher');
 *   static readonly policy = OperatorPolicy({ rerunOnNewData: true });
 *   static readonly dependsOn = [Url];
 *   static readonly produces = [Title];
 *   async *run(ctx: OperatorContext): AsyncIterable<DataPointEmission> { … }
 * }
 * ```
 *
 * The type sets are declared here rather than on {@link Operator} itself so that a subclass
 * setting one does not have to write `static override` for a field the base only holds a default
 * of — the engine reads an absent set as empty, exactly as Python's class defaults do.
 */
export interface OperatorStatics {
  /** Identity of this operator; two operators in one orchestrator may not share it. */
  readonly operatorId: OperatorId;

  /** Explicit; no default. */
  readonly policy: OperatorPolicy;

  /**
   * Readiness gate: EVERY `dependsOn` type must be present (subtype-aware) before the operator can
   * run at all; new data of these types also re-triggers a `rerunOnNewData` operator.
   */
  readonly dependsOn?: readonly DataPointClass[];

  /**
   * Data the operator READS but does not require: new data of a `uses` type re-triggers a
   * `rerunOnNewData` operator exactly like `dependsOn`, but its ABSENCE never blocks readiness.
   */
  readonly uses?: readonly DataPointClass[];

  /** Outputs (graph edges). */
  readonly produces?: readonly DataPointClass[];

  /** Capability dependencies (subtype-aware). */
  readonly requires?: readonly CapabilityClass[];

  /**
   * The DataPoint types this operator is the sink for. When any operator in a session declares
   * `consumes`, the orchestrator runs ONLY the operators whose output is transitively needed to
   * produce those sinks and prunes the rest. Empty everywhere (the default) opts out of pruning.
   */
  readonly consumes?: readonly DataPointClass[];

  /** Every class has one; the framework uses it in error messages and logs. */
  readonly name: string;
}

/**
 * An operator class as the framework handles it — Python's `type[Operator]`.
 *
 * Abstract on purpose: the graph and the scheduler only ever *read* declarations off a class, and
 * an abstract intermediate in a family of operators must be as usable there as a leaf.
 */
export type OperatorClass<T extends Operator = Operator> = OperatorStatics & (abstract new () => T);

/** A constructible operator class: what an orchestrator registers and instantiates per run. */
export type ConcreteOperatorClass<T extends Operator = Operator> = OperatorStatics & (new () => T);

/**
 * The unit of work. Subclass it, declare {@link OperatorStatics}, implement `run`.
 *
 * A fresh no-argument instance is constructed per run, so an operator holds no state between
 * runs: what changed since the last one arrives as `ctx.delta`.
 */
export abstract class Operator {
  /** This operator's id, read off the class — the counterpart of Python's `self.operator_id`. */
  public get operatorId(): OperatorId {
    return (this.constructor as unknown as OperatorStatics).operatorId;
  }

  /**
   * Emit DataPoints value-only by yielding `SomeDataPoint.emit(value)`.
   *
   * Implement as an `async *` generator. The orchestrator stamps provenance and observation time
   * on each emission as it is written, and merges it the instant it is yielded — so a long run
   * that streams its results unblocks its consumers as it goes.
   */
  public abstract run(ctx: OperatorContext): AsyncIterable<DataPointEmission>;
}

/**
 * Reject a concrete operator that declares no scheduling `policy`.
 *
 * Python catches this in `__init_subclass__`, at class-definition time. TypeScript has no such
 * hook without a decorator, and this package deliberately has no registration decorators, so the
 * check runs where classes enter the framework instead: the orchestrator calls it on every
 * operator it is handed. The type system already demands `policy`, so this catches the call from
 * untyped JavaScript — which is exactly the caller that would otherwise fail much later, as an
 * operator that is never scheduled.
 */
export const validateOperatorDeclaration = (operator: OperatorClass): void => {
  if (operator.policy === undefined || operator.policy === null) {
    throw new InvalidOperatorError(`${operator.name} must declare a scheduling \`policy\``);
  }
};

/**
 * What changed since *this* operator last ran — so a rerun does incremental work.
 *
 * A frozen plain object, not a validated model: the loop is its only constructor and builds one
 * per rerun-eligible operator on every pass, so validating hundreds of DataPoints against types
 * the engine itself just computed would cost the hot path for nothing.
 */
export interface InvocationDelta {
  /** New identities. */
  readonly added: ReadonlySet<AnyDataPoint>;

  /** Existing identities re-observed (`lastRetrieved` bumped). */
  readonly updated: ReadonlySet<AnyDataPoint>;

  /** Capabilities that came online since the last run. */
  readonly newlyAvailableCaps: ReadonlySet<CapabilityId>;

  /** First run → `added` is the full current set. */
  readonly isFirstInvocation: boolean;
}

/** What an {@link InvocationDelta} is built from; the sets are copied. */
export interface InvocationDeltaInit {
  readonly added: Iterable<AnyDataPoint>;
  readonly updated: Iterable<AnyDataPoint>;
  readonly newlyAvailableCaps: Iterable<CapabilityId>;
  readonly isFirstInvocation: boolean;
}

/** Build an {@link InvocationDelta} — the session state's job, and no-one else's. */
export const InvocationDelta = (init: InvocationDeltaInit): InvocationDelta =>
  Object.freeze({
    added: new Set(init.added),
    updated: new Set(init.updated),
    newlyAvailableCaps: new Set(init.newlyAvailableCaps),
    isFirstInvocation: init.isFirstInvocation,
  });

/** What an {@link OperatorContext} is built from — everything one invocation may see. */
export interface OperatorContextInit {
  readonly sessionId: SessionId;
  readonly namespaceId: NamespaceId;
  readonly store: DataPointView;
  readonly capabilities: CapabilityView;
  readonly delta: InvocationDelta;

  /** Aborted when the run is cut short — its timeout, or the session deadline. */
  readonly signal: AbortSignal;
}

/**
 * What one invocation sees: the session's DataPoints, its capabilities, and what is new.
 *
 * Built by the loop and frozen, never validated: the engine would only be checking itself, once
 * per launched run. `signal` is the port of the cancellation Python gets from `asyncio`: a
 * promise cannot be cancelled, so a run that must stop — because it timed out or the session
 * deadline passed — is told through the signal, and anything genuinely in flight inside `run`
 * should be tied to it.
 */
export class OperatorContext {
  public readonly sessionId: SessionId;

  public readonly namespaceId: NamespaceId;

  /** Every DataPoint the session holds, not only this operator's dependencies. */
  public readonly store: DataPointView;

  public readonly capabilities: CapabilityView;

  /** What changed since this operator last ran. */
  public readonly delta: InvocationDelta;

  /** Aborted when this run is cut short; tie anything long-running to it. */
  public readonly signal: AbortSignal;

  public constructor(init: OperatorContextInit) {
    this.sessionId = init.sessionId;
    this.namespaceId = init.namespaceId;
    this.store = init.store;
    this.capabilities = init.capabilities;
    this.delta = init.delta;
    this.signal = init.signal;
    Object.freeze(this);
  }

  /** The newest DataPoint of `dataPointType` by `lastRetrieved` (ergonomic single-read). */
  public latest<T extends AnyDataPoint>(dataPointType: DataPointClass<T>): T | null {
    return this.store.latest(dataPointType);
  }
}
