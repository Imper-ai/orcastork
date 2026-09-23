/**
 * `Operator` — the single base unifying the old collector / enricher / detector.
 *
 * An operator declares its data dependencies (`dependsOn` in, `produces` out), its capability needs
 * (`requires`), and an explicit scheduling `policy` (no default — the author must choose
 * `rerunOnNewData`). It implements one async generator, `run`, that emits DataPoints by yielding
 * them. Concrete operators register under `operatorId` through the {@link operator} decorator; a
 * duplicate id raises (unlike a silently-overwriting factory dict).
 *
 * @module
 */

import { z } from 'zod';
import { type RetryPolicy, retryPolicyBounds } from '../aggregation/retry.js';
import type { CapabilityClass } from '../capabilities/base.js';
import type { AnyDataPoint, DataPointClass, DataPointEmission } from '../datapoints/index.js';
import { DuplicateRegistrationError, InvalidOperatorError } from '../exceptions.js';
import type { OperatorId } from '../ids.js';
import { Registry } from '../internal/registry.js';
import type { OperatorContext } from './context.js';

/**
 * Registry of concrete operators (aggregators included) keyed by `operatorId`.
 *
 * Module-level, as Python's `Operator._registry` is: registering is a side effect of declaring the
 * class. It is a {@link Registry}, so the test setup snapshots and restores it around every test
 * without knowing it exists. {@link Operator.registered} is the public read of it; the registry
 * itself is exported from this module only (not from the barrel), like `capabilityRegistry` and
 * the DataPoint registry, so a test can simulate a redeploy without reaching through
 * `registries()`.
 */
export const operatorRegistry = new Registry<OperatorId, ConcreteOperatorClass>('orcastork.operators');

/**
 * Which delta kinds count as rerun-worthy new data.
 *
 * `ADDED_OR_UPDATED` also reruns on freshness-only re-observations of an existing `(type, value)`
 * identity (`delta.updated`); `ADDED_ONLY` ignores those, so chatty re-observation cannot keep
 * re-triggering a pure value-computation operator. A newly-available capability always warrants a
 * rerun, under either mode.
 */
export const RerunOn = {
  ADDED_OR_UPDATED: 'added_or_updated',
  ADDED_ONLY: 'added_only',
} as const;

/** One of the two {@link RerunOn} modes. */
export type RerunOn = (typeof RerunOn)[keyof typeof RerunOn];

/** How an operator is scheduled: whether new data re-triggers it, and every bound on that. */
export interface OperatorPolicy {
  /** NO DEFAULT — the author must decide whether new data re-triggers this operator. */
  readonly rerunOnNewData: boolean;

  /** Consulted only when `rerunOnNewData` is true; inert otherwise (deliberately not an error). */
  readonly rerunOn: RerunOn;

  /** Per-operator override of the scheduler's global default coalescing window, in milliseconds. */
  readonly debounceMs: number | null;

  /** Circuit-breaker iteration cap; required to sit on a graph cycle. */
  readonly maxCycles: number | null;

  /** Per-operator override of the orchestrator's global operation timeout, in milliseconds. */
  readonly timeoutMs: number | null;

  /**
   * Bounded relaunch-on-failure: the gather loop relaunches a failed run on a jittered backoff
   * window; `null` abandons the operator after a single failed run.
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
  debounceMs: z.number().min(0).nullish(),
  maxCycles: z.number().int().min(1).nullish(),
  timeoutMs: z.number().min(0).nullish(),
  // The nested policy is held to the bounds it is built with, rather than restating them.
  retry: z.object(retryPolicyBounds).nullish(),
});

/**
 * Build an {@link OperatorPolicy}; an omitted knob is stored as `null` (the scheduler's default).
 *
 * `rerunOnNewData` has no default on purpose: how an operator answers new data is a decision only
 * its author can make, and a default would make the quiet answer the common one. Validation pays at
 * the trust boundary — a policy is authored once, by hand.
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
 * @operator
 * class TitleFetcher extends Operator {
 *   static readonly operatorId = OperatorId('title_fetcher');
 *   static readonly policy = OperatorPolicy({ rerunOnNewData: true });
 *   static readonly dependsOn = [UrlDataPoint];
 *   static readonly produces = [TitleDataPoint];
 *   async *run(ctx: OperatorContext): AsyncIterable<DataPointEmission> { … }
 * }
 * ```
 *
 * The type sets are declared here rather than on {@link Operator} itself so that a subclass setting
 * one does not have to write `static override` for a field the base only holds a default of — the
 * engine reads an absent set as empty, exactly as Python's class defaults do.
 */
export interface OperatorStatics {
  /** Registry key AND provenance identity. */
  readonly operatorId: OperatorId;

  /** Explicit; no default. */
  readonly policy: OperatorPolicy;

  /**
   * Readiness gate: EVERY `dependsOn` type must be present (subtype-aware) before the operator can
   * run at all, and new data of these types also re-triggers a `rerunOnNewData` operator.
   */
  readonly dependsOn?: readonly DataPointClass<AnyDataPoint>[];

  /**
   * Data the operator READS but does not require to run: new data of a `uses` type re-triggers a
   * `rerunOnNewData` operator (subtype-aware), exactly like `dependsOn` — but its ABSENCE never
   * blocks readiness. Declare here every consumed type that is optional / may arrive late (e.g. an
   * aggregator folds many optional attributes it must re-fold on arrival, yet must run without them).
   */
  readonly uses?: readonly DataPointClass<AnyDataPoint>[];

  /** Outputs (graph edges). */
  readonly produces?: readonly DataPointClass<AnyDataPoint>[];

  /** Capability dependencies (subtype-aware). */
  readonly requires?: readonly CapabilityClass[];

  /** Every class has one; the framework uses it in error messages and logs. */
  readonly name: string;
}

/**
 * An operator class as the framework handles it — Python's `type[Operator]`.
 *
 * Abstract on purpose: the graph and the scheduler only ever *read* declarations off a class, and an
 * abstract intermediate in a family of operators must be as usable there as a leaf.
 */
export type OperatorClass<T extends Operator = Operator> = OperatorStatics & (abstract new () => T);

/** A constructible, registered operator class: what the orchestrator instantiates per run. */
export type ConcreteOperatorClass<T extends Operator = Operator> = OperatorStatics & (new () => T);

/**
 * The unit of work. Subclass it, declare {@link OperatorStatics}, implement `run`, decorate it with
 * {@link operator}.
 *
 * A fresh no-argument instance is constructed per run, so an operator holds no state between runs:
 * what changed since the last one arrives as `ctx.delta`.
 */
export abstract class Operator {
  /** This operator's id, read off the class — the counterpart of Python's `self.operator_id`. */
  public get operatorId(): OperatorId {
    return (this.constructor as unknown as OperatorStatics).operatorId;
  }

  /** Every registered operator (aggregators included), keyed by id — a detached copy. */
  public static registered(): ReadonlyMap<OperatorId, ConcreteOperatorClass> {
    return operatorRegistry.snapshot();
  }

  /**
   * Emit DataPoints value-only by yielding `SomeDataPoint.emit(value)`.
   *
   * Implement as an `async *` generator. An operator declares *what* it observed (the leaf type +
   * value); the orchestrator stamps provenance and observation time on write.
   */
  public abstract run(ctx: OperatorContext): AsyncIterable<DataPointEmission>;
}

/** Validate this class's declarations, then register it under its `operatorId`. */
const declareOperator = (target: ConcreteOperatorClass): void => {
  // Both are typed as possibly absent because only an untyped JavaScript caller can get here
  // without them; the type system demands both of anything the decorator accepts.
  const operatorId: OperatorId | undefined = target.operatorId;
  const policy: OperatorPolicy | undefined = target.policy;
  if (operatorId === undefined) {
    throw new InvalidOperatorError(`${target.name} must declare an \`operatorId\``);
  }
  if (policy === undefined || policy === null) {
    throw new InvalidOperatorError(`${target.name} must declare a scheduling \`policy\``);
  }
  operatorRegistry.set(operatorId, target, (key, existing) => {
    if (existing !== target) {
      throw new DuplicateRegistrationError(`operator_id '${key}' is already registered to ${existing.name}`);
    }
  });
};

/**
 * Declare a concrete operator (or aggregator): validate its declarations and register it.
 *
 * ```ts
 * @operator
 * class TitleFetcher extends Operator { … }
 * ```
 *
 * The port's `__init_subclass__`. It runs at class-definition time, so a missing `policy` raises
 * {@link InvalidOperatorError} and a duplicate id raises {@link DuplicateRegistrationError} there,
 * as in Python; re-applying it to the same class (a module reload) is benign. An abstract base or
 * intermediate is simply not decorated — that is the port of "no `operator_id`, still abstract, so
 * not registered".
 */
export const operator = <T extends ConcreteOperatorClass>(target: T, context?: ClassDecoratorContext): void => {
  if (context !== undefined) {
    // A class decorator runs BEFORE the class's static fields are initialized, so `operatorId` and
    // `policy` would still be undefined here; a class-decorator initializer runs after they are,
    // which is where the declaration can actually read what the class declared. Both compilers this
    // package is built and tested with agree on that order.
    context.addInitializer(function (this: unknown) {
      declareOperator(this as ConcreteOperatorClass);
    });
    return;
  }
  // Applied by hand — `operator(SomeClass)`, which is how a class *expression* (a test factory)
  // registers. The class is fully initialized by then, so its statics are readable straight away.
  declareOperator(target);
};
