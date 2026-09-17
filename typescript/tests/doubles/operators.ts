/**
 * Operator/Aggregator test doubles.
 *
 * Because `operatorId` and the dependency sets are class statics (the registry keys off them), a
 * configurable stub is a *factory* that creates a fresh registered subclass per call. The
 * registry-isolation setup in `tests/setup.ts` removes them after each test.
 *
 * A class *expression* cannot carry a decorator, so each factory registers its stub by applying the
 * `operator` decorator by hand — the manual form the decorator exists to support.
 *
 * @module
 */

import type { RetryPolicy } from '../../src/orcastork/aggregation/index.js';
import type { CapabilityClass } from '../../src/orcastork/capabilities/index.js';
import type { AnyDataPoint, DataPointClass } from '../../src/orcastork/datapoints/index.js';
import { DataPointEmission } from '../../src/orcastork/datapoints/index.js';
import { OperatorId } from '../../src/orcastork/ids.js';
import type {
  ConcreteAggregatorClass,
  ConcreteOperatorClass,
  OperatorContext,
  RerunOn,
} from '../../src/orcastork/operators/index.js';
import { Aggregator, Operator, OperatorPolicy, operator } from '../../src/orcastork/operators/index.js';

/** What a stub may emit: a value-only emission, or a full DataPoint the factory unwraps into one. */
export type Emitted = DataPointEmission | AnyDataPoint;

/** The policy knobs every stub shares; they mirror {@link OperatorPolicy} one for one. */
interface StubPolicyOptions {
  readonly rerunOnNewData?: boolean;
  readonly rerunOn?: RerunOn;
  readonly debounceMs?: number | null;
  readonly maxCycles?: number | null;
  readonly timeoutMs?: number | null;
  readonly retry?: RetryPolicy | null;
}

/** How a stub operator differs from the plain one. */
export interface MakeOperatorOptions extends StubPolicyOptions {
  readonly dependsOn?: readonly DataPointClass<AnyDataPoint>[];
  readonly uses?: readonly DataPointClass<AnyDataPoint>[];
  readonly produces?: readonly DataPointClass<AnyDataPoint>[];
  readonly requires?: readonly CapabilityClass[];

  /** Emitted on every run, in order. */
  readonly emits?: Iterable<Emitted>;

  /** Emissions computed from the context instead — what a rerun test needs to see its delta. */
  readonly emitFactory?: (ctx: OperatorContext) => Iterable<Emitted>;

  /** Stay in flight this long after emitting, so a real timeout bound or the deadline can fire. */
  readonly sleepAfterMs?: number | null;

  /** Throw this on every run (to exercise the failure-isolation path). */
  readonly raiseError?: Error | null;

  /** Throw on that many runs before succeeding (retry tests). */
  readonly failFirst?: number;

  /** Collects every context the stub was invoked with, so a test can inspect deltas. */
  readonly seen?: OperatorContext[];
}

/** How a stub aggregator differs from the plain one. */
export interface MakeAggregatorOptions extends StubPolicyOptions {
  readonly dependsOn?: readonly DataPointClass<AnyDataPoint>[];
  readonly uses?: readonly DataPointClass<AnyDataPoint>[];
  readonly requires?: readonly CapabilityClass[];
  readonly consumes?: readonly DataPointClass<AnyDataPoint>[];
  readonly interimRefresh?: boolean;

  /** The body of `aggregate`. */
  readonly onAggregate?: (ctx: OperatorContext) => Promise<void>;
}

/** The class name Python's factories give a stub: `risk_report` → `RiskReport`. */
const titleCase = (identifier: string): string =>
  identifier
    .split('_')
    .map((part) => (part === '' ? part : `${part[0]?.toUpperCase() ?? ''}${part.slice(1)}`))
    .join('');

const policyFrom = (options: StubPolicyOptions): OperatorPolicy =>
  OperatorPolicy({
    rerunOnNewData: options.rerunOnNewData ?? false,
    rerunOn: options.rerunOn ?? 'added_or_updated',
    debounceMs: options.debounceMs ?? null,
    maxCycles: options.maxCycles ?? null,
    timeoutMs: options.timeoutMs ?? null,
    retry: options.retry ?? null,
  });

/** A full DataPoint stands in for the emission an operator would have yielded. */
const asEmission = (item: Emitted): DataPointEmission =>
  item instanceof DataPointEmission
    ? item
    : new DataPointEmission(item.constructor as DataPointClass<AnyDataPoint>, item.value);

/**
 * Wait `ms` of REAL time, abandoning the wait as soon as `signal` aborts.
 *
 * The counterpart of the Python stub's `asyncio.sleep(...)`, and deliberately not a `Clock` sleep:
 * it exists to hold a run genuinely in flight against a bound that runs on real time (an operation
 * timeout, the clipped deadline). The timer is `unref`'d and cleared on abort, so a test never waits
 * it out and a stray timer never holds the worker open.
 */
export const abortableSleep = (ms: number, signal: AbortSignal): Promise<void> =>
  new Promise<void>((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    const timer = setTimeout(resolve, ms);
    timer.unref();
    signal.addEventListener(
      'abort',
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
  });

/**
 * Create (and register) a stub Operator subclass with the given declarations.
 *
 * `emits`/`emitFactory` yield value-only `DataPointEmission`s (`Leaf.emit(value)`); a full DataPoint
 * is also accepted and normalized to an emission, so the orchestrator stamps its provenance just
 * like a real operator's. `emitFactory` derives emissions from the context (e.g. from the delta);
 * `sleepAfterMs` stays in flight (to trigger per-operation timeouts); `raiseError` throws after
 * emitting (to exercise the failure-isolation path).
 */
export const makeOperator = (operatorId: string, options: MakeOperatorOptions = {}): ConcreteOperatorClass => {
  const policy = policyFrom(options);
  // Materialized once, as the Python factory does, so a generator passed as `emits` is not spent by
  // the first run.
  const emits = [...(options.emits ?? [])];
  const { emitFactory, failFirst = 0, raiseError = null, seen, sleepAfterMs = null } = options;
  let runs = 0;

  class StubOperator extends Operator {
    public static readonly operatorId = OperatorId(operatorId);
    public static readonly policy = policy;
    public static readonly dependsOn: readonly DataPointClass<AnyDataPoint>[] = options.dependsOn ?? [];
    public static readonly uses: readonly DataPointClass<AnyDataPoint>[] = options.uses ?? [];
    public static readonly produces: readonly DataPointClass<AnyDataPoint>[] = options.produces ?? [];
    public static readonly requires: readonly CapabilityClass[] = options.requires ?? [];

    public async *run(ctx: OperatorContext): AsyncIterable<DataPointEmission> {
      runs += 1;
      seen?.push(ctx);
      for (const item of emitFactory === undefined ? emits : emitFactory(ctx)) {
        yield asEmission(item);
      }
      if (sleepAfterMs !== null) {
        await abortableSleep(sleepAfterMs, ctx.signal);
      }
      if (raiseError !== null) {
        throw raiseError;
      }
      if (runs <= failFirst) {
        throw new Error(`${operatorId} failed on run ${runs}`);
      }
    }
  }

  // Named like the Python stub, so an error message or a log names the operator a reader knows.
  Object.defineProperty(StubOperator, 'name', { value: titleCase(operatorId) });
  operator(StubOperator);
  return StubOperator;
};

/** Create (and register) a stub Aggregator subclass. */
export const makeAggregator = (operatorId: string, options: MakeAggregatorOptions = {}): ConcreteAggregatorClass => {
  const policy = policyFrom(options);
  const { onAggregate } = options;

  class StubAggregator extends Aggregator {
    public static readonly operatorId = OperatorId(operatorId);
    public static readonly policy = policy;
    public static readonly dependsOn: readonly DataPointClass<AnyDataPoint>[] = options.dependsOn ?? [];
    public static readonly uses: readonly DataPointClass<AnyDataPoint>[] = options.uses ?? [];
    public static readonly requires: readonly CapabilityClass[] = options.requires ?? [];
    public static readonly interimRefresh = options.interimRefresh ?? false;
    public static readonly consumes: readonly DataPointClass<AnyDataPoint>[] = options.consumes ?? [];

    public async aggregate(ctx: OperatorContext): Promise<void> {
      await onAggregate?.(ctx);
    }
  }

  Object.defineProperty(StubAggregator, 'name', { value: titleCase(operatorId) });
  operator(StubAggregator);
  return StubAggregator;
};
