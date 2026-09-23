/**
 * Fixtures and doubles for the orcastork_lite suite.
 *
 * There are no registries to isolate: the factories build plain subclasses and hand them straight
 * to the orchestrator, so every test owns exactly the classes it creates.
 *
 * @module
 */

import { CapabilityId } from '../../src/orcastork_lite/ids.js';
import type {
  AnyDataPoint,
  CapabilityCatalog,
  CapabilityClass,
  CapabilityStatics,
  ConcreteCapabilityClass,
  ConcreteDataPointClass,
  ConcreteOperatorClass,
  Credentials,
  DataPointClass,
  DataPointEmission,
  OperatorContext,
  OperatorStatics,
  RetryPolicy,
  SessionResult,
} from '../../src/orcastork_lite/index.js';
import {
  buildRuntime,
  Capability,
  type CapabilityContext,
  DataPoint,
  DEFAULT_OPERATION_TIMEOUT_MS,
  NamespaceId,
  Operator,
  OperatorId,
  OperatorPolicy,
  Orchestrator,
  RerunOn,
  SessionId,
} from '../../src/orcastork_lite/index.js';
import type { FakeClock } from '../doubles/clock.js';

export const NAMESPACE = NamespaceId('ns');
export const SESSION = SessionId('s');
export const SEED = OperatorId('seed');

// --- a small DataPoint zoo -----------------------------------------------------------------------
export class Ip extends DataPoint<string> {}

export class Risk extends DataPoint<number> {}

export class Flag extends DataPoint<boolean> {}

/** An intermediate: operators may depend on it, leaves below it satisfy the dependency. */
export class Email extends DataPoint<string> {}

export class WorkEmail extends Email {}

export class PersonalEmail extends Email {}

/** Build a DataPoint of `leaf` observed at `at` — the test-side counterpart of an emission. */
export const dp = <T extends AnyDataPoint>(
  leaf: ConcreteDataPointClass<T>,
  value: T['value'],
  at: Date,
  options: { readonly by?: OperatorId } = {},
): T => new leaf({ value, retrievedBy: options.by ?? SEED, firstRetrieved: at, lastRetrieved: at });

// --- capability factory --------------------------------------------------------------------------
/** The typed shape every stub capability shares: remembers its credentials, exposes one action. */
export abstract class StubCapability extends Capability {
  public creds: Credentials = {};

  public async token(): Promise<string> {
    const token = this.creds.token;
    return token === undefined ? '' : String(token);
  }
}

/** What {@link makeCapability} hands back: a constructible stub class with its statics declared. */
export interface StubCapabilityClass extends CapabilityStatics {
  new (): StubCapability;
}

/** How a stub capability differs from the plain one. */
export interface MakeCapabilityOptions {
  readonly dependsOn?: readonly DataPointClass[];
  readonly requires?: readonly CapabilityClass[];

  /** The family base, for provider families: several stubs below one abstract capability. */
  readonly base?: abstract new () => StubCapability;

  /** Extra work inside `activate` — record the context, raise, or hang. */
  readonly onActivate?: (ctx: CapabilityContext) => Promise<void>;

  /** Collects this capability's id on every activation, so a test can assert order and count. */
  readonly recordOrder?: string[];
}

/** The class name Python's `make_capability` gives a stub: `work_email` → `WorkEmail`. */
const titleCase = (identifier: string): string =>
  identifier
    .split('_')
    .map((part) => (part === '' ? part : `${part[0]?.toUpperCase() ?? ''}${part.slice(1)}`))
    .join('');

/** A stub capability (optionally below `base`, for provider families) that records activation order. */
export const makeCapability = (capabilityId: string, options: MakeCapabilityOptions = {}): StubCapabilityClass => {
  const base = options.base ?? StubCapability;

  class Stub extends base {
    public static readonly capabilityId = CapabilityId(capabilityId);
    public static readonly dependsOn: readonly DataPointClass[] = options.dependsOn ?? [];
    public static readonly requires: readonly CapabilityClass[] = options.requires ?? [];

    public async activate(ctx: CapabilityContext): Promise<void> {
      options.recordOrder?.push(capabilityId);
      await options.onActivate?.(ctx);
      this.creds = { ...ctx.credentials };
    }
  }

  // Named like the Python stub, so an error message or a log names the capability a reader knows.
  Object.defineProperty(Stub, 'name', { value: titleCase(capabilityId) });
  return Stub;
};

// --- operator factory ---------------------------------------------------------------------------
/** What {@link makeOperator} hands back: a constructible stub class with its statics declared. */
export interface StubOperatorClass extends OperatorStatics {
  new (): Operator;
}

/** How a stub operator differs from the plain one; the policy knobs mirror {@link OperatorPolicy}. */
export interface MakeOperatorOptions {
  readonly dependsOn?: readonly DataPointClass[];
  readonly uses?: readonly DataPointClass[];
  readonly produces?: readonly DataPointClass[];
  readonly requires?: readonly CapabilityClass[];
  readonly consumes?: readonly DataPointClass[];
  readonly rerunOnNewData?: boolean;
  readonly rerunOn?: RerunOn;
  readonly debounceMs?: number | null;
  readonly maxCycles?: number | null;
  readonly timeoutMs?: number | null;
  readonly retry?: RetryPolicy | null;

  /** Emitted on every run, in order. */
  readonly emits?: Iterable<DataPointEmission>;

  /** Emissions computed from the context instead — what a rerun test needs to see its delta. */
  readonly emitFactory?: (ctx: OperatorContext) => Iterable<DataPointEmission>;

  /** Stay in flight this long after emitting, so a real timeout bound or the deadline can fire. */
  readonly sleepAfterMs?: number | null;

  /** Raise this on every run. */
  readonly raiseError?: Error | null;

  /** Raise on that many runs before succeeding (retry tests). */
  readonly failFirst?: number;

  /** Collects every context the stub was invoked with, so a test can inspect deltas. */
  readonly seen?: OperatorContext[];
}

/**
 * Wait `ms` of REAL time, abandoning the wait as soon as `signal` aborts.
 *
 * The counterpart of the Python stub's `asyncio.sleep(...)`, and deliberately not a `Clock` sleep:
 * it exists to hold a run genuinely in flight against a bound that runs on real time (an operation
 * timeout, the clipped deadline). The timer is `unref`'d and cleared on abort, so a test never
 * waits it out and a stray timer never holds the worker open.
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

/** A stub operator: emits `emits` (or `emitFactory(ctx)`), optionally sleeps, optionally raises. */
export const makeOperator = (operatorId: string, options: MakeOperatorOptions = {}): StubOperatorClass => {
  const policy = OperatorPolicy({
    rerunOnNewData: options.rerunOnNewData ?? false,
    rerunOn: options.rerunOn ?? RerunOn.ADDED_OR_UPDATED,
    debounceMs: options.debounceMs ?? null,
    maxCycles: options.maxCycles ?? null,
    timeoutMs: options.timeoutMs ?? null,
    retry: options.retry ?? null,
  });
  // Materialized once, as the Python factory does, so a generator passed as `emits` is not spent
  // by the first run.
  const emits = [...(options.emits ?? [])];
  const { emitFactory, failFirst = 0, raiseError = null, seen, sleepAfterMs = null } = options;
  let runs = 0;

  class Stub extends Operator {
    public static readonly operatorId = OperatorId(operatorId);
    public static readonly policy = policy;
    public static readonly dependsOn: readonly DataPointClass[] = options.dependsOn ?? [];
    public static readonly uses: readonly DataPointClass[] = options.uses ?? [];
    public static readonly produces: readonly DataPointClass[] = options.produces ?? [];
    public static readonly requires: readonly CapabilityClass[] = options.requires ?? [];
    public static readonly consumes: readonly DataPointClass[] = options.consumes ?? [];

    public async *run(ctx: OperatorContext): AsyncIterable<DataPointEmission> {
      runs += 1;
      seen?.push(ctx);
      for (const emission of emitFactory === undefined ? emits : emitFactory(ctx)) {
        yield emission;
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
  Object.defineProperty(Stub, 'name', { value: titleCase(operatorId) });
  return Stub;
};

// --- one-call session runner ---------------------------------------------------------------------
/** How one `runSession` differs from the plain one; every part is optional, as in Python. */
export interface RunSessionOptions {
  readonly capabilities?: readonly ConcreteCapabilityClass[];
  readonly seed?: readonly AnyDataPoint[];
  readonly catalog?: CapabilityCatalog;
  readonly namespaceId?: NamespaceId;
  readonly operationTimeoutMs?: number;
}

/** Run one session on `clock` and hand back its result — the shape most tests need. */
export const runSession = async (
  clock: FakeClock,
  operators: readonly ConcreteOperatorClass[],
  options: RunSessionOptions = {},
): Promise<SessionResult> =>
  await new Orchestrator({
    sessionId: SESSION,
    namespaceId: options.namespaceId ?? NAMESPACE,
    // `exactOptionalPropertyTypes` refuses an explicit `undefined` catalog, so it is spread in only
    // when the test supplied one — which is what leaves `buildRuntime` its own default.
    runtime: buildRuntime(clock, options.catalog === undefined ? {} : { catalog: options.catalog }),
    operators,
    capabilities: options.capabilities ?? [],
    seed: options.seed ?? [],
    operationTimeoutMs: options.operationTimeoutMs ?? DEFAULT_OPERATION_TIMEOUT_MS,
  }).run();
