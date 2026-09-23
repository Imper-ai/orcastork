/**
 * Scheduling primitives: readiness, coalescing windows, the cycle breaker, retry backoff.
 *
 * Readiness is the data-driven trigger: an operator is ready iff all its `dependsOn` types are
 * present (subtype-aware) and all its `requires` capabilities are available. Reruns and retries
 * are both **loop-scheduled windows** on the injected clock ({@link DebounceController}) — never
 * an in-task sleep — so the gathering loop stays in charge of time.
 *
 * @module
 */

import { crc32 } from 'node:zlib';
import type { CapabilityClass } from './capabilities.js';
import type { Clock } from './clock.js';
import type { DataPointClass } from './datapoints.js';
import { isSubclass } from './datapoints.js';
import type { OperatorId } from './ids.js';
import type { OperatorClass, RetryPolicy } from './operators.js';

/** No window at all: a rerun armed without one is due the moment it is armed. */
export const DEFAULT_DEBOUNCE_MS = 0;

/** What readiness is judged against — the session's present data and available capabilities. */
export interface ReadinessInputs {
  /** The concrete DataPoint classes the session holds. */
  readonly presentTypes: ReadonlySet<DataPointClass>;

  /** The classes of the capabilities that are currently available. */
  readonly availableCapabilityTypes: ReadonlySet<CapabilityClass>;
}

/** Whether `operator` can run given the present DataPoint types + available capabilities. */
export const isReady = (operator: OperatorClass, inputs: ReadinessInputs): boolean => {
  const dataReady = (operator.dependsOn ?? []).every((required) =>
    [...inputs.presentTypes].some((present) => isSubclass(present, required)),
  );
  const capsReady = (operator.requires ?? []).every((required) =>
    [...inputs.availableCapabilityTypes].some((available) => isSubclass(available, required)),
  );
  return dataReady && capsReady;
};

/** How long a window lasts when it is armed without one of its own. */
export interface DebounceControllerOptions {
  readonly defaultWindowMs?: number;
}

/** A window's width, in milliseconds; `null` (or omitted) takes the controller's default. */
export interface WindowOptions {
  readonly windowMs?: number | null;
}

/** Tracks, per operator, the monotonic time at which a coalesced rerun (or a retry) becomes due. */
export class DebounceController {
  private readonly clock: Clock;
  private readonly defaultWindowMs: number;
  private readonly dueAtMs = new Map<OperatorId, number>();

  public constructor(clock: Clock, options: DebounceControllerOptions = {}) {
    this.clock = clock;
    this.defaultWindowMs = options.defaultWindowMs ?? DEFAULT_DEBOUNCE_MS;
  }

  /** (Re)arm a window for `operatorId`; arrivals within it coalesce. */
  public schedule(operatorId: OperatorId, options: WindowOptions = {}): void {
    const effective = options.windowMs ?? this.defaultWindowMs;
    this.dueAtMs.set(operatorId, this.clock.monotonic() + effective);
  }

  public isScheduled(operatorId: OperatorId): boolean {
    return this.dueAtMs.has(operatorId);
  }

  /** The monotonic instant this operator's window comes due, or `null` when none is armed. */
  public dueAt(operatorId: OperatorId): number | null {
    return this.dueAtMs.get(operatorId) ?? null;
  }

  public isDue(operatorId: OperatorId): boolean {
    const due = this.dueAtMs.get(operatorId);
    return due !== undefined && this.clock.monotonic() >= due;
  }

  /** Consume a window (once the orchestrator has launched the operator). */
  public clear(operatorId: OperatorId): void {
    this.dueAtMs.delete(operatorId);
  }
}

/** Bounds a permitted cycle per session: an operator at its `maxCycles` cap is *tripped*. */
export class CircuitBreaker {
  private readonly caps: ReadonlyMap<OperatorId, number>;
  private readonly counts = new Map<OperatorId, number>();

  public constructor(caps: Iterable<readonly [OperatorId, number]>) {
    this.caps = new Map(caps);
  }

  public recordRun(operatorId: OperatorId): void {
    this.counts.set(operatorId, (this.counts.get(operatorId) ?? 0) + 1);
  }

  /** True once a capped operator has run its cap; an uncapped operator never trips. */
  public isTripped(operatorId: OperatorId): boolean {
    const cap = this.caps.get(operatorId);
    return cap !== undefined && (this.counts.get(operatorId) ?? 0) >= cap;
  }
}

/** A stable jitter seed derived from e.g. `(sessionId, operatorId)`. */
export const seedFor = (...parts: string[]): number => crc32(parts.join('|'));

/**
 * A deterministic pseudo-random source for one backoff schedule.
 *
 * Python seeds `random.Random(seed)`; JavaScript's `Math.random` cannot be seeded, so the package
 * carries this one small generator instead. Only the *property* matters — the same seed yields the
 * same schedule, different seeds spread two operators' relaunches apart — and nothing about a
 * backoff window is ever compared across languages or processes.
 */
const seededRandom = (seed: number): (() => number) => {
  let state = seed >>> 0;
  return (): number => {
    state = (state + 0x6d2b79f5) >>> 0;
    let mixed = Math.imul(state ^ (state >>> 15), state | 1);
    mixed ^= mixed + Math.imul(mixed ^ (mixed >>> 7), mixed | 61);
    return ((mixed ^ (mixed >>> 14)) >>> 0) / 4_294_967_296;
  };
};

/** Which schedule to draw — the seed that makes it reproducible. */
export interface BackoffOptions {
  readonly seed: number;
}

/** The jittered exponential backoff schedule, in milliseconds — deterministic for a given seed. */
export const backoffDelays = (policy: RetryPolicy, options: BackoffOptions): readonly number[] => {
  const random = seededRandom(options.seed);
  const delays: number[] = [];
  for (let attempt = 0; attempt < policy.maxAttempts; attempt += 1) {
    // `uniform(-jitter, jitter)`: a draw is taken even at zero jitter, so the schedule's shape
    // does not depend on whether jitter is configured.
    const jitter = -policy.jitter + random() * 2 * policy.jitter;
    delays.push(policy.baseDelayMs * 2 ** attempt * (1 + jitter));
  }
  return Object.freeze(delays);
};
