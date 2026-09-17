/**
 * Debounce / coalescing of reruns + the rerun-eligibility decision.
 *
 * When depended-on data arrives for a `rerunOnNewData` operator, the rerun is debounced on a
 * coalescing window — a small global default, overridable per operator (`OperatorPolicy.debounceMs`).
 * Multiple arrivals within the window collapse into a single rerun; a window of zero reruns
 * immediately. Time is read from the injected clock, and the window is **loop-scheduled**: the
 * controller only records when a rerun becomes due, so the gathering loop stays in charge of time
 * and nothing here ever sleeps in a task of its own.
 *
 * An armed window normally holds the gathering loop open until it comes due — that is what makes the
 * coalescing real rather than advisory. {@link windowDefersToFinalize} names the one case where it
 * must not.
 *
 * @module
 */

import type { Clock } from '../clock.js';
import { isSubclass } from '../datapoints/index.js';
import type { OperatorId } from '../ids.js';
import { Aggregator, type AggregatorStatics } from '../operators/aggregator.js';
import type { OperatorClass, OperatorPolicy } from '../operators/base.js';

/** No window at all: a rerun armed without one is due the moment it is armed. */
export const DEFAULT_DEBOUNCE_MS = 0;

/** Whether relevant new data actually arrived for the operator whose policy is being consulted. */
export interface RerunEligibilityInputs {
  readonly hasRelevantNewData: boolean;
}

/** A completed operator reruns only if its policy opts in AND relevant data arrived. */
export const rerunEligible = (policy: OperatorPolicy, inputs: RerunEligibilityInputs): boolean =>
  policy.rerunOnNewData && inputs.hasRelevantNewData;

/** The completion verdict this pass reached — the one thing the exemption turns on. */
export interface FinalizeProximity {
  readonly completionSatisfied: boolean;
}

/**
 * Whether an armed-but-not-due window may be abandoned rather than hold gathering open.
 *
 * True for exactly one shape: an `interimRefresh` aggregator whose session already satisfies its
 * completion condition. Reaching a would-be-quiescent pass in that state means the next thing that
 * happens is the aggregation phase, whose finalize pass rewrites the very document the refold would
 * produce — so waiting out the window buys a durable write no reader can observe while charging its
 * full width to every session's completion tail. Nothing is lost by abandoning it: the finalize
 * folds the same data, authoritatively.
 *
 * Every other window still holds. An ordinary operator's rerun feeds the finalize with data it would
 * otherwise never see, and a retry's window IS its backoff — collapsing it would spend the retry
 * budget before the fault it is waiting out could clear. An interim window under an *unsatisfied*
 * condition holds too: that session is heading for an inbox wait of unbounded length, where the
 * interim write is the only live view a reader gets.
 */
export const windowDefersToFinalize = (operator: OperatorClass, proximity: FinalizeProximity): boolean =>
  proximity.completionSatisfied &&
  isSubclass(operator, Aggregator) &&
  ((operator as AggregatorStatics).interimRefresh ?? false);

/** How long a window lasts when it is armed without one of its own. */
export interface DebounceControllerOptions {
  readonly defaultWindowMs?: number;
}

/** A window's width, in milliseconds; `null` (or omitted) takes the controller's default. */
export interface WindowOptions {
  readonly windowMs?: number | null;
}

/** Tracks, per operator, the monotonic time at which a coalesced rerun becomes due. */
export class DebounceController {
  private readonly clock: Clock;
  private readonly defaultWindowMs: number;
  private readonly dueAtMs = new Map<OperatorId, number>();

  public constructor(clock: Clock, options: DebounceControllerOptions = {}) {
    this.clock = clock;
    this.defaultWindowMs = options.defaultWindowMs ?? DEFAULT_DEBOUNCE_MS;
  }

  /** (Re)arm a rerun for `operatorId`; arrivals within the window coalesce. */
  public schedule(operatorId: OperatorId, options: WindowOptions = {}): void {
    const effective = options.windowMs ?? this.defaultWindowMs;
    this.dueAtMs.set(operatorId, this.clock.monotonic() + effective);
  }

  /** Whether a (coalesced) rerun is already armed — so arrivals within the window don't re-arm it. */
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

  /** Consume a due rerun (called once the orchestrator has re-invoked the operator). */
  public clear(operatorId: OperatorId): void {
    this.dueAtMs.delete(operatorId);
  }
}
