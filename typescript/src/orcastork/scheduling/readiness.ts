/**
 * Operator readiness — the data-driven trigger.
 *
 * An operator is **ready** iff all its `dependsOn` DataPoint types are present (subtype-aware: a
 * present leaf satisfies a base-type dependency) and all its `requires` capabilities are available.
 * An operator with an empty `dependsOn` is ready at session start. Readiness is recomputed whenever
 * the present types or available capabilities change (operator emissions and inbox arrivals both
 * feed this), so the last missing dependency triggers the operator immediately — no phase wait.
 *
 * @module
 */

import type { CapabilityClass } from '../capabilities/base.js';
import type { AnyDataPoint, DataPointClass } from '../datapoints/index.js';
import { isSubclass } from '../datapoints/index.js';
import type { OperatorId } from '../ids.js';
import type { OperatorClass } from '../operators/base.js';

/**
 * What readiness is judged against: the session's present DataPoint types and the capability
 * classes that are currently available.
 *
 * The port of Python's keyword-only `present_types` / `available_capability_types` pair — one
 * options object, so the two can never be passed the wrong way round.
 */
export interface ReadinessInputs {
  /** The DataPoint classes the session currently holds. */
  readonly presentTypes: ReadonlySet<DataPointClass<AnyDataPoint>>;

  /** The classes of the capabilities that are currently available. */
  readonly availableCapabilityTypes: ReadonlySet<CapabilityClass>;
}

/**
 * Exactly which declared inputs are keeping an operator from running.
 *
 * An operator that never ran is otherwise indistinguishable from one that ran and found nothing,
 * which matters when the output is a verdict: "not checked" and "checked, clean" are very different
 * claims. This carries the gate's own reason so a report can state which.
 */
export interface ReadinessGap {
  /** Declared `dependsOn` types no present type satisfies, sorted by class name. */
  readonly missingDataPoints: readonly DataPointClass<AnyDataPoint>[];

  /** Declared `requires` capabilities no available provider satisfies, sorted by class name. */
  readonly missingCapabilities: readonly CapabilityClass[];

  /** Whether nothing is missing — the readiness verdict itself. */
  readonly isReady: boolean;
}

/** Sorted by name so a stored report diffs cleanly between runs (code-unit order, never locale). */
const byName = (left: { readonly name: string }, right: { readonly name: string }): number => {
  if (left.name === right.name) {
    return 0;
  }
  return left.name < right.name ? -1 : 1;
};

/**
 * The unsatisfied half of `operator`'s declared inputs, subtype-aware like the gate itself.
 *
 * {@link isReady} is defined in terms of this rather than the two conditions being written twice, so
 * an explanation can never disagree with the decision it explains.
 */
export const readinessGap = (operator: OperatorClass, inputs: ReadinessInputs): ReadinessGap => {
  const present = [...inputs.presentTypes];
  const available = [...inputs.availableCapabilityTypes];
  const missingDataPoints = (operator.dependsOn ?? [])
    .filter((required) => !present.some((candidate) => isSubclass(candidate, required)))
    .sort(byName);
  const missingCapabilities = (operator.requires ?? [])
    .filter((required) => !available.some((candidate) => isSubclass(candidate, required)))
    .sort(byName);
  return Object.freeze({
    missingDataPoints: Object.freeze(missingDataPoints),
    missingCapabilities: Object.freeze(missingCapabilities),
    isReady: missingDataPoints.length === 0 && missingCapabilities.length === 0,
  });
};

/** Whether `operator` can run given the present DataPoint types + available capabilities. */
export const isReady = (operator: OperatorClass, inputs: ReadinessInputs): boolean =>
  readinessGap(operator, inputs).isReady;

/** {@link ReadinessInputs} plus the operators that have already run this session. */
export interface ReadyOperatorsOptions extends ReadinessInputs {
  /** Operators the orchestrator has already launched; omitted means none have. */
  readonly alreadyRun?: ReadonlySet<OperatorId>;
}

/** All not-yet-run operators that are currently ready (the orchestrator runs them together). */
export const readyOperators = (
  operators: Iterable<OperatorClass>,
  options: ReadyOperatorsOptions,
): readonly OperatorClass[] => {
  const alreadyRun = options.alreadyRun;
  return [...operators].filter(
    (operator) => !(alreadyRun?.has(operator.operatorId) ?? false) && isReady(operator, options),
  );
};
