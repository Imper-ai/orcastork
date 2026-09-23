/**
 * Graph-aware quiescence.
 *
 * Gathering is quiescent when **no not-yet-run operator has a producible-input path remaining** —
 * i.e. no pending operator could ever become ready, even after every other pending operator that
 * could run produces its outputs. This is a producibility fixpoint: starting from the present types,
 * repeatedly admit pending operators whose inputs are reachable (and whose capabilities are
 * available), accumulating what they produce. If none are reachable, gathering is quiescent and the
 * aggregation phase may begin.
 *
 * @module
 */

import type { CapabilityClass } from '../capabilities/base.js';
import type { AnyDataPoint, DataPointClass } from '../datapoints/index.js';
import { isSubclass } from '../datapoints/index.js';
import { effectiveProduces } from '../graph/builder.js';
import type { OperatorClass } from '../operators/base.js';
import type { ReadinessInputs } from './readiness.js';

const depsReachable = (operator: OperatorClass, producible: ReadonlySet<DataPointClass<AnyDataPoint>>): boolean =>
  (operator.dependsOn ?? []).every((required) => [...producible].some((candidate) => isSubclass(candidate, required)));

const capsAvailable = (operator: OperatorClass, available: ReadonlySet<CapabilityClass>): boolean =>
  (operator.requires ?? []).every((required) => [...available].some((candidate) => isSubclass(candidate, required)));

/** The pending operators that could still become ready from the current state (the fixpoint). */
export const reachablePending = (
  pendingOperators: Iterable<OperatorClass>,
  inputs: ReadinessInputs,
): ReadonlySet<OperatorClass> => {
  const pending = [...pendingOperators];
  const producible = new Set<DataPointClass<AnyDataPoint>>(inputs.presentTypes);
  const reachable = new Set<OperatorClass>();
  let changed = true;
  while (changed) {
    changed = false;
    for (const operator of pending) {
      if (reachable.has(operator)) {
        continue;
      }
      if (depsReachable(operator, producible) && capsAvailable(operator, inputs.availableCapabilityTypes)) {
        reachable.add(operator);
        for (const produced of effectiveProduces(operator)) {
          producible.add(produced);
        }
        changed = true;
      }
    }
  }
  return reachable;
};

/** True iff no pending operator can ever become ready from the current state. */
export const isQuiescent = (pendingOperators: Iterable<OperatorClass>, inputs: ReadinessInputs): boolean =>
  reachablePending(pendingOperators, inputs).size === 0;
