/**
 * Backward reachability — the operators/capabilities whose output is actually consumed.
 *
 * Given the **sink** DataPoint types a flow ultimately considers/persists (an aggregator's declared
 * `consumes`, plus its own gate inputs and the completion condition), this computes the operators
 * whose output is transitively needed to produce those sinks, and the capabilities those operators
 * require. Everything outside that closure is *dead*: its output feeds neither the aggregator nor any
 * operator the aggregator (transitively) needs, so running it only wastes work and can needlessly
 * hold the session open. The orchestrator prunes the dead operators before gathering.
 *
 * Matching is subtype-aware and mirrors the graph builder's edge rule exactly — a producer of `P`
 * feeds a need for `T` iff `P` is a subclass of `T` — so abstract intermediates and concrete leaves
 * line up the same way the live scheduler resolves readiness.
 *
 * @module
 */

import type { CapabilityClass } from '../capabilities/base.js';
import type { AnyDataPoint, DataPointClass } from '../datapoints/index.js';
import { isSubclass } from '../datapoints/index.js';
import type { OperatorClass } from '../operators/base.js';
import type { Node } from './builder.js';
import { effectiveProduces, requiresOf } from './builder.js';

/**
 * The operators producing (transitively) toward `sinks` + the capabilities they require.
 *
 * Empty `sinks` yields the empty set (the caller treats "no declared sinks" as "no pruning").
 */
export const backwardReachable = (
  operators: Iterable<OperatorClass>,
  capabilities: Iterable<CapabilityClass>,
  sinks: ReadonlySet<DataPointClass<AnyDataPoint>>,
): ReadonlySet<Node> => {
  const operatorList = [...operators];
  const capabilityList = [...capabilities];

  // Backward fixpoint over needed DataPoint types: keep an operator once it produces a needed type,
  // then its own inputs (`dependsOn`) become needed too — pulling its upstream producers in.
  const needed = new Set<DataPointClass<AnyDataPoint>>(sinks);
  const keptOperators = new Set<OperatorClass>();
  let changed = true;
  while (changed) {
    changed = false;
    for (const operator of operatorList) {
      if (keptOperators.has(operator)) {
        continue;
      }
      const produces = [...effectiveProduces(operator)];
      if (produces.some((produced) => [...needed].some((need) => isSubclass(produced, need)))) {
        keptOperators.add(operator);
        for (const required of operator.dependsOn ?? []) {
          needed.add(required);
        }
        changed = true;
      }
    }
  }

  // A capability is kept iff a kept node requires it (subtype-aware), transitively across layering.
  const keptCapabilities = new Set<CapabilityClass>();
  changed = true;
  while (changed) {
    changed = false;
    const requirers: Node[] = [...keptOperators, ...keptCapabilities];
    for (const requirer of requirers) {
      for (const required of requiresOf(requirer)) {
        for (const provider of capabilityList) {
          if (!keptCapabilities.has(provider) && isSubclass(provider, required)) {
            keptCapabilities.add(provider);
            changed = true;
          }
        }
      }
    }
  }

  return new Set<Node>([...keptOperators, ...keptCapabilities]);
};
