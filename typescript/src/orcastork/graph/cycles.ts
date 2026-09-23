/**
 * Cycle detection (Tarjan SCC) + the deploy-time bounded-cycle rule.
 *
 * CI builds the graph and flags every cycle. A cycle is **permitted only if bounded** — every
 * operator on it must declare a `maxCycles` circuit-breaker cap (and the cycle must contain at least
 * one operator to bound). An unbounded cycle throws {@link UnboundedCycleError}, the same posture as
 * rejecting bad config at startup but earlier.
 *
 * @module
 */

import { isSubclass } from '../datapoints/index.js';
import { UnboundedCycleError } from '../exceptions.js';
import { Operator, type OperatorClass } from '../operators/base.js';
import type { EdgeMap, Node } from './builder.js';

const isOperatorNode = (node: Node): node is OperatorClass => isSubclass(node, Operator);

/** Return each strongly-connected component that forms a cycle (size > 1, or a self-loop). */
export const findCycles = (edges: EdgeMap): readonly ReadonlySet<Node>[] => {
  let indexCounter = 0;
  const indices = new Map<Node, number>();
  const lowlinks = new Map<Node, number>();
  const stack: Node[] = [];
  const onStack = new Set<Node>();
  const cycles: ReadonlySet<Node>[] = [];

  const strongconnect = (node: Node): void => {
    indices.set(node, indexCounter);
    lowlinks.set(node, indexCounter);
    indexCounter += 1;
    stack.push(node);
    onStack.add(node);
    for (const successor of edges.get(node) ?? []) {
      if (!indices.has(successor)) {
        strongconnect(successor);
        lowlinks.set(node, Math.min(lowlinks.get(node) ?? 0, lowlinks.get(successor) ?? 0));
      } else if (onStack.has(successor)) {
        lowlinks.set(node, Math.min(lowlinks.get(node) ?? 0, indices.get(successor) ?? 0));
      }
    }
    if (lowlinks.get(node) === indices.get(node)) {
      const component: Node[] = [];
      for (;;) {
        const member = stack.pop();
        if (member === undefined) {
          break;
        }
        onStack.delete(member);
        component.push(member);
        if (member === node) {
          break;
        }
      }
      if (component.length > 1 || (edges.get(node)?.has(node) ?? false)) {
        cycles.push(new Set(component));
      }
    }
  };

  for (const node of edges.keys()) {
    if (!indices.has(node)) {
      strongconnect(node);
    }
  }
  return cycles;
};

/** Sorted by name, so the rejection message is identical run to run (code-unit order, not locale). */
const sortedNames = (nodes: Iterable<{ readonly name: string }>): string =>
  [...nodes]
    .map((node) => node.name)
    .sort((left, right) => (left === right ? 0 : left < right ? -1 : 1))
    .join(', ');

/** Throw {@link UnboundedCycleError} unless every cycle is bounded by circuit-breaker caps. */
export const validateAcyclicOrBounded = (edges: EdgeMap): void => {
  for (const cycle of findCycles(edges)) {
    const operatorsOnCycle = [...cycle].filter(isOperatorNode);
    if (operatorsOnCycle.length === 0) {
      throw new UnboundedCycleError(`cycle with no boundable operator: ${sortedNames(cycle)}`);
    }
    const unbounded = operatorsOnCycle.filter((candidate) => candidate.policy.maxCycles === null);
    if (unbounded.length > 0) {
      throw new UnboundedCycleError(
        `cycle has operators without a maxCycles circuit-breaker: ${sortedNames(unbounded)}`,
      );
    }
  }
};
