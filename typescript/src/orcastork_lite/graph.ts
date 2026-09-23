/**
 * The static dependency graph over operator + capability classes.
 *
 * A directed edge `X → Y` means "Y depends on something X provides": an operator *produces* a
 * DataPoint type that Y *dependsOn*, or a capability is *required* by Y. Matching is subtype-aware
 * in both directions — a producer of `P` feeds a need for `T` iff one is a subclass of the other —
 * so an abstract `produces` reaches a leaf consumer and a leaf producer reaches an abstract
 * consumer without any registry of concrete leaves.
 *
 * Cycles are detected with Tarjan's SCC algorithm and permitted only if **bounded**: every
 * operator on the cycle declares a `maxCycles` cap. Backward reachability from a set of sink types
 * tells the orchestrator which operators are worth running at all.
 *
 * @module
 */

import type { CapabilityClass } from './capabilities.js';
import { Capability } from './capabilities.js';
import type { DataPointClass } from './datapoints.js';
import { isSubclass } from './datapoints.js';
import { UnboundedCycleError } from './exceptions.js';
import type { CapabilityId, OperatorId } from './ids.js';
import type { OperatorClass } from './operators.js';
import { Operator } from './operators.js';

/** A graph node: an operator class or a capability class. */
export type Node = OperatorClass | CapabilityClass;

/** The readiness graph as an adjacency map, producer → everything it feeds. */
export type EdgeMap = ReadonlyMap<Node, ReadonlySet<Node>>;

const isOperatorNode = (node: Node): node is OperatorClass => isSubclass(node, Operator);

const isCapabilityNode = (node: Node): node is CapabilityClass => isSubclass(node, Capability);

/** Both node kinds declare these; an absent declaration reads as empty, as in Python. */
const dependsOnOf = (node: Node): readonly DataPointClass[] => node.dependsOn ?? [];

const requiresOf = (node: Node): readonly CapabilityClass[] => node.requires ?? [];

const producesOf = (operator: OperatorClass): readonly DataPointClass[] => operator.produces ?? [];

const usesOf = (operator: OperatorClass): readonly DataPointClass[] => operator.uses ?? [];

const overlaps = (produced: DataPointClass, needed: DataPointClass): boolean =>
  isSubclass(produced, needed) || isSubclass(needed, produced);

const feeds = (producer: OperatorClass, neededTypes: Iterable<DataPointClass>): boolean => {
  const needed = [...neededTypes];
  return producesOf(producer).some((produced) => needed.some((need) => overlaps(produced, need)));
};

/** The readiness graph (adjacency map) over operators + capabilities. */
export const buildEdges = (operators: Iterable<OperatorClass>, capabilities: Iterable<CapabilityClass>): EdgeMap => {
  const operatorList = [...operators];
  const capabilityList = [...capabilities];
  const nodes: Node[] = [...operatorList, ...capabilityList];
  const edges = new Map<Node, Set<Node>>(nodes.map((node) => [node, new Set<Node>()]));
  for (const consumer of nodes) {
    for (const producer of operatorList) {
      if (feeds(producer, dependsOnOf(consumer))) {
        edges.get(producer)?.add(consumer);
      }
    }
    for (const required of requiresOf(consumer)) {
      for (const provider of capabilityList) {
        if (isSubclass(provider, required)) {
          edges.get(provider)?.add(consumer);
        }
      }
    }
  }
  return edges;
};

/**
 * Producer→consumer edges for the weaker `uses` relation (rerun triggers, not readiness gates).
 *
 * Kept apart from {@link buildEdges} so the cycle rule reasons over readiness edges only; the
 * graph tool renders these dotted. Self-edges are dropped.
 */
export const buildUsesEdges = (operators: Iterable<OperatorClass>): EdgeMap => {
  const operatorList = [...operators];
  const edges = new Map<Node, Set<Node>>(operatorList.map((operator) => [operator, new Set<Node>()]));
  for (const consumer of operatorList) {
    for (const producer of operatorList) {
      if (producer !== consumer && feeds(producer, usesOf(consumer))) {
        edges.get(producer)?.add(consumer);
      }
    }
  }
  return edges;
};

/** The per-namespace subgraph: drop capability nodes the namespace does not permit (operators stay). */
export const restrictToPermitted = (edges: EdgeMap, permitted: ReadonlySet<CapabilityId>): EdgeMap => {
  const keep = (node: Node): boolean => (isCapabilityNode(node) ? permitted.has(node.capabilityId) : true);
  const restricted = new Map<Node, ReadonlySet<Node>>();
  for (const [node, targets] of edges) {
    if (keep(node)) {
      restricted.set(node, new Set([...targets].filter(keep)));
    }
  }
  return restricted;
};

/** Each strongly-connected component that forms a cycle (size > 1, or a self-loop). */
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

/** Throw {@link UnboundedCycleError} unless every cycle is bounded by `maxCycles` caps. */
export const validateAcyclicOrBounded = (edges: EdgeMap): void => {
  for (const cycle of findCycles(edges)) {
    const operatorsOnCycle = [...cycle].filter(isOperatorNode);
    if (operatorsOnCycle.length === 0) {
      const names = [...cycle].map((node) => node.name).sort();
      throw new UnboundedCycleError(`cycle with no boundable operator: ${names.join(', ')}`);
    }
    const unbounded = operatorsOnCycle.filter((operator) => operator.policy.maxCycles === null);
    if (unbounded.length > 0) {
      const names = unbounded.map((operator) => operator.name).sort();
      throw new UnboundedCycleError(`cycle has operators without a maxCycles circuit-breaker: ${names.join(', ')}`);
    }
  }
};

/** The `maxCycles` cap of every operator that sits on a cycle — what the circuit breaker enforces. */
export const cycleCaps = (edges: EdgeMap): ReadonlyMap<OperatorId, number> => {
  const caps = new Map<OperatorId, number>();
  for (const cycle of findCycles(edges)) {
    for (const node of cycle) {
      if (isOperatorNode(node) && node.policy.maxCycles !== null) {
        caps.set(node.operatorId, node.policy.maxCycles);
      }
    }
  }
  return caps;
};

/**
 * The operators producing (transitively) toward `sinks` + the capabilities they require.
 *
 * Empty `sinks` yields the empty set (the caller treats "no declared sinks" as "no pruning").
 */
export const backwardReachable = (
  operators: Iterable<OperatorClass>,
  capabilities: Iterable<CapabilityClass>,
  sinks: ReadonlySet<DataPointClass>,
): ReadonlySet<Node> => {
  const operatorList = [...operators];
  const capabilityList = [...capabilities];
  const needed = new Set<DataPointClass>(sinks);
  const keptOperators = new Set<OperatorClass>();
  let changed = true;
  while (changed) {
    changed = false;
    for (const operator of operatorList) {
      if (!keptOperators.has(operator) && feeds(operator, needed)) {
        keptOperators.add(operator);
        // Its own gate inputs are needed too, which is what walks the chain further upstream.
        for (const required of dependsOnOf(operator)) {
          needed.add(required);
        }
        changed = true;
      }
    }
  }
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
