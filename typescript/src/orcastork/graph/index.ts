/**
 * Static dependency graph: builder, Tarjan cycle detection, runtime circuit-breaker.
 *
 * @module
 */

export { buildGraph, buildUsesEdges, type EdgeMap, type Node, restrictToPermitted } from './builder.js';
export { CircuitBreaker } from './circuit_breaker.js';
export { findCycles, validateAcyclicOrBounded } from './cycles.js';
export { backwardReachable } from './reachability.js';
