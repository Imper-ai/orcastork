/**
 * GRAPH — dependency graph construction, cycle detection, bounded-cycle rule, breaker.
 *
 * @module
 */

import { describe, expect, it } from 'vitest';
import { z } from 'zod';
import type { CapabilityClass } from '../src/orcastork/capabilities/index.js';
import { abstractDataPoint, BaseDataPoint, dataPointType, isSubclass } from '../src/orcastork/datapoints/index.js';
import { UnboundedCycleError } from '../src/orcastork/exceptions.js';
import { effectiveProduces } from '../src/orcastork/graph/builder.js';
import {
  buildGraph,
  CircuitBreaker,
  findCycles,
  restrictToPermitted,
  validateAcyclicOrBounded,
} from '../src/orcastork/graph/index.js';
import { CapabilityId, OperatorId } from '../src/orcastork/ids.js';
import { Operator, type OperatorClass } from '../src/orcastork/operators/index.js';
import { makeCapability } from './doubles/capabilities.js';
import { EmailDataPoint, IpDataPoint, RiskDataPoint, WorkEmailDataPoint } from './doubles/datapoints.js';
import { makeAggregator, makeOperator } from './doubles/operators.js';

/**
 * The caps a breaker was built with — the port of Python's `breaker._caps` reach-in.
 *
 * A breaker deliberately exposes no reader for them (only `isTripped`), so the test reads the
 * private field directly, exactly as the Python suite does.
 */
const capsOf = (breaker: CircuitBreaker): ReadonlyMap<OperatorId, number> =>
  (breaker as unknown as { readonly caps: ReadonlyMap<OperatorId, number> }).caps;

/**
 * Mirror `Orchestrator.buildCircuitBreaker` over a (possibly pruned) gathering set.
 *
 * The orchestrator recomputes cycles on the run-time gathering set and caps only the operators that
 * survive; reproducing that here exercises the graph primitives the live breaker is built from
 * without standing up a whole orchestrator.
 */
const buildBreakerFor = (
  operators: readonly OperatorClass[],
  capabilities: readonly CapabilityClass[],
): CircuitBreaker => {
  const caps = new Map<OperatorId, number>();
  for (const cycle of findCycles(buildGraph(operators, capabilities))) {
    for (const node of cycle) {
      if (isSubclass(node, Operator)) {
        const policy = (node as OperatorClass).policy;
        if (policy.maxCycles !== null) {
          caps.set((node as OperatorClass).operatorId, policy.maxCycles);
        }
      }
    }
  }
  return new CircuitBreaker(caps);
};

describe('dependency graph', () => {
  it('builds nodes and subtype-aware edges', () => {
    const producer = makeOperator('produces_work', { produces: [WorkEmailDataPoint] });
    const consumer = makeOperator('consumes_email', { dependsOn: [EmailDataPoint] }); // subtype-aware

    const edges = buildGraph([producer, consumer], []);

    expect(edges.get(producer)?.has(consumer)).toBe(true);
  });

  it('detects both acyclic and cyclic graphs with Tarjan', () => {
    const a = makeOperator('acyc_a', { produces: [IpDataPoint] });
    const b = makeOperator('acyc_b', { dependsOn: [IpDataPoint] });

    expect(findCycles(buildGraph([a, b], []))).toEqual([]);

    const x = makeOperator('cyc_x', { produces: [IpDataPoint], dependsOn: [RiskDataPoint] });
    const y = makeOperator('cyc_y', { produces: [RiskDataPoint], dependsOn: [IpDataPoint] });

    expect(findCycles(buildGraph([x, y], [])).length).toBeGreaterThan(0);
  });

  it('permits a bounded cycle and rejects an unbounded one', () => {
    const boundedX = makeOperator('b_x', { produces: [IpDataPoint], dependsOn: [RiskDataPoint], maxCycles: 3 });
    const boundedY = makeOperator('b_y', { produces: [RiskDataPoint], dependsOn: [IpDataPoint], maxCycles: 3 });

    expect(() => validateAcyclicOrBounded(buildGraph([boundedX, boundedY], []))).not.toThrow();

    const unboundedY = makeOperator('u_y', { produces: [RiskDataPoint], dependsOn: [IpDataPoint] }); // no maxCycles

    expect(() => validateAcyclicOrBounded(buildGraph([boundedX, unboundedY], []))).toThrow(UnboundedCycleError);
  });

  it('detects a cycle that runs through capabilities', () => {
    const provider = makeCapability('cap_d');
    const consumer = makeCapability('cap_c', { requires: [provider] });
    // Close the requires loop after the fact — the port of the Python test's class-attribute rewrite.
    (provider as { requires: readonly CapabilityClass[] }).requires = [consumer];

    expect(findCycles(buildGraph([], [provider, consumer])).length).toBeGreaterThan(0);
  });

  it('detects a cycle through a capability that requires a DataPoint', () => {
    const cap = makeCapability('cap_needs_ip', { dependsOn: [IpDataPoint] });
    const operator = makeOperator('op_needs_cap', { produces: [IpDataPoint], requires: [cap], maxCycles: 2 });

    const cycles = findCycles(buildGraph([operator], [cap]));

    expect(cycles.some((cycle) => cycle.has(operator) && cycle.has(cap))).toBe(true);
  });

  it('treats aggregators as sinks', () => {
    const producer = makeOperator('feeds_agg', { produces: [IpDataPoint] });
    const aggregatorClass = makeAggregator('sink_agg', { dependsOn: [IpDataPoint] }); // produces nothing

    const edges = buildGraph([producer, aggregatorClass], []);

    expect(edges.get(aggregatorClass)).toEqual(new Set()); // no out-edges
    expect(findCycles(edges).every((cycle) => !cycle.has(aggregatorClass))).toBe(true);
  });

  it('resolves an abstract `produces` to its concrete leaves when drawing edges', () => {
    const producer = makeOperator('produces_email', { produces: [EmailDataPoint] }); // abstract
    const consumer = makeOperator('needs_work', { dependsOn: [WorkEmailDataPoint] }); // concrete leaf

    const edges = buildGraph([producer, consumer], []);

    expect(edges.get(producer)?.has(consumer)).toBe(true);
  });

  it('resolves an UNDECORATED abstract `produces` to its leaves too (the decorator is optional)', () => {
    // PORT-SPECIFIC. Python cannot define a DataPoint class that is neither a registered leaf nor
    // an `__abstract__` intermediate — a concrete leaf with no discriminator raises at definition —
    // so `effective_produces` may read the abstract set. Here an intermediate that is simply never
    // decorated runs no code at all, and the graph must still expand it: an operator producing it
    // would otherwise lose every edge it should draw, silently, and its consumers would never be
    // scheduled.
    abstract class UndecoratedEmailBase extends WorkEmailDataPoint {}

    const producer = makeOperator('produces_undecorated', { produces: [EmailDataPoint] });
    const consumer = makeOperator('needs_undecorated', { dependsOn: [UndecoratedEmailBase] });

    expect(effectiveProduces(makeOperator('probe', { produces: [UndecoratedEmailBase] }))).toEqual(new Set());
    expect(buildGraph([producer, consumer], []).get(producer)?.has(consumer)).toBe(false);

    // …and the shape that matters: an undecorated intermediate ABOVE registered leaves expands.
    abstract class UndecoratedMid extends BaseDataPoint<string> {}
    class LeafBelow extends UndecoratedMid {}
    dataPointType<string>('undecorated_leaf', { pii: false, ephemeral: false }, { value: z.string() })(LeafBelow);
    const midProducer = makeOperator('produces_mid', { produces: [UndecoratedMid] });
    const leafConsumer = makeOperator('needs_leaf', { dependsOn: [LeafBelow] });

    expect(effectiveProduces(midProducer)).toEqual(new Set([LeafBelow]));
    expect(buildGraph([midProducer, leafConsumer], []).get(midProducer)?.has(leafConsumer)).toBe(true);
  });

  it('drops unpermitted capabilities from the per-session subgraph', () => {
    const cap = makeCapability('gated_cap');
    const operator = makeOperator('uses_cap', { requires: [cap] });
    const edges = buildGraph([operator], [cap]);

    const without = restrictToPermitted(edges, new Set());
    expect(without.has(cap)).toBe(false);

    const withPermission = restrictToPermitted(edges, new Set([CapabilityId('gated_cap')]));
    expect(withPermission.has(cap)).toBe(true);
  });

  it('trips the circuit breaker after the cap', () => {
    const loop = OperatorId('loop');
    const breaker = new CircuitBreaker([[loop, 2]]);

    breaker.recordRun(loop);
    expect(breaker.isTripped(loop)).toBe(false);
    breaker.recordRun(loop);
    expect(breaker.isTripped(loop)).toBe(true);
  });

  it('keeps the breaker counter per session', () => {
    const loop = OperatorId('loop');
    const first = new CircuitBreaker([[loop, 1]]);
    const second = new CircuitBreaker([[loop, 1]]);

    first.recordRun(loop);

    expect(first.isTripped(loop)).toBe(true);
    expect(second.isTripped(loop)).toBe(false); // independent across sessions
  });

  it('treats a self-edge as a one-node cycle that still needs a cap', () => {
    const unbounded = makeOperator('self_loop', { produces: [IpDataPoint], dependsOn: [IpDataPoint] });
    const edges = buildGraph([unbounded], []);

    expect(findCycles(edges).some((cycle) => cycle.has(unbounded))).toBe(true);
    expect(() => validateAcyclicOrBounded(edges)).toThrow(UnboundedCycleError);

    const bounded = makeOperator('self_loop_capped', {
      produces: [IpDataPoint],
      dependsOn: [IpDataPoint],
      maxCycles: 1,
    });

    expect(() => validateAcyclicOrBounded(buildGraph([bounded], []))).not.toThrow();
  });

  it('draws no edges for an abstract `produces` with no concrete leaves', () => {
    // An abstract DataPoint with zero registered leaves: the abstract-produces rule must degrade to
    // "produces nothing" — never match the abstract itself or unrelated leaves.
    abstract class OrphanAbstractDataPoint extends BaseDataPoint<string> {}
    abstractDataPoint<string>({ pii: false, ephemeral: false }, { value: z.string() })(OrphanAbstractDataPoint);

    const producer = makeOperator('orphan_producer', { produces: [OrphanAbstractDataPoint] });
    const consumer = makeOperator('orphan_consumer', { dependsOn: [OrphanAbstractDataPoint] });
    const edges = buildGraph([producer, consumer], []);

    expect(effectiveProduces(producer)).toEqual(new Set());
    expect(edges.get(producer)?.has(consumer)).toBe(false);
  });

  it('rejects a capability-only cycle, which has no boundable operator', () => {
    // A cycle made of only capabilities has no operator to attach a maxCycles cap to, so it can
    // never be bounded and must be rejected at deploy time.
    const provider = makeCapability('cap_d');
    const consumer = makeCapability('cap_c', { requires: [provider] });
    (provider as { requires: readonly CapabilityClass[] }).requires = [consumer];

    let thrown: unknown;
    try {
      validateAcyclicOrBounded(buildGraph([], [provider, consumer]));
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toBeInstanceOf(UnboundedCycleError);
    const message = (thrown as Error).message;
    expect(message).toContain('no boundable operator');
    expect(message).toContain(provider.name);
    expect(message).toContain(consumer.name);
  });

  it('drops a breaker cap when pruning the gathering set breaks the cycle', () => {
    // S1: gating one operator out of the run-time gathering set breaks the cycle, so the breaker
    // recomputed on the pruned set must NOT carry a cap for the now-broken cycle.
    const x = makeOperator('s1_x', { produces: [IpDataPoint], dependsOn: [RiskDataPoint], maxCycles: 2 });
    const y = makeOperator('s1_y', { produces: [RiskDataPoint], dependsOn: [IpDataPoint], maxCycles: 2 });

    const fullBreaker = buildBreakerFor([x, y], []);
    expect(capsOf(fullBreaker)).toEqual(
      new Map([
        ['s1_x', 2],
        ['s1_y', 2],
      ]),
    );

    // Namespace gating excludes Y; X no longer participates in any cycle on the pruned set.
    const prunedBreaker = buildBreakerFor([x], []);
    expect(capsOf(prunedBreaker)).toEqual(new Map());
    prunedBreaker.recordRun(x.operatorId);
    prunedBreaker.recordRun(x.operatorId);
    expect(prunedBreaker.isTripped(x.operatorId)).toBe(false); // no spurious trip without a cap
  });

  it('leaves no dangling breaker cap when gating out the capability the cycle enters through', () => {
    // S1 (capability-availability variant): the cycle enters through a capability the namespace does
    // not provide. With that capability pruned the cycle is gone, so its operator must not keep a
    // maxCycles cap that would never arm.
    const cap = makeCapability('s1_cap_needs_ip', { dependsOn: [IpDataPoint] });
    const looped = makeOperator('s1_op_needs_cap', { produces: [IpDataPoint], requires: [cap], maxCycles: 3 });

    const withCap = buildBreakerFor([looped], [cap]);
    expect(capsOf(withCap)).toEqual(new Map([['s1_op_needs_cap', 3]]));

    // Capability unavailable for this namespace: pruned graph has no cycle, hence no cap.
    const withoutCap = buildBreakerFor([looped], []);
    expect(capsOf(withoutCap)).toEqual(new Map());
    withoutCap.recordRun(looped.operatorId);
    expect(withoutCap.isTripped(looped.operatorId)).toBe(false);
  });
});
