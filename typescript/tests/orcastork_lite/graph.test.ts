/** The static graph: edges, cycles, the bounded-cycle rule, permitted subgraphs, backward reachability. */

import { describe, expect, it } from 'vitest';
import {
  backwardReachable,
  buildEdges,
  buildUsesEdges,
  cycleCaps,
  findCycles,
  restrictToPermitted,
  validateAcyclicOrBounded,
} from '../../src/orcastork_lite/graph.js';
import { CapabilityId } from '../../src/orcastork_lite/ids.js';
import type { CapabilityClass, DataPointClass } from '../../src/orcastork_lite/index.js';
import { OperatorId, UnboundedCycleError } from '../../src/orcastork_lite/index.js';
import { Email, Flag, Ip, makeCapability, makeOperator, Risk, WorkEmail } from './fixtures.js';

describe('readiness edges', () => {
  it('follow produces → dependsOn in both subtype directions', () => {
    const abstractProducer = makeOperator('abstract', { produces: [Email] });
    const leafProducer = makeOperator('leaf', { produces: [WorkEmail] });
    const leafConsumer = makeOperator('leaf_consumer', { dependsOn: [WorkEmail] });
    const abstractConsumer = makeOperator('abstract_consumer', { dependsOn: [Email] });
    const unrelated = makeOperator('unrelated', { dependsOn: [Ip] });

    const edges = buildEdges([abstractProducer, leafProducer, leafConsumer, abstractConsumer, unrelated], []);

    expect(edges.get(abstractProducer)).toEqual(new Set([leafConsumer, abstractConsumer]));
    expect(edges.get(leafProducer)).toEqual(new Set([leafConsumer, abstractConsumer]));
    expect(edges.get(unrelated)).toEqual(new Set());
  });

  it('feed a capability to its requirers and to the capabilities layered on it', () => {
    const base = makeCapability('base');
    const layer = makeCapability('layer', { requires: [base] });
    const user = makeOperator('user', { requires: [layer] });

    const edges = buildEdges([user], [base, layer]);

    expect(edges.get(base)).toEqual(new Set([layer]));
    expect(edges.get(layer)).toEqual(new Set([user]));
  });

  it('keep `uses` edges separate and drop self-edges', () => {
    const producer = makeOperator('producer', { produces: [Risk] });
    const folder = makeOperator('folder', { dependsOn: [Ip], uses: [Risk], produces: [Risk] });

    expect(buildUsesEdges([producer, folder])).toEqual(
      new Map([
        [producer, new Set([folder])],
        [folder, new Set()],
      ]),
    );
    // `uses` is not a readiness edge.
    expect(buildEdges([producer, folder], []).get(producer)).toEqual(new Set());
  });
});

describe('the bounded-cycle rule', () => {
  it('rejects an unbounded cycle and reports the caps of a bounded one', () => {
    const a = makeOperator('a', { dependsOn: [Ip], produces: [Risk] });
    const b = makeOperator('b', { dependsOn: [Risk], produces: [Ip] });

    expect(() => validateAcyclicOrBounded(buildEdges([a, b], []))).toThrow(UnboundedCycleError);

    const aBounded = makeOperator('a', { dependsOn: [Ip], produces: [Risk], maxCycles: 3 });
    const bBounded = makeOperator('b', { dependsOn: [Risk], produces: [Ip], maxCycles: 5 });
    const edges = buildEdges([aBounded, bBounded], []);

    expect(() => validateAcyclicOrBounded(edges)).not.toThrow();
    expect(findCycles(edges)).toEqual([new Set([aBounded, bBounded])]);
    expect(cycleCaps(edges)).toEqual(
      new Map([
        [OperatorId('a'), 3],
        [OperatorId('b'), 5],
      ]),
    );
    expect(cycleCaps(buildEdges([makeOperator('lonely', { dependsOn: [Ip], produces: [Risk] })], []))).toEqual(
      new Map(),
    );
  });

  it('counts a self-loop as a cycle, and cannot bound a capability-only one', () => {
    const selfie = makeOperator('selfie', { dependsOn: [Ip], produces: [Ip], maxCycles: 2 });

    expect(findCycles(buildEdges([selfie], []))).toEqual([new Set([selfie])]);

    const x = makeCapability('x');
    const y = makeCapability('y', { requires: [x] });
    // The cycle can only be tied once both classes exist, exactly as the Python test ties it.
    Object.assign(x, { requires: [y] as readonly CapabilityClass[] });

    expect(() => validateAcyclicOrBounded(buildEdges([], [x, y]))).toThrow(UnboundedCycleError);
    expect(() => validateAcyclicOrBounded(buildEdges([], [x, y]))).toThrow(/no boundable operator/);
  });
});

describe('the permitted subgraph', () => {
  it('drops capability nodes only', () => {
    const cap = makeCapability('cap');
    const user = makeOperator('user', { requires: [cap] });
    const edges = buildEdges([user], [cap]);

    const restricted = restrictToPermitted(edges, new Set<CapabilityId>());

    expect([...restricted.keys()]).toEqual([user]);
    expect(restricted.get(user)).toEqual(new Set());
    expect(restrictToPermitted(edges, new Set([CapabilityId('cap')]))).toEqual(edges);
  });
});

describe('backward reachability', () => {
  it('keeps the upstream chain and the capabilities it requires', () => {
    const cap = makeCapability('cap');
    const root = makeOperator('root', { dependsOn: [Flag], produces: [Ip], requires: [cap] });
    const mid = makeOperator('mid', { dependsOn: [Ip], produces: [Risk] });
    const dead = makeOperator('dead', { dependsOn: [Ip], produces: [WorkEmail] });

    expect(backwardReachable([root, mid, dead], [cap], new Set([Risk]))).toEqual(new Set([root, mid, cap]));
    // No declared sinks: nothing is reachable, and the caller reads that as "no pruning".
    expect(backwardReachable([root, mid, dead], [cap], new Set<DataPointClass>())).toEqual(new Set());
  });
});
