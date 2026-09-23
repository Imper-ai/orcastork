/**
 * PRUNING — the orchestrator runs only the backward-reachable closure of the aggregator's consumes.
 *
 * When an aggregator declares `consumes`, operators whose output nothing in that closure considers
 * must never run. With no `consumes` declared, every operator runs (backward-compatible).
 */

import { describe, expect, it } from 'vitest';
import type { DataPointView } from '../src/orcastork/datapoints/index.js';
import type { NamespaceId } from '../src/orcastork/ids.js';
import { OperatorId, SessionId, NamespaceId as toNamespaceId } from '../src/orcastork/ids.js';
import type { ConcreteAggregatorClass, ConcreteOperatorClass } from '../src/orcastork/operators/index.js';
import { Orchestrator, SessionStatus } from '../src/orcastork/orchestrator/index.js';
import { buildInMemoryRuntime } from '../src/orcastork/runtime.js';
import type { CompletionCondition } from '../src/orcastork/scheduling/index.js';
import { allOf, anyOf, normalizeCompletion, referencedTypes } from '../src/orcastork/scheduling/index.js';
import { FakeClock } from './doubles/clock.js';
import { EmailDataPoint, IpDataPoint, ip, RiskDataPoint, risk, workEmail } from './doubles/datapoints.js';
import { makeAggregator, makeOperator } from './doubles/operators.js';

const NAMESPACE: NamespaceId = toNamespaceId('prune-namespace');

const useful = (): ConcreteOperatorClass => makeOperator('prune_useful', { produces: [IpDataPoint], emits: [ip()] });

const detector = (): ConcreteOperatorClass =>
  makeOperator('prune_detector', { dependsOn: [IpDataPoint], produces: [RiskDataPoint], emits: [risk()] });

/** Produces a type nothing in the aggregator's closure consumes. */
const dead = (): ConcreteOperatorClass =>
  makeOperator('prune_dead', { produces: [EmailDataPoint], emits: [workEmail()] });

describe('referencedTypes', () => {
  it('is empty for no completion condition', () => {
    expect(referencedTypes(null)).toEqual(new Set());
  });

  it('unwraps a TypePresent', () => {
    expect(referencedTypes(normalizeCompletion(IpDataPoint))).toEqual(new Set([IpDataPoint]));
  });

  it('walks nested combinators', () => {
    const condition = allOf(IpDataPoint, anyOf(RiskDataPoint, EmailDataPoint));
    expect(referencedTypes(condition)).toEqual(new Set([IpDataPoint, RiskDataPoint, EmailDataPoint]));
  });

  it('returns null for an opaque custom condition', () => {
    // A custom CompletionCondition the AST can't introspect — the caller must treat this as
    // "types unknown" and refuse to prune rather than risk dropping a completion producer.
    class Opaque implements CompletionCondition {
      public isSatisfied(_view: DataPointView): boolean {
        return true;
      }
    }

    expect(referencedTypes(new Opaque())).toBeNull();
  });
});

describe('operator pruning', () => {
  it('prunes a dead operator when the aggregator declares consumes', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const agg: ConcreteAggregatorClass = makeAggregator('prune_agg', {
      dependsOn: [RiskDataPoint],
      consumes: [RiskDataPoint],
    });
    const result = await new Orchestrator({
      sessionId: SessionId('prune-on'),
      namespaceId: NAMESPACE,
      runtime,
      operators: [useful(), detector(), dead(), agg],
      capabilities: [],
      seed: [],
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(result.operatorRuns.get(OperatorId('prune_useful'))).toBe(1); // feeds the detector → kept
    expect(result.operatorRuns.get(OperatorId('prune_detector'))).toBe(1); // produces the consumed Risk → kept
    expect(result.operatorRuns.has(OperatorId('prune_dead'))).toBe(false); // output consumed by nothing → pruned
  });

  it('runs every operator when nothing declares consumes', async () => {
    const runtime = buildInMemoryRuntime(new FakeClock());
    const agg: ConcreteAggregatorClass = makeAggregator('compat_agg', { dependsOn: [RiskDataPoint] }); // no pruning
    const result = await new Orchestrator({
      sessionId: SessionId('prune-off'),
      namespaceId: NAMESPACE,
      runtime,
      operators: [useful(), detector(), dead(), agg],
      capabilities: [],
      seed: [],
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    expect(result.operatorRuns.get(OperatorId('prune_dead'))).toBe(1); // nothing declared → dead still runs
  });
});
