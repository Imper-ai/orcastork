/**
 * FLOW — FlowDefinition: one named definition per flow + a stable graph-shape fingerprint.
 *
 * @module
 */

import { describe, expect, it } from 'vitest';
import { RetryPolicy } from '../src/orcastork/aggregation/index.js';
import { capabilityRegistry } from '../src/orcastork/capabilities/base.js';
import { Capability } from '../src/orcastork/capabilities/index.js';
import { capabilityName, FlowDefinition } from '../src/orcastork/flow.js';
import type { CapabilityId } from '../src/orcastork/ids.js';
import { OperatorId as makeOperatorId } from '../src/orcastork/ids.js';
import { operatorRegistry } from '../src/orcastork/operators/base.js';
import type { ConcreteOperatorClass } from '../src/orcastork/operators/index.js';
import { RerunOn } from '../src/orcastork/operators/index.js';
import { allOf, anyOf, describeCondition, TypePresent } from '../src/orcastork/scheduling/index.js';
import { makeCapability } from './doubles/capabilities.js';
import {
  ChatAnswerDataPoint,
  EmailDataPoint,
  IpDataPoint,
  RiskDataPoint,
  WorkEmailDataPoint,
} from './doubles/datapoints.js';
import type { MakeOperatorOptions } from './doubles/operators.js';
import { makeOperator } from './doubles/operators.js';

/**
 * Re-register `operatorId` with new declarations — the deploy-changed-the-operator case.
 *
 * Dropping the entry first is the port of the Python suite's
 * `Operator._registry.pop(...)  # noqa: SLF001` — a deliberate reach-in, in a test, to simulate a
 * redeploy that redefines an operator under the same id.
 */
const redefined = (operatorId: string, options: MakeOperatorOptions = {}): ConcreteOperatorClass => {
  operatorRegistry.delete(makeOperatorId(operatorId));
  return makeOperator(operatorId, options);
};

describe('flow fingerprints', () => {
  it('is a stable sha256 hexdigest', () => {
    const flow = new FlowDefinition({ name: 'f', operators: [makeOperator('op', { dependsOn: [EmailDataPoint] })] });

    expect(flow.fingerprint()).toBe(flow.fingerprint());
    expect(flow.fingerprint()).toMatch(/^[0-9a-f]{64}$/);
  });

  it('is insensitive to declaration order', () => {
    const opA = makeOperator('a', { dependsOn: [EmailDataPoint], produces: [RiskDataPoint] });
    const opB = makeOperator('b', { dependsOn: [RiskDataPoint], produces: [IpDataPoint] });
    const capA = makeCapability('cap-a', { dependsOn: [EmailDataPoint] });
    const capB = makeCapability('cap-b', { dependsOn: [IpDataPoint] });

    const forward = new FlowDefinition({ name: 'f', operators: [opA, opB], capabilities: [capA, capB] });
    const backward = new FlowDefinition({ name: 'f', operators: [opB, opA], capabilities: [capB, capA] });

    expect(forward.fingerprint()).toBe(backward.fingerprint());
  });

  it('changes when an operator is added or removed', () => {
    const opA = makeOperator('a', { dependsOn: [EmailDataPoint] });
    const opB = makeOperator('b', { dependsOn: [RiskDataPoint] });

    const smaller = new FlowDefinition({ name: 'f', operators: [opA] });
    const larger = new FlowDefinition({ name: 'f', operators: [opA, opB] });

    expect(smaller.fingerprint()).not.toBe(larger.fingerprint());
  });

  it('changes when a dependency changes', () => {
    const original = new FlowDefinition({
      name: 'f',
      operators: [makeOperator('op', { dependsOn: [EmailDataPoint] })],
    }).fingerprint();
    const redeployed = new FlowDefinition({
      name: 'f',
      operators: [redefined('op', { dependsOn: [IpDataPoint] })],
    }).fingerprint();

    expect(original).not.toBe(redeployed);
  });

  it('changes when a scheduling policy knob changes', () => {
    const variants = [
      new FlowDefinition({
        name: 'f',
        operators: [redefined('op', { dependsOn: [IpDataPoint], rerunOnNewData: false })],
      }).fingerprint(),
      new FlowDefinition({
        name: 'f',
        operators: [redefined('op', { dependsOn: [IpDataPoint], rerunOnNewData: true })],
      }).fingerprint(),
      new FlowDefinition({
        name: 'f',
        operators: [redefined('op', { dependsOn: [IpDataPoint], rerunOnNewData: true, rerunOn: RerunOn.ADDED_ONLY })],
      }).fingerprint(),
      new FlowDefinition({
        name: 'f',
        operators: [redefined('op', { dependsOn: [IpDataPoint], rerunOnNewData: true, maxCycles: 3 })],
      }).fingerprint(),
    ];

    expect(new Set(variants).size).toBe(variants.length); // every scheduling knob is part of the identity
  });

  it('ignores runtime configuration', () => {
    const operator = makeOperator('op', { dependsOn: [EmailDataPoint] });
    const plain = new FlowDefinition({ name: 'f', operators: [operator] });
    const tuned = new FlowDefinition({
      name: 'renamed', // the name identifies the flow to humans, not to the graph
      operators: [operator],
      retryPolicy: RetryPolicy({ maxAttempts: 2, baseDelayMs: 0 }),
      parkAfterMs: 20_000,
      operationTimeoutMs: 1_000,
      sessionDeadlineMs: 60_000,
      maxInboxDeliveries: 2,
      emissionQueueSize: 16,
    });

    expect(plain.fingerprint()).toBe(tuned.fingerprint());
  });

  it('changes with capability declarations', () => {
    const operator = makeOperator('op', { dependsOn: [EmailDataPoint] });
    const capA = makeCapability('cap-a', { dependsOn: [EmailDataPoint] });
    const capB = makeCapability('cap-b', { dependsOn: [IpDataPoint] });

    const one = new FlowDefinition({ name: 'f', operators: [operator], capabilities: [capA] });
    const two = new FlowDefinition({ name: 'f', operators: [operator], capabilities: [capA, capB] });

    expect(one.fingerprint()).not.toBe(two.fingerprint());
  });

  it('normalizes a bare completion type', () => {
    const operator = makeOperator('op', { dependsOn: [EmailDataPoint] });
    const bare = new FlowDefinition({ name: 'f', operators: [operator], completesWhen: RiskDataPoint });
    const explicit = new FlowDefinition({
      name: 'f',
      operators: [operator],
      completesWhen: new TypePresent(RiskDataPoint),
    });
    const unconditioned = new FlowDefinition({ name: 'f', operators: [operator] });

    expect(bare.fingerprint()).toBe(explicit.fingerprint()); // the shorthand and the AST are one condition
    expect(bare.fingerprint()).not.toBe(unconditioned.fingerprint());
  });

  it('distinguishes completion conditions', () => {
    const operator = makeOperator('op', { dependsOn: [EmailDataPoint] });
    const conjunction = new FlowDefinition({
      name: 'f',
      operators: [operator],
      completesWhen: allOf(RiskDataPoint, ChatAnswerDataPoint),
    });
    const disjunction = new FlowDefinition({
      name: 'f',
      operators: [operator],
      completesWhen: anyOf(RiskDataPoint, ChatAnswerDataPoint),
    });

    expect(conjunction.fingerprint()).not.toBe(disjunction.fingerprint());
  });

  it('renders a condition with class names and no object forms', () => {
    const condition = allOf(RiskDataPoint, anyOf(ChatAnswerDataPoint, new TypePresent(EmailDataPoint)));

    const text = describeCondition(condition);

    expect(text).toBe(
      'AllOf(TypePresent(RiskDataPoint), AnyOf(TypePresent(ChatAnswerDataPoint), TypePresent(EmailDataPoint)))',
    );
    // No class-object or default-instance forms: a class's default string form is its whole source.
    expect(text).not.toContain('class ');
    expect(text).not.toContain(' at 0x');
  });

  it('carries the name and fingerprint in the flow identity', () => {
    const flow = new FlowDefinition({ name: 'sample-flow', operators: [makeOperator('op')] });

    const identity = flow.identity();

    expect(identity.name).toBe('sample-flow');
    expect(identity.fingerprint).toBe(flow.fingerprint());
  });

  it('changes when an operator requires a different capability', () => {
    // The capability *family* an operator consumes is part of the graph-shape identity: a deploy that
    // rewires `requires` while the operator id/dependsOn/produces stay identical must change the
    // digest (so a resume audits FLOW_DRIFT_DETECTED), and two flows differing only in `requires`
    // must not collide.
    const capA = makeCapability('cap-a', { dependsOn: [EmailDataPoint] });
    const capB = makeCapability('cap-b', { dependsOn: [IpDataPoint] });
    // Both capabilities are registered in BOTH flows, so the capability section is identical — the
    // ONLY difference is which capability family the operator declares it `requires`.
    const caps = [capA, capB];

    const consumesA = new FlowDefinition({
      name: 'f',
      operators: [redefined('op', { dependsOn: [EmailDataPoint], requires: [capA] })],
      capabilities: caps,
    }).fingerprint();
    const consumesAAgain = new FlowDefinition({
      name: 'f',
      operators: [redefined('op', { dependsOn: [EmailDataPoint], requires: [capA] })],
      capabilities: caps,
    }).fingerprint();
    const consumesB = new FlowDefinition({
      name: 'f',
      operators: [redefined('op', { dependsOn: [EmailDataPoint], requires: [capB] })],
      capabilities: caps,
    }).fingerprint();

    expect(consumesA).toBe(consumesAAgain); // same `requires` → stable digest, every process
    expect(consumesA).not.toBe(consumesB); // the only difference is the consumed capability family
  });

  it("changes when a capability's own wiring changes", () => {
    // A capability's own dependsOn/requires are part of the graph shape too: changing only a
    // capability's declarations (operators untouched) must still move the digest.
    const leafA = makeCapability('leaf-a', { dependsOn: [EmailDataPoint] });
    const leafB = makeCapability('leaf-b', { dependsOn: [EmailDataPoint] });
    const operator = makeOperator('op', { dependsOn: [EmailDataPoint] });

    const dependsOnEmail = makeCapability('wired', { dependsOn: [EmailDataPoint] });
    const dependsOnEmailFp = new FlowDefinition({
      name: 'f',
      operators: [operator],
      capabilities: [dependsOnEmail],
    }).fingerprint();

    capabilityRegistry.delete(dependsOnEmail.capabilityId);
    const dependsOnIp = makeCapability('wired', { dependsOn: [IpDataPoint] }); // only dependsOn changed
    const dependsOnIpFp = new FlowDefinition({
      name: 'f',
      operators: [operator],
      capabilities: [dependsOnIp],
    }).fingerprint();

    expect(dependsOnEmailFp).not.toBe(dependsOnIpFp); // a changed capability dependsOn is drift

    const requiresA = makeCapability('wired2', { dependsOn: [EmailDataPoint], requires: [leafA] });
    const requiresAFp = new FlowDefinition({
      name: 'f',
      operators: [operator],
      capabilities: [requiresA, leafA, leafB],
    }).fingerprint();

    capabilityRegistry.delete(requiresA.capabilityId);
    const requiresB = makeCapability('wired2', { dependsOn: [EmailDataPoint], requires: [leafB] }); // only requires
    const requiresBFp = new FlowDefinition({
      name: 'f',
      operators: [operator],
      capabilities: [requiresB, leafA, leafB],
    }).fingerprint();

    expect(requiresAFp).not.toBe(requiresBFp); // a changed capability `requires` is drift too
  });

  it('hashes the exact text the Python implementation hashes (cross-language resume parity)', () => {
    // PORT-SPECIFIC. The fingerprint is persisted per session and compared on resume, so a session
    // started by a Python worker must resume under a TypeScript one without reading as drift. This
    // pins the hashed lines byte for byte — Python's `True`/`False`/`None` spellings included — and
    // the digest, computed independently with `hashlib.sha256` over:
    //
    //   operator op_a depends_on=[EmailDataPoint,WorkEmailDataPoint] produces=[RiskDataPoint] …
    //   operator op_b depends_on=[] produces=[] requires=[] rerun_on_new_data=False …
    //   capability cap-a depends_on=[IpDataPoint] requires=[]
    //   capability cap-b depends_on=[] requires=[cap-a]
    //   completes_when AllOf(TypePresent(RiskDataPoint), TypePresent(WorkEmailDataPoint))
    const capA = makeCapability('cap-a', { dependsOn: [IpDataPoint] });
    const capB = makeCapability('cap-b', { requires: [capA] });
    const opA = makeOperator('op_a', {
      dependsOn: [EmailDataPoint, WorkEmailDataPoint],
      produces: [RiskDataPoint],
      requires: [capA, capB],
      rerunOnNewData: true,
      rerunOn: RerunOn.ADDED_ONLY,
      maxCycles: 3,
    });
    const opB = makeOperator('op_b');

    const flow = new FlowDefinition({
      name: 'risk-report',
      operators: [opA, opB],
      capabilities: [capB, capA],
      completesWhen: allOf(RiskDataPoint, WorkEmailDataPoint),
    });

    expect(flow.fingerprint()).toBe('5248974c4f7b427bc6764f61f6de6349c386b0f4ced18c60424f5bdb84d04c17');
  });

  it('names an abstract capability by its class name, stably', () => {
    // An abstract intermediate (no capabilityId, deliberately unregistered) must contribute a stable
    // name via its class name — never a per-process object form — so a cross-pod resume of a flow
    // listing it never falsely reads as drift.
    abstract class AbstractProvider extends Capability {
      public abstract fetch(): Promise<void>;
    }

    // The no-id fallback branch is the one exercised.
    expect((AbstractProvider as { capabilityId?: CapabilityId }).capabilityId).toBeUndefined();

    const operator = makeOperator('op', { dependsOn: [EmailDataPoint] });
    const first = new FlowDefinition({ name: 'f', operators: [operator], capabilities: [AbstractProvider] });
    const second = new FlowDefinition({ name: 'f', operators: [operator], capabilities: [AbstractProvider] });

    expect(first.fingerprint()).toBe(second.fingerprint()); // two instances ⇒ identical (cross-process)

    const fallbackName = capabilityName(AbstractProvider);
    expect(fallbackName.endsWith('AbstractProvider')).toBe(true); // named by its class, not an object form
    expect(fallbackName).not.toContain('0x');
    expect(fallbackName).not.toContain('class ');

    // Including the abstract capability must actually move the digest off the no-capability
    // baseline, proving the fallback name is what reached the hashed lines (not silently dropped).
    const baseline = new FlowDefinition({ name: 'f', operators: [operator] }).fingerprint();
    expect(first.fingerprint()).not.toBe(baseline);
  });
});
