/**
 * REG — operator / capability / aggregator auto-registration.
 *
 * The second half covers the DataPoint registry: the version-counter-driven lazy-union cache, the
 * idempotent same-class re-registration branch, the single-leaf and empty registry boundaries, and
 * the production registration lifecycle (declaring leaves accumulates the union; `resetCache`
 * forces a rebuild) that the registry-isolation setup normally hides.
 */

import { describe, expect, it } from 'vitest';
import { z } from 'zod';
import { capabilityRegistry } from '../src/orcastork/capabilities/base.js';
import { abstractDataPoint, abstractDataPointTypes, dataPointRegistry } from '../src/orcastork/datapoints/base.js';
import {
  BaseDataPoint,
  dataPointAdapter,
  dataPointType,
  isSubclass,
  parseDataPoint,
  registeredLeaves,
  registryVersion,
} from '../src/orcastork/datapoints/index.js';
import { resetCache } from '../src/orcastork/datapoints/registry.js';
import { DuplicateRegistrationError, UnknownDataPointTypeError } from '../src/orcastork/exceptions.js';
import { CapabilityId, OperatorId, type OperatorRef } from '../src/orcastork/ids.js';
import type { OperatorPolicyInit } from '../src/orcastork/operators/index.js';
import { Aggregator, Operator, OperatorPolicy } from '../src/orcastork/operators/index.js';
import { makeCapability } from './doubles/capabilities.js';
import { makeAggregator, makeOperator } from './doubles/operators.js';

const T0 = new Date('2026-01-01T00:00:00.000Z');
const OP: OperatorRef = OperatorId('reg_stub_operator');

const plain = { pii: false, ephemeral: false } as const;

// --- operator / capability / aggregator registration -------------------------------------

describe('operator and capability registration', () => {
  it('registers an operator under its operatorId', () => {
    const stub = makeOperator('reg_op');
    expect(Operator.registered().get(OperatorId('reg_op'))).toBe(stub);
  });

  it('registers a capability and an aggregator too', () => {
    const stub = makeCapability('reg_cap');
    const aggregating = makeAggregator('reg_agg');
    expect(capabilityRegistry.get(CapabilityId('reg_cap'))).toBe(stub);
    expect(Operator.registered().get(OperatorId('reg_agg'))).toBe(aggregating); // an aggregator is an operator
  });

  it('rejects a duplicate operatorId at definition', () => {
    makeOperator('reg_dup');
    expect(() => makeOperator('reg_dup')).toThrow(DuplicateRegistrationError);
  });

  it('does not register an abstract base that declares no key', () => {
    const before = new Set(Operator.registered().keys());

    // No `operatorId`, `run` still abstract, and — the port's decisive part — never decorated.
    abstract class AbstractOperator extends Operator {}

    expect(new Set(Operator.registered().keys())).toEqual(before);
    expect(AbstractOperator.prototype).toBeInstanceOf(Operator);
    // The Aggregator base itself is abstract and unregistered.
    expect([...Operator.registered().values()]).not.toContain(Aggregator);
  });

  it('requires an explicit rerun choice on an operator policy', () => {
    // `rerunOnNewData` has NO default; only an untyped caller can omit it, and it is refused there.
    expect(() => OperatorPolicy({} as OperatorPolicyInit)).toThrow(/rerunOnNewData/);
  });

  it('keeps the operator and capability registries isolated', () => {
    makeOperator('reg_iso_op');
    expect(capabilityRegistry.has(CapabilityId('reg_iso_op'))).toBe(false);
    makeCapability('reg_iso_cap');
    expect(Operator.registered().has(OperatorId('reg_iso_cap'))).toBe(false);
  });

  it('accumulates every declared operator in the registry', () => {
    for (let index = 0; index < 3; index += 1) {
      makeOperator(`reg_startup_${index}`);
    }
    const registered = new Set(Operator.registered().keys());
    for (let index = 0; index < 3; index += 1) {
      expect(registered.has(OperatorId(`reg_startup_${index}`))).toBe(true);
    }
  });

  it('makes an aggregator a subtype of operator', () => {
    expect(isSubclass(Aggregator, Operator)).toBe(true);
    const aggregating = makeAggregator('reg_agg_subtype');
    expect(isSubclass(aggregating, Operator)).toBe(true);
    expect(new aggregating()).toBeInstanceOf(Operator);
  });
});

/**
 * Claim one id in each registry with a class declared *inside this call*.
 *
 * Two tests calling it register three genuinely different classes under the same three keys. If
 * `tests/setup.ts` did not restore the registries between them, the second call would hit the
 * duplicate-key policy and throw — which is what makes the pair a real check of isolation rather
 * than a check that runs clean either way.
 */
const claimTheSharedIds = (): { readonly leaf: unknown; readonly operator: unknown; readonly capability: unknown } => {
  class SharedLeafDataPoint extends BaseDataPoint<string> {}
  dataPointType<string>('reg_isolation_leaf', plain, { value: z.string() })(SharedLeafDataPoint);
  return {
    leaf: SharedLeafDataPoint,
    operator: makeOperator('reg_isolation_op'),
    capability: makeCapability('reg_isolation_cap'),
  };
};

describe('registry isolation between tests', () => {
  // PORT-SPECIFIC in shape, not in intent: Python's autouse fixture snapshots the registries around
  // every test, and `tests/setup.ts` is its counterpart. Nothing else asserts that the setup file is
  // actually wired, so these two cases do — a leaked registration from the first would make the
  // second raise `DuplicateRegistrationError`.
  it('leaves the ids it claims free for the next test (first half)', () => {
    const claimed = claimTheSharedIds();
    expect(dataPointRegistry.get('reg_isolation_leaf')).toBe(claimed.leaf);
    expect(Operator.registered().get(OperatorId('reg_isolation_op'))).toBe(claimed.operator);
    expect(capabilityRegistry.get(CapabilityId('reg_isolation_cap'))).toBe(claimed.capability);
  });

  it('claims the very same ids again, with different classes (second half)', () => {
    const claimed = claimTheSharedIds();
    expect(dataPointRegistry.get('reg_isolation_leaf')).toBe(claimed.leaf);
    expect(Operator.registered().get(OperatorId('reg_isolation_op'))).toBe(claimed.operator);
    expect(capabilityRegistry.get(CapabilityId('reg_isolation_cap'))).toBe(claimed.capability);
  });
});

// --- DataPoint registry: idempotent re-registration, union-cache lifecycle, boundaries ----

describe('the DataPoint registry', () => {
  it('re-registers the same class idempotently without raising', () => {
    // A re-import / reload path re-invokes the registration on the IDENTICAL class. Because the
    // registry already maps the discriminator to that same class, re-registration must be benign
    // (no DuplicateRegistrationError) and must leave the mapping pointing at it.
    @dataPointType('reg_dp_reimported', plain, { value: z.string() })
    class ReimportedLeafDataPoint extends BaseDataPoint<string> {}

    expect(dataPointRegistry.get('reg_dp_reimported')).toBe(ReimportedLeafDataPoint);
    const versionBefore = registryVersion();

    // Re-run the exact registration on the same class object (the reload scenario).
    dataPointType('reg_dp_reimported', plain, { value: z.string() })(ReimportedLeafDataPoint);

    expect(dataPointRegistry.get('reg_dp_reimported')).toBe(ReimportedLeafDataPoint); // still the same class
    expect(registryVersion()).toBe(versionBefore + 1); // a harmless re-register still bumps the version
  });

  it('still raises when a different class claims a registered discriminator', () => {
    // The idempotent branch must NOT swallow a genuine collision: a DIFFERENT class claiming an
    // already-registered discriminator is a hard error.
    @dataPointType('reg_dp_collide', plain, { value: z.string() })
    class FirstClaimantDataPoint extends BaseDataPoint<string> {}

    expect(() => {
      @dataPointType('reg_dp_collide', plain, { value: z.string() })
      class SecondClaimantDataPoint extends BaseDataPoint<string> {}
      void SecondClaimantDataPoint;
    }).toThrow(DuplicateRegistrationError);

    expect(dataPointRegistry.get('reg_dp_collide')).toBe(FirstClaimantDataPoint); // the first registrant is untouched
  });

  it('round-trips a single-leaf union and still rejects any other type', () => {
    // With exactly ONE registered leaf the adapter is built from the bare leaf class (a
    // single-member union is degenerate). It must still round-trip that leaf and still reject any
    // other.
    @dataPointType('reg_dp_sole', plain, { value: z.string() })
    class SoleLeafDataPoint extends BaseDataPoint<string> {}

    // Collapse the registry to just this leaf (the isolation setup restores it afterwards).
    dataPointRegistry.clear();
    dataPointRegistry.set('reg_dp_sole', SoleLeafDataPoint);
    resetCache();

    expect(registeredLeaves()).toHaveLength(1);
    // The single-leaf branch parses straight into the bare leaf — a degenerate single-member
    // tagged union is avoided, so the schema is a plain model, not a tagged union (which is what
    // the multi-leaf path would produce).
    expect(dataPointAdapter().schemaKind).toBe('model');
    const sole = new SoleLeafDataPoint({ value: 'x', retrievedBy: OP, firstRetrieved: T0, lastRetrieved: T0 });
    const restored = parseDataPoint(sole.toWire());
    expect(restored.constructor).toBe(SoleLeafDataPoint);
    expect(restored.equals(sole)).toBe(true);

    expect(() =>
      parseDataPoint({
        type: 'other',
        value: 'x',
        retrieved_by: 'op',
        first_retrieved: T0,
        last_retrieved: T0,
      }),
    ).toThrow(UnknownDataPointTypeError);
  });

  it('fails loudly when the registry is empty', () => {
    // The lazy union must fail loudly (not return a degenerate adapter) when nothing is registered.
    dataPointRegistry.clear();
    resetCache();

    expect(() => dataPointAdapter()).toThrow(UnknownDataPointTypeError);
  });

  it('serves the current leaf set after a version bump and after a bare cache reset', () => {
    // The production lifecycle the snapshot/restore harness normally hides: registrations
    // accumulate, the union is rebuilt on a version bump, a reset forces a fresh rebuild, and each
    // rebuild serves exactly the CURRENT leaf set (no stale-cache split-brain).
    dataPointRegistry.clear();
    resetCache();

    @dataPointType('reg_dp_life_a', plain, { value: z.string() })
    class LeafADataPoint extends BaseDataPoint<string> {}

    // First build: only A is registered, so A round-trips and B is unknown.
    const adapterA = dataPointAdapter();
    const a = new LeafADataPoint({ value: 'a', retrievedBy: OP, firstRetrieved: T0, lastRetrieved: T0 });
    expect(adapterA.validate(a.toWire()).constructor).toBe(LeafADataPoint);

    const versionAfterA = registryVersion();

    @dataPointType('reg_dp_life_b', plain, { value: z.string() })
    class LeafBDataPoint extends BaseDataPoint<string> {}

    // Declaring B bumps the version; the cache must rebuild and now pick B too.
    expect(registryVersion()).toBeGreaterThan(versionAfterA);
    const b = new LeafBDataPoint({ value: 'b', retrievedBy: OP, firstRetrieved: T0, lastRetrieved: T0 });
    const rebuilt = dataPointAdapter();
    expect(rebuilt).not.toBe(adapterA); // a version bump invalidates the prior cached adapter
    expect(rebuilt.validate(b.toWire()).constructor).toBe(LeafBDataPoint);
    expect(rebuilt.validate(a.toWire()).constructor).toBe(LeafADataPoint); // A still served

    // A bare reset (no version change) forces a rebuild that still serves the same current set.
    resetCache();
    const afterReset = dataPointAdapter();
    expect(afterReset).not.toBe(rebuilt);
    expect(afterReset.validate(a.toWire()).constructor).toBe(LeafADataPoint);
    expect(afterReset.validate(b.toWire()).constructor).toBe(LeafBDataPoint);
  });

  it('does not leak the abstract marker down to a concrete leaf', () => {
    // An intermediate is abstract only if it is declared so in its OWN right; the marker must not
    // leak via inheritance, so a leaf below an abstract intermediate is a concrete union member.
    @abstractDataPoint({ pii: true, ephemeral: false }, { value: z.string() })
    abstract class AbstractMidDataPoint extends BaseDataPoint<string> {}

    @dataPointType('reg_dp_leaf_under_abstract')
    class ConcreteLeafDataPoint extends AbstractMidDataPoint {}

    expect(abstractDataPointTypes().has(AbstractMidDataPoint)).toBe(true);
    expect(registeredLeaves()).not.toContain(AbstractMidDataPoint);
    expect(registeredLeaves()).toContain(ConcreteLeafDataPoint); // concrete despite the abstract parent
    expect(dataPointRegistry.get('reg_dp_leaf_under_abstract')).toBe(ConcreteLeafDataPoint);
  });
});
