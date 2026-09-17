/**
 * SCHED — data-driven readiness and graph-aware quiescence.
 *
 * @module
 */

import { describe, expect, it } from 'vitest';
import type { CapabilityClass } from '../src/orcastork/capabilities/index.js';
import type { AnyDataPoint, DataPointClass } from '../src/orcastork/datapoints/index.js';
import {
  isQuiescent,
  isReady,
  reachablePending,
  readinessGap,
  readyOperators,
} from '../src/orcastork/scheduling/index.js';
import { makeCapability } from './doubles/capabilities.js';
import {
  ChatAnswerDataPoint,
  EmailDataPoint,
  IpDataPoint,
  RiskDataPoint,
  WorkEmailDataPoint,
} from './doubles/datapoints.js';
import { makeOperator } from './doubles/operators.js';

const NO_CAPS: ReadonlySet<CapabilityClass> = new Set();

/** `frozenset({...})` of DataPoint classes, spelled so a test reads like the Python original. */
const present = (...types: readonly DataPointClass<AnyDataPoint>[]): ReadonlySet<DataPointClass<AnyDataPoint>> =>
  new Set(types);

/** `frozenset({...})` of capability classes. */
const caps = (...types: readonly CapabilityClass[]): ReadonlySet<CapabilityClass> => new Set(types);

describe('readiness and quiescence', () => {
  it('is ready iff every dependency is present and every capability is available', () => {
    const cap = makeCapability('idp');
    const operator = makeOperator('o', { dependsOn: [WorkEmailDataPoint], requires: [cap] });

    expect(isReady(operator, { presentTypes: present(), availableCapabilityTypes: NO_CAPS })).toBe(false);
    expect(isReady(operator, { presentTypes: present(WorkEmailDataPoint), availableCapabilityTypes: NO_CAPS })).toBe(
      false,
    );
    expect(isReady(operator, { presentTypes: present(WorkEmailDataPoint), availableCapabilityTypes: caps(cap) })).toBe(
      true,
    );
  });

  it('triggers on the last missing dependency, with no phase wait', () => {
    const operator = makeOperator('o', { dependsOn: [WorkEmailDataPoint, IpDataPoint] });

    expect(isReady(operator, { presentTypes: present(WorkEmailDataPoint), availableCapabilityTypes: NO_CAPS })).toBe(
      false,
    );
    expect(
      isReady(operator, {
        presentTypes: present(WorkEmailDataPoint, IpDataPoint),
        availableCapabilityTypes: NO_CAPS,
      }),
    ).toBe(true);
  });

  it('is quiescent when no producible-input path remains', () => {
    const blocked = makeOperator('needs_risk', { dependsOn: [RiskDataPoint] }); // nothing produces Risk

    expect(isQuiescent([blocked], { presentTypes: present(), availableCapabilityTypes: NO_CAPS })).toBe(true);
  });

  it('is ready at session start when it depends on nothing', () => {
    const seedOnly = makeOperator('seed', { dependsOn: [] });

    expect(isReady(seedOnly, { presentTypes: present(), availableCapabilityTypes: NO_CAPS })).toBe(true);
  });

  it('accepts a subtype for a base-type dependency', () => {
    const operator = makeOperator('needs_email', { dependsOn: [EmailDataPoint] });

    expect(isReady(operator, { presentTypes: present(WorkEmailDataPoint), availableCapabilityTypes: NO_CAPS })).toBe(
      true,
    );
  });

  it('returns every simultaneously ready operator', () => {
    const operators = [0, 1, 2].map((index) => makeOperator(`seed_${index}`));

    const ready = readyOperators(operators, { presentTypes: present(), availableCapabilityTypes: NO_CAPS });

    expect(ready).toHaveLength(3);
  });

  it('waits until a required capability comes online', () => {
    const cap = makeCapability('idp');
    const operator = makeOperator('needs_cap', { requires: [cap] });

    expect(isReady(operator, { presentTypes: present(), availableCapabilityTypes: NO_CAPS })).toBe(false);
    expect(isReady(operator, { presentTypes: present(), availableCapabilityTypes: caps(cap) })).toBe(true);
  });

  it('is not quiescent while a satisfiable path remains', () => {
    const producer = makeOperator('produces_ip', { produces: [IpDataPoint] }); // empty deps → runnable now
    const consumer = makeOperator('needs_ip', { dependsOn: [IpDataPoint] });

    expect(isQuiescent([producer, consumer], { presentTypes: present(), availableCapabilityTypes: NO_CAPS })).toBe(
      false,
    );
  });

  it('is quiescent when what remains is permanently unsatisfiable', () => {
    const consumer = makeOperator('needs_ip', { dependsOn: [IpDataPoint] }); // no producer present or pending

    expect(isQuiescent([consumer], { presentTypes: present(), availableCapabilityTypes: NO_CAPS })).toBe(true);
  });

  it('re-evaluates readiness after an upstream emission', () => {
    const consumer = makeOperator('needs_ip', { dependsOn: [IpDataPoint] });

    expect(isReady(consumer, { presentTypes: present(), availableCapabilityTypes: NO_CAPS })).toBe(false);
    // An upstream operator emitted an IpDataPoint → readiness re-evaluates to ready.
    expect(isReady(consumer, { presentTypes: present(IpDataPoint), availableCapabilityTypes: NO_CAPS })).toBe(true);
  });

  it('re-evaluates readiness after an inbox arrival', () => {
    const scorer = makeOperator('scores_chat', { dependsOn: [ChatAnswerDataPoint], rerunOnNewData: true });

    expect(isReady(scorer, { presentTypes: present(), availableCapabilityTypes: NO_CAPS })).toBe(false);
    // A chat answer arrived via the inbox and was merged → readiness re-evaluates to ready.
    expect(isReady(scorer, { presentTypes: present(ChatAnswerDataPoint), availableCapabilityTypes: NO_CAPS })).toBe(
      true,
    );
  });

  it('names the operators behind quiescence', () => {
    const producer = makeOperator('produces_ip', { produces: [IpDataPoint] }); // empty deps → runnable now
    const consumer = makeOperator('needs_ip', { dependsOn: [IpDataPoint] }); // reachable through the producer
    const orphan = makeOperator('needs_chat', { dependsOn: [ChatAnswerDataPoint] }); // nothing can ever produce this

    const reachable = reachablePending([producer, consumer, orphan], {
      presentTypes: present(),
      availableCapabilityTypes: NO_CAPS,
    });

    expect(reachable).toEqual(new Set([producer, consumer]));
    expect(isQuiescent([orphan], { presentTypes: present(), availableCapabilityTypes: NO_CAPS })).toBe(true);
  });

  it('respects capability gates when computing what is still reachable', () => {
    const cap = makeCapability('idp');
    const gated = makeOperator('gated', { requires: [cap] });

    expect(reachablePending([gated], { presentTypes: present(), availableCapabilityTypes: NO_CAPS })).toEqual(
      new Set(),
    );
    expect(reachablePending([gated], { presentTypes: present(), availableCapabilityTypes: caps(cap) })).toEqual(
      new Set([gated]),
    );
  });

  it('names the missing inputs in the readiness gap', () => {
    // "Never ran" and "ran and found nothing" are indistinguishable without this, and downstream that
    // is the difference between "not checked" and "checked, clean".
    const cap = makeCapability('idp');
    const operator = makeOperator('o', { dependsOn: [WorkEmailDataPoint, IpDataPoint], requires: [cap] });

    const gap = readinessGap(operator, { presentTypes: present(IpDataPoint), availableCapabilityTypes: NO_CAPS });

    expect(gap.isReady).toBe(false);
    expect(gap.missingDataPoints).toEqual([WorkEmailDataPoint]);
    expect(gap.missingCapabilities).toEqual([cap]);
  });

  it('reports an empty gap when the operator is ready', () => {
    const cap = makeCapability('idp');
    const operator = makeOperator('o', { dependsOn: [WorkEmailDataPoint], requires: [cap] });

    const gap = readinessGap(operator, {
      presentTypes: present(WorkEmailDataPoint),
      availableCapabilityTypes: caps(cap),
    });

    expect(gap.isReady).toBe(true);
    expect(gap.missingDataPoints).toEqual([]);
    expect(gap.missingCapabilities).toEqual([]);
  });

  it('makes the gap subtype-aware, like the gate', () => {
    const operator = makeOperator('needs_email', { dependsOn: [EmailDataPoint] });

    const gap = readinessGap(operator, {
      presentTypes: present(WorkEmailDataPoint),
      availableCapabilityTypes: NO_CAPS,
    });

    expect(gap.missingDataPoints).toEqual([]);
  });

  it('never lets the gap disagree with the readiness decision', () => {
    // The explanation and the decision must not drift: isReady is defined in terms of the gap, and
    // this pins that for every combination of satisfied/unsatisfied data and capability inputs.
    const cap = makeCapability('idp');
    const operator = makeOperator('o', { dependsOn: [WorkEmailDataPoint, IpDataPoint], requires: [cap] });

    for (const presentTypes of [present(), present(IpDataPoint), present(WorkEmailDataPoint, IpDataPoint)]) {
      for (const availableCapabilityTypes of [NO_CAPS, caps(cap)]) {
        const gap = readinessGap(operator, { presentTypes, availableCapabilityTypes });
        expect(gap.isReady, `disagreement for present=${[...presentTypes].map((leaf) => leaf.name).join(',')}`).toBe(
          isReady(operator, { presentTypes, availableCapabilityTypes }),
        );
      }
    }
  });

  it('orders the gap stably', () => {
    // The gap is stored in a report, so it has to diff cleanly rather than following set order.
    const operator = makeOperator('o', { dependsOn: [WorkEmailDataPoint, IpDataPoint, RiskDataPoint] });

    const gap = readinessGap(operator, { presentTypes: present(), availableCapabilityTypes: NO_CAPS });

    expect(gap.missingDataPoints.map((leaf) => leaf.name)).toEqual(
      [WorkEmailDataPoint, IpDataPoint, RiskDataPoint].map((leaf) => leaf.name).sort(),
    );
  });
});
