/**
 * REACHABILITY — backward-reachable operator/capability pruning from a sink (consumed) type set.
 *
 * The closure: keep an operator iff its output is (transitively, subtype-aware) consumed toward the
 * sinks; keep a capability iff a kept node requires it. Operators producing only datapoints nothing
 * in the closure consumes are dropped, so they never run.
 *
 * @module
 */

import { describe, expect, it } from 'vitest';
import type { AnyDataPoint, DataPointClass } from '../src/orcastork/datapoints/index.js';
import { backwardReachable } from '../src/orcastork/graph/index.js';
import { makeCapability } from './doubles/capabilities.js';
import { EmailDataPoint, IpDataPoint, RiskDataPoint, WorkEmailDataPoint } from './doubles/datapoints.js';
import { makeOperator } from './doubles/operators.js';

/** `frozenset({...})` of sink types, spelled so a test reads like the Python original. */
const sinks = (...types: readonly DataPointClass<AnyDataPoint>[]): ReadonlySet<DataPointClass<AnyDataPoint>> =>
  new Set(types);

describe('backward reachability', () => {
  it('keeps an operator producing a sink type', () => {
    const producer = makeOperator('r_keep_direct', { produces: [IpDataPoint] });

    expect(backwardReachable([producer], [], sinks(IpDataPoint)).has(producer)).toBe(true);
  });

  it('prunes an operator whose output feeds nothing', () => {
    const consumed = makeOperator('r_consumed', { produces: [IpDataPoint] });
    const dead = makeOperator('r_dead', { produces: [RiskDataPoint] }); // nothing in the closure reads Risk

    const kept = backwardReachable([consumed, dead], [], sinks(IpDataPoint));

    expect(kept.has(consumed)).toBe(true);
    expect(kept.has(dead)).toBe(false);
  });

  it('keeps the transitive upstream chain', () => {
    // sink = RiskDataPoint; detector produces Risk dependsOn Ip; collector produces Ip; unrelated is dead.
    const detector = makeOperator('r_detector', { produces: [RiskDataPoint], dependsOn: [IpDataPoint] });
    const collector = makeOperator('r_collector', { produces: [IpDataPoint] });
    const unrelated = makeOperator('r_unrelated', { produces: [EmailDataPoint] });

    const kept = backwardReachable([detector, collector, unrelated], [], sinks(RiskDataPoint));

    expect(kept.has(detector)).toBe(true);
    expect(kept.has(collector)).toBe(true);
    expect(kept.has(unrelated)).toBe(false);
  });

  it('matches a sink subtype-aware', () => {
    // An abstract sink (EmailDataPoint) is satisfied by a concrete-leaf (WorkEmail) producer.
    const producer = makeOperator('r_subtype', { produces: [WorkEmailDataPoint] });

    expect(backwardReachable([producer], [], sinks(EmailDataPoint)).has(producer)).toBe(true);
  });

  it('keeps only the capabilities kept operators require', () => {
    const capKept = makeCapability('r_cap_kept');
    const capDead = makeCapability('r_cap_dead');
    const keeper = makeOperator('r_uses_cap', { produces: [IpDataPoint], requires: [capKept] });
    const dead = makeOperator('r_dead_uses_cap', { produces: [RiskDataPoint], requires: [capDead] });

    const kept = backwardReachable([keeper, dead], [capKept, capDead], sinks(IpDataPoint));

    expect(kept.has(keeper)).toBe(true);
    expect(kept.has(capKept)).toBe(true);
    expect(kept.has(dead)).toBe(false);
    expect(kept.has(capDead)).toBe(false);
  });

  it('keeps a transitively layered capability', () => {
    const baseCap = makeCapability('r_cap_base');
    const layerCap = makeCapability('r_cap_layer', { requires: [baseCap] });
    const op = makeOperator('r_uses_layer', { produces: [IpDataPoint], requires: [layerCap] });

    const kept = backwardReachable([op], [baseCap, layerCap], sinks(IpDataPoint));

    expect(kept.has(op)).toBe(true);
    expect(kept.has(layerCap)).toBe(true);
    expect(kept.has(baseCap)).toBe(true);
  });

  it('keeps nothing when there are no sinks', () => {
    const op = makeOperator('r_anything', { produces: [IpDataPoint] });

    expect(backwardReachable([op], [], sinks())).toEqual(new Set());
  });
});
