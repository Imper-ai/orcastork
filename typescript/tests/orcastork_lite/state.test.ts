/** SessionState: keyed-merge, revisions, and the per-operator delta. */

import { describe, expect, it } from 'vitest';
import type { AnyDataPoint } from '../../src/orcastork_lite/index.js';
import { CapabilityId, identityKey, OperatorId } from '../../src/orcastork_lite/index.js';
import { SessionState } from '../../src/orcastork_lite/state.js';
import { FakeClock } from '../doubles/clock.js';
import { dp, Ip, Risk } from './fixtures.js';

const OP = OperatorId('op');
const SECOND = 1_000;

/** `now + delta`, the counterpart of Python's `now + timedelta(...)`. */
const shifted = (at: Date, deltaMs: number): Date => new Date(at.getTime() + deltaMs);

/**
 * DataPoints compared the way Python compares them: by identity, timestamps excluded.
 *
 * A Python `frozenset`/tuple of DataPoints compares through `__eq__`, which is the `(class, value)`
 * identity — so a freshened sighting still equals the one the test built.
 */
const identities = (dataPoints: Iterable<AnyDataPoint>): readonly string[] => [...dataPoints].map(identityKey);

describe('SessionState.merge', () => {
  it('adds, then updates, and advances the revision only when something changed', () => {
    const now = new FakeClock().now();
    const state = new SessionState();

    const first = state.merge([dp(Ip, 'a', now)]);
    expect(first.revision).toBe(1);
    expect(identities(first.added)).toEqual(identities([dp(Ip, 'a', now)]));
    expect(first.updated).toEqual([]);

    // An identical re-observation: nothing changed.
    const same = state.merge([dp(Ip, 'a', now)]);
    expect(same.revision).toBe(1);
    expect(same.changed).toBe(false);

    // A fresher sighting of the same identity: updated, not added.
    const fresher = state.merge([dp(Ip, 'a', shifted(now, SECOND))]);
    expect(fresher.revision).toBe(2);
    expect(identities(fresher.updated)).toEqual(identities([dp(Ip, 'a', now)]));
    expect(fresher.added).toEqual([]);
    expect(state.merge([]).revision).toBe(2);

    const held = state.view().all();
    expect(held).toHaveLength(1);
    expect(held[0]?.firstRetrieved).toEqual(now);
    expect(held[0]?.lastRetrieved).toEqual(shifted(now, SECOND));
  });
});

describe('SessionState.deltaFor', () => {
  it('presents the whole set as added on a first invocation', () => {
    const now = new FakeClock().now();
    const state = new SessionState();
    state.merge([dp(Ip, 'a', now), dp(Risk, 0.1, now)]);

    const delta = state.deltaFor(OP, { previousCaps: new Set(), availableCaps: new Set() });

    expect(delta.isFirstInvocation).toBe(true);
    expect(new Set(identities(delta.added))).toEqual(new Set(identities([dp(Ip, 'a', now), dp(Risk, 0.1, now)])));
    expect(delta.updated.size).toBe(0);
    expect(state.hasRun(OP)).toBe(false);
  });

  it('splits added from updated against the watermark, and reports new capabilities', () => {
    const now = new FakeClock().now();
    const state = new SessionState();
    state.advanceWatermark(OP, state.merge([dp(Ip, 'a', now)]).revision);
    state.merge([dp(Ip, 'a', shifted(now, SECOND)), dp(Ip, 'b', now)]);

    const delta = state.deltaFor(OP, {
      previousCaps: new Set([CapabilityId('x')]),
      availableCaps: new Set([CapabilityId('x'), CapabilityId('y')]),
    });

    expect(delta.isFirstInvocation).toBe(false);
    expect(identities(delta.added)).toEqual(identities([dp(Ip, 'b', now)]));
    expect(identities(delta.updated)).toEqual(identities([dp(Ip, 'a', now)]));
    expect(delta.newlyAvailableCaps).toEqual(new Set([CapabilityId('y')]));
    expect(state.hasRun(OP)).toBe(true);
  });
});
