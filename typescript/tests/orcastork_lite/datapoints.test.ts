/** DataPoint identity, the view's subtype-aware queries, and value-only emissions. */

import { describe, expect, it } from 'vitest';
import {
  DataPoint,
  DataPointView,
  identityKey,
  OperatorId,
  UnhashableValueError,
} from '../../src/orcastork_lite/index.js';
import { FakeClock } from '../doubles/clock.js';
import { dp, Email, Ip, PersonalEmail, Risk, SEED, WorkEmail } from './fixtures.js';

const SECOND = 1_000;
const HOUR = 3_600 * SECOND;

/** `now + delta`, the counterpart of Python's `now + timedelta(...)`. */
const shifted = (at: Date, deltaMs: number): Date => new Date(at.getTime() + deltaMs);

describe('DataPoint identity', () => {
  it('keys identity on class and value, excluding timestamps', () => {
    const now = new FakeClock().now();
    const a = dp(Ip, '1.1.1.1', now);
    const b = dp(Ip, '1.1.1.1', shifted(now, HOUR), { by: OperatorId('other') });

    expect(a.equals(b)).toBe(true);
    expect(identityKey(a)).toBe(identityKey(b));
    expect(dp(Ip, '2.2.2.2', now).equals(a)).toBe(false);
    // Same value, different class.
    expect(dp(WorkEmail, 'x', now).equals(dp(PersonalEmail, 'x', now))).toBe(false);
    // Two classes may share a name; the identity is keyed on the class, as Python keys it on the
    // class object, so a name is never enough to merge them.
    const first = class Duplicate extends DataPoint<string> {};
    const second = class Duplicate extends DataPoint<string> {};
    expect(dp(first, 'x', now).equals(dp(second, 'x', now))).toBe(false);
  });

  it('treats object values as order-insensitive for identity', () => {
    class Blob extends DataPoint<Record<string, unknown>> {}

    const now = new FakeClock().now();

    expect(dp(Blob, { a: 1, b: [1, 2] }, now).equals(dp(Blob, { b: [1, 2], a: 1 }, now))).toBe(true);
  });

  it('computes the identity once, at construction, and carries it through a re-observation', () => {
    class Blob extends DataPoint<{ host: string }> {}

    const now = new FakeClock().now();
    const value = { host: '1.1.1.1' };
    const point = new Blob({ value, retrievedBy: SEED, firstRetrieved: now, lastRetrieved: now });
    const taken = point.identity;

    // Moving the value afterwards cannot move an identity that was taken when the DataPoint was built.
    value.host = 'moved';
    expect(point.identity).toBe(taken);
    expect(point.reobserved(shifted(now, SECOND)).identity).toBe(taken);
    expect(dp(Blob, { host: '1.1.1.1' }, now).identity).toBe(taken);
  });

  it('gives heterogeneous sets and structured values a stable identity', () => {
    class Tags extends DataPoint<Set<unknown>> {}

    class Shape {
      public constructor(
        public readonly name: string,
        public readonly sides: readonly number[],
      ) {}
    }

    class Shaped extends DataPoint<Shape> {}

    const now = new FakeClock().now();

    expect(dp(Tags, new Set([1, 'a', [2, 3]]), now).equals(dp(Tags, new Set([[2, 3], 'a', 1]), now))).toBe(true);
    expect(dp(Shaped, new Shape('tri', [1, 2, 3]), now).equals(dp(Shaped, new Shape('tri', [1, 2, 3]), now))).toBe(
      true,
    );
    expect(dp(Shaped, new Shape('tri', [1, 2, 3]), now).equals(dp(Shaped, new Shape('sq', [1, 2, 3]), now))).toBe(
      false,
    );
  });

  it('rejects a value with no stable form where the DataPoint is built', () => {
    class Raw extends DataPoint<unknown> {}

    const now = new FakeClock().now();

    expect(() => dp(Raw, () => 'x', now)).toThrow(UnhashableValueError);
    expect(() => dp(Raw, () => 'x', now)).toThrow(/function/);
    expect(() => Raw.emit({ nested: [() => 'x'] }).finalize({ retrievedBy: OperatorId('op'), at: now })).toThrow(
      UnhashableValueError,
    );
  });

  it('only advances lastRetrieved when re-observed, and never backwards', () => {
    const now = new FakeClock().now();
    const point = dp(Ip, '1.1.1.1', now);

    const later = point.reobserved(shifted(now, 30 * SECOND));

    expect(later.firstRetrieved).toEqual(now);
    expect(later.lastRetrieved).toEqual(shifted(now, 30 * SECOND));
    expect(point.reobserved(shifted(now, -30 * SECOND)).lastRetrieved).toEqual(now);
  });
});

describe('DataPointView', () => {
  it('answers queries subtype-aware, and latest picks the newest', () => {
    const now = new FakeClock().now();
    const work = dp(WorkEmail, 'w', now);
    const personal = dp(PersonalEmail, 'p', shifted(now, 5 * SECOND));
    const risk = dp(Risk, 0.5, now);
    const view = new DataPointView([work, personal, risk]);

    expect(new Set(view.ofType(Email))).toEqual(new Set([work, personal]));
    expect(view.ofType(WorkEmail)).toEqual([work]);
    expect(view.latest(Email)).toBe(personal);
    expect(view.latest(Ip)).toBeNull();
    expect(view.presentTypes()).toEqual(new Set([WorkEmail, PersonalEmail, Risk]));
    expect(view.size).toBe(3);
    expect([...view]).toEqual([work, personal, risk]);
  });
});

describe('DataPointEmission', () => {
  it('carries only a value, and finalize stamps provenance and the observation time', () => {
    const now = new FakeClock().now();

    const emission = Ip.emit('9.9.9.9');

    expect(emission.leafType).toBe(Ip);
    expect(emission.value).toBe('9.9.9.9');

    const finalized = emission.finalize({ retrievedBy: OperatorId('scanner'), at: now });

    expect(finalized).toBeInstanceOf(Ip);
    expect(finalized.retrievedBy).toBe('scanner');
    expect(finalized.firstRetrieved).toEqual(now);
    expect(finalized.lastRetrieved).toEqual(now);
  });
});
