import { describe, expect, it } from 'vitest';
import { UnstableValueError } from '../../src/orcastork/exceptions.js';
import { canonicalValue, stableStringify } from '../../src/orcastork/internal/stable_json.js';

describe('stableStringify', () => {
  it('sorts object keys so declaration order cannot change an identity', () => {
    expect(stableStringify({ b: 1, a: 2 })).toBe('{"a":2,"b":1}');
    expect(stableStringify({ a: 2, b: 1 })).toBe(stableStringify({ b: 1, a: 2 }));
  });

  it('sorts nested objects too', () => {
    expect(stableStringify({ a: [1, { c: 3, b: 2 }] })).toBe('{"a":[1,{"b":2,"c":3}]}');
  });

  it('keeps array order, because a list is ordered', () => {
    expect(stableStringify([2, 1])).toBe('[2,1]');
    expect(stableStringify([1, 2])).not.toBe(stableStringify([2, 1]));
  });

  it('ignores set order, because membership is the identity', () => {
    expect(stableStringify(new Set([2, 1]))).toBe(stableStringify(new Set([1, 2])));
  });

  it('encodes a set like the array of its members in canonical order, as the Python tuple does', () => {
    expect(stableStringify(new Set(['b', 'a']))).toBe(stableStringify(['a', 'b']));
  });

  it('encodes a map like the object with the same entries', () => {
    expect(
      stableStringify(
        new Map([
          ['b', 1],
          ['a', 2],
        ]),
      ),
    ).toBe(stableStringify({ a: 2, b: 1 }));
  });

  it('keeps undefined distinguishable from null and from an absent key', () => {
    expect(stableStringify(undefined)).toBe('undefined');
    expect(stableStringify({ a: undefined })).toBe('{"a":undefined}');
    expect(stableStringify({ a: null })).toBe('{"a":null}');
    expect(stableStringify({})).toBe('{}');
  });

  it('keeps NaN and the infinities distinguishable, which JSON.stringify does not', () => {
    expect(stableStringify(Number.NaN)).toBe('NaN');
    expect(stableStringify(Number.POSITIVE_INFINITY)).toBe('Infinity');
    expect(stableStringify(Number.NEGATIVE_INFINITY)).toBe('-Infinity');
    expect(new Set([stableStringify(Number.NaN), stableStringify(null), stableStringify(0)]).size).toBe(3);
  });

  it('encodes a Date as its ISO string', () => {
    expect(stableStringify(new Date('2026-01-01T00:00:00.000Z'))).toBe('"2026-01-01T00:00:00.000Z"');
    expect(stableStringify({ at: new Date(0) })).toBe('{"at":"1970-01-01T00:00:00.000Z"}');
  });

  it('honours toJSON, so a model type has one serialization and not two', () => {
    const value = { toJSON: (): unknown => ({ b: 1, a: 2 }) };
    expect(stableStringify(value)).toBe('{"a":2,"b":1}');
  });

  it('encodes the same value identically on repeated calls', () => {
    const value = { id: 'x', tags: new Set(['b', 'a']), seen: [1, 2, 3], at: new Date('2026-06-01T12:00:00Z') };
    expect(stableStringify(value)).toBe(stableStringify(structuredClone(value)));
  });

  it('rejects a bigint, which has no JSON form', () => {
    expect(() => stableStringify(1n)).toThrow(UnstableValueError);
    expect(() => stableStringify({ big: 1n })).toThrow(/bigint/);
  });

  it('rejects a function and a symbol', () => {
    expect(() => stableStringify(() => undefined)).toThrow(UnstableValueError);
    expect(() => stableStringify(Symbol('s'))).toThrow(UnstableValueError);
  });

  it('rejects a circular reference instead of recursing forever', () => {
    const cyclic: Record<string, unknown> = { name: 'loop' };
    cyclic.self = cyclic;
    expect(() => stableStringify(cyclic)).toThrow(UnstableValueError);
  });

  it('accepts the same object referenced twice side by side, which is not a cycle', () => {
    const shared = { a: 1 };
    expect(stableStringify({ left: shared, right: shared })).toBe('{"left":{"a":1},"right":{"a":1}}');
  });

  it('rejects an invalid Date rather than encoding it as something else', () => {
    expect(() => stableStringify(new Date('not a date'))).toThrow(UnstableValueError);
  });
});

describe('canonicalValue', () => {
  it('is the same canonical string, under the name the archive key derivation uses', () => {
    expect(canonicalValue({ b: 1, a: 2 })).toBe(stableStringify({ b: 1, a: 2 }));
  });
});
