/**
 * PRP — `pythonRepr`: `repr(_make_hashable(value))`, byte for byte, against CPython itself.
 *
 * The canonical value is on the wire — the Redis store's hash field is
 * `f'{type}\x00{canonical_value(value)}'` and the archive key is a digest over it — so "close
 * enough" means a Python worker and a TypeScript worker writing the same DataPoint to two rows.
 * The expectations here are therefore not written by hand: `python_repr_fixtures.py` prints what
 * CPython 3.13 prints, and this suite asserts the port reproduces it. Regenerate with
 * `python3.13 tests/internal/python_repr_fixtures.py`.
 *
 * Every fixture carries its value in a form both runtimes parse identically (a JSON document, or a
 * decimal literal for the float rows), so nothing is lost in transcription. The rows that carry an
 * `expected_js` are the two limits JavaScript cannot escape — an integral float is the int, and an
 * integer beyond 2^53 is the double it rounds to — and they are asserted as divergences on
 * purpose, so the port's behaviour is pinned *and* the disagreement stays visible.
 */

import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { canonicalValue, identityKey } from '../../src/orcastork/datapoints/index.js';
import { UnstableValueError } from '../../src/orcastork/exceptions.js';
import { pythonFloatRepr, pythonRepr, pythonStrRepr } from '../../src/orcastork/internal/python_repr.js';
import { workEmail } from '../doubles/datapoints.js';

/** A value given as a JSON document, with what CPython's `repr(_make_hashable(...))` prints for it. */
interface ReprFixture {
  readonly id: string;
  readonly json: string;
  readonly expected: string;
  /** Present only where JavaScript's single number type cannot reach `expected`. */
  readonly expected_js?: string;
}

/** A double given as a decimal literal — `float(literal)` and `Number(literal)` are the same value. */
interface LiteralFixture {
  readonly id: string;
  readonly literal: string;
  readonly expected: string;
}

interface Fixtures {
  readonly python_version: string;
  readonly repr: readonly ReprFixture[];
  readonly float: readonly LiteralFixture[];
  readonly str: readonly ReprFixture[];
}

const fixtures = JSON.parse(readFileSync(new URL('./python_repr_fixtures.json', import.meta.url), 'utf8')) as Fixtures;

describe('the CPython fixtures', () => {
  it('are present and large enough to be worth trusting', () => {
    // A generator that wrote nothing would make every `it.each` below pass vacuously.
    expect(fixtures.python_version).toMatch(/^3\.\d+/);
    expect(fixtures.repr.length + fixtures.float.length + fixtures.str.length).toBeGreaterThanOrEqual(60);
  });
});

describe('pythonRepr matches CPython over a JSON-native value', () => {
  it.each(fixtures.repr)('$id', (fixture) => {
    const value: unknown = JSON.parse(fixture.json);

    expect(pythonRepr(value)).toBe(fixture.expected_js ?? fixture.expected);
  });

  it.each(fixtures.repr.filter((fixture) => fixture.expected_js !== undefined))(
    'documents what JavaScript cannot see for $id',
    (fixture) => {
      // The divergence is the point of the row: pin it rather than let a future change quietly
      // "fix" one of these into agreement and break the other.
      expect(fixture.expected_js).not.toBe(fixture.expected);
      expect(pythonRepr(JSON.parse(fixture.json))).not.toBe(fixture.expected);
    },
  );
});

describe('pythonFloatRepr matches CPython', () => {
  it.each(fixtures.float)('$id', (fixture) => {
    expect(pythonFloatRepr(Number(fixture.literal))).toBe(fixture.expected);
  });
});

describe('pythonStrRepr matches CPython', () => {
  it.each(fixtures.str)('$id', (fixture) => {
    const value: unknown = JSON.parse(fixture.json);
    expect(typeof value).toBe('string');

    expect(pythonStrRepr(value as string)).toBe(fixture.expected);
  });
});

describe('the numbers JSON cannot carry', () => {
  it('reprs the non-finite doubles as Python names them', () => {
    // `nan`/`inf`/`-inf` are pinned by the float fixtures; these reach them through `pythonRepr`,
    // which routes any non-integral number — NaN and the infinities included — to the float form.
    expect(pythonRepr(Number.NaN)).toBe('nan');
    expect(pythonRepr(Number.POSITIVE_INFINITY)).toBe('inf');
    expect(pythonRepr(Number.NEGATIVE_INFINITY)).toBe('-inf');
    expect(pythonRepr([Number.NaN, Number.POSITIVE_INFINITY])).toBe('(nan, inf)');
  });

  it('renders an integral number as Python renders the int of that value', () => {
    // The documented limit: a JavaScript number has no memory of having been a float.
    expect(pythonRepr(1)).toBe('1');
    expect(pythonRepr(-0)).toBe('0');
    expect(pythonRepr(2 ** 53)).toBe('9007199254740992');
    expect(pythonRepr(1e21)).toBe('1000000000000000000000');
  });
});

describe('a Set normalizes as Python normalizes a set', () => {
  it('sorts its members, so member order cannot split an identity', () => {
    // `_make_hashable` turns a set into `tuple(sorted(...))`; a list of the same members in that
    // order reduces to the same tuple, exactly as it does in Python.
    expect(pythonRepr(new Set(['b', 'a']))).toBe("('a', 'b')");
    expect(pythonRepr(new Set(['a', 'b']))).toBe(pythonRepr(new Set(['b', 'a'])));
    expect(pythonRepr(new Set([2, 1]))).toBe(pythonRepr([1, 2]));
    expect(pythonRepr(new Set())).toBe('()');
    expect(pythonRepr(new Set(['only']))).toBe("('only',)");
  });

  it('refuses members Python could not have ordered', () => {
    // `sorted({'a', 1})` is a TypeError in Python, and a value with no canonical form there has
    // none here either.
    expect(() => pythonRepr(new Set(['a', 1]))).toThrow(UnstableValueError);
    expect(() => pythonRepr(new Set([null, 1]))).toThrow(/not supported between instances of/);
  });

  it('compares numbers across the int/float line, as Python does', () => {
    expect(pythonRepr(new Set([2, 0.5, 1]))).toBe('(0.5, 1, 2)');
    expect(pythonRepr(new Set([true, false]))).toBe('(False, True)');
  });

  it('orders tuples element by element and then by length', () => {
    expect(pythonRepr(new Set([['b'], ['a', 'z'], ['a']]))).toBe("(('a',), ('a', 'z'), ('b',))");
  });

  it('sorts a single unorderable member without comparing it, as sorted() does', () => {
    // `sorted([None])` never invokes `<`, so it succeeds; the port must not be stricter.
    expect(pythonRepr(new Set([null]))).toBe('(None,)');
  });
});

describe('a Map normalizes as Python normalizes a dict', () => {
  it('is the same sorted tuple of pairs a plain object gives', () => {
    expect(
      pythonRepr(
        new Map([
          ['b', 1],
          ['a', 2],
        ]),
      ),
    ).toBe(pythonRepr({ a: 2, b: 1 }));
    expect(pythonRepr(new Map())).toBe('()');
  });
});

describe('object keys sort by code point, as Python sorts str', () => {
  it('orders an astral key after every BMP key', () => {
    // JavaScript's `<` compares UTF-16 code units, which puts a surrogate pair (lead \uD83D)
    // *before* U+FFFD; Python compares code points and puts it after. Sorting the JavaScript way
    // would give a Python worker's Redis hash field a different name for the same value.
    expect(pythonRepr({ '\u{1F600}': 1, '�': 2, z: 3 })).toBe("(('z', 3), ('�', 2), ('\u{1F600}', 1))");
  });

  it('orders a prefix before the string that extends it', () => {
    expect(pythonRepr({ ab: 1, a: 2, b: 3 })).toBe("(('a', 2), ('ab', 1), ('b', 3))");
  });

  it('is insensitive to the order the keys were written in', () => {
    expect(pythonRepr({ lon: -0.12, lat: 51.5 })).toBe(pythonRepr({ lat: 51.5, lon: -0.12 }));
  });
});

describe('values with no Python counterpart', () => {
  it('rejects undefined rather than inventing a form for it', () => {
    expect(() => pythonRepr(undefined)).toThrow(UnstableValueError);
    expect(() => pythonRepr({ a: undefined })).toThrow(UnstableValueError);
    expect(() => pythonRepr([undefined])).toThrow(/undefined/);
  });

  it('rejects a bigint, a function and a symbol', () => {
    expect(() => pythonRepr(1n)).toThrow(UnstableValueError);
    expect(() => pythonRepr({ big: 1n })).toThrow(/bigint/);
    expect(() => pythonRepr(() => undefined)).toThrow(UnstableValueError);
    expect(() => pythonRepr(Symbol('s'))).toThrow(UnstableValueError);
  });

  it('rejects a Date, which is not a JSON-native value', () => {
    // Python would print `datetime.datetime(...)` here; a `Date` could only be mapped onto that by
    // inventing a convention, and a canonical form no Python worker produces is worse than a
    // refusal at the emitting operator's fault boundary.
    expect(() => pythonRepr(new Date('2026-01-01T00:00:00.000Z'))).toThrow(UnstableValueError);
    expect(() => pythonRepr({ at: new Date(0) })).toThrow(/Date/);
  });

  it('rejects a circular reference but not a value shared side by side', () => {
    const cyclic: Record<string, unknown> = { a: 1 };
    cyclic.self = cyclic;
    expect(() => pythonRepr(cyclic)).toThrow(UnstableValueError);

    const shared = { a: 1 };
    expect(pythonRepr({ left: shared, right: shared })).toBe("(('left', (('a', 1),)), ('right', (('a', 1),)))");
  });

  it('honours toJSON, so a model type has one canonical form and not two', () => {
    const model = {
      hidden: 'ignored',
      toJSON: () => ({ b: 1, a: 2 }),
    };

    expect(pythonRepr(model)).toBe(pythonRepr({ a: 2, b: 1 }));
  });
});

describe('canonicalValue and the identity built on it', () => {
  it('is pythonRepr, under the name the archive key derivation uses', () => {
    expect(canonicalValue({ b: 1, a: 2 })).toBe(pythonRepr({ b: 1, a: 2 }));
    expect(canonicalValue({ b: 1, a: 2 })).toBe("(('a', 2), ('b', 1))");
  });

  it('is the Redis store hash field: the discriminator, a NUL, and the canonical value', () => {
    // `f'{type}\x00{canonical_value(value)}'` in `adapters/redis/store.py`. A session written by a
    // Python worker and one written here must land on the same field.
    const point = workEmail('alice@work.example');

    expect(identityKey(point)).toBe(`work_email 'alice@work.example'`);
  });
});
