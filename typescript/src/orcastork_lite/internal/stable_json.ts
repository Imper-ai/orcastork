/**
 * Deterministic canonical encoding of a value — the port of `datapoints._make_hashable`.
 *
 * Python keys a DataPoint's identity on a hashable normalization of its value: lists and sets
 * become tuples, dicts become sorted key/value tuples, a pydantic model is reduced through its
 * dump. TypeScript has no structural hashing and no hashable protocol, so the same job is done by
 * a **canonical string**: two values Python would give one identity encode to the same string
 * here, and a `Map` or `Set` keyed on that string dedups exactly as Python's dict does.
 *
 * The encoding is JSON for JSON-native values, with three deliberate departures, each of which
 * exists to keep an identity from silently colliding:
 *
 * - Object keys are sorted, so `{a, b}` and `{b, a}` are one identity (Python's sorted tuples).
 * - A `Set` encodes as an array of its members in canonical order, so member order cannot change
 *   the identity. A set and an array holding the same members in that order therefore encode
 *   alike — which is exactly what Python does, where both reduce to the same tuple.
 * - `undefined`, `NaN` and the infinities have no JSON form and `JSON.stringify` flattens them
 *   all to `null` or drops them. They are written as their bare tokens instead, so they stay
 *   distinguishable from `null` and from each other.
 *
 * Values with no stable encoding at all (`bigint` — whose textual form is fine but whose JSON
 * round-trip is not, functions, symbols, circular references, an invalid `Date`) are rejected
 * with {@link UnhashableValueError}, the counterpart of Python's `hash(obj)` raising `TypeError`.
 * Rejecting is the point: the alternative is an identity that quietly collides with another.
 *
 * Its own copy, not a shared one: `orcastork_lite` never imports `orcastork`.
 *
 * @module
 */

import { UnhashableValueError } from '../exceptions.js';

/**
 * A canonical string for a value — the normalized half of a DataPoint's identity.
 *
 * @throws {UnhashableValueError} when the value (or anything inside it) has no stable encoding.
 */
export const stableStringify = (value: unknown): string => encode(value, new Set<object>());

/** Phrased like Python's message, so the offending type is the first thing a reader sees. */
const reject = (what: string): never => {
  throw new UnhashableValueError(`${what} cannot be part of a DataPoint identity`);
};

const encode = (value: unknown, seen: Set<object>): string => {
  switch (typeof value) {
    case 'string':
      return JSON.stringify(value);
    case 'number':
      // `NaN`/`Infinity` would both become `null` under JSON.stringify, collapsing three distinct
      // values (and `null` itself) into one identity.
      return Number.isFinite(value) ? JSON.stringify(value) : String(value);
    case 'boolean':
      return value ? 'true' : 'false';
    case 'undefined':
      return 'undefined';
    case 'object':
      return value === null ? 'null' : encodeObject(value, seen);
    case 'bigint':
      return reject('a bigint value (it has no JSON form; pass it as a string)');
    case 'function':
      return reject('a function value');
    case 'symbol':
      return reject('a symbol value');
    default:
      return reject(`a ${typeof value} value`);
  }
};

const encodeObject = (value: object, seen: Set<object>): string => {
  if (seen.has(value)) {
    return reject('a circular reference');
  }
  seen.add(value);
  try {
    if (value instanceof Date) {
      return encodeDate(value);
    }
    if (Array.isArray(value)) {
      return `[${value.map((item: unknown) => encode(item, seen)).join(',')}]`;
    }
    if (value instanceof Set) {
      // Sorted, then written as an array: membership is the identity, order is not — and the
      // sorted-array form is what the equivalent Python tuple reduces to.
      const members = [...value].map((item: unknown) => encode(item, seen)).sort();
      return `[${members.join(',')}]`;
    }
    if (value instanceof Map) {
      return encodeEntries([...value].map(([key, item]) => ({ key: encode(key, seen), value: encode(item, seen) })));
    }
    const custom = (value as { toJSON?: unknown }).toJSON;
    if (typeof custom === 'function') {
      // The escape hatch a class uses to say what it *is*, honoured exactly as JSON.stringify
      // honours it, so a model type has one serialization and not two — the counterpart of
      // Python reducing a pydantic model through `model_dump()`.
      return encode((custom as (this: object) => unknown).call(value), seen);
    }
    return encodeEntries(
      Object.keys(value).map((key) => ({
        key: JSON.stringify(key),
        value: encode((value as Record<string, unknown>)[key], seen),
      })),
    );
  } finally {
    // Removed on the way out so a value referenced twice side by side — a shared constant, not a
    // cycle — is encoded twice rather than rejected.
    seen.delete(value);
  }
};

const encodeDate = (value: Date): string => {
  if (Number.isNaN(value.getTime())) {
    return reject('an invalid Date');
  }
  return JSON.stringify(value.toISOString());
};

/** One encoded key/value pair, before the sort that makes the object form canonical. */
interface EncodedEntry {
  readonly key: string;
  readonly value: string;
}

/** Code-unit ordering, not locale ordering: the canonical form must not depend on the host. */
const compareEntries = (left: EncodedEntry, right: EncodedEntry): number => {
  if (left.key === right.key) {
    return 0;
  }
  return left.key < right.key ? -1 : 1;
};

const encodeEntries = (entries: readonly EncodedEntry[]): string => {
  const sorted = [...entries].sort(compareEntries);
  return `{${sorted.map((entry) => `${entry.key}:${entry.value}`).join(',')}}`;
};
