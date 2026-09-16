/**
 * Deterministic canonical encoding of a value — the port's `_make_hashable` / `canonical_value`.
 *
 * Python keys a DataPoint's identity on a hashable normalization of its value: lists and sets
 * become tuples, dicts become sorted key/value tuples. TypeScript has no structural hashing and
 * no hashable protocol, so the same job is done by a **canonical string**: two values that Python
 * would consider the same identity encode to the same string here, and a `Map` or `Set` keyed on
 * that string dedups exactly as Python's dict does.
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
 * Values that have no stable encoding at all (`bigint` — whose textual form is fine but whose
 * JSON round-trip is not, functions, symbols, circular references, an invalid `Date`) are
 * rejected with {@link UnstableValueError} rather than silently canonicalized to something else.
 *
 * @module
 */

import { UnstableValueError } from '../exceptions.js';

/**
 * A process-stable canonical string for a value.
 *
 * Deterministic across processes and runs — unlike a hash of an object identity — which is what
 * makes a durable archive key built from it resolvable by a later session, and by the Python
 * worker running the same flow.
 */
export const stableStringify = (value: unknown): string => encode(value, new Set<object>());

/**
 * A process-stable canonical string for a JSON-native DataPoint value.
 *
 * Mirrors the keyed-merge identity normalization (dict/set order insensitive), so a durable
 * archive key built from it is stable across processes. The archive adapter feeds this to
 * `ValueCipher.mac` (PII — a keyed digest) or a plain SHA-256 (non-PII) to derive the key; the
 * canonical form itself is never persisted for PII.
 */
export const canonicalValue = (value: unknown): string => stableStringify(value);

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
      throw new UnstableValueError(
        'bigint has no JSON form and cannot be part of a canonical value; pass it as a string',
      );
    case 'function':
      throw new UnstableValueError('a function cannot be part of a canonical value');
    case 'symbol':
      throw new UnstableValueError('a symbol cannot be part of a canonical value');
    default:
      throw new UnstableValueError(`unsupported value of type ${typeof value} in a canonical value`);
  }
};

const encodeObject = (value: object, seen: Set<object>): string => {
  if (seen.has(value)) {
    throw new UnstableValueError('a circular reference cannot be part of a canonical value');
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
      // honours it, so a model type has one serialization and not two.
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
    throw new UnstableValueError('an invalid Date cannot be part of a canonical value');
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
