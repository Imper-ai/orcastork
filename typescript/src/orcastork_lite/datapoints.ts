/**
 * The `DataPoint` model — the unit of data — and its read-only query view.
 *
 * A `DataPoint` is a frozen value object whose **identity is `(class, value)`**, timestamps
 * excluded, so re-observing a value merges (bumping `lastRetrieved`) instead of duplicating.
 * There is no registry and no discriminator: nothing is ever serialized, so the class itself is
 * the type. The identity is computed once, when the DataPoint is built, so a value that cannot be
 * reduced to a stable form is rejected there with an `UnhashableValueError`. Any subclass —
 * however abstract — may appear in an operator's `dependsOn`; a present leaf satisfies a
 * base-type dependency through plain `instanceof`.
 *
 * @module
 */

import type { OperatorId } from './ids.js';
import { stableStringify } from './internal/stable_json.js';

/** Any DataPoint, whatever value it carries — the TypeScript spelling of `DataPoint[Any]`. */
export type AnyDataPoint = DataPoint<unknown>;

/**
 * A DataPoint class as the framework handles it — the spelling of Python's `type[DataPoint[Any]]`.
 *
 * The constructor parameter is `never` on purpose: a class type is used here only to *match*
 * (`instanceof`, `isSubclass`, a `dependsOn` entry), never to build, and a `never` parameter is
 * what makes `typeof WorkEmail` assignable to `DataPointClass<Email>` the way Python's
 * `type[WorkEmail]` is usable as a `type[Email]`. {@link ConcreteDataPointClass} is the
 * constructible counterpart.
 */
export type DataPointClass<T extends AnyDataPoint = AnyDataPoint> = abstract new (init: never) => T;

/** A constructible DataPoint class: what an emission carries and the framework builds from. */
export type ConcreteDataPointClass<T extends AnyDataPoint = AnyDataPoint> = new (init: DataPointInit<T['value']>) => T;

/** Any class at all — what {@link isSubclass} compares, DataPoint or Capability alike. */
export type AnyClass = abstract new (...args: never[]) => unknown;

/**
 * Python's `issubclass`: is `candidate` `base`, or does it descend from it?
 *
 * Walks the prototype chain, which is where JavaScript keeps exactly the relation `issubclass`
 * reads — and, like Python, a class is a subclass of itself.
 */
export const isSubclass = (candidate: AnyClass, base: AnyClass): boolean => {
  let current: unknown = candidate;
  while (typeof current === 'function') {
    if (current === base) {
      return true;
    }
    current = Object.getPrototypeOf(current);
  }
  return false;
};

/**
 * A process-unique tag per DataPoint class.
 *
 * Python keys an identity on the class *object*, so two classes that happen to share a name are
 * two identities. A name alone would merge them here, so each class gets a tag the first time it
 * is used — the name for legibility, a counter for uniqueness. Nothing in the package is ever
 * serialized, so the tag never has to mean anything in another process.
 */
const classTags = new WeakMap<object, string>();
let classTagCount = 0;

const classTag = (leaf: object): string => {
  const known = classTags.get(leaf);
  if (known !== undefined) {
    return known;
  }
  classTagCount += 1;
  const name = (leaf as { readonly name?: unknown }).name;
  const tag = `${typeof name === 'string' && name.length > 0 ? name : 'DataPoint'}#${classTagCount}`;
  classTags.set(leaf, tag);
  return tag;
};

/** The fields a DataPoint is built from — value plus the provenance the orchestrator stamps. */
export interface DataPointInit<V = unknown> {
  /** What was observed. Anything with a stable canonical form; the class says what it means. */
  readonly value: V;

  /** The operator that first observed this identity. */
  readonly retrievedBy: OperatorId;

  /** When this identity was first observed. */
  readonly firstRetrieved: Date;

  /** When this identity was last observed; advanced by {@link DataPoint.reobserved}. */
  readonly lastRetrieved: Date;
}

/**
 * The unit of data: a value, its provenance, and when it was seen.
 *
 * Subclass it with an empty body — the class *is* the type:
 *
 * ```ts
 * class Url extends DataPoint<string> {}
 * ```
 *
 * An instance is frozen when it is built, exactly as the Python model is `frozen`, so a subclass
 * adds no instance fields of its own: what varies between two DataPoints is the value, not the
 * shape.
 */
export class DataPoint<V = unknown> {
  /** What was observed. */
  public readonly value: V;

  /** The operator that first observed this identity (an `updated` merge does not change it). */
  public readonly retrievedBy: OperatorId;

  /** When this identity was first observed. */
  public readonly firstRetrieved: Date;

  /** When this identity was last observed. */
  public readonly lastRetrieved: Date;

  /**
   * The keyed-merge identity — class and normalized value, timestamps excluded.
   *
   * Computed once at construction: the instance is frozen, so the identity can never change, and
   * every keyed merge and equality check reads it back instead of re-normalizing the value.
   * Computing it eagerly also means an unhashable value fails where the DataPoint is built —
   * inside the emitting operator's fault boundary — rather than deep in the session state.
   *
   * A canonical **string**, because TypeScript has no structural hashing: it is what a `Map` or
   * `Set` must key on for two sightings of one value to be one entry.
   */
  public readonly identity: string;

  public constructor(init: DataPointInit<V>) {
    this.value = init.value;
    this.retrievedBy = init.retrievedBy;
    // Copies: `Date` is mutable, and a frozen value object that hands out a live one is not frozen.
    this.firstRetrieved = new Date(init.firstRetrieved.getTime());
    this.lastRetrieved = new Date(init.lastRetrieved.getTime());
    this.identity = `${classTag(this.constructor)}:${stableStringify(init.value)}`;
    Object.freeze(this);
  }

  /** Two DataPoints are the same when their identities are — the same class, the same value. */
  public equals(other: unknown): boolean {
    return other instanceof DataPoint && other.identity === this.identity;
  }

  /** A copy with `lastRetrieved` advanced to `at` (`firstRetrieved` kept, never moved backwards). */
  public reobserved(at: Date): this {
    const lastRetrieved = at.getTime() > this.lastRetrieved.getTime() ? new Date(at.getTime()) : this.lastRetrieved;
    // A field copy rather than a rebuild, the counterpart of pydantic's `model_copy`: re-observation
    // is the merge hot path, and the identity of an unchanged value is already known.
    const copy = Object.assign(Object.create(Object.getPrototypeOf(this) as object) as this, this, { lastRetrieved });
    Object.freeze(copy);
    return copy;
  }

  /**
   * Emit this DataPoint type carrying `value` — an operator's sole responsibility.
   *
   * The orchestrator stamps provenance (`retrievedBy`) and the observation time when it writes the
   * result to the session, so operators never fabricate bookkeeping fields.
   */
  public static emit<T extends AnyDataPoint>(
    this: ConcreteDataPointClass<T>,
    value: T['value'],
  ): DataPointEmission<T> {
    // biome-ignore lint/complexity/noThisInStatic: `this` is the leaf the call was made on — Python's `cls`.
    return new DataPointEmission<T>(this, value);
  }
}

/** The keyed-merge identity of a DataPoint — the key a `Map` of session state is kept under. */
export const identityKey = (dataPoint: AnyDataPoint): string => dataPoint.identity;

/** A value-only DataPoint emitted by an operator (see {@link DataPoint.emit}). */
export class DataPointEmission<T extends AnyDataPoint = AnyDataPoint> {
  /** The leaf class to build — the emission's "type". */
  public readonly leafType: DataPointClass<T>;

  /** The observed value, unstamped. */
  public readonly value: T['value'];

  public constructor(leafType: DataPointClass<T>, value: T['value']) {
    this.leafType = leafType;
    this.value = value;
    Object.freeze(this);
  }

  /** Build the full DataPoint, stamping provenance and observation time. */
  public finalize(options: { readonly retrievedBy: OperatorId; readonly at: Date }): T {
    // The matching form of a class type cannot be called; the emission only ever holds the
    // concrete leaf `emit` was invoked on, which is exactly what this asserts.
    const leaf = this.leafType as unknown as ConcreteDataPointClass<T>;
    return new leaf({
      value: this.value,
      retrievedBy: options.retrievedBy,
      firstRetrieved: options.at,
      lastRetrieved: options.at,
    });
  }
}

/** The read-only, subtype-aware query facade handed to operators and capabilities. */
export class DataPointView {
  private readonly items: readonly AnyDataPoint[];

  public constructor(items: Iterable<AnyDataPoint> = []) {
    this.items = Object.freeze([...items]);
  }

  /** Every DataPoint in the session, in arrival order. */
  public all(): readonly AnyDataPoint[] {
    return this.items;
  }

  /** All DataPoints that are instances of `dataPointType` (subtype-aware). */
  public ofType<T extends AnyDataPoint>(dataPointType: DataPointClass<T>): readonly T[] {
    return this.items.filter((item): item is T => item instanceof dataPointType);
  }

  /** The newest DataPoint of `dataPointType` by `lastRetrieved`, or `null`. */
  public latest<T extends AnyDataPoint>(dataPointType: DataPointClass<T>): T | null {
    let newest: T | null = null;
    for (const item of this.ofType(dataPointType)) {
      // Strictly newer, so a tie keeps the earlier arrival — Python's `max` picks the first maximum.
      if (newest === null || item.lastRetrieved.getTime() > newest.lastRetrieved.getTime()) {
        newest = item;
      }
    }
    return newest;
  }

  /** The concrete classes present — what readiness and capability availability are computed from. */
  public presentTypes(): ReadonlySet<DataPointClass> {
    return new Set(this.items.map((item) => item.constructor as DataPointClass));
  }

  /** How many DataPoints the view holds. */
  public get size(): number {
    return this.items.length;
  }

  public [Symbol.iterator](): Iterator<AnyDataPoint> {
    return this.items[Symbol.iterator]();
  }
}
