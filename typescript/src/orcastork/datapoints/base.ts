/**
 * The `DataPoint` model — the framework's unit of data.
 *
 * A `DataPoint` is a frozen value object whose **identity is `(type, value)`**, excluding
 * timestamps, so re-observing a value dedups (keyed-merge). Concrete leaves pin a `type`
 * discriminator and carry a class-level {@link DataPointTypeConfig} (`pii` / `ephemeral`);
 * abstract intermediates group leaves for subtype substitution but are not union members.
 *
 * Registration happens in the {@link dataPointType} class decorator — the port's stand-in for
 * Python's `__pydantic_init_subclass__`: concrete leaves self-register by their discriminator
 * (raising on a duplicate), abstract intermediates are tracked for substitution through
 * {@link abstractDataPoint}. A monotonically increasing version counter lets `./registry.ts`
 * rebuild the discriminated-union parser lazily.
 *
 * **Where the port differs from Python, and why.** Python raises `InvalidDataPointError` at class
 * *definition* time for a concrete leaf with no discriminator, because defining the class is what
 * runs the hook. In TypeScript a class that is simply never decorated runs no code at all, so the
 * same mistake is caught at *first use* instead: constructing or emitting an undecorated concrete
 * class throws `InvalidDataPointError`. A decorated declaration that is malformed (no `config`
 * anywhere on its chain) still fails at definition time, exactly as Python does.
 *
 * @module
 */

import { z } from 'zod';
import { DuplicateRegistrationError, InvalidDataPointError } from '../exceptions.js';
import type { DataPointType, OperatorRef } from '../ids.js';
import { pythonRepr } from '../internal/python_repr.js';
import { Registry } from '../internal/registry.js';

/**
 * A process-stable canonical string for a JSON-native DataPoint value.
 *
 * Mirrors the keyed-merge identity normalization (`_make_hashable` — dict/set order insensitive),
 * so a durable archive key built from it is stable across processes (Python's builtin `hash` is
 * per-process salted). The archive adapter feeds this to `ValueCipher.mac` (PII — a keyed digest)
 * or a plain SHA-256 (non-PII) to derive the key; the canonical form itself is never persisted for
 * PII.
 *
 * It is Python's `repr(_make_hashable(value))`, byte for byte — see `internal/python_repr.ts` for
 * why (the string is a Redis hash field and a digest input, so both runtimes must produce it
 * identically) and for the two things a JavaScript number cannot tell us.
 */
export const canonicalValue = (value: unknown): string => pythonRepr(value);

/**
 * Registry of concrete leaves keyed by discriminator value.
 *
 * Module-level, as Python's `_REGISTRY` is: registering is a side effect of declaring the class.
 * It is a {@link Registry}, so the test setup snapshots and restores it around every test without
 * knowing it exists.
 */
export const dataPointRegistry = new Registry<DataPointType, DataPointLeafClass>('orcastork.datapoints');

/**
 * The abstract intermediates, tracked for substitution.
 *
 * Nothing in the engine reads this — substitution works off the prototype chain — but the set is
 * what makes "is this class an intermediate or an unfinished leaf?" answerable, and it keeps the
 * Python module's shape.
 */
export const abstractDataPointRegistry = new Registry<AnyDataPointClass, true>('orcastork.datapoints.abstract');

/** Any DataPoint, whatever value it carries — the TypeScript spelling of `BaseDataPoint[Any]`. */
export type AnyDataPoint = BaseDataPoint<unknown>;

/**
 * A DataPoint class as the framework handles it — the spelling of Python's `type[BaseDataPoint[Any]]`.
 *
 * The constructor parameter is `never` on purpose: a class type is used here only to *match*
 * (`instanceof`, {@link isSubclass}, a `dependsOn` entry), never to build, and a `never` parameter
 * is what makes `typeof WorkEmailDataPoint` assignable to `DataPointClass<EmailDataPoint>` the way
 * Python's `type[WorkEmailDataPoint]` is usable as a `type[EmailDataPoint]`.
 * {@link ConcreteDataPointClass} is the constructible counterpart.
 */
export type DataPointClass<T extends AnyDataPoint = AnyDataPoint> = abstract new (init: never) => T;

/** A constructible DataPoint class: what an emission carries and the registry parser builds from. */
export type ConcreteDataPointClass<T extends AnyDataPoint = AnyDataPoint> = new (init: DataPointInit<T['value']>) => T;

/** The class-level declarations the decorators write, readable off any DataPoint class. */
export interface DataPointStatics {
  /** The discriminator, or `undefined` on an abstract intermediate / an undecorated class. */
  readonly type: DataPointType | undefined;

  /** The classification, inherited from an abstract intermediate when a leaf declares none. */
  readonly config: DataPointTypeConfig | undefined;

  /** The value contract, used by the registry parser and checked on construction. */
  readonly valueSchema: z.ZodType;
}

/** Any DataPoint class at all, with the statics the decorators set. */
export type AnyDataPointClass = DataPointClass & DataPointStatics;

/** A registered concrete leaf: a DataPoint class whose statics the decorator has filled in. */
export type DataPointLeafClass = AnyDataPointClass;

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
 * Class-level classification of a DataPoint type.
 *
 * `pii` drives encryption/redaction/retention; `ephemeral` means "never written to the durable
 * store" (a transient DataPoint emitted only to trigger another operator).
 *
 * `auditEveryEmission` is the escape hatch for high-volume types. A per-frame debugger probe or a
 * page-view stream emits hundreds of DataPoints in one session, and a row each dominates the trail —
 * two such types were ~78% of it — while every row is also a durable buffer write, so the cost lands
 * on the database twice and the completion flush inherits all of it. Opting a type out replaces its
 * per-emission rows with one counted summary, so the trail still says how many were merged.
 */
export interface DataPointTypeConfig {
  readonly pii: boolean;

  readonly ephemeral: boolean;

  readonly auditEveryEmission: boolean;
}

/** The fields a {@link DataPointTypeConfig} is built from; `auditEveryEmission` defaults to `true`. */
export interface DataPointTypeConfigInit {
  readonly pii: boolean;

  readonly ephemeral: boolean;

  readonly auditEveryEmission?: boolean;
}

const configSchema = z.object({
  pii: z.boolean(),
  ephemeral: z.boolean(),
  auditEveryEmission: z.boolean().optional(),
});

/**
 * Build a {@link DataPointTypeConfig}.
 *
 * The opt-out must be explicit: a new type silently losing its audit trail would be a bad default,
 * so `auditEveryEmission` is `true` unless a type says otherwise.
 */
export const DataPointTypeConfig = (init: DataPointTypeConfigInit): DataPointTypeConfig => {
  configSchema.parse(init);
  return Object.freeze({
    pii: init.pii,
    ephemeral: init.ephemeral,
    auditEveryEmission: init.auditEveryEmission ?? true,
  });
};

/** The fields a DataPoint is built from — value plus the provenance the orchestrator stamps. */
export interface DataPointInit<V = unknown> {
  /** What was observed; the leaf's `valueSchema` says what shape it must have. */
  readonly value: V;

  /** The operator that first observed this identity. */
  readonly retrievedBy: OperatorRef;

  /** When this identity was first observed. */
  readonly firstRetrieved: Date;

  /** When this identity was last observed; advanced by {@link BaseDataPoint.reobserved}. */
  readonly lastRetrieved: Date;
}

/**
 * The serialized form of a DataPoint — Python's `model_dump(mode='json')`.
 *
 * Keys stay snake_case and instants ISO-8601, so a session written by a TypeScript worker is
 * readable by a Python one and the other way round.
 */
export interface DataPointWire {
  readonly type: DataPointType;
  readonly value: unknown;
  readonly retrieved_by: string;
  readonly first_retrieved: string;
  readonly last_retrieved: string;
}

/** The provenance a {@link DataPointEmission} is stamped with when the orchestrator writes it. */
export interface EmissionProvenance {
  readonly retrievedBy: OperatorRef;

  readonly at: Date;
}

/** The statics of the class an instance was built from, plus its name for diagnostics. */
type LeafStatics = DataPointStatics & { readonly name: string };

const staticsOf = (instance: object): LeafStatics => instance.constructor as unknown as LeafStatics;

/**
 * The unit of data: a value, its provenance, and when it was seen.
 *
 * A concrete leaf is a decorated subclass with an empty body:
 *
 * ```ts
 * @dataPointType('work_email', { pii: true, ephemeral: false }, { value: z.string() })
 * class WorkEmailDataPoint extends BaseDataPoint<string> {}
 * ```
 *
 * An instance is frozen when it is built, exactly as the Python model is `frozen`, so a subclass
 * adds no instance fields of its own: what varies between two DataPoints is the value, not the
 * shape.
 */
export abstract class BaseDataPoint<ValueT = unknown> {
  /**
   * The discriminator — every concrete leaf pins one through {@link dataPointType}.
   *
   * Written once, by the decorator, at class-definition time; `undefined` on the base and on an
   * abstract intermediate.
   */
  public static type: DataPointType | undefined = undefined;

  /**
   * The classification — set on an intermediate (and inherited) or per leaf.
   *
   * Static inheritance is what makes a leaf under `@abstractDataPoint` pick up its parent's
   * config without redeclaring it, the way Python's class attribute does.
   */
  public static config: DataPointTypeConfig | undefined = undefined;

  /**
   * The value contract.
   *
   * `z.unknown()` unless a leaf (or its intermediate) declares one — an undeclared value shape is
   * a leaf that accepts anything, not a leaf that rejects everything. Checked on construction and
   * by the registry parser, which is where a malformed inbox payload is caught.
   */
  public static valueSchema: z.ZodType = z.unknown();

  /** What was observed. */
  public readonly value: ValueT;

  /** The operator that first observed this identity (an `updated` merge does not change it). */
  public readonly retrievedBy: OperatorRef;

  /** When this identity was first observed. */
  public readonly firstRetrieved: Date;

  /** When this identity was last observed. */
  public readonly lastRetrieved: Date;

  /**
   * The keyed-merge identity — discriminator and normalized value, timestamps excluded.
   *
   * A canonical **string**, because TypeScript has no structural hashing: it is what a `Map` or
   * `Set` must key on for two sightings of one value to be one entry, and it is the same
   * normalization {@link canonicalValue} gives the archive key, so dedup decisions never diverge
   * across adapters.
   *
   * Its shape — the discriminator, a NUL, and {@link canonicalValue} of the value — is exactly the
   * Redis store's hash field (`f'{type}\x00{canonical_value(value)}'`), so a session written by a
   * Python worker and one written here land on the same field.
   *
   * Computed once, at construction: the instance is frozen, so the identity can never change, and
   * a value with no stable canonical form fails inside the emitting operator's fault boundary
   * rather than deep in the session state.
   */
  public readonly identity: string;

  public constructor(init: DataPointInit<ValueT>) {
    const leaf = staticsOf(this);
    if (leaf.type === undefined || leaf.config === undefined) {
      // Python catches this when the class is defined; an undecorated TypeScript class runs no
      // code at definition time, so first use is the earliest honest place to fail.
      throw new InvalidDataPointError(
        `${leaf.name} is not a registered concrete DataPoint leaf (declare it with @dataPointType)`,
      );
    }
    const checked = leaf.valueSchema.safeParse(init.value);
    if (!checked.success) {
      // The raw validation error, as pydantic raises it: a wrong value shape is a caller error,
      // not a framework condition, and the real issue list is what says which field is wrong.
      throw checked.error;
    }
    this.value = init.value;
    this.retrievedBy = init.retrievedBy;
    // Copies: `Date` is mutable, and a frozen value object that hands out a live one is not frozen.
    this.firstRetrieved = new Date(init.firstRetrieved.getTime());
    this.lastRetrieved = new Date(init.lastRetrieved.getTime());
    this.identity = `${leaf.type}\x00${canonicalValue(init.value)}`;
    Object.freeze(this);
  }

  /** The discriminator this leaf pins — the instance-side view of the class static. */
  public get type(): DataPointType {
    return staticsOf(this).type as DataPointType;
  }

  /** The class-level classification, inherited from an abstract intermediate where one declares it. */
  public get config(): DataPointTypeConfig {
    return staticsOf(this).config as DataPointTypeConfig;
  }

  public get isPii(): boolean {
    return this.config.pii;
  }

  public get isEphemeral(): boolean {
    return this.config.ephemeral;
  }

  public get auditsEveryEmission(): boolean {
    return this.config.auditEveryEmission;
  }

  /**
   * A non-PII summary for the audit trail, used in place of the value.
   *
   * Defaults to `null` — the orchestrator then redacts a PII value and stringifies a non-PII one.
   * A PII DataPoint may override this to surface a non-sensitive identifier (e.g. a collector
   * name) so the audit can attribute the entry — and time it — without ever leaking the redacted
   * payload.
   */
  public auditSummary(): string | null {
    return null;
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

  /** Two DataPoints are the same when their identities are — same discriminator, same value. */
  public equals(other: unknown): boolean {
    return other instanceof BaseDataPoint && other.identity === this.identity;
  }

  /** The serialized form: snake_case keys, ISO instants — what the store and the inbox carry. */
  public toWire(): DataPointWire {
    return {
      type: this.type,
      value: this.value,
      retrieved_by: this.retrievedBy,
      first_retrieved: this.firstRetrieved.toISOString(),
      last_retrieved: this.lastRetrieved.toISOString(),
    };
  }

  /**
   * Emit this DataPoint type carrying `value` — an operator's sole responsibility.
   *
   * Returns a value-only {@link DataPointEmission}; the orchestrator stamps provenance
   * (`retrievedBy`) and the observation time (`firstRetrieved`/`lastRetrieved`) when it writes the
   * result to the store. Operators never fabricate provenance.
   */
  public static emit<T extends AnyDataPoint>(this: DataPointClass<T>, value: T['value']): DataPointEmission<T> {
    // biome-ignore lint/complexity/noThisInStatic: `this` is the leaf the call was made on — Python's `cls`.
    return emitFrom<T>(this, value);
  }
}

/**
 * The body of {@link BaseDataPoint.emit}, taking the leaf as an argument.
 *
 * Only a concrete leaf that is *currently registered under its own discriminator* may emit: an
 * abstract intermediate has no discriminator at all, and an undecorated subclass of a leaf
 * inherits one that resolves to its parent, so neither can be finalized into a DataPoint.
 */
const emitFrom = <T extends AnyDataPoint>(leaf: DataPointClass<T>, value: T['value']): DataPointEmission<T> => {
  const statics = leaf as unknown as LeafStatics;
  if (statics.type === undefined || dataPointRegistry.get(statics.type) !== (leaf as unknown as DataPointLeafClass)) {
    throw new InvalidDataPointError(`${statics.name} is not a concrete DataPoint leaf and cannot emit`);
  }
  return new DataPointEmission<T>(leaf, value);
};

/**
 * A value-only DataPoint emitted by an operator (see {@link BaseDataPoint.emit}).
 *
 * Operators own only *what* they observed — the concrete leaf type and its value. The orchestrator
 * owns provenance: it stamps `retrievedBy` (the emitting operator) and the observation time via
 * {@link DataPointEmission.finalize} before the DataPoint is written to the store, so an operator
 * never sees or fabricates the bookkeeping fields.
 */
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

  /** Build the full DataPoint, stamping provenance (`retrievedBy`) and observation time. */
  public finalize(provenance: EmissionProvenance): T {
    // The matching form of a class type cannot be called; the emission only ever holds the concrete
    // leaf `emit` was invoked on, which is exactly what this asserts.
    const leaf = this.leafType as unknown as ConcreteDataPointClass<T>;
    return new leaf({
      value: this.value,
      retrievedBy: provenance.retrievedBy,
      firstRetrieved: provenance.at,
      lastRetrieved: provenance.at,
    });
  }
}

/**
 * The keyed-merge identity of a DataPoint — `(type, normalized-value)`, timestamps excluded.
 *
 * The single source of truth for "the same DataPoint": the in-memory store, the keyed-merge set
 * and the string-keyed contexts (Redis hash fields, the archive key) all key on this one canonical
 * string, so dedup decisions never diverge across adapters — or across runtimes, since
 * {@link canonicalValue} is Python's `repr(_make_hashable(value))` byte for byte.
 */
export const identityKey = (dataPoint: AnyDataPoint): string => dataPoint.identity;

/** Current registry version — bumped on every (de)registration of a leaf or an intermediate. */
export const registryVersion = (): number => dataPointRegistry.version + abstractDataPointRegistry.version;

/** All registered concrete leaf classes (the discriminated-union members). */
export const registeredLeaves = (): readonly DataPointLeafClass[] => dataPointRegistry.values();

/** The abstract intermediates declared with {@link abstractDataPoint}. */
export const abstractDataPointTypes = (): ReadonlySet<AnyDataPointClass> => new Set(abstractDataPointRegistry.keys());

/**
 * Registered concrete leaves that satisfy `dataPointType` (subtype substitution).
 *
 * A dependency on an abstract intermediate (e.g. `EmailDataPoint`) is satisfied by any concrete
 * leaf below it; a dependency on a concrete leaf is satisfied by itself.
 */
export const subtypesOf = (dataPointType: DataPointClass): readonly DataPointLeafClass[] =>
  registeredLeaves().filter((leaf) => isSubclass(leaf, dataPointType));

/** What a decorator may declare about a type's value beyond its classification. */
export interface DataPointTypeOptions<V = unknown> {
  /**
   * The value contract, checked on construction and when a serialized payload is read.
   *
   * Omitted, the class inherits whatever its ancestors declared (`z.unknown()` at the base), so an
   * intermediate can bind the value shape once for a whole substitution group.
   */
  readonly value?: z.ZodType<V>;
}

/** The mutable view of the statics, used only by the two decorators that write them. */
type WritableStatics = { type?: DataPointType; config?: DataPointTypeConfig; valueSchema?: z.ZodType; name: string };

const writeStatics = (
  target: object,
  declared: { readonly type?: DataPointType; readonly config?: DataPointTypeConfigInit; readonly value?: z.ZodType },
): WritableStatics => {
  const statics = target as WritableStatics;
  if (declared.type !== undefined) {
    statics.type = declared.type;
  }
  if (declared.config !== undefined) {
    statics.config = DataPointTypeConfig(declared.config);
  }
  if (declared.value !== undefined) {
    // Only when declared: an absent schema must leave the inherited one in place, not shadow it
    // with the base's `z.unknown()`.
    statics.valueSchema = declared.value;
  }
  return statics;
};

/**
 * Declare a concrete DataPoint leaf: pin its discriminator, classify it, and register it.
 *
 * ```ts
 * @dataPointType('url', { pii: false, ephemeral: false }, { value: z.string() })
 * class UrlDataPoint extends BaseDataPoint<string> {}
 * ```
 *
 * The port's `__pydantic_init_subclass__`: it validates the declaration and registers the leaf
 * under its discriminator at class-definition time, raising `DuplicateRegistrationError` when a
 * *different* class already claims that discriminator. Re-running it on the same class (a module
 * reload) is benign, as in Python.
 *
 * `config` may be omitted when an abstract intermediate above the leaf declares one — static
 * inheritance carries it down, the way Python's class attribute does. A leaf with no `config`
 * anywhere on its chain is a definition error.
 */
export const dataPointType = <V = unknown>(
  type: DataPointType,
  config?: DataPointTypeConfigInit,
  options?: DataPointTypeOptions<V>,
) => {
  return <T extends DataPointClass<BaseDataPoint<V>>>(target: T, _context?: ClassDecoratorContext): void => {
    if (typeof type !== 'string' || type.length === 0) {
      throw new InvalidDataPointError('a concrete DataPoint leaf must pin a non-empty `type` discriminator');
    }
    const statics = writeStatics(target, { type, ...(config !== undefined ? { config } : {}), ...(options ?? {}) });
    if (statics.config === undefined) {
      throw new InvalidDataPointError(`${statics.name} must declare a class-level \`config\``);
    }
    const leaf = target as unknown as DataPointLeafClass;
    dataPointRegistry.set(type, leaf, (key, existing) => {
      if (existing !== leaf) {
        throw new DuplicateRegistrationError(
          `DataPoint type '${key}' is already registered to ${(existing as unknown as LeafStatics).name}`,
        );
      }
    });
  };
};

/**
 * Declare an abstract intermediate: a grouping type that leaves inherit from and depend on.
 *
 * ```ts
 * @abstractDataPoint({ pii: true, ephemeral: false }, { value: z.string() })
 * abstract class EmailDataPoint extends BaseDataPoint<string> {}
 * ```
 *
 * The port's `__abstract__ = True`. An intermediate is never a union member and never registers a
 * discriminator; what it contributes is the classification (and optionally the value contract) its
 * leaves inherit, plus membership in the substitution group every leaf below it satisfies.
 *
 * Decorating is optional — an undecorated base class is still an intermediate everywhere it
 * matters: `instanceof` and {@link subtypesOf} read the prototype chain, and the dependency graph
 * decides "abstract" by asking whether the class is a *registered leaf*, not whether it carries
 * this marker. Use it when the intermediate carries a shared `config` or value contract its leaves
 * should inherit, or to have it counted in {@link abstractDataPointTypes}.
 */
export const abstractDataPoint = <V = unknown>(
  config?: DataPointTypeConfigInit,
  options?: DataPointTypeOptions<V>,
) => {
  return <T extends DataPointClass<BaseDataPoint<V>>>(target: T, _context?: ClassDecoratorContext): void => {
    writeStatics(target, { ...(config !== undefined ? { config } : {}), ...(options ?? {}) });
    abstractDataPointRegistry.set(target as unknown as AnyDataPointClass, true);
  };
};
