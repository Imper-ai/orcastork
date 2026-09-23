/**
 * Lazy discriminated-union assembly for DataPoints.
 *
 * The canonical `DataPoint` type is a discriminated union over the registered concrete leaves,
 * discriminated on `type`. It is assembled **lazily** from the registry (no hand-maintained list)
 * and rebuilt only when the registry changes — so leaf import order is irrelevant. Deserializing
 * an unknown discriminator raises a clear `UnknownDataPointTypeError` rather than silently
 * producing the wrong class, and a *known* type carrying a malformed payload raises the raw zod
 * validation error: the inbox quarantines only unreadable wire bytes, so a semantic failure has to
 * stay distinguishable from a poison payload.
 *
 * @module
 */

import { z } from 'zod';
import { UnknownDataPointTypeError } from '../exceptions.js';
import type { DataPointType, OperatorRef } from '../ids.js';
import type { AnyDataPoint, ConcreteDataPointClass, DataPointLeafClass } from './base.js';
import { registeredLeaves, registryVersion } from './base.js';

/**
 * How the adapter dispatches — the port of pydantic's `core_schema['type']`.
 *
 * With exactly one registered leaf there is no union to discriminate: the adapter parses straight
 * into that leaf (`model`), and a payload that omits `type` altogether still reads, because the
 * leaf pins the only discriminator there is. With more than one it is a tagged union and the
 * discriminator decides (`tagged_union`).
 */
export type DataPointSchemaKind = 'model' | 'tagged_union';

/** The assembled parser over the registered leaves — the port of pydantic's `TypeAdapter`. */
export interface DataPointAdapter {
  /** The leaves this adapter was built from; a later registration builds a new adapter. */
  readonly leaves: readonly DataPointLeafClass[];

  /** Whether the adapter dispatches on a discriminator at all. */
  readonly schemaKind: DataPointSchemaKind;

  /**
   * Deserialize a raw payload into its concrete leaf class.
   *
   * @throws UnknownDataPointTypeError when the discriminator names no registered leaf.
   * @throws ZodError when a known leaf's payload does not satisfy its value/provenance contract.
   */
  validate(raw: unknown): AnyDataPoint;
}

/**
 * Instants are read tolerantly: a `Date` (a same-process round trip through `toWire`-less code),
 * or an ISO-8601 string with either spelling of UTC — `Z` as JavaScript writes it, `+00:00` as
 * Python does.
 */
const instantSchema = z.union([z.date(), z.string()]).transform((value, ctx) => {
  const parsed = value instanceof Date ? new Date(value.getTime()) : new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    ctx.addIssue({ code: 'custom', message: 'expected an ISO-8601 instant' });
    return z.NEVER;
  }
  return parsed;
});

/**
 * The value must be *present*, whatever its declared shape.
 *
 * `z.unknown()` accepts `undefined`, which would make the key optional and let a payload with no
 * `value` at all parse into a DataPoint whose identity is the canonical form of nothing.
 */
const presentValue = (schema: z.ZodType): z.ZodType =>
  schema.refine((value: unknown) => value !== undefined, { message: 'Required' });

/** The envelope every leaf shares. The discriminator is consumed by the dispatch, not re-parsed. */
const envelopeSchema = (leaf: DataPointLeafClass): z.ZodType =>
  z.object({
    value: presentValue(leaf.valueSchema),
    retrieved_by: z.string(),
    first_retrieved: instantSchema,
    last_retrieved: instantSchema,
  });

/** What the envelope schema yields once parsed. */
interface EnvelopeFields {
  readonly value: unknown;
  readonly retrieved_by: string;
  readonly first_retrieved: Date;
  readonly last_retrieved: Date;
}

/** The discriminator carried by a raw payload, if it carries one at all. */
const readDiscriminator = (raw: unknown): DataPointType | undefined => {
  if (typeof raw !== 'object' || raw === null) {
    return undefined;
  }
  const value = (raw as { readonly type?: unknown }).type;
  return typeof value === 'string' ? value : undefined;
};

/** Python's `repr` of the discriminator, so the message reads the same in both ports. */
const describeDiscriminator = (discriminator: DataPointType | undefined): string =>
  discriminator === undefined ? 'None' : `'${discriminator}'`;

class RegistryAdapter implements DataPointAdapter {
  public readonly leaves: readonly DataPointLeafClass[];
  public readonly schemaKind: DataPointSchemaKind;
  private readonly byType = new Map<DataPointType, DataPointLeafClass>();
  private readonly envelopes = new Map<DataPointLeafClass, z.ZodType>();

  public constructor(leaves: readonly DataPointLeafClass[]) {
    this.leaves = leaves;
    this.schemaKind = leaves.length === 1 ? 'model' : 'tagged_union';
    for (const leaf of leaves) {
      if (leaf.type !== undefined) {
        this.byType.set(leaf.type, leaf);
      }
    }
  }

  public validate(raw: unknown): AnyDataPoint {
    const leaf = this.dispatch(raw);
    const fields = this.envelopeFor(leaf).parse(raw) as EnvelopeFields;
    const concrete = leaf as unknown as ConcreteDataPointClass;
    return new concrete({
      value: fields.value,
      retrievedBy: fields.retrieved_by as OperatorRef,
      firstRetrieved: fields.first_retrieved,
      lastRetrieved: fields.last_retrieved,
    });
  }

  /** Pick the leaf a payload belongs to, mirroring how pydantic resolves the tagged union. */
  private dispatch(raw: unknown): DataPointLeafClass {
    const discriminator = readDiscriminator(raw);
    const sole = this.leaves[0];
    if (this.schemaKind === 'model' && sole !== undefined && discriminator === undefined) {
      // A bare model has nothing to discriminate on, so an absent `type` is simply the default.
      return sole;
    }
    const leaf = discriminator === undefined ? undefined : this.byType.get(discriminator);
    if (leaf === undefined) {
      throw new UnknownDataPointTypeError(
        `cannot deserialize DataPoint with type=${describeDiscriminator(discriminator)}`,
      );
    }
    return leaf;
  }

  private envelopeFor(leaf: DataPointLeafClass): z.ZodType {
    const known = this.envelopes.get(leaf);
    if (known !== undefined) {
      return known;
    }
    const built = envelopeSchema(leaf);
    this.envelopes.set(leaf, built);
    return built;
  }
}

let cachedAdapter: DataPointAdapter | undefined;
let cachedVersion = -1;

/** The discriminated-union adapter over all registered leaves (rebuilt on change). */
export const dataPointAdapter = (): DataPointAdapter => {
  const version = registryVersion();
  if (cachedAdapter !== undefined && cachedVersion === version) {
    return cachedAdapter;
  }
  const leaves = registeredLeaves();
  if (leaves.length === 0) {
    throw new UnknownDataPointTypeError('no DataPoint leaves are registered');
  }
  cachedAdapter = new RegistryAdapter(leaves);
  cachedVersion = version;
  return cachedAdapter;
};

/**
 * Deserialize a raw payload (or revalidate a DataPoint's wire form) into its concrete leaf class.
 *
 * @throws UnknownDataPointTypeError when nothing is registered, or the discriminator names no leaf.
 * @throws ZodError when a known leaf's payload is malformed — the real validation error, preserved.
 */
export const parseDataPoint = (raw: unknown): AnyDataPoint => dataPointAdapter().validate(raw);

/** Drop the cached adapter — used by the test registry-isolation fixture. */
export const resetCache = (): void => {
  cachedAdapter = undefined;
  cachedVersion = -1;
};
