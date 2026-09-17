/**
 * The `ArchivedDataPoint` model — one durable document per `(session, type, value)`.
 *
 * Framework-owned and storage-free; the Mongo adapter binds it to the `orcastork-datapoints`
 * collection. Identity mirrors the keyed-merge (timestamps excluded, like the live store's
 * identity): `(sessionId, type, valueHash)`. PII values are encrypted at rest by the adapter via
 * the injected `ValueCipher`; the model itself always carries the plaintext value.
 *
 * @module
 */

import { z } from 'zod';
import type { AnyDataPoint } from '../datapoints/index.js';
import type { DataPointType, Epoch, NamespaceId, OperatorRef, SessionId } from '../ids.js';

/** Where an archived observation came from — the provenance the orchestrator stamps on it. */
export interface ArchiveProvenance {
  readonly sessionId: SessionId;

  readonly namespaceId: NamespaceId;

  /** The fencing token of the writer that observed the DataPoint. */
  readonly epoch: Epoch;
}

/** The fields an {@link ArchivedDataPoint} is built from. */
export interface ArchivedDataPointInit {
  readonly sessionId: SessionId;

  readonly namespaceId: NamespaceId;

  /** The DataPoint discriminator (part of the upsert key). */
  readonly type: DataPointType;

  /**
   * Derived by the archive adapter (which holds the cipher key): a keyed MAC of the value for PII,
   * a plain SHA-256 otherwise. Left empty on construction; never the plaintext value for PII.
   */
  readonly valueHash?: string;

  /** Encrypted at rest by the adapter when `isPii` (the model holds plaintext). */
  readonly value: unknown;

  readonly retrievedBy: OperatorRef;

  /** Set on first observation, immutable. */
  readonly firstRetrieved: Date;

  /** Bumped on every re-observation (keyed-upsert). */
  readonly lastRetrieved: Date;

  readonly isPii: boolean;

  /** Fencing token of the writer. */
  readonly epoch: Epoch;

  readonly schemaVersion?: number;
}

/** `ArchivedDataPoint` as persisted — Python's `model_dump(mode='json')`, key for key. */
export interface ArchivedDataPointWire {
  readonly session_id: string;
  readonly namespace_id: string;
  readonly type: string;
  readonly value_hash: string;
  readonly value: unknown;
  readonly retrieved_by: string;
  readonly first_retrieved: string;
  readonly last_retrieved: string;
  readonly is_pii: boolean;
  readonly epoch: number;
  readonly schema_version: number;
}

const initSchema = z.object({
  sessionId: z.string(),
  namespaceId: z.string(),
  type: z.string(),
  valueHash: z.string().optional(),
  retrievedBy: z.string(),
  firstRetrieved: z.date(),
  lastRetrieved: z.date(),
  isPii: z.boolean(),
  epoch: z.number().int(),
  schemaVersion: z.number().int().optional(),
});

/**
 * Instants are read tolerantly: an ISO-8601 string with either spelling of UTC, or the `Date` a
 * driver hands back for a BSON date (the committed archive rows carry real dates so `$max` orders
 * them chronologically and the retention TTL can act on `last_retrieved`).
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
 * A stored `value` must be *present*, whatever its shape.
 *
 * `z.unknown()` alone accepts a missing key, which would let a truncated row read back as a
 * document whose value is nothing at all — and whose archive key is the canonical form of that.
 */
const presentValue = z.unknown().refine((value: unknown) => value !== undefined, { message: 'Required' });

const wireSchema = z.object({
  session_id: z.string(),
  namespace_id: z.string(),
  type: z.string(),
  value_hash: z.string().optional(),
  value: presentValue,
  retrieved_by: z.string(),
  first_retrieved: instantSchema,
  last_retrieved: instantSchema,
  is_pii: z.boolean(),
  epoch: z.number().int(),
  schema_version: z.number().int().optional(),
});

/**
 * One durable archive document: a raw DataPoint plus the provenance of the run that saw it.
 *
 * Instances are frozen in the constructor (the port of `ConfigDict(frozen=True)`), so a subclass
 * redeclares {@link ArchivedDataPoint.tableName} and nothing else — a field initializer of its own
 * would run after the freeze.
 */
export class ArchivedDataPoint {
  /**
   * The durable destination, decided here by the model — the adapter routes by it rather than
   * hardcoding a collection. A consuming flow may subclass with its own `tableName`.
   */
  public static readonly tableName: string = 'orcastork-datapoints';

  public readonly sessionId: SessionId;

  public readonly namespaceId: NamespaceId;

  /** The DataPoint discriminator (part of the upsert key). */
  public readonly type: DataPointType;

  /**
   * The archive key for the value: a keyed MAC for PII, a plain SHA-256 otherwise.
   *
   * Empty until the adapter — which holds the cipher key — derives it, so an entry handed to the
   * archive carries no digest of a PII value the caller could not have keyed.
   */
  public readonly valueHash: string;

  /** Encrypted at rest by the adapter when `isPii`; this model always holds the plaintext. */
  public readonly value: unknown;

  public readonly retrievedBy: OperatorRef;

  /** Set on first observation, immutable. */
  public readonly firstRetrieved: Date;

  /** Bumped on every re-observation (keyed-upsert). */
  public readonly lastRetrieved: Date;

  public readonly isPii: boolean;

  /** Fencing token of the writer. */
  public readonly epoch: Epoch;

  public readonly schemaVersion: number;

  public constructor(init: ArchivedDataPointInit) {
    initSchema.parse(init);
    this.sessionId = init.sessionId;
    this.namespaceId = init.namespaceId;
    this.type = init.type;
    this.valueHash = init.valueHash ?? '';
    this.value = init.value;
    this.retrievedBy = init.retrievedBy;
    // Copies: `Date` is mutable, and a frozen document that handed out a live one is not frozen.
    this.firstRetrieved = new Date(init.firstRetrieved.getTime());
    this.lastRetrieved = new Date(init.lastRetrieved.getTime());
    this.isPii = init.isPii;
    this.epoch = init.epoch;
    this.schemaVersion = init.schemaVersion ?? 1;
    Object.freeze(this);
  }

  /** Project a live DataPoint into its archive document (provenance supplied by the orchestrator). */
  public static fromDataPoint<T extends ArchivedDataPoint>(
    this: new (
      init: ArchivedDataPointInit,
    ) => T,
    dataPoint: AnyDataPoint,
    provenance: ArchiveProvenance,
  ): T {
    return new this({
      sessionId: provenance.sessionId,
      namespaceId: provenance.namespaceId,
      type: dataPoint.type,
      // The JSON projection of the value, not the live one: what is archived must be what a reader
      // gets back out of the document.
      value: dataPoint.toWire().value,
      retrievedBy: dataPoint.retrievedBy,
      firstRetrieved: dataPoint.firstRetrieved,
      lastRetrieved: dataPoint.lastRetrieved,
      isPii: dataPoint.isPii,
      epoch: provenance.epoch,
    });
  }

  /**
   * A copy with `changes` applied — the port of pydantic's `model_copy(update=...)`.
   *
   * The instance is frozen, so every field the sealing seam and the keyed-upsert fold advance
   * (the sealed value, the derived key, `lastRetrieved`, `epoch`) is applied by rebuilding. Built
   * through `this.constructor`, so a flow that subclasses the model keeps its own class.
   */
  public copyWith(changes: Partial<ArchivedDataPointInit>): this {
    const subclass = this.constructor as new (init: ArchivedDataPointInit) => this;
    return new subclass({ ...this.toInit(), ...changes });
  }

  /** The persisted form: snake_case keys, ISO instants — what an adapter writes. */
  public toWire(): ArchivedDataPointWire {
    return {
      session_id: this.sessionId,
      namespace_id: this.namespaceId,
      type: this.type,
      value_hash: this.valueHash,
      value: this.value,
      retrieved_by: this.retrievedBy,
      first_retrieved: this.firstRetrieved.toISOString(),
      last_retrieved: this.lastRetrieved.toISOString(),
      is_pii: this.isPii,
      epoch: this.epoch,
      schema_version: this.schemaVersion,
    };
  }

  private toInit(): ArchivedDataPointInit {
    return {
      sessionId: this.sessionId,
      namespaceId: this.namespaceId,
      type: this.type,
      valueHash: this.valueHash,
      value: this.value,
      retrievedBy: this.retrievedBy,
      firstRetrieved: this.firstRetrieved,
      lastRetrieved: this.lastRetrieved,
      isPii: this.isPii,
      epoch: this.epoch,
      schemaVersion: this.schemaVersion,
    };
  }
}

/**
 * Read a persisted archive document back — the counterpart of {@link ArchivedDataPoint.toWire}.
 *
 * Extra keys are ignored (a committed Mongo row carries the composite `_id` the adapter derives),
 * and both date spellings are accepted, so a buffered row (ISO strings) and a committed row (BSON
 * dates) validate through the same path.
 */
export const parseArchivedDataPoint = (raw: unknown): ArchivedDataPoint => {
  const wire = wireSchema.parse(raw);
  return new ArchivedDataPoint({
    sessionId: wire.session_id as SessionId,
    namespaceId: wire.namespace_id as NamespaceId,
    type: wire.type,
    valueHash: wire.value_hash ?? '',
    value: wire.value,
    retrievedBy: wire.retrieved_by as OperatorRef,
    firstRetrieved: wire.first_retrieved,
    lastRetrieved: wire.last_retrieved,
    isPii: wire.is_pii,
    epoch: wire.epoch as Epoch,
    schemaVersion: wire.schema_version ?? 1,
  });
};
