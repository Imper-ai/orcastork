/**
 * Value types shared across ports.
 *
 * @module
 */

import type { AnyDataPoint } from '../datapoints/index.js';

/** The fields a {@link ChangeSet} is built from. */
export interface ChangeSetInit {
  readonly added: readonly AnyDataPoint[];
  readonly updated: readonly AnyDataPoint[];
}

/**
 * The DataPoints that changed since a given store revision.
 *
 * `added` are new `(type, value)` identities; `updated` are existing identities re-observed since
 * the revision (`lastRetrieved` advanced).
 */
export class ChangeSet {
  public readonly added: readonly AnyDataPoint[];

  public readonly updated: readonly AnyDataPoint[];

  public constructor(init: ChangeSetInit) {
    // Copied and frozen: a change set is handed to readiness logic that must not be able to
    // mutate the store's answer under the next reader.
    this.added = Object.freeze([...init.added]);
    this.updated = Object.freeze([...init.updated]);
    Object.freeze(this);
  }
}

/** The fields an {@link InboxEntry} is built from. */
export interface InboxEntryInit {
  readonly entryId: string;
  readonly dataPoint: AnyDataPoint;
  readonly deliveryCount: number;
}

/** A claimed inbox message: a DataPoint plus its delivery bookkeeping. */
export class InboxEntry {
  public readonly entryId: string;

  public readonly dataPoint: AnyDataPoint;

  public readonly deliveryCount: number;

  public constructor(init: InboxEntryInit) {
    this.entryId = init.entryId;
    this.dataPoint = init.dataPoint;
    this.deliveryCount = init.deliveryCount;
    Object.freeze(this);
  }
}

/** The fields a {@link PoisonInboxEntry} is built from. */
export interface PoisonInboxEntryInit {
  readonly entryId: string;
  readonly error: string;
  readonly deliveryCount: number;

  /** The undecodable wire payload, when cheaply available. */
  readonly rawPayload?: string | null;
}

/**
 * An inbox entry whose wire payload is malformed (bytes that do not decode).
 *
 * Adapters deliver this instead of raising for wire-format decode failures ONLY — semantic
 * deserialization failures (an unknown DataPoint type, a validation error) propagate so a
 * parser/registry regression surfaces loudly. Redelivery can never fix malformed bytes, so the
 * orchestrator quarantines these on sight.
 *
 * A class rather than a bare shape so `entry instanceof PoisonInboxEntry` narrows a
 * {@link DeliveredInboxEntry}, exactly as Python's `isinstance` does.
 */
export class PoisonInboxEntry {
  public readonly entryId: string;

  public readonly error: string;

  public readonly deliveryCount: number;

  /** The undecodable wire payload, when cheaply available. */
  public readonly rawPayload: string | null;

  public constructor(init: PoisonInboxEntryInit) {
    this.entryId = init.entryId;
    this.error = init.error;
    this.deliveryCount = init.deliveryCount;
    this.rawPayload = init.rawPayload ?? null;
    Object.freeze(this);
  }
}

/**
 * What `consume`/`reclaim` deliver: a parsed entry, or the poison representation of one that could
 * not be parsed — adapters never raise per entry, so one bad message cannot block the rest of its
 * batch.
 */
export type DeliveredInboxEntry = InboxEntry | PoisonInboxEntry;

/** The fields a {@link QuarantinedEntry} is built from. */
export interface QuarantinedEntryInit {
  readonly entryId: string;
  readonly reason: string;
  readonly deliveryCount: number;

  /** The original wire payload, when cheaply available. */
  readonly rawPayload?: string | null;
}

/** A durably-recorded quarantined inbox entry (operator inspection / manual re-drive). */
export class QuarantinedEntry {
  public readonly entryId: string;

  public readonly reason: string;

  public readonly deliveryCount: number;

  /** The original wire payload, when cheaply available. */
  public readonly rawPayload: string | null;

  public constructor(init: QuarantinedEntryInit) {
    this.entryId = init.entryId;
    this.reason = init.reason;
    this.deliveryCount = init.deliveryCount;
    this.rawPayload = init.rawPayload ?? null;
    Object.freeze(this);
  }
}

/** The fields a {@link VersionedDocument} is built from. */
export interface VersionedDocumentInit {
  readonly document: Readonly<Record<string, unknown>>;
  readonly version: number;

  /** `'in_progress'` during interim aggregation, `'final'` once finalized. */
  readonly status?: string | null;

  /** When the framework last stamped the record. */
  readonly updatedAt?: Date | null;
}

/** A durable document plus its optimistic-concurrency version and live-status metadata. */
export class VersionedDocument {
  public readonly document: Readonly<Record<string, unknown>>;

  public readonly version: number;

  /** `'in_progress'` during interim aggregation, `'final'` once finalized. */
  public readonly status: string | null;

  /** When the framework last stamped the record. */
  public readonly updatedAt: Date | null;

  public constructor(init: VersionedDocumentInit) {
    this.document = init.document;
    this.version = init.version;
    this.status = init.status ?? null;
    this.updatedAt = init.updatedAt ?? null;
    Object.freeze(this);
  }
}
