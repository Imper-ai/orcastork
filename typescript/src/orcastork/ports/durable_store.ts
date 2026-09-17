/**
 * The `DurableStore` port — idempotent, OCC-guarded durable outputs + contribution markers.
 *
 * Aggregators are the sole writers of durable *outputs*: shared aggregates use optimistic
 * concurrency — read a {@link VersionedDocument}, compute the next document, then `upsert` with the
 * expected version (a mismatch raises `OptimisticConcurrencyError` → caller retries). The
 * **destination is decided by the written model's** `table` (its `tableName`) — the adapter routes
 * each write to that collection rather than a single shared one. The store also holds a
 * `(sessionId, operatorId)` contribution marker that makes an aggregator's effect at-most-once.
 * Every mutating method is epoch-guarded. Session completion lives on the `SessionLock`, co-located
 * with the fencing epoch.
 *
 * @module
 */

import type { Epoch, OperatorId, SessionId } from '../ids.js';
import type { VersionedDocument } from './change_set.js';

/** The optimistic-concurrency and record-metadata arguments of a durable upsert. */
export interface DurableUpsertOptions {
  /** The version the caller read; the write lands only if the stored version still equals it. */
  readonly expectedVersion: number;

  readonly epoch: Epoch;

  /** Framework-owned record status (`'in_progress'` / `'final'`), stored beside the document. */
  readonly status?: string | null;

  /** Framework-owned stamp of when the record was last written. */
  readonly updatedAt?: Date | null;
}

/** Curated durable outputs, routed by table, guarded by version and epoch. */
export interface DurableStore {
  /** Read a document + its version from `table`, or `null` if absent. */
  read(table: string, key: string): Promise<VersionedDocument | null>;

  /**
   * Insert/replace `key` in `table` iff its stored version equals `expectedVersion`.
   *
   * `table` is the destination collection (the output model's `tableName`). Returns the new
   * version. `status`/`updatedAt` are framework-owned record metadata stored alongside
   * `version`/`epoch` (the business `document` is never mutated).
   *
   * @throws OptimisticConcurrencyError on a version mismatch.
   * @throws StaleEpochError on a stale epoch.
   */
  upsert(
    table: string,
    key: string,
    document: Readonly<Record<string, unknown>>,
    options: DurableUpsertOptions,
  ): Promise<number>;

  /**
   * Idempotently add `value` to a set-valued field of `key` in `table`; returns the size.
   *
   * Set-cardinality union (never a double-counting increment): re-adding a member leaves the size
   * unchanged, a distinct member grows it. Epoch-fenced on the SAME per-`(table, key)` scope as
   * {@link DurableStore.upsert} — once any epoch has mutated a key, a strictly-lower-epoch write to
   * that key (any field, or its versioned document) is rejected, so the two write paths can never
   * be superseded behind each other (no split-brain).
   *
   * @throws StaleEpochError on a stale epoch.
   */
  addToSet(
    table: string,
    key: string,
    fieldName: string,
    value: string,
    options: { readonly epoch: Epoch },
  ): Promise<number>;

  /**
   * Record an aggregator's contribution; `true` if newly marked, `false` if already done.
   *
   * @throws StaleEpochError on a stale epoch.
   */
  markContribution(sessionId: SessionId, operatorId: OperatorId, options: { readonly epoch: Epoch }): Promise<boolean>;

  /** Whether an aggregator has already contributed for this session (resume skips it). */
  isContributionMarked(sessionId: SessionId, operatorId: OperatorId): Promise<boolean>;

  /**
   * Remove an aggregator's contribution marker so a re-open can re-aggregate.
   *
   * Called by the manager before re-opening a completed session for late non-ephemeral data:
   * clearing the marker lets the next orchestrator run the aggregator again, folding the new data
   * into the result. The new orchestrator acquires a fresh, strictly higher epoch, so the
   * re-aggregation's `markContribution` call is epoch-fenced against any stale predecessor.
   */
  clearContribution(sessionId: SessionId, operatorId: OperatorId): Promise<void>;
}
