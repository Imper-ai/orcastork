/**
 * In-memory `DurableStore` — OCC upsert, idempotent set-add, contribution markers.
 *
 * Optimistic concurrency: a document carries a version; `upsert` succeeds only when the caller's
 * `expectedVersion` matches, otherwise throws `OptimisticConcurrencyError`. Set-add is idempotent;
 * contribution markers make an aggregator's effect at-most-once. Each durable output is namespaced
 * by its `table` (the destination decider), and every mutation is epoch-guarded per `(table, key)`.
 *
 * @module
 */

import { OptimisticConcurrencyError, StaleEpochError } from '../../exceptions.js';
import type { Epoch, OperatorId, SessionId } from '../../ids.js';
import { VersionedDocument } from '../../ports/change_set.js';
import type { DurableStore, DurableUpsertOptions } from '../../ports/durable_store.js';

/**
 * The byte a composite map key joins its parts with.
 *
 * The port of Python's tuple keys: no table, key, field name or id may contain it, so two
 * different tuples can never collide into one string.
 */
const KEY_SEPARATOR = '\u0000';

/** One stored document: the curated business document plus its OCC and status metadata. */
interface StoredDocument {
  document: Readonly<Record<string, unknown>>;
  version: number;
  status: string | null;
  updatedAt: Date | null;
}

/**
 * A detached copy of a document, so neither the caller's object nor the stored one can be mutated
 * through the other — the port of the `deepcopy` on both the write and the read path.
 *
 * A durable document is persisted state, so it is structured-cloneable by construction: a real
 * backend serializes it on the way out.
 */
const detached = (document: Readonly<Record<string, unknown>>): Readonly<Record<string, unknown>> =>
  structuredClone(document) as Readonly<Record<string, unknown>>;

/** Curated durable outputs held in process memory — the test and local-run substrate. */
export class InMemoryDurableStore implements DurableStore {
  /** `table` + `key` → the stored document. */
  private readonly documents = new Map<string, StoredDocument>();

  /** `table` + `key` + `fieldName` → the set's members. */
  private readonly sets = new Map<string, Set<string>>();

  /** `sessionId` + `operatorId` for every aggregator that has contributed. */
  private readonly contributions = new Set<string>();

  /** Fencing scope (`table` + `key`, or `contribution:<sessionId>`) → highest accepted epoch. */
  private readonly maxEpoch = new Map<string, number>();

  /**
   * Reject a superseded writer and record the highest ACCEPTED epoch for the scope.
   *
   * The scope is the whole `(table, key)` — never a single field — so the versioned document and
   * every set on that key share one fence and neither write path can be superseded behind the
   * other's back.
   */
  private guardEpoch(scope: string, epoch: Epoch): void {
    const current = this.maxEpoch.get(scope) ?? 0;
    if (epoch < current) {
      throw new StaleEpochError(`epoch ${epoch} is stale for '${scope}'`);
    }
    this.maxEpoch.set(scope, epoch);
  }

  public async read(table: string, key: string): Promise<VersionedDocument | null> {
    const stored = this.documents.get(`${table}${KEY_SEPARATOR}${key}`);
    if (stored === undefined) {
      return null;
    }
    return new VersionedDocument({
      document: detached(stored.document),
      version: stored.version,
      status: stored.status,
      updatedAt: stored.updatedAt,
    });
  }

  public async upsert(
    table: string,
    key: string,
    document: Readonly<Record<string, unknown>>,
    options: DurableUpsertOptions,
  ): Promise<number> {
    const scope = `${table}${KEY_SEPARATOR}${key}`;
    this.guardEpoch(scope, options.epoch);
    const current = this.documents.get(scope);
    const currentVersion = current === undefined ? 0 : current.version;
    if (currentVersion !== options.expectedVersion) {
      throw new OptimisticConcurrencyError(
        `version conflict on ${table}/'${key}': expected ${options.expectedVersion}, got ${currentVersion}`,
      );
    }
    const newVersion = currentVersion + 1;
    this.documents.set(scope, {
      document: detached(document),
      version: newVersion,
      status: options.status ?? null,
      updatedAt: options.updatedAt ?? null,
    });
    return newVersion;
  }

  public async addToSet(
    table: string,
    key: string,
    fieldName: string,
    value: string,
    options: { readonly epoch: Epoch },
  ): Promise<number> {
    this.guardEpoch(`${table}${KEY_SEPARATOR}${key}`, options.epoch);
    const field = `${table}${KEY_SEPARATOR}${key}${KEY_SEPARATOR}${fieldName}`;
    const known = this.sets.get(field);
    const members = known ?? new Set<string>();
    if (known === undefined) {
      this.sets.set(field, members);
    }
    members.add(value);
    return members.size;
  }

  public async markContribution(
    sessionId: SessionId,
    operatorId: OperatorId,
    options: { readonly epoch: Epoch },
  ): Promise<boolean> {
    this.guardEpoch(`contribution:${sessionId}`, options.epoch);
    const marker = `${sessionId}${KEY_SEPARATOR}${operatorId}`;
    if (this.contributions.has(marker)) {
      return false;
    }
    this.contributions.add(marker);
    return true;
  }

  public async isContributionMarked(sessionId: SessionId, operatorId: OperatorId): Promise<boolean> {
    return this.contributions.has(`${sessionId}${KEY_SEPARATOR}${operatorId}`);
  }

  public async clearContribution(sessionId: SessionId, operatorId: OperatorId): Promise<void> {
    this.contributions.delete(`${sessionId}${KEY_SEPARATOR}${operatorId}`);
  }
}
