/**
 * Mongo `DurableStore` — OCC version-guarded upsert, idempotent set-add, markers.
 *
 * A document carries `version` + `epoch`; `upsert` succeeds only when the stored version equals
 * `expectedVersion` (else `OptimisticConcurrencyError`) and the stored epoch is not newer (else
 * `StaleEpochError`). Set-add uses `$addToSet` (idempotent); contribution markers are insert-once.
 * Each durable output is routed to the collection named by its `table` (the output model's
 * `tableName`) — versioned docs (`_id = key`) and set docs (`_id = key\x00field`) coexist there.
 *
 * Fencing epoch-guard docs live in dedicated companion collections (`<table>-fencing` for per-key
 * durable guards, `contributions-fencing` for contribution guards) rather than the data
 * collections. A TTL index on `updated_at` (default 7 days) bounds growth: active sessions keep
 * refreshing the timestamp and are never reaped; dead sessions' fencing docs expire naturally after
 * the configured window. TTL >> max session lifetime (<= ~15 min), so a fence is only reaped long
 * after its session is dead — the TTL never touches a live fence.
 *
 * @module
 */

import type { Collection, Db } from 'mongodb';
import { OptimisticConcurrencyError, StaleEpochError } from '../../exceptions.js';
import type { Epoch, OperatorId, SessionId } from '../../ids.js';
import { getLogger } from '../../logging.js';
import { VersionedDocument } from '../../ports/change_set.js';
import type { DurableStore, DurableUpsertOptions } from '../../ports/durable_store.js';
import type { StringIdDocument } from './indexes.js';
import { isDuplicateKeyError, isMongoDriverError } from './indexes.js';

const LOGGER_NAME = 'orcastork.adapters.mongo.durable_store';

/**
 * The byte a composite `_id` joins its parts with.
 *
 * The port of Python's `f'{key}\x00{field_name}'`: no table, key, field name or id may contain it,
 * so two different tuples can never collide into one document.
 */
const KEY_SEPARATOR = '\u0000';

/** The collection holding the real contribution markers (`_id = session\x00operator`). */
const CONTRIBUTIONS_COLLECTION = 'contributions';

/** The companion collection holding the session-scope contribution epoch guards. */
const CONTRIBUTIONS_FENCING_COLLECTION = 'contributions-fencing';

/** A week, in milliseconds — how long a fencing document outlives the session that wrote it. */
const DEFAULT_FENCING_TTL_MS = 604_800_000;

/** One second, in the milliseconds the port speaks; Mongo's TTL option is in seconds. */
const ONE_SECOND_MS = 1000;

/** How a {@link MongoDurableStore} is wired. */
export interface MongoDurableStoreOptions {
  /**
   * How long a fencing document survives its last write; a week by default.
   *
   * Milliseconds here, as everywhere in this package; the TTL index carries the seconds Mongo's
   * `expireAfterSeconds` speaks.
   */
  readonly fencingTtlMs?: number;
}

/** Curated durable outputs in Mongo, routed by table and guarded by version and epoch. */
export class MongoDurableStore implements DurableStore {
  /** The collection is chosen per call by `table` (the destination decider). */
  private readonly database: Db;

  private readonly contributions: Collection<StringIdDocument>;

  private readonly fencingTtlSeconds: number;

  /** Fencing collections that already have the TTL index ensured (at most once per process). */
  private readonly ttlEnsured = new Set<string>();

  public constructor(database: Db, options: MongoDurableStoreOptions = {}) {
    this.database = database;
    this.contributions = database.collection<StringIdDocument>(CONTRIBUTIONS_COLLECTION);
    this.fencingTtlSeconds = Math.floor((options.fencingTtlMs ?? DEFAULT_FENCING_TTL_MS) / ONE_SECOND_MS);
  }

  /** One collection of this family's own string-keyed documents. */
  private table(name: string): Collection<StringIdDocument> {
    return this.database.collection<StringIdDocument>(name);
  }

  public async read(table: string, key: string): Promise<VersionedDocument | null> {
    const stored = await this.table(table).findOne({ _id: key });
    if (stored === null) {
      return null;
    }
    // Python restores the timezone pymongo strips off a BSON date here; the Node driver hands back
    // a `Date`, which is an instant already, so the stored stamp needs no fixing up.
    return new VersionedDocument({
      document: { ...(stored.document as Record<string, unknown>) },
      version: Number(stored.version),
      status: (stored.status ?? null) as string | null,
      updatedAt: (stored.updated_at ?? null) as Date | null,
    });
  }

  public async upsert(
    table: string,
    key: string,
    document: Readonly<Record<string, unknown>>,
    options: DurableUpsertOptions,
  ): Promise<number> {
    const collection = this.table(table);
    const { expectedVersion, epoch } = options;
    // Fence on the shared per-(table, key) guard, NOT the versioned doc's own epoch: upsert and
    // addToSet both mutate this key, so they must consult and advance one fence. A per-doc check
    // would let a set-add and a versioned write to the same key be superseded behind each other.
    await this.guardDurableEpoch(table, key, epoch);
    const status = options.status ?? null;
    const updatedAt = options.updatedAt ?? null;
    const existing = await collection.findOne({ _id: key });
    if (existing === null) {
      if (expectedVersion !== 0) {
        throw new OptimisticConcurrencyError(
          `expected version ${expectedVersion} but ${table}/'${key}' does not exist`,
        );
      }
      try {
        await collection.insertOne({
          _id: key,
          document,
          version: 1,
          epoch: Number(epoch),
          status,
          updated_at: updatedAt,
        });
      } catch (error) {
        if (!isDuplicateKeyError(error)) {
          throw error;
        }
        throw new OptimisticConcurrencyError(`concurrent insert of ${table}/'${key}'`, { cause: error });
      }
      return 1;
    }
    if (Number(existing.version) !== expectedVersion) {
      throw new OptimisticConcurrencyError(
        `version conflict on ${table}/'${key}': expected ${expectedVersion}, got ${String(existing.version)}`,
      );
    }
    const newVersion = expectedVersion + 1;
    const updated = await collection.findOneAndUpdate(
      { _id: key, version: expectedVersion },
      { $set: { document, version: newVersion, epoch: Number(epoch), status, updated_at: updatedAt } },
      { returnDocument: 'after' },
    );
    if (updated === null) {
      throw new OptimisticConcurrencyError(`version conflict on ${table}/'${key}' (lost the race)`);
    }
    return newVersion;
  }

  public async addToSet(
    table: string,
    key: string,
    fieldName: string,
    value: string,
    options: { readonly epoch: Epoch },
  ): Promise<number> {
    const collection = this.table(table);
    // Share the upsert fence scope: the guard is per (table, key), so a superseded predecessor
    // cannot mutate ANY set on a key a higher epoch already touched — and cannot diverge from a
    // versioned-doc write to the same key. The set doc itself only carries membership.
    await this.guardDurableEpoch(table, key, options.epoch);
    const setId = `${key}${KEY_SEPARATOR}${fieldName}`;
    // One round-trip that both adds and reports, so the size is the set as of THIS write. Adding
    // and then reading back separately lets a concurrent addToSet on the same key land in the gap,
    // and the caller is handed a count matching no single writer's view. The epoch guard above does
    // not cover this: it fences a superseded predecessor, not two writers within the same epoch.
    const stored = await collection.findOneAndUpdate(
      { _id: setId },
      { $addToSet: { members: value } },
      { upsert: true, returnDocument: 'after' },
    );
    // `upsert` plus `returnDocument: 'after'` always hands the stored document back.
    const members = (stored?.members ?? []) as readonly string[];
    return members.length;
  }

  public async markContribution(
    sessionId: SessionId,
    operatorId: OperatorId,
    options: { readonly epoch: Epoch },
  ): Promise<boolean> {
    await this.guardContributionEpoch(sessionId, options.epoch);
    try {
      await this.contributions.insertOne({
        _id: `${sessionId}${KEY_SEPARATOR}${operatorId}`,
        epoch: Number(options.epoch),
      });
      return true;
    } catch (error) {
      if (!isDuplicateKeyError(error)) {
        throw error;
      }
      return false;
    }
  }

  public async isContributionMarked(sessionId: SessionId, operatorId: OperatorId): Promise<boolean> {
    return (await this.contributions.findOne({ _id: `${sessionId}${KEY_SEPARATOR}${operatorId}` })) !== null;
  }

  public async clearContribution(sessionId: SessionId, operatorId: OperatorId): Promise<void> {
    await this.contributions.deleteOne({ _id: `${sessionId}${KEY_SEPARATOR}${operatorId}` });
  }

  /**
   * Lazily create the TTL index on `updated_at` the first time we write to a fencing collection.
   *
   * The cache avoids a round-trip on every guard call. A pre-existing index with a different
   * `expireAfterSeconds` is an ops concern (it requires a manual drop and recreate); we log and
   * proceed rather than failing writes over an index conflict.
   */
  private async ensureFencingTtl(fencingCollectionName: string): Promise<void> {
    if (this.ttlEnsured.has(fencingCollectionName)) {
      return;
    }
    try {
      await this.table(fencingCollectionName).createIndex(
        { updated_at: 1 },
        { expireAfterSeconds: this.fencingTtlSeconds },
      );
    } catch (error) {
      if (!isMongoDriverError(error)) {
        throw error;
      }
      getLogger().warning(
        'Could not ensure TTL index on fencing collection — index may already exist with a different TTL',
        { logger_name: LOGGER_NAME, fencing_collection: fencingCollectionName, error },
      );
    }
    this.ttlEnsured.add(fencingCollectionName);
  }

  /**
   * Per-(table, key) epoch fence shared by `upsert` and `addToSet`.
   *
   * Mirrors the in-memory adapter, which fences per (table, key). The guard lives in a dedicated
   * companion collection (`<table>-fencing`) so it never pollutes the data collection; `_id = key`
   * (no namespacing is needed in a dedicated collection), and `updated_at` enables TTL reaping
   * after sessions are dead.
   */
  private async guardDurableEpoch(table: string, key: string, epoch: Epoch): Promise<void> {
    const fencingName = `${table}-fencing`;
    await this.ensureFencingTtl(fencingName);
    await this.advanceFence(fencingName, key, epoch, `epoch ${epoch} is stale for durable '${key}'`);
  }

  /**
   * Session-scope epoch fence for contribution markers.
   *
   * Mirrors the in-memory adapter: a fenced predecessor must not be able to insert a contribution
   * marker that a higher epoch would then read as already done. The guard lives in
   * `contributions-fencing` (`_id = sessionId`) separate from the real markers in `contributions`
   * (`_id = session\x00operator`), so the data collection stays clean. `updated_at` enables TTL
   * reaping.
   */
  private async guardContributionEpoch(sessionId: SessionId, epoch: Epoch): Promise<void> {
    await this.ensureFencingTtl(CONTRIBUTIONS_FENCING_COLLECTION);
    await this.advanceFence(
      CONTRIBUTIONS_FENCING_COLLECTION,
      sessionId,
      epoch,
      `epoch ${epoch} is stale for contributions in session ${sessionId}`,
    );
  }

  /**
   * Reject a superseded writer and record the highest ACCEPTED epoch for the fence.
   *
   * The conditional upsert matches only while the stored epoch is not newer; a stale writer fails
   * the predicate, and the upsert's insert then collides on `_id` — either the driver raises the
   * duplicate key or the result reports neither a match nor an upsert, and both mean superseded.
   *
   * `updated_at` is stamped by the SERVER (`$currentDate`) rather than by this process: the TTL
   * reaper that acts on it runs on the server's clock, and a framework module reads no wall clock
   * of its own.
   */
  private async advanceFence(
    fencingCollectionName: string,
    fenceId: string,
    epoch: Epoch,
    staleMessage: string,
  ): Promise<void> {
    const current = Number(epoch);
    const notSuperseded = [{ epoch: { $exists: false } }, { epoch: { $lte: current } }];
    let result: { readonly matchedCount: number; readonly upsertedId: unknown };
    try {
      result = await this.table(fencingCollectionName).updateOne(
        { _id: fenceId, $or: notSuperseded },
        { $max: { epoch: current }, $currentDate: { updated_at: true } },
        { upsert: true },
      );
    } catch (error) {
      if (!isDuplicateKeyError(error)) {
        throw error;
      }
      throw new StaleEpochError(staleMessage, { cause: error });
    }
    if (result.matchedCount === 0 && result.upsertedId === null) {
      throw new StaleEpochError(staleMessage);
    }
  }
}
