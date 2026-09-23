/**
 * Mongo `DataPointArchive` — a durable per-observation buffer folded into raw docs.
 *
 * `archiveMany` allocates one contiguous sequence range under a single meta CAS (rejecting a
 * stale-epoch writer for the whole batch, exactly as the audit sink does) and buffers the
 * epoch-stamped, sequence-numbered documents in one ordered `insertMany`; `archive` is the batch of
 * one. `flush` folds the buffered observations into the `orcastork-datapoints` collection via
 * **keyed-upsert** on a composite `_id` of `(session, type, valueHash)`: `$setOnInsert` fixes the
 * immutable fields and `$max` advances `last_retrieved`/`epoch`, so redelivery and replay are
 * idempotent and the buffer survives a crash before flush.
 *
 * PII protection: the value is sealed via a `ValueCipher`, and the `value_hash` key is a **keyed**
 * MAC of the value (mixing in `sessionId`) — never a bare plaintext digest. The cipher is resolved
 * per batch: an injected `NamespaceCipherProvider` keys each namespace's PII under its own key
 * (resolved from the entries' `namespaceId`), or a single static cipher applies to all namespaces.
 * This adapter is the *production*, PII-heavy store, so it **fails closed**: archiving an `isPii`
 * value while only the passthrough `NullCipher` is wired (or the provider could not resolve a key)
 * raises `UnprotectedPiiError` rather than persisting cleartext. An optional retention TTL ages the
 * raw archive out on `last_retrieved` (a native BSON date).
 *
 * Python drops every stamp to the millisecond a BSON date can hold on the way into the buffer, so
 * both sides of a flush carry the same instant; a JS `Date` is already at exactly that resolution,
 * so the port has nothing to truncate and the two sides agree for free.
 *
 * @module
 */

import type { AnyBulkWriteOperation, Collection, Db } from 'mongodb';
import type { NamespaceCipherProvider, ValueCipher } from '../../archive/index.js';
import { ArchivedDataPoint, NullCipher, parseArchivedDataPoint } from '../../archive/index.js';
import { seal, unseal, valueHash } from '../../archive/sealing.js';
import { PiiKeyUnavailableError, StaleEpochError, UnprotectedPiiError } from '../../exceptions.js';
import type { NamespaceId, SessionId } from '../../ids.js';
import { getLogger } from '../../logging.js';
import type { DataPointArchive } from '../../ports/datapoint_archive.js';
import type { StringIdDocument } from './indexes.js';
import { ensureIndex, isDuplicateKeyError } from './indexes.js';

const LOGGER_NAME = 'orcastork.adapters.mongo.datapoint_archive';

/** The collection each observation is buffered in until a flush folds it in. */
const BUFFER_COLLECTION = 'datapoint_archive_buffer';

/** The collection holding one sequence/epoch allocator document per session. */
const META_COLLECTION = 'datapoint_archive_meta';

/**
 * The byte the composite `_id` and the fold key join their parts with.
 *
 * The port of Python's `f'{session}\x00{type}\x00{value_hash}'` and its `(type, value_hash)` tuple
 * key: none of the parts can contain it, so two identities never collide into one.
 */
const KEY_SEPARATOR = '\u0000';

/** One second, in the milliseconds the port speaks; Mongo's TTL option is in seconds. */
const ONE_SECOND_MS = 1000;

/**
 * Rows carried per round-trip, by a flush draining the buffer and by a read paging either side.
 *
 * The audit sink writes at append and so needs no equivalent; this one is the archive's own.
 * Exported for the same reason as the audit sink's: a test about the paging sizes its fixture from
 * it, a module constant being unpatchable where Python narrows `_PAGE_SIZE`.
 */
export const PAGE_SIZE = 500;

/**
 * Apply one sealed observation the way the committed keyed-upsert would.
 *
 * Mirrors the `$setOnInsert` + `$max` merge `flush` performs in Mongo: the first observation of a
 * `(type, valueHash)` supplies every field, and a re-observation only advances `lastRetrieved` and
 * `epoch`. This is a second expression of that merge, in TypeScript, so it is pinned to the first
 * by the conformance property that a read before a flush equals a read after one — a divergence
 * fails the suite rather than silently returning what no flush would produce.
 */
export const foldIn = (folded: Map<string, ArchivedDataPoint>, sealed: ArchivedDataPoint): void => {
  const key = `${sealed.type}${KEY_SEPARATOR}${sealed.valueHash}`;
  const existing = folded.get(key);
  if (existing === undefined) {
    folded.set(key, sealed);
    return;
  }
  folded.set(
    key,
    existing.copyWith({
      lastRetrieved: new Date(Math.max(existing.lastRetrieved.getTime(), sealed.lastRetrieved.getTime())),
      epoch: existing.epoch > sealed.epoch ? existing.epoch : sealed.epoch,
    }),
  );
};

/** How a {@link MongoDataPointArchive} is wired. */
export interface MongoDataPointArchiveOptions {
  /** Seals PII values at rest and derives their keys; applies to every namespace. */
  readonly cipher?: ValueCipher;

  /** Per-namespace keys; takes precedence over a single static {@link MongoDataPointArchiveOptions.cipher}. */
  readonly cipherProvider?: NamespaceCipherProvider;

  /**
   * How long a committed row survives its last observation; unbounded when omitted.
   *
   * Milliseconds here, as everywhere in this package; the TTL index carries the seconds Mongo's
   * `expireAfterSeconds` speaks.
   */
  readonly retentionMs?: number;
}

/** The second durable write path, in Mongo: a per-observation buffer folded into raw documents. */
export class MongoDataPointArchive implements DataPointArchive {
  private readonly database: Db;

  private readonly buffer: Collection;

  /**
   * The committed collection is named by the model's `tableName` (the destination decider), not a
   * hardcoded constant — a flow that subclasses `ArchivedDataPoint` reroutes by overriding it.
   */
  private readonly committed: Collection<StringIdDocument>;

  private readonly meta: Collection<StringIdDocument>;

  /**
   * A per-namespace provider (PII sealed under a per-namespace key) takes precedence; otherwise a
   * single static cipher applies to every namespace. With neither, the passthrough refuses PII
   * (fails closed).
   */
  private readonly cipher: ValueCipher;

  private readonly cipherProvider: NamespaceCipherProvider | null;

  private readonly retentionSeconds: number | null;

  private indexesReady = false;

  public constructor(database: Db, options: MongoDataPointArchiveOptions = {}) {
    this.database = database;
    this.buffer = database.collection(BUFFER_COLLECTION);
    this.committed = database.collection<StringIdDocument>(ArchivedDataPoint.tableName);
    this.meta = database.collection<StringIdDocument>(META_COLLECTION);
    this.cipher = options.cipher ?? new NullCipher();
    this.cipherProvider = options.cipherProvider ?? null;
    this.retentionSeconds = options.retentionMs === undefined ? null : Math.floor(options.retentionMs / ONE_SECOND_MS);
  }

  /**
   * Resolve the cipher for a namespace.
   *
   * Without a provider, the static cipher applies to all namespaces. A provider failure degrades to
   * `NullCipher` so the PII gate refuses rather than sealing under a wrong/shared key — fail closed.
   */
  private async cipherFor(namespaceId: NamespaceId): Promise<ValueCipher> {
    if (this.cipherProvider === null) {
      return this.cipher;
    }
    try {
      return await this.cipherProvider.forNamespace(namespaceId);
    } catch (error) {
      // Provider-defined failures; degrade to the refusing passthrough.
      getLogger().warning('per-namespace cipher provider failed to resolve key; degrading to fail-closed', {
        logger_name: LOGGER_NAME,
        namespace_id: namespaceId,
        error,
      });
      return new NullCipher();
    }
  }

  private async ensureIndexes(): Promise<void> {
    if (this.indexesReady) {
      return;
    }
    // Secondary index for analytics scans by namespace/type; the unique (session, type, valueHash)
    // key is the composite `_id`, so it needs no separate index.
    await ensureIndex(this.database, this.committed, { namespace_id: 1, type: 1 }, { name: 'namespace_type_scan' });
    // The buffer is read by session in sequence order, a page at a time, and counted by session —
    // so it wants the same shape as the audit log's. Unindexed, each page of a flush scans every
    // session's buffered rows, which is the cost that made an interrupted flush compound.
    await ensureIndex(this.database, this.buffer, { session_id: 1, sequence: 1 }, { name: 'session_trail' });
    if (this.retentionSeconds !== null) {
      // last_retrieved is a native BSON date, so the TTL reaper can act on it directly.
      await ensureIndex(
        this.database,
        this.committed,
        { last_retrieved: 1 },
        { name: 'retention_reaper', expireAfterSeconds: this.retentionSeconds },
      );
    }
    this.indexesReady = true;
  }

  public async archive(entry: ArchivedDataPoint): Promise<void> {
    await this.archiveMany([entry]);
  }

  public async archiveMany(entries: readonly ArchivedDataPoint[]): Promise<void> {
    const first = entries[0];
    if (first === undefined) {
      return;
    }
    await this.ensureIndexes();
    // A batch is one writer's entries for one session, hence one namespace — resolve its cipher once.
    const cipher = await this.cipherFor(first.namespaceId);
    for (const entry of entries) {
      if (entry.isPii && cipher instanceof NullCipher) {
        // Fail closed, and BEFORE any sequence allocation: never persist PII at rest without a real
        // cipher (a wiring mistake must not leak), and a refused batch must not burn sequence
        // numbers either.
        throw new UnprotectedPiiError(
          `refusing to archive PII '${entry.type}' for ${entry.sessionId} without a cipher`,
        );
      }
    }
    // Fence the epoch and allocate the whole sequence range atomically (mirrors the audit sink):
    // the conditional upsert matches only when max_epoch is not newer, so a stale writer fails the
    // predicate and the upsert insert then collides on _id -> duplicate key — the entire batch is
    // rejected before anything is buffered. A batch is one writer's entries for one session, so the
    // first entry carries the epoch/session for the whole range, numbered last-len+1..last.
    const currentEpoch = Number(first.epoch);
    const notSuperseded = [{ max_epoch: { $exists: false } }, { max_epoch: { $lte: currentEpoch } }];
    const stale = `epoch ${first.epoch} is stale for session ${first.sessionId}`;
    let meta: Record<string, unknown> | null;
    try {
      meta = await this.meta.findOneAndUpdate(
        { _id: first.sessionId, $or: notSuperseded },
        { $inc: { sequence: entries.length }, $max: { max_epoch: currentEpoch } },
        { upsert: true, returnDocument: 'after' },
      );
    } catch (error) {
      if (!isDuplicateKeyError(error)) {
        throw error;
      }
      throw new StaleEpochError(stale, { cause: error });
    }
    if (meta === null) {
      throw new StaleEpochError(stale);
    }
    const last = Number(meta.sequence);
    const documents = entries.map((entry, index) => ({
      session_id: entry.sessionId,
      sequence: last - entries.length + 1 + index,
      // Derive the key from the plaintext value, then seal the value — order matters.
      entry: seal(entry, cipher)
        .copyWith({ valueHash: valueHash(entry, cipher) })
        .toWire(),
    }));
    await this.buffer.insertMany(documents, { ordered: true });
  }

  /**
   * Drain this session's buffered rows into the committed collection, a page at a time.
   *
   * Paged, committed and cleared per batch so the cost of a flush is bounded rather than
   * proportional to the session's whole trail: reading it all at once grows memory with the trail,
   * and clearing only after every row is committed means an interrupted pass leaves work committed
   * but nothing cleared, so the next attempt re-reads the identical buffer and fails in the same
   * place — each pass making the following one slower. This shares the bounded tail step's timeout
   * with the rest of the tail, so leaving durable progress behind is what lets a resume finish.
   *
   * The page advances on `sequence` rather than on the delete having landed, so a pass that commits
   * and then dies before clearing still terminates; re-committing is harmless because the `_id` is
   * derived from the value and the merge is `$setOnInsert` plus `$max`.
   *
   * A page is one round trip rather than one per row, which is the whole cost of a flush against a
   * remote primary.
   *
   * Ordered, for equivalence rather than safety: the buffer does not deduplicate, so a page can
   * carry two upserts for one `_id`, and applying them in sequence is exactly what a row-at-a-time
   * loop would do. Unordered was measured against a real server and does NOT collide on that pair,
   * and `$max` makes the merged result order-independent in any case — so ordered costs nothing
   * here and removes a question rather than answering one. Revisit it if a page ever grows large
   * enough for the server's parallelism to matter.
   */
  public async flush(sessionId: SessionId): Promise<number> {
    await this.ensureIndexes();
    let flushed = 0;
    let afterSequence = -1;
    for (;;) {
      const batch = await this.buffer
        .find({ session_id: sessionId, sequence: { $gt: afterSequence } })
        .sort('sequence', 1)
        .limit(PAGE_SIZE)
        .toArray();
      const last = batch[batch.length - 1];
      if (last === undefined) {
        return flushed;
      }
      const operations: AnyBulkWriteOperation<StringIdDocument>[] = batch.map((document) => {
        // Round-trip the buffered JSON back to the model so the committed doc carries native BSON
        // dates: `$max` then orders chronologically regardless of string formatting, and the TTL
        // reaper can act on last_retrieved. The sealed value/keyed valueHash ride along unchanged.
        const entry = parseArchivedDataPoint(document.entry);
        return {
          updateOne: {
            filter: { _id: `${sessionId}${KEY_SEPARATOR}${entry.type}${KEY_SEPARATOR}${entry.valueHash}` },
            update: {
              $setOnInsert: {
                session_id: entry.sessionId,
                namespace_id: entry.namespaceId,
                type: entry.type,
                value_hash: entry.valueHash,
                value: entry.value,
                retrieved_by: entry.retrievedBy,
                first_retrieved: entry.firstRetrieved,
                is_pii: entry.isPii,
                schema_version: entry.schemaVersion,
              },
              $max: { last_retrieved: entry.lastRetrieved, epoch: Number(entry.epoch) },
            },
            upsert: true,
          },
        };
      });
      await this.committed.bulkWrite(operations, { ordered: true });
      await this.buffer.deleteMany({ _id: { $in: batch.map((document) => document._id) } });
      afterSequence = Number(last.sequence);
      flushed += batch.length;
    }
  }

  /**
   * Committed rows folded together with any still-buffered ones, in first-observation order.
   *
   * Folded rather than concatenated: the buffer keeps per-observation granularity, so appending it
   * raw would surface duplicates the keyed-upsert collapses. Committed rows fold in first and the
   * buffer follows in sequence order — the order a flush would apply them — so an in-flight session
   * reads as what a flush-then-read would return, without the read writing anything.
   *
   * Both sides are paged, and neither is materialized whole. A session's committed row count is the
   * number of distinct VALUES it observed — `value_hash` is a MAC of the value, not of the leaf
   * type — so a chatty session (a per-frame probe, a page-view stream) reaches tens of thousands of
   * rows, and the buffer holds one row per observation on top of that until a flush drains it.
   * Folding a page at a time keeps the peak at one page plus the answer itself. The answer is the
   * cap that remains: one entry per identity is what an "everything for this session" contract
   * owes, and `replaySession` re-runs a flow over exactly these rows, so a limit here would
   * silently replay a different session.
   */
  public async read(sessionId: SessionId): Promise<readonly ArchivedDataPoint[]> {
    await this.ensureIndexes();
    const folded = new Map<string, ArchivedDataPoint>();
    // Ranged over the composite `_id` rather than filtered on `session_id`, which has no index on
    // the committed collection — an equality there scans every namespace's and every session's rows
    // inside the retention window. The `_id` is `session\x00type\x00value_hash` and a session id
    // holds no NUL, so `[sid\x00, sid\x01)` is exactly this session's keys and rides the `_id`
    // index. Paging on the same key needs no second sort.
    const upperBound = `${sessionId}\u0001`;
    let afterId = `${sessionId}${KEY_SEPARATOR}`;
    for (;;) {
      const page = await this.committed
        .find({ _id: { $gt: afterId, $lt: upperBound } })
        .sort('_id', 1)
        .limit(PAGE_SIZE)
        .toArray();
      for (const row of page) {
        // Python restores the timezone pymongo strips off the stored dates here; the Node driver
        // hands back `Date`s, which the model reads as readily as the buffer's ISO strings.
        foldIn(folded, parseArchivedDataPoint(row));
      }
      const last = page[page.length - 1];
      if (page.length < PAGE_SIZE || last === undefined) {
        break;
      }
      afterId = String(last._id);
    }
    // The buffer holds each observation as a JSON dump under `entry`, so it validates directly.
    // Paged on `sequence`, the way `flush` drains it, so the two agree on the order they apply.
    let afterSequence = -1;
    for (;;) {
      const page = await this.buffer
        .find({ session_id: sessionId, sequence: { $gt: afterSequence } })
        .sort('sequence', 1)
        .limit(PAGE_SIZE)
        .toArray();
      for (const row of page) {
        foldIn(folded, parseArchivedDataPoint(row.entry));
      }
      const last = page[page.length - 1];
      if (page.length < PAGE_SIZE || last === undefined) {
        break;
      }
      afterSequence = Number(last.sequence);
    }
    // Ties broken on the identity key, not on the order the pages happened to arrive in: rows
    // sharing a firstRetrieved sort one way when they are read from the buffer and another once
    // they are committed, which would make a read before a flush differ from a read after one for
    // no reason a caller could see.
    const entries = [...folded.values()].sort(byFirstObservation);
    const first = entries[0];
    if (first === undefined) {
      return [];
    }
    // All rows for a session share its namespace, so resolve the unsealing cipher once.
    const namespaceId = first.namespaceId;
    const cipher = await this.cipherFor(namespaceId);
    // PII is only ever persisted under a real cipher (the write fails closed), so a passthrough
    // cipher against sealed PII rows means the namespace key is unavailable — surface it clearly
    // instead of letting unseal recover garbage (or crash opaquely) with no context.
    if (cipher instanceof NullCipher && entries.some((entry) => entry.isPii)) {
      getLogger().error('per-namespace cipher unavailable; cannot unseal sealed PII on read', {
        logger_name: LOGGER_NAME,
        namespace_id: namespaceId,
      });
      throw new PiiKeyUnavailableError(`cannot unseal PII for session ${sessionId}: namespace key unavailable`);
    }
    return entries.map((entry) => unseal(entry, cipher));
  }

  public async bufferedCount(sessionId: SessionId): Promise<number> {
    return this.buffer.countDocuments({ session_id: sessionId });
  }
}

/** Sort key: first observation, then the identity that breaks a tie deterministically. */
const byFirstObservation = (left: ArchivedDataPoint, right: ArchivedDataPoint): number => {
  const byInstant = left.firstRetrieved.getTime() - right.firstRetrieved.getTime();
  if (byInstant !== 0) {
    return byInstant;
  }
  if (left.type !== right.type) {
    return left.type < right.type ? -1 : 1;
  }
  if (left.valueHash === right.valueHash) {
    return 0;
  }
  return left.valueHash < right.valueHash ? -1 : 1;
};
