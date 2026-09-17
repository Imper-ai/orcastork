/**
 * Mongo `AuditSink` — a durable, epoch-fenced, append-only event log.
 *
 * `appendMany` allocates one contiguous sequence range under a single meta CAS (rejecting a
 * stale-epoch writer for the whole batch) and writes the epoch-stamped, sequence-numbered documents
 * straight to the `orcastork-audit-log` collection in one ordered `insertMany`; `append` is the
 * batch of one. An entry is durable and visible to `replay` as soon as its append returns, so the
 * trail of a session that crashes — or one still running — is readable without any further step.
 * The audit is independent of the live store.
 *
 * @module
 */

import type { Collection, Db } from 'mongodb';
import type { AuditLogEntry } from '../../audit/index.js';
import { auditLogEntryToWire, parseAuditLogEntry } from '../../audit/index.js';
import { StaleEpochError } from '../../exceptions.js';
import type { SessionId } from '../../ids.js';
import type { AuditSink } from '../../ports/audit_sink.js';
import type { StringIdDocument } from './indexes.js';
import { ensureIndex, isDuplicateKeyError } from './indexes.js';

/** The collection every session's trail is written to. */
export const AUDIT_LOG_COLLECTION = 'orcastork-audit-log';

/** The collection holding one sequence/epoch allocator document per session. */
const AUDIT_META_COLLECTION = 'audit_meta';

/**
 * The byte the `(session, sequence)` `_id` joins its parts with — a byte no session id can hold.
 */
const KEY_SEPARATOR = '\u0000';

/**
 * Rows fetched per round trip when replaying a trail.
 *
 * A session's audit is unbounded by construction — it grows with everything the flow did, and a
 * chatty one (a per-frame probe, a page-view stream) runs to tens of thousands of entries — so the
 * read pages rather than opening one cursor over all of it.
 *
 * Exported because a module constant cannot be monkeypatched the way the Python suite narrows its
 * `_PAGE_SIZE`: a test that is about the paging sizes its fixture from this instead.
 */
export const PAGE_SIZE = 500;

/** The durable event log, in Mongo. */
export class MongoAuditSink implements AuditSink {
  private readonly database: Db;

  private readonly committed: Collection<StringIdDocument>;

  private readonly meta: Collection<StringIdDocument>;

  private indexesReady = false;

  public constructor(database: Db) {
    this.database = database;
    this.committed = database.collection<StringIdDocument>(AUDIT_LOG_COLLECTION);
    this.meta = database.collection<StringIdDocument>(AUDIT_META_COLLECTION);
  }

  private async ensureIndexes(): Promise<void> {
    if (this.indexesReady) {
      return;
    }
    // Every read of this collection is one session's trail in sequence order — `replay` here, and
    // whatever timeline view the embedding application builds over the same collection. Without
    // this the trail is a collection scan across every session's rows plus an in-memory sort,
    // which grows with the whole log rather than with the session being read. `audit_meta` needs
    // nothing: it is keyed by session id, so `_id` already serves it.
    await ensureIndex(this.database, this.committed, { session_id: 1, sequence: 1 }, { name: 'session_trail' });
    this.indexesReady = true;
  }

  public async append(entry: AuditLogEntry): Promise<void> {
    await this.appendMany([entry]);
  }

  public async appendMany(entries: readonly AuditLogEntry[]): Promise<void> {
    const first = entries[0];
    if (first === undefined) {
      return;
    }
    await this.ensureIndexes();
    // Fence the epoch and allocate the whole sequence RANGE in one atomic step: the conditional
    // upsert only matches when the session's max_epoch is not newer, so concurrent appends can
    // neither both pass a stale check nor collide on sequence numbers. A stale writer fails the
    // predicate, and the upsert insert then collides on _id -> duplicate key — the entire batch is
    // rejected before anything is written. A batch is one writer's events for one session (the sole
    // mutator batches its own appends), so the first entry carries the epoch/session for the whole
    // range; entries take the range's numbers in input order.
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
    const firstSequence = Number(meta.sequence) - entries.length + 1;
    // The `_id` is derived from the session and the sequence, and the CAS above hands out each
    // sequence number exactly once, so a duplicate key here means two writers claimed the same
    // number — a fault worth surfacing, not a re-presented row to absorb. The session id comes
    // from the same entry the CAS fenced on: a sequence number only means anything within the
    // session whose meta allocated it.
    await this.committed.insertMany(
      entries.map((entry, index) => ({
        _id: `${first.sessionId}${KEY_SEPARATOR}${firstSequence + index}`,
        session_id: first.sessionId,
        sequence: firstSequence + index,
        entry: MongoAuditSink.entryDocument(entry),
      })),
      { ordered: true },
    );
  }

  /**
   * JSON-safe primitives for Mongo, EXCEPT `timestamp`: persisted as a BSON Date (not an ISO
   * string) so the audit log is queryable by time in Mongo (range filters, aggregation). It is the
   * only instant on the entry.
   */
  private static entryDocument(entry: AuditLogEntry): Record<string, unknown> {
    return { ...auditLogEntryToWire(entry), timestamp: entry.timestamp };
  }

  /**
   * Every entry this session appended, in the order the sequence numbers were allocated.
   *
   * Paged rather than drained in one cursor. A trail is not bounded by anything the flow declares:
   * it grows with what the session actually did, and the chatty shapes are the ones that most need
   * replaying. Paging keeps the peak at one page rather than the whole trail; the answer is
   * necessarily the whole of it, since replaying a subset would reconstruct a different session.
   *
   * The page walks `sequence` forward, which the `(session_id, sequence)` index serves directly —
   * an equality on the session then a range on the sequence — so no page costs a sort.
   *
   * Python restores the timezone pymongo strips off the stored `timestamp`; the Node driver hands
   * back a `Date`, and the entry model reads one as readily as an ISO string.
   */
  public async replay(sessionId: SessionId): Promise<readonly AuditLogEntry[]> {
    const entries: AuditLogEntry[] = [];
    let afterSequence = -1;
    for (;;) {
      const page = await this.committed
        .find({ session_id: sessionId, sequence: { $gt: afterSequence } })
        .sort('sequence', 1)
        .limit(PAGE_SIZE)
        .toArray();
      for (const row of page) {
        entries.push(parseAuditLogEntry(row.entry));
      }
      const last = page[page.length - 1];
      if (page.length < PAGE_SIZE || last === undefined) {
        return entries;
      }
      afterSequence = Number(last.sequence);
    }
  }
}
