/**
 * The `DataPointArchive` port — a second durable write path for raw DataPoints.
 *
 * Distinct from `DurableStore` (curated aggregate outputs, written only by aggregators) and
 * `AuditSink` (events, not values), but built on the **same write-behind machinery**: `archive`
 * appends to a durable, epoch-guarded buffer off the hot path; `flush` folds the buffer into the
 * committed store in a batch via **keyed-upsert** on `(session, type, valueHash)` — re-observing an
 * identity bumps `lastRetrieved` and never duplicates. `read` folds the buffer over the committed
 * rows under that same rule, so what it returns does not depend on whether a flush has run yet. The
 * archive lags the live store (eventually consistent) and is never read on a decision path.
 *
 * @module
 */

import type { ArchivedDataPoint } from '../archive/index.js';
import type { SessionId } from '../ids.js';

/** The live, keyed-upsert record of every non-ephemeral DataPoint a session saw. */
export interface DataPointArchive {
  /**
   * Append one raw DataPoint to the durable buffer.
   *
   * Epoch-guarded: an entry whose epoch is below the session's highest accepted epoch is rejected
   * (a fenced writer never reaches the archive).
   *
   * @throws StaleEpochError when the entry's epoch is below the session's highest accepted epoch.
   */
  archive(entry: ArchivedDataPoint): Promise<void>;

  /**
   * Append a batch of raw DataPoints to the durable buffer, preserving order.
   *
   * Observably identical to archiving each entry in sequence — same per-observation buffer
   * granularity, same keyed-upsert fold on flush — but an adapter may allocate the whole batch in
   * one round-trip. The batch is one writer's entries for one session: a stale epoch (or any
   * per-entry refusal, e.g. unprotected PII) rejects the entire batch atomically and buffers
   * nothing.
   *
   * @throws StaleEpochError when the batch's epoch is below the session's highest accepted epoch.
   */
  archiveMany(entries: readonly ArchivedDataPoint[]): Promise<void>;

  /** Fold buffered entries into the committed store (keyed-upsert); return the count flushed. */
  flush(sessionId: SessionId): Promise<number>;

  /**
   * Every archived DataPoint for the session, deduped — still-buffered rows included.
   *
   * Committed rows and any not-yet-flushed buffered rows are folded together under the same
   * keyed-upsert rule `flush` applies, so a read is buffer-transparent: it returns what a read
   * after a flush would return, and a session whose entries are still buffered — in flight, or
   * dead before its flush ran — is readable rather than invisible. Reading never flushes; the
   * buffer is left intact.
   */
  read(sessionId: SessionId): Promise<readonly ArchivedDataPoint[]>;

  /** Entries appended but not yet flushed. */
  bufferedCount(sessionId: SessionId): Promise<number>;
}
