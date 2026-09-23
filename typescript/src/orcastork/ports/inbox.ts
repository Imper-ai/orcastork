/**
 * The `Inbox` port — durable, ordered, at-least-once session ingestion.
 *
 * A stateless front door appends user-action DataPoints; the orchestrator claims them, applies them
 * to the store, then acks. Crash-before-ack leaves the entry reclaimable, so delivery is
 * at-least-once (the store's keyed-merge makes redelivery safe). Append also publishes a push
 * wakeup that `waitForEntry` blocks on, but the wakeup only bounds latency — delivery correctness
 * never depends on it.
 *
 * Delivery tolerates malformed wire bytes: an entry whose payload cannot be decoded is presented as
 * a {@link PoisonInboxEntry} instead of raising, so one malformed message never blocks its batch
 * (or crash-loops a resuming session). Semantic deserialization failures (an unknown DataPoint
 * type, a validation error) propagate instead — those must fail fast, and redelivery on resume lets
 * a newer deployment parse them. The consumer disposes of poison entries via `quarantine`, which
 * removes them from delivery while keeping them durably inspectable through `quarantined`.
 *
 * @module
 */

import type { AnyDataPoint } from '../datapoints/index.js';
import type { Epoch, SessionId } from '../ids.js';
import type { DeliveredInboxEntry, PoisonInboxEntry, QuarantinedEntry } from './change_set.js';

/** How much of the backlog one `consume` claims. */
export interface ConsumeOptions {
  /** At most this many entries; `null`/omitted claims everything pending. */
  readonly maxEntries?: number | null;
}

/** Why an entry is leaving delivery, and under which epoch. */
export interface QuarantineOptions {
  /** The parse failure, or the apply error that hit the delivery cap. */
  readonly reason: string;

  readonly epoch: Epoch;
}

/** The session's durable front door. */
export interface Inbox {
  /** Append a DataPoint to the session inbox (+publish wakeup); returns the entry id. */
  append(sessionId: SessionId, dataPoint: AnyDataPoint): Promise<string>;

  /**
   * Claim the not-yet-delivered entries, in append order (marks them in-flight).
   *
   * An entry whose wire payload cannot be decoded is delivered as a {@link PoisonInboxEntry},
   * never raised — the other entries in the batch still flow.
   */
  consume(sessionId: SessionId, options?: ConsumeOptions): Promise<readonly DeliveredInboxEntry[]>;

  /**
   * Re-present in-flight entries that were never acked (crash recovery / redelivery).
   *
   * Undecodable entries surface as {@link PoisonInboxEntry} here too (same tolerance as
   * `consume`), so a poison entry left by a crashed predecessor cannot crash the resume.
   */
  reclaim(sessionId: SessionId): Promise<readonly DeliveredInboxEntry[]>;

  /**
   * Acknowledge an entry after its durable apply (epoch-guarded; no-op if unknown).
   *
   * @throws StaleEpochError when the session has already accepted a higher epoch.
   */
  ack(sessionId: SessionId, entryId: string, options: { readonly epoch: Epoch }): Promise<void>;

  /**
   * Remove an entry from pending delivery AND record it durably for operator inspection.
   *
   * Epoch-guarded like `ack`; an unknown / already-disposed entry is a safe no-op (so a repeated
   * quarantine never double-records).
   *
   * @throws StaleEpochError when the session has already accepted a higher epoch.
   */
  quarantine(sessionId: SessionId, entryId: string, options: QuarantineOptions): Promise<void>;

  /** The session's quarantined entries, in quarantine order (a read — ops and tests). */
  quarantined(sessionId: SessionId): Promise<readonly QuarantinedEntry[]>;

  /** Number of entries not yet acked (claimed or unclaimed). */
  pendingCount(sessionId: SessionId): Promise<number>;

  /**
   * Block until an entry may be available — a push nudge, not delivery.
   *
   * Spurious wakeups are allowed; missed wakeups are not: an append after the call begins MUST
   * wake it, and entries already pending when it is called return it immediately (implementations
   * subscribe before checking, closing the race). Returning claims nothing — callers still drain
   * via consume/reclaim/ack.
   *
   * `signal` is how a caller that stopped waiting gives the wait up, the way Python cancels the
   * task it raced. An implementation MUST release everything the wait holds when it fires — a
   * pub/sub connection above all — and then return: an abandoned promise never settles, so a
   * teardown that only runs when the wait ends would leak one connection per parked session.
   * Returning on an abort is a spurious wakeup, which this contract already allows.
   */
  waitForEntry(sessionId: SessionId, signal?: AbortSignal): Promise<void>;
}
