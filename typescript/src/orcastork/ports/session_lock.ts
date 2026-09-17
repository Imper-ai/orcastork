/**
 * The `SessionLock` port — liveness lock + the fencing epoch (correctness) + completion.
 *
 * Acquiring ownership grants a lock (mutual exclusion, TTL-bounded) **and** mints an epoch strictly
 * greater than any prior epoch for the session. The lock is only a liveness hint; the epoch is the
 * correctness primitive that every write path validates. The lock expires after its TTL when the
 * holder dies, letting a successor acquire (with a higher epoch). The session-completion flag is
 * co-located with the epoch counter, so marking a session done is a single compare-and-set against
 * the live epoch — a fenced predecessor can never finalize.
 *
 * @module
 */

import type { Epoch, SessionId } from '../ids.js';

/** Ownership of a session: the lease, the fencing epoch it mints, and the completion flag. */
export interface SessionLock {
  /**
   * Grant the lock and mint a strictly-increasing epoch.
   *
   * @throws LockHeldError when a live lease is already held (mutual exclusion).
   */
  acquire(sessionId: SessionId): Promise<Epoch>;

  /** Extend the lease TTL while held (epoch must be current). */
  renew(sessionId: SessionId, options: { readonly epoch: Epoch }): Promise<void>;

  /** Release the lock for reuse (epoch must be current). */
  release(sessionId: SessionId, options: { readonly epoch: Epoch }): Promise<void>;

  /** The highest epoch minted for the session (`0` if never acquired). */
  currentEpoch(sessionId: SessionId): Promise<Epoch>;

  /** Whether a live (un-expired) lease currently exists. */
  isHeld(sessionId: SessionId): Promise<boolean>;

  /**
   * Record that the session finished, gated atomically on the fencing epoch.
   *
   * The completion flag is co-located with the epoch counter, so the mark is a single
   * compare-and-set: a writer whose `epoch` is below the current one (a fenced predecessor
   * superseded by a takeover) is rejected. Idempotent for the current holder. A supervisor — even
   * on another pod — reads {@link SessionLock.isComplete} to skip a finished session.
   *
   * @throws StaleEpochError when the epoch is below the session's current one.
   */
  markComplete(sessionId: SessionId, options: { readonly epoch: Epoch }): Promise<void>;

  /** Whether the session has been marked complete (a supervisor must not resume it). */
  isComplete(sessionId: SessionId): Promise<boolean>;

  /**
   * Clear the completion flag so a completed session can be re-opened.
   *
   * Called by the manager before re-opening a session for late non-ephemeral data: the flag is
   * removed so the next acquire succeeds and the re-opening orchestrator can mark complete again
   * once it has re-aggregated. This does NOT reset or alter the epoch counter — the re-open still
   * acquires a fresh, strictly higher epoch so the re-aggregation write is fenced against any
   * stale predecessor.
   */
  clearComplete(sessionId: SessionId): Promise<void>;
}
