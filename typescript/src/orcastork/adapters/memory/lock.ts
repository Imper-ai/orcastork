/**
 * In-memory `SessionLock` — a TTL lease (liveness) plus a monotonic epoch (correctness).
 *
 * The epoch counter only ever increases, so every grant (including taking over an expired lease)
 * mints a strictly higher epoch. Liveness is decided against the injected clock, so TTL expiry is
 * deterministic in tests.
 *
 * @module
 */

import type { Clock } from '../../clock.js';
import { LockHeldError, StaleEpochError } from '../../exceptions.js';
import type { Epoch, SessionId } from '../../ids.js';
import { Epoch as toEpoch } from '../../ids.js';
import type { SessionLock } from '../../ports/session_lock.js';

/** How long a lease stays live without a renew (Python's `30.0` seconds). */
export const DEFAULT_TTL_MS = 30_000;

/** How an {@link InMemorySessionLock} is configured. */
export interface InMemorySessionLockOptions {
  /** Lease lifetime; it must exceed the longest single unrenewed await in the completion tail. */
  readonly ttlMs?: number;
}

/** A granted lease: the epoch it minted and the monotonic instant it lapses at. */
interface Lease {
  readonly epoch: number;
  expiresAt: number;
}

/** One session's ownership state: the epoch mint, the live lease and the completion flag. */
interface LockState {
  epochCounter: number;
  lease: Lease | null;
  complete: boolean;
}

/** Ownership of a session, held in process memory and timed on the injected clock. */
export class InMemorySessionLock implements SessionLock {
  private readonly clock: Clock;
  private readonly ttlMs: number;
  private readonly sessions = new Map<SessionId, LockState>();

  public constructor(clock: Clock, options: InMemorySessionLockOptions = {}) {
    this.clock = clock;
    this.ttlMs = options.ttlMs ?? DEFAULT_TTL_MS;
  }

  private stateOf(sessionId: SessionId): LockState {
    const known = this.sessions.get(sessionId);
    if (known !== undefined) {
      return known;
    }
    const created: LockState = { epochCounter: 0, lease: null, complete: false };
    this.sessions.set(sessionId, created);
    return created;
  }

  private isLive(state: LockState): boolean {
    return state.lease !== null && this.clock.monotonic() < state.lease.expiresAt;
  }

  public async acquire(sessionId: SessionId): Promise<Epoch> {
    const state = this.stateOf(sessionId);
    if (this.isLive(state)) {
      throw new LockHeldError(`session ${sessionId} is already locked`);
    }
    state.epochCounter += 1;
    state.lease = { epoch: state.epochCounter, expiresAt: this.clock.monotonic() + this.ttlMs };
    return toEpoch(state.epochCounter);
  }

  public async renew(sessionId: SessionId, options: { readonly epoch: Epoch }): Promise<void> {
    const state = this.stateOf(sessionId);
    if (state.lease === null || state.lease.epoch !== options.epoch || !this.isLive(state)) {
      throw new StaleEpochError(`epoch ${options.epoch} does not hold the lock for session ${sessionId}`);
    }
    state.lease.expiresAt = this.clock.monotonic() + this.ttlMs;
  }

  public async release(sessionId: SessionId, options: { readonly epoch: Epoch }): Promise<void> {
    const state = this.stateOf(sessionId);
    if (state.lease !== null && state.lease.epoch === options.epoch) {
      state.lease = null;
    }
  }

  public async currentEpoch(sessionId: SessionId): Promise<Epoch> {
    return toEpoch(this.stateOf(sessionId).epochCounter);
  }

  public async isHeld(sessionId: SessionId): Promise<boolean> {
    return this.isLive(this.stateOf(sessionId));
  }

  public async markComplete(sessionId: SessionId, options: { readonly epoch: Epoch }): Promise<void> {
    // The flag and the epoch counter live in one state object mutated under the single event loop,
    // so this compare-and-set is atomic: a writer below the current epoch (a fenced predecessor
    // superseded by a takeover) is rejected and cannot finalize the session.
    const state = this.stateOf(sessionId);
    if (options.epoch < state.epochCounter) {
      throw new StaleEpochError(`epoch ${options.epoch} is stale for session ${sessionId}`);
    }
    state.complete = true;
  }

  public async isComplete(sessionId: SessionId): Promise<boolean> {
    return this.stateOf(sessionId).complete;
  }

  public async clearComplete(sessionId: SessionId): Promise<void> {
    this.stateOf(sessionId).complete = false;
  }
}
