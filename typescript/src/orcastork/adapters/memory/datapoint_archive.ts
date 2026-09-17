/**
 * In-memory `DataPointArchive` — durable buffer + batched keyed-upsert, epoch-fenced.
 *
 * `archive` appends to a durable buffer (rejecting a stale-epoch writer's entry). `flush` folds the
 * buffer into a committed map keyed by `(type, valueHash)`: first observation inserts,
 * re-observation bumps `lastRetrieved` and keeps `firstRetrieved` — never a duplicate, so
 * redelivery/replay is idempotent. PII values are sealed via the injected cipher (encrypted at rest
 * in the buffer and the committed map) and unsealed on read, which folds the buffer over the
 * committed map so a session in flight is readable. An optional clock-driven retention TTL expires
 * committed entries lazily on read.
 *
 * @module
 */

import type { ArchivedDataPoint, ValueCipher } from '../../archive/index.js';
import { NullCipher } from '../../archive/index.js';
import { seal, unseal, valueHash } from '../../archive/sealing.js';
import type { Clock } from '../../clock.js';
import { StaleEpochError } from '../../exceptions.js';
import type { SessionId } from '../../ids.js';
import type { DataPointArchive } from '../../ports/datapoint_archive.js';

/**
 * The byte the keyed-upsert key joins its parts with.
 *
 * The port of Python's `(type, value_hash)` tuple key: neither part can contain it, so two
 * different identities never collide into one string.
 */
const KEY_SEPARATOR = '\u0000';

/** One session's archive: what is buffered, what is committed, and the highest epoch that wrote. */
interface ArchiveState {
  buffer: ArchivedDataPoint[];
  readonly committed: Map<string, ArchivedDataPoint>;
  maxEpoch: number;
}

/** How an {@link InMemoryDataPointArchive} is wired. */
export interface InMemoryDataPointArchiveOptions {
  /** Seals PII values at rest and derives their keys; the passthrough `NullCipher` by default. */
  readonly cipher?: ValueCipher;

  /** Required with `retentionMs` — expiry is decided against the injected clock, never the wall clock. */
  readonly clock?: Clock;

  /** How long a committed entry stays readable after `lastRetrieved`; unbounded when omitted. */
  readonly retentionMs?: number;
}

/**
 * Apply one sealed observation to a keyed-upsert map: first inserts, re-observation advances.
 *
 * The single implementation of the fold, so `flush` (which folds into the committed map) and `read`
 * (which folds the buffer on top of a copy of it) cannot disagree about what a flush would have
 * produced.
 */
export const foldIn = (folded: Map<string, ArchivedDataPoint>, sealed: ArchivedDataPoint): void => {
  const key = `${sealed.type}${KEY_SEPARATOR}${sealed.valueHash}`;
  const existing = folded.get(key);
  if (existing === undefined) {
    folded.set(key, sealed);
    return;
  }
  // Keyed-upsert: keep the original firstRetrieved, advance lastRetrieved and epoch. Advancing the
  // epoch is what makes the stored row say which epoch last SAW this datapoint, rather than which
  // epoch first recorded it — the Mongo adapter's `$max` decides the same way, and a row whose
  // epoch is frozen at its first sighting reads as older than the session that actually produced it.
  folded.set(
    key,
    existing.copyWith({
      lastRetrieved: new Date(Math.max(existing.lastRetrieved.getTime(), sealed.lastRetrieved.getTime())),
      epoch: existing.epoch > sealed.epoch ? existing.epoch : sealed.epoch,
    }),
  );
};

/** The second durable write path, held in process memory — the test and local-run substrate. */
export class InMemoryDataPointArchive implements DataPointArchive {
  private readonly cipher: ValueCipher;
  private readonly clock: Clock | null;
  private readonly retentionMs: number | null;
  private readonly sessions = new Map<SessionId, ArchiveState>();

  public constructor(options: InMemoryDataPointArchiveOptions = {}) {
    this.cipher = options.cipher ?? new NullCipher();
    this.clock = options.clock ?? null;
    this.retentionMs = options.retentionMs ?? null;
  }

  private stateOf(sessionId: SessionId): ArchiveState {
    const known = this.sessions.get(sessionId);
    if (known !== undefined) {
      return known;
    }
    const created: ArchiveState = { buffer: [], committed: new Map<string, ArchivedDataPoint>(), maxEpoch: 0 };
    this.sessions.set(sessionId, created);
    return created;
  }

  public async archive(entry: ArchivedDataPoint): Promise<void> {
    const state = this.stateOf(entry.sessionId);
    if (entry.epoch < state.maxEpoch) {
      throw new StaleEpochError(`epoch ${entry.epoch} is stale (current ${state.maxEpoch})`);
    }
    state.maxEpoch = entry.epoch;
    // Derive the key from the plaintext value, then seal the value — order matters.
    const keyed = seal(entry, this.cipher).copyWith({ valueHash: valueHash(entry, this.cipher) });
    state.buffer.push(keyed);
  }

  public async archiveMany(entries: readonly ArchivedDataPoint[]): Promise<void> {
    // In memory a batch costs the same as N archives, and a (contract-homogeneous) stale batch is
    // rejected by its first entry before anything lands — atomic either way.
    for (const entry of entries) {
      await this.archive(entry);
    }
  }

  public async flush(sessionId: SessionId): Promise<number> {
    const state = this.stateOf(sessionId);
    const flushed = state.buffer.length;
    for (const sealed of state.buffer) {
      foldIn(state.committed, sealed);
    }
    state.buffer = [];
    return flushed;
  }

  /**
   * Committed entries folded together with any still-buffered ones.
   *
   * Folded rather than concatenated: the buffer keeps per-observation granularity, so appending it
   * raw would surface duplicates the keyed-upsert collapses. Folding on a copy leaves the buffer
   * intact — a read is not a flush — while giving the same answer a flush-then-read would.
   */
  public async read(sessionId: SessionId): Promise<readonly ArchivedDataPoint[]> {
    const state = this.stateOf(sessionId);
    const folded = new Map(state.committed);
    for (const sealed of state.buffer) {
      foldIn(folded, sealed);
    }
    return [...folded.values()].filter((entry) => this.live(entry)).map((entry) => unseal(entry, this.cipher));
  }

  public async bufferedCount(sessionId: SessionId): Promise<number> {
    return this.stateOf(sessionId).buffer.length;
  }

  private live(entry: ArchivedDataPoint): boolean {
    if (this.retentionMs === null || this.clock === null) {
      return true;
    }
    return this.clock.now().getTime() - entry.lastRetrieved.getTime() <= this.retentionMs;
  }
}
