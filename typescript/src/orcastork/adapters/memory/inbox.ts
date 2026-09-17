/**
 * In-memory `Inbox` — ordered, at-least-once, with explicit claim/ack/reclaim.
 *
 * `consume` claims not-yet-delivered entries (in append order); `reclaim` re-presents
 * claimed-but-unacked entries (crash recovery), bumping their delivery count so poison messages
 * surface; `ack` (epoch-guarded) removes a claimed entry, and is a safe no-op on an unclaimed one
 * (Redis `XACK` semantics). Append sets a per-session arrival event that `waitForEntry` blocks on —
 * a push wakeup, no polling — but delivery never depends on it.
 *
 * Messages are held in their serialized wire form (what a stateless front door would put on a real
 * transport), so delivery exercises the same tolerant-reader path as the Redis adapter: a payload
 * whose bytes do not decode as JSON is surfaced as a {@link PoisonInboxEntry} instead of raising,
 * and `quarantine` removes it from delivery while keeping it inspectable. Semantic deserialization
 * failures (an unknown DataPoint type, a validation error) propagate instead.
 *
 * @module
 */

import { randomUUID } from 'node:crypto';
import type { AnyDataPoint } from '../../datapoints/index.js';
import { parseDataPoint } from '../../datapoints/index.js';
import { StaleEpochError } from '../../exceptions.js';
import type { Epoch, SessionId } from '../../ids.js';
import { Deferred } from '../../internal/deferred.js';
import type { DeliveredInboxEntry } from '../../ports/change_set.js';
import { InboxEntry, PoisonInboxEntry, QuarantinedEntry } from '../../ports/change_set.js';
import type { ConsumeOptions, Inbox, QuarantineOptions } from '../../ports/inbox.js';

/**
 * The port of `asyncio.Event`: set on append, cleared by a waiter just before it waits.
 *
 * Several waiters may be parked at once (each run of a session waits on its own), so a set must
 * release all of them — a single-slot promise would strand every waiter but the last.
 */
class ArrivalEvent {
  private signalled = false;
  private waiters: Deferred<void>[] = [];

  public set(): void {
    this.signalled = true;
    const released = this.waiters;
    this.waiters = [];
    for (const waiter of released) {
      waiter.resolve();
    }
  }

  public clear(): void {
    this.signalled = false;
  }

  public wait(): Promise<void> {
    if (this.signalled) {
      return Promise.resolve();
    }
    const waiter = new Deferred<void>();
    this.waiters.push(waiter);
    return waiter.promise;
  }
}

/** One appended message: its id, the wire bytes, and its delivery bookkeeping. */
interface Message {
  readonly entryId: string;

  /** The serialized wire form; parsed back on every delivery (tolerant reader). */
  readonly payload: string;

  deliveryCount: number;
  claimed: boolean;
}

/** One session's front door: its backlog, its quarantine and its fence. */
interface InboxState {
  readonly messages: Message[];
  readonly quarantined: QuarantinedEntry[];
  maxEpoch: number;
  readonly arrival: ArrivalEvent;
}

/** The session's durable front door, held in process memory. */
export class InMemoryInbox implements Inbox {
  private readonly sessions = new Map<SessionId, InboxState>();

  private stateOf(sessionId: SessionId): InboxState {
    const known = this.sessions.get(sessionId);
    if (known !== undefined) {
      return known;
    }
    const created: InboxState = { messages: [], quarantined: [], maxEpoch: 0, arrival: new ArrivalEvent() };
    this.sessions.set(sessionId, created);
    return created;
  }

  private guardEpoch(sessionId: SessionId, epoch: Epoch): InboxState {
    const state = this.stateOf(sessionId);
    if (epoch < state.maxEpoch) {
      throw new StaleEpochError(`epoch ${epoch} is stale (current ${state.maxEpoch})`);
    }
    state.maxEpoch = epoch;
    return state;
  }

  public async append(sessionId: SessionId, dataPoint: AnyDataPoint): Promise<string> {
    return this.appendSerialized(sessionId, JSON.stringify(dataPoint.toWire()));
  }

  /**
   * Append a raw wire payload — the seam a foreign producer writes through.
   *
   * This is what a direct `XADD` is for the Redis adapter: the producer may run newer code (or be
   * plain wrong), so the payload is not guaranteed to parse on this side.
   */
  public async appendSerialized(sessionId: SessionId, payload: string): Promise<string> {
    const state = this.stateOf(sessionId);
    const entryId = randomUUID();
    state.messages.push({ entryId, payload, deliveryCount: 0, claimed: false });
    state.arrival.set();
    return entryId;
  }

  /**
   * Present one message, tolerating malformed bytes and nothing else.
   *
   * Poison is reserved for a bad wire payload: only bytes that fail to decode as JSON. Semantic
   * deserialization failures from `parseDataPoint` (an unknown type, a validation error, a registry
   * bug) propagate and fail fast — a quiet quarantine would hide a parser regression, and
   * redelivery on resume lets a newer deployment parse what an older one could not.
   */
  private toEntry(message: Message): DeliveredInboxEntry {
    let decoded: unknown;
    try {
      decoded = JSON.parse(message.payload);
    } catch (error) {
      // Named the way Python names it (`type(error).__name__: error`), so a quarantine record reads
      // the same whichever runtime wrote it.
      const failure = error instanceof Error ? error : new Error(String(error));
      return new PoisonInboxEntry({
        entryId: message.entryId,
        error: `${failure.name}: ${failure.message}`,
        deliveryCount: message.deliveryCount,
        rawPayload: message.payload,
      });
    }
    return new InboxEntry({
      entryId: message.entryId,
      dataPoint: parseDataPoint(decoded),
      deliveryCount: message.deliveryCount,
    });
  }

  public async consume(sessionId: SessionId, options: ConsumeOptions = {}): Promise<readonly DeliveredInboxEntry[]> {
    const maxEntries = options.maxEntries ?? null;
    if (maxEntries !== null && maxEntries <= 0) {
      return [];
    }
    const claimed: DeliveredInboxEntry[] = [];
    for (const message of this.stateOf(sessionId).messages) {
      if (maxEntries !== null && claimed.length >= maxEntries) {
        break; // check the cap before claiming, so maxEntries=0 claims nothing
      }
      if (message.claimed) {
        continue;
      }
      message.claimed = true;
      message.deliveryCount += 1;
      claimed.push(this.toEntry(message));
    }
    return claimed;
  }

  public async reclaim(sessionId: SessionId): Promise<readonly DeliveredInboxEntry[]> {
    const reclaimed: DeliveredInboxEntry[] = [];
    for (const message of this.stateOf(sessionId).messages) {
      if (!message.claimed) {
        continue;
      }
      message.deliveryCount += 1;
      reclaimed.push(this.toEntry(message));
    }
    return reclaimed;
  }

  public async ack(sessionId: SessionId, entryId: string, options: { readonly epoch: Epoch }): Promise<void> {
    const state = this.guardEpoch(sessionId, options.epoch);
    const index = state.messages.findIndex((message) => message.entryId === entryId);
    const message = index === -1 ? undefined : state.messages[index];
    if (message === undefined) {
      return; // unknown / already-acked entry is a safe no-op
    }
    // Redis XACK only removes claimed (pending) entries; mirroring that, an ack that races ahead of
    // delivery is a safe no-op and the entry stays deliverable.
    if (message.claimed) {
      state.messages.splice(index, 1); // ack removes the entry — acked messages must not accumulate
    }
  }

  public async quarantine(sessionId: SessionId, entryId: string, options: QuarantineOptions): Promise<void> {
    const state = this.guardEpoch(sessionId, options.epoch);
    const index = state.messages.findIndex((message) => message.entryId === entryId);
    const message = index === -1 ? undefined : state.messages[index];
    if (message === undefined) {
      return; // unknown / already-disposed entry is a safe no-op (mirrors ack) — never double-records
    }
    // Disposal mirrors the Redis script: the record is pushed iff the XACK-equivalent actually
    // removed a claimed entry — a never-delivered entry is neither removed nor recorded, so a
    // premature quarantine cannot destroy or double-record it.
    if (!message.claimed) {
      return;
    }
    state.messages.splice(index, 1); // out of delivery for good — quarantine is terminal, unlike un-acked
    state.quarantined.push(
      new QuarantinedEntry({
        entryId,
        reason: options.reason,
        deliveryCount: message.deliveryCount,
        rawPayload: message.payload,
      }),
    );
  }

  public async quarantined(sessionId: SessionId): Promise<readonly QuarantinedEntry[]> {
    return [...this.stateOf(sessionId).quarantined];
  }

  public async pendingCount(sessionId: SessionId): Promise<number> {
    return this.stateOf(sessionId).messages.length;
  }

  public async waitForEntry(sessionId: SessionId): Promise<void> {
    const state = this.stateOf(sessionId);
    if (state.messages.length > 0) {
      return; // entries already pending — never wait on data that is already here
    }
    // The pending check and the clear happen with no await between them (single event loop), so an
    // append can never slip into that gap unobserved: it either landed above or it will set the
    // event we are about to wait on.
    state.arrival.clear();
    await state.arrival.wait();
  }
}
