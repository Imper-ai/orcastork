/**
 * In-memory `SessionEventSink` — every published event, in order, for tests and local runs.
 *
 * @module
 */

import type { SessionEvent, SessionEventSink } from '../events.js';
import type { SessionId } from '../ids.js';

/** Keeps every event it is given; nothing is bounded, so it is for tests and local runs only. */
export class InMemorySessionEventSink implements SessionEventSink {
  /** Every event published to this sink, across all sessions, in publication order. */
  public readonly events: SessionEvent[] = [];

  public publish(event: SessionEvent): Promise<void> {
    this.events.push(event);
    return Promise.resolve();
  }

  /** The events of one session, in order — one sink may serve several. */
  public forSession(sessionId: SessionId): readonly SessionEvent[] {
    return this.events.filter((event) => event.sessionId === sessionId);
  }
}
