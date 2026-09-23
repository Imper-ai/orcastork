/**
 * In-memory `AuditSink` — a durable, append-only event log, epoch-fenced.
 *
 * `append` commits the entry where `replay` can see it (rejecting a stale-epoch writer's entries so
 * a fenced writer produces no authoritative audit), preserving order and per-event granularity. A
 * run that stops partway still has every entry it appended, with no recovery step standing between
 * the append and the read.
 *
 * @module
 */

import type { AuditLogEntry } from '../../audit/index.js';
import { StaleEpochError } from '../../exceptions.js';
import type { SessionId } from '../../ids.js';
import type { AuditSink } from '../../ports/audit_sink.js';

/** One session's trail: everything committed for it, and the highest epoch that wrote. */
interface AuditState {
  readonly committed: AuditLogEntry[];
  maxEpoch: number;
}

/** The event log every other in-memory adapter's contract is audited against. */
export class InMemoryAuditSink implements AuditSink {
  private readonly sessions = new Map<SessionId, AuditState>();

  private stateOf(sessionId: SessionId): AuditState {
    const known = this.sessions.get(sessionId);
    if (known !== undefined) {
      return known;
    }
    const created: AuditState = { committed: [], maxEpoch: 0 };
    this.sessions.set(sessionId, created);
    return created;
  }

  public async append(entry: AuditLogEntry): Promise<void> {
    const state = this.stateOf(entry.sessionId);
    if (entry.epoch < state.maxEpoch) {
      throw new StaleEpochError(`epoch ${entry.epoch} is stale (current ${state.maxEpoch})`);
    }
    state.maxEpoch = entry.epoch;
    state.committed.push(entry);
  }

  public async appendMany(entries: readonly AuditLogEntry[]): Promise<void> {
    // In memory a batch costs the same as N appends, and a (contract-homogeneous) stale batch is
    // rejected by its first entry before anything lands — atomic either way.
    for (const entry of entries) {
      await this.append(entry);
    }
  }

  public async replay(sessionId: SessionId): Promise<readonly AuditLogEntry[]> {
    return [...this.stateOf(sessionId).committed];
  }
}
