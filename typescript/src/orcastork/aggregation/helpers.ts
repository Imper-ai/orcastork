/**
 * `AggregationHelpers` — the durable-write API handed to an aggregator via its context.
 *
 * Bound to one `(sessionId, operatorId, epoch)`, it offers an OCC upsert (read the current version,
 * write guarded by it), an idempotent set-add, and a contribution marker (so a session contributes
 * at most once). The writes here are epoch-fenced and version-guarded; the bounded-retry /
 * dead-letter wrapping is layered on by the orchestrator around the aggregator's whole run.
 *
 * @module
 */

import type { Clock } from '../clock.js';
import type { Epoch, OperatorId, SessionId } from '../ids.js';
import type { DurableStore } from '../ports/durable_store.js';

/** The framework-owned lifecycle of one durable output record. */
export const AggregateStatus = {
  /** Written by an `interimRefresh` aggregator during gathering; a later write may still change it. */
  IN_PROGRESS: 'in_progress',

  /** Written by the authoritative finalize pass; terminal for this output. */
  FINAL: 'final',
} as const;

/** One of the {@link AggregateStatus} values; the string is what the durable record stores. */
export type AggregateStatus = (typeof AggregateStatus)[keyof typeof AggregateStatus];

/** What an {@link AggregationHelpers} is bound to — everything but the store itself. */
export interface AggregationHelpersOptions {
  readonly sessionId: SessionId;

  readonly operatorId: OperatorId;

  readonly epoch: Epoch;

  readonly clock: Clock;

  /** True only during the authoritative finalize pass; false for an interim refresh. */
  readonly isFinal: boolean;
}

/** The durable-write surface an aggregator gets through `ctx.aggregation`. */
export class AggregationHelpers {
  private readonly durable: DurableStore;
  private readonly sessionId: SessionId;
  private readonly operatorId: OperatorId;
  private readonly epoch: Epoch;
  private readonly clock: Clock;
  private readonly isFinal: boolean;

  public constructor(durable: DurableStore, options: AggregationHelpersOptions) {
    this.durable = durable;
    this.sessionId = options.sessionId;
    this.operatorId = options.operatorId;
    this.epoch = options.epoch;
    this.clock = options.clock;
    this.isFinal = options.isFinal;
  }

  /**
   * OCC upsert into `table` (the output model's `tableName`): read the current version, then write
   * guarded by it. Returns the new version.
   */
  public async upsert(table: string, key: string, document: Readonly<Record<string, unknown>>): Promise<number> {
    const existing = await this.durable.read(table, key);
    const expectedVersion = existing === null ? 0 : existing.version;
    if (!this.isFinal && existing !== null && existing.status === AggregateStatus.FINAL) {
      // FINAL is terminal for this output: a later interim refresh must never walk it back. A
      // resumed or reopened epoch re-runs `interimRefresh` aggregators during gathering, and its
      // finalize pass is skipped once the contribution is marked — so without this guard the record
      // is left at `in_progress` with no finalize left to restore it, and every consumer that waits
      // for `final` reads a finished session as having produced nothing. The version is returned
      // unchanged: the caller's contract is "the record's current version", and nothing was written.
      return existing.version;
    }
    const status = this.isFinal ? AggregateStatus.FINAL : AggregateStatus.IN_PROGRESS;
    return await this.durable.upsert(table, key, document, {
      expectedVersion,
      epoch: this.epoch,
      status,
      updatedAt: this.clock.now(),
    });
  }

  /**
   * Idempotent set-add into `table` keyed by id (set-cardinality, never a double-counting
   * increment).
   */
  public async addToSet(table: string, key: string, fieldName: string, value: string): Promise<number> {
    return await this.durable.addToSet(table, key, fieldName, value, { epoch: this.epoch });
  }

  /** Record this session's contribution; `false` if it was already recorded. */
  public async markContribution(): Promise<boolean> {
    return await this.durable.markContribution(this.sessionId, this.operatorId, { epoch: this.epoch });
  }
}
