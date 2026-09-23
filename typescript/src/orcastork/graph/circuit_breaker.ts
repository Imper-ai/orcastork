/**
 * Runtime circuit-breaker — bounds a permitted cycle per session.
 *
 * The orchestrator records each run of a cyclic operator; once an operator reaches its `maxCycles`
 * cap it is *tripped* and the orchestrator stops re-running it, halting the loop. The counter is
 * per-session per-operator — a fresh breaker per session resets it.
 *
 * @module
 */

import type { OperatorId } from '../ids.js';

/** Counts runs per operator and trips the ones that reach their cap. */
export class CircuitBreaker {
  private readonly caps: ReadonlyMap<OperatorId, number>;
  private readonly counts = new Map<OperatorId, number>();

  /**
   * `caps` is the cap of every operator on a cycle, as the orchestrator computed it for THIS
   * session's (possibly pruned) gathering set — a `Map`, or any iterable of entries.
   */
  public constructor(caps: Iterable<readonly [OperatorId, number]>) {
    this.caps = new Map(caps);
  }

  public recordRun(operatorId: OperatorId): void {
    this.counts.set(operatorId, (this.counts.get(operatorId) ?? 0) + 1);
  }

  /** True once a capped operator has run its cap; an uncapped operator never trips. */
  public isTripped(operatorId: OperatorId): boolean {
    const cap = this.caps.get(operatorId);
    return cap !== undefined && (this.counts.get(operatorId) ?? 0) >= cap;
  }

  /** How many times this operator has run in this session. */
  public runs(operatorId: OperatorId): number {
    return this.counts.get(operatorId) ?? 0;
  }
}
