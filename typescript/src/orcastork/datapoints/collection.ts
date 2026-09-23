/**
 * `DataPointSet` — a keyed collection with merge-on-add (the dedup primitive).
 *
 * Identity is `(type, value)` (timestamps excluded), so adding an equal DataPoint **merges** into
 * the existing entry — keeping `firstRetrieved` and advancing `lastRetrieved` — rather than
 * duplicating or dropping the new timestamp. The in-memory store and delta machinery build on this.
 *
 * @module
 */

import type { AnyDataPoint } from './base.js';
import { BaseDataPoint, identityKey } from './base.js';

/** How a DataPoint landed: a new `(type, value)` identity, or a re-observation of one held. */
export const MergeKind = {
  /** A new `(type, value)` identity. */
  ADDED: 'added',

  /** An existing identity re-observed (`lastRetrieved` bumped). */
  UPDATED: 'updated',
} as const;

/** One of the two {@link MergeKind} values; the strings reach the audit trail and the wire. */
export type MergeKind = (typeof MergeKind)[keyof typeof MergeKind];

/** What one `add` did, and the entry the set holds afterwards. */
export interface MergeResult {
  readonly kind: MergeKind;

  readonly dataPoint: AnyDataPoint;
}

/** A keyed set of DataPoints: adding an identity already present merges into it. */
export class DataPointSet {
  private readonly items = new Map<string, AnyDataPoint>();

  public constructor(items: Iterable<AnyDataPoint> = []) {
    for (const item of items) {
      this.add(item);
    }
  }

  /** Insert a new identity, or merge into an existing one (bumping `lastRetrieved`). */
  public add(dataPoint: AnyDataPoint): MergeResult {
    const key = identityKey(dataPoint);
    const existing = this.items.get(key);
    if (existing === undefined) {
      this.items.set(key, dataPoint);
      return { kind: MergeKind.ADDED, dataPoint };
    }
    const merged = existing.reobserved(dataPoint.lastRetrieved);
    this.items.set(key, merged);
    return { kind: MergeKind.UPDATED, dataPoint: merged };
  }

  /** Every held DataPoint, in first-insertion order. */
  public all(): readonly AnyDataPoint[] {
    return [...this.items.values()];
  }

  /**
   * Whether this identity is held — timestamps excluded.
   *
   * Anything that is not a DataPoint is simply absent, rather than an error: the guard is what
   * keeps a membership test on a foreign value from blowing up inside the identity normalization.
   */
  public has(dataPoint: unknown): boolean {
    return dataPoint instanceof BaseDataPoint && this.items.has(identityKey(dataPoint));
  }

  /** How many identities the set holds. */
  public get size(): number {
    return this.items.size;
  }

  public [Symbol.iterator](): Iterator<AnyDataPoint> {
    return this.items.values();
  }
}
