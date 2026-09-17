/**
 * `DataPointView` — the read-only, subtype-aware query facade handed to operators.
 *
 * Queries are polymorphic (`instanceof` / the prototype chain): asking for an abstract intermediate
 * (`EmailDataPoint`) returns every concrete leaf below it. `latest(Type)` returns the newest
 * matching DataPoint by `lastRetrieved` while the full set stays queryable.
 *
 * @module
 */

import type { AnyDataPoint, DataPointClass } from './base.js';

/** A read-only window onto a session's DataPoints. */
export class DataPointView {
  private readonly items: readonly AnyDataPoint[];

  public constructor(items: Iterable<AnyDataPoint> = []) {
    this.items = Object.freeze([...items]);
  }

  /** Every DataPoint in the view, in arrival order. */
  public all(): readonly AnyDataPoint[] {
    return this.items;
  }

  /** All DataPoints that are instances of `dataPointType` (subtype-aware). */
  public ofType<T extends AnyDataPoint>(dataPointType: DataPointClass<T>): readonly T[] {
    return this.items.filter((item): item is T => item instanceof dataPointType);
  }

  /** The newest DataPoint of `dataPointType` by `lastRetrieved`, or `null`. */
  public latest<T extends AnyDataPoint>(dataPointType: DataPointClass<T>): T | null {
    let newest: T | null = null;
    for (const item of this.ofType(dataPointType)) {
      // Strictly newer, so a tie keeps the earlier arrival — Python's `max` picks the first maximum.
      if (newest === null || item.lastRetrieved.getTime() > newest.lastRetrieved.getTime()) {
        newest = item;
      }
    }
    return newest;
  }

  /** How many DataPoints the view holds. */
  public get size(): number {
    return this.items.length;
  }

  public [Symbol.iterator](): Iterator<AnyDataPoint> {
    return this.items[Symbol.iterator]();
  }
}
