/**
 * `SessionState` — the session's in-memory DataPoint set, revisions and watermarks.
 *
 * Keyed-merge on `(class, value)`: an equal DataPoint **merges** into the existing entry (keeping
 * `firstRetrieved`, advancing `lastRetrieved`) rather than duplicating. Every batch that changes
 * something advances one monotonic revision, and each entry remembers the revision it was added
 * at and last freshened at; an operator's **watermark** is the revision its last run observed, so
 * the delta since then splits cleanly into added vs updated.
 *
 * @module
 */

import type { AnyDataPoint } from './datapoints.js';
import { DataPointView, identityKey } from './datapoints.js';
import type { CapabilityId, OperatorId } from './ids.js';
import { InvocationDelta } from './operators.js';

/**
 * One held identity: the DataPoint itself and the revisions that touched it.
 *
 * Engine internals are plain objects, not validated models: the loop is their only constructor,
 * so validation would only check the engine against itself — per merge, on the hot path. Mutable
 * on purpose, and private to this module: a re-observation rewrites the entry in place rather
 * than rebuilding the map.
 */
interface Entry {
  dataPoint: AnyDataPoint;
  addedRev: number;
  updatedRev: number;
}

/** What one {@link SessionState.merge} did: the resulting revision, and what was new vs freshened. */
export interface MergeOutcome {
  readonly revision: number;

  /** Identities the batch introduced. */
  readonly added: readonly AnyDataPoint[];

  /** Held identities the batch freshened, as they now stand. */
  readonly updated: readonly AnyDataPoint[];

  /** Whether anything changed at all — precomputed, because every caller asks. */
  readonly changed: boolean;
}

/** Whose caps changed since an operator last ran, as {@link SessionState.deltaFor} takes them. */
export interface DeltaCapabilities {
  /** The capabilities available when this operator last ran. */
  readonly previousCaps: ReadonlySet<CapabilityId>;

  /** The capabilities available now. */
  readonly availableCaps: ReadonlySet<CapabilityId>;
}

/** The session's DataPoints, keyed by identity, plus the revisions the scheduler reads. */
export class SessionState {
  private readonly entries = new Map<string, Entry>();
  private currentRevision = 0;
  private readonly watermarks = new Map<OperatorId, number>();

  /** The current revision; it advances once per batch that changed something. */
  public get revision(): number {
    return this.currentRevision;
  }

  /** Keyed-merge a batch; the revision advances iff something was added or freshened. */
  public merge(dataPoints: Iterable<AnyDataPoint>): MergeOutcome {
    const nextRevision = this.currentRevision + 1;
    const added: AnyDataPoint[] = [];
    const updated: AnyDataPoint[] = [];
    for (const dataPoint of dataPoints) {
      const key = identityKey(dataPoint);
      const existing = this.entries.get(key);
      if (existing === undefined) {
        this.entries.set(key, { dataPoint, addedRev: nextRevision, updatedRev: nextRevision });
        added.push(dataPoint);
      } else if (dataPoint.lastRetrieved.getTime() > existing.dataPoint.lastRetrieved.getTime()) {
        existing.dataPoint = existing.dataPoint.reobserved(dataPoint.lastRetrieved);
        existing.updatedRev = nextRevision;
        updated.push(existing.dataPoint);
      }
    }
    const changed = added.length > 0 || updated.length > 0;
    if (changed) {
      this.currentRevision = nextRevision;
    }
    return Object.freeze({
      revision: this.currentRevision,
      added: Object.freeze(added),
      updated: Object.freeze(updated),
      changed,
    });
  }

  /** A read view over everything held, in the order the identities first arrived. */
  public view(): DataPointView {
    return new DataPointView([...this.entries.values()].map((entry) => entry.dataPoint));
  }

  /** Whether this operator has ever run in this session (it has a watermark). */
  public hasRun(operatorId: OperatorId): boolean {
    return this.watermarks.has(operatorId);
  }

  /** Record the revision an operator's run observed; its next delta starts from there. */
  public advanceWatermark(operatorId: OperatorId, revision: number): void {
    this.watermarks.set(operatorId, revision);
  }

  /** What changed since `operatorId` last ran; a first run presents the whole set as `added`. */
  public deltaFor(operatorId: OperatorId, capabilities: DeltaCapabilities): InvocationDelta {
    const newlyAvailable = [...capabilities.availableCaps].filter((id) => !capabilities.previousCaps.has(id));
    const watermark = this.watermarks.get(operatorId);
    const entries = [...this.entries.values()];
    if (watermark === undefined) {
      return InvocationDelta({
        added: entries.map((entry) => entry.dataPoint),
        updated: [],
        newlyAvailableCaps: newlyAvailable,
        isFirstInvocation: true,
      });
    }
    return InvocationDelta({
      added: entries.filter((entry) => entry.addedRev > watermark).map((entry) => entry.dataPoint),
      // Added at or before the watermark but freshened after it: seen before, seen again since.
      updated: entries
        .filter((entry) => entry.addedRev <= watermark && watermark < entry.updatedRev)
        .map((entry) => entry.dataPoint),
      newlyAvailableCaps: newlyAvailable,
      isFirstInvocation: false,
    });
  }
}
