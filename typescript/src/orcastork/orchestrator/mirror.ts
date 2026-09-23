/**
 * `SessionStateMirror` — the orchestrator's local copy of one session's DataPoint state.
 *
 * While the orchestrator holds the fencing epoch it is the session's **sole mutator**, so the
 * store's contents are fully determined by what this process has already written — the
 * local-state-plus-durable-changelog insight from Kafka Streams / Flink. The mirror rehydrates
 * once (one snapshot + one revision read), then serves every snapshot / revision / change-set
 * read locally and resolves every keyed-merge locally, forwarding each resolved batch to the
 * durable store via {@link DataPointStore.applyResolved}. The store stays the system of record
 * **and the revision allocator** — the mirror never invents a revision; it applies its local
 * copy at the revision the store returned.
 *
 * Fencing is untouched: every forwarded write is epoch-guarded by the store, and a
 * `StaleEpochError` propagates *before* the local copy is touched — a fenced orchestrator dies
 * holding a mirror that never diverged from what the store accepted. The local reads are
 * trustworthy for the same reason the optimization is sound: any other writer must hold a higher
 * epoch, and this process's next forwarded write would then raise instead of silently diverging.
 * Sanctioned mid-session input (the inbox) is merged *through* the mirror on the gathering loop,
 * so it is never invisible to local reads.
 *
 * Change-set answers for revisions that predate the rehydration (a resumed operator's persisted
 * watermark) cannot be derived from the snapshot alone — those per-identity revision stamps live
 * only in the store — so the orchestrator primes each such baseline once at gather start
 * ({@link SessionStateMirror.primeChangeBaseline}); every later query overlays the local stamps on
 * the primed split. Add-stamps are immutable in the store, so a primed baseline never goes stale:
 * only *later updates* can change an answer, and those are exactly what the local stamps record.
 *
 * @module
 */

import type { AnyDataPoint, MergeResult } from '../datapoints/index.js';
import { DataPointView, identityKey, MergeKind } from '../datapoints/index.js';
import { StateMirrorError } from '../exceptions.js';
import type { Epoch, Revision, SessionId } from '../ids.js';
import { Revision as toRevision } from '../ids.js';
import { ChangeSet } from '../ports/change_set.js';
import type { DataPointReader, DataPointStore } from '../ports/datapoint_store.js';

/**
 * One mirrored identity: the DataPoint plus the revision stamps mirroring the store's
 * added/updated bookkeeping.
 *
 * `0` means "at or before rehydration": those stamps live only in the store, so pre-rehydration
 * queries consult a primed baseline instead of these fields.
 */
interface MirrorEntry {
  dataPoint: AnyDataPoint;
  addedRev: number;
  updatedRev: number;
}

/** A batch keyed-merge resolved against the local state, before it is durably applied. */
interface Resolution {
  /** One per presented DataPoint, in input order. */
  readonly outcomes: readonly MergeResult[];

  /** Identities new to the session → final values. */
  readonly added: ReadonlyMap<string, AnyDataPoint>;

  /** Existing identities → merged values (freshened only). */
  readonly updated: ReadonlyMap<string, AnyDataPoint>;
}

/** What one write through the mirror did: the store's revision, and one outcome per input. */
export interface MirrorWriteResult {
  /** Allocated by the store, never locally. */
  readonly revision: Revision;

  /** One per presented DataPoint, in input order. */
  readonly outcomes: readonly MergeResult[];
}

/** The sole-mutator local copy of one session's DataPoint state. */
export class SessionStateMirror implements DataPointReader {
  private readonly store: DataPointStore;
  private readonly sessionId: SessionId;
  private entries = new Map<string, MirrorEntry>();
  private baselines = new Map<number, ReadonlyMap<string, MergeKind>>();
  private currentRevision = 0;
  private rehydratedAt = 0;
  private hydrated = false;

  public constructor(store: DataPointStore, sessionId: SessionId) {
    this.store = store;
    this.sessionId = sessionId;
  }

  /** Load the authoritative local copy — the mirror's only full read of the store. */
  public async rehydrate(): Promise<void> {
    const view = await this.store.snapshot(this.sessionId);
    const revision = await this.store.revision(this.sessionId);
    this.entries = new Map(
      view.all().map((dataPoint) => [identityKey(dataPoint), { dataPoint, addedRev: 0, updatedRev: 0 }]),
    );
    this.currentRevision = revision;
    this.rehydratedAt = revision;
    this.baselines = new Map();
    this.hydrated = true;
  }

  /**
   * Capture the store's change split for one pre-rehydration revision (one read, reused forever).
   *
   * A revision at or past the rehydration point needs no baseline — the local stamps answer it
   * exactly — so priming one is a no-op.
   */
  public async primeChangeBaseline(since: Revision): Promise<void> {
    this.requireHydrated();
    if (since >= this.rehydratedAt || this.baselines.has(since)) {
      return;
    }
    const change = await this.store.changeSetSince(this.sessionId, since);
    const baseline = new Map<string, MergeKind>();
    for (const dataPoint of change.added) {
      baseline.set(identityKey(dataPoint), MergeKind.ADDED);
    }
    for (const dataPoint of change.updated) {
      baseline.set(identityKey(dataPoint), MergeKind.UPDATED);
    }
    this.baselines.set(since, baseline);
  }

  /**
   * How many `(type, value)` identities the local copy holds.
   *
   * The mirror is the sole mutator's whole in-memory footprint for a session, so this is what a
   * bounded-growth assertion counts — and it is a plain read, needing neither the session guard nor
   * a snapshot allocation.
   */
  public get size(): number {
    return this.entries.size;
  }

  public async snapshot(sessionId: SessionId): Promise<DataPointView> {
    this.guardRead(sessionId);
    return new DataPointView([...this.entries.values()].map((entry) => entry.dataPoint));
  }

  public async revision(sessionId: SessionId): Promise<Revision> {
    this.guardRead(sessionId);
    return toRevision(this.currentRevision);
  }

  public async changeSetSince(sessionId: SessionId, since: Revision): Promise<ChangeSet> {
    this.guardRead(sessionId);
    if (since >= this.rehydratedAt) {
      return this.localChangeSet(since);
    }
    return this.baselinedChangeSet(since);
  }

  /**
   * Keyed-merge locally, forward the resolved batch, apply at the store-returned revision.
   *
   * Even an all-no-op batch is forwarded, keeping fencing byte-identical to
   * {@link DataPointStore.write}: a stale writer is rejected (and this orchestrator stops) whether
   * or not its batch would have changed anything. A rejected forward leaves the local copy
   * untouched, so the mirror can never run ahead of what the store accepted.
   */
  public async write(
    dataPoints: readonly AnyDataPoint[],
    options: { readonly epoch: Epoch },
  ): Promise<MirrorWriteResult> {
    this.requireHydrated();
    const resolution = this.resolve(dataPoints);
    const revision = await this.store.applyResolved(this.sessionId, {
      added: [...resolution.added.values()],
      updated: [...resolution.updated.values()],
      epoch: options.epoch,
    });
    this.apply(resolution, revision);
    return { revision, outcomes: resolution.outcomes };
  }

  private resolve(dataPoints: readonly AnyDataPoint[]): Resolution {
    // Reproduces the store's merge semantics over the local state, including duplicates within one
    // batch: an identity added then re-observed in the same batch stays one `added` row carrying
    // its final timestamps; `lastRetrieved` advances only when newer (`reobserved` keeps the max)
    // and `firstRetrieved` is immutable.
    const outcomes: MergeResult[] = [];
    const added = new Map<string, AnyDataPoint>();
    const updated = new Map<string, AnyDataPoint>();
    for (const dataPoint of dataPoints) {
      const key = identityKey(dataPoint);
      const pendingAdd = added.get(key);
      if (pendingAdd !== undefined) {
        const merged = pendingAdd.reobserved(dataPoint.lastRetrieved);
        added.set(key, merged);
        outcomes.push({ kind: MergeKind.UPDATED, dataPoint: merged });
        continue;
      }
      const pendingUpdate = updated.get(key);
      if (pendingUpdate !== undefined) {
        const merged = pendingUpdate.reobserved(dataPoint.lastRetrieved);
        updated.set(key, merged);
        outcomes.push({ kind: MergeKind.UPDATED, dataPoint: merged });
        continue;
      }
      const existing = this.entries.get(key);
      if (existing === undefined) {
        added.set(key, dataPoint);
        outcomes.push({ kind: MergeKind.ADDED, dataPoint });
        continue;
      }
      const merged = existing.dataPoint.reobserved(dataPoint.lastRetrieved);
      outcomes.push({ kind: MergeKind.UPDATED, dataPoint: merged });
      if (dataPoint.lastRetrieved.getTime() > existing.dataPoint.lastRetrieved.getTime()) {
        updated.set(key, merged);
      }
    }
    return { outcomes, added, updated };
  }

  private apply(resolution: Resolution, revision: number): void {
    for (const [key, dataPoint] of resolution.added) {
      this.entries.set(key, { dataPoint, addedRev: revision, updatedRev: revision });
    }
    for (const [key, dataPoint] of resolution.updated) {
      const entry = this.entries.get(key);
      if (entry === undefined) {
        continue;
      }
      entry.dataPoint = dataPoint;
      entry.updatedRev = revision;
    }
    this.currentRevision = revision;
  }

  private localChangeSet(since: number): ChangeSet {
    const entries = [...this.entries.values()];
    return new ChangeSet({
      added: entries.filter((entry) => entry.addedRev > since).map((entry) => entry.dataPoint),
      updated: entries
        .filter((entry) => entry.addedRev <= since && since < entry.updatedRev)
        .map((entry) => entry.dataPoint),
    });
  }

  private baselinedChangeSet(since: number): ChangeSet {
    const baseline = this.baselines.get(since);
    if (baseline === undefined) {
      throw new StateMirrorError(
        `no primed change baseline for pre-rehydration revision ${since}` +
          ` (mirror rehydrated at revision ${this.rehydratedAt})`,
      );
    }
    const added: AnyDataPoint[] = [];
    const updated: AnyDataPoint[] = [];
    for (const [key, entry] of this.entries) {
      // A local add postdates rehydration, so it postdates `since` too; a baseline ADDED stays
      // added forever (add-stamps are immutable). Anything else changed iff the baseline saw an
      // update or the local stamps recorded one after rehydration.
      if (entry.addedRev > since || baseline.get(key) === MergeKind.ADDED) {
        added.push(entry.dataPoint);
      } else if (baseline.get(key) === MergeKind.UPDATED || entry.updatedRev > since) {
        updated.push(entry.dataPoint);
      }
    }
    return new ChangeSet({ added, updated });
  }

  private requireHydrated(): void {
    if (!this.hydrated) {
      throw new StateMirrorError('the session mirror was used before rehydrate()');
    }
  }

  private guardRead(sessionId: SessionId): void {
    this.requireHydrated();
    if (sessionId !== this.sessionId) {
      throw new StateMirrorError(`mirror for session '${this.sessionId}' asked about session '${sessionId}'`);
    }
  }
}
