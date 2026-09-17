/**
 * In-memory `DataPointStore` — deterministic, infra-free, fenced keyed-merge store.
 *
 * Per session it keeps each identity's DataPoint plus the revisions at which it was first added
 * and last freshened, so `changeSetSince` can cleanly split added vs updated. The session revision
 * advances once per write that actually changed something. Writes are guarded by the highest epoch
 * seen for the session (lower epoch → `StaleEpochError`).
 *
 * @module
 */

import type { AnyDataPoint } from '../../datapoints/index.js';
import { DataPointView, identityKey } from '../../datapoints/index.js';
import { StaleEpochError } from '../../exceptions.js';
import type { Epoch, OperatorId, Revision, SessionId } from '../../ids.js';
import { Revision as toRevision } from '../../ids.js';
import { ChangeSet } from '../../ports/change_set.js';
import type { ApplyResolvedOptions, ClaimEffectOptions, DataPointStore } from '../../ports/datapoint_store.js';
import { EFFECT_COMMITTED, EffectClaim, effectPendingState } from '../../ports/datapoint_store.js';

/** One stored identity: the DataPoint plus the revisions that stamp it added / last freshened. */
interface Entry {
  dataPoint: AnyDataPoint;
  addedRev: number;
  updatedRev: number;
}

/** Everything one session owns: its blackboard and its epoch-fenced meta state. */
interface SessionState {
  readonly entries: Map<string, Entry>;
  revision: number;
  maxEpoch: number;
  readonly watermarks: Map<OperatorId, number>;

  /** effect key → `pending:<epoch>` | `committed`. */
  readonly effects: Map<string, string>;

  /** The persisted wall-clock session deadline (set once, rehydrated on resume). */
  deadline: Date | null;

  /** The latest flow that drove the session (drift detection on resume). */
  flowFingerprint: string | null;
}

const newSessionState = (): SessionState => ({
  entries: new Map<string, Entry>(),
  revision: 0,
  maxEpoch: 0,
  watermarks: new Map<OperatorId, number>(),
  effects: new Map<string, string>(),
  deadline: null,
  flowFingerprint: null,
});

/**
 * Reject a superseded writer and record the highest ACCEPTED epoch.
 *
 * Ownership is the lock's job; recording the highest accepted epoch on EVERY guarded write is what
 * completes stale-writer rejection — a session whose first mutation is an effect claim or a
 * session-meta write must still fence a later lower-epoch writer.
 */
const guardEpoch = (state: SessionState, epoch: Epoch): void => {
  if (epoch < state.maxEpoch) {
    throw new StaleEpochError(`epoch ${epoch} is stale (current ${state.maxEpoch})`);
  }
  state.maxEpoch = epoch;
};

/** The blackboard every other in-memory adapter is written against. */
export class InMemoryDataPointStore implements DataPointStore {
  private readonly sessions = new Map<SessionId, SessionState>();

  private stateOf(sessionId: SessionId): SessionState {
    const known = this.sessions.get(sessionId);
    if (known !== undefined) {
      return known;
    }
    const created = newSessionState();
    this.sessions.set(sessionId, created);
    return created;
  }

  public async write(
    sessionId: SessionId,
    dataPoints: Iterable<AnyDataPoint>,
    options: { readonly epoch: Epoch },
  ): Promise<Revision> {
    const state = this.stateOf(sessionId);
    guardEpoch(state, options.epoch);
    const nextRevision = state.revision + 1;
    let changed = false;
    for (const dataPoint of dataPoints) {
      const key = identityKey(dataPoint);
      const existing = state.entries.get(key);
      if (existing === undefined) {
        state.entries.set(key, { dataPoint, addedRev: nextRevision, updatedRev: nextRevision });
        changed = true;
      } else if (dataPoint.lastRetrieved.getTime() > existing.dataPoint.lastRetrieved.getTime()) {
        existing.dataPoint = existing.dataPoint.reobserved(dataPoint.lastRetrieved);
        existing.updatedRev = nextRevision;
        changed = true;
      }
    }
    if (changed) {
      state.revision = nextRevision;
    }
    return toRevision(state.revision);
  }

  public async applyResolved(sessionId: SessionId, options: ApplyResolvedOptions): Promise<Revision> {
    // The caller (a sole mutator) already keyed-merged, so this applies blindly: new identities
    // land with both stamps at the batch revision, merged identities overwrite their entry and
    // advance only the updated stamp — exactly the stamping `write` does.
    const state = this.stateOf(sessionId);
    guardEpoch(state, options.epoch);
    if (options.added.length === 0 && options.updated.length === 0) {
      return toRevision(state.revision);
    }
    const nextRevision = state.revision + 1;
    for (const dataPoint of options.added) {
      state.entries.set(identityKey(dataPoint), {
        dataPoint,
        addedRev: nextRevision,
        updatedRev: nextRevision,
      });
    }
    for (const dataPoint of options.updated) {
      const key = identityKey(dataPoint);
      const existing = state.entries.get(key);
      // An update of an unknown identity still lands (blind application, mirroring the Redis HSET)
      // — it simply carries no added stamp, like a field set by mode 'u'.
      const addedRev = existing === undefined ? 0 : existing.addedRev;
      state.entries.set(key, { dataPoint, addedRev, updatedRev: nextRevision });
    }
    state.revision = nextRevision;
    return toRevision(nextRevision);
  }

  public async snapshot(sessionId: SessionId): Promise<DataPointView> {
    return new DataPointView([...this.stateOf(sessionId).entries.values()].map((entry) => entry.dataPoint));
  }

  public async revision(sessionId: SessionId): Promise<Revision> {
    return toRevision(this.stateOf(sessionId).revision);
  }

  public async changeSetSince(sessionId: SessionId, since: Revision): Promise<ChangeSet> {
    const entries = [...this.stateOf(sessionId).entries.values()];
    return new ChangeSet({
      added: entries.filter((entry) => entry.addedRev > since).map((entry) => entry.dataPoint),
      updated: entries
        .filter((entry) => entry.addedRev <= since && since < entry.updatedRev)
        .map((entry) => entry.dataPoint),
    });
  }

  public async getWatermark(sessionId: SessionId, operatorId: OperatorId): Promise<Revision | null> {
    const watermark = this.stateOf(sessionId).watermarks.get(operatorId);
    return watermark === undefined ? null : toRevision(watermark);
  }

  public async setWatermark(
    sessionId: SessionId,
    operatorId: OperatorId,
    revision: Revision,
    options: { readonly epoch: Epoch },
  ): Promise<void> {
    const state = this.stateOf(sessionId);
    guardEpoch(state, options.epoch);
    state.watermarks.set(operatorId, revision);
  }

  public async claimEffect(
    sessionId: SessionId,
    effectKey: string,
    options: ClaimEffectOptions,
  ): Promise<EffectClaim> {
    const state = this.stateOf(sessionId);
    guardEpoch(state, options.epoch);
    const stored = state.effects.get(effectKey);
    if (stored === undefined) {
      state.effects.set(effectKey, effectPendingState(options.epoch));
      return EffectClaim.ACQUIRED;
    }
    if (stored === EFFECT_COMMITTED) {
      return EffectClaim.ALREADY_COMMITTED;
    }
    if (stored === effectPendingState(options.epoch)) {
      return EffectClaim.PENDING_SAME_EPOCH;
    }
    if (options.reclaimStale) {
      state.effects.set(effectKey, effectPendingState(options.epoch));
      return EffectClaim.ACQUIRED;
    }
    return EffectClaim.PENDING_STALE_EPOCH;
  }

  public async commitEffect(
    sessionId: SessionId,
    effectKey: string,
    options: { readonly epoch: Epoch },
  ): Promise<void> {
    const state = this.stateOf(sessionId);
    guardEpoch(state, options.epoch);
    // Only this epoch's own pending mark transitions; 'committed' stays as-is (idempotent), and any
    // other state is left untouched — never fabricate 'committed' for an unowned claim.
    const stored = state.effects.get(effectKey);
    if (stored === effectPendingState(options.epoch) || stored === EFFECT_COMMITTED) {
      state.effects.set(effectKey, EFFECT_COMMITTED);
    }
  }

  public async revertEffect(
    sessionId: SessionId,
    effectKey: string,
    options: { readonly epoch: Epoch },
  ): Promise<void> {
    const state = this.stateOf(sessionId);
    guardEpoch(state, options.epoch);
    // Deletes ONLY this epoch's own pending mark: never 'committed' (the effect DID run) and never
    // another epoch's pending (resolved by its owner, or by a successor's recovery policy).
    if (state.effects.get(effectKey) === effectPendingState(options.epoch)) {
      state.effects.delete(effectKey);
    }
  }

  public async getEffectState(sessionId: SessionId, effectKey: string): Promise<string | null> {
    return this.stateOf(sessionId).effects.get(effectKey) ?? null;
  }

  public async getSessionDeadline(sessionId: SessionId): Promise<Date | null> {
    return this.stateOf(sessionId).deadline;
  }

  public async setSessionDeadline(
    sessionId: SessionId,
    deadline: Date,
    options: { readonly epoch: Epoch },
  ): Promise<void> {
    const state = this.stateOf(sessionId);
    guardEpoch(state, options.epoch);
    state.deadline = deadline;
  }

  public async getFlowFingerprint(sessionId: SessionId): Promise<string | null> {
    return this.stateOf(sessionId).flowFingerprint;
  }

  public async setFlowFingerprint(
    sessionId: SessionId,
    fingerprint: string,
    options: { readonly epoch: Epoch },
  ): Promise<void> {
    const state = this.stateOf(sessionId);
    guardEpoch(state, options.epoch);
    state.flowFingerprint = fingerprint;
  }
}
