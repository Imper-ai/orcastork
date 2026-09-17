/**
 * The `DataPointStore` port — session-scoped, keyed-merge, monotonic-revision, fenced.
 *
 * Every mutating method is guarded by the session's fencing `epoch`: a write whose epoch is below
 * the highest epoch the store has accepted for the session is rejected atomically with
 * `StaleEpochError` (no partial write).
 *
 * @module
 */

import type { AnyDataPoint, DataPointView } from '../datapoints/index.js';
import type { Epoch, OperatorId, Revision, SessionId } from '../ids.js';
import { Epoch as toEpoch } from '../ids.js';
import type { ChangeSet } from './change_set.js';

/**
 * The stored form of a finished effect — the `fx:{session}` keyspace contract, shared by every
 * adapter.
 */
export const EFFECT_COMMITTED = 'committed';

/** The prefix of an in-flight claim's stored state, which names its owning epoch. */
export const EFFECT_PENDING_PREFIX = 'pending:';

/** The stored form of an in-flight claim owned by `epoch`. */
export const effectPendingState = (epoch: Epoch): string => `${EFFECT_PENDING_PREFIX}${epoch}`;

/** The owning epoch of a `pending:<epoch>` state string, or `null` for any other state. */
export const effectPendingEpoch = (state: string): Epoch | null => {
  if (!state.startsWith(EFFECT_PENDING_PREFIX)) {
    return null;
  }
  const suffix = state.slice(EFFECT_PENDING_PREFIX.length);
  // Corrupt persisted state (a mangled epoch suffix) must not take down claim/recovery — an owner
  // that cannot be parsed is reported as unknown, exactly like a non-pending state.
  if (!/^-?\d+$/.test(suffix)) {
    return null;
  }
  return toEpoch(Number.parseInt(suffix, 10));
};

/** Outcome of {@link DataPointStore.claimEffect} — what the stored state was and who owns it now. */
export const EffectClaim = {
  /** The caller owns the claim and MUST resolve it (commit or revert). */
  ACQUIRED: 'acquired',

  /** The effect ran to completion in some earlier attempt. */
  ALREADY_COMMITTED: 'already_committed',

  /** A duplicate claim within this run — do not run the effect. */
  PENDING_SAME_EPOCH: 'pending_same_epoch',

  /** A predecessor died mid-effect; its outcome is unknown. */
  PENDING_STALE_EPOCH: 'pending_stale_epoch',
} as const;

/** One of the {@link EffectClaim} outcomes. */
export type EffectClaim = (typeof EffectClaim)[keyof typeof EffectClaim];

/** The pre-resolved keyed merge a sole-mutator caller hands {@link DataPointStore.applyResolved}. */
export interface ApplyResolvedOptions {
  /** Identities new to the session, carrying their final field values. */
  readonly added: readonly AnyDataPoint[];

  /** Existing identities, carrying their merged timestamps. */
  readonly updated: readonly AnyDataPoint[];

  readonly epoch: Epoch;
}

/** How a side-effect key is claimed for an epoch. */
export interface ClaimEffectOptions {
  readonly epoch: Epoch;

  /** Whether another epoch's abandoned `pending` mark may be taken over by this one. */
  readonly reclaimStale: boolean;
}

/**
 * The read surface delta computation needs — satisfied by every store and by the orchestrator's
 * sole-mutator session mirror, so readiness/delta logic can be served from local state without
 * touching the backend.
 */
export interface DataPointReader {
  /** A read-only, subtype-aware view of the session's current DataPoints. */
  snapshot(sessionId: SessionId): Promise<DataPointView>;

  /** The session's current monotonic revision (baseline `0` when empty). */
  revision(sessionId: SessionId): Promise<Revision>;

  /** DataPoints added/updated since revision `since`. */
  changeSetSince(sessionId: SessionId, since: Revision): Promise<ChangeSet>;
}

/** The session blackboard plus the session's epoch-fenced meta state. */
export interface DataPointStore extends DataPointReader {
  /**
   * Keyed-merge `dataPoints` into the session and return the resulting revision.
   *
   * Equal `(type, value)` identities merge (bumping `lastRetrieved`) rather than duplicate; the
   * revision advances iff at least one DataPoint was added or freshened.
   *
   * @throws StaleEpochError when the session has already accepted a higher epoch.
   */
  write(
    sessionId: SessionId,
    dataPoints: Iterable<AnyDataPoint>,
    options: { readonly epoch: Epoch },
  ): Promise<Revision>;

  /**
   * Apply a keyed-merge a sole-mutator caller has ALREADY resolved; return the new revision.
   *
   * `added` are identities new to the session carrying their final field values; `updated` are
   * existing identities carrying their merged timestamps. The adapter applies them blindly — no
   * re-read, no merge of its own — atomically and epoch-guarded, stamping per-identity
   * added/updated revisions exactly as {@link DataPointStore.write} would (one new revision for
   * the whole batch, allocated only when the batch is non-empty). A stale epoch is rejected with
   * no partial write, even for an empty batch — fencing semantics are byte-identical to `write`
   * on every path.
   *
   * @throws StaleEpochError when the session has already accepted a higher epoch.
   */
  applyResolved(sessionId: SessionId, options: ApplyResolvedOptions): Promise<Revision>;

  /** The revision at an operator's last run, or `null` if it has never run. */
  getWatermark(sessionId: SessionId, operatorId: OperatorId): Promise<Revision | null>;

  /**
   * Persist an operator's watermark (epoch-guarded session state).
   *
   * @throws StaleEpochError when the session has already accepted a higher epoch.
   */
  setWatermark(
    sessionId: SessionId,
    operatorId: OperatorId,
    revision: Revision,
    options: { readonly epoch: Epoch },
  ): Promise<void>;

  /**
   * Atomically claim a side-effect key for this epoch (epoch-guarded, atomic per key).
   *
   * Absent → stored as `pending:<epoch>`, {@link EffectClaim.ACQUIRED} — the caller owns running
   * the effect and MUST resolve the claim via {@link DataPointStore.commitEffect} or
   * {@link DataPointStore.revertEffect}. `committed` → {@link EffectClaim.ALREADY_COMMITTED}. This
   * epoch's own pending → {@link EffectClaim.PENDING_SAME_EPOCH}. Another epoch's pending (a
   * predecessor died mid-effect, outcome unknown) → overwritten to `pending:<epoch>` and
   * {@link EffectClaim.ACQUIRED} when `reclaimStale` is true, else
   * {@link EffectClaim.PENDING_STALE_EPOCH} with the stale mark left in place.
   *
   * Marks live alongside the session's other state and share its lifetime — they are read and
   * written outside the DataPoint merge path (no revision/change-set interplay).
   *
   * @throws StaleEpochError when the session has already accepted a higher epoch.
   */
  claimEffect(sessionId: SessionId, effectKey: string, options: ClaimEffectOptions): Promise<EffectClaim>;

  /**
   * Transition this epoch's `pending:<epoch>` mark to `committed` (epoch-guarded).
   *
   * Idempotent over `committed`. Any other state (absent, or another epoch's pending) is left
   * untouched — a commit must never fabricate `committed` for an effect this epoch does not own.
   *
   * @throws StaleEpochError when the session has already accepted a higher epoch.
   */
  commitEffect(sessionId: SessionId, effectKey: string, options: { readonly epoch: Epoch }): Promise<void>;

  /**
   * Delete ONLY a `pending:<epoch>` mark owned by this epoch (epoch-guarded).
   *
   * Never deletes `committed` (the effect DID run) and never deletes another epoch's pending (that
   * claim is resolved by its owner, or by a successor's recovery policy).
   *
   * @throws StaleEpochError when the session has already accepted a higher epoch.
   */
  revertEffect(sessionId: SessionId, effectKey: string, options: { readonly epoch: Epoch }): Promise<void>;

  /** The stored effect state (`pending:<epoch>` or `committed`), or `null` if never claimed. */
  getEffectState(sessionId: SessionId, effectKey: string): Promise<string | null>;

  /** The session's persisted wall-clock completion deadline, or `null` if never set. */
  getSessionDeadline(sessionId: SessionId): Promise<Date | null>;

  /**
   * Persist the session's wall-clock deadline (epoch-guarded session state).
   *
   * Written once at the session's first gather and rehydrated by every later run, so the overall
   * budget keeps shrinking across parks, crashes and resumes — a process restart must never grant
   * a fresh full deadline window.
   *
   * @throws StaleEpochError when the session has already accepted a higher epoch.
   */
  setSessionDeadline(sessionId: SessionId, deadline: Date, options: { readonly epoch: Epoch }): Promise<void>;

  /** The flow fingerprint persisted for the session, or `null` if never set. */
  getFlowFingerprint(sessionId: SessionId): Promise<string | null>;

  /**
   * Persist the session's flow fingerprint (epoch-guarded session state).
   *
   * Written at spawn and rewritten whenever drift is detected, so every later resume compares
   * against the *latest* flow that drove the session — repeated resumes with the same changed flow
   * stay quiet.
   *
   * @throws StaleEpochError when the session has already accepted a higher epoch.
   */
  setFlowFingerprint(sessionId: SessionId, fingerprint: string, options: { readonly epoch: Epoch }): Promise<void>;
}
