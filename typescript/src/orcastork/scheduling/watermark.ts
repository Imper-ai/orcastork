/**
 * Per-operator watermark → change delta.
 *
 * The store carries a monotonic revision; each operator has a watermark (the revision at its last
 * run). The delta since that watermark is what the operator processes on a rerun. The first run (no
 * watermark) presents the whole current set as `added`; a lost watermark degrades safely to
 * first-invocation semantics (sound because operators are idempotent). The caller supplies the
 * watermark — the orchestrator caches them (it is the session's sole watermark writer while it holds
 * the epoch), so the hot loop never re-reads them per operator. Reads go through the narrow
 * {@link DataPointReader} surface, so the orchestrator can hand in its local session mirror instead
 * of the backend store.
 *
 * @module
 */

import type { AnyDataPoint } from '../datapoints/index.js';
import type { CapabilityId, Revision, SessionId } from '../ids.js';
import { InvocationDelta } from '../operators/context.js';
import { ChangeSet } from '../ports/change_set.js';
import type { DataPointReader } from '../ports/datapoint_store.js';

/** Everything {@link computeDelta} folds into one {@link InvocationDelta}. */
export interface ComputeDeltaOptions {
  /** What the store reports changed since the operator's watermark (ignored on a first run). */
  readonly changeSet: ChangeSet;

  /** The session's whole current set — the `added` of a first invocation. */
  readonly currentSet: readonly AnyDataPoint[];

  /** The capabilities that were available at the operator's last run. */
  readonly previousCaps: ReadonlySet<CapabilityId>;

  /** The capabilities available now. */
  readonly availableCaps: ReadonlySet<CapabilityId>;

  /** Whether this is the operator's first invocation this session (no watermark). */
  readonly isFirstInvocation: boolean;
}

/** Build the {@link InvocationDelta} from a change-set (or the full set on first run). */
export const computeDelta = (options: ComputeDeltaOptions): InvocationDelta => {
  const newlyAvailable = [...options.availableCaps].filter((capabilityId) => !options.previousCaps.has(capabilityId));
  if (options.isFirstInvocation) {
    return InvocationDelta({
      added: options.currentSet,
      updated: [],
      newlyAvailableCaps: newlyAvailable,
      isFirstInvocation: true,
    });
  }
  return InvocationDelta({
    added: options.changeSet.added,
    updated: options.changeSet.updated,
    newlyAvailableCaps: newlyAvailable,
    isFirstInvocation: false,
  });
};

/** What {@link operatorDelta} needs beyond the store it reads and the session it reads for. */
export interface OperatorDeltaOptions {
  /** The operator's watermark, or `null` when it has none (first run, or a lost watermark). */
  readonly watermark: Revision | null;

  /** The capabilities available now. */
  readonly availableCaps: ReadonlySet<CapabilityId>;

  /** The capabilities that were available at the operator's last run. */
  readonly previousCaps: ReadonlySet<CapabilityId>;
}

/** Compute an operator's delta from its watermark (`null`/lost → first-invocation). */
export const operatorDelta = async (
  store: DataPointReader,
  sessionId: SessionId,
  options: OperatorDeltaOptions,
): Promise<InvocationDelta> => {
  if (options.watermark === null) {
    const current = (await store.snapshot(sessionId)).all();
    const empty = new ChangeSet({ added: [], updated: [] });
    return computeDelta({
      changeSet: empty,
      currentSet: current,
      previousCaps: options.previousCaps,
      availableCaps: options.availableCaps,
      isFirstInvocation: true,
    });
  }
  const change = await store.changeSetSince(sessionId, options.watermark);
  return computeDelta({
    changeSet: change,
    currentSet: [],
    previousCaps: options.previousCaps,
    availableCaps: options.availableCaps,
    isFirstInvocation: false,
  });
};
