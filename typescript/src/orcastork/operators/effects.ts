/**
 * `EffectGuard` — the claim/commit/revert gate for non-idempotent operator side effects.
 *
 * Everything else the engine repeats is safe to repeat: emissions keyed-merge, archive writes
 * keyed-upsert, aggregator outputs are OCC-guarded and contribution-marked. A *side effect* (sending
 * an OTP, opening an ITSM ticket) is not — and gathering operators are deliberately re-driven:
 * retried on failure, rerun on new data, and re-run on crash-resume.
 * `await ctx.once(key, async (acquired) => { … })` is the explicit guard such effects need: entering
 * claims the `(operator, key)` durably, the caller performs the effect iff `acquired` is `true`, a
 * clean exit commits the claim, and a failing exit reverts it — so a retry of a failed attempt
 * re-runs the effect instead of skipping it, while a committed effect never fires again across
 * reruns, retries and resumes.
 *
 * Sole-mutator nuance: effect marks are written from INSIDE operator tasks, concurrent with the
 * gathering loop's own store writes. This does not violate the sole-mutator invariant — the marks
 * live in a separate keyspace from the DataPoint merge path (no revision, change-set, or watermark
 * interplay), each transition is a single-key atomic step, and every write is epoch-fenced like any
 * other, so a fenced predecessor's mark is rejected, never half-applied.
 *
 * **Where the port differs from Python, and why.** Python's guard is an async context manager
 * (`async with ctx.once(key) as acquired:`). JavaScript has no such construct, so the block becomes
 * a callback: `once(key, body)` runs `body(acquired)` exactly where the `async with` body would run,
 * with the same claim before it and the same commit/revert after it. The body runs whether or not
 * this attempt acquired the claim — `acquired` is a parameter, not a gate — because that is what the
 * Python block does, and an attempt that did not acquire must be able to fail on its own unrelated
 * work without touching the owner's mark.
 *
 * @module
 */

import type { Epoch, OperatorId, SessionId } from '../ids.js';
import { getLogger } from '../logging.js';
import type { DataPointStore } from '../ports/datapoint_store.js';
import { EffectClaim, effectPendingEpoch } from '../ports/datapoint_store.js';

/** The module a log record names as its origin, so the OTel bridge can filter on it. */
const LOGGER_NAME = 'orcastork.operators.effects';

/**
 * Policy for a predecessor's mid-effect crash — its `pending` mark survives under a stale epoch, so
 * whether the effect actually happened is unknowable.
 */
export const EffectRecovery = {
  /**
   * Reclaim and re-run: the framework's at-least-once posture everywhere else (inbox redelivery,
   * operator re-drives), accepting a possible duplicate over a possibly-lost effect.
   */
  RERUN: 'rerun',

  /**
   * Skip, leaving the stale mark in place: a later resume then sees the same unknown state and
   * applies its own policy, rather than a fabricated 'committed'.
   */
  SKIP: 'skip',
} as const;

/** One of the {@link EffectRecovery} policies. */
export type EffectRecovery = (typeof EffectRecovery)[keyof typeof EffectRecovery];

/** What an {@link EffectGuard} is bound to — everything but the store itself. */
export interface EffectGuardOptions {
  readonly sessionId: SessionId;

  readonly operatorId: OperatorId;

  readonly epoch: Epoch;
}

/** How one guarded effect treats a predecessor's unresolved claim. */
export interface OnceOptions {
  /** Defaults to {@link EffectRecovery.RERUN}. */
  readonly onUnknown?: EffectRecovery;
}

/**
 * The guarded block: the port of the `async with ctx.once(key) as acquired:` body.
 *
 * `acquired` is `true` iff this attempt owns running the effect. The block runs either way, so a
 * caller that only wants the effect branches on it.
 */
export type EffectBody = (acquired: boolean) => Promise<void> | void;

/** Bound to one `(store, sessionId, operatorId, epoch)`; handed to operators via the context. */
export class EffectGuard {
  private readonly store: DataPointStore;
  private readonly sessionId: SessionId;
  private readonly operatorId: OperatorId;
  private readonly epoch: Epoch;

  public constructor(store: DataPointStore, options: EffectGuardOptions) {
    this.store = store;
    this.sessionId = options.sessionId;
    this.operatorId = options.operatorId;
    this.epoch = options.epoch;
  }

  /**
   * Guard a non-idempotent side effect: `acquired` is `true` iff this attempt owns running it.
   *
   * The stored key is namespaced as `{operatorId}:{effectKey}`, so two operators using the same key
   * can never collide. Entering claims the key (`onUnknown` decides what a predecessor's mid-effect
   * crash means); a clean exit commits; any exception thrown by the body reverts the claim and
   * propagates, so the loop-scheduled retry of this attempt re-enters with `true` and the effect
   * actually runs.
   *
   * Returns whether this attempt acquired the claim — the same value the body was handed, for a
   * caller that would rather branch after the block than inside it. A body that threw never returns
   * a value: the error propagates, as it does out of Python's `async with`.
   */
  public async once(effectKey: string, body: EffectBody, options: OnceOptions = {}): Promise<boolean> {
    const namespaced = `${this.operatorId}:${effectKey}`;
    if (!(await this.claim(namespaced, options.onUnknown ?? EffectRecovery.RERUN))) {
      // A non-owning attempt still runs its block (Python yields `False` and returns) and must never
      // resolve a claim it does not own — including when that block throws, which propagates from
      // here untouched.
      await body(false);
      return false;
    }
    try {
      await body(true);
    } catch (error) {
      // In Python the revert often runs during cancellation unwind (a deadline/timeout cancelling
      // the operator task) and is shielded so a further cancellation cannot interrupt it mid-flight.
      // A promise cannot be cancelled, so a plain await is the port — but the reason the call must
      // run to completion is unchanged.
      try {
        await this.store.revertEffect(this.sessionId, namespaced, { epoch: this.epoch });
      } catch (revertError) {
        // Rare double failure: the effect failed AND the revert failed, so the mark stays pending
        // under this epoch and a same-epoch retry sees PENDING_SAME_EPOCH → false. We prefer a
        // possibly-skipped effect over a possibly-double-fired one within a single epoch; a
        // cross-epoch resume still gets the RERUN/SKIP recovery policy.
        getLogger().warning('Effect claim revert failed; a same-epoch retry will skip this effect', {
          logger_name: LOGGER_NAME,
          session_id: this.sessionId,
          effect_key: namespaced,
          error: revertError,
        });
      }
      throw error;
    }
    // The commit can also run during cancellation unwind in Python (the body finishes right as a
    // deadline/timeout cancels the operator task) — shielded like the revert, or the claim is
    // stranded as `pending:<epoch>` and a same-epoch retry SKIPS an effect that DID run.
    try {
      await this.store.commitEffect(this.sessionId, namespaced, { epoch: this.epoch });
    } catch (commitError) {
      // Rare double failure, mirroring the revert path: the effect ran AND the commit failed, so the
      // mark stays pending under this epoch and a same-epoch retry sees PENDING_SAME_EPOCH → false.
      // We prefer a possibly-skipped effect over a possibly-double-fired one within a single epoch;
      // a cross-epoch resume still gets the RERUN/SKIP recovery policy.
      getLogger().warning(
        'Effect commit failed; the claim stays pending and a same-epoch retry will skip this effect',
        {
          logger_name: LOGGER_NAME,
          session_id: this.sessionId,
          effect_key: namespaced,
          error: commitError,
        },
      );
    }
    return true;
  }

  private async claim(namespaced: string, onUnknown: EffectRecovery): Promise<boolean> {
    const claim = await this.store.claimEffect(this.sessionId, namespaced, {
      epoch: this.epoch,
      reclaimStale: false,
    });
    switch (claim) {
      case EffectClaim.ACQUIRED:
        return true;
      case EffectClaim.ALREADY_COMMITTED:
      case EffectClaim.PENDING_SAME_EPOCH:
        return false;
      case EffectClaim.PENDING_STALE_EPOCH:
        return await this.recover(namespaced, onUnknown);
      default: {
        const unreachable: never = claim;
        return unreachable;
      }
    }
  }

  private async recover(namespaced: string, onUnknown: EffectRecovery): Promise<boolean> {
    switch (onUnknown) {
      case EffectRecovery.SKIP:
        // The stale mark is deliberately left in place: a later resume must see the same unknown
        // state and apply its own policy, not a fabricated outcome.
        return false;
      case EffectRecovery.RERUN: {
        const staleState = await this.store.getEffectState(this.sessionId, namespaced);
        getLogger().warning(
          'Stale pending effect mark found: the outcome of a predecessor attempt is unknown; re-running',
          {
            logger_name: LOGGER_NAME,
            session_id: this.sessionId,
            effect_key: namespaced,
            stale_epoch: staleState === null ? null : effectPendingEpoch(staleState),
            epoch: this.epoch,
          },
        );
        const reclaim = await this.store.claimEffect(this.sessionId, namespaced, {
          epoch: this.epoch,
          reclaimStale: true,
        });
        // A concurrent commit may have raced the reclaim; anything but ACQUIRED means the effect must
        // not run here.
        return reclaim === EffectClaim.ACQUIRED;
      }
      default: {
        const unreachable: never = onUnknown;
        return unreachable;
      }
    }
  }
}
