/**
 * The per-invocation `OperatorContext` and its change `InvocationDelta`.
 *
 * Every `run(ctx)` receives the current state (`store`), the currently-available capabilities
 * (`capabilities`), and a per-operator `delta` of what changed since *this* operator last ran — so a
 * rerun does incremental work instead of re-scanning.
 *
 * @module
 */

import type { AggregationHelpers } from '../aggregation/helpers.js';
import type { Capability, CapabilityClass } from '../capabilities/base.js';
import type { AnyDataPoint, DataPointClass, DataPointView } from '../datapoints/index.js';
import { CapabilityUnavailableError } from '../exceptions.js';
import type { CapabilityId, Epoch, SessionId } from '../ids.js';
import type { EffectBody, EffectGuard, OnceOptions } from './effects.js';

/**
 * What changed since *this* operator last ran — so a rerun does incremental work.
 *
 * A frozen plain object, not a validated model: the loop is its only constructor and builds one per
 * rerun-eligible operator on every pass, so validating DataPoints against types the engine itself
 * just computed would cost the hot path for nothing.
 */
export interface InvocationDelta {
  /** New `(type, value)` identities. */
  readonly added: ReadonlySet<AnyDataPoint>;

  /** Existing identities re-observed (`lastRetrieved` bumped). */
  readonly updated: ReadonlySet<AnyDataPoint>;

  /** Capabilities that came online since the last run. */
  readonly newlyAvailableCaps: ReadonlySet<CapabilityId>;

  /** First run → `added` is the full current set. */
  readonly isFirstInvocation: boolean;
}

/** What an {@link InvocationDelta} is built from; the sets are copied. */
export interface InvocationDeltaInit {
  readonly added: Iterable<AnyDataPoint>;
  readonly updated: Iterable<AnyDataPoint>;
  readonly newlyAvailableCaps: Iterable<CapabilityId>;
  readonly isFirstInvocation: boolean;
}

/** Build an {@link InvocationDelta} — the orchestrator's job, and no-one else's. */
export const InvocationDelta = (init: InvocationDeltaInit): InvocationDelta =>
  Object.freeze({
    added: new Set(init.added),
    updated: new Set(init.updated),
    newlyAvailableCaps: new Set(init.newlyAvailableCaps),
    isFirstInvocation: init.isFirstInvocation,
  });

/** Read view over the currently-available capabilities (resolve → preferred provider). */
export class CapabilityView {
  private readonly available: ReadonlyMap<CapabilityId, Capability>;
  private readonly preference: readonly CapabilityId[];

  public constructor(
    available: ReadonlyMap<CapabilityId, Capability> = new Map(),
    preference: readonly CapabilityId[] = [],
  ) {
    this.available = new Map(available);
    this.preference = [...preference];
  }

  public availableIds(): ReadonlySet<CapabilityId> {
    return new Set(this.available.keys());
  }

  public availableTypes(): ReadonlySet<CapabilityClass> {
    return new Set([...this.available.values()].map((capability) => capability.constructor as CapabilityClass));
  }

  public isAvailable(capabilityId: CapabilityId): boolean {
    return this.available.has(capabilityId);
  }

  /**
   * The single preferred available provider of `capabilityType`, or `null`.
   *
   * Providers listed in the namespace's preference order win, in listed order; unlisted providers
   * rank after every listed one and fall back to the stable alphabetical tie-break by id.
   *
   * Call the returned capability's action methods directly — they keep their real typed signatures,
   * and each call audits itself (the framework wires the auditor when the capability activates).
   */
  public resolve<C extends Capability>(capabilityType: CapabilityClass<C>): C | null {
    const matches = [...this.available.values()].filter(
      (capability): capability is C => capability instanceof capabilityType,
    );
    matches.sort((left, right) => this.compareByPreference(left, right));
    return matches[0] ?? null;
  }

  /**
   * Like {@link CapabilityView.resolve}, but throw `CapabilityUnavailableError` when no provider is
   * available.
   *
   * An operator that declared the capability in `requires` is only scheduled once it is available,
   * so it can `require` the provider and use it without a `null` check.
   */
  public require<C extends Capability>(capabilityType: CapabilityClass<C>): C {
    const capability = this.resolve(capabilityType);
    if (capability === null) {
      throw new CapabilityUnavailableError(`no available provider for ${capabilityType.name}`);
    }
    return capability;
  }

  private compareByPreference(left: Capability, right: Capability): number {
    const ranks = this.preferenceRank(left) - this.preferenceRank(right);
    if (ranks !== 0) {
      return ranks;
    }
    // Code-unit ordering, not locale ordering: the tie-break must not depend on the host.
    if (left.capabilityId === right.capabilityId) {
      return 0;
    }
    return left.capabilityId < right.capabilityId ? -1 : 1;
  }

  private preferenceRank(capability: Capability): number {
    const index = this.preference.indexOf(capability.capabilityId);
    return index === -1 ? this.preference.length : index;
  }
}

/** What an {@link OperatorContext} is built from — everything one invocation may see. */
export interface OperatorContextInit {
  readonly sessionId: SessionId;

  readonly epoch: Epoch;

  readonly store: DataPointView;

  readonly capabilities: CapabilityView;

  readonly delta: InvocationDelta;

  /** Claim/commit/revert gate for non-idempotent side effects (see `operators/effects.ts`). */
  readonly effects: EffectGuard;

  /** Aborted when the run is cut short — its timeout, or the session deadline. */
  readonly signal: AbortSignal;

  /** Set only for aggregators (the durable-write API); `null` for a gathering operator. */
  readonly aggregation?: AggregationHelpers | null;

  /** True only during the authoritative finalize pass; false for interim runs. */
  readonly isFinal?: boolean;
}

/**
 * What one invocation sees: the session's DataPoints, its capabilities, and what is new.
 *
 * Built by the orchestrator and frozen, never validated: the engine would only be checking itself,
 * once per launched run. `signal` has no Python counterpart — it is the port of the cancellation
 * `asyncio` gives Python for free: a promise cannot be cancelled, so a run that must stop (its
 * timeout fired, or the session deadline passed) is told through the signal, and anything genuinely
 * in flight inside `run` should be tied to it.
 */
export class OperatorContext {
  public readonly sessionId: SessionId;

  public readonly epoch: Epoch;

  /** Every DataPoint the session holds, not only this operator's dependencies. */
  public readonly store: DataPointView;

  public readonly capabilities: CapabilityView;

  /** What changed since this operator last ran. */
  public readonly delta: InvocationDelta;

  public readonly effects: EffectGuard;

  /** Aborted when this run is cut short; tie anything long-running to it. */
  public readonly signal: AbortSignal;

  /** The durable-write API — set only for aggregators. */
  public readonly aggregation: AggregationHelpers | null;

  /** True only during the authoritative finalize pass; false for interim runs. */
  public readonly isFinal: boolean;

  public constructor(init: OperatorContextInit) {
    this.sessionId = init.sessionId;
    this.epoch = init.epoch;
    this.store = init.store;
    this.capabilities = init.capabilities;
    this.delta = init.delta;
    this.effects = init.effects;
    this.signal = init.signal;
    this.aggregation = init.aggregation ?? null;
    this.isFinal = init.isFinal ?? false;
    Object.freeze(this);
  }

  /** The newest DataPoint of `dataPointType` by `lastRetrieved` (ergonomic single-read). */
  public latest<T extends AnyDataPoint>(dataPointType: DataPointClass<T>): T | null {
    return this.store.latest(dataPointType);
  }

  /**
   * Guard a non-idempotent side effect — `await ctx.once('send-otp', async (acquired) => { … })`.
   *
   * `acquired` is `true` iff this attempt owns running the effect; a clean exit commits the claim
   * durably, a body that throws reverts it so a retry re-runs the effect.
   */
  public async once(effectKey: string, body: EffectBody, options: OnceOptions = {}): Promise<boolean> {
    return await this.effects.once(effectKey, body, options);
  }
}
