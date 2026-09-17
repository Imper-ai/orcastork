/**
 * Capability availability (a fixpoint) and lazy activation.
 *
 * A capability is **available** iff (1) it is namespace-permitted (catalog), (2) all its
 * `dependsOn` DataPoints are present (subtype-aware), and (3) all its `requires` capabilities are
 * available. Condition (3) makes availability a fixpoint (layered capabilities need their base
 * first), computed base-before-layer until stable. Because `dependsOn` is presence-based and
 * DataPoints are only ever added within a session, availability is **monotonic** (the only way to
 * lose a capability is a namespace-config change).
 *
 * A newly-available capability is **activated lazily** — constructed from catalog-supplied
 * credentials the first time it becomes available — so never-needed capabilities are never
 * constructed. A failed activation is re-attempted on a later `refresh` once a jittered cool-off
 * (the aggregation retry schedule, seeded on the capability id) has elapsed; after `maxAttempts`
 * total failures it is terminal for the session and never retried again.
 *
 * @module
 */

import { backoffDelays, RetryPolicy, seedFor } from '../aggregation/retry.js';
import type { Clock } from '../clock.js';
import type { AnyDataPoint, DataPointClass, DataPointView } from '../datapoints/index.js';
import { isSubclass } from '../datapoints/index.js';
import { OrchestrationError } from '../exceptions.js';
import type { CapabilityId, NamespaceId } from '../ids.js';
import { getLogger } from '../logging.js';
import { CapabilityView } from '../operators/context.js';
import type { CapabilityCatalog } from '../ports/capability_catalog.js';
import type { RateLimiter } from '../ports/rate_limiter.js';
import { Telemetry, withSpan } from '../telemetry.js';
import type { Capability, CapabilityClass, ConcreteCapabilityClass, InvocationAuditor } from './base.js';
import { CapabilityContext } from './base.js';

/** The module a log record names as its origin, so the OTel bridge can filter on it. */
const LOGGER_NAME = 'orcastork.capabilities.availability';

/** Notified exactly once when a capability's activation retries are exhausted for the session. */
export type OnTerminalFailure = (capabilityId: CapabilityId, error: unknown) => Promise<void>;

/** One capability's activation-failure bookkeeping for this session. */
interface ActivationFailure {
  /** Total failed activation attempts so far. */
  readonly attempts: number;

  /** Monotonic time (ms) the next attempt becomes eligible; `null` → terminal. */
  readonly nextAttemptAt: number | null;
}

const dependsOnOf = (capability: ConcreteCapabilityClass): readonly DataPointClass<AnyDataPoint>[] =>
  capability.dependsOn ?? [];

const requiresOf = (capability: ConcreteCapabilityClass): readonly CapabilityClass[] => capability.requires ?? [];

const depsPresent = (
  dependsOn: readonly DataPointClass<AnyDataPoint>[],
  presentTypes: ReadonlySet<DataPointClass<AnyDataPoint>>,
): boolean => dependsOn.every((required) => [...presentTypes].some((present) => isSubclass(present, required)));

const requiresAvailable = (
  requires: readonly CapabilityClass[],
  available: Iterable<CapabilityId>,
  registered: ReadonlyMap<CapabilityId, ConcreteCapabilityClass>,
): boolean => {
  const availableTypes = [...available]
    .map((availableId) => registered.get(availableId))
    .filter((provider): provider is ConcreteCapabilityClass => provider !== undefined);
  return requires.every((required) => availableTypes.some((provider) => isSubclass(provider, required)));
};

/** What {@link computeAvailable} needs to decide availability (Python's keyword-only arguments). */
export interface ComputeAvailableOptions {
  readonly registered: ReadonlyMap<CapabilityId, ConcreteCapabilityClass>;

  readonly permitted: ReadonlySet<CapabilityId>;

  /** The classes of the DataPoints currently in the session. */
  readonly presentTypes: ReadonlySet<DataPointClass<AnyDataPoint>>;
}

/** The set of currently-available capabilities (least fixpoint, base-before-layer). */
export const computeAvailable = (options: ComputeAvailableOptions): ReadonlySet<CapabilityId> => {
  const { registered, permitted, presentTypes } = options;
  const available = new Set<CapabilityId>();
  let changed = true;
  while (changed) {
    changed = false;
    for (const [capabilityId, capability] of registered) {
      if (available.has(capabilityId) || !permitted.has(capabilityId)) {
        continue;
      }
      if (!depsPresent(dependsOnOf(capability), presentTypes)) {
        continue;
      }
      if (!requiresAvailable(requiresOf(capability), available, registered)) {
        continue;
      }
      available.add(capabilityId);
      changed = true;
    }
  }
  return available;
};

/** Everything the activator takes beyond the four it cannot work without. */
export interface CapabilityActivatorOptions {
  /** Threaded into every {@link CapabilityView} so invocations are audited. */
  readonly onInvoke?: InvocationAuditor | null;

  /** The activation cool-off schedule; defaults to the shared {@link RetryPolicy} defaults. */
  readonly activationRetry?: RetryPolicy | null;

  readonly onTerminalFailure?: OnTerminalFailure | null;

  /** Bound per activation so every action paces fleet-wide. */
  readonly rateLimiter?: RateLimiter | null;

  readonly telemetry?: Telemetry | null;
}

/**
 * Tracks lazy activation across a session and produces the current {@link CapabilityView}.
 *
 * Activated instances are cached, so availability is monotonic and a capability is built at most
 * once. `refresh` recomputes availability from the present DataPoints + the current namespace
 * catalog and activates any newly-available capability from its credentials. A failed activation
 * cools off (jittered backoff against the injected clock — eligibility is checked, never awaited)
 * and is re-attempted by a later `refresh`; after `activationRetry.maxAttempts` total failures it is
 * terminal for the session.
 */
export class CapabilityActivator {
  private readonly registered: ReadonlyMap<CapabilityId, ConcreteCapabilityClass>;
  private readonly catalog: CapabilityCatalog;
  private readonly namespaceId: NamespaceId;
  private readonly clock: Clock;
  private readonly onInvoke: InvocationAuditor | null;
  private readonly activationRetry: RetryPolicy;
  private readonly onTerminalFailure: OnTerminalFailure | null;
  private readonly rateLimiter: RateLimiter | null;
  private readonly telemetry: Telemetry;
  private readonly activated = new Map<CapabilityId, Capability>();
  private readonly failures = new Map<CapabilityId, ActivationFailure>();

  public constructor(
    registered: Iterable<readonly [CapabilityId, ConcreteCapabilityClass]>,
    catalog: CapabilityCatalog,
    namespaceId: NamespaceId,
    clock: Clock,
    options: CapabilityActivatorOptions = {},
  ) {
    this.registered = new Map(registered);
    this.catalog = catalog;
    this.namespaceId = namespaceId;
    this.clock = clock;
    this.onInvoke = options.onInvoke ?? null;
    this.activationRetry = options.activationRetry ?? RetryPolicy();
    this.onTerminalFailure = options.onTerminalFailure ?? null;
    this.rateLimiter = options.rateLimiter ?? null;
    this.telemetry = options.telemetry ?? new Telemetry();
  }

  /** The capabilities built so far — an instance survives a revocation, as an in-flight user does. */
  public activatedIds(): ReadonlySet<CapabilityId> {
    return new Set(this.activated.keys());
  }

  /** Recompute availability, activate what is newly available, and hand back the current view. */
  public async refresh(storeView: DataPointView): Promise<CapabilityView> {
    const presentTypes = new Set<DataPointClass<AnyDataPoint>>(
      storeView.all().map((dataPoint) => dataPoint.constructor as DataPointClass<AnyDataPoint>),
    );
    const permitted = await this.catalog.permittedCapabilities(this.namespaceId);
    const preference = await this.catalog.preferredOrder(this.namespaceId);
    const availableIds = computeAvailable({ registered: this.registered, permitted, presentTypes });
    // Activate newly-available capabilities base-before-layer: a capability is activated only once
    // all the capabilities it requires are already activated (the fixpoint guarantees this
    // terminates). Previously-failed capabilities re-enter once their cool-off has elapsed;
    // terminally-failed ones never do.
    const now = this.clock.monotonic();
    let pending = [...availableIds].filter((id) => !this.activated.has(id) && this.mayAttempt(id, now));
    while (pending.length > 0) {
      const ready = pending.filter((id) =>
        requiresAvailable(requiresOf(this.classOf(id)), this.activated.keys(), this.registered),
      );
      if (ready.length === 0) {
        break;
      }
      pending = pending.filter((id) => !ready.includes(id));
      for (const capabilityId of ready) {
        await this.activateOne(capabilityId, storeView);
      }
    }
    // Only currently-available activated capabilities are exposed for new resolution (revocation
    // blocks new acquisitions; in-flight users are not cancelled).
    const availableInstances = new Map<CapabilityId, Capability>();
    for (const capabilityId of availableIds) {
      const instance = this.activated.get(capabilityId);
      if (instance !== undefined) {
        availableInstances.set(capabilityId, instance);
      }
    }
    return new CapabilityView(availableInstances, preference);
  }

  private classOf(capabilityId: CapabilityId): ConcreteCapabilityClass {
    const capability = this.registered.get(capabilityId);
    if (capability === undefined) {
      // Unreachable: every id considered comes from the registered map this activator was built
      // with, so this is the port of the `KeyError` Python's `self._registered[capability_id]`
      // would raise — not a bare `Error`, which core never throws.
      throw new OrchestrationError(`capability ${capabilityId} is not registered with this activator`);
    }
    return capability;
  }

  private mayAttempt(capabilityId: CapabilityId, now: number): boolean {
    const failure = this.failures.get(capabilityId);
    if (failure === undefined) {
      return true;
    }
    return failure.nextAttemptAt !== null && now >= failure.nextAttemptAt;
  }

  private async activateOne(capabilityId: CapabilityId, storeView: DataPointView): Promise<void> {
    const capability = new (this.classOf(capabilityId))();
    capability.bindAuditor(this.onInvoke); // every action call on this instance now audits itself
    // The bucket is per (namespace, capability): one namespace's fleet of sessions shares the budget
    // for a provider, while other namespaces and other providers are unaffected.
    capability.bindRateLimit(this.rateLimiter, `${this.namespaceId}:${capabilityId}`);
    capability.bindTelemetry(this.telemetry); // action spans ride the audited seam
    // Defensive copy so an adapter can't mutate catalog-owned credentials.
    const credentials = { ...((await this.catalog.credentials(this.namespaceId, capabilityId)) ?? {}) };
    try {
      // A raised activation error propagates through the span (which records it); the catch below
      // owns the cool-off bookkeeping.
      await withSpan(
        this.telemetry.tracer,
        `capability.activate ${capabilityId}`,
        async () => await capability.activate(new CapabilityContext({ credentials, store: storeView })),
        { attributes: { capability_id: capabilityId } },
      );
    } catch (error) {
      // A failed activation is isolated like an operator failure: the capability stays unavailable
      // and the session continues; a later refresh re-attempts it after the cool-off, until the
      // retry budget is exhausted.
      await this.recordFailure(capabilityId, error);
      return;
    }
    this.failures.delete(capabilityId);
    this.activated.set(capabilityId, capability);
    this.telemetry.capabilityActivationsTotal.add(1, { capability_id: capabilityId, outcome: 'succeeded' });
  }

  private async recordFailure(capabilityId: CapabilityId, error: unknown): Promise<void> {
    const previous = this.failures.get(capabilityId);
    const attempts = previous === undefined ? 1 : previous.attempts + 1;
    if (attempts >= this.activationRetry.maxAttempts) {
      this.failures.set(capabilityId, { attempts, nextAttemptAt: null });
      this.telemetry.capabilityActivationsTotal.add(1, { capability_id: capabilityId, outcome: 'terminal' });
      getLogger().warning('Capability activation failed terminally; the capability is unavailable for this session', {
        logger_name: LOGGER_NAME,
        capability_id: capabilityId,
        attempts,
        error,
      });
      if (this.onTerminalFailure !== null) {
        await this.onTerminalFailure(capabilityId, error);
      }
      return;
    }
    // The cool-off schedule is the shared jittered backoff, seeded on the capability id so it is
    // deterministic (and reproducible across processes) without coordinating any extra state.
    const coolOffMs = backoffDelays(this.activationRetry, { seed: seedFor(capabilityId) })[attempts - 1] ?? 0;
    this.failures.set(capabilityId, { attempts, nextAttemptAt: this.clock.monotonic() + coolOffMs });
    this.telemetry.capabilityActivationsTotal.add(1, { capability_id: capabilityId, outcome: 'failed' });
    getLogger().warning('Capability activation failed; it will be re-attempted after a cool-off', {
      logger_name: LOGGER_NAME,
      capability_id: capabilityId,
      attempts,
      cool_off_ms: coolOffMs,
      error,
    });
  }
}
