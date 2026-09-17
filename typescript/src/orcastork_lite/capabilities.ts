/**
 * `Capability` — an injected provider of typed actions — and how it reaches an operator.
 *
 * A capability declares the DataPoints it needs (`dependsOn`) and the capabilities it builds on
 * (`requires`). It is **available** iff it is namespace-permitted ({@link CapabilityCatalog}), its
 * `dependsOn` are present (subtype-aware) and its `requires` are available — a fixpoint that
 * activates layered capabilities base-before-layer. A newly-available capability is **activated
 * lazily**, once, from catalog-supplied credentials; one that never becomes available is never
 * constructed. Operators get the activated instance from `ctx.capabilities` and call its methods
 * directly, fully typed.
 *
 * @module
 */

import type { DataPointClass, DataPointView } from './datapoints.js';
import { isSubclass } from './datapoints.js';
import { CapabilityUnavailableError } from './exceptions.js';
import type { CapabilityId, NamespaceId, OperatorId } from './ids.js';
import { withTimeout } from './internal/timeouts.js';
import { getLogger } from './logging.js';

/** The secrets a capability is built from — whatever the deployment's catalog stores for it. */
export type Credentials = Readonly<Record<string, unknown>>;

/**
 * What a capability declares about itself, as `static readonly` fields on the class:
 *
 * ```ts
 * class SearchApi extends Capability {
 *   static readonly capabilityId = CapabilityId('search_api');
 *   static readonly dependsOn = [Tenant];
 *   static readonly requires = [HttpClient];
 *   async activate(ctx: CapabilityContext): Promise<void> { … }
 * }
 * ```
 *
 * They are declared here rather than on {@link Capability} itself so that a subclass setting one
 * does not have to write `static override` for a field the base only holds a default of — the
 * engine reads an absent `dependsOn`/`requires` as empty, exactly as Python's class defaults do.
 */
export interface CapabilityStatics {
  /** Identity of this provider; two capabilities in one orchestrator may not share it. */
  readonly capabilityId: CapabilityId;

  /** DataPoint types that must be present (subtype-aware) before this can be built. */
  readonly dependsOn?: readonly DataPointClass[];

  /** Capabilities this one builds on; they activate first. */
  readonly requires?: readonly CapabilityClass[];

  /** Every class has one; the framework uses it in error messages and logs. */
  readonly name: string;
}

/**
 * A capability class as the framework handles it — Python's `type[Capability]`.
 *
 * Abstract on purpose: a `requires` entry or a `resolve` argument may name a provider *family*
 * that no instance is ever built from, exactly as an abstract Python class can.
 */
export type CapabilityClass<T extends Capability = Capability> = CapabilityStatics & (abstract new () => T);

/** A constructible capability class: what an orchestrator registers and the activator builds. */
export type ConcreteCapabilityClass<T extends Capability = Capability> = CapabilityStatics & (new () => T);

/** The fields a {@link CapabilityContext} is built from. */
export interface CapabilityContextInit {
  readonly namespaceId: NamespaceId;
  readonly credentials: Credentials;
  readonly store: DataPointView;
}

/**
 * What a capability sees when it activates: whose session it is, its credentials, the DataPoints.
 *
 * `namespaceId` is there so an implementation can key a client cache across sessions of the same
 * namespace (one authenticated HTTP client per tenant, say) instead of rebuilding it per session.
 */
export class CapabilityContext {
  public readonly namespaceId: NamespaceId;

  public readonly credentials: Credentials;

  public readonly store: DataPointView;

  public constructor(init: CapabilityContextInit) {
    this.namespaceId = init.namespaceId;
    this.credentials = init.credentials;
    this.store = init.store;
    Object.freeze(this);
  }
}

/** An injected provider of typed actions; subclass it, declare {@link CapabilityStatics}, add methods. */
export abstract class Capability {
  /** This provider's id, read off the class — the counterpart of Python's `self.capability_id`. */
  public get capabilityId(): CapabilityId {
    return (this.constructor as unknown as CapabilityStatics).capabilityId;
  }

  /** Build the underlying client from catalog-supplied credentials (called at most once per session). */
  public abstract activate(ctx: CapabilityContext): Promise<void>;
}

/** Per-namespace configuration: what a namespace may run, and the secrets its capabilities need. */
export interface CapabilityCatalog {
  /** The capabilities the namespace is permitted to use. */
  permittedCapabilities(namespaceId: NamespaceId): Promise<ReadonlySet<CapabilityId>>;

  /**
   * The operators the namespace may run, or `null` when it declares no restriction.
   *
   * `null` — not an empty set — is the unrestricted default, because an empty set means the
   * namespace may run *nothing*, and conflating "unconfigured" with "deny everything" would
   * silently disable every unconfigured namespace.
   */
  permittedOperators(namespaceId: NamespaceId): Promise<ReadonlySet<OperatorId> | null>;

  /** Credentials for a capability, or `null` if not configured. */
  credentials(namespaceId: NamespaceId, capabilityId: CapabilityId): Promise<Credentials | null>;

  /** The namespace's provider preference (may be empty); listed providers resolve first, in order. */
  preferredOrder(namespaceId: NamespaceId): Promise<readonly CapabilityId[]>;
}

/** One namespace's credentials for one capability, as {@link InMemoryCapabilityCatalog} takes them. */
export interface CredentialEntry {
  readonly namespaceId: NamespaceId;
  readonly capabilityId: CapabilityId;
  readonly credentials: Credentials;
}

/** What {@link InMemoryCapabilityCatalog} is seeded with; every part is optional and defaults to empty. */
export interface InMemoryCapabilityCatalogInit {
  readonly permitted?: Iterable<readonly [NamespaceId, Iterable<CapabilityId>]>;
  readonly credentials?: Iterable<CredentialEntry>;
  readonly preferred?: Iterable<readonly [NamespaceId, Iterable<CapabilityId>]>;
  readonly permittedOperators?: Iterable<readonly [NamespaceId, Iterable<OperatorId>]>;
}

/** Explicit permitted-set + credentials maps — the default catalog and the test substrate. */
export class InMemoryCapabilityCatalog implements CapabilityCatalog {
  private readonly permitted = new Map<NamespaceId, ReadonlySet<CapabilityId>>();
  private readonly credentialsByNamespace = new Map<NamespaceId, Map<CapabilityId, Credentials>>();
  private readonly preferred = new Map<NamespaceId, readonly CapabilityId[]>();
  private readonly permittedOperatorsByNamespace = new Map<NamespaceId, ReadonlySet<OperatorId>>();

  public constructor(init: InMemoryCapabilityCatalogInit = {}) {
    for (const [namespaceId, capabilities] of init.permitted ?? []) {
      this.setPermitted(namespaceId, capabilities);
    }
    for (const entry of init.credentials ?? []) {
      this.setCredentials(entry.namespaceId, entry.capabilityId, entry.credentials);
    }
    for (const [namespaceId, order] of init.preferred ?? []) {
      this.setPreferredOrder(namespaceId, order);
    }
    for (const [namespaceId, operators] of init.permittedOperators ?? []) {
      this.setPermittedOperators(namespaceId, operators);
    }
  }

  public permittedCapabilities(namespaceId: NamespaceId): Promise<ReadonlySet<CapabilityId>> {
    return Promise.resolve(this.permitted.get(namespaceId) ?? new Set());
  }

  public permittedOperators(namespaceId: NamespaceId): Promise<ReadonlySet<OperatorId> | null> {
    return Promise.resolve(this.permittedOperatorsByNamespace.get(namespaceId) ?? null);
  }

  public credentials(namespaceId: NamespaceId, capabilityId: CapabilityId): Promise<Credentials | null> {
    const stored = this.credentialsByNamespace.get(namespaceId)?.get(capabilityId);
    // A copy, so a caller cannot alter catalog state.
    return Promise.resolve(stored === undefined ? null : { ...stored });
  }

  public preferredOrder(namespaceId: NamespaceId): Promise<readonly CapabilityId[]> {
    return Promise.resolve(this.preferred.get(namespaceId) ?? []);
  }

  public setPermitted(namespaceId: NamespaceId, capabilities: Iterable<CapabilityId>): void {
    this.permitted.set(namespaceId, new Set(capabilities));
  }

  public setPermittedOperators(namespaceId: NamespaceId, operators: Iterable<OperatorId>): void {
    this.permittedOperatorsByNamespace.set(namespaceId, new Set(operators));
  }

  public setCredentials(namespaceId: NamespaceId, capabilityId: CapabilityId, credentials: Credentials): void {
    const forNamespace = this.credentialsByNamespace.get(namespaceId) ?? new Map<CapabilityId, Credentials>();
    forNamespace.set(capabilityId, { ...credentials });
    this.credentialsByNamespace.set(namespaceId, forNamespace);
  }

  public setPreferredOrder(namespaceId: NamespaceId, order: Iterable<CapabilityId>): void {
    this.preferred.set(namespaceId, [...order]);
  }
}

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
   * Providers in the namespace's preference order win, in listed order; unlisted providers rank
   * after every listed one on a stable alphabetical tie-break by id.
   */
  public resolve<C extends Capability>(capabilityType: CapabilityClass<C>): C | null {
    const matches = [...this.available.values()].filter((capability): capability is C => {
      return capability instanceof capabilityType;
    });
    matches.sort((left, right) => this.compareByPreference(left, right));
    return matches[0] ?? null;
  }

  /**
   * Like {@link resolve}, but throw when no provider is available.
   *
   * An operator that declared the capability in `requires` is only scheduled once it is available,
   * so it can `require` the provider without a `null` check.
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

const dependsOnOf = (capability: CapabilityClass): readonly DataPointClass[] => capability.dependsOn ?? [];

const requiresOf = (capability: CapabilityClass): readonly CapabilityClass[] => capability.requires ?? [];

const depsPresent = (dependsOn: readonly DataPointClass[], presentTypes: ReadonlySet<DataPointClass>): boolean => {
  return dependsOn.every((required) => [...presentTypes].some((present) => isSubclass(present, required)));
};

const requiresAvailable = (
  requires: readonly CapabilityClass[],
  available: Iterable<CapabilityId>,
  registered: ReadonlyMap<CapabilityId, CapabilityClass>,
): boolean => {
  const availableTypes = [...available]
    .map((availableId) => registered.get(availableId))
    .filter((provider): provider is CapabilityClass => provider !== undefined);
  return requires.every((required) => availableTypes.some((provider) => isSubclass(provider, required)));
};

/** What {@link computeAvailable} needs to decide availability. */
export interface ComputeAvailableOptions {
  readonly registered: ReadonlyMap<CapabilityId, CapabilityClass>;
  readonly permitted: ReadonlySet<CapabilityId>;
  readonly presentTypes: ReadonlySet<DataPointClass>;
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

/** How long one activation may take before it is abandoned. */
export interface CapabilityActivatorOptions {
  readonly activationTimeoutMs: number;
}

/**
 * Tracks lazy activation across a session and produces the current {@link CapabilityView}.
 *
 * Activated instances are cached, so a capability is built at most once. `activate` runs on the
 * gathering loop, so it is bounded by `activationTimeoutMs`: a hung client build (a network call
 * that never returns) would otherwise stall every operator in the session, not just the ones that
 * need this capability. A failed or timed-out activation is isolated — logged, and the capability
 * stays unavailable for the rest of the session — so one broken provider never wedges the rest.
 */
export class CapabilityActivator {
  private readonly registered: ReadonlyMap<CapabilityId, ConcreteCapabilityClass>;
  private readonly catalog: CapabilityCatalog;
  private readonly namespaceId: NamespaceId;
  private readonly activationTimeoutMs: number;
  private readonly activated = new Map<CapabilityId, Capability>();
  private readonly failed = new Set<CapabilityId>();

  public constructor(
    registered: Iterable<readonly [CapabilityId, ConcreteCapabilityClass]>,
    catalog: CapabilityCatalog,
    namespaceId: NamespaceId,
    options: CapabilityActivatorOptions,
  ) {
    this.registered = new Map(registered);
    this.catalog = catalog;
    this.namespaceId = namespaceId;
    this.activationTimeoutMs = options.activationTimeoutMs;
  }

  public activatedIds(): ReadonlySet<CapabilityId> {
    return new Set(this.activated.keys());
  }

  public failedIds(): ReadonlySet<CapabilityId> {
    return new Set(this.failed);
  }

  public async refresh(storeView: DataPointView): Promise<CapabilityView> {
    const permitted = await this.catalog.permittedCapabilities(this.namespaceId);
    const preference = await this.catalog.preferredOrder(this.namespaceId);
    const availableIds = computeAvailable({
      registered: this.registered,
      permitted,
      presentTypes: storeView.presentTypes(),
    });
    // Activate newly-available capabilities base-before-layer: a capability is activated only once
    // every capability it requires is already activated (the fixpoint guarantees this terminates).
    let pending = [...availableIds].filter((id) => !this.activated.has(id) && !this.failed.has(id));
    while (pending.length > 0) {
      const ready = pending.filter((id) => {
        return requiresAvailable(requiresOf(this.classOf(id)), this.activated.keys(), this.registered);
      });
      if (ready.length === 0) {
        break;
      }
      pending = pending.filter((id) => !ready.includes(id));
      for (const capabilityId of ready) {
        await this.activateOne(capabilityId, storeView);
      }
    }
    // Only currently-available activated capabilities are exposed for new resolution (a namespace
    // revocation blocks new acquisitions; in-flight users are not cancelled).
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
      // Unreachable: every id considered comes from the registered map this activator was built with.
      throw new CapabilityUnavailableError(`capability ${capabilityId} is not registered with this activator`);
    }
    return capability;
  }

  private async activateOne(capabilityId: CapabilityId, storeView: DataPointView): Promise<void> {
    const capability = new (this.classOf(capabilityId))();
    const credentials = { ...((await this.catalog.credentials(this.namespaceId, capabilityId)) ?? {}) };
    try {
      const context = new CapabilityContext({ namespaceId: this.namespaceId, credentials, store: storeView });
      await withTimeout(capability.activate(context), this.activationTimeoutMs);
    } catch (error) {
      // Capability fault-isolation boundary — the session continues without it.
      this.failed.add(capabilityId);
      getLogger().warning('Capability activation failed or timed out; it stays unavailable for this session', {
        capabilityId,
        activationTimeoutMs: this.activationTimeoutMs,
        error: error instanceof Error ? error.message : String(error),
      });
      return;
    }
    this.activated.set(capabilityId, capability);
    getLogger().debug('Capability activated', { capabilityId });
  }
}
