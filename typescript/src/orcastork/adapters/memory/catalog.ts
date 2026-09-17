/**
 * In-memory `CapabilityCatalog` — explicit permitted-set + credentials maps.
 *
 * Enough to satisfy the port contract and the conformance suite; a deployment writes its own
 * adapter over its real configuration store. The mutators let tests model a namespace
 * enabling/revoking a capability between grants.
 *
 * @module
 */

import type { CapabilityId, NamespaceId, OperatorId } from '../../ids.js';
import type { CapabilityCatalog } from '../../ports/capability_catalog.js';

/** The secrets a capability is built from — whatever the deployment's catalog stores for it. */
export type Credentials = Readonly<Record<string, unknown>>;

/**
 * One namespace's credentials for one capability.
 *
 * An entry rather than a map key, because the Python catalog keys credentials by the
 * `(namespace, capability)` tuple and JavaScript has no value-equal composite key.
 */
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

/** Per-namespace configuration held in process memory. */
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

  public async permittedCapabilities(namespaceId: NamespaceId): Promise<ReadonlySet<CapabilityId>> {
    return this.permitted.get(namespaceId) ?? new Set<CapabilityId>();
  }

  public async permittedOperators(namespaceId: NamespaceId): Promise<ReadonlySet<OperatorId> | null> {
    // A namespace with no configured restriction is unrestricted (null), per the port contract — an
    // empty set is the explicit "run nothing" configuration, never the default.
    return this.permittedOperatorsByNamespace.get(namespaceId) ?? null;
  }

  public async credentials(namespaceId: NamespaceId, capabilityId: CapabilityId): Promise<Credentials | null> {
    // Hand back a copy so a caller mutating the returned mapping cannot alter catalog state.
    const stored = this.credentialsByNamespace.get(namespaceId)?.get(capabilityId);
    return stored === undefined ? null : { ...stored };
  }

  public async preferredOrder(namespaceId: NamespaceId): Promise<readonly CapabilityId[]> {
    return this.preferred.get(namespaceId) ?? [];
  }

  /** Test helper: model a namespace config change (enable/revoke) visible to the next grant. */
  public setPermitted(namespaceId: NamespaceId, capabilities: Iterable<CapabilityId>): void {
    this.permitted.set(namespaceId, new Set(capabilities));
  }

  /** Test helper: model a namespace changing its provider preference, visible to the next refresh. */
  public setPreferredOrder(namespaceId: NamespaceId, order: Iterable<CapabilityId>): void {
    this.preferred.set(namespaceId, [...order]);
  }

  /** Test helper: model a namespace config change to its operator gating, visible to the next grant. */
  public setPermittedOperators(namespaceId: NamespaceId, operators: Iterable<OperatorId>): void {
    this.permittedOperatorsByNamespace.set(namespaceId, new Set(operators));
  }

  /** Test helper: supply (or replace) the credentials one namespace holds for one capability. */
  public setCredentials(namespaceId: NamespaceId, capabilityId: CapabilityId, credentials: Credentials): void {
    const forNamespace = this.credentialsByNamespace.get(namespaceId) ?? new Map<CapabilityId, Credentials>();
    forNamespace.set(capabilityId, { ...credentials });
    this.credentialsByNamespace.set(namespaceId, forNamespace);
  }
}
