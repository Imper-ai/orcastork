/**
 * The `CapabilityCatalog` port — per-namespace permitted capabilities/operators + credentials.
 *
 * Decides *what* a namespace may use and holds the secrets; availability and lazy activation are
 * the orchestrator's concern. A config change is visible to the next grant (enable/revoke). The
 * framework ships the port plus an in-memory adapter; a deployment backs it with wherever its own
 * configuration and secrets actually live.
 *
 * @module
 */

import type { CapabilityId, NamespaceId, OperatorId } from '../ids.js';

/** What a namespace may run, and the credentials it runs it with. */
export interface CapabilityCatalog {
  /** The capability families the namespace is permitted to use. */
  permittedCapabilities(namespaceId: NamespaceId): Promise<ReadonlySet<CapabilityId>>;

  /**
   * The operators the namespace may run, or `null` when the namespace declares no restriction.
   *
   * `null` — not an empty set — is the unrestricted default because an empty set means the
   * namespace may run *nothing*: most namespaces configure no operator restriction at all, and
   * conflating "unconfigured" with "deny everything" would silently disable every unconfigured
   * namespace's flows. Like capability permissions, a change is visible to the next grant, never
   * to a run already in flight.
   */
  permittedOperators(namespaceId: NamespaceId): Promise<ReadonlySet<OperatorId> | null>;

  /** Credentials for a capability, or `null` if not permitted / not configured. */
  credentials(namespaceId: NamespaceId, capabilityId: CapabilityId): Promise<Readonly<Record<string, unknown>> | null>;

  /**
   * The namespace's provider preference ranking (may be empty); listed providers resolve first, in
   * order.
   */
  preferredOrder(namespaceId: NamespaceId): Promise<readonly CapabilityId[]>;
}
