/**
 * CAT — the `CapabilityCatalog` port + its in-memory adapter.
 *
 * The framework ships the port and an in-memory adapter; a deployment backs the catalog with its own
 * configuration store. These verify the port contract: permitted-family resolution, credential
 * supply, and config changes being visible to the next grant (enable / revoke).
 */

import { describe, expect, it } from 'vitest';
import { InMemoryCapabilityCatalog } from '../../src/orcastork/adapters/memory/index.js';
import { CapabilityId, NamespaceId, OperatorId } from '../../src/orcastork/ids.js';

const NAMESPACE = NamespaceId('cat-namespace');
const IDP = CapabilityId('idp');
const ATS = CapabilityId('ats');

describe('the in-memory capability catalog', () => {
  it('resolves a namespace permitted families', async () => {
    const catalog = new InMemoryCapabilityCatalog({ permitted: [[NAMESPACE, [IDP, ATS]]] });
    expect(await catalog.permittedCapabilities(NAMESPACE)).toEqual(new Set([IDP, ATS]));
    expect(await catalog.permittedCapabilities(NamespaceId('unknown'))).toEqual(new Set());
  });

  it('supplies credentials', async () => {
    const catalog = new InMemoryCapabilityCatalog({
      permitted: [[NAMESPACE, [IDP]]],
      credentials: [{ namespaceId: NAMESPACE, capabilityId: IDP, credentials: { token: 'secret' } }],
    });
    expect(await catalog.credentials(NAMESPACE, IDP)).toEqual({ token: 'secret' });
    expect(await catalog.credentials(NAMESPACE, ATS)).toBeNull(); // permitted but not configured
  });

  it('makes a config change visible to the next grant', async () => {
    const catalog = new InMemoryCapabilityCatalog({ permitted: [[NAMESPACE, [IDP]]] });
    expect(await catalog.permittedCapabilities(NAMESPACE)).toEqual(new Set([IDP]));
    catalog.setPermitted(NAMESPACE, [IDP, ATS]); // enable ATS
    expect(await catalog.permittedCapabilities(NAMESPACE)).toEqual(new Set([IDP, ATS]));
    catalog.setPermitted(NAMESPACE, []); // revoke everything
    expect(await catalog.permittedCapabilities(NAMESPACE)).toEqual(new Set());
  });

  it('defaults the operator restriction to null, meaning unrestricted', async () => {
    const catalog = new InMemoryCapabilityCatalog();
    expect(await catalog.permittedOperators(NAMESPACE)).toBeNull(); // unconfigured ≠ deny-everything
  });

  it('round-trips the operator restriction and tells empty apart from unset', async () => {
    const catalog = new InMemoryCapabilityCatalog({ permittedOperators: [[NAMESPACE, [OperatorId('scorer')]]] });
    expect(await catalog.permittedOperators(NAMESPACE)).toEqual(new Set([OperatorId('scorer')]));
    catalog.setPermittedOperators(NAMESPACE, []); // the explicit deny-everything configuration
    expect(await catalog.permittedOperators(NAMESPACE)).toEqual(new Set());
    expect(await catalog.permittedOperators(NamespaceId('cat-unknown-namespace'))).toBeNull();
  });
});
