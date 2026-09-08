"""CAT — the CapabilityCatalog port + its in-memory adapter.

The framework ships the port and an in-memory adapter; a deployment backs the catalog with
its own configuration store. These verify the
port contract: permitted-family resolution, credential supply, and config changes being
visible to the next grant (enable / revoke).
"""

from __future__ import annotations

from orcastork.adapters.memory import InMemoryCapabilityCatalog
from orcastork.ids import CapabilityId, NamespaceId, OperatorId

NAMESPACE = NamespaceId('cat-namespace')
IDP = CapabilityId('idp')
ATS = CapabilityId('ats')


async def test_cat_01_resolves_namespace_permitted_families() -> None:
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {IDP, ATS}})
    assert await catalog.permitted_capabilities(NAMESPACE) == frozenset({IDP, ATS})
    assert await catalog.permitted_capabilities(NamespaceId('unknown')) == frozenset()


async def test_cat_02_supplies_credentials() -> None:
    catalog = InMemoryCapabilityCatalog(
        permitted={NAMESPACE: {IDP}}, credentials={(NAMESPACE, IDP): {'token': 'secret'}}
    )
    assert await catalog.credentials(NAMESPACE, IDP) == {'token': 'secret'}
    assert await catalog.credentials(NAMESPACE, ATS) is None  # permitted but not configured


async def test_cat_03_config_change_visible_to_next_grant() -> None:
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {IDP}})
    assert await catalog.permitted_capabilities(NAMESPACE) == frozenset({IDP})
    catalog.set_permitted(NAMESPACE, {IDP, ATS})  # enable ATS
    assert await catalog.permitted_capabilities(NAMESPACE) == frozenset({IDP, ATS})
    catalog.set_permitted(NAMESPACE, set())  # revoke everything
    assert await catalog.permitted_capabilities(NAMESPACE) == frozenset()


async def test_cat_04_operator_restriction_defaults_to_none_meaning_unrestricted() -> None:
    catalog = InMemoryCapabilityCatalog()
    assert await catalog.permitted_operators(NAMESPACE) is None  # unconfigured ≠ deny-everything


async def test_cat_05_operator_restriction_roundtrips_and_distinguishes_empty_from_unset() -> None:
    catalog = InMemoryCapabilityCatalog(permitted_operators={NAMESPACE: {OperatorId('scorer')}})
    assert await catalog.permitted_operators(NAMESPACE) == frozenset({OperatorId('scorer')})
    catalog.set_permitted_operators(NAMESPACE, set())  # the explicit deny-everything configuration
    assert await catalog.permitted_operators(NAMESPACE) == frozenset()
    assert await catalog.permitted_operators(NamespaceId('cat-unknown-namespace')) is None
