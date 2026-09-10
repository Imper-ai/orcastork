"""Availability fixpoint, lazy activation, isolation of a failed activation, provider preference."""

from __future__ import annotations

import pytest

from orcastork_lite import (
    CapabilityActivator,
    CapabilityContext,
    CapabilityUnavailableError,
    CapabilityView,
    DataPointView,
    InMemoryCapabilityCatalog,
    compute_available,
)
from orcastork_lite.ids import CapabilityId

from ..doubles.clock import FakeClock
from .conftest import NAMESPACE, Email, Ip, WorkEmail, dp, make_capability

BASE, LAYER, OTHER = CapabilityId('base'), CapabilityId('layer'), CapabilityId('other')


def test_availability_is_a_fixpoint_gated_by_permission_data_and_layering() -> None:
    base = make_capability('base', depends_on={Email})
    layer = make_capability('layer', requires={base})
    registered = {BASE: base, LAYER: layer}

    assert (
        compute_available(registered=registered, permitted=frozenset({BASE, LAYER}), present_types=frozenset())
        == set()
    )
    assert compute_available(
        registered=registered, permitted=frozenset({BASE, LAYER}), present_types=frozenset({WorkEmail})
    ) == {BASE, LAYER}
    assert (
        compute_available(registered=registered, permitted=frozenset({LAYER}), present_types=frozenset({WorkEmail}))
        == set()
    )


async def test_activation_is_lazy_once_ordered_base_before_layer_and_uses_credentials(fake_clock: FakeClock) -> None:
    order: list[str] = []
    base = make_capability('base', depends_on={Email}, record_order=order)
    layer = make_capability('layer', requires={base}, record_order=order)
    catalog = InMemoryCapabilityCatalog(
        permitted={NAMESPACE: {BASE, LAYER}}, credentials={(NAMESPACE, BASE): {'token': 't-1'}}
    )
    activator = CapabilityActivator({LAYER: layer, BASE: base}, catalog, NAMESPACE)

    empty = await activator.refresh(DataPointView([]))
    assert not empty.available_ids() and order == []

    view = await activator.refresh(DataPointView([dp(WorkEmail, 'w', fake_clock.now())]))
    assert view.available_ids() == {BASE, LAYER}
    assert order == ['base', 'layer']
    assert await view.require(base).token() == 't-1'

    await activator.refresh(DataPointView([dp(WorkEmail, 'w', fake_clock.now())]))
    assert order == ['base', 'layer']  # constructed at most once per session


async def test_failed_activation_is_isolated_and_never_retried() -> None:
    attempts: list[str] = []

    async def explode(_ctx: CapabilityContext) -> None:
        raise RuntimeError('no network')

    broken = make_capability('base', on_activate=explode, record_order=attempts)
    healthy = make_capability('other')
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {BASE, OTHER}})
    activator = CapabilityActivator({BASE: broken, OTHER: healthy}, catalog, NAMESPACE)

    for _ in range(3):
        view = await activator.refresh(DataPointView([]))
    assert view.available_ids() == {OTHER}
    assert attempts == ['base']


async def test_revocation_blocks_new_resolution_without_destroying_the_instance() -> None:
    cap = make_capability('base')
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {BASE}})
    activator = CapabilityActivator({BASE: cap}, catalog, NAMESPACE)
    assert (await activator.refresh(DataPointView([]))).is_available(BASE)

    catalog.set_permitted(NAMESPACE, [])
    assert not (await activator.refresh(DataPointView([]))).is_available(BASE)
    assert activator.activated_ids() == {BASE}


def test_resolve_prefers_listed_providers_then_alphabetical_and_require_raises() -> None:
    family = make_capability('family')
    alpha, zulu = make_capability('alpha', base=family), make_capability('zulu', base=family)
    available = {CapabilityId('zulu'): zulu(), CapabilityId('alpha'): alpha()}

    assert type(CapabilityView(available).resolve(family)) is alpha  # alphabetical tie-break
    assert type(CapabilityView(available, preference=(CapabilityId('zulu'),)).resolve(family)) is zulu
    assert CapabilityView(available).resolve(make_capability('none')) is None
    assert CapabilityView(available).available_types() == {alpha, zulu}
    with pytest.raises(CapabilityUnavailableError):
        CapabilityView().require(family)


async def test_catalog_hands_back_copies_and_defaults() -> None:
    catalog = InMemoryCapabilityCatalog(credentials={(NAMESPACE, BASE): {'token': 't'}})
    creds = await catalog.credentials(NAMESPACE, BASE)
    assert creds == {'token': 't'}
    dict(creds or {})['token'] = 'changed'
    assert await catalog.credentials(NAMESPACE, BASE) == {'token': 't'}
    assert await catalog.credentials(NAMESPACE, OTHER) is None
    assert await catalog.permitted_operators(NAMESPACE) is None
    assert await catalog.preferred_order(NAMESPACE) == ()
    assert await catalog.permitted_capabilities(NAMESPACE) == frozenset()
    assert Ip is not Email  # keep the zoo import honest for the reader
