"""DELTA — per-operator invocation deltas computed from the store watermark."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from orcastork.adapters.memory import InMemoryDataPointStore
from orcastork.datapoints import BaseDataPoint
from orcastork.ids import CapabilityId, Epoch, OperatorId, SessionId
from orcastork.scheduling import operator_delta

from .doubles.datapoints import T0, personal_email, work_email

SID = SessionId('delta-session')
OP = OperatorId('delta-op')
OTHER_OP = OperatorId('delta-op-2')
EPOCH = Epoch(1)
NO_CAPS: frozenset[CapabilityId] = frozenset()
T2 = T0 + timedelta(hours=2)


async def _store_with(*points: BaseDataPoint[Any]) -> InMemoryDataPointStore:
    store = InMemoryDataPointStore()
    await store.write(SID, list(points), epoch=EPOCH)
    return store


async def test_delta_01_first_invocation_presents_full_set_as_added() -> None:
    store = await _store_with(work_email('a@e.example'), personal_email('p@e.example'))
    delta = await operator_delta(store, SID, watermark=None, available_caps=NO_CAPS, previous_caps=NO_CAPS)
    assert delta.is_first_invocation
    assert {p.value for p in delta.added} == {'a@e.example', 'p@e.example'}
    assert not delta.updated


async def test_delta_02_reinvocation_reflects_changes_since_watermark() -> None:
    store = InMemoryDataPointStore()
    first_rev = await store.write(SID, [work_email('a@e.example')], epoch=EPOCH)
    await store.set_watermark(SID, OP, first_rev, epoch=EPOCH)
    await store.write(SID, [personal_email('p@e.example')], epoch=EPOCH)

    delta = await operator_delta(
        store, SID, watermark=await store.get_watermark(SID, OP), available_caps=NO_CAPS, previous_caps=NO_CAPS
    )
    assert not delta.is_first_invocation
    assert {p.value for p in delta.added} == {'p@e.example'}


async def test_delta_03_added_are_new_identities_updated_are_reobservations() -> None:
    store = InMemoryDataPointStore()
    base = await store.write(SID, [work_email('a@e.example', last=T0)], epoch=EPOCH)
    await store.set_watermark(SID, OP, base, epoch=EPOCH)
    await store.write(SID, [work_email('a@e.example', last=T2), personal_email('p@e.example')], epoch=EPOCH)

    delta = await operator_delta(
        store, SID, watermark=await store.get_watermark(SID, OP), available_caps=NO_CAPS, previous_caps=NO_CAPS
    )
    assert {p.type for p in delta.added} == {'personal_email'}
    assert {p.type for p in delta.updated} == {'work_email'}


async def test_delta_04_newly_available_caps_since_last_run() -> None:
    store = await _store_with(work_email('a@e.example'))
    delta = await operator_delta(
        store,
        SID,
        watermark=None,
        available_caps=frozenset({CapabilityId('a'), CapabilityId('b')}),
        previous_caps=frozenset({CapabilityId('a')}),
    )
    assert delta.newly_available_caps == frozenset({CapabilityId('b')})


async def test_delta_05_persisted_watermark_reconstructs_delta_on_resume() -> None:
    store = InMemoryDataPointStore()
    rev = await store.write(SID, [work_email('a@e.example')], epoch=EPOCH)
    await store.set_watermark(SID, OP, rev, epoch=EPOCH)
    # "Resume": the watermark is still in the store; a later write is the only delta.
    await store.write(SID, [personal_email('p@e.example')], epoch=EPOCH)
    assert await store.get_watermark(SID, OP) == rev
    delta = await operator_delta(
        store, SID, watermark=await store.get_watermark(SID, OP), available_caps=NO_CAPS, previous_caps=NO_CAPS
    )
    assert {p.value for p in delta.added} == {'p@e.example'}


async def test_delta_06_lost_watermark_degrades_to_first_invocation() -> None:
    store = await _store_with(work_email('a@e.example'))
    assert await store.get_watermark(SID, OP) is None  # never set / lost
    delta = await operator_delta(
        store, SID, watermark=await store.get_watermark(SID, OP), available_caps=NO_CAPS, previous_caps=NO_CAPS
    )
    assert delta.is_first_invocation
    assert {p.value for p in delta.added} == {'a@e.example'}


async def test_delta_07_operators_have_independent_watermarks() -> None:
    store = InMemoryDataPointStore()
    rev = await store.write(SID, [work_email('a@e.example')], epoch=EPOCH)
    await store.set_watermark(SID, OP, rev, epoch=EPOCH)  # OP has run; OTHER_OP has not

    op_delta = await operator_delta(
        store, SID, watermark=await store.get_watermark(SID, OP), available_caps=NO_CAPS, previous_caps=NO_CAPS
    )
    other_delta = await operator_delta(
        store, SID, watermark=await store.get_watermark(SID, OTHER_OP), available_caps=NO_CAPS, previous_caps=NO_CAPS
    )
    assert not op_delta.is_first_invocation
    assert other_delta.is_first_invocation


async def test_delta_08_new_value_for_existing_type_is_added_not_replacement() -> None:
    store = InMemoryDataPointStore()
    base = await store.write(SID, [work_email('a@e.example')], epoch=EPOCH)
    await store.set_watermark(SID, OP, base, epoch=EPOCH)
    await store.write(SID, [work_email('b@e.example')], epoch=EPOCH)  # new value, same type

    delta = await operator_delta(
        store, SID, watermark=await store.get_watermark(SID, OP), available_caps=NO_CAPS, previous_caps=NO_CAPS
    )
    assert {p.value for p in delta.added} == {'b@e.example'}
    assert len((await store.snapshot(SID)).all()) == 2  # both coexist


async def test_delta_09_empty_delta_when_nothing_changed_since_watermark() -> None:
    store = InMemoryDataPointStore()
    rev = await store.write(SID, [work_email('a@e.example')], epoch=EPOCH)
    await store.set_watermark(SID, OP, rev, epoch=EPOCH)
    delta = await operator_delta(store, SID, watermark=rev, available_caps=NO_CAPS, previous_caps=NO_CAPS)
    assert not delta.added and not delta.updated
