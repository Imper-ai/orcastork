"""MIR — SessionStateMirror: sole-mutator local state with exact store parity.

The mirror must reproduce the store's keyed-merge and change-set semantics EXACTLY (it
replaces the store on every hot-loop read), so the core tests here are parity tables: the
same adversarial batch sequences applied to a mirror and to the in-memory store directly
must yield identical snapshots, revisions and change-set answers at every revision. The
orchestrator-level tests pin the read budget: one full store read per run (the rehydrate),
plus one change-set read per distinct pre-rehydration watermark on resume — never per pass.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from typing import Any

import pytest

from orcastork.adapters.memory import InMemoryDataPointStore
from orcastork.datapoints import BaseDataPoint, DataPointView, MergeKind, identity_key
from orcastork.exceptions import StaleEpochError, StateMirrorError
from orcastork.ids import Epoch, NamespaceId, OperatorId, Revision, SessionId
from orcastork.operators import OperatorContext
from orcastork.orchestrator import Orchestrator, SessionStatus
from orcastork.orchestrator.mirror import SessionStateMirror
from orcastork.runtime import build_in_memory_runtime

from .doubles.clock import FakeClock
from .doubles.datapoints import (
    DEFAULT_OP,
    T0,
    ChatAnswerDataPoint,
    EmailDataPoint,
    GeoDataPoint,
    IpDataPoint,
    RiskDataPoint,
    ip,
    personal_email,
    risk,
    work_email,
)
from .doubles.operators import make_operator

SID = SessionId('mir-session')
NAMESPACE = NamespaceId('mir-namespace')
EPOCH = Epoch(1)
T1 = T0 + timedelta(hours=1)
T2 = T0 + timedelta(hours=2)


def geo(value: dict[str, float], *, last: datetime = T0) -> GeoDataPoint:
    return GeoDataPoint(value=value, retrieved_by=DEFAULT_OP, first_retrieved=T0, last_retrieved=last)


def _state(view: DataPointView) -> dict[tuple[str, Any], tuple[Any, ...]]:
    """Full observable state per identity: provenance + both timestamps."""
    return {identity_key(dp): (dp.retrieved_by, dp.first_retrieved, dp.last_retrieved) for dp in view.all()}


def _ids(points: tuple[BaseDataPoint[Any], ...]) -> dict[tuple[str, Any], datetime]:
    """Change-set rows by identity, carrying the merged payload's last_retrieved."""
    return {identity_key(dp): dp.last_retrieved for dp in points}


class _CountingStore(InMemoryDataPointStore):
    """In-memory store counting full reads and resolved applies (pins the mirror's I/O budget)."""

    def __init__(self) -> None:
        super().__init__()
        self.snapshot_reads = 0
        self.revision_reads = 0
        self.change_set_reads = 0
        self.apply_calls = 0

    async def snapshot(self, session_id: SessionId) -> Any:
        self.snapshot_reads += 1
        return await super().snapshot(session_id)

    async def revision(self, session_id: SessionId) -> Any:
        self.revision_reads += 1
        return await super().revision(session_id)

    async def change_set_since(self, session_id: SessionId, since: Revision) -> Any:
        self.change_set_reads += 1
        return await super().change_set_since(session_id, since)

    async def apply_resolved(self, session_id: SessionId, *, added: Any, updated: Any, epoch: Epoch) -> Any:
        self.apply_calls += 1
        return await super().apply_resolved(session_id, added=added, updated=updated, epoch=epoch)


# Adversarial merge sequences: re-observations (newer, older, equal), intra-batch duplicates,
# mixed add+update batches, value coexistence, and an unhashable (dict) identity.
_PARITY_SEQUENCES: dict[str, list[list[BaseDataPoint[Any]]]] = {
    'newer_reobservation': [[work_email('a@e.example', last=T0)], [work_email('a@e.example', last=T2)]],
    'older_reobservation_is_noop': [[work_email('a@e.example', last=T2)], [work_email('a@e.example', last=T0)]],
    'equal_reobservation_is_noop': [[work_email('a@e.example', last=T1)], [work_email('a@e.example', last=T1)]],
    'intra_batch_duplicate_add': [[work_email('a@e.example', last=T0), work_email('a@e.example', last=T2)]],
    'mixed_add_and_update_batch': [
        [work_email('a@e.example', last=T0)],
        [work_email('a@e.example', last=T2), personal_email('p@e.example', last=T0)],
        [ip('203.0.113.1', last=T1)],
    ],
    'same_type_new_value_coexists': [
        [work_email('a@e.example', last=T0), work_email('b@e.example', last=T0)],
        [work_email('a@e.example', last=T1)],
    ],
    'unhashable_dict_identity': [
        [geo({'lat': 1.0, 'lon': 2.0})],
        [geo({'lon': 2.0, 'lat': 1.0}, last=T2)],  # key-order-insensitive identity → an update
        [geo({'lat': 3.0, 'lon': 4.0})],
    ],
    'interleaved_updates_across_batches': [
        [work_email('a@e.example', last=T0), ip('203.0.113.1', last=T0)],
        [ip('203.0.113.1', last=T2)],
        [work_email('a@e.example', last=T1), ip('203.0.113.1', last=T1)],
    ],
}


@pytest.mark.parametrize('batches', _PARITY_SEQUENCES.values(), ids=_PARITY_SEQUENCES.keys())
async def test_mir_01_merge_parity_with_the_store_under_adversarial_sequences(
    batches: list[list[BaseDataPoint[Any]]],
) -> None:
    reference = InMemoryDataPointStore()  # the store's own keyed-merge is the contract
    backing = InMemoryDataPointStore()
    mirror = SessionStateMirror(backing, SID)
    await mirror.rehydrate()

    for batch in batches:
        await reference.write(SID, batch, epoch=EPOCH)
        await mirror.write(batch, epoch=EPOCH)

    assert await mirror.revision(SID) == await reference.revision(SID)
    assert _state(await mirror.snapshot(SID)) == _state(await reference.snapshot(SID))
    assert _state(await backing.snapshot(SID)) == _state(await reference.snapshot(SID))  # written through
    for revision in range(int(await reference.revision(SID)) + 1):
        expected = await reference.change_set_since(SID, Revision(revision))
        local = await mirror.change_set_since(SID, Revision(revision))
        durable = await backing.change_set_since(SID, Revision(revision))
        assert _ids(local.added) == _ids(expected.added) == _ids(durable.added)
        assert _ids(local.updated) == _ids(expected.updated) == _ids(durable.updated)


async def test_mir_02_rehydrated_mirror_answers_pre_rehydration_change_sets_exactly() -> None:
    store = InMemoryDataPointStore()
    await store.write(SID, [work_email('a@e.example', last=T0)], epoch=EPOCH)  # rev 1
    await store.write(SID, [ip('203.0.113.1', last=T0)], epoch=EPOCH)  # rev 2
    await store.write(SID, [work_email('a@e.example', last=T1)], epoch=EPOCH)  # rev 3 — an update

    mirror = SessionStateMirror(store, SID)
    await mirror.rehydrate()
    for revision in range(4):
        await mirror.prime_change_baseline(Revision(revision))  # 3 is the rehydration point → no-op

    await mirror.write([risk(0.5)], epoch=EPOCH)  # rev 4 — a local add on top of rehydrated state
    await mirror.write([ip('203.0.113.1', last=T2)], epoch=EPOCH)  # rev 5 — updates a pre-rehydration identity

    assert await mirror.revision(SID) == await store.revision(SID)
    for revision in range(int(await store.revision(SID)) + 1):
        expected = await store.change_set_since(SID, Revision(revision))
        actual = await mirror.change_set_since(SID, Revision(revision))
        assert _ids(actual.added) == _ids(expected.added), f'added diverged at revision {revision}'
        assert _ids(actual.updated) == _ids(expected.updated), f'updated diverged at revision {revision}'


async def test_mir_03_local_reads_do_no_store_io_after_rehydration() -> None:
    store = _CountingStore()
    first = await store.write(SID, [work_email('a@e.example', last=T0)], epoch=EPOCH)
    await store.write(SID, [ip('203.0.113.1', last=T0)], epoch=EPOCH)
    store.snapshot_reads = store.revision_reads = store.change_set_reads = 0

    mirror = SessionStateMirror(store, SID)
    await mirror.rehydrate()
    await mirror.prime_change_baseline(first)
    for index in range(5):
        await mirror.write([risk(float(index))], epoch=EPOCH)
        await mirror.snapshot(SID)
        await mirror.revision(SID)
        await mirror.change_set_since(SID, first)  # pre-rehydration → served from the primed baseline
        await mirror.change_set_since(SID, Revision(2))  # rehydration point → served from local stamps

    assert store.snapshot_reads == 1  # the rehydrate
    assert store.revision_reads == 1  # the rehydrate
    assert store.change_set_reads == 1  # the one primed baseline; never re-read per query


async def test_mir_04_stale_epoch_propagates_before_the_local_copy_is_touched() -> None:
    store = InMemoryDataPointStore()
    await store.write(SID, [work_email('a@e.example')], epoch=Epoch(2))
    mirror = SessionStateMirror(store, SID)
    await mirror.rehydrate()

    with pytest.raises(StaleEpochError):
        await mirror.write([personal_email('p@e.example')], epoch=Epoch(1))

    assert {p.value for p in (await mirror.snapshot(SID)).all()} == {'a@e.example'}  # local copy untouched
    assert await mirror.revision(SID) == 1
    assert {p.value for p in (await store.snapshot(SID)).all()} == {'a@e.example'}  # store untouched (atomic)


async def test_mir_05_contract_violations_raise_state_mirror_error() -> None:
    store = InMemoryDataPointStore()
    unhydrated = SessionStateMirror(store, SID)
    with pytest.raises(StateMirrorError):
        await unhydrated.snapshot(SID)
    with pytest.raises(StateMirrorError):
        await unhydrated.write([work_email()], epoch=EPOCH)

    await store.write(SID, [work_email()], epoch=EPOCH)  # revision 1, so revision 0 predates rehydration
    mirror = SessionStateMirror(store, SID)
    await mirror.rehydrate()
    with pytest.raises(StateMirrorError):
        await mirror.snapshot(SessionId('mir-other-session'))  # a mirror serves exactly one session
    with pytest.raises(StateMirrorError):
        await mirror.change_set_since(SID, Revision(0))  # pre-rehydration and never primed → loud, not wrong


async def test_mir_06_write_outcomes_split_added_and_updated_per_presented_point() -> None:
    mirror = SessionStateMirror(InMemoryDataPointStore(), SID)
    await mirror.rehydrate()

    first = await mirror.write([work_email('a@e.example', last=T0)], epoch=EPOCH)
    assert [outcome.kind for outcome in first.outcomes] == [MergeKind.ADDED]

    second = await mirror.write(
        [
            work_email('a@e.example', last=T2),  # existing identity → updated
            personal_email('p@e.example', last=T0),  # new identity → added
            personal_email('p@e.example', last=T0),  # intra-batch re-observation → updated
        ],
        epoch=EPOCH,
    )
    assert [outcome.kind for outcome in second.outcomes] == [MergeKind.UPDATED, MergeKind.ADDED, MergeKind.UPDATED]

    # An older re-observation merges into the existing entry (an UPDATED presentation) without
    # changing anything durably — the revision stays where it was.
    third = await mirror.write([work_email('a@e.example', last=T0)], epoch=EPOCH)
    assert [outcome.kind for outcome in third.outcomes] == [MergeKind.UPDATED]
    assert third.revision == second.revision


async def test_mir_07_orchestrator_run_reads_the_store_once_not_per_pass(fake_clock: FakeClock) -> None:
    store = _CountingStore()
    runtime = replace(build_in_memory_runtime(fake_clock), store=store)
    values = iter(f'203.0.113.{index}' for index in range(1, 100))
    # A bounded self-cycle (multiple passes + reruns) plus a multi-emission producer: plenty of
    # merges and re-plans, all of which must be served by the mirror, not store re-reads.
    self_cycle = make_operator(
        'selfloop',
        produces={IpDataPoint},
        depends_on={IpDataPoint},
        rerun_on_new_data=True,
        max_cycles=3,
        debounce=timedelta(seconds=1),
        emit_factory=lambda _ctx: [ip(next(values))],  # noqa: ARG005
    )
    chatty = make_operator(
        'chatty',
        depends_on={EmailDataPoint},
        produces={RiskDataPoint},
        emits=[RiskDataPoint.emit(0.1), RiskDataPoint.emit(0.2), RiskDataPoint.emit(0.3)],
    )

    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[self_cycle, chatty],
        seed=[work_email(), ip('seed')],
    ).run()

    assert result.status is SessionStatus.COMPLETED
    assert result.operator_runs[OperatorId('selfloop')] >= 2  # the loop genuinely iterated
    assert store.snapshot_reads == 1  # ONE full read per run — the mirror rehydrate
    assert store.revision_reads == 1  # likewise; never re-read per pass or per merge
    assert store.change_set_reads == 0  # a fresh session computes every delta locally
    assert len((await store.snapshot(SID)).of_type(RiskDataPoint)) == 3  # the work still all landed


async def test_mir_08_resume_primes_each_pre_rehydration_watermark_once(fake_clock: FakeClock) -> None:
    store = _CountingStore()
    runtime = replace(build_in_memory_runtime(fake_clock), store=store)
    # A predecessor ran the watcher (watermark at revision 1), then merged one more relevant
    # DataPoint (revision 2) and died; its lease expires.
    epoch = await runtime.lock.acquire(SID)
    seen_by_watcher = await runtime.store.write(SID, [ip('203.0.113.1')], epoch=epoch)
    await runtime.store.set_watermark(SID, OperatorId('watcher'), seen_by_watcher, epoch=epoch)
    await runtime.store.write(SID, [ip('203.0.113.2')], epoch=epoch)
    fake_clock.advance(31.0)

    deltas: list[set[str]] = []

    def observe(ctx: OperatorContext) -> list[Any]:
        deltas.append({dp.value for dp in ctx.delta.added})
        return [ChatAnswerDataPoint.emit('seen')]

    watcher = make_operator(
        'watcher',
        depends_on={IpDataPoint},
        produces={ChatAnswerDataPoint},
        rerun_on_new_data=True,
        debounce=timedelta(0),
        emit_factory=observe,
    )
    store.snapshot_reads = store.revision_reads = store.change_set_reads = 0  # count only the resumed run

    result = await Orchestrator(session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[watcher]).run()

    assert result.status is SessionStatus.COMPLETED
    assert result.operator_runs == {OperatorId('watcher'): 1}
    assert deltas == [{'203.0.113.2'}]  # the rerun's delta matches what the store itself would answer
    assert store.snapshot_reads == 1  # the rehydrate
    assert store.revision_reads == 1
    assert store.change_set_reads == 1  # exactly one primed baseline for the rehydrated watermark
