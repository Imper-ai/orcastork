"""REPLAY — the session-replay harness over archived raw DataPoints.

A recorded session's :class:`ArchivedDataPoint`s are reconstructed and re-run through a
flow on a fresh in-memory runtime: same flow → same durable output (regression testing);
different flow → new aggregates re-derived from the raw signals without re-running
collection (the design's reprocessing promise, made executable).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from pydantic import ValidationError

from orcastork import replay as replay_module
from orcastork.adapters.memory import InMemoryCapabilityCatalog
from orcastork.archive import ArchivedDataPoint
from orcastork.exceptions import ReplayError, UnknownDataPointTypeError
from orcastork.flow import FlowDefinition
from orcastork.ids import CapabilityId, Epoch, NamespaceId, OperatorId, SessionId
from orcastork.operators import Operator, OperatorContext
from orcastork.orchestrator import Orchestrator, SessionStatus
from orcastork.replay import replay_session
from orcastork.runtime import build_in_memory_runtime

from .doubles.capabilities import make_capability
from .doubles.clock import FakeClock
from .doubles.datapoints import T0, EmailDataPoint, RiskDataPoint, WorkEmailDataPoint, risk, work_email
from .doubles.operators import make_aggregator, make_operator

NAMESPACE = NamespaceId('replay-namespace')
SID = SessionId('replay-session')
ORIGINAL_COLLECTOR = OperatorId('original_collector')


def _flow(*operators: type[Operator], **kwargs: Any) -> FlowDefinition:
    return FlowDefinition(name='replay-flow', operators=tuple(operators), **kwargs)


async def _write_risk_count(ctx: OperatorContext) -> None:
    assert ctx.aggregation is not None
    await ctx.aggregation.upsert('reports', 'report', {'risk_count': len(ctx.store.of_type(RiskDataPoint))})


def _archived_email(
    value: str = 'alice@work.example',
    *,
    first: datetime = T0,
    last: datetime = T0,
    by: OperatorId = ORIGINAL_COLLECTOR,
) -> ArchivedDataPoint:
    return ArchivedDataPoint.from_data_point(
        work_email(value, first=first, last=last, by=by), session_id=SID, namespace_id=NAMESPACE, epoch=Epoch(1)
    )


async def test_replay_same_flow_reproduces_original_durable_output(fake_clock: FakeClock) -> None:
    # Original session: collect → score → aggregate, live-archiving as it goes.
    runtime = build_in_memory_runtime(fake_clock)
    scorer = make_operator('scorer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk(0.8)])
    report = make_aggregator('report', depends_on={RiskDataPoint}, on_aggregate=_write_risk_count)
    original = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[scorer, report], seed=[work_email()]
    ).run()
    assert original.status is SessionStatus.COMPLETED
    original_document = await runtime.durable.read('reports', 'report')
    assert original_document is not None
    archived = await runtime.archive.read(SID)
    assert {entry.type for entry in archived} == {'work_email', 'risk'}

    replayed = await replay_session(archived, flow=_flow(scorer, report), clock=FakeClock())

    assert replayed.result.status is SessionStatus.COMPLETED
    replayed_document = await replayed.runtime.durable.read('reports', 'report')
    assert replayed_document is not None and replayed_document.document == original_document.document
    # Every archived identity is back in the replayed store (the flow may add more on top).
    replayed_identities = {(dp.type, dp.value) for dp in replayed.data_points}
    assert {(entry.type, entry.value) for entry in archived} <= replayed_identities


async def test_replay_rederives_new_aggregate_without_rerunning_collection(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    collect_runs = {'count': 0}

    def collect(ctx: OperatorContext) -> list[RiskDataPoint]:  # noqa: ARG001
        collect_runs['count'] += 1
        return [risk(0.4), risk(0.9)]

    collector = make_operator('collector', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emit_factory=collect)
    original = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[collector], seed=[work_email()]
    ).run()
    assert original.status is SessionStatus.COMPLETED and collect_runs['count'] == 1
    archived = await runtime.archive.read(SID)

    # A what-if flow: an aggregator that did not exist when the session ran; the collector is
    # NOT part of the replay flow — its raw signals arrive via the reconstructed seed.
    async def write_max_risk(ctx: OperatorContext) -> None:
        assert ctx.aggregation is not None
        risks = ctx.store.of_type(RiskDataPoint)
        await ctx.aggregation.upsert('reports', 'max_risk', {'max_risk': max(dp.value for dp in risks)})

    new_report = make_aggregator('max_risk_report', depends_on={RiskDataPoint}, on_aggregate=write_max_risk)
    replayed = await replay_session(archived, flow=_flow(new_report))

    assert replayed.result.status is SessionStatus.COMPLETED
    assert collect_runs['count'] == 1  # collection was not re-run
    assert OperatorId('collector') not in replayed.result.operator_runs
    document = await replayed.runtime.durable.read('reports', 'max_risk')
    assert document is not None and document.document == {'max_risk': 0.9}


async def test_replay_preserves_archived_timestamps_and_provenance() -> None:
    first = datetime(2026, 3, 1, tzinfo=timezone.utc)
    last = datetime(2026, 3, 2, tzinfo=timezone.utc)
    entry = _archived_email(first=first, last=last)

    replayed = await replay_session([entry], flow=_flow())

    emails = [dp for dp in replayed.data_points if isinstance(dp, WorkEmailDataPoint)]
    assert len(emails) == 1
    assert emails[0].first_retrieved == first
    assert emails[0].last_retrieved == last
    assert emails[0].retrieved_by == ORIGINAL_COLLECTOR


async def test_replay_unknown_archived_type_propagates() -> None:
    # The leaf this entry was archived under no longer ships — a meaningful replay failure.
    entry = ArchivedDataPoint(
        session_id=SID,
        namespace_id=NAMESPACE,
        type='vanished_signal',
        value='whatever',
        retrieved_by=OperatorId('old_operator'),
        first_retrieved=T0,
        last_retrieved=T0,
        is_pii=False,
        epoch=Epoch(1),
    )
    with pytest.raises(UnknownDataPointTypeError):
        await replay_session([entry], flow=_flow())


async def test_replay_derives_session_and_namespace_from_entries() -> None:
    replayed = await replay_session([_archived_email()], flow=_flow())

    # The replayed state lives under the archived session id, and the audit trail carries the
    # archived namespace id — both were derived, not supplied.
    assert (await replayed.runtime.store.snapshot(SID)).all() == replayed.data_points
    assert len(replayed.data_points) == 1
    audit_entries = await replayed.runtime.audit.replay(SID)
    assert audit_entries and all(entry.namespace_id == NAMESPACE for entry in audit_entries)


async def test_replay_empty_archive_without_explicit_ids_raises() -> None:
    with pytest.raises(ReplayError, match='session_id and namespace_id'):
        await replay_session([], flow=_flow())


async def test_replay_threads_flow_emission_queue_size_to_the_orchestrator(monkeypatch: pytest.MonkeyPatch) -> None:
    spawned: list[Orchestrator] = []

    class _CapturingOrchestrator(Orchestrator):
        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            spawned.append(self)

    monkeypatch.setattr(replay_module, 'Orchestrator', _CapturingOrchestrator)

    replayed = await replay_session([_archived_email()], flow=_flow(emission_queue_size=1))

    assert replayed.result.status is SessionStatus.COMPLETED
    (orchestrator,) = spawned
    assert orchestrator._build_emission_queue().maxsize == 1  # the flow-level bound reached the replay spawn


async def test_replay_auto_permits_flow_capabilities_with_empty_credentials() -> None:
    # The default (catalog=None) replay path must permit every flow capability and hand the
    # activator EMPTY credentials — a replayed flow reprocesses already-arrived data and must
    # never reach for a live backend. The standard suite only ever replays capability-free flows,
    # so the dict-comprehension over flow.capabilities runs on a non-empty set for the first time.
    netcap = make_capability('netcap', depends_on={EmailDataPoint})
    consumer = make_operator(
        'consumer', depends_on={EmailDataPoint}, requires={netcap}, produces={RiskDataPoint}, emits=[risk(0.3)]
    )

    replayed = await replay_session([_archived_email()], flow=_flow(consumer, capabilities=(netcap,)))

    assert replayed.result.status is SessionStatus.COMPLETED
    assert OperatorId('consumer') in replayed.result.operator_runs  # the required capability became available
    assert any(isinstance(dp, RiskDataPoint) and dp.value == 0.3 for dp in replayed.data_points)  # emission landed
    # auto-permitted AND activated with empty credentials (no live backend)
    assert netcap.activations == [{}]  # type: ignore[attr-defined]


async def test_replay_empty_archive_with_explicit_ids_runs_an_empty_session() -> None:
    # The 0/empty boundary: entries is empty but both ids are supplied, so the ReplayError guard is
    # NOT tripped and the orchestrator runs to a well-defined empty completion over zero seed data.
    replayed = await replay_session([], flow=_flow(), session_id=SID, namespace_id=NAMESPACE)

    assert replayed.result.status is SessionStatus.COMPLETED
    assert replayed.data_points == ()  # nothing was seeded, nothing was derived
    # The replayed durable state lives under the explicitly-supplied ids (not derived from entries[0],
    # which would IndexError on an empty archive).
    assert (await replayed.runtime.store.snapshot(SID)).all() == ()
    assert await replayed.runtime.lock.is_complete(SID) is True


async def test_replay_caller_supplied_restrictive_catalog_overrides_auto_permit() -> None:
    # When the caller injects a catalog, the auto-permit-all block is skipped and the supplied
    # catalog governs availability exactly as in a live run. A catalog that permits no capability
    # leaves the required netcap unavailable, so its dependent operator never launches.
    netcap = make_capability('netcap', depends_on={EmailDataPoint})
    consumer = make_operator(
        'consumer', depends_on={EmailDataPoint}, requires={netcap}, produces={RiskDataPoint}, emits=[risk(0.3)]
    )
    flow = _flow(consumer, capabilities=(netcap,))

    forbidden = await replay_session(
        [_archived_email()], flow=flow, catalog=InMemoryCapabilityCatalog(permitted={NAMESPACE: set()})
    )

    assert OperatorId('consumer') not in forbidden.result.operator_runs  # capability forbidden → never launched
    assert not any(isinstance(dp, RiskDataPoint) for dp in forbidden.data_points)  # its emission is absent
    assert netcap.activations == []  # type: ignore[attr-defined]  # never activated under the restrictive catalog

    # Contrast: the default (auto-permit) run of the same flow DOES launch the consumer.
    permitted = await replay_session([_archived_email()], flow=flow)
    assert OperatorId('consumer') in permitted.result.operator_runs


async def test_replay_caller_supplied_permissive_catalog_supplies_its_credentials() -> None:
    # The caller-governed branch must thread the injected catalog's credentials through to
    # activation (distinct from the auto-permit branch, which always supplies empty credentials).
    netcap = make_capability('netcap', depends_on={EmailDataPoint})
    consumer = make_operator(
        'consumer', depends_on={EmailDataPoint}, requires={netcap}, produces={RiskDataPoint}, emits=[risk(0.3)]
    )
    catalog = InMemoryCapabilityCatalog(
        permitted={NAMESPACE: {CapabilityId('netcap')}},
        credentials={(NAMESPACE, CapabilityId('netcap')): {'token': 'live'}},
    )

    replayed = await replay_session([_archived_email()], flow=_flow(consumer, capabilities=(netcap,)), catalog=catalog)

    assert OperatorId('consumer') in replayed.result.operator_runs
    # the injected catalog's credentials reached activation
    assert netcap.activations == [{'token': 'live'}]  # type: ignore[attr-defined]


async def test_replay_schema_drifted_value_surfaces_raw_validation_error() -> None:
    # A still-registered leaf ('risk' binds float) whose archived value drifted to a dict hits
    # parse_data_point's "known type, malformed payload" branch and re-raises the bare pydantic
    # ValidationError — it propagates out of replay_session uncaught (NOT wrapped as
    # UnknownDataPointTypeError/ReplayError, which the docstring's Raises section names). This pins
    # the currently-undocumented leak so a future contract change is a deliberate, visible decision.
    entry = ArchivedDataPoint(
        session_id=SID,
        namespace_id=NAMESPACE,
        type='risk',
        value={'x': 1},  # schema drift: 'risk' binds float, not a dict
        retrieved_by=OperatorId('old_scorer'),
        first_retrieved=T0,
        last_retrieved=T0,
        is_pii=False,
        epoch=Epoch(1),
    )
    with pytest.raises(ValidationError):
        await replay_session([entry], flow=_flow())
    # And it is NOT mapped onto either documented replay exception.
    assert not issubclass(ValidationError, (UnknownDataPointTypeError, ReplayError))


async def test_replay_is_equivalent_to_the_original_run_and_idempotent_under_re_replay(fake_clock: FakeClock) -> None:
    # SYSTEMIC: replay-equivalence + replay-of-a-replay idempotency. Replaying an archived session
    # under the SAME flow must reproduce the original run's durable documents, and replaying the
    # replay's own archive again must produce byte-identical durable output (a stable fixpoint).
    runtime = build_in_memory_runtime(fake_clock)
    scorer = make_operator('scorer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk(0.8)])
    report = make_aggregator('report', depends_on={RiskDataPoint}, on_aggregate=_write_risk_count)
    flow = _flow(scorer, report)
    original = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[scorer, report], seed=[work_email()]
    ).run()
    assert original.status is SessionStatus.COMPLETED
    original_document = await runtime.durable.read('reports', 'report')
    assert original_document is not None
    archived = await runtime.archive.read(SID)

    first_replay = await replay_session(archived, flow=flow, clock=FakeClock())
    first_doc = await first_replay.runtime.durable.read('reports', 'report')
    assert first_doc is not None and first_doc.document == original_document.document  # replay-equivalence

    # Re-replay the FIRST replay's own archive: a second derivation must land on the same document.
    re_archived = await first_replay.runtime.archive.read(SID)
    second_replay = await replay_session(re_archived, flow=flow, clock=FakeClock())
    second_doc = await second_replay.runtime.durable.read('reports', 'report')
    assert second_doc is not None and second_doc.document == first_doc.document  # idempotent under re-replay
