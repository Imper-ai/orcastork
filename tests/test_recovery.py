"""RECOV — end-to-end crash-recovery scenarios (in-memory + a simulated kill switch)."""

from __future__ import annotations

from typing import Any

import pytest

from orcastork.adapters.memory import InMemoryDataPointStore, InMemoryInbox
from orcastork.aggregation import RetryPolicy
from orcastork.datapoints import parse_data_point
from orcastork.exceptions import StaleEpochError
from orcastork.flow import FlowDefinition
from orcastork.ids import Epoch, NamespaceId, OperatorId, SessionId
from orcastork.manager import SessionOrchestrationManager
from orcastork.operators import Operator, OperatorContext
from orcastork.orchestrator import Orchestrator
from orcastork.orchestrator.orchestrator import SessionStatus
from orcastork.runtime import OrchestratorRuntime, build_in_memory_runtime

from .doubles.clock import FakeClock
from .doubles.datapoints import (
    T0,
    ChatAnswerDataPoint,
    EmailDataPoint,
    IpDataPoint,
    RiskDataPoint,
    chat_answer,
    ip,
    risk,
    work_email,
)
from .doubles.operators import make_aggregator, make_operator

SID = SessionId('recov-session')
NAMESPACE = NamespaceId('recov-namespace')


def _flow(*operators: type[Operator], **kwargs: Any) -> FlowDefinition:
    return FlowDefinition(name='recov-flow', operators=tuple(operators), **kwargs)


async def _crash_after_seed(runtime: OrchestratorRuntime, clock: FakeClock) -> Epoch:
    """A predecessor held an epoch and wrote the seed, then died; its lock then expires."""
    epoch = await runtime.lock.acquire(SID)
    await runtime.store.write(SID, [work_email()], epoch=epoch)
    clock.advance(31.0)  # ownership lock TTL expires
    return epoch


async def test_recov_01_operator_failure_is_isolated(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    boom = make_operator('boom', produces={IpDataPoint}, emits=[ip()], raise_error=ValueError('x'))
    healthy = make_operator('healthy', produces={RiskDataPoint}, emits=[risk()])
    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[boom, healthy]
    ).run()
    types = {dp.type for dp in (await runtime.store.snapshot(SID)).all()}
    assert result.status is SessionStatus.COMPLETED  # scheduler proceeded
    assert 'ip' in types and 'risk' in types  # failed op's emission persisted; peer ran


async def test_recov_02_resume_redelivers_inbox_entries(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    epoch = await runtime.lock.acquire(SID)
    await runtime.store.write(SID, [work_email()], epoch=epoch)
    await runtime.inbox.append(SID, chat_answer('pending'))  # arrived but never processed
    fake_clock.advance(31.0)

    scorer = make_operator('scorer', depends_on={ChatAnswerDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    await manager.resume(session_id=SID, namespace_id=NAMESPACE, flow=_flow(scorer))
    types = {dp.type for dp in (await runtime.store.snapshot(SID)).all()}
    assert 'chat_answer' in types  # the inbox entry was not lost — redelivered on resume
    assert 'risk' in types  # and the operator it unblocked ran


async def test_recov_03_stale_orchestrator_writes_rejected(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    stale_epoch = await _crash_after_seed(runtime, fake_clock)
    scorer = make_operator('scorer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    await manager.resume(session_id=SID, namespace_id=NAMESPACE, flow=_flow(scorer))
    with pytest.raises(StaleEpochError):
        await runtime.store.write(SID, [ip()], epoch=stale_epoch)  # fenced predecessor


async def test_recov_04_resume_reruns_only_unfinished_aggregators(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    calls = {'n': 0}

    async def write(ctx: OperatorContext) -> None:
        calls['n'] += 1
        assert ctx.aggregation is not None
        await ctx.aggregation.upsert('reports', 'report', {'v': 1})

    scorer = make_operator('scorer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    reporter = make_aggregator('rep', depends_on={RiskDataPoint}, on_aggregate=write)
    await manager.start_session(
        session_id=SID, namespace_id=NAMESPACE, flow=_flow(scorer, reporter), seed=[work_email()]
    )
    assert calls['n'] == 1
    # A redundant resume (same session) re-runs nothing — the aggregator's contribution is marked.
    await Orchestrator(session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[scorer, reporter]).run()
    assert calls['n'] == 1


async def test_recov_05_aggregator_dead_letter_completes_with_flag(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)

    async def boom(ctx: OperatorContext) -> None:  # noqa: ARG001
        raise ValueError('cannot aggregate')

    scorer = make_operator('scorer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    failing = make_aggregator('failing', depends_on={RiskDataPoint}, on_aggregate=boom)
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[scorer, failing],
        seed=[work_email()],
        retry_policy=RetryPolicy(max_attempts=2, base_delay=0.0),
    ).run()
    assert result.status is SessionStatus.COMPLETED
    assert {dead.operator_id for dead in result.dead_letters} == {OperatorId('failing')}


async def test_recov_06_audit_survives_live_store_loss(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    scorer = make_operator('scorer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[scorer], seed=[work_email()]
    ).run()
    assert await runtime.audit.replay(SID)  # the audit is durable
    # Losing the live store loses the live session, but the audit is independent of it.
    assert len((await InMemoryDataPointStore().snapshot(SID)).all()) == 0
    assert await runtime.audit.replay(SID)


async def test_recov_07_resume_is_deterministic(fake_clock: FakeClock) -> None:  # noqa: ARG001
    scorer = make_operator('scorer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    operators = [scorer]

    clean = build_in_memory_runtime(FakeClock())
    await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=clean, operators=operators, seed=[work_email()]
    ).run()
    clean_types = {dp.type for dp in (await clean.store.snapshot(SID)).all()}

    crash_clock = FakeClock()
    crashed = build_in_memory_runtime(crash_clock)
    await _crash_after_seed(crashed, crash_clock)
    await SessionOrchestrationManager(crashed).resume(session_id=SID, namespace_id=NAMESPACE, flow=_flow(*operators))
    resumed_types = {dp.type for dp in (await crashed.store.snapshot(SID)).all()}

    assert clean_types == resumed_types  # identical readiness/availability ⇒ identical final state


def test_recov_08_tolerant_reader_ignores_unknown_fields() -> None:
    # Rolling deploy: a record written by newer code carries a field this reader does not know.
    raw = {
        'type': 'work_email',
        'value': 'a@e.example',
        'retrieved_by': 'op',
        'first_retrieved': T0,
        'last_retrieved': T0,
        'field_from_a_newer_version': 'ignored',
    }
    restored = parse_data_point(raw)
    assert restored.type == 'work_email'
    assert not hasattr(restored, 'field_from_a_newer_version')


async def test_recov_09_resume_reclaims_claimed_but_unapplied_inbox_entry(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    epoch = await runtime.lock.acquire(SID)
    await runtime.store.write(SID, [work_email()], epoch=epoch)
    await runtime.inbox.append(SID, chat_answer('in-flight'))
    await runtime.inbox.consume(SID)  # predecessor CLAIMED it, then died before applying/acking
    fake_clock.advance(31.0)  # ownership lock TTL expires

    scorer = make_operator('scorer', depends_on={ChatAnswerDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    await manager.resume(session_id=SID, namespace_id=NAMESPACE, flow=_flow(scorer))
    types = {dp.type for dp in (await runtime.store.snapshot(SID)).all()}
    assert 'chat_answer' in types  # the claimed-but-unapplied entry was reclaimed on resume, not lost
    assert 'risk' in types  # and the operator it unblocked ran


async def test_recov_10_poison_inbox_entry_is_quarantined_on_resume_not_a_crash_loop(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    epoch = await runtime.lock.acquire(SID)
    await runtime.store.write(SID, [work_email()], epoch=epoch)
    inbox = runtime.inbox
    assert isinstance(inbox, InMemoryInbox)
    # A producer wrote bytes no deployment can decode (poison == bad wire payload); the
    # predecessor claimed the entry, then died before disposing of it.
    poison_id = await inbox.append_serialized(SID, 'not-json{')
    await inbox.consume(SID)
    fake_clock.advance(31.0)  # ownership lock TTL expires

    scorer = make_operator('scorer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    result = await manager.resume(session_id=SID, namespace_id=NAMESPACE, flow=_flow(scorer))
    assert result is not None
    assert result.status is SessionStatus.COMPLETED  # the resume disposed of the poison instead of crashing
    (record,) = await runtime.inbox.quarantined(SID)
    assert record.entry_id == poison_id
    assert await runtime.inbox.pending_count(SID) == 0  # nothing left for the next resume to re-present
    assert 'risk' in {dp.type for dp in (await runtime.store.snapshot(SID)).all()}  # the session still progressed
