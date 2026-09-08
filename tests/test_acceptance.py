"""ACC — Tier-0 walking-skeleton acceptance tests (in-memory + FakeClock).

These are the headline end-to-end behaviours that prove the engine exists. ACC-04 (kill +
resume) lands with the manager (M9); ACC-06 (aggregator dead-letter) with M8.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from itertools import count
from typing import Any

import pytest

from orcastork.adapters.memory import (
    InMemoryCapabilityCatalog,
    InMemoryDataPointArchive,
    InMemorySessionLock,
)
from orcastork.aggregation import RetryPolicy
from orcastork.audit import AuditKind
from orcastork.exceptions import StaleEpochError
from orcastork.flow import FlowDefinition
from orcastork.ids import CapabilityId, Epoch, NamespaceId, OperatorId, SessionId
from orcastork.manager import SessionOrchestrationManager
from orcastork.operators import OperatorContext
from orcastork.orchestrator import Orchestrator, SessionStatus
from orcastork.runtime import build_in_memory_runtime

from .doubles.capabilities import make_capability
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
from .doubles.logs import capture_logs
from .doubles.operators import make_aggregator, make_operator

SID = SessionId('acc-session')
NAMESPACE = NamespaceId('acc-namespace')


async def test_acc_01_headline_seed_operator_emit_quiesce_aggregate_complete(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)

    async def write_report(ctx: OperatorContext) -> None:
        assert ctx.aggregation is not None
        await ctx.aggregation.upsert('reports', 'risk-report', {'risk_count': len(ctx.store.of_type(RiskDataPoint))})

    scorer = make_operator('scorer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk(0.9)])
    reporter = make_aggregator('risk_report', depends_on={RiskDataPoint}, on_aggregate=write_report)
    orchestrator = Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[scorer, reporter], seed=[work_email()]
    )

    result = await orchestrator.run()

    assert result.status is SessionStatus.COMPLETED
    assert result.operator_runs[OperatorId('scorer')] == 1  # ran exactly once
    document = await runtime.durable.read('reports', 'risk-report')
    assert document is not None and document.document == {'risk_count': 1}  # durable output written
    assert not await runtime.lock.is_held(SID)  # epoch released
    assert AuditKind.DATA_POINT_ADDED in {entry.kind for entry in await runtime.audit.replay(SID)}


async def test_acc_02_capability_coming_online_unblocks_operator(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(
        fake_clock, catalog=InMemoryCapabilityCatalog(permitted={NAMESPACE: {CapabilityId('netcap')}})
    )
    seeder = make_operator('seeder', produces={IpDataPoint}, emits=[ip()])
    netcap = make_capability('netcap', depends_on={IpDataPoint})  # available once an Ip exists
    consumer = make_operator('netconsumer', requires={netcap}, produces={RiskDataPoint}, emits=[risk()])
    orchestrator = Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[seeder, consumer],
        capabilities=[netcap],
    )

    result = await orchestrator.run()

    assert result.status is SessionStatus.COMPLETED
    assert result.operator_runs[OperatorId('netconsumer')] == 1  # unblocked once netcap came online
    assert {dp.type for dp in (await runtime.store.snapshot(SID)).all()} == {'ip', 'risk'}


async def test_acc_03_inbox_datapoint_reflected_in_aggregate(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)

    def score(ctx: OperatorContext) -> list[Any]:
        answers = ctx.store.of_type(ChatAnswerDataPoint)
        return [
            RiskDataPoint(
                value=float(len(answers)), retrieved_by=OperatorId('scorer'), first_retrieved=T0, last_retrieved=T0
            )
        ]

    async def write_report(ctx: OperatorContext) -> None:
        assert ctx.aggregation is not None
        latest = ctx.latest(RiskDataPoint)
        await ctx.aggregation.upsert('reports', 'risk', {'score': None if latest is None else latest.value})

    scorer = make_operator(
        'scorer',
        depends_on={ChatAnswerDataPoint},
        produces={RiskDataPoint},
        rerun_on_new_data=True,
        emit_factory=score,
    )
    reporter = make_aggregator('risk_report', depends_on={RiskDataPoint}, on_aggregate=write_report)
    # A user action arrives via the durable inbox.
    await runtime.inbox.append(SID, chat_answer('second'))
    orchestrator = Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[scorer, reporter],
        seed=[chat_answer('first')],
    )

    result = await orchestrator.run()

    assert result.status is SessionStatus.COMPLETED
    document = await runtime.durable.read('reports', 'risk')
    assert document is not None and document.document == {'score': 2.0}  # both seed + inbox answers reflected


async def test_acc_05_bounded_cycle_converges_and_breaker_halts(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    ip_counter, risk_counter = count(), count()

    def emit_ip(ctx: OperatorContext) -> list[Any]:  # noqa: ARG001
        return [
            IpDataPoint(
                value=f'ip-{next(ip_counter)}', retrieved_by=OperatorId('cyc_a'), first_retrieved=T0, last_retrieved=T0
            )
        ]

    def emit_risk(ctx: OperatorContext) -> list[Any]:  # noqa: ARG001
        return [
            RiskDataPoint(
                value=float(next(risk_counter)),
                retrieved_by=OperatorId('cyc_b'),
                first_retrieved=T0,
                last_retrieved=T0,
            )
        ]

    op_a = make_operator(
        'cyc_a',
        produces={IpDataPoint},
        depends_on={RiskDataPoint},
        rerun_on_new_data=True,
        max_cycles=2,
        emit_factory=emit_ip,
    )
    op_b = make_operator(
        'cyc_b',
        produces={RiskDataPoint},
        depends_on={IpDataPoint},
        rerun_on_new_data=True,
        max_cycles=2,
        emit_factory=emit_risk,
    )
    orchestrator = Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[op_a, op_b], seed=[ip('seed')]
    )

    result = await orchestrator.run()

    assert result.status is SessionStatus.COMPLETED  # the loop terminated (no wedge)
    assert 1 <= result.operator_runs[OperatorId('cyc_a')] <= 2  # bounded by the circuit-breaker cap
    assert 1 <= result.operator_runs[OperatorId('cyc_b')] <= 2


async def test_acc_06_failing_aggregator_dead_lettered_but_session_completes(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)

    async def boom(ctx: OperatorContext) -> None:  # noqa: ARG001
        raise ValueError('cannot aggregate')

    async def healthy(ctx: OperatorContext) -> None:
        assert ctx.aggregation is not None
        await ctx.aggregation.upsert('reports', 'healthy-report', {'ok': True})

    scorer = make_operator('scorer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    failing = make_aggregator('failing_report', depends_on={RiskDataPoint}, on_aggregate=boom)
    healthy_report = make_aggregator('healthy_report', depends_on={RiskDataPoint}, on_aggregate=healthy)
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[scorer, failing, healthy_report],
        seed=[work_email()],
        retry_policy=RetryPolicy(max_attempts=2, base_delay=0.0),
    ).run()

    assert result.status is SessionStatus.COMPLETED  # session reaches COMPLETED despite the failure
    assert any(dead.operator_id == OperatorId('failing_report') for dead in result.dead_letters)  # dead-lettered
    assert await runtime.durable.read('reports', 'healthy-report') is not None  # other aggregator unaffected


async def test_acc_04_kill_mid_gathering_and_resume_yields_identical_output() -> None:
    async def write_report(ctx: OperatorContext) -> None:
        assert ctx.aggregation is not None
        await ctx.aggregation.upsert('reports', 'risk-report', {'risk_count': len(ctx.store.of_type(RiskDataPoint))})

    # Same operator classes drive both runs (the registry holds one definition).
    scorer = make_operator('scorer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk(0.9)])
    reporter = make_aggregator('risk_report', depends_on={RiskDataPoint}, on_aggregate=write_report)
    operators = [scorer, reporter]

    # Baseline: a clean, uninterrupted run.
    clean = build_in_memory_runtime(FakeClock())
    await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=clean, operators=operators, seed=[work_email()]
    ).run()
    clean_report = await clean.durable.read('reports', 'risk-report')

    # Crash mid-gathering: a predecessor held epoch 1 and wrote the seed, then died (lock expires).
    crash_clock = FakeClock()
    crashed = build_in_memory_runtime(crash_clock)
    predecessor_epoch = await crashed.lock.acquire(SID)
    await crashed.store.write(SID, [work_email()], epoch=predecessor_epoch)
    crash_clock.advance(31.0)
    manager = SessionOrchestrationManager(crashed)

    resumed = await manager.resume(
        session_id=SID, namespace_id=NAMESPACE, flow=FlowDefinition(name='acc-flow', operators=tuple(operators))
    )
    resumed_report = await crashed.durable.read('reports', 'risk-report')

    assert resumed is not None and resumed.epoch == Epoch(2)  # resumed under a higher epoch
    assert clean_report is not None and resumed_report is not None
    assert clean_report.document == resumed_report.document  # identical durable output


class _FlushFailingArchive(InMemoryDataPointArchive):
    """Archive whose ``flush`` raises — a fault on the run()-finally cleanup path."""

    async def flush(self, session_id: SessionId) -> int:  # noqa: ARG002
        raise RuntimeError('archive flush exploded')


async def test_acc_08_archive_flush_failure_on_cleanup_still_releases_the_epoch(fake_clock: FakeClock) -> None:
    # The run()-finally flushes the write-behind archive, then releases the lock — a nested try/finally
    # whose whole point is that a flush raising still lets lock.release run. A stranded epoch would
    # block recovery for the full lease TTL. With archive.flush raising, the exception surfaces (it is
    # not silently swallowed) but the epoch MUST still be released.
    runtime = replace(build_in_memory_runtime(fake_clock), archive=_FlushFailingArchive())
    scorer = make_operator('scorer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    orchestrator = Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[scorer], seed=[work_email()]
    )

    with pytest.raises(RuntimeError, match='archive flush exploded'):  # the flush fault is not masked away
        await orchestrator.run()
    assert not await runtime.lock.is_held(SID)  # released even though the archive flush raised
    # The audit is a separate, independently durable write path, so the trail of the run that failed
    # its archive flush is intact — the durable record of a failure must survive the failure.
    assert AuditKind.DATA_POINT_ADDED in {entry.kind for entry in await runtime.audit.replay(SID)}


class _FenceOnRenewLock(InMemorySessionLock):
    """Lock whose renew always reports a takeover — drives the run into a SUPERSEDED stop."""

    async def renew(self, session_id: SessionId, *, epoch: Epoch) -> None:  # noqa: ARG002
        raise StaleEpochError('a higher epoch took over')


async def test_acc_09_flush_failure_during_a_superseded_stop_still_releases_the_epoch(fake_clock: FakeClock) -> None:
    # Error-during-error on the cleanup path: the run is ALREADY ending SUPERSEDED (a fenced renew)
    # when archive.flush ALSO raises in the finally. The lock must still be released — a held epoch on
    # a superseded predecessor would strand the session against its own successor for the lease TTL.
    runtime = replace(
        build_in_memory_runtime(fake_clock), lock=_FenceOnRenewLock(fake_clock), archive=_FlushFailingArchive()
    )
    self_cycle = make_operator(
        'selfloop',
        produces={IpDataPoint},
        depends_on={IpDataPoint},
        rerun_on_new_data=True,
        max_cycles=3,
        debounce=timedelta(seconds=20),  # forces a loop sleep that crosses the renew interval → fenced renew
        emit_factory=lambda _ctx: [ip('looped')],  # noqa: ARG005
    )
    orchestrator = Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[self_cycle], seed=[ip('seed')]
    )

    with capture_logs(level='WARNING') as records, pytest.raises(RuntimeError, match='archive flush exploded'):
        await orchestrator.run()
    assert any('fenced by a higher epoch' in r['message'] for r in records)  # the SUPERSEDED stop was reached
    assert not await runtime.lock.is_held(SID)  # release ran despite the flush fault during the fenced stop
