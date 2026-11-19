"""LLS — what a session that lives for minutes to hours, across pods, relies on.

The lease is renewed while operators run, a DataPoint type may declare its identity, a park
rechecks the inbox, ``start_session`` never starts a second run of an existing session, and an
audit sink that fails never stalls one. The scheduler fixes ported from ``orcastork_lite`` live
here too: a hard session deadline, an armed retry made terminal, windows honoured while operators
run, and identity computed once.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Iterable
from dataclasses import replace
from datetime import timedelta
from typing import Any

import pytest
from fakeredis.aioredis import FakeRedis

from orcastork.adapters.memory import InMemoryAuditSink, InMemoryCapabilityCatalog, InMemoryInbox
from orcastork.adapters.redis import RedisSessionLock, build_redis_runtime
from orcastork.aggregation import RetryPolicy
from orcastork.audit import AuditLogEntry
from orcastork.datapoints import BaseDataPoint, DataPointEmission, DataPointSet, MergeKind
from orcastork.exceptions import UnhashableValueError
from orcastork.flow import FlowDefinition
from orcastork.ids import CapabilityId, Epoch, NamespaceId, OperatorId, SessionId
from orcastork.manager import SessionOrchestrationManager
from orcastork.operators import Operator, OperatorContext, OperatorPolicy
from orcastork.orchestrator import Orchestrator, SessionStatus
from orcastork.runtime import OrchestratorRuntime, build_in_memory_runtime

from .doubles.capabilities import make_capability
from .doubles.clock import FakeClock
from .doubles.datapoints import (
    DEFAULT_OP,
    T0,
    ChatAnswerDataPoint,
    EmailDataPoint,
    IpDataPoint,
    RawDataPoint,
    RiskDataPoint,
    Turn,
    TurnDataPoint,
    chat_answer,
    work_email,
)
from .doubles.logs import capture_logs
from .doubles.operators import make_operator
from .doubles.redis_support import TimeControlledServer

SID = SessionId('lls-session')
NAMESPACE = NamespaceId('lls-namespace')
# A bound on how long a test waits for something the engine must do on its own; never a pacing sleep.
_GUARD_SECONDS = 5.0


def _turn(index: int, kind: str) -> TurnDataPoint:
    return TurnDataPoint(
        value=Turn(index=index, kind=kind), retrieved_by=DEFAULT_OP, first_retrieved=T0, last_retrieved=T0
    )


def _turns(points: Iterable[BaseDataPoint[Any]]) -> set[tuple[int, str]]:
    return {(point.value.index, point.value.kind) for point in points if isinstance(point, TurnDataPoint)}


def _blocking_operator(
    operator_id: str, started: asyncio.Event, release: asyncio.Event, *, emits: DataPointEmission | None = None
) -> type[Operator]:
    class _Blocking(Operator):
        policy = OperatorPolicy(rerun_on_new_data=False)
        depends_on = frozenset({EmailDataPoint})
        produces = frozenset({RiskDataPoint})

        async def run(self, _ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:
            started.set()
            await release.wait()
            if emits is not None:
                yield emits

    _Blocking.operator_id = OperatorId(operator_id)
    return _Blocking


def _orchestrator(runtime: OrchestratorRuntime, operators: Iterable[type[Operator]], **kwargs: Any) -> Orchestrator:
    return Orchestrator(session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=operators, **kwargs)


# --- the lease is renewed while operators run ---------------------------------------------------------------


async def test_lls_01_an_operator_running_past_the_lock_ttl_keeps_its_session(
    fake_clock: FakeClock, redis_client: FakeRedis, redis_server: TimeControlledServer
) -> None:
    renewed = asyncio.Event()

    class _ObservedLock(RedisSessionLock):
        async def renew(self, session_id: SessionId, *, epoch: Epoch) -> None:
            await super().renew(session_id, epoch=epoch)
            renewed.set()

    started, release = asyncio.Event(), asyncio.Event()
    slow = _blocking_operator('slow', started, release, emits=RiskDataPoint.emit(0.5))
    runtime = replace(build_redis_runtime(redis_client, clock=fake_clock), lock=_ObservedLock(redis_client))
    run = asyncio.create_task(
        _orchestrator(runtime, [slow], seed=[work_email()], lease_renew_interval=0.01, operation_timeout=60.0).run()
    )
    await asyncio.wait_for(started.wait(), _GUARD_SECONDS)

    # 60s of lease time against a 30s TTL, all of it with the operator in flight.
    for _ in range(3):
        redis_server.advance(20.0)
        renewed.clear()
        await asyncio.wait_for(renewed.wait(), _GUARD_SECONDS)

    other_pod = SessionOrchestrationManager(build_redis_runtime(redis_client, clock=fake_clock))
    flow = FlowDefinition(name='lls-flow', operators=(slow,))
    assert await other_pod.resume(session_id=SID, namespace_id=NAMESPACE, flow=flow) is None  # it stands down
    release.set()
    result = await run
    assert result.status is SessionStatus.COMPLETED and result.epoch == 1
    assert result.operator_runs == {OperatorId('slow'): 1}


# --- identity by declared fields ----------------------------------------------------------------------------


def test_lls_02_a_declared_identity_keeps_its_first_value() -> None:
    points = DataPointSet([_turn(0, 'wait')])
    assert points.add(_turn(0, 'auto')).kind is MergeKind.DROPPED
    assert points.add(_turn(0, 'wait')).kind is MergeKind.UPDATED  # the identical value still refreshes
    assert _turns(points) == {(0, 'wait')}


@pytest.mark.parametrize('backend', ['memory', 'redis'])
async def test_lls_03_two_values_with_one_declared_identity_store_one_data_point(
    backend: str, fake_clock: FakeClock, redis_client: FakeRedis
) -> None:
    runtime = (
        build_in_memory_runtime(fake_clock)
        if backend == 'memory'
        else build_redis_runtime(redis_client, clock=fake_clock)
    )
    walk = make_operator(
        'walk',
        depends_on={EmailDataPoint},
        produces={TurnDataPoint},
        emits=[TurnDataPoint.emit(Turn(index=0, kind='auto')), TurnDataPoint.emit(Turn(index=1, kind='auto'))],
    )
    with capture_logs() as records:
        result = await _orchestrator(runtime, [walk], seed=[work_email(), _turn(0, 'wait')]).run()

    assert result.status is SessionStatus.COMPLETED
    assert _turns((await runtime.store.snapshot(SID)).all()) == {(0, 'wait'), (1, 'auto')}
    assert any('identity is already held' in record['message'] for record in records)


# --- identity computed once, unhashable values refused (ported from lite) -----------------------------------


def test_lls_04_identity_is_computed_once_and_survives_reobservation() -> None:
    point = _turn(3, 'wait')
    assert point.identity is point.identity
    assert point.reobserved(T0 + timedelta(seconds=1)).identity == point.identity == ('turn', (('index', 3),))


def test_lls_05_mixed_sets_and_model_values_get_a_stable_identity() -> None:
    def raw(value: Any) -> RawDataPoint:
        return RawDataPoint(value=value, retrieved_by=DEFAULT_OP, first_retrieved=T0, last_retrieved=T0)

    assert raw({1, 'a', (2, 3)}) == raw({(2, 3), 'a', 1})
    assert raw(Turn(index=1, kind='a')) == raw(Turn(index=1, kind='a'))
    assert raw(Turn(index=1, kind='a')) != raw(Turn(index=1, kind='b'))
    with pytest.raises(UnhashableValueError, match='bytearray'):
        raw({'nested': [bytearray(b'x')]})


async def test_lls_06_an_unhashable_emission_fails_only_its_operator(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    careless = make_operator(
        'careless', depends_on={EmailDataPoint}, produces={RawDataPoint}, emits=[RawDataPoint.emit(bytearray(b'x'))]
    )
    peer = make_operator(
        'peer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[RiskDataPoint.emit(0.1)]
    )

    result = await _orchestrator(runtime, [careless, peer], seed=[work_email()]).run()

    assert result.operator_runs == {OperatorId('careless'): 1, OperatorId('peer'): 1}
    present = {type(point) for point in (await runtime.store.snapshot(SID)).all()}
    assert RiskDataPoint in present and RawDataPoint not in present


# --- an armed retry made terminal when its operator loses readiness (ported from lite) ----------------------


class _RevocableCatalog(InMemoryCapabilityCatalog):
    def revoke(self, namespace_id: NamespaceId) -> None:
        self._permitted[namespace_id] = frozenset()


async def test_lls_07_an_armed_retry_becomes_terminal_when_its_operator_loses_readiness(fake_clock: FakeClock) -> None:
    intel = make_capability('intel')
    catalog = _RevocableCatalog(permitted={NAMESPACE: {CapabilityId('intel')}})

    def revoke(_ctx: OperatorContext) -> list[DataPointEmission]:
        catalog.revoke(NAMESPACE)  # the namespace drops the capability while the retry window is armed
        return []

    flaky = make_operator(
        'flaky',
        depends_on={EmailDataPoint},
        requires={intel},
        raise_error=RuntimeError('boom'),
        retry=RetryPolicy(max_attempts=3, base_delay=1.0, jitter=0.0),
    )
    revoker = make_operator('revoker', depends_on={EmailDataPoint}, emit_factory=revoke)
    runtime = build_in_memory_runtime(fake_clock, catalog=catalog)

    with capture_logs() as records:
        result = await _orchestrator(runtime, [flaky, revoker], capabilities=[intel], seed=[work_email()]).run()

    assert result.operator_runs == {OperatorId('flaky'): 1, OperatorId('revoker'): 1}
    assert any('lost readiness' in record['message'] for record in records)
    # Terminal like any failure: the watermark the retry held back advances to what the failed run saw.
    assert await runtime.store.get_watermark(SID, OperatorId('flaky')) is not None
    assert fake_clock.monotonic() == 0.0  # nothing waited out a window that could never fire


# --- a hard session deadline (ported from lite) -------------------------------------------------------------


async def test_lls_08_the_session_deadline_is_hard_while_operators_run(fake_clock: FakeClock) -> None:
    # Nothing advances the fake clock, so only the run's timeout, clipped to the budget left, can end it.
    sleeper = make_operator('sleeper', depends_on={EmailDataPoint}, sleep_after=_GUARD_SECONDS)
    quick = make_operator(
        'quick', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[RiskDataPoint.emit(1.0)]
    )
    runtime = build_in_memory_runtime(fake_clock)

    result = await _orchestrator(runtime, [sleeper, quick], seed=[work_email()], session_deadline=0.05).run()

    assert result.status is SessionStatus.COMPLETED
    assert result.operator_runs == {OperatorId('quick'): 1}  # the sleeper never finished a run of its own
    assert RiskDataPoint in {type(point) for point in (await runtime.store.snapshot(SID)).all()}


async def test_lls_09_an_operator_that_finishes_during_the_last_pass_is_recorded(fake_clock: FakeClock) -> None:
    peer_may_finish = asyncio.Event()

    class SlowA(Operator):
        operator_id = OperatorId('slow_a')
        policy = OperatorPolicy(rerun_on_new_data=False)
        depends_on = frozenset({EmailDataPoint})
        produces = frozenset({IpDataPoint})

        async def run(self, _ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:
            yield IpDataPoint.emit('10.0.0.1')
            await asyncio.sleep(_GUARD_SECONDS)  # still in flight when the deadline passes

    class QuickB(Operator):
        operator_id = OperatorId('quick_b')
        policy = OperatorPolicy(rerun_on_new_data=False)
        depends_on = frozenset({EmailDataPoint})
        produces = frozenset({RiskDataPoint})

        async def run(self, _ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:
            await peer_may_finish.wait()
            yield RiskDataPoint.emit(0.2)

    class _GatingAudit(InMemoryAuditSink):
        async def append(self, entry: AuditLogEntry) -> None:
            await super().append(entry)
            if entry.data_point is not None and entry.data_point.data_point_type == 'ip':
                # While the loop records A's merge, B finishes; then the deadline passes.
                peer_may_finish.set()
                for _ in range(5):
                    await asyncio.sleep(0)
                fake_clock.advance(1.0)

    runtime = replace(build_in_memory_runtime(fake_clock), audit=_GatingAudit())
    result = await _orchestrator(runtime, [SlowA, QuickB], seed=[work_email()], session_deadline=0.2).run()

    assert result.status is SessionStatus.COMPLETED
    assert result.operator_runs == {OperatorId('quick_b'): 1}  # finished, so counted, not swept as in flight
    assert await runtime.store.get_watermark(SID, OperatorId('quick_b')) is not None


# --- armed windows honoured while operators run (ported from lite) ------------------------------------------


async def test_lls_10_an_armed_window_fires_while_a_silent_operator_is_still_running(fake_clock: FakeClock) -> None:
    # The blocker never emits and outlives the session, so the rerunner's window can only come due if the
    # loop honours it while the blocker is in flight. The deadline, which clips the blocker, ends the session.
    blocker = make_operator('blocker', depends_on={EmailDataPoint}, sleep_after=_GUARD_SECONDS)
    trigger = make_operator(
        'trigger', depends_on={EmailDataPoint}, produces={IpDataPoint}, emits=[IpDataPoint.emit('2')]
    )
    rerunner = make_operator(
        'rerunner',
        depends_on={EmailDataPoint},
        uses={IpDataPoint},
        rerun_on_new_data=True,
        debounce=timedelta(seconds=0.2),
    )
    runtime = build_in_memory_runtime(fake_clock)

    result = await _orchestrator(
        runtime, [blocker, trigger, rerunner], seed=[work_email()], session_deadline=0.3
    ).run()

    assert result.operator_runs[OperatorId('rerunner')] == 2  # the debounced rerun happened
    assert OperatorId('blocker') not in result.operator_runs
    assert fake_clock.monotonic() == pytest.approx(0.2)  # fast-forwarded to the window, not the blocker's exit


# --- a park rechecks the inbox before it releases the lease -------------------------------------------------


class _DeliversAsTheSessionParks(InMemoryInbox):
    """Appends one entry the moment the park decision cancels the inbox wait — before the lease is released."""

    def __init__(self, late: BaseDataPoint[Any]) -> None:
        super().__init__()
        self._late: BaseDataPoint[Any] | None = late

    async def wait_for_entry(self, session_id: SessionId) -> None:
        try:
            await asyncio.Event().wait()
        finally:
            if self._late is not None:
                late, self._late = self._late, None
                await self.append(session_id, late)


async def test_lls_11_a_delivery_landing_as_the_session_parks_is_processed(fake_clock: FakeClock) -> None:
    runtime = replace(build_in_memory_runtime(fake_clock), inbox=_DeliversAsTheSessionParks(chat_answer('late')))
    scorer = make_operator(
        'scorer', depends_on={ChatAnswerDataPoint}, produces={RiskDataPoint}, emits=[RiskDataPoint.emit(0.3)]
    )

    result = await _orchestrator(runtime, [scorer], completes_when=RiskDataPoint, park_after=5.0).run()

    assert result.status is SessionStatus.COMPLETED  # gathered on the same epoch, never parked
    assert result.operator_runs == {OperatorId('scorer'): 1}
    assert await runtime.inbox.pending_count(SID) == 0


# --- start_session on an existing session -------------------------------------------------------------------


def _counting_scorer(runs: list[int]) -> type[Operator]:
    def score(_ctx: OperatorContext) -> list[DataPointEmission]:
        runs.append(1)
        return [RiskDataPoint.emit(0.4)]

    return make_operator('scorer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emit_factory=score)


async def test_lls_12_start_session_returns_a_completed_session_as_it_is(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    runs: list[int] = []
    flow = FlowDefinition(name='lls-flow', operators=(_counting_scorer(runs),))

    first = await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])
    again = await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])

    assert first.status is SessionStatus.COMPLETED and first.epoch == 1
    assert again.status is SessionStatus.COMPLETED and again.epoch == 1 and again.operator_runs == {}
    assert runs == [1] and await runtime.lock.current_epoch(SID) == 1  # no second run, no new epoch


async def test_lls_13_start_session_returns_a_live_session_as_running(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    started, release = asyncio.Event(), asyncio.Event()
    flow = FlowDefinition(name='lls-flow', operators=(_blocking_operator('slow', started, release),))
    live = asyncio.create_task(
        manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])
    )
    await asyncio.wait_for(started.wait(), _GUARD_SECONDS)

    again = await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])
    release.set()

    assert again.status is SessionStatus.RUNNING and again.epoch == 1
    assert (await live).status is SessionStatus.COMPLETED


async def test_lls_14_start_session_resumes_a_parked_session(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    scorer = make_operator(
        'scorer', depends_on={ChatAnswerDataPoint}, produces={RiskDataPoint}, emits=[RiskDataPoint.emit(0.3)]
    )
    flow = FlowDefinition(name='lls-flow', operators=(scorer,), completes_when=RiskDataPoint, park_after=5.0)

    parked = await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])
    await runtime.inbox.append(SID, chat_answer('answer'))
    resumed = await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])

    assert parked.status is SessionStatus.PARKED and parked.epoch == 1
    assert resumed.status is SessionStatus.COMPLETED and resumed.epoch == 2
    assert resumed.operator_runs == {OperatorId('scorer'): 1}


async def test_lls_15_start_session_starts_a_session_with_no_state(fake_clock: FakeClock) -> None:
    manager = SessionOrchestrationManager(build_in_memory_runtime(fake_clock))
    runs: list[int] = []
    flow = FlowDefinition(name='lls-flow', operators=(_counting_scorer(runs),))

    result = await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])

    assert result.status is SessionStatus.COMPLETED and result.epoch == 1 and runs == [1]


# --- an audit sink that fails never stalls a session --------------------------------------------------------


class _RaisingAudit(InMemoryAuditSink):
    async def append(self, entry: AuditLogEntry) -> None:  # noqa: ARG002
        raise ConnectionError('audit store down')

    async def append_many(self, entries: Any) -> None:  # noqa: ARG002
        raise ConnectionError('audit store down')


class _HangingAudit(InMemoryAuditSink):
    async def append(self, entry: AuditLogEntry) -> None:  # noqa: ARG002
        await asyncio.Event().wait()

    async def append_many(self, entries: Any) -> None:  # noqa: ARG002
        await asyncio.Event().wait()


@pytest.mark.parametrize('sink', [InMemoryAuditSink, _RaisingAudit, _HangingAudit])
async def test_lls_16_a_failing_or_hanging_audit_sink_leaves_the_result_unchanged(
    sink: Callable[[], InMemoryAuditSink], fake_clock: FakeClock
) -> None:
    runtime = replace(build_in_memory_runtime(fake_clock), audit=sink())
    scorer = make_operator(
        'scorer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[RiskDataPoint.emit(0.4)]
    )

    result = await _orchestrator(runtime, [scorer], seed=[work_email()], audit_timeout=0.01).run()

    assert result.status is SessionStatus.COMPLETED and result.operator_runs == {OperatorId('scorer'): 1}
    assert {type(point) for point in (await runtime.store.snapshot(SID)).all()} == {
        type(work_email()),
        RiskDataPoint,
    }


# --- one INFO per flow --------------------------------------------------------------------------------------


async def test_lls_17_the_completion_tail_logs_its_steps_at_debug(fake_clock: FakeClock) -> None:
    scorer = make_operator(
        'scorer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[RiskDataPoint.emit(0.4)]
    )
    with capture_logs('INFO') as records:
        await _orchestrator(build_in_memory_runtime(fake_clock), [scorer], seed=[work_email()]).run()

    assert not [record for record in records if 'Completion-tail step' in record['message']]
