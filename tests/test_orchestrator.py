"""ORCH — orchestrator lifecycle, sole-mutator, timeouts, fault isolation, breaker arming.

Also covers POLICY-03 (no cancellation) and POLICY-06 (stopped only by timeout/deadline)
via the timeout/exception isolation tests.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from dataclasses import replace
from datetime import timedelta
from itertools import count
from typing import Any

import pytest

from orcastork.adapters.memory import (
    InMemoryAuditSink,
    InMemoryCapabilityCatalog,
    InMemoryDataPointStore,
    InMemoryInbox,
    InMemorySessionLock,
)
from orcastork.aggregation import RetryPolicy
from orcastork.audit import AuditKind, OperatorOutcome
from orcastork.capabilities import Capability, CapabilityContext
from orcastork.datapoints import DataPointEmission
from orcastork.exceptions import StaleEpochError
from orcastork.flow import FlowIdentity
from orcastork.ids import CapabilityId, Epoch, NamespaceId, OperatorId, SessionId
from orcastork.operators import Operator, OperatorContext, OperatorPolicy, RerunOn
from orcastork.orchestrator import Orchestrator, SessionStatus
from orcastork.orchestrator.mirror import SessionStateMirror
from orcastork.runtime import build_in_memory_runtime
from orcastork.scheduling import all_of

from .doubles.capabilities import make_capability
from .doubles.cipher import ReversingCipher
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
from .doubles.otel import TelemetryProbe

SID = SessionId('orch-session')
NAMESPACE = NamespaceId('orch-namespace')


class _SingleNamespaceCipherProvider:
    """A NamespaceCipherProvider that hands the same cipher back for every namespace (a real per-namespace provider in
    production resolves a distinct key per namespace)."""

    def __init__(self, cipher: ReversingCipher) -> None:
        self._cipher = cipher

    async def for_namespace(self, namespace_id: str) -> ReversingCipher:  # noqa: ARG002
        return self._cipher


class _RenewCountingLock(InMemorySessionLock):
    """In-memory lock that counts lease renewals (to assert the orchestrator keeps ownership alive)."""

    def __init__(self, clock: FakeClock) -> None:
        super().__init__(clock)
        self.renews = 0

    async def renew(self, session_id: SessionId, *, epoch: Epoch) -> None:
        self.renews += 1
        await super().renew(session_id, epoch=epoch)


class _FenceOnRenewLock(InMemorySessionLock):
    """In-memory lock whose renew always reports a takeover (models being fenced mid-run)."""

    async def renew(self, session_id: SessionId, *, epoch: Epoch) -> None:  # noqa: ARG002
        raise StaleEpochError('a higher epoch took over')


async def _noop(ctx: OperatorContext) -> None:  # noqa: ARG001
    return None


async def test_orch_01_lifecycle_completes_and_releases_epoch(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    operator = make_operator('op', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    orchestrator = Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[operator], seed=[work_email()]
    )
    result = await orchestrator.run()
    assert result.status is SessionStatus.COMPLETED
    assert result.epoch == 1  # first epoch minted on grant
    assert not await runtime.lock.is_held(SID)  # epoch released at completion


async def test_orch_02_aggregation_runs_after_gathering_quiescent(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    seen: dict[str, int] = {}

    async def aggregate(ctx: OperatorContext) -> None:
        seen['risk_count'] = len(ctx.store.of_type(RiskDataPoint))

    operator = make_operator('op', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    reporter = make_aggregator('rep', depends_on={RiskDataPoint}, on_aggregate=aggregate)
    await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[operator, reporter], seed=[work_email()]
    ).run()
    assert seen['risk_count'] == 1  # the aggregator observed the fully-gathered state


async def test_orch_03_sole_mutator_applies_all_emissions(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    first = make_operator('o1', produces={IpDataPoint}, emits=[ip()])
    second = make_operator('o2', produces={RiskDataPoint}, emits=[risk()])
    await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[first, second], seed=[work_email()]
    ).run()
    assert {dp.type for dp in (await runtime.store.snapshot(SID)).all()} == {'work_email', 'ip', 'risk'}


async def test_orch_04_operation_timeout_persists_emissions_and_proceeds(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    slow = make_operator('slow', produces={IpDataPoint}, emits=[ip('slow-ip')], sleep_after=5.0)
    fast = make_operator('fast', produces={RiskDataPoint}, emits=[risk()])
    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[slow, fast], operation_timeout=0.02
    ).run()
    types = {dp.type for dp in (await runtime.store.snapshot(SID)).all()}
    assert result.status is SessionStatus.COMPLETED
    assert 'ip' in types  # the slow operator's pre-timeout emission persisted
    assert 'risk' in types  # the scheduler proceeded with the other operator


async def test_orch_policy_timeout_lets_a_slow_operator_outlive_the_global_default(fake_clock: FakeClock) -> None:
    # A wrapped streaming collector legitimately runs past the orchestrator-wide bound: its policy
    # timeout raises the per-run limit, so it finishes (and its post-sleep emission lands) where the
    # global default alone would have cancelled it mid-run.
    runtime = build_in_memory_runtime(fake_clock)

    class _Streaming(Operator):
        operator_id = OperatorId('streaming')
        policy = OperatorPolicy(rerun_on_new_data=False, timeout=timedelta(seconds=5))
        produces = frozenset({IpDataPoint})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:  # noqa: ARG002
            await asyncio.sleep(0.05)  # longer than the global bound — only the policy override allows this
            yield IpDataPoint.emit('survived')

    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[_Streaming], operation_timeout=0.02
    ).run()
    assert result.status is SessionStatus.COMPLETED
    stored = (await runtime.store.snapshot(SID)).of_type(IpDataPoint)
    assert [dp.value for dp in stored] == ['survived']  # the post-sleep emission proves the run was not cut short
    runs = {
        entry.operator_id: entry.operator for entry in await runtime.audit.replay(SID) if entry.operator is not None
    }
    assert runs[OperatorId('streaming')].outcome is OperatorOutcome.SUCCEEDED


async def test_orch_policy_timeout_can_cut_an_operator_shorter_than_the_global_default(fake_clock: FakeClock) -> None:
    # The override works in both directions: a scoring operator that must answer in well under the
    # global bound is timed out by its own (shorter) policy timeout, and the scheduler proceeds.
    runtime = build_in_memory_runtime(fake_clock)
    slow = make_operator(
        'slow', produces={IpDataPoint}, emits=[ip('slow-ip')], sleep_after=0.5, timeout=timedelta(seconds=0.02)
    )
    fast = make_operator('fast', produces={RiskDataPoint}, emits=[risk()])
    result = await Orchestrator(session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[slow, fast]).run()
    types = {dp.type for dp in (await runtime.store.snapshot(SID)).all()}
    assert result.status is SessionStatus.COMPLETED
    assert 'ip' in types and 'risk' in types  # the pre-timeout emission persisted; the scheduler proceeded
    runs = {
        entry.operator_id: entry.operator for entry in await runtime.audit.replay(SID) if entry.operator is not None
    }
    assert runs[OperatorId('slow')].outcome is OperatorOutcome.FAILED  # cut off well before the 30s global default
    assert runs[OperatorId('fast')].outcome is OperatorOutcome.SUCCEEDED


async def test_orch_05_session_deadline_runs_aggregation_then_completes(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    seen: dict[str, int] = {}

    async def aggregate(ctx: OperatorContext) -> None:
        seen['risk_count'] = len(ctx.store.of_type(RiskDataPoint))

    reporter = make_aggregator('rep', depends_on={RiskDataPoint}, on_aggregate=aggregate)
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[reporter],
        seed=[risk()],
        session_deadline=0.0,  # deadline already reached → straight to aggregation
    ).run()
    assert result.status is SessionStatus.COMPLETED
    assert seen['risk_count'] == 1


async def test_orch_06_operator_exception_persists_emissions_and_proceeds(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    boom = make_operator('boom', produces={IpDataPoint}, emits=[ip('boom-ip')], raise_error=ValueError('boom'))
    fast = make_operator('fast', produces={RiskDataPoint}, emits=[risk()])
    with capture_logs(level='ERROR') as records:
        result = await Orchestrator(
            session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[boom, fast]
        ).run()
    types = {dp.type for dp in (await runtime.store.snapshot(SID)).all()}
    assert result.status is SessionStatus.COMPLETED  # no wedge
    assert 'ip' in types and 'risk' in types
    failure = next(record for record in records if record['extra'].get('operator_id') == OperatorId('boom'))
    assert failure['exception'] is not None  # the log carries the traceback, not just the operator id
    assert 'boom' in str(failure['exception'].value)


async def test_orch_07_no_in_band_control_datapoint(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    operator = make_operator('op', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[operator], seed=[work_email()]
    ).run()
    # Only real DataPoints exist — quiescence is computed directly, not signalled by a marker.
    assert {dp.type for dp in (await runtime.store.snapshot(SID)).all()} == {'work_email', 'risk'}


async def test_orch_08_orchestrator_never_writes_durable_itself(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    operator = make_operator('op', produces={RiskDataPoint}, emits=[risk()])  # no aggregator in the flow
    await Orchestrator(session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[operator]).run()
    assert len((await runtime.store.snapshot(SID)).all()) == 1  # gathering wrote to the live store
    assert await runtime.durable.read('reports', 'risk') is None  # no aggregator ran → curated DurableStore empty


async def test_orch_09_writes_are_epoch_stamped(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    operator = make_operator('op', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[operator], seed=[work_email()]
    ).run()
    entries = await runtime.audit.replay(SID)
    assert entries and all(entry.epoch == result.epoch for entry in entries)


async def test_orch_10_graph_recheck_arms_circuit_breaker(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    counter = count()
    self_cycle = make_operator(
        'selfloop',
        produces={IpDataPoint},
        depends_on={IpDataPoint},
        rerun_on_new_data=True,
        max_cycles=3,
        emit_factory=lambda _ctx: [ip(f'ip-{next(counter)}')],  # noqa: ARG005
    )
    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[self_cycle], seed=[ip('seed')]
    ).run()
    assert result.status is SessionStatus.COMPLETED  # the self-cycle did not run forever
    assert result.operator_runs[OperatorId('selfloop')] <= 3  # bounded by the armed circuit-breaker


async def test_orch_11_completed_is_terminal(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    operator = make_operator('op', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    reporter = make_aggregator('rep', depends_on={RiskDataPoint}, on_aggregate=_noop)
    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[operator, reporter], seed=[work_email()]
    ).run()
    assert result.operator_runs[OperatorId('op')] == 1  # no post-completion re-runs
    assert result.operator_runs[OperatorId('rep')] == 1


async def test_orch_12_orchestrator_stamps_emission_provenance_and_time(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    # The operator emits value-only via the public ``Leaf.emit(value)`` API — no provenance plumbing.
    operator = make_operator('emitter', produces={RiskDataPoint}, emits=[RiskDataPoint.emit(0.7)])
    await Orchestrator(session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[operator]).run()

    stored = (await runtime.store.snapshot(SID)).of_type(RiskDataPoint)
    assert len(stored) == 1 and stored[0].value == 0.7
    assert stored[0].retrieved_by == OperatorId('emitter')  # provenance stamped by the orchestrator, not the operator
    assert stored[0].first_retrieved == stored[0].last_retrieved == fake_clock.now()  # observation time stamped too


async def test_orch_13_malformed_emission_is_isolated_not_session_aborting(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    # The bad value only fails when the orchestrator finalizes the emission into a DataPoint (pydantic
    # validation); that must be isolated to its operator like any other fault, not abort the session.
    bad = make_operator('bad', produces={RiskDataPoint}, emits=[DataPointEmission(RiskDataPoint, 'not-a-number')])
    good = make_operator('good', produces={IpDataPoint}, emits=[ip()])
    result = await Orchestrator(session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[bad, good]).run()

    assert result.status is SessionStatus.COMPLETED  # the malformed emission did not wedge the session
    assert result.operator_runs.get(OperatorId('good')) == 1  # the healthy operator still ran
    assert {dp.type for dp in (await runtime.store.snapshot(SID)).all()} == {'ip'}  # only the valid one persisted


async def test_orch_pipelines_emission_to_consumer_while_producer_still_runs(fake_clock: FakeClock) -> None:
    # Eager streaming: an emission is merged and its consumer launched while the producer is still
    # running. The producer here only finishes *after* the consumer runs (it blocks on an event the
    # consumer sets), so reaching its post-gate emission proves the two overlapped — a phase/gather
    # scheduler would block the whole wave on the producer and never start the consumer in time.
    runtime = build_in_memory_runtime(fake_clock)
    consumer_ran = asyncio.Event()

    class _Producer(Operator):
        operator_id = OperatorId('pipeline_producer')
        policy = OperatorPolicy(rerun_on_new_data=False)
        depends_on = frozenset({EmailDataPoint})  # ready from the seed
        produces = frozenset({IpDataPoint, RiskDataPoint})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:  # noqa: ARG002
            yield IpDataPoint.emit('203.0.113.7')  # the consumer's dependency
            await consumer_ran.wait()  # released only by the consumer running concurrently
            yield RiskDataPoint.emit(0.123)  # post-gate marker — reached only if the consumer ran first

    class _Consumer(Operator):
        operator_id = OperatorId('pipeline_consumer')
        policy = OperatorPolicy(rerun_on_new_data=False)
        depends_on = frozenset({IpDataPoint})  # not ready until the producer emits
        produces = frozenset({ChatAnswerDataPoint})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:  # noqa: ARG002
            consumer_ran.set()  # unblock the still-running producer
            yield ChatAnswerDataPoint.emit('done')

    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[_Producer, _Consumer], seed=[work_email()]
    ).run()

    assert result.status is SessionStatus.COMPLETED
    assert result.operator_runs == {OperatorId('pipeline_producer'): 1, OperatorId('pipeline_consumer'): 1}
    by_type = {dp.type: dp.value for dp in (await runtime.store.snapshot(SID)).all()}
    assert by_type['risk'] == 0.123  # producer reached its post-gate emission → consumer ran while it waited
    assert by_type['chat_answer'] == 'done'


async def test_orch_reruns_for_relevant_data_merged_while_it_was_running(fake_clock: FakeClock) -> None:
    # Watermark correctness under streaming: a run's watermark must advance to the revision its launch
    # snapshot observed, not the live revision at completion. Here a relevant DataPoint (Ip 'b') is
    # merged *while* the watcher is still running; a live-revision watermark would swallow it and the
    # watcher would never rerun. (The rerun also proves own-emissions don't spuriously re-trigger it.)
    runtime = build_in_memory_runtime(fake_clock)
    watcher_started = asyncio.Event()
    second_ip_merged = asyncio.Event()
    seen: list[set[str]] = []

    class _Watcher(Operator):
        operator_id = OperatorId('watcher')
        policy = OperatorPolicy(rerun_on_new_data=True, debounce=timedelta(0))  # rerun immediately on new Ip
        depends_on = frozenset({IpDataPoint})
        produces = frozenset({ChatAnswerDataPoint})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:
            seen.append({dp.value for dp in ctx.store.of_type(IpDataPoint)})
            if not watcher_started.is_set():
                watcher_started.set()  # release the feeder to emit the second Ip
                await second_ip_merged.wait()  # stay running until that Ip has actually been merged
            yield ChatAnswerDataPoint.emit('seen')  # nothing depends on this — must NOT re-trigger the watcher

    class _Feeder(Operator):
        operator_id = OperatorId('feeder')
        policy = OperatorPolicy(rerun_on_new_data=False)
        depends_on = frozenset({EmailDataPoint})
        produces = frozenset({IpDataPoint})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:  # noqa: ARG002
            yield IpDataPoint.emit('a')
            await watcher_started.wait()  # emit the second Ip only once the watcher is mid-run
            yield IpDataPoint.emit('b')

    async def release_once_second_ip_is_merged() -> None:
        deadline = asyncio.get_running_loop().time() + 1.0
        while asyncio.get_running_loop().time() < deadline:
            if any(dp.value == 'b' for dp in (await runtime.store.snapshot(SID)).of_type(IpDataPoint)):
                second_ip_merged.set()
                return
            await asyncio.sleep(0)  # yield to the orchestrator loop (real sleep, not the fake clock)
        raise AssertionError('timed out waiting for IpDataPoint(value="b") to merge')

    orchestrator = Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[_Watcher, _Feeder], seed=[work_email()]
    )
    result, _ = await asyncio.gather(orchestrator.run(), release_once_second_ip_is_merged())

    assert result.operator_runs[OperatorId('watcher')] == 2  # reran exactly once, for the mid-run Ip
    assert seen == [{'a'}, {'a', 'b'}]  # the rerun observed the Ip that landed while it was running
    watcher_runs = [
        entry.operator
        for entry in await runtime.audit.replay(SID)
        if entry.operator is not None and entry.operator_id == OperatorId('watcher')
    ]
    assert [run.run_count for run in watcher_runs] == [1, 2]  # the audit records each rerun by its run number
    assert all(run.outcome is OperatorOutcome.SUCCEEDED for run in watcher_runs)


async def _watcher_runs_after_second_observation(
    fake_clock: FakeClock, *, rerun_on: RerunOn, second_value: str
) -> int:
    """Run a watcher/feeder pair where the feeder makes a second Ip observation mid-watcher-run.

    The second observation lands 5s after the first: with the same value it merges as a
    freshness-only update (``delta.updated``), with a different value as a new identity
    (``delta.added``). Returns how many times the watcher ran.
    """
    runtime = build_in_memory_runtime(fake_clock)
    watcher_started = asyncio.Event()
    second_merged = asyncio.Event()
    observed_at_start = fake_clock.now()

    class _Watcher(Operator):
        operator_id = OperatorId('watcher')
        policy = OperatorPolicy(rerun_on_new_data=True, rerun_on=rerun_on, debounce=timedelta(0))
        depends_on = frozenset({IpDataPoint})
        produces = frozenset({ChatAnswerDataPoint})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:  # noqa: ARG002
            if not watcher_started.is_set():
                watcher_started.set()  # release the feeder to make its second observation
                await second_merged.wait()  # stay running until that observation has actually been merged
            yield ChatAnswerDataPoint.emit('seen')  # nothing depends on this — must NOT re-trigger the watcher

    class _Feeder(Operator):
        operator_id = OperatorId('feeder')
        policy = OperatorPolicy(rerun_on_new_data=False)
        depends_on = frozenset({EmailDataPoint})
        produces = frozenset({IpDataPoint})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:  # noqa: ARG002
            yield IpDataPoint.emit('a')
            await watcher_started.wait()  # observe again only once the watcher is mid-run
            fake_clock.advance(5)  # a later observation time, so an identical value still lands as an update
            yield IpDataPoint.emit(second_value)

    async def release_once_second_observation_is_merged() -> None:
        deadline = asyncio.get_running_loop().time() + 1.0
        while asyncio.get_running_loop().time() < deadline:
            ips = (await runtime.store.snapshot(SID)).of_type(IpDataPoint)
            if any(dp.last_retrieved > observed_at_start for dp in ips):
                second_merged.set()
                return
            await asyncio.sleep(0)  # yield to the orchestrator loop (real sleep, not the fake clock)
        raise AssertionError('timed out waiting for the second Ip observation to merge')

    orchestrator = Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[_Watcher, _Feeder], seed=[work_email()]
    )
    result, _ = await asyncio.gather(orchestrator.run(), release_once_second_observation_is_merged())
    assert result.status is SessionStatus.COMPLETED
    return result.operator_runs[OperatorId('watcher')]


async def test_orch_added_only_watcher_ignores_freshness_only_reobservation(fake_clock: FakeClock) -> None:
    # The same (type, value) re-observed with a bumped last_retrieved is a freshness-only update:
    # an ADDED_ONLY watcher must not rerun for it.
    assert await _watcher_runs_after_second_observation(fake_clock, rerun_on=RerunOn.ADDED_ONLY, second_value='a') == 1


async def test_orch_added_or_updated_watcher_reruns_for_freshness_only_reobservation(fake_clock: FakeClock) -> None:
    # The default keeps today's behavior: a freshness-only update still re-triggers the watcher.
    assert (
        await _watcher_runs_after_second_observation(fake_clock, rerun_on=RerunOn.ADDED_OR_UPDATED, second_value='a')
        == 2
    )


async def test_orch_added_only_watcher_still_reruns_for_a_genuinely_new_value(fake_clock: FakeClock) -> None:
    # ADDED_ONLY narrows reruns to new identities — it must not suppress a genuinely new value.
    assert await _watcher_runs_after_second_observation(fake_clock, rerun_on=RerunOn.ADDED_ONLY, second_value='b') == 2


async def test_orch_audits_operator_runs_with_success_and_failure(fake_clock: FakeClock) -> None:
    # Every operator run is recorded with its outcome — including a run that emits nothing or raises,
    # which leaves no DATA_POINT_ADDED trace and so was previously invisible in the audit log.
    runtime = build_in_memory_runtime(fake_clock)
    producer = make_operator('producer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    failer = make_operator('failer', depends_on={RiskDataPoint}, raise_error=ValueError('boom'))
    await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[producer, failer], seed=[work_email()]
    ).run()

    runs = {
        entry.operator_id: entry.operator for entry in await runtime.audit.replay(SID) if entry.operator is not None
    }
    assert runs[OperatorId('producer')].outcome is OperatorOutcome.SUCCEEDED
    assert runs[OperatorId('failer')].outcome is OperatorOutcome.FAILED
    assert 'boom' in (runs[OperatorId('failer')].error or '')  # the failure reason is captured


async def test_orch_audits_capability_invocation_with_redacted_parameters(fake_clock: FakeClock) -> None:
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {CapabilityId('idp')}})
    runtime = build_in_memory_runtime(fake_clock, catalog=catalog)
    performed: list[tuple[str, dict[str, Any]]] = []

    class _Idp(Capability):
        capability_id = CapabilityId('idp')
        depends_on = frozenset({EmailDataPoint})

        async def activate(self, ctx: CapabilityContext) -> None:  # noqa: ARG002
            return None

        async def send_challenge(self, user_id: str, email: str) -> str:  # a real, typed action method
            performed.append(('send_challenge', {'user_id': user_id, 'email': email}))
            return 'challenge-sent'

    class _Caller(Operator):
        operator_id = OperatorId('caller')
        policy = OperatorPolicy(rerun_on_new_data=False)
        depends_on = frozenset({EmailDataPoint})
        requires = frozenset({_Idp})
        produces = frozenset({ChatAnswerDataPoint})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:
            idp = ctx.capabilities.require(_Idp)  # typed; `requires` gating means it is always available here
            result = await idp.send_challenge(user_id='u-1', email='alice@work.example')  # direct, typed call
            yield ChatAnswerDataPoint.emit(result)

    await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[_Caller],
        capabilities=[_Idp],
        seed=[work_email()],
    ).run()

    # The real arguments reach the capability; only the audit log redacts them.
    assert performed == [('send_challenge', {'user_id': 'u-1', 'email': 'alice@work.example'})]
    invoked = [entry for entry in await runtime.audit.replay(SID) if entry.kind == AuditKind.CAPABILITY_INVOKED]
    assert len(invoked) == 1
    capability = invoked[0].capability
    assert capability is not None
    assert capability.capability_id == CapabilityId('idp')
    assert capability.action == 'send_challenge'
    # Parameter keys are recorded; their values are redacted, so PII never lands in the audit log.
    assert capability.parameters == {'user_id': '<redacted>', 'email': '<redacted>'}
    assert 'alice@work.example' not in str(capability.parameters)


async def test_orch_seals_pii_audit_value_under_the_namespace_cipher(fake_clock: FakeClock) -> None:
    # With a real per-namespace provider, a PII value is sealed under the namespace key rather than discarded, so
    # an operator can recover it from the audit with the key — but it is never stored in clear.
    cipher = ReversingCipher()
    runtime = replace(build_in_memory_runtime(fake_clock), cipher_provider=_SingleNamespaceCipherProvider(cipher))
    collector = make_operator('collect', produces={IpDataPoint}, emits=[ip('203.0.113.9')])

    result = await Orchestrator(session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[collector]).run()

    assert result.status is SessionStatus.COMPLETED
    points = [e.data_point for e in await runtime.audit.replay(SID) if e.kind is AuditKind.DATA_POINT_ADDED]
    ip_summary = next(dp.summary for dp in points if dp is not None and dp.data_point_type == 'ip')
    assert ip_summary != '<redacted>'
    assert '203.0.113.9' not in ip_summary  # ciphertext at rest, never the plaintext value
    assert cipher.decrypt(ip_summary) == '203.0.113.9'  # recoverable with the key


async def test_orch_redacts_pii_audit_value_without_a_real_cipher(fake_clock: FakeClock) -> None:
    # The default passthrough provider must keep the historical behavior: PII stays redacted, never
    # written in clear by an identity "encrypt".
    runtime = build_in_memory_runtime(fake_clock)
    collector = make_operator('collect', produces={IpDataPoint}, emits=[ip('203.0.113.9')])

    await Orchestrator(session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[collector]).run()

    points = [e.data_point for e in await runtime.audit.replay(SID) if e.kind is AuditKind.DATA_POINT_ADDED]
    ip_summary = next(dp.summary for dp in points if dp is not None and dp.data_point_type == 'ip')
    assert ip_summary == '<redacted>'


async def test_orch_renews_lease_while_a_long_session_runs(fake_clock: FakeClock) -> None:
    # A session that runs longer than the lock TTL must renew its lease, or a supervisor would treat
    # the still-working orchestrator as orphaned and take it over. A bounded self-cycle with a long
    # debounce makes the gather loop fast-forward the clock past the renew interval between reruns.
    counting = _RenewCountingLock(fake_clock)
    runtime = replace(build_in_memory_runtime(fake_clock), lock=counting)
    counter = count()
    self_cycle = make_operator(
        'selfloop',
        produces={IpDataPoint},
        depends_on={IpDataPoint},
        rerun_on_new_data=True,
        max_cycles=3,
        debounce=timedelta(seconds=20),  # each rerun fast-forwards the clock 20s (> the 10s renew interval)
        emit_factory=lambda _ctx: [ip(f'ip-{next(counter)}')],  # noqa: ARG005
    )
    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[self_cycle], seed=[ip('seed')]
    ).run()
    assert result.status is SessionStatus.COMPLETED
    assert counting.renews >= 1  # the lease was renewed while the session was still working


async def test_orch_fenced_renew_stops_cleanly_as_superseded(fake_clock: FakeClock) -> None:
    # A renew that reveals a higher epoch took over ends the run as SUPERSEDED rather than raising:
    # the successor now owns the session and re-drives any unfinished work idempotently.
    runtime = replace(build_in_memory_runtime(fake_clock), lock=_FenceOnRenewLock(fake_clock))
    counter = count()
    self_cycle = make_operator(
        'selfloop',
        produces={IpDataPoint},
        depends_on={IpDataPoint},
        rerun_on_new_data=True,
        max_cycles=3,
        debounce=timedelta(seconds=20),  # forces a loop sleep that crosses the renew interval
        emit_factory=lambda _ctx: [ip(f'ip-{next(counter)}')],  # noqa: ARG005
    )
    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[self_cycle], seed=[ip('seed')]
    ).run()
    assert result.status is SessionStatus.SUPERSEDED  # fenced mid-run → clean stop, not a raise


async def test_orch_undeclared_emission_is_merged_and_logged_once(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    # Declares only Risk but emits two IPs: both must merge (data is never dropped); the
    # mismatch is reported once per (operator, type), not per emission.
    sneaky = make_operator('sneaky', produces={RiskDataPoint}, emits=[risk(), ip('198.51.100.1'), ip('198.51.100.2')])
    with capture_logs(level='ERROR') as records:
        result = await Orchestrator(session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[sneaky]).run()
    assert result.status is SessionStatus.COMPLETED
    stored = (await runtime.store.snapshot(SID)).of_type(IpDataPoint)
    assert {dp.value for dp in stored} == {'198.51.100.1', '198.51.100.2'}  # merged anyway
    undeclared = [record for record in records if record['extra'].get('data_point_type') == 'ip']
    assert len(undeclared) == 1  # one ERROR per operator-and-type, no spam
    assert undeclared[0]['extra']['operator_id'] == OperatorId('sneaky')
    assert undeclared[0]['extra']['declared_produces'] == ['RiskDataPoint']


async def test_orch_leaf_of_declared_abstract_produces_is_not_flagged(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    # Declaring the abstract intermediate covers every leaf in its substitution group.
    emitter = make_operator('emitter', produces={EmailDataPoint}, emits=[work_email('bob@work.example')])
    with capture_logs(level='ERROR') as records:
        result = await Orchestrator(session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[emitter]).run()
    assert result.status is SessionStatus.COMPLETED
    assert not records  # a leaf of a declared abstract type is a declared emission


async def test_orch_emissions_carry_their_actual_observation_time(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)

    def emissions(ctx: OperatorContext) -> Iterator[DataPointEmission]:  # noqa: ARG001
        yield IpDataPoint.emit('198.51.100.1')
        fake_clock.advance(5.0)  # the operator keeps running; later yields happen later
        yield RiskDataPoint.emit(0.5)

    operator = make_operator('op', produces={IpDataPoint, RiskDataPoint}, emit_factory=emissions)
    await Orchestrator(session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[operator]).run()
    view = await runtime.store.snapshot(SID)
    (first,) = view.of_type(IpDataPoint)
    (second,) = view.of_type(RiskDataPoint)
    assert second.first_retrieved - first.first_retrieved == timedelta(seconds=5)


async def test_orch_completes_without_waiting_when_completion_type_already_present(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    operator = make_operator('op', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[operator],
        seed=[work_email()],
        completes_when=RiskDataPoint,
    ).run()
    assert result.status is SessionStatus.COMPLETED
    assert fake_clock.monotonic() < 1.0  # the condition was met by gathering itself — no inbox wait happened


async def test_orch_wait_is_bounded_by_session_deadline_then_aggregates(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    seen: dict[str, int] = {}

    async def aggregate(ctx: OperatorContext) -> None:
        seen['emails'] = len(ctx.store.of_type(EmailDataPoint))

    reporter = make_aggregator('rep', depends_on={EmailDataPoint}, on_aggregate=aggregate)
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[reporter],
        seed=[work_email()],
        completes_when=ChatAnswerDataPoint,  # never arrives; the inbox stays empty
        session_deadline=30.0,
    ).run()
    assert result.status is SessionStatus.COMPLETED  # the deadline bounds the wait; aggregation still ran
    assert seen == {'emails': 1}
    assert fake_clock.monotonic() >= 30.0  # the session genuinely waited out its deadline


async def test_orch_renews_lease_while_waiting_on_the_inbox(fake_clock: FakeClock) -> None:
    counting = _RenewCountingLock(fake_clock)
    runtime = replace(build_in_memory_runtime(fake_clock), lock=counting)
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[],
        seed=[work_email()],
        completes_when=ChatAnswerDataPoint,
        session_deadline=25.0,  # spans two 10s renew intervals while waiting
    ).run()
    assert result.status is SessionStatus.COMPLETED
    assert counting.renews >= 2  # ownership was kept alive across the whole wait


async def test_orch_fenced_renew_during_wait_stops_cleanly_as_superseded(fake_clock: FakeClock) -> None:
    runtime = replace(build_in_memory_runtime(fake_clock), lock=_FenceOnRenewLock(fake_clock))
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[],
        seed=[work_email()],
        completes_when=ChatAnswerDataPoint,
        session_deadline=60.0,
    ).run()
    assert result.status is SessionStatus.SUPERSEDED  # the successor owns the session; the waiter stood down


class _CountingStore(InMemoryDataPointStore):
    """In-memory store that counts hot-loop reads and applies (per-batch, not per-emission, cost)."""

    def __init__(self) -> None:
        super().__init__()
        self.snapshots = 0
        self.watermark_reads = 0
        self.applies = 0

    async def snapshot(self, session_id: SessionId) -> Any:
        self.snapshots += 1
        return await super().snapshot(session_id)

    async def get_watermark(self, session_id: SessionId, operator_id: OperatorId) -> Any:
        self.watermark_reads += 1
        return await super().get_watermark(session_id, operator_id)

    async def apply_resolved(self, session_id: SessionId, *, added: Any, updated: Any, epoch: Epoch) -> Any:
        self.applies += 1
        return await super().apply_resolved(session_id, added=added, updated=updated, epoch=epoch)


async def test_orch_replans_per_batch_not_per_emission(fake_clock: FakeClock) -> None:
    store = _CountingStore()
    runtime = replace(build_in_memory_runtime(fake_clock), store=store)
    chatty = make_operator('chatty', produces={IpDataPoint}, emits=[ip(f'198.51.100.{n}') for n in range(1, 6)])
    result = await Orchestrator(session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[chatty]).run()
    assert result.status is SessionStatus.COMPLETED
    assert len((await runtime.store.snapshot(SID)).of_type(IpDataPoint)) == 5  # one snapshot read by the test
    assert store.snapshots <= 5  # signals were drained in batches — far fewer re-plans than emissions


async def test_orch_emission_burst_applies_once_but_keeps_per_event_granularity(fake_clock: FakeClock) -> None:
    store = _CountingStore()
    runtime = replace(build_in_memory_runtime(fake_clock), store=store)
    chatty = make_operator('chatty', produces={IpDataPoint}, emits=[ip(f'198.51.100.{n}') for n in range(1, 6)])

    result = await Orchestrator(session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[chatty]).run()

    assert result.status is SessionStatus.COMPLETED
    assert store.applies == 1  # the five-emission burst was merged in ONE mirror/store apply
    added = [e for e in await runtime.audit.replay(SID) if e.kind is AuditKind.DATA_POINT_ADDED]
    assert len(added) == 5  # batching the write never collapses per-event audit granularity
    assert len(await runtime.archive.read(SID)) == 5  # nor the per-DataPoint archive documents
    assert len((await runtime.store.snapshot(SID)).of_type(IpDataPoint)) == 5


async def test_orch_inbox_drain_applies_a_pending_batch_in_one_store_write(fake_clock: FakeClock) -> None:
    store = _CountingStore()
    runtime = replace(build_in_memory_runtime(fake_clock), store=store)
    for index in range(3):
        await runtime.inbox.append(SID, chat_answer(f'answer-{index}'))

    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[],
        completes_when=ChatAnswerDataPoint,
    ).run()

    assert result.status is SessionStatus.COMPLETED
    assert store.applies == 1  # the whole drained inbox batch landed in ONE apply
    assert await runtime.inbox.pending_count(SID) == 0  # every entry was still acked individually
    added = [e for e in await runtime.audit.replay(SID) if e.kind is AuditKind.DATA_POINT_ADDED]
    assert len(added) == 3  # per-entry audit granularity intact
    assert {dp.value for dp in (await runtime.store.snapshot(SID)).of_type(ChatAnswerDataPoint)} == {
        'answer-0',
        'answer-1',
        'answer-2',
    }


class _HighWaterQueue(asyncio.Queue):
    """Queue recording the highest depth it ever held (to pin the backpressure bound)."""

    def __init__(self, maxsize: int) -> None:
        super().__init__(maxsize)
        self.high_water = 0

    def _put(self, item: Any) -> None:
        super()._put(item)
        self.high_water = max(self.high_water, self.qsize())


class _ProbeQueueOrchestrator(Orchestrator):
    """Orchestrator whose emission queue is observable (same bound, recorded high-water mark)."""

    probe: _HighWaterQueue

    def _build_emission_queue(self) -> asyncio.Queue[Any]:
        self.probe = _HighWaterQueue(self._emission_queue_size)
        return self.probe


async def test_orch_bounded_queue_backpressures_a_burst_without_losing_emissions(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    burst = make_operator('burst', produces={IpDataPoint}, emits=[ip(f'198.51.100.{n}') for n in range(1, 6)])
    orchestrator = _ProbeQueueOrchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[burst], emission_queue_size=1
    )

    result = await orchestrator.run()

    assert result.status is SessionStatus.COMPLETED  # the suspended emitter resumed every time the loop drained
    stored = {dp.value for dp in (await runtime.store.snapshot(SID)).of_type(IpDataPoint)}
    assert stored == {f'198.51.100.{n}' for n in range(1, 6)}  # backpressure, not loss — every emission landed
    assert orchestrator.probe.maxsize == 1
    assert orchestrator.probe.high_water <= 1  # the queue never exceeded its bound
    runs = {
        entry.operator_id: entry.operator for entry in await runtime.audit.replay(SID) if entry.operator is not None
    }
    assert runs[OperatorId('burst')].outcome is OperatorOutcome.SUCCEEDED  # the _Completed signal was not lost


async def test_orch_emission_queue_is_bounded_by_default(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    operator = make_operator('op', produces={RiskDataPoint}, emits=[risk()])
    orchestrator = Orchestrator(session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[operator])
    assert orchestrator._build_emission_queue().maxsize == 1024  # bounded out of the box, never unbounded


async def test_orch_poison_inbox_entry_is_quarantined_and_session_completes(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    inbox = runtime.inbox
    assert isinstance(inbox, InMemoryInbox)
    poison_id = await inbox.append_serialized(SID, 'not-json{')  # a payload no deploy can parse
    operator = make_operator('op', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    with capture_logs() as records:
        result = await Orchestrator(
            session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[operator], seed=[work_email()]
        ).run()

    assert result.status is SessionStatus.COMPLETED  # the malformed payload did not wedge the session
    assert result.operator_runs[OperatorId('op')] == 1  # the healthy flow still ran
    (record,) = await runtime.inbox.quarantined(SID)
    assert record.entry_id == poison_id
    assert await runtime.inbox.pending_count(SID) == 0  # nothing left to crash-loop a resume
    (audit_entry,) = [e for e in await runtime.audit.replay(SID) if e.kind is AuditKind.INBOX_ENTRY_QUARANTINED]
    assert audit_entry.inbox is not None
    assert audit_entry.inbox.entry_id == poison_id
    assert audit_entry.epoch == result.epoch  # the quarantine is epoch-stamped like every other event
    assert any(log['extra'].get('entry_id') == poison_id for log in records)  # WARNING logged for ops


class _RejectingStore(InMemoryDataPointStore):
    """In-memory store that rejects applies containing a marked value (a persistently bad apply)."""

    async def apply_resolved(self, session_id: SessionId, *, added: Any, updated: Any, epoch: Epoch) -> Any:
        if any(dp.value == 'merge-bomb' for dp in (*added, *updated)):
            raise ValueError('store rejected the write')
        return await super().apply_resolved(session_id, added=added, updated=updated, epoch=epoch)


class _FailingAuditSink(InMemoryAuditSink):
    """Audit sink whose DataPoint-added appends always fail (a broken post-apply bookkeeping sink)."""

    async def append(self, entry: Any) -> None:
        if entry.kind is AuditKind.DATA_POINT_ADDED:
            raise ValueError('audit sink down')
        await super().append(entry)


async def test_orch_post_apply_audit_failure_still_acks_the_inbox_entry(fake_clock: FakeClock) -> None:
    # The ack gates on the durable STORE apply alone: once that landed, a failing audit append is
    # logged and absorbed — redelivering an already-applied entry would burn its delivery budget
    # and duplicate audit without fixing anything.
    runtime = replace(build_in_memory_runtime(fake_clock), audit=_FailingAuditSink())
    await runtime.inbox.append(SID, chat_answer('applied'))
    with capture_logs() as records:
        result = await Orchestrator(
            session_id=SID,
            namespace_id=NAMESPACE,
            runtime=runtime,
            operators=[],
            completes_when=ChatAnswerDataPoint,
            session_deadline=30.0,
        ).run()

    assert result.status is SessionStatus.COMPLETED
    assert {dp.value for dp in (await runtime.store.snapshot(SID)).of_type(ChatAnswerDataPoint)} == {'applied'}
    assert await runtime.inbox.pending_count(SID) == 0  # acked despite the audit failure — no redelivery
    assert await runtime.inbox.quarantined(SID) == ()  # and no delivery budget was burned toward quarantine
    error = next(record for record in records if record['level'].name == 'ERROR')
    assert 'bookkeeping' in error['message']


async def test_orch_post_apply_audit_failure_on_a_batch_still_acks_every_entry(fake_clock: FakeClock) -> None:
    # The batched-merge fallback is reserved for STORE apply failures; a bookkeeping failure after
    # a successful batch apply must not push the entries onto the per-entry redelivery path.
    runtime = replace(build_in_memory_runtime(fake_clock), audit=_FailingAuditSink())
    for index in range(3):
        await runtime.inbox.append(SID, chat_answer(f'answer-{index}'))
    with capture_logs() as records:
        result = await Orchestrator(
            session_id=SID,
            namespace_id=NAMESPACE,
            runtime=runtime,
            operators=[],
            completes_when=ChatAnswerDataPoint,
            session_deadline=30.0,
        ).run()

    assert result.status is SessionStatus.COMPLETED
    assert {dp.value for dp in (await runtime.store.snapshot(SID)).of_type(ChatAnswerDataPoint)} == {
        'answer-0',
        'answer-1',
        'answer-2',
    }
    assert await runtime.inbox.pending_count(SID) == 0  # the whole batch was acked
    assert await runtime.inbox.quarantined(SID) == ()
    assert any(record['level'].name == 'ERROR' for record in records)


async def test_orch_unappliable_inbox_entry_is_quarantined_at_the_delivery_cap(fake_clock: FakeClock) -> None:
    # A valid entry whose merge keeps failing is redelivered (the fault may be transient), but only
    # up to max_inbox_deliveries — past the cap it is quarantined so it cannot grind forever.
    runtime = replace(build_in_memory_runtime(fake_clock), store=_RejectingStore())
    await runtime.inbox.append(SID, chat_answer('merge-bomb'))
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[],
        seed=[work_email()],
        completes_when=ChatAnswerDataPoint,  # keeps the loop draining instead of exiting on the first pass
        session_deadline=30.0,
        max_inbox_deliveries=3,
    ).run()

    assert result.status is SessionStatus.COMPLETED
    (record,) = await runtime.inbox.quarantined(SID)
    assert record.delivery_count == 3  # redelivered up to the cap, then quarantined
    assert 'store rejected the write' in record.reason
    assert await runtime.inbox.pending_count(SID) == 0
    assert not (await runtime.store.snapshot(SID)).of_type(ChatAnswerDataPoint)  # the apply never landed
    (audit_entry,) = [e for e in await runtime.audit.replay(SID) if e.kind is AuditKind.INBOX_ENTRY_QUARANTINED]
    assert audit_entry.inbox is not None
    assert audit_entry.inbox.delivery_count == 3


async def test_orch_reads_watermarks_once_per_gather_not_per_iteration(fake_clock: FakeClock) -> None:
    store = _CountingStore()
    runtime = replace(build_in_memory_runtime(fake_clock), store=store)
    counter = count()
    # A bounded self-cycle reruns several times, so the loop iterates well over once per operator.
    self_cycle = make_operator(
        'selfloop',
        produces={IpDataPoint},
        depends_on={IpDataPoint},
        rerun_on_new_data=True,
        max_cycles=3,
        debounce=timedelta(seconds=1),
        emit_factory=lambda _ctx: [ip(f'ip-{next(counter)}')],  # noqa: ARG005
    )
    other = make_operator('other', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[self_cycle, other], seed=[ip('seed')]
    ).run()
    assert result.status is SessionStatus.COMPLETED
    assert store.watermark_reads == 2  # exactly one rehydration read per gathering operator


async def test_orch_parks_after_idle_wait_exceeds_park_after(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    aggregated = {'ran': False}

    async def aggregate(ctx: OperatorContext) -> None:  # noqa: ARG001
        aggregated['ran'] = True

    reporter = make_aggregator('rep', depends_on={EmailDataPoint}, on_aggregate=aggregate)
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[reporter],
        seed=[work_email()],
        completes_when=ChatAnswerDataPoint,  # never arrives — the wait stays idle
        session_deadline=300.0,
        park_after=30.0,
    ).run()

    assert result.status is SessionStatus.PARKED
    assert aggregated == {'ran': False}  # parking skips aggregation entirely
    assert not await runtime.lock.is_complete(SID)  # not finalized — a deliver/resume must re-drive it
    assert not await runtime.lock.is_held(SID)  # the epoch was released; the pod holds nothing
    assert fake_clock.monotonic() == 30.0  # parked at the idle window, far before the 300s deadline
    parked_entries = [e for e in await runtime.audit.replay(SID) if e.kind is AuditKind.SESSION_PARKED]
    assert len(parked_entries) == 1
    assert parked_entries[0].epoch == result.epoch  # the park is epoch-stamped like every other event


async def test_orch_park_after_none_waits_out_the_deadline_exactly_as_before(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[],
        seed=[work_email()],
        completes_when=ChatAnswerDataPoint,
        session_deadline=30.0,
        park_after=None,  # parking disabled — the deadline alone bounds the wait
    ).run()
    assert result.status is SessionStatus.COMPLETED
    assert fake_clock.monotonic() >= 30.0  # the session genuinely waited out its deadline
    assert await runtime.lock.is_complete(SID)
    assert not any(e.kind is AuditKind.SESSION_PARKED for e in await runtime.audit.replay(SID))


async def test_orch_armed_retry_backoff_is_progress_not_idleness_and_never_parks(fake_clock: FakeClock) -> None:
    # The 20s retry backoff dwarfs park_after, yet the session must not park during it: armed
    # retry windows ride the next_due_in sleep path, and the idle window only starts once the
    # session actually reaches the inbox wait (here at t=20, parking at t=20+30).
    runtime = build_in_memory_runtime(fake_clock)
    flaky = make_operator(
        'flaky',
        depends_on={EmailDataPoint},
        raise_error=ValueError('boom'),
        retry=RetryPolicy(max_attempts=2, base_delay=20.0, jitter=0.0),
    )
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[flaky],
        seed=[work_email()],
        completes_when=ChatAnswerDataPoint,  # never arrives — the wait branch follows the retries
        session_deadline=300.0,
        park_after=30.0,
    ).run()

    assert result.status is SessionStatus.PARKED
    assert result.operator_runs[OperatorId('flaky')] == 2  # the armed retry ran out its backoff un-parked
    assert fake_clock.monotonic() == 50.0  # 20s backoff (progress) + the full 30s idle window


async def test_orch_without_flow_identity_persists_no_fingerprint(fake_clock: FakeClock) -> None:
    # A directly-constructed orchestrator (tests, spikes) carries no flow identity — drift
    # detection is skipped entirely and nothing is written to the session meta.
    runtime = build_in_memory_runtime(fake_clock)
    operator = make_operator('op', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[operator], seed=[work_email()]
    ).run()
    assert await runtime.store.get_flow_fingerprint(SID) is None


async def test_orch_flow_identity_fingerprint_persisted_on_first_spawn(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    operator = make_operator('op', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[operator],
        seed=[work_email()],
        flow_identity=FlowIdentity(name='orch-flow', fingerprint='fp-1'),
    ).run()
    assert await runtime.store.get_flow_fingerprint(SID) == 'fp-1'  # absent → persisted, no drift event
    assert not [e for e in await runtime.audit.replay(SID) if e.kind is AuditKind.FLOW_DRIFT_DETECTED]


async def test_orch_namespace_gated_operator_never_runs_even_with_inputs_present(fake_clock: FakeClock) -> None:
    catalog = InMemoryCapabilityCatalog(permitted_operators={NAMESPACE: {OperatorId('kept')}})
    runtime = build_in_memory_runtime(fake_clock, catalog=catalog)
    kept = make_operator('kept', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    gated = make_operator('gated', depends_on={EmailDataPoint}, produces={IpDataPoint}, emits=[ip()])
    with capture_logs(level='INFO') as records:
        result = await Orchestrator(
            session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[kept, gated], seed=[work_email()]
        ).run()

    assert result.status is SessionStatus.COMPLETED  # the gated operator does not stall quiescence
    assert result.operator_runs == {OperatorId('kept'): 1}  # gated never ran although its input was present
    assert {dp.type for dp in (await runtime.store.snapshot(SID)).all()} == {'work_email', 'risk'}
    exclusion = next(record for record in records if record['extra'].get('excluded_operator_ids') is not None)
    assert exclusion['level'].name == 'INFO'
    assert exclusion['extra']['excluded_operator_ids'] == [OperatorId('gated')]
    assert not any(record['level'].name == 'WARNING' for record in records)  # no graph-stall warning either


async def test_orch_operator_gating_for_another_namespace_changes_nothing(fake_clock: FakeClock) -> None:
    # Our namespace has no operator restriction configured (None) — behavior is exactly as before.
    catalog = InMemoryCapabilityCatalog(permitted_operators={NamespaceId('some-other-namespace'): set()})
    runtime = build_in_memory_runtime(fake_clock, catalog=catalog)
    operator = make_operator('op', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[operator], seed=[work_email()]
    ).run()
    assert result.status is SessionStatus.COMPLETED
    assert result.operator_runs == {OperatorId('op'): 1}


async def test_orch_empty_permitted_operators_runs_nothing_and_still_completes(fake_clock: FakeClock) -> None:
    # An empty frozenset is the explicit deny-everything configuration (distinct from None).
    catalog = InMemoryCapabilityCatalog(permitted_operators={NAMESPACE: set()})
    runtime = build_in_memory_runtime(fake_clock, catalog=catalog)
    operator = make_operator('op', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[operator], seed=[work_email()]
    ).run()
    assert result.status is SessionStatus.COMPLETED  # nothing ran, but the session terminated cleanly
    assert result.operator_runs == {}
    assert {dp.type for dp in (await runtime.store.snapshot(SID)).all()} == {'work_email'}
    assert await runtime.lock.is_complete(SID)


async def test_orch_gated_aggregator_is_skipped_without_wedging_the_session(fake_clock: FakeClock) -> None:
    catalog = InMemoryCapabilityCatalog(permitted_operators={NAMESPACE: {OperatorId('scorer')}})
    runtime = build_in_memory_runtime(fake_clock, catalog=catalog)

    async def write_report(ctx: OperatorContext) -> None:
        assert ctx.aggregation is not None
        await ctx.aggregation.upsert('reports', 'report', {'risk_count': len(ctx.store.of_type(RiskDataPoint))})

    scorer = make_operator('scorer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    reporter = make_aggregator('rep', depends_on={RiskDataPoint}, on_aggregate=write_report)
    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[scorer, reporter], seed=[work_email()]
    ).run()

    assert result.status is SessionStatus.COMPLETED  # the gated aggregator did not wedge aggregation
    assert result.operator_runs == {OperatorId('scorer'): 1}
    assert result.dead_letters == ()
    assert await runtime.durable.read('reports', 'report') is None  # its output domain was simply not written


async def test_orch_resumed_session_continues_a_shrinking_deadline_budget(fake_clock: FakeClock) -> None:
    # The wall-clock deadline persisted at the first gather is rehydrated on resume: the second
    # run gets the REMAINING 40s of the 100s budget, not a fresh 100s window.
    runtime = build_in_memory_runtime(fake_clock)

    def orchestrator(*, park_after: float | None, seed: list[Any]) -> Orchestrator:
        return Orchestrator(
            session_id=SID,
            namespace_id=NAMESPACE,
            runtime=runtime,
            operators=[],
            seed=seed,
            completes_when=ChatAnswerDataPoint,  # never arrives — only the deadline can end the wait
            session_deadline=100.0,
            park_after=park_after,
        )

    first = await orchestrator(park_after=10.0, seed=[work_email()]).run()
    assert first.status is SessionStatus.PARKED  # 10s of the budget spent waiting
    fake_clock.advance(50.0)  # 50 more seconds pass while the session sits parked

    resumed_at = fake_clock.monotonic()
    second = await orchestrator(park_after=None, seed=[]).run()

    assert second.status is SessionStatus.COMPLETED  # the deadline, not a park, ended the resume
    assert fake_clock.monotonic() - resumed_at == 40.0  # 100s budget - 10s waited - 50s parked


async def test_orch_resume_past_the_stored_deadline_aggregates_immediately(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    seen: dict[str, int] = {}

    async def aggregate(ctx: OperatorContext) -> None:
        seen['emails'] = len(ctx.store.of_type(EmailDataPoint))

    reporter = make_aggregator('rep', depends_on={EmailDataPoint}, on_aggregate=aggregate)

    def orchestrator(seed: list[Any]) -> Orchestrator:
        return Orchestrator(
            session_id=SID,
            namespace_id=NAMESPACE,
            runtime=runtime,
            operators=[reporter],
            seed=seed,
            completes_when=ChatAnswerDataPoint,
            session_deadline=100.0,
            park_after=10.0,
        )

    first = await orchestrator(seed=[work_email()]).run()
    assert first.status is SessionStatus.PARKED and seen == {}
    fake_clock.advance(150.0)  # the stored wall-clock deadline passes while the session is parked

    resumed_at = fake_clock.monotonic()
    second = await orchestrator(seed=[]).run()

    assert second.status is SessionStatus.COMPLETED
    assert seen == {'emails': 1}  # aggregation ran over the rehydrated state
    assert fake_clock.monotonic() == resumed_at  # no gathering wait at all — the budget was already spent


async def test_orch_tripped_breaker_makes_an_armed_retry_terminal(fake_clock: FakeClock) -> None:
    # A self-cycle operator declares BOTH a circuit breaker (max_cycles) and a retry policy, and
    # fails on every attempt. record_completion records the breaker run BEFORE _maybe_schedule_retry,
    # so the attempt that trips the breaker must make the retry terminal even with attempts remaining:
    # the operator is permanently skipped in _plan, so an armed retry could never launch — arming one
    # would leave _failed_attempts dangling and the session waiting on a relaunch that never comes due.
    runtime = build_in_memory_runtime(fake_clock)
    flaky_cycle = make_operator(
        'flakyloop',
        produces={IpDataPoint},
        depends_on={IpDataPoint},
        rerun_on_new_data=True,
        max_cycles=2,  # breaker trips on the 2nd run
        retry=RetryPolicy(max_attempts=5, base_delay=20.0, jitter=0.0),  # attempts remain when the breaker trips
        raise_error=ValueError('boom'),
    )
    with capture_logs(level='WARNING') as records:
        result = await Orchestrator(
            session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[flaky_cycle], seed=[ip('seed')]
        ).run()

    assert result.status is SessionStatus.COMPLETED  # the loop reached aggregation deterministically, no stall
    assert result.operator_runs[OperatorId('flakyloop')] == 2  # bounded by max_cycles, not by max_attempts
    scheduled = [r for r in records if 'a retry is scheduled' in r['message']]
    terminal = [r for r in records if 'no retry remaining' in r['message']]
    # Only the FIRST failure arms a retry (breaker count 1 < cap 2). The SECOND failure trips the breaker
    # (record_run runs before _maybe_schedule_retry), so its retry is foreclosed and recorded as terminal —
    # no third relaunch is ever armed, so _failed_attempts leaves nothing dangling for quiescence to wait on.
    assert len(scheduled) == 1
    assert len(terminal) == 1


async def test_orch_non_satisfying_inbox_arrival_restarts_the_idle_park_clock(fake_clock: FakeClock) -> None:
    # The completion condition needs BOTH a chat answer and an Ip. The session enters the inbox wait,
    # idles partway, then a chat answer (activity, but not enough to satisfy all_of) arrives at t=20.
    # The loop re-plans, finds the condition unsatisfied, and re-enters the wait with a FRESH park_at:
    # parking is measured from continuous idleness, so the session parks a full park_after AFTER the
    # arrival (t=20+30=50), not at the original t=30.
    runtime = build_in_memory_runtime(fake_clock)

    async def deliver_a_partial_arrival_at_t20() -> None:
        deadline = asyncio.get_running_loop().time() + 1.0
        while asyncio.get_running_loop().time() < deadline:
            if fake_clock.monotonic() >= 20.0:
                await runtime.inbox.append(SID, chat_answer('partial'))  # activity, but all_of still needs an Ip
                return
            await asyncio.sleep(0)  # yield to the gather loop (its FakeClock.sleep advances the clock)
        raise AssertionError('the fake clock never reached t=20 while the session waited')

    orchestrator = Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[],
        seed=[work_email()],
        completes_when=all_of(ChatAnswerDataPoint, IpDataPoint),  # the Ip never arrives
        session_deadline=300.0,
        park_after=30.0,
    )
    result, _ = await asyncio.gather(orchestrator.run(), deliver_a_partial_arrival_at_t20())

    assert result.status is SessionStatus.PARKED  # never satisfied → eventually parks
    folded = {dp.value for dp in (await runtime.store.snapshot(SID)).of_type(ChatAnswerDataPoint)}
    assert folded == {'partial'}  # the arrival was folded into the session before re-parking
    # The park fired a full 30s window measured from the t=20 arrival, NOT from the original wait start —
    # an inbox arrival is activity, so only continuous idleness can park the session.
    assert fake_clock.monotonic() == 50.0


async def test_orch_newly_available_capability_reruns_an_already_run_operator(fake_clock: FakeClock) -> None:
    # A newly-available capability is a first-class rerun trigger, distinct from new data: the watcher
    # depends only on Email (present from the seed) and does NOT depend on Ip. It runs once while the
    # capability is offline (the capability's own Ip dependency has not arrived). A feeder then emits Ip
    # mid-run, the capability comes online, and the watcher reruns SOLELY because delta.newly_available_caps
    # is non-empty — the Ip itself is not a depended-on type, so the data branch could never have triggered.
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {CapabilityId('geo_lookup')}})
    runtime = build_in_memory_runtime(fake_clock, catalog=catalog)
    geo_lookup = make_capability('geo_lookup', depends_on={IpDataPoint})  # offline until an Ip lands
    watcher_started = asyncio.Event()
    ip_merged = asyncio.Event()
    deltas: list[tuple[frozenset[CapabilityId], frozenset[str]]] = []

    class _Watcher(Operator):
        operator_id = OperatorId('cap_watcher')
        policy = OperatorPolicy(rerun_on_new_data=True, debounce=timedelta(0))
        depends_on = frozenset({EmailDataPoint})  # ready from the seed; does NOT depend on Ip
        produces = frozenset({ChatAnswerDataPoint})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:
            deltas.append((ctx.delta.newly_available_caps, frozenset(dp.type for dp in ctx.delta.added)))
            if not watcher_started.is_set():
                watcher_started.set()  # release the feeder to emit the Ip that brings the capability online
                await ip_merged.wait()  # stay running until that Ip has actually been merged
            yield ChatAnswerDataPoint.emit('seen')  # nothing depends on this — must not re-trigger the watcher

    class _Feeder(Operator):
        operator_id = OperatorId('cap_feeder')
        policy = OperatorPolicy(rerun_on_new_data=False)
        depends_on = frozenset({EmailDataPoint})
        produces = frozenset({IpDataPoint})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:  # noqa: ARG002
            await watcher_started.wait()  # emit the Ip only once the watcher is mid-run
            yield IpDataPoint.emit('203.0.113.7')

    async def release_once_ip_is_merged() -> None:
        deadline = asyncio.get_running_loop().time() + 1.0
        while asyncio.get_running_loop().time() < deadline:
            if (await runtime.store.snapshot(SID)).of_type(IpDataPoint):
                ip_merged.set()
                return
            await asyncio.sleep(0)  # yield to the orchestrator loop
        raise AssertionError('timed out waiting for the Ip to merge')

    orchestrator = Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[_Watcher, _Feeder],
        capabilities=[geo_lookup],
        seed=[work_email()],
    )
    result, _ = await asyncio.gather(orchestrator.run(), release_once_ip_is_merged())

    assert result.status is SessionStatus.COMPLETED
    assert result.operator_runs[OperatorId('cap_watcher')] == 2  # reran once the capability came online
    first_caps, _first_added = deltas[0]
    second_caps, second_added = deltas[1]
    assert first_caps == frozenset()  # the capability was offline at the first run
    assert CapabilityId('geo_lookup') in second_caps  # the capability drove the rerun
    # The Ip is NOT a watcher dependency, so even though it appears in delta.added it cannot have
    # triggered the rerun via the data branch — only the newly-available capability could.
    assert not any(added_type == 'work_email' for added_type in second_added)


async def test_orch_mirror_reobserves_a_preexisting_identity_presented_twice_in_one_batch() -> None:
    # The intra-batch duplicate-of-a-PRE-EXISTING-identity branch (`key in updated`) must reproduce the
    # store's keyed-merge: a row established before the batch, then presented twice within one later batch,
    # stays a SINGLE updated row carrying its final (max) last_retrieved — never two rows, never a stale stamp.
    t1, t2 = T0 + timedelta(seconds=5), T0 + timedelta(seconds=10)
    epoch = Epoch(1)
    mirror_store = InMemoryDataPointStore()
    reference = InMemoryDataPointStore()
    mirror = SessionStateMirror(mirror_store, SID)
    await mirror.rehydrate()

    # batch1 establishes the identity at T0 in both the mirror and the reference store.
    await mirror.write([work_email(last=T0)], epoch=epoch)
    await reference.write(SID, [work_email(last=T0)], epoch=epoch)
    revision_after_batch1 = await mirror.revision(SID)

    # batch2 presents the SAME pre-existing identity twice: the first occurrence takes the existing-entry
    # branch into `updated`, the second occurrence takes the `key in updated` branch and re-observes again.
    batch2 = [work_email(last=t1), work_email(last=t2)]
    mirror_result = await mirror.write(batch2, epoch=epoch)
    await reference.write(SID, batch2, epoch=epoch)

    # Exact parity with the store reference: snapshot, revision, and the change-set since batch1.
    mirror_snapshot = sorted((dp.value, dp.last_retrieved) for dp in (await mirror.snapshot(SID)).all())
    reference_snapshot = sorted((dp.value, dp.last_retrieved) for dp in (await reference.snapshot(SID)).all())
    assert mirror_snapshot == reference_snapshot
    assert int(await mirror.revision(SID)) == int(await reference.revision(SID))
    mirror_changes = await mirror.change_set_since(SID, revision_after_batch1)
    reference_changes = await reference.change_set_since(SID, revision_after_batch1)
    assert [dp.value for dp in mirror_changes.updated] == [dp.value for dp in reference_changes.updated]
    assert [dp.value for dp in mirror_changes.added] == [dp.value for dp in reference_changes.added]

    # The single surviving row carries the final (max) timestamp, and the batch produced exactly one row.
    (entry,) = (await mirror.snapshot(SID)).all()
    assert entry.last_retrieved == t2
    assert len(mirror_result.outcomes) == 2  # two presented DataPoints, both resolved as UPDATED
    assert all(outcome.kind.value == 'updated' for outcome in mirror_result.outcomes)


async def test_orch_deadline_wins_the_tie_against_an_equal_park_window(fake_clock: FakeClock) -> None:
    # park_at == deadline (park_after == session_deadline): the deadline check precedes the park check in
    # _wait_for_inbox, so the deadline wins the exact tie — the session aggregates and COMPLETEs and never
    # parks, despite park_after being set. This ordering is load-bearing for a flow whose shrinking budget
    # drops to exactly park_after.
    runtime = build_in_memory_runtime(fake_clock)
    aggregated = {'ran': False}

    async def aggregate(ctx: OperatorContext) -> None:  # noqa: ARG001
        aggregated['ran'] = True

    reporter = make_aggregator('rep', depends_on={EmailDataPoint}, on_aggregate=aggregate)
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[reporter],
        seed=[work_email()],
        completes_when=ChatAnswerDataPoint,  # never arrives — only the deadline/park can end the wait
        session_deadline=30.0,
        park_after=30.0,  # exactly equal to the deadline → the tie must resolve to the deadline
    ).run()

    assert result.status is SessionStatus.COMPLETED  # the deadline won the tie, not parking
    assert aggregated == {'ran': True}  # aggregation ran (parking would have skipped it)
    assert fake_clock.monotonic() == 30.0  # the wait ended exactly at the tie instant
    assert not [e for e in await runtime.audit.replay(SID) if e.kind is AuditKind.SESSION_PARKED]


async def test_orch_deadline_wins_when_park_after_exceeds_the_deadline(fake_clock: FakeClock) -> None:
    # park_after > session_deadline can never park: the deadline always elapses first, so the session
    # completes at the deadline regardless of the larger park window.
    runtime = build_in_memory_runtime(fake_clock)
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[],
        seed=[work_email()],
        completes_when=ChatAnswerDataPoint,
        session_deadline=30.0,
        park_after=60.0,  # larger than the deadline → unreachable, the deadline bounds the wait
    ).run()

    assert result.status is SessionStatus.COMPLETED
    assert fake_clock.monotonic() == 30.0  # ended at the deadline, never at the (later) park window
    assert not [e for e in await runtime.audit.replay(SID) if e.kind is AuditKind.SESSION_PARKED]


async def test_orch_emission_queue_size_zero_does_not_silently_unbound_the_queue(fake_clock: FakeClock) -> None:
    # asyncio.Queue(maxsize<=0) is UNBOUNDED — passing emission_queue_size=0 straight through would defeat
    # backpressure entirely, letting a runaway streamer grow the heap without limit. A non-positive size
    # must be coerced to the bounded default rather than becoming an immortal-buffer footgun.
    runtime = build_in_memory_runtime(fake_clock)
    operator = make_operator('op', produces={RiskDataPoint}, emits=[risk()])
    orchestrator = Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[operator], emission_queue_size=0
    )
    queue = orchestrator._build_emission_queue()
    assert queue.maxsize > 0  # bounded, never the asyncio "unbounded" sentinel
    assert queue.maxsize == 1024  # coerced to the bounded default


async def test_orch_emission_queue_size_zero_burst_stays_bounded(fake_clock: FakeClock) -> None:
    # End to end: a multi-emission burst under emission_queue_size=0 must NOT let the queue grow to the
    # full burst size (which an unbounded queue would). The high-water mark proves backpressure held.
    runtime = build_in_memory_runtime(fake_clock)
    burst = make_operator('burst', produces={IpDataPoint}, emits=[ip(f'198.51.100.{n}') for n in range(1, 21)])
    orchestrator = _ProbeQueueOrchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[burst], emission_queue_size=0
    )

    result = await orchestrator.run()

    assert result.status is SessionStatus.COMPLETED
    stored = {dp.value for dp in (await runtime.store.snapshot(SID)).of_type(IpDataPoint)}
    assert stored == {f'198.51.100.{n}' for n in range(1, 21)}  # backpressure, not loss — every emission landed
    assert orchestrator.probe.maxsize > 0  # coerced to a bounded queue, not the asyncio unbounded sentinel
    # The queue never exceeds its own bound — the genuine backpressure invariant. With maxsize==0 it would
    # have grown to hold the full burst at once; bounded, it never can.
    assert orchestrator.probe.high_water <= orchestrator.probe.maxsize


async def test_orch_session_deadline_cancels_an_in_flight_operator_and_redrains(fake_clock: FakeClock) -> None:
    # The deadline fires while an operator is genuinely mid-stream. The loop exits its while-condition with
    # the straggler still in `running`, logs the deadline hit naming it, and _drain_remaining persists the
    # already-queued emissions, cancels the straggler (the deadline is the ONE signal allowed to stop a
    # running operator), then drains ONCE MORE to capture the emission the task enqueues as it is cancelled.
    probe = TelemetryProbe()
    runtime = build_in_memory_runtime(fake_clock, telemetry=probe.telemetry)
    ip_emitted = asyncio.Event()
    wake_loop = asyncio.Event()

    class _Straggler(Operator):
        operator_id = OperatorId('straggler')
        # A large per-op timeout so only the session deadline (not the per-op timeout) can stop it.
        policy = OperatorPolicy(rerun_on_new_data=False, timeout=timedelta(seconds=3600))
        depends_on = frozenset({EmailDataPoint})
        produces = frozenset({IpDataPoint})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:  # noqa: ARG002
            yield IpDataPoint.emit('before-deadline')  # a pre-deadline emission that must persist
            ip_emitted.set()
            try:
                await asyncio.sleep(3600)  # real sleep — keeps the task in flight until cancelled
            except asyncio.CancelledError:
                yield IpDataPoint.emit('on-cancel')  # a final emission enqueued right as the task is cancelled
                raise

    class _Ticker(Operator):
        # Wakes the gather loop (blocked on queue.get) after the clock has been advanced past the deadline,
        # so the loop re-checks the deadline with the straggler still running.
        operator_id = OperatorId('ticker')
        policy = OperatorPolicy(rerun_on_new_data=False)
        depends_on = frozenset({EmailDataPoint})
        produces = frozenset({RiskDataPoint})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:  # noqa: ARG002
            await wake_loop.wait()
            yield RiskDataPoint.emit(0.5)

    async def trip_the_deadline_mid_run() -> None:
        deadline = asyncio.get_running_loop().time() + 1.0
        while asyncio.get_running_loop().time() < deadline:
            merged = {dp.value for dp in (await runtime.store.snapshot(SID)).of_type(IpDataPoint)}
            if ip_emitted.is_set() and 'before-deadline' in merged:
                fake_clock.advance(20.0)  # push monotonic past the 10s deadline while the straggler runs
                wake_loop.set()  # wake the loop so it re-checks the (now elapsed) deadline
                return
            await asyncio.sleep(0)
        raise AssertionError('the straggler never emitted before the deadline was tripped')

    orchestrator = Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[_Straggler, _Ticker],
        seed=[work_email()],
        operation_timeout=3600.0,  # the global default must not pre-empt either operator
        session_deadline=10.0,
    )
    with capture_logs(level='WARNING') as records:
        result, _ = await asyncio.gather(orchestrator.run(), trip_the_deadline_mid_run())

    assert result.status is SessionStatus.COMPLETED  # aggregation still ran after the deadline-cancel
    merged = {dp.value for dp in (await runtime.store.snapshot(SID)).of_type(IpDataPoint)}
    assert 'before-deadline' in merged  # the pre-deadline emission persisted
    assert 'on-cancel' in merged  # the post-cancel final emission was captured by the second drain
    assert probe.counter('session_deadline_hits_total') == 1
    hit = next(r for r in records if 'Session deadline hit' in r['message'])
    assert OperatorId('straggler') in hit['extra']['in_flight_operator_ids']  # the running op is named


async def test_orch_session_deadline_cuts_a_running_operator_short_not_awaiting_its_end(fake_clock: FakeClock) -> None:
    # POLICY-03/06: the deadline is the ONE signal allowed to stop a running operator. An operator that
    # would emit again after a long sleep is cancelled mid-sleep — its pre-deadline emission persists, but
    # the post-sleep emission never lands, proving the run was cut short rather than awaited to completion.
    runtime = build_in_memory_runtime(fake_clock)
    pre_emitted = asyncio.Event()
    wake_loop = asyncio.Event()

    class _LongRunner(Operator):
        operator_id = OperatorId('long_runner')
        policy = OperatorPolicy(rerun_on_new_data=False, timeout=timedelta(seconds=3600))
        depends_on = frozenset({EmailDataPoint})
        produces = frozenset({IpDataPoint})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:  # noqa: ARG002
            yield IpDataPoint.emit('pre-deadline')
            pre_emitted.set()
            await asyncio.sleep(3600)  # the deadline must cancel this, not wait it out
            yield IpDataPoint.emit('post-sleep')  # unreachable once cancelled

    class _Ticker(Operator):
        operator_id = OperatorId('ticker')
        policy = OperatorPolicy(rerun_on_new_data=False)
        depends_on = frozenset({EmailDataPoint})
        produces = frozenset({RiskDataPoint})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:  # noqa: ARG002
            await wake_loop.wait()
            yield RiskDataPoint.emit(0.5)

    async def trip_the_deadline_mid_run() -> None:
        deadline = asyncio.get_running_loop().time() + 1.0
        while asyncio.get_running_loop().time() < deadline:
            merged = {dp.value for dp in (await runtime.store.snapshot(SID)).of_type(IpDataPoint)}
            if pre_emitted.is_set() and 'pre-deadline' in merged:
                fake_clock.advance(20.0)
                wake_loop.set()
                return
            await asyncio.sleep(0)
        raise AssertionError('the long-runner never emitted before the deadline was tripped')

    with capture_logs(level='WARNING') as records:
        result, _ = await asyncio.gather(
            Orchestrator(
                session_id=SID,
                namespace_id=NAMESPACE,
                runtime=runtime,
                operators=[_LongRunner, _Ticker],
                seed=[work_email()],
                operation_timeout=3600.0,
                session_deadline=10.0,
            ).run(),
            trip_the_deadline_mid_run(),
        )

    assert result.status is SessionStatus.COMPLETED  # aggregation still runs after the deadline cancel
    merged = {dp.value for dp in (await runtime.store.snapshot(SID)).of_type(IpDataPoint)}
    assert 'pre-deadline' in merged  # the emission produced before the deadline persisted
    assert 'post-sleep' not in merged  # the run was cancelled mid-sleep, never reaching its later yield
    hit = next(r for r in records if 'Session deadline hit' in r['message'])
    assert OperatorId('long_runner') in hit['extra']['in_flight_operator_ids']


async def test_orch_wakeup_in_the_same_pass_as_an_elapsed_park_window_wins(fake_clock: FakeClock) -> None:
    # 'Data beats parking': the wait loop checks waiter.done() at the TOP, before re-checking park_at. When
    # the renew timer advances now to exactly park_at on the same pass the inbox waiter resolves, the next
    # iteration sees the wakeup and returns it — the elapsed park window does not win the tie.
    probe = TelemetryProbe()
    runtime = build_in_memory_runtime(fake_clock, telemetry=probe.telemetry)

    async def deliver_exactly_at_the_park_instant() -> None:
        deadline = asyncio.get_running_loop().time() + 1.0
        while asyncio.get_running_loop().time() < deadline:
            # park_after is 30; append the moment the fake clock reaches 30 (== park_at), inside the timer's
            # sleep(0) yield, so the waiter resolves in the same asyncio.wait the park window elapses.
            if fake_clock.monotonic() >= 30.0:
                await runtime.inbox.append(SID, chat_answer('arrived'))
                return
            await asyncio.sleep(0)
        raise AssertionError('the fake clock never reached the park instant')

    orchestrator = Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[],
        seed=[work_email()],
        completes_when=ChatAnswerDataPoint,  # satisfied by the arrival → the wakeup ends the run
        session_deadline=300.0,  # far away — only the park-vs-wakeup tie is under test
        park_after=30.0,
    )
    result, _ = await asyncio.gather(orchestrator.run(), deliver_exactly_at_the_park_instant())

    assert result.status is SessionStatus.COMPLETED  # the wakeup won; parking would have left it PARKED
    assert {dp.value for dp in (await runtime.store.snapshot(SID)).of_type(ChatAnswerDataPoint)} == {'arrived'}
    assert not [e for e in await runtime.audit.replay(SID) if e.kind is AuditKind.SESSION_PARKED]
    # The wait span's own outcome attribute records what ended the wait: a wakeup, not a park.
    wait_spans = probe.spans('session.inbox_wait')
    assert wait_spans  # the session did enter an inbox wait
    assert dict(wait_spans[-1].attributes or {})['outcome'] == 'wakeup'


class _BatchAuditFailingSink(InMemoryAuditSink):
    """Audit sink whose batched DataPoint-added append fails (a transient sink fault mid-batch)."""

    async def append_many(self, entries: Any) -> None:
        if any(entry.kind is AuditKind.DATA_POINT_ADDED for entry in entries):
            raise ValueError('audit sink down mid-batch')
        await super().append_many(entries)


async def test_orch_merge_counter_does_not_outrun_the_audit_trail_when_the_append_fails(
    fake_clock: FakeClock,
) -> None:
    # README invariant: metrics ride the audit seam so they can never disagree. When the batched audit
    # append fails after a successful store apply (a transient sink fault), the data_points_merged_total
    # counter must NOT advance for entries whose audit row never committed — otherwise the metric and the
    # replayed audit trail disagree, exactly what the seam is meant to prevent.
    probe = TelemetryProbe()
    runtime = replace(build_in_memory_runtime(fake_clock, telemetry=probe.telemetry), audit=_BatchAuditFailingSink())
    for index in range(3):
        await runtime.inbox.append(SID, chat_answer(f'answer-{index}'))  # a 3-entry batch → one append_many

    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[],
        completes_when=ChatAnswerDataPoint,
        session_deadline=30.0,
    ).run()

    assert result.status is SessionStatus.COMPLETED  # the bookkeeping failure is absorbed, the session finishes
    committed_added = len([e for e in await runtime.audit.replay(SID) if e.kind is AuditKind.DATA_POINT_ADDED])
    # The counter must equal the number of DATA_POINT_ADDED rows that actually committed — they ride the
    # same seam, so a failed append leaves neither the row nor the count behind.
    assert probe.counter_total('data_points_merged_total') == committed_added


async def test_orch_chatty_self_cycle_store_and_mirror_are_bounded_by_the_cycle_cap(fake_clock: FakeClock) -> None:
    # S2 (unbounded-growth bound beyond emission_queue_size): a chatty self-cycle that emits a NEW distinct
    # identity every cycle must not grow the session's in-memory state without limit. The circuit breaker
    # caps the cycle at max_cycles, so both the durable store and the sole-mutator mirror hold at most one
    # identity per cycle plus the seed — bounded by the cap, not by unbounded distinct identities.
    runtime = build_in_memory_runtime(fake_clock)
    counter = count()
    chatty_cycle = make_operator(
        'chattyloop',
        produces={IpDataPoint},
        depends_on={IpDataPoint},
        rerun_on_new_data=True,
        max_cycles=3,
        emit_factory=lambda _ctx: [ip(f'ip-{next(counter)}')],  # a brand-new identity each cycle  # noqa: ARG005
    )
    orchestrator = Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[chatty_cycle], seed=[ip('seed')]
    )
    result = await orchestrator.run()

    assert result.status is SessionStatus.COMPLETED
    runs = result.operator_runs[OperatorId('chattyloop')]
    assert runs <= 3  # bounded by the armed circuit breaker
    stored_ips = (await runtime.store.snapshot(SID)).of_type(IpDataPoint)
    # Each run adds exactly one new identity; with the seed that is runs + 1, and never more — the cap
    # bounds the distinct-identity growth, so the store cannot accumulate without limit.
    assert len(stored_ips) == runs + 1
    assert len(stored_ips) <= 4  # max_cycles (3) distinct emissions + the seed
    # The sole-mutator mirror holds the identical bounded set — it never diverges from the store it writes.
    assert len(orchestrator._mirror._entries) == len(stored_ips)


@pytest.mark.parametrize(
    ('park_after', 'expected_status', 'expected_monotonic'),
    [
        (29.0, SessionStatus.PARKED, 29.0),  # park_at < deadline (-epsilon): the park window wins
        (30.0, SessionStatus.COMPLETED, 30.0),  # park_at == deadline (the exact tie): the deadline wins
        (31.0, SessionStatus.COMPLETED, 30.0),  # park_at > deadline (+epsilon): the deadline elapses first
    ],
)
async def test_orch_park_vs_deadline_boundary_table(
    fake_clock: FakeClock, park_after: float, expected_status: SessionStatus, expected_monotonic: float
) -> None:
    # S3 (systematic clock-edge table): evaluate the deadline-vs-park boundary at -epsilon / exact tie /
    # +epsilon around session_deadline=30. The wait checks the deadline BEFORE park_at, so the exact tie
    # and the over-shoot both resolve to the deadline (COMPLETED); only a strictly-earlier park window parks.
    runtime = build_in_memory_runtime(fake_clock)
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[],
        seed=[work_email()],
        completes_when=ChatAnswerDataPoint,  # never arrives — only the deadline or the park can end the wait
        session_deadline=30.0,
        park_after=park_after,
    ).run()

    assert result.status is expected_status
    assert fake_clock.monotonic() == expected_monotonic
    parked = [e for e in await runtime.audit.replay(SID) if e.kind is AuditKind.SESSION_PARKED]
    assert bool(parked) is (expected_status is SessionStatus.PARKED)  # the audit trail agrees with the verdict


async def test_orch_drains_late_inbox_entry_before_release_same_epoch(fake_clock: FakeClock) -> None:
    # A late /participant deliver that lands during the completion flush must be drained in-run on the
    # SAME epoch (no re-spawn): after aggregating + flushing the orchestrator re-checks the inbox and,
    # finding a pending entry, loops back to gather + aggregate before releasing the lock.
    runtime = build_in_memory_runtime(fake_clock)
    # Unique session id + durable table + operator id so the test is hermetic under any ordering
    # (the shared 'rep'/'reports' names are reused by ~50 other tests).
    sid = SessionId('orch-drain-late-session')
    seen_emails: list[list[str]] = []
    injected = {'done': False}

    async def write(ctx: OperatorContext) -> None:
        assert ctx.aggregation is not None
        emails = sorted(dp.value for dp in ctx.store.of_type(EmailDataPoint))
        seen_emails.append(emails)
        await ctx.aggregation.upsert('drain_late_reports', 'report', {'emails': emails})
        # Simulate a /participant deliver landing during the FIRST completion pass (mid-aggregate),
        # i.e. inside the flush window: append a late attribute to the inbox exactly once.
        if not injected['done']:
            injected['done'] = True
            await runtime.inbox.append(sid, work_email('late@work.example'))

    reporter = make_aggregator('drain_late_reporter', depends_on={EmailDataPoint}, on_aggregate=write)
    result = await Orchestrator(
        session_id=sid,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[reporter],
        completes_when=EmailDataPoint,
        seed=[work_email()],
    ).run()

    assert result.status is SessionStatus.COMPLETED
    assert result.epoch == 1  # SAME run/epoch — the late entry was drained in-run, NOT via a re-spawn
    doc = await runtime.durable.read('drain_late_reports', 'report')
    assert doc is not None and sorted(doc.document['emails']) == ['alice@work.example', 'late@work.example']
    assert await runtime.inbox.pending_count(sid) == 0  # fully drained before release
    assert len(seen_emails) == 2  # aggregator ran twice in the one run: initial + re-drive


async def test_operator_constructor_failure_is_recorded_not_silently_dropped(fake_clock: FakeClock) -> None:
    # An operator whose __init__ raises (e.g. config validation) must be recorded as a FAILED run —
    # logged + audited — not dropped before it can enqueue completion (which would wedge it 'in flight'
    # with no trace). A second healthy operator must still complete the session.
    runtime = build_in_memory_runtime(fake_clock)

    class _BadInit(Operator):
        operator_id = OperatorId('bad_init')
        policy = OperatorPolicy(rerun_on_new_data=False)
        depends_on = frozenset({EmailDataPoint})
        produces = frozenset({RiskDataPoint})

        def __init__(self) -> None:
            raise ValueError('construction blew up')

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:  # noqa: ARG002
            yield RiskDataPoint.emit(0.5)

    class _Healthy(Operator):
        operator_id = OperatorId('healthy')
        policy = OperatorPolicy(rerun_on_new_data=False)
        depends_on = frozenset({EmailDataPoint})
        produces = frozenset({IpDataPoint})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:  # noqa: ARG002
            yield IpDataPoint.emit('203.0.113.9')

    with capture_logs(level='ERROR') as records:
        result = await Orchestrator(
            session_id=SID,
            namespace_id=NAMESPACE,
            runtime=runtime,
            operators=[_BadInit, _Healthy],
            seed=[work_email()],
        ).run()

    assert result.status is SessionStatus.COMPLETED  # the healthy operator still finishes the session
    assert result.operator_runs[OperatorId('bad_init')] == 1  # the construction failure is counted as a run
    runs = {
        entry.operator_id: entry.operator for entry in await runtime.audit.replay(SID) if entry.operator is not None
    }
    assert runs[OperatorId('bad_init')].outcome is OperatorOutcome.FAILED  # audited as FAILED, not absent
    # The audit records each run's own time (invoked -> completed) for profiling — present even on failure.
    assert runs[OperatorId('healthy')].run_seconds is not None
    assert runs[OperatorId('bad_init')].run_seconds is not None
    failure = next(record for record in records if record['extra'].get('operator_id') == OperatorId('bad_init'))
    assert failure['exception'] is not None  # logged with a traceback, not swallowed
    assert (await runtime.store.snapshot(SID)).of_type(IpDataPoint)  # the healthy operator's output landed
