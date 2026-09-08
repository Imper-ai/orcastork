"""E2E — cross-cutting end-to-end flows over the in-memory runtime + FakeClock.

These layer on the unit/conformance suites (ACC = single happy paths, RECOV = single crash
scenarios) to drive the *whole* engine under multi-actor, multi-wave conditions. "Cross-pod"
is modelled by two ``SessionOrchestrationManager``s sharing one ``OrchestratorRuntime`` — the
store/inbox/lock/durable/audit that real pods would share via Redis + Mongo. The fencing
epoch and the durable completion/contribution markers are what keep the pods correct, so
each flow asserts the durable end state, not just the in-process result.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import replace
from typing import Any

import pytest

from orcastork.adapters.memory import (
    InMemoryCapabilityCatalog,
    InMemoryDataPointStore,
    InMemorySessionLock,
)
from orcastork.audit import AuditKind
from orcastork.datapoints import BaseDataPoint, DataPointEmission
from orcastork.exceptions import StaleEpochError
from orcastork.flow import FlowDefinition
from orcastork.ids import CapabilityId, Epoch, NamespaceId, OperatorId, Revision, SessionId
from orcastork.manager import SessionOrchestrationManager
from orcastork.operators import Operator, OperatorContext, OperatorPolicy
from orcastork.orchestrator import Orchestrator, OrchestratorResult, SessionStatus
from orcastork.runtime import build_in_memory_runtime

from .doubles.capabilities import make_capability
from .doubles.clock import FakeClock
from .doubles.datapoints import (
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

NAMESPACE = NamespaceId('e2e-namespace')


def _flow(*operators: type[Operator], **kwargs: Any) -> FlowDefinition:
    return FlowDefinition(name='e2e-flow', operators=tuple(operators), **kwargs)


async def _write_risk_count(ctx: OperatorContext) -> None:
    """A representative aggregator: fold the gathered risks into one durable report."""
    assert ctx.aggregation is not None
    await ctx.aggregation.upsert('reports', 'report', {'risk_count': len(ctx.store.of_type(RiskDataPoint))})


async def _write_ip_count(ctx: OperatorContext) -> None:
    """A representative aggregator over Ip DataPoints (used by the deadline-straggler flow)."""
    assert ctx.aggregation is not None
    await ctx.aggregation.upsert('reports', 'report', {'ip_count': len(ctx.store.of_type(IpDataPoint))})


class _FenceAfterNApplies(InMemoryDataPointStore):
    """Store whose ``apply_resolved`` raises :class:`StaleEpochError` on its Nth call.

    Models a successor minting a higher epoch mid-batch: the very next forwarded merge is
    fenced by the store, exactly as the real CAS would reject a stale writer's write.
    """

    def __init__(self, *, raise_on_call: int) -> None:
        super().__init__()
        self._raise_on_call = raise_on_call
        self.applies = 0

    async def apply_resolved(self, session_id: SessionId, *, added: Any, updated: Any, epoch: Epoch) -> Revision:
        self.applies += 1
        if self.applies == self._raise_on_call:
            raise StaleEpochError('a higher epoch took over mid-batch')
        return await super().apply_resolved(session_id, added=added, updated=updated, epoch=epoch)


class _FenceOnCommitEffectStore(InMemoryDataPointStore):
    """Store whose ``commit_effect`` is fenced — a takeover lands before the effect's commit."""

    async def commit_effect(self, session_id: SessionId, effect_key: str, *, epoch: Epoch) -> None:  # noqa: ARG002
        raise StaleEpochError('a higher epoch took over before the effect commit landed')


class _FenceOnMarkCompleteLock(InMemorySessionLock):
    """Lock that grants/renews normally but fences the completion CAS — a takeover just before it."""

    async def mark_complete(self, session_id: SessionId, *, epoch: Epoch) -> None:  # noqa: ARG002
        raise StaleEpochError('a higher epoch took over before the completion CAS')


async def test_e2e_01_pod_handoff_reconstructs_unfinished_work_and_restores_capabilities(
    fake_clock: FakeClock,
) -> None:
    sid = SessionId('e2e-handoff')
    runtime = build_in_memory_runtime(
        fake_clock, catalog=InMemoryCapabilityCatalog(permitted={NAMESPACE: {CapabilityId('netcap')}})
    )
    collect = make_operator('collect', produces={IpDataPoint}, emits=[ip()])
    netcap = make_capability('netcap', depends_on={IpDataPoint})  # available once an Ip exists
    score = make_operator('score', requires={netcap}, produces={RiskDataPoint}, emits=[risk(0.8)])
    report = make_aggregator('report', depends_on={RiskDataPoint}, on_aggregate=_write_risk_count)
    operators = [collect, score, report]

    # Pod A ran `collect` (its Ip is persisted, its watermark advanced) then crashed before the rest.
    epoch_a = await runtime.lock.acquire(sid)
    await runtime.store.write(sid, [ip()], epoch=epoch_a)
    revision = await runtime.store.revision(sid)
    await runtime.store.set_watermark(sid, OperatorId('collect'), revision, epoch=epoch_a)
    fake_clock.advance(31.0)  # Pod A's ownership lease expires → the session is orphaned

    pod_b = SessionOrchestrationManager(runtime)
    assert await pod_b.is_orphaned(sid) is True
    result = await pod_b.resume(session_id=sid, namespace_id=NAMESPACE, flow=_flow(*operators, capabilities=(netcap,)))

    assert result is not None and result.epoch == 2  # taken over under a higher epoch
    assert OperatorId('collect') not in result.operator_runs  # reconstructed but not re-run (its watermark)
    assert result.operator_runs.get(OperatorId('score')) == 1  # only the unfinished operator ran
    assert len(netcap.activations) == 1  # type: ignore[attr-defined]  # capability restored: activated fresh on Pod B
    document = await runtime.durable.read('reports', 'report')
    assert document is not None and document.document == {'risk_count': 1}
    assert await runtime.lock.is_complete(sid) is True
    # A third pod must not re-drive the now-finished session.
    third = await SessionOrchestrationManager(runtime).resume(
        session_id=sid, namespace_id=NAMESPACE, flow=_flow(*operators, capabilities=(netcap,))
    )
    assert third is None


async def test_e2e_02_concurrent_pods_resume_one_orphan_exactly_once(fake_clock: FakeClock) -> None:
    sid = SessionId('e2e-race')
    runtime = build_in_memory_runtime(fake_clock)
    aggregate_calls = {'n': 0}

    async def write_report(ctx: OperatorContext) -> None:
        aggregate_calls['n'] += 1
        await _write_risk_count(ctx)

    scorer = make_operator('scorer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    report = make_aggregator('report', depends_on={RiskDataPoint}, on_aggregate=write_report)
    flow = _flow(scorer, report)

    # A predecessor seeded the session then crashed (lease expires) → orphaned.
    epoch_a = await runtime.lock.acquire(sid)
    await runtime.store.write(sid, [work_email()], epoch=epoch_a)
    fake_clock.advance(31.0)

    # Two pods both notice the orphan and race to resume it.
    pod_a, pod_b = SessionOrchestrationManager(runtime), SessionOrchestrationManager(runtime)
    outcomes = await asyncio.gather(
        pod_a.resume(session_id=sid, namespace_id=NAMESPACE, flow=flow),
        pod_b.resume(session_id=sid, namespace_id=NAMESPACE, flow=flow),
        return_exceptions=True,
    )

    driven = [outcome for outcome in outcomes if isinstance(outcome, OrchestratorResult)]
    assert len(driven) == 1 and driven[0].status is SessionStatus.COMPLETED  # exactly one pod drove it
    assert aggregate_calls['n'] == 1  # the aggregator ran exactly once — no double-drive
    document = await runtime.durable.read('reports', 'report')
    assert document is not None and document.document == {'risk_count': 1}


async def test_e2e_03_layered_capabilities_unblock_a_multi_wave_pipeline(fake_clock: FakeClock) -> None:
    sid = SessionId('e2e-layered')
    runtime = build_in_memory_runtime(
        fake_clock,
        catalog=InMemoryCapabilityCatalog(permitted={NAMESPACE: {CapabilityId('auth'), CapabilityId('net')}}),
    )
    activation_order: list[CapabilityId] = []
    auth = make_capability('auth', depends_on={EmailDataPoint}, record_order=activation_order)
    net = make_capability('net', depends_on={IpDataPoint}, requires={auth}, record_order=activation_order)
    collect = make_operator('collect', requires={auth}, produces={IpDataPoint}, emits=[ip()])
    score = make_operator('score', requires={net}, produces={RiskDataPoint}, emits=[risk()])
    report = make_aggregator('report', depends_on={RiskDataPoint}, on_aggregate=_write_risk_count)

    result = await Orchestrator(
        session_id=sid,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[collect, score, report],
        capabilities=[auth, net],
        seed=[work_email()],
    ).run()

    assert result.status is SessionStatus.COMPLETED
    assert result.operator_runs.get(OperatorId('collect')) == 1  # ran in wave 1 once `auth` was online
    assert result.operator_runs.get(OperatorId('score')) == 1  # ran in wave 2 once the layered `net` was online
    assert activation_order == [CapabilityId('auth'), CapabilityId('net')]  # base activated before the layer
    assert {dp.type for dp in (await runtime.store.snapshot(sid)).all()} == {'work_email', 'ip', 'risk'}
    activated = sorted(
        entry.capability.capability_id
        for entry in await runtime.audit.replay(sid)
        if entry.kind is AuditKind.CAPABILITY_ACTIVATED and entry.capability is not None
    )
    assert activated == ['auth', 'net']  # both activations were audited


async def test_e2e_04_slow_and_failing_operators_are_isolated(fake_clock: FakeClock) -> None:
    sid = SessionId('e2e-isolation')
    runtime = build_in_memory_runtime(fake_clock)
    collector = make_operator('collector', produces={RiskDataPoint}, emits=[risk()])
    slow = make_operator('slow', produces={IpDataPoint}, emits=[ip()], sleep_after=1.0)  # exceeds the op timeout
    failing = make_operator(
        'failing', produces={ChatAnswerDataPoint}, emits=[chat_answer()], raise_error=ValueError('boom')
    )
    report = make_aggregator('report', depends_on={RiskDataPoint}, on_aggregate=_write_risk_count)

    result = await Orchestrator(
        session_id=sid,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[collector, slow, failing, report],
        operation_timeout=0.02,
    ).run()

    assert result.status is SessionStatus.COMPLETED  # neither the timeout nor the failure wedged the session
    types = {dp.type for dp in (await runtime.store.snapshot(sid)).all()}
    assert {'risk', 'ip', 'chat_answer'} <= types  # each operator's emission persisted, even slow/failing ones
    document = await runtime.durable.read('reports', 'report')
    assert document is not None and document.document == {'risk_count': 1}  # the aggregator still ran


async def test_e2e_05_fenced_predecessor_cannot_mutate_or_complete_after_takeover(fake_clock: FakeClock) -> None:
    sid = SessionId('e2e-fencing')
    runtime = build_in_memory_runtime(fake_clock)
    scorer = make_operator('scorer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    report = make_aggregator('report', depends_on={RiskDataPoint}, on_aggregate=_write_risk_count)

    # Predecessor (Pod A) seeds under epoch 1 then stalls; its lease expires.
    stale_epoch = await runtime.lock.acquire(sid)
    await runtime.store.write(sid, [work_email()], epoch=stale_epoch)
    fake_clock.advance(31.0)

    # Pod B takes over under a higher epoch and finishes the session.
    resumed = await SessionOrchestrationManager(runtime).resume(
        session_id=sid, namespace_id=NAMESPACE, flow=_flow(scorer, report)
    )
    assert resumed is not None and resumed.epoch == 2
    assert await runtime.lock.is_complete(sid) is True

    # The fenced predecessor (still holding epoch 1) can neither mutate state nor declare the session done.
    with pytest.raises(StaleEpochError):
        await runtime.store.write(sid, [ip()], epoch=stale_epoch)
    with pytest.raises(StaleEpochError):
        await runtime.lock.mark_complete(sid, epoch=stale_epoch)

    # And a fresh supervisor still sees the session finished — it is not re-driven.
    not_resumed = await SessionOrchestrationManager(runtime).resume(
        session_id=sid, namespace_id=NAMESPACE, flow=_flow(scorer, report)
    )
    assert not_resumed is None


async def test_e2e_06_inbox_arrivals_survive_crash_and_apply_exactly_once(fake_clock: FakeClock) -> None:
    sid = SessionId('e2e-inbox')
    runtime = build_in_memory_runtime(fake_clock)

    def score(ctx: OperatorContext) -> list[RiskDataPoint]:
        return [risk(float(len(ctx.store.of_type(ChatAnswerDataPoint))))]

    scorer = make_operator('scorer', depends_on={ChatAnswerDataPoint}, produces={RiskDataPoint}, emit_factory=score)

    async def write_score(ctx: OperatorContext) -> None:
        assert ctx.aggregation is not None
        latest = ctx.latest(RiskDataPoint)
        await ctx.aggregation.upsert('reports', 'report', {'answers': None if latest is None else latest.value})

    report = make_aggregator('report', depends_on={RiskDataPoint}, on_aggregate=write_score)

    # Pod A: one inbox entry claimed-but-unacked (in-flight at the crash), one that arrived while it was down.
    await runtime.lock.acquire(sid)
    await runtime.inbox.append(sid, chat_answer('q1'))
    await runtime.inbox.consume(sid)  # claims q1, then the pod dies before applying/acking it
    await runtime.inbox.append(sid, chat_answer('q2'))  # arrives while the pod is offline
    fake_clock.advance(31.0)

    resumed = await SessionOrchestrationManager(runtime).resume(
        session_id=sid, namespace_id=NAMESPACE, flow=_flow(scorer, report)
    )

    assert resumed is not None
    answers = {dp.value for dp in (await runtime.store.snapshot(sid)).of_type(ChatAnswerDataPoint)}
    assert answers == {'q1', 'q2'}  # in-flight entry reclaimed, the new one consumed — each applied exactly once
    document = await runtime.durable.read('reports', 'report')
    assert document is not None and document.document == {'answers': 2.0}  # both reflected downstream
    assert await runtime.inbox.pending_count(sid) == 0  # both acked


async def test_e2e_07_session_waits_for_user_input_then_completes(fake_clock: FakeClock) -> None:
    """A challenge-shaped flow: present a question (operator), wait for the user's answer to
    arrive on the inbox (no operator holds a connection open), then validate and aggregate."""
    runtime = build_in_memory_runtime(fake_clock)
    session_id = SessionId('e2e-wait')
    question_presented = asyncio.Event()

    def present_question(ctx: OperatorContext) -> list[DataPointEmission]:  # noqa: ARG001
        question_presented.set()
        return [IpDataPoint.emit('203.0.113.9')]

    triage = make_operator(
        'triage', depends_on={EmailDataPoint}, produces={IpDataPoint}, emit_factory=present_question
    )
    answer_handler = make_operator(
        'answer_handler', depends_on={ChatAnswerDataPoint}, produces={RiskDataPoint}, emits=[risk(0.9)]
    )
    written: dict[str, int] = {}

    async def aggregate(ctx: OperatorContext) -> None:
        written['risk_count'] = len(ctx.store.of_type(RiskDataPoint))

    reporter = make_aggregator('reporter', depends_on={RiskDataPoint}, on_aggregate=aggregate)

    async def user_answers() -> None:
        await question_presented.wait()
        for _ in range(5):
            await asyncio.sleep(0)  # give the session time to reach the inbox wait (not required for correctness)
        await runtime.inbox.append(session_id, chat_answer('it was me'))

    orchestrator = Orchestrator(
        session_id=session_id,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[triage, answer_handler, reporter],
        seed=[work_email()],
        completes_when=RiskDataPoint,
    )
    result, _ = await asyncio.gather(orchestrator.run(), user_answers())

    assert result.status is SessionStatus.COMPLETED
    assert result.operator_runs[OperatorId('answer_handler')] == 1  # woken by the inbox arrival
    assert written == {'risk_count': 1}  # aggregation saw the post-answer state
    assert await runtime.inbox.pending_count(session_id) == 0  # the user action was applied and acked


async def test_e2e_08_pod_kill_during_inbox_wait_is_recovered_by_deliver(fake_clock: FakeClock) -> None:
    """Pod A dies while waiting for the user's answer; the answer lands on pod B's ingress.
    `deliver` appends durably, detects the orphan, and resumes the session on pod B."""
    sid = SessionId('e2e-deliver')
    runtime = build_in_memory_runtime(fake_clock)
    answer_handler = make_operator(
        'answer_handler', depends_on={ChatAnswerDataPoint}, produces={RiskDataPoint}, emits=[risk(0.9)]
    )
    report = make_aggregator('report', depends_on={RiskDataPoint}, on_aggregate=_write_risk_count)

    # Pod A seeded the session and was waiting on the inbox when it died (lease expires unreleased).
    epoch_a = await runtime.lock.acquire(sid)
    await runtime.store.write(sid, [work_email()], epoch=epoch_a)
    fake_clock.advance(31.0)

    pod_b = SessionOrchestrationManager(runtime)
    result = await pod_b.deliver(
        session_id=sid,
        namespace_id=NAMESPACE,
        data_point=chat_answer('it was me'),
        flow=_flow(answer_handler, report, completes_when=RiskDataPoint),
    )

    assert result is not None and result.status is SessionStatus.COMPLETED
    assert result.epoch == 2  # a fresh, higher-epoch orchestrator on pod B
    assert result.operator_runs.get(OperatorId('answer_handler')) == 1  # the answer was processed
    document = await runtime.durable.read('reports', 'report')
    assert document is not None and document.document == {'risk_count': 1}
    assert await runtime.inbox.pending_count(sid) == 0  # applied and acked — no message lost


def _deadline_straggler(
    op_id: str, *, emits: list[BaseDataPoint[Any]], clock: FakeClock, deadline: float
) -> type[Operator]:
    """An operator that emits, pushes the fake clock past the session deadline, then blocks forever.

    Emitting first lets the loop merge the DataPoint; advancing the clock makes the loop's NEXT
    while-check fail (a deadline hit with this operator still in ``running``); the never-set event
    keeps the task alive so ``_drain_remaining`` is the thing that cancels it as a straggler.
    """
    never = asyncio.Event()
    _emits = list(emits)

    class _Straggler(Operator):
        operator_id = OperatorId(op_id)
        policy = OperatorPolicy(rerun_on_new_data=False)
        produces = frozenset(type(dp) for dp in _emits)

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:  # noqa: ARG002
            for data_point in _emits:
                yield DataPointEmission(type(data_point), data_point.value)
            clock.advance(deadline + 1.0)  # the loop's next while-check now sees the deadline elapsed
            await never.wait()  # stay running so only the deadline-cancel can stop us

    return _Straggler


async def test_e2e_09_deadline_cancels_a_straggler_after_running_aggregation(fake_clock: FakeClock) -> None:
    # The deadline elapses while an operator is mid-run: the while/else deadline branch fires,
    # _drain_remaining persists the pre-deadline emission and CANCELS the straggler, and the
    # session still runs aggregation and COMPLETES — the deadline is the one signal that may stop
    # a running operator (no other cancellation).
    sid = SessionId('e2e-deadline-straggler')
    probe = TelemetryProbe()
    runtime = replace(build_in_memory_runtime(fake_clock), telemetry=probe.telemetry)
    straggler = _deadline_straggler('straggler', emits=[ip('203.0.113.5')], clock=fake_clock, deadline=5.0)
    report = make_aggregator('report', depends_on={IpDataPoint}, on_aggregate=_write_ip_count)

    with capture_logs(level='WARNING') as records:
        result = await Orchestrator(
            session_id=sid,
            namespace_id=NAMESPACE,
            runtime=runtime,
            operators=[straggler, report],
            session_deadline=5.0,
        ).run()

    assert result.status is SessionStatus.COMPLETED  # the deadline runs aggregation, not a wedge
    stored = {dp.value for dp in (await runtime.store.snapshot(sid)).of_type(IpDataPoint)}
    assert stored == {'203.0.113.5'}  # the pre-sleep emission persisted before the cancel
    assert probe.counter_total('session_deadline_hits_total') == 1  # exactly one deadline hit recorded
    document = await runtime.durable.read('reports', 'report')
    assert document is not None and document.document == {'ip_count': 1}  # aggregation still ran
    deadline_warn = next(r for r in records if 'deadline hit' in r['message'])
    assert deadline_warn['extra']['in_flight_operator_ids'] == [OperatorId('straggler')]  # the straggler named
    assert not await runtime.lock.is_held(sid)  # the epoch was released even on the deadline path


async def test_e2e_10_store_fence_on_operator_emission_merge_stops_as_superseded(fake_clock: FakeClock) -> None:
    # A successor mints a higher epoch mid-batch; the very next forwarded apply_resolved (the
    # operator-emission merge, after the seed merge already landed) is rejected with
    # StaleEpochError. That must unwind the gather loop into a clean SUPERSEDED stop — no durable
    # output, not marked complete, lock released.
    sid = SessionId('e2e-store-fence-emit')
    store = _FenceAfterNApplies(raise_on_call=2)  # call 1 = seed merge; call 2 = the operator emission
    runtime = replace(build_in_memory_runtime(fake_clock), store=store)
    scorer = make_operator('scorer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    report = make_aggregator('report', depends_on={RiskDataPoint}, on_aggregate=_write_risk_count)

    result = await Orchestrator(
        session_id=sid, namespace_id=NAMESPACE, runtime=runtime, operators=[scorer, report], seed=[work_email()]
    ).run()

    assert result.status is SessionStatus.SUPERSEDED  # the fenced merge ended the run cleanly
    assert await runtime.durable.read('reports', 'report') is None  # no durable output was written
    assert not await runtime.lock.is_complete(sid)  # never finalized
    assert not await runtime.lock.is_held(sid)  # the epoch was released on the fenced path


async def test_e2e_11_store_fence_on_inbox_apply_stops_as_superseded(fake_clock: FakeClock) -> None:
    # The fence lands inside the inbox-apply write path (_apply_inbox_data_points → mirror.write):
    # a takeover rejects the inbox merge, which must also propagate as a clean SUPERSEDED stop.
    sid = SessionId('e2e-store-fence-inbox')
    store = _FenceAfterNApplies(raise_on_call=1)  # the first apply is the inbox entry (no seed here)
    runtime = replace(build_in_memory_runtime(fake_clock), store=store)
    await runtime.inbox.append(sid, chat_answer('q1'))
    scorer = make_operator('scorer', depends_on={ChatAnswerDataPoint}, produces={RiskDataPoint}, emits=[risk()])

    result = await Orchestrator(
        session_id=sid, namespace_id=NAMESPACE, runtime=runtime, operators=[scorer], completes_when=RiskDataPoint
    ).run()

    assert result.status is SessionStatus.SUPERSEDED  # the inbox-apply fence ended the run cleanly
    assert await runtime.inbox.pending_count(sid) == 1  # the entry stays un-acked for the successor
    assert not await runtime.lock.is_complete(sid)
    assert not await runtime.lock.is_held(sid)


async def test_e2e_12_mark_complete_losing_the_cas_stops_as_superseded(fake_clock: FakeClock) -> None:
    # A higher epoch is minted between the end of aggregation and the completion CAS; mark_complete
    # raises StaleEpochError, which the orchestrator must turn into a clean SUPERSEDED stop rather
    # than leaking the rejection as a run failure or a (wrong) COMPLETED.
    sid = SessionId('e2e-fenced-finalize')
    lock = _FenceOnMarkCompleteLock(fake_clock)
    runtime = replace(build_in_memory_runtime(fake_clock), lock=lock)
    scorer = make_operator('scorer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    report = make_aggregator('report', depends_on={RiskDataPoint}, on_aggregate=_write_risk_count)

    result = await Orchestrator(
        session_id=sid, namespace_id=NAMESPACE, runtime=runtime, operators=[scorer, report], seed=[work_email()]
    ).run()

    assert result.status is SessionStatus.SUPERSEDED  # the lost completion CAS is a clean stop
    assert await runtime.lock.is_complete(sid) is False  # the predecessor never finalized
    assert not await runtime.lock.is_held(sid)  # the epoch was released


async def test_e2e_13_effect_commit_fenced_mid_once_degrades_cleanly(fake_clock: FakeClock) -> None:
    # Cross-subsystem cleanup-under-fencing: an operator claims an effect via ctx.once and runs its
    # body, but a takeover mints a higher epoch before the commit lands, so commit_effect(epoch=1)
    # is rejected. EffectGuard deliberately ABSORBS a failed commit (preferring a possibly-skipped
    # effect over a possibly-double-fired one within an epoch) — so the fenced commit degrades to a
    # logged warning, the claim is left as this epoch's pending mark for the successor's RERUN/SKIP
    # recovery policy, the operator is NOT crashed by the cleanup-path failure, and the session runs
    # to its normal disposition rather than a secondary crash masking the unwind.
    sid = SessionId('e2e-effect-fence')
    store = _FenceOnCommitEffectStore()
    runtime = replace(build_in_memory_runtime(fake_clock), store=store)
    effect_ran = {'fired': False}

    class _Sender(Operator):
        operator_id = OperatorId('sender')
        policy = OperatorPolicy(rerun_on_new_data=False)
        depends_on = frozenset({EmailDataPoint})
        produces = frozenset({RiskDataPoint})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:
            async with ctx.once('send-otp') as acquired:
                if acquired:
                    effect_ran['fired'] = True  # the side effect actually ran this attempt
                    yield DataPointEmission(RiskDataPoint, 0.7)

    with capture_logs(level='WARNING') as records:
        result = await Orchestrator(
            session_id=sid, namespace_id=NAMESPACE, runtime=runtime, operators=[_Sender], seed=[work_email()]
        ).run()

    assert result.status is SessionStatus.COMPLETED  # the absorbed commit failure is not a secondary crash
    assert effect_ran['fired'] is True  # the effect did run; only its commit was fenced
    assert result.operator_runs.get(OperatorId('sender')) == 1  # the operator ran without an injected error
    # The claim is left as this epoch's pending mark — never fabricated 'committed' — so the
    # successor's RERUN/SKIP recovery policy decides what the half-run effect means.
    assert await store.get_effect_state(sid, 'sender:send-otp') == 'pending:1'
    commit_warn = next(r for r in records if 'commit failed' in r['message'])
    assert commit_warn['extra']['effect_key'] == 'sender:send-otp'  # the degrade was logged, not raised
    assert not await runtime.lock.is_held(sid)  # the epoch was released regardless


def _cancel_emitting_straggler(
    op_id: str, *, wake_emit: BaseDataPoint[Any], final_emit: BaseDataPoint[Any], clock: FakeClock, deadline: float
) -> type[Operator]:
    """A straggler whose ONLY interesting emission is produced *as it is cancelled*.

    ``wake_emit`` is yielded normally — it merely unblocks the loop's ``queue.get`` so the next
    while-check can observe the elapsed deadline (without an emission the loop would block on an
    empty queue forever). ``final_emit`` is yielded from the ``CancelledError`` handler, i.e. only
    after ``_drain_remaining`` cancels the straggler. That emission is therefore enqueued *after*
    the loop's last in-loop drain, so the second drain-after-cancel is the only thing that can
    capture it — the precise path the first-drain alone cannot reach.
    """
    never = asyncio.Event()

    class _Straggler(Operator):
        operator_id = OperatorId(op_id)
        policy = OperatorPolicy(rerun_on_new_data=False)
        produces = frozenset({type(wake_emit), type(final_emit)})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:  # noqa: ARG002
            yield DataPointEmission(type(wake_emit), wake_emit.value)  # wakes the loop; merged in-loop
            clock.advance(deadline + 1.0)  # the loop's next while-check now sees the deadline elapsed
            try:
                await never.wait()  # stay running so only the deadline-cancel can stop us
            except asyncio.CancelledError:
                # The cancel arrives from _drain_remaining; emit one last DataPoint as we unwind so
                # only the post-cancel second drain can persist it, then let cancellation finish.
                yield DataPointEmission(type(final_emit), final_emit.value)
                raise

    return _Straggler


async def test_e2e_14_post_cancel_drain_captures_an_emission_made_during_cancellation(fake_clock: FakeClock) -> None:
    # At-least-once across the deadline-cancel boundary: an emission a straggler enqueues *as it is
    # cancelled* (already past the epoch-guarded merge boundary) must not be dropped. _drain_remaining
    # drains once, cancels the stragglers, then drains a SECOND time precisely to catch this. The
    # wake emission is merged in-loop; the final emission exists only because the post-cancel drain ran.
    sid = SessionId('e2e-post-cancel-drain')
    probe = TelemetryProbe()
    runtime = replace(build_in_memory_runtime(fake_clock), telemetry=probe.telemetry)
    straggler = _cancel_emitting_straggler(
        'straggler', wake_emit=ip('203.0.113.1'), final_emit=ip('203.0.113.2'), clock=fake_clock, deadline=5.0
    )
    report = make_aggregator('report', depends_on={IpDataPoint}, on_aggregate=_write_ip_count)

    result = await Orchestrator(
        session_id=sid,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[straggler, report],
        session_deadline=5.0,
    ).run()

    assert result.status is SessionStatus.COMPLETED  # the deadline runs aggregation, not a wedge
    stored = {dp.value for dp in (await runtime.store.snapshot(sid)).of_type(IpDataPoint)}
    assert stored == {'203.0.113.1', '203.0.113.2'}  # BOTH the wake and the cancel-time emission persisted
    assert probe.counter_total('session_deadline_hits_total') == 1  # the deadline branch fired exactly once
    document = await runtime.durable.read('reports', 'report')
    assert document is not None and document.document == {'ip_count': 2}  # aggregation saw the post-drain state
    assert not await runtime.lock.is_held(sid)  # the epoch was released on the deadline path
