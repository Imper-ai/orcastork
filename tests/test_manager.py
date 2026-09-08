"""MGR — SessionOrchestrationManager: spawn, orphan-resume, fencing, gate, catalog, flow drift."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from dataclasses import replace
from typing import Any

import pytest

from orcastork.adapters.memory import (
    InMemoryCapabilityCatalog,
    InMemoryCooldownGate,
    InMemoryDataPointStore,
    InMemorySessionLock,
)
from orcastork.audit import AuditKind, OperatorOutcome
from orcastork.exceptions import LockHeldError, SchedulingGateBlockedError, StaleEpochError
from orcastork.flow import FlowDefinition
from orcastork.ids import CapabilityId, Epoch, NamespaceId, OperatorId, SessionId
from orcastork.manager import SchedulingGate, SessionOrchestrationManager
from orcastork.manager import manager as manager_module
from orcastork.operators import Operator, OperatorContext
from orcastork.orchestrator import Orchestrator
from orcastork.orchestrator.orchestrator import SessionStatus
from orcastork.runtime import OrchestratorRuntime, build_in_memory_runtime

from .doubles.capabilities import make_capability
from .doubles.clock import FakeClock
from .doubles.conformance import CooldownGateConformance
from .doubles.datapoints import (
    T0,
    ChatAnswerDataPoint,
    EmailDataPoint,
    IpDataPoint,
    RiskDataPoint,
    TriggerDataPoint,
    chat_answer,
    ip,
    risk,
    work_email,
)
from .doubles.logs import capture_logs
from .doubles.operators import make_aggregator, make_operator

SID = SessionId('mgr-session')
NAMESPACE = NamespaceId('mgr-namespace')
PAST_TTL = 31.0  # lock TTL default is 30s
_STUB_OP = OperatorId('op')


def _trigger() -> TriggerDataPoint:
    """A minimal ephemeral DataPoint for tests that need a late-ephemeral deliver."""
    return TriggerDataPoint(value='late-signal', retrieved_by=_STUB_OP, first_retrieved=T0, last_retrieved=T0)


def _flow(*operators: type[Operator], **kwargs: Any) -> FlowDefinition:
    return FlowDefinition(name='mgr-flow', operators=tuple(operators), **kwargs)


def _scorer() -> type[Operator]:
    return make_operator('scorer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])


async def _simulate_crashed_predecessor(runtime: OrchestratorRuntime, clock: FakeClock) -> Epoch:
    """A predecessor held epoch 1 and wrote the seed, then died; its lock then expires."""
    epoch = await runtime.lock.acquire(SID)
    await runtime.store.write(SID, [work_email()], epoch=epoch)
    clock.advance(PAST_TTL)
    return epoch


async def test_mgr_01_start_session_spawns_with_epoch_1(fake_clock: FakeClock) -> None:
    manager = SessionOrchestrationManager(build_in_memory_runtime(fake_clock))
    result = await manager.start_session(
        session_id=SID, namespace_id=NAMESPACE, flow=_flow(_scorer()), seed=[work_email()]
    )
    assert result.status is SessionStatus.COMPLETED
    assert result.epoch == 1


async def test_mgr_02_orphan_resumed_with_higher_epoch(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    await _simulate_crashed_predecessor(runtime, fake_clock)
    assert await manager.is_orphaned(SID)

    result = await manager.resume(session_id=SID, namespace_id=NAMESPACE, flow=_flow(_scorer()))
    assert result is not None and result.epoch == 2 and result.status is SessionStatus.COMPLETED


async def test_mgr_02b_a_second_resume_does_not_enter_the_spawn_while_one_is_in_flight(
    fake_clock: FakeClock,
) -> None:
    """A racing resume must not build an orchestrator only to discover it lost.

    The distributed lock decides ownership, but it is acquired INSIDE the spawn. Without the
    per-session takeover guard each racer first constructed an orchestrator and activated the whole
    capability set, learning only afterwards that it had lost — so one takeover's Mongo/Redis/HTTP
    clients and secret loading were paid once per racer. On dev2 four racers in 61ms OOM-killed a 1Gi
    pod. Blocking inside the spawn is what makes the overlap deterministic; without it the in-memory
    runtime finishes the first resume before the second is scheduled and no race exists to observe.
    """
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    await _simulate_crashed_predecessor(runtime, fake_clock)
    flow = _flow(_scorer())

    entered = 0
    release = asyncio.Event()

    async def blocking_spawn(*args: Any, **kwargs: Any) -> Any:  # noqa: ARG001
        nonlocal entered
        entered += 1
        await release.wait()
        return None

    manager._spawn_and_drain = blocking_spawn  # type: ignore[method-assign]

    first = asyncio.create_task(manager.resume(session_id=SID, namespace_id=NAMESPACE, flow=flow))
    await asyncio.sleep(0)  # let the first reach the spawn and block there
    second = asyncio.create_task(manager.resume(session_id=SID, namespace_id=NAMESPACE, flow=flow))
    await asyncio.sleep(0)

    # The property that prevents the OOM: no two resumes construct at the same time. Once the winner
    # finishes, a queued caller proceeding is correct — in production it then sees the session complete
    # and stands down, which the stub spawn here cannot reproduce.
    assert entered == 1, f'the second resume also entered the spawn ({entered} entries)'

    release.set()
    await asyncio.gather(first, second)


async def test_mgr_42_a_queued_resume_keeps_the_takeover_entry_alive_for_the_next_caller(
    fake_clock: FakeClock,
) -> None:
    """A caller queued behind the winner must still be serializing the next caller that arrives.

    An ``asyncio.Lock`` is unlocked between ``release()`` and the woken waiter re-acquiring, so evicting
    the per-session entry on "not locked" drops it while a caller is still queued. The next resume then
    finds nothing mapped, mints a second lock, and enters the spawn alongside the queued one — the
    concurrent full-orchestrator build (clients + secret loading, seconds long) this guard exists to
    collapse. A name+email burst to a parked session overlapping the redelivery recheck reaches three
    concurrent same-session resumes on routine traffic.
    """
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    await _simulate_crashed_predecessor(runtime, fake_clock)
    flow = _flow(_scorer())

    entered = 0
    in_spawn = 0
    peak_in_spawn = 0
    winner_gate = asyncio.Event()
    open_gate = asyncio.Event()

    async def blocking_spawn(*args: Any, **kwargs: Any) -> Any:  # noqa: ARG001
        nonlocal entered, in_spawn, peak_in_spawn
        entered += 1
        gate = winner_gate if entered == 1 else open_gate
        in_spawn += 1
        peak_in_spawn = max(peak_in_spawn, in_spawn)
        await gate.wait()
        in_spawn -= 1
        return None

    manager._spawn_and_drain = blocking_spawn  # type: ignore[method-assign]

    winner = asyncio.create_task(manager.resume(session_id=SID, namespace_id=NAMESPACE, flow=flow))
    await asyncio.sleep(0)  # the winner reaches the spawn and blocks there
    queued = asyncio.create_task(manager.resume(session_id=SID, namespace_id=NAMESPACE, flow=flow))
    await asyncio.sleep(0)  # the second resume is now queued on the guard
    winner_gate.set()
    await asyncio.sleep(0)  # the winner returns and releases; the queued caller is woken, not yet running
    await asyncio.sleep(0)  # the queued caller re-acquires and enters the spawn
    late = asyncio.create_task(manager.resume(session_id=SID, namespace_id=NAMESPACE, flow=flow))
    await asyncio.sleep(0)

    assert peak_in_spawn == 1, f'two resumes built an orchestrator at once ({peak_in_spawn} in the spawn)'

    open_gate.set()
    await asyncio.gather(winner, queued, late)
    # Still evicted once nobody holds or waits, so a long-lived process keeps no lock per session touched.
    assert manager._takeover_locks == {}  # noqa: SLF001


async def test_mgr_03_resume_rehydrates_and_redrives(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    await _simulate_crashed_predecessor(runtime, fake_clock)

    async def write(ctx: OperatorContext) -> None:
        assert ctx.aggregation is not None
        await ctx.aggregation.upsert('reports', 'report', {'risk_count': len(ctx.store.of_type(RiskDataPoint))})

    reporter = make_aggregator('rep', depends_on={RiskDataPoint}, on_aggregate=write)
    await manager.resume(session_id=SID, namespace_id=NAMESPACE, flow=_flow(_scorer(), reporter))
    document = await runtime.durable.read('reports', 'report')
    assert document is not None and document.document == {'risk_count': 1}  # re-driven from persisted seed


async def test_mgr_04_stale_predecessor_writes_rejected_after_resume(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    stale_epoch = await _simulate_crashed_predecessor(runtime, fake_clock)

    result = await manager.resume(session_id=SID, namespace_id=NAMESPACE, flow=_flow(_scorer()))
    assert result is not None and result.epoch == 2
    with pytest.raises(StaleEpochError):  # the fenced predecessor cannot write
        await runtime.store.write(SID, [ip()], epoch=stale_epoch)


async def test_mgr_05_capabilities_redriven_fresh_on_resume(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(
        fake_clock, catalog=InMemoryCapabilityCatalog(permitted={NAMESPACE: {CapabilityId('netcap')}})
    )
    netcap = make_capability('netcap', depends_on={IpDataPoint})
    seeder = make_operator('seeder', produces={IpDataPoint}, emits=[ip()])
    consumer = make_operator('consumer', requires={netcap}, produces={RiskDataPoint}, emits=[risk()])
    operators = [seeder, consumer]

    await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=operators, capabilities=[netcap]
    ).run()
    after_first = len(netcap.activations)  # type: ignore[attr-defined]
    # A fresh orchestrator on the same session re-activates the capability from credentials.
    await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=operators, capabilities=[netcap]
    ).run()
    assert len(netcap.activations) == after_first + 1  # type: ignore[attr-defined]


async def test_mgr_06_completed_sessions_are_not_resumed(fake_clock: FakeClock) -> None:
    manager = SessionOrchestrationManager(build_in_memory_runtime(fake_clock))
    flow = _flow(_scorer())
    await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])
    assert await manager.resume(session_id=SID, namespace_id=NAMESPACE, flow=flow) is None


async def test_mgr_07_epoch_strictly_increases_across_grants(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    first_epoch = await _simulate_crashed_predecessor(runtime, fake_clock)  # grant 1
    result = await manager.resume(session_id=SID, namespace_id=NAMESPACE, flow=_flow(_scorer()))  # grant 2
    assert result is not None and first_epoch == 1 and result.epoch == 2


async def test_mgr_08_scheduling_gate_enforces_cooldown(fake_clock: FakeClock) -> None:
    gate = SchedulingGate(InMemoryCooldownGate(fake_clock), cooldown_seconds=100.0)
    assert await gate.try_start('device-1')  # the winning start arms the cooldown atomically
    assert not await gate.try_start('device-1')  # within cooldown
    fake_clock.advance(100.0)
    assert await gate.try_start('device-1')  # cooldown elapsed


async def test_mgr_09_catalog_revocation_visible_to_next_grant(fake_clock: FakeClock) -> None:
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {CapabilityId('netcap')}})
    runtime = build_in_memory_runtime(fake_clock, catalog=catalog)
    manager = SessionOrchestrationManager(runtime)
    netcap = make_capability('netcap', depends_on={IpDataPoint})
    seeder = make_operator('seeder', produces={IpDataPoint}, emits=[ip()])
    consumer = make_operator('consumer', requires={netcap}, produces={RiskDataPoint}, emits=[risk()])
    flow = _flow(seeder, consumer, capabilities=(netcap,))

    first = await manager.start_session(session_id=SessionId('s1'), namespace_id=NAMESPACE, flow=flow)
    assert first.operator_runs.get(OperatorId('consumer')) == 1  # netcap permitted → consumer ran

    catalog.set_permitted(NAMESPACE, set())  # revoke
    second = await manager.start_session(session_id=SessionId('s2'), namespace_id=NAMESPACE, flow=flow)
    assert OperatorId('consumer') not in second.operator_runs  # revocation visible to the new grant


async def test_mgr_10_concurrent_grants_have_exactly_one_winner(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    outcomes = await asyncio.gather(runtime.lock.acquire(SID), runtime.lock.acquire(SID), return_exceptions=True)
    epochs = [outcome for outcome in outcomes if isinstance(outcome, int)]
    conflicts = [outcome for outcome in outcomes if isinstance(outcome, LockHeldError)]
    assert len(epochs) == 1 and len(conflicts) == 1  # atomic mint → exactly one successor wins


async def test_mgr_11_completion_is_visible_to_a_second_manager(fake_clock: FakeClock) -> None:
    """Durable completion: a peer supervisor over the same backends sees the session finished."""
    runtime = build_in_memory_runtime(fake_clock)
    pod_a = SessionOrchestrationManager(runtime)
    pod_b = SessionOrchestrationManager(runtime)  # a separate supervisor sharing the same infra
    flow = _flow(_scorer())
    await pod_a.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])
    # The ownership lock is free again, yet pod_b must not treat the finished session as orphaned.
    assert await pod_b.is_orphaned(SID) is False
    assert await pod_b.resume(session_id=SID, namespace_id=NAMESPACE, flow=flow) is None


async def test_mgr_12_start_session_enforced_by_scheduling_gate(fake_clock: FakeClock) -> None:
    gate = SchedulingGate(InMemoryCooldownGate(fake_clock), cooldown_seconds=100.0)
    manager = SessionOrchestrationManager(build_in_memory_runtime(fake_clock), scheduling_gate=gate)
    flow = _flow(_scorer())
    first = await manager.start_session(
        session_id=SessionId('s1'), namespace_id=NAMESPACE, flow=flow, seed=[work_email()]
    )
    assert first.status is SessionStatus.COMPLETED
    # A second start for the same gate key (defaulting to the namespace) is refused while cooling down.
    with pytest.raises(SchedulingGateBlockedError):
        await manager.start_session(session_id=SessionId('s2'), namespace_id=NAMESPACE, flow=flow, seed=[work_email()])
    fake_clock.advance(100.0)  # cooldown elapses
    third = await manager.start_session(
        session_id=SessionId('s3'), namespace_id=NAMESPACE, flow=flow, seed=[work_email()]
    )
    assert third.status is SessionStatus.COMPLETED


async def test_mgr_13_distinct_gate_keys_have_independent_cooldowns(fake_clock: FakeClock) -> None:
    gate = SchedulingGate(InMemoryCooldownGate(fake_clock), cooldown_seconds=100.0)
    manager = SessionOrchestrationManager(build_in_memory_runtime(fake_clock), scheduling_gate=gate)
    flow = _flow(_scorer())
    await manager.start_session(
        session_id=SessionId('d1'), namespace_id=NAMESPACE, flow=flow, seed=[work_email()], gate_key='device-1'
    )
    # 'device-1' is cooling down, but 'device-2' is an independent key and may start immediately.
    second = await manager.start_session(
        session_id=SessionId('d2'), namespace_id=NAMESPACE, flow=flow, seed=[work_email()], gate_key='device-2'
    )
    assert second.status is SessionStatus.COMPLETED
    with pytest.raises(SchedulingGateBlockedError):
        await manager.start_session(
            session_id=SessionId('d3'), namespace_id=NAMESPACE, flow=flow, seed=[work_email()], gate_key='device-1'
        )


async def test_mgr_14_deliver_to_held_session_appends_then_rechecks_after_the_lease(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    answer_handler = make_operator(
        'answer_handler', depends_on={ChatAnswerDataPoint}, produces={RiskDataPoint}, emits=[risk(0.9)]
    )
    manager = SessionOrchestrationManager(runtime)
    await runtime.lock.acquire(SID)  # a live orchestrator owns the session — and then silently dies

    result = await manager.deliver(
        session_id=SID,
        namespace_id=NAMESPACE,
        data_point=chat_answer('answer'),
        flow=_flow(answer_handler, completes_when=RiskDataPoint),
    )

    assert result is None  # left to the (presumed live) owner
    assert await runtime.inbox.pending_count(SID) == 1  # but the entry is already durable
    assert len(manager._recheck_tasks) == 1  # noqa: SLF001
    await asyncio.gather(*manager._recheck_tasks)  # noqa: SLF001  # the recheck fires after the lease TTL
    assert await runtime.lock.is_complete(SID)  # the dead holder's session was resumed and driven to completion
    assert await runtime.inbox.pending_count(SID) == 0  # the delivery-window message was not lost


async def test_mgr_15_deliver_ephemeral_to_completed_session_appends_but_never_redrives(
    fake_clock: FakeClock,
) -> None:
    # A late deliver to a completed session with no re-open requested is not re-driven: no re-open, no
    # recheck. (This is the default path; ephemerality is irrelevant — reopen_if_complete gates a re-open.)
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    flow = _flow(_scorer())
    await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])
    trigger = _trigger()

    result = await manager.deliver(session_id=SID, namespace_id=NAMESPACE, data_point=trigger, flow=flow)

    assert result is None  # no re-open requested -> the late entry is left to expire
    assert not manager._recheck_tasks  # noqa: SLF001  # nothing is scheduled to re-drive a finished session
    assert await runtime.lock.is_complete(SID)  # still complete — no re-open


async def test_mgr_30_resume_and_deliver_bypass_the_start_cooldown(fake_clock: FakeClock) -> None:
    # The SchedulingGate decides when a NEW session may start; resume/deliver are recovery paths for
    # EXISTING sessions and must be exempt. A start arms the namespace cooldown, but an orphaned session for
    # that same namespace must still resume, and a parked one must still accept delivery, well within the
    # cooldown window. A regression that gated resume/deliver would deadlock crash recovery behind the
    # cooldown — so both must succeed despite the gate being armed and still cooling down.
    gate = SchedulingGate(InMemoryCooldownGate(fake_clock), cooldown_seconds=100.0)
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime, scheduling_gate=gate)
    scorer_flow = _flow(_scorer())  # one registered scorer reused across the start and the resume

    # A start for NAMESPACE arms the cooldown for the default gate key (the namespace); a second start is now fenced.
    await manager.start_session(session_id=SessionId('starter'), namespace_id=NAMESPACE, flow=scorer_flow)
    with pytest.raises(SchedulingGateBlockedError):
        await manager.start_session(session_id=SessionId('blocked'), namespace_id=NAMESPACE, flow=scorer_flow)

    # An orphaned session under the SAME namespace: a predecessor wrote the seed then died; its lock lapses
    # (31s) but the 100s cooldown is still active.
    orphan = SessionId('orphan-under-cooldown')
    orphan_epoch = await runtime.lock.acquire(orphan)
    await runtime.store.write(orphan, [work_email()], epoch=orphan_epoch)
    fake_clock.advance(PAST_TTL)  # lock TTL expires; still 31s ≪ 100s cooldown

    resumed = await manager.resume(session_id=orphan, namespace_id=NAMESPACE, flow=scorer_flow)
    assert resumed is not None  # recovery is not fenced by the start cooldown
    assert resumed.status is SessionStatus.COMPLETED

    # A parked session under the SAME namespace accepts a late delivery and resumes despite the live cooldown.
    parked = SessionId('parked-under-cooldown')
    answer_handler = make_operator(
        'answer_handler', depends_on={ChatAnswerDataPoint}, produces={RiskDataPoint}, emits=[risk(0.9)]
    )
    parking_flow = _flow(answer_handler, completes_when=RiskDataPoint, park_after=20.0)
    # Start under an independent gate key so the start itself isn't fenced; the deliver below still uses
    # NAMESPACE and must bypass the (still-armed) namespace cooldown regardless.
    parked_result = await manager.start_session(
        session_id=parked, namespace_id=NAMESPACE, flow=parking_flow, seed=[work_email()], gate_key='parked-device'
    )
    assert parked_result.status is SessionStatus.PARKED  # holds no lock, not complete — the deliver/orphan branch
    assert await manager.is_orphaned(parked)

    delivered = await manager.deliver(
        session_id=parked, namespace_id=NAMESPACE, data_point=chat_answer('it was me'), flow=parking_flow
    )
    assert delivered is not None  # deliver-driven resume is not fenced by the start cooldown either
    assert delivered.status is SessionStatus.COMPLETED


async def test_mgr_29_deliver_ephemeral_to_completed_session_appends_before_discarding(
    fake_clock: FakeClock,
) -> None:
    # deliver is crash-safe ingress: the durable inbox append always happens FIRST, *then* the
    # completed-session-without-reopen branch returns None. A late delivery (no re-open requested) to a
    # finished session leaves its entry durably appended and never processed (it expires with the session
    # state). A regression that moved the is_complete check ahead of the append would silently drop this
    # ingress write; pinning the pending_count proves the append-first ordering.
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    flow = _flow(_scorer())
    await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])
    assert await runtime.lock.is_complete(SID)  # the session finished
    assert await runtime.inbox.pending_count(SID) == 0  # nothing in the inbox before the late delivery
    trigger = _trigger()

    result = await manager.deliver(session_id=SID, namespace_id=NAMESPACE, data_point=trigger, flow=flow)

    assert result is None  # no re-open requested -> the entry is never processed
    assert await runtime.inbox.pending_count(SID) == 1  # but the append already landed durably, append-first


async def test_mgr_16_resume_losing_the_acquire_race_stands_down_without_raising(fake_clock: FakeClock) -> None:
    class _StaleIsHeldLock(InMemorySessionLock):
        """Models the check/acquire window: is_held reports free while the lease is in fact live."""

        async def is_held(self, session_id: SessionId) -> bool:  # noqa: ARG002
            return False

    lock = _StaleIsHeldLock(fake_clock)
    runtime = replace(build_in_memory_runtime(fake_clock), lock=lock)
    await lock.acquire(SID)  # a concurrent resumer actually holds the lease

    result = await SessionOrchestrationManager(runtime).resume(
        session_id=SID, namespace_id=NAMESPACE, flow=_flow(_scorer())
    )

    assert result is None  # lost the race → stand down; the winner's higher epoch fences us


async def test_mgr_38_a_never_started_session_is_not_orphaned(fake_clock: FakeClock) -> None:
    # Orphaned means "was started and its owner died". A session with no minted epoch was never started,
    # so there is no owner to have died and nothing persisted to rehydrate — reporting it orphaned invites
    # a resume that would spawn it from an empty store.
    manager = SessionOrchestrationManager(build_in_memory_runtime(fake_clock))
    assert not await manager.is_orphaned(SID)


async def test_mgr_39_resume_stands_down_on_a_never_started_session(fake_clock: FakeClock) -> None:
    # Resume rehydrates from the persisted store; with no epoch ever minted there is nothing to rehydrate,
    # so spawning here would create a session whose only state is whatever the inbox happens to hold.
    runtime = build_in_memory_runtime(fake_clock)

    result = await SessionOrchestrationManager(runtime).resume(
        session_id=SID, namespace_id=NAMESPACE, flow=_flow(_scorer())
    )

    assert result is None
    assert await runtime.lock.current_epoch(SID) == 0  # stood down without minting


async def test_mgr_40_deliver_before_the_first_start_preserves_the_entry_without_spawning(
    fake_clock: FakeClock,
) -> None:
    # A deliver that arrives before the session's first start must not become the session's creator: the
    # durable append still happens (delivery is never lost), but the spawn is left to start_session, which
    # is the only caller that carries the seed.
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)

    result = await manager.deliver(
        session_id=SID, namespace_id=NAMESPACE, data_point=_trigger(), flow=_flow(_scorer())
    )

    assert result is None
    assert await runtime.inbox.pending_count(SID) == 1  # held for the real start to drain
    assert await runtime.lock.current_epoch(SID) == 0


async def test_mgr_41_a_deliver_racing_ahead_of_start_session_does_not_strand_the_seed(
    fake_clock: FakeClock,
) -> None:
    # The collection websocket spawns start_session as a task and begins receiving frames immediately, so a
    # client frame's deliver can reach its free-lock check before start_session acquires. A deliver that
    # spawned the session there would own the lock with an empty seed, start_session would lose the acquire
    # race, and the seed the flow depends on would never be applied — the session then runs to its deadline
    # collecting nothing it needs and finalizes no result.
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)

    async def write(ctx: OperatorContext) -> None:
        assert ctx.aggregation is not None
        await ctx.aggregation.upsert('reports', 'report', {'risk_count': len(ctx.store.of_type(RiskDataPoint))})

    flow = _flow(_scorer(), make_aggregator('rep', depends_on={RiskDataPoint}, on_aggregate=write))

    await manager.deliver(session_id=SID, namespace_id=NAMESPACE, data_point=_trigger(), flow=flow)
    result = await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])

    assert result.epoch == 1  # start_session owns the session, not the deliver that arrived first
    assert result.status is SessionStatus.COMPLETED
    document = await runtime.durable.read('reports', 'report')
    assert document is not None and document.document == {'risk_count': 1}  # the seed was applied


async def test_mgr_17_deliver_resumes_a_parked_session_and_folds_the_late_answer_in(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    answer_handler = make_operator(
        'answer_handler', depends_on={ChatAnswerDataPoint}, produces={RiskDataPoint}, emits=[risk(0.9)]
    )
    flow = _flow(answer_handler, completes_when=RiskDataPoint, park_after=20.0)

    parked = await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])
    assert parked.status is SessionStatus.PARKED  # park_after threads through to the orchestrator
    assert await manager.is_orphaned(SID)  # no lock + not complete — exactly what deliver resumes
    assert not manager._recheck_tasks  # noqa: SLF001  # nothing else is scheduled to re-drive it

    fake_clock.advance(100.0)  # the user answers much later, well within the persisted session budget
    result = await manager.deliver(
        session_id=SID, namespace_id=NAMESPACE, data_point=chat_answer('it was me'), flow=flow
    )

    assert result is not None and result.status is SessionStatus.COMPLETED
    assert result.epoch == 2  # a fresh, higher-epoch orchestrator resumed the parked session
    assert result.operator_runs.get(OperatorId('answer_handler')) == 1  # the late answer was folded in
    assert await runtime.lock.is_complete(SID)  # finalized this time — no further resume will re-drive it
    assert await runtime.inbox.pending_count(SID) == 0  # applied and acked — no message lost


async def test_mgr_18_operator_gating_change_between_grants_is_visible_on_resume(fake_clock: FakeClock) -> None:
    catalog = InMemoryCapabilityCatalog(permitted_operators={NAMESPACE: set()})  # everything gated at first
    runtime = build_in_memory_runtime(fake_clock, catalog=catalog)
    manager = SessionOrchestrationManager(runtime)
    answer_handler = make_operator(
        'answer_handler', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()]
    )
    flow = _flow(answer_handler, completes_when=RiskDataPoint, park_after=20.0)

    parked = await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])
    assert parked.status is SessionStatus.PARKED  # gated → nothing could produce the risk; the session idled
    assert parked.operator_runs == {}

    catalog.set_permitted_operators(
        NAMESPACE, {OperatorId('answer_handler')}
    )  # namespace config change between grants
    result = await manager.resume(session_id=SID, namespace_id=NAMESPACE, flow=flow)

    assert result is not None and result.status is SessionStatus.COMPLETED
    assert result.operator_runs.get(OperatorId('answer_handler')) == 1  # the resumed grant sees the new gating
    assert await runtime.lock.is_complete(SID)


async def test_mgr_19_same_flow_resume_emits_no_drift_audit(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    answer_handler = make_operator(
        'answer_handler', depends_on={ChatAnswerDataPoint}, produces={RiskDataPoint}, emits=[risk()]
    )
    flow = _flow(answer_handler, completes_when=RiskDataPoint, park_after=20.0)

    parked = await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])
    assert parked.status is SessionStatus.PARKED
    resumed = await manager.resume(session_id=SID, namespace_id=NAMESPACE, flow=flow)

    assert resumed is not None and resumed.status is SessionStatus.PARKED  # same flow → re-parked quietly
    assert not [e for e in await runtime.audit.replay(SID) if e.kind is AuditKind.FLOW_DRIFT_DETECTED]
    assert await runtime.store.get_flow_fingerprint(SID) == flow.fingerprint()  # persisted at the first spawn


async def test_mgr_20_changed_flow_resume_audits_drift_once_then_stays_quiet(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    answer_handler = make_operator(
        'answer_handler', depends_on={ChatAnswerDataPoint}, produces={RiskDataPoint}, emits=[risk()]
    )
    late_addition = make_operator('late_addition', depends_on={IpDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    original = _flow(answer_handler, completes_when=RiskDataPoint, park_after=20.0)
    changed = _flow(answer_handler, late_addition, completes_when=RiskDataPoint, park_after=20.0)

    parked = await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=original, seed=[work_email()])
    assert parked.status is SessionStatus.PARKED

    first_resume = await manager.resume(session_id=SID, namespace_id=NAMESPACE, flow=changed)
    assert first_resume is not None and first_resume.status is SessionStatus.PARKED  # drift never blocks the session
    (drift,) = [e for e in await runtime.audit.replay(SID) if e.kind is AuditKind.FLOW_DRIFT_DETECTED]
    assert drift.flow is not None
    assert drift.flow.flow_name == 'mgr-flow'
    assert drift.flow.stored_fingerprint == original.fingerprint()
    assert drift.flow.current_fingerprint == changed.fingerprint()
    assert await runtime.store.get_flow_fingerprint(SID) == changed.fingerprint()  # rebaselined immediately

    second_resume = await manager.resume(session_id=SID, namespace_id=NAMESPACE, flow=changed)
    assert second_resume is not None and second_resume.status is SessionStatus.PARKED
    drift_entries = [e for e in await runtime.audit.replay(SID) if e.kind is AuditKind.FLOW_DRIFT_DETECTED]
    assert len(drift_entries) == 1  # the same changed flow resumes quietly after the rebaseline

    completed = await manager.deliver(
        session_id=SID, namespace_id=NAMESPACE, data_point=chat_answer('finally'), flow=changed
    )
    assert completed is not None and completed.status is SessionStatus.COMPLETED  # the drifted flow still completes


async def test_mgr_21_flow_session_deadline_overrides_the_orchestrator_default(fake_clock: FakeClock) -> None:
    manager = SessionOrchestrationManager(build_in_memory_runtime(fake_clock))
    flow = _flow(completes_when=ChatAnswerDataPoint, session_deadline=30.0)  # the answer never arrives

    result = await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])

    assert result.status is SessionStatus.COMPLETED
    assert 30.0 <= fake_clock.monotonic() < 300.0  # bounded by the flow-level deadline, not the 300s default


async def test_mgr_22_flow_operation_timeout_overrides_the_orchestrator_default(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    slow = make_operator('slow', produces={IpDataPoint}, emits=[ip('slow-ip')], sleep_after=5.0)
    fast = make_operator('fast', produces={RiskDataPoint}, emits=[risk()])

    result = await manager.start_session(
        session_id=SID, namespace_id=NAMESPACE, flow=_flow(slow, fast, operation_timeout=0.02)
    )

    assert result.status is SessionStatus.COMPLETED
    runs = {
        entry.operator_id: entry.operator for entry in await runtime.audit.replay(SID) if entry.operator is not None
    }
    assert runs[OperatorId('slow')].outcome is OperatorOutcome.FAILED  # cut by the flow-level bound, not 30s
    assert runs[OperatorId('fast')].outcome is OperatorOutcome.SUCCEEDED


async def test_mgr_24_flow_emission_queue_size_reaches_the_spawned_orchestrator(
    fake_clock: FakeClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    spawned: list[Orchestrator] = []

    class _CapturingOrchestrator(Orchestrator):
        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            spawned.append(self)

    monkeypatch.setattr(manager_module, 'Orchestrator', _CapturingOrchestrator)
    burst = make_operator(
        'burst',
        depends_on={EmailDataPoint},
        produces={IpDataPoint},
        emits=[ip(f'203.0.113.{n}') for n in range(1, 6)],
    )

    result = await manager.start_session(
        session_id=SID, namespace_id=NAMESPACE, flow=_flow(burst, emission_queue_size=1), seed=[work_email()]
    )

    assert result.status is SessionStatus.COMPLETED
    (orchestrator,) = spawned
    assert orchestrator._build_emission_queue().maxsize == 1  # the flow-level bound reached the spawn
    stored = {dp.value for dp in (await runtime.store.snapshot(SID)).of_type(IpDataPoint)}
    assert stored == {f'203.0.113.{n}' for n in range(1, 6)}  # the tight bound backpressured, never dropped


async def test_mgr_23_flow_max_inbox_deliveries_overrides_the_orchestrator_default(fake_clock: FakeClock) -> None:
    class _RejectingStore(InMemoryDataPointStore):
        """Rejects applies containing the marked value (a persistently unappliable inbox entry)."""

        async def apply_resolved(
            self, session_id: SessionId, *, added: Iterable[Any], updated: Iterable[Any], epoch: Epoch
        ) -> Any:
            if any(dp.value == 'merge-bomb' for dp in (*added, *updated)):
                raise ValueError('store rejected the write')
            return await super().apply_resolved(session_id, added=tuple(added), updated=tuple(updated), epoch=epoch)

    runtime = replace(build_in_memory_runtime(fake_clock), store=_RejectingStore())
    manager = SessionOrchestrationManager(runtime)
    await runtime.inbox.append(SID, chat_answer('merge-bomb'))
    flow = _flow(completes_when=ChatAnswerDataPoint, session_deadline=30.0, max_inbox_deliveries=2)

    result = await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])

    assert result.status is SessionStatus.COMPLETED
    (record,) = await runtime.inbox.quarantined(SID)
    assert record.delivery_count == 2  # quarantined at the flow-level cap, not the default 5


async def test_mgr_25_gate_blocked_start_logs_a_warning(fake_clock: FakeClock) -> None:
    gate = SchedulingGate(InMemoryCooldownGate(fake_clock), cooldown_seconds=100.0)
    manager = SessionOrchestrationManager(build_in_memory_runtime(fake_clock), scheduling_gate=gate)
    flow = _flow(_scorer())
    await manager.start_session(session_id=SessionId('s1'), namespace_id=NAMESPACE, flow=flow, seed=[work_email()])

    with capture_logs() as records, pytest.raises(SchedulingGateBlockedError):
        await manager.start_session(session_id=SessionId('s2'), namespace_id=NAMESPACE, flow=flow, seed=[work_email()])

    (blocked,) = [r for r in records if r['message'] == 'Session start blocked by the scheduling gate cool-down']
    assert blocked['extra']['session_id'] == SessionId('s2')
    assert blocked['extra']['gate_key'] == str(NAMESPACE)


async def test_mgr_26_deliver_dispositions_are_logged(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    flow = _flow(_scorer())
    await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])
    trigger = _trigger()

    with capture_logs(level='DEBUG') as records:
        await manager.deliver(session_id=SID, namespace_id=NAMESPACE, data_point=trigger, flow=flow)

    messages = [r['message'] for r in records]
    assert 'DataPoint delivered to the session inbox' in messages
    # A late deliver to a finished session with no re-open requested is left to expire — that
    # disposition must be visible in logs.
    (ignored,) = [
        r for r in records if r['message'].startswith('Delivery to a completed session without a re-open request')
    ]
    assert ignored['level'].name == 'INFO' and ignored['extra']['data_point_type'] == 'trigger'


async def test_mgr_27_resuming_an_orphaned_session_logs_at_info(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    await _simulate_crashed_predecessor(runtime, fake_clock)
    manager = SessionOrchestrationManager(runtime)

    with capture_logs(level='INFO') as records:
        result = await manager.resume(session_id=SID, namespace_id=NAMESPACE, flow=_flow(_scorer()))

    assert result is not None and result.status is SessionStatus.COMPLETED
    (resumed,) = [
        r for r in records if r['message'] == 'Resuming orphaned session with a fresh higher-epoch orchestrator'
    ]
    assert resumed['extra'] == {'session_id': SID, 'namespace_id': NAMESPACE, 'flow_name': 'mgr-flow'}


async def test_mgr_28_recheck_failure_is_logged_and_does_not_propagate(
    fake_clock: FakeClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = SessionOrchestrationManager(build_in_memory_runtime(fake_clock))

    async def _failing_resume(**_: Any) -> None:
        raise RuntimeError('store unreachable')  # an unexpected fault inside the fire-and-forget recheck

    monkeypatch.setattr(manager, 'resume', _failing_resume)
    manager._schedule_recheck(session_id=SID, namespace_id=NAMESPACE, flow=_flow(_scorer()), redrive=manager.resume)  # noqa: SLF001

    with capture_logs(level='ERROR') as records:
        await asyncio.gather(*manager._recheck_tasks)  # noqa: SLF001  # the swallowed failure must not surface here

    (failed,) = [r for r in records if r['message'].startswith('Deferred recheck failed')]
    assert failed['exception'] is not None  # logged with the traceback, not just the ids
    assert failed['extra'] == {'session_id': SID, 'namespace_id': NAMESPACE}


async def test_mgr_31_late_non_ephemeral_deliver_reopens_and_aggregator_re_runs(fake_clock: FakeClock) -> None:
    # A non-ephemeral DataPoint (e.g. NAME/WORK_EMAIL) delivered AFTER a session completes must trigger
    # a re-open so the aggregator re-runs and the durable result reflects the late data.
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    collected: list[str] = []

    async def write(ctx: OperatorContext) -> None:
        assert ctx.aggregation is not None
        emails = [dp.value for dp in ctx.store.of_type(EmailDataPoint)]
        await ctx.aggregation.upsert('reports', 'report', {'emails': sorted(emails)})
        collected.extend(emails)

    reporter = make_aggregator('rep', depends_on={EmailDataPoint}, on_aggregate=write)
    flow = _flow(reporter, completes_when=EmailDataPoint)

    # Start and complete the session with one email.
    first = await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])
    assert first.status is SessionStatus.COMPLETED
    before_doc = await runtime.durable.read('reports', 'report')
    assert before_doc is not None and before_doc.document == {'emails': ['alice@work.example']}

    # Deliver a second email AFTER completion with the re-open intent — must re-open and re-aggregate.
    result = await manager.deliver(
        session_id=SID,
        namespace_id=NAMESPACE,
        data_point=work_email('late@work.example'),
        flow=flow,
        reopen_if_complete=True,
    )
    assert result is not None and result.status is SessionStatus.COMPLETED
    assert result.epoch == 2  # a fresh higher-epoch orchestrator ran the re-open
    after_doc = await runtime.durable.read('reports', 'report')
    assert after_doc is not None
    assert sorted(after_doc.document['emails']) == ['alice@work.example', 'late@work.example']
    assert await runtime.lock.is_complete(SID)  # session is still complete after the re-open finishes


async def test_mgr_32_late_ephemeral_deliver_without_flag_discards_without_reopen(
    fake_clock: FakeClock,
) -> None:
    # An ephemeral late deliver WITHOUT reopen_if_complete must NOT re-open — completing the flag matrix
    # alongside mgr_35 (ephemeral + flag -> re-open) and mgr_36 (non-ephemeral, no flag -> no re-open).
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    flow = _flow(_scorer(), completes_when=EmailDataPoint)
    await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])
    assert await runtime.lock.is_complete(SID)

    trigger = _trigger()
    result = await manager.deliver(session_id=SID, namespace_id=NAMESPACE, data_point=trigger, flow=flow)

    assert result is None  # ephemeral late data is discarded, no re-open
    assert await runtime.lock.is_complete(SID)  # still complete — ephemeral did not re-open


async def test_mgr_33_concurrent_reopens_exactly_one_wins_no_corruption(fake_clock: FakeClock) -> None:
    # Two concurrent re-open races: exactly one wins the acquire. The loser stands down via LockHeldError
    # (same as resume's concurrent-grant contract) — no corruption, no partial state.
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)

    async def write(ctx: OperatorContext) -> None:
        assert ctx.aggregation is not None
        await ctx.aggregation.upsert('reports', 'report', {'count': 1})

    reporter = make_aggregator('rep', depends_on={EmailDataPoint}, on_aggregate=write)
    flow = _flow(reporter, completes_when=EmailDataPoint)
    await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])

    # Race two reopens simultaneously.
    results = await asyncio.gather(
        manager.reopen(session_id=SID, namespace_id=NAMESPACE, flow=flow),
        manager.reopen(session_id=SID, namespace_id=NAMESPACE, flow=flow),
        return_exceptions=True,
    )
    # Neither should raise — one wins (returns OrchestratorResult), the other stands down (returns None).
    non_none = [r for r in results if r is not None and not isinstance(r, BaseException)]
    nones = [r for r in results if r is None]
    assert not [r for r in results if isinstance(r, BaseException)], f'unexpected exception: {results}'
    assert len(non_none) + len(nones) == 2
    assert len(non_none) == 1  # exactly one winner
    assert await runtime.lock.is_complete(SID)  # the session completed after the winner's re-aggregation


async def test_mgr_34_reopen_with_elapsed_original_deadline_still_re_aggregates(fake_clock: FakeClock) -> None:
    # The original deadline is blown long before the late participant data arrives. A fresh deadline is
    # granted on re-open so the aggregation phase has a live budget, not a negative one that would fire
    # spurious deadline-hit telemetry and skip aggregation.
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    collected: list[str] = []

    async def write(ctx: OperatorContext) -> None:
        assert ctx.aggregation is not None
        emails = [dp.value for dp in ctx.store.of_type(EmailDataPoint)]
        await ctx.aggregation.upsert('reports', 'report', {'emails': sorted(emails)})
        collected.extend(emails)

    reporter = make_aggregator('rep', depends_on={EmailDataPoint}, on_aggregate=write)
    # Use a very short deadline (1s) to simulate a budgeted flow.
    flow = _flow(reporter, completes_when=EmailDataPoint, session_deadline=1.0)

    await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])
    assert await runtime.lock.is_complete(SID)

    # Advance past the original deadline — any resume rehydrating the old deadline would get a
    # non-positive budget and the gather loop would fire deadline-hit telemetry.
    fake_clock.advance(300.0)  # well past the 1s budget

    # A late re-open with a fresh deadline must still re-aggregate successfully.
    result = await manager.deliver(
        session_id=SID,
        namespace_id=NAMESPACE,
        data_point=work_email('late@work.example'),
        flow=flow,
        reopen_if_complete=True,
    )
    assert result is not None and result.status is SessionStatus.COMPLETED
    after_doc = await runtime.durable.read('reports', 'report')
    assert after_doc is not None
    assert 'late@work.example' in after_doc.document['emails']  # the late data landed in the result


async def test_mgr_35_late_ephemeral_attribute_with_reopen_flag_reopens_and_folds(fake_clock: FakeClock) -> None:
    # REGRESSION (production): NAME/WORK_EMAIL are EPHEMERAL leaves, yet the aggregator folds them into
    # the durable result. A late deliver of an EPHEMERAL attribute WITH reopen_if_complete=True must
    # re-open and re-aggregate. Ephemerality must NOT gate the re-open — the caller's explicit intent
    # does. (A prior ephemeral-based gate silently dropped exactly these attributes.)
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)

    async def write(ctx: OperatorContext) -> None:
        assert ctx.aggregation is not None
        signals = sorted(dp.value for dp in ctx.store.of_type(TriggerDataPoint))
        await ctx.aggregation.upsert('reports', 'report', {'signals': signals})

    reporter = make_aggregator('rep', depends_on={EmailDataPoint}, on_aggregate=write)
    flow = _flow(reporter, completes_when=EmailDataPoint)

    first = await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])
    assert first.status is SessionStatus.COMPLETED
    before_doc = await runtime.durable.read('reports', 'report')
    assert before_doc is not None and before_doc.document == {'signals': []}

    trigger = _trigger()
    assert trigger.is_ephemeral  # mirrors the real ephemeral NAME/WORK_EMAIL leaves
    result = await manager.deliver(
        session_id=SID, namespace_id=NAMESPACE, data_point=trigger, flow=flow, reopen_if_complete=True
    )
    assert result is not None and result.status is SessionStatus.COMPLETED
    assert result.epoch == 2  # a fresh higher-epoch orchestrator ran the re-open
    after_doc = await runtime.durable.read('reports', 'report')
    assert after_doc is not None
    assert after_doc.document == {'signals': ['late-signal']}  # the late EPHEMERAL attribute was folded in
    assert await runtime.lock.is_complete(SID)


async def test_mgr_36_late_deliver_without_reopen_flag_never_reopens(fake_clock: FakeClock) -> None:
    # The discriminator is the caller's reopen_if_complete intent, NOT ephemerality. A late deliver of a
    # NON-ephemeral DataPoint WITHOUT the flag must append-and-expire, never re-open — proving the flag,
    # not the data point's ephemerality, drives the re-open.
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    flow = _flow(_scorer(), completes_when=EmailDataPoint)
    await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])
    assert await runtime.lock.is_complete(SID)

    # work_email() is NON-ephemeral, yet without the flag it must not re-open.
    result = await manager.deliver(
        session_id=SID, namespace_id=NAMESPACE, data_point=work_email('late@work.example'), flow=flow
    )

    assert result is None  # no reopen requested -> appended and left to expire
    assert await runtime.lock.is_complete(SID)


async def test_mgr_37_held_complete_deliver_defers_to_the_holder_no_reopen_recheck(fake_clock: FakeClock) -> None:
    # Flush-window race, new model: a late result-affecting deliver can land while the JUST-completed
    # orchestrator still holds its lease (it drains its own inbox BEFORE releasing the epoch). Because the
    # holder owns the inbox while it holds the epoch, the deliver must NOT schedule a deliver-side
    # reopen-recheck — reopen() stands down on the held lock and defers to the holder, which folds the late
    # data in-run. A recheck here would spawn a redundant re-aggregation (the latency + timeline pollution
    # this change removes). Once the lock frees, a genuinely free-lock deliver still reopens and folds.
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)

    async def write(ctx: OperatorContext) -> None:
        assert ctx.aggregation is not None
        emails = sorted(dp.value for dp in ctx.store.of_type(EmailDataPoint))
        await ctx.aggregation.upsert('reports', 'report', {'emails': emails})

    reporter = make_aggregator('rep', depends_on={EmailDataPoint}, on_aggregate=write)
    flow = _flow(reporter, completes_when=EmailDataPoint)
    await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])
    assert await runtime.lock.is_complete(SID)

    # Simulate the completing orchestrator still holding its lease during the post-completion flush window.
    await runtime.lock.acquire(SID)
    assert await runtime.lock.is_held(SID)

    # Late deliver with the re-open intent onto the held+complete session: reopen() stands down on the held
    # lock, returning None. Crucially, NO deliver-side reopen-recheck is scheduled — the holder drains it.
    result = await manager.deliver(
        session_id=SID,
        namespace_id=NAMESPACE,
        data_point=work_email('late@work.example'),
        flow=flow,
        reopen_if_complete=True,
    )
    assert result is None
    assert len(manager._recheck_tasks) == 0  # noqa: SLF001  # the holder drains it in-run; no deliver-side recheck
    assert await runtime.inbox.pending_count(SID) == 1  # appended durably, awaiting the holder's own drain

    # Once the holder releases (lease expires here), a genuinely free-lock reopen deliver still folds the
    # late data — the free-lock spawn path is intact; only the held-lock recheck hack is gone.
    fake_clock.advance(PAST_TTL)  # the holder's lease expires; the lock is now free
    assert not await runtime.lock.is_held(SID)
    folded_result = await manager.deliver(
        session_id=SID,
        namespace_id=NAMESPACE,
        data_point=work_email('late@work.example'),
        flow=flow,
        reopen_if_complete=True,
    )
    assert folded_result is not None and folded_result.status is SessionStatus.COMPLETED
    assert folded_result.epoch == 3  # a fresh higher-epoch orchestrator (epoch 1 start, epoch 2 stub-holder)
    folded = await runtime.durable.read('reports', 'report')
    assert folded is not None and sorted(folded.document['emails']) == ['alice@work.example', 'late@work.example']
    assert await runtime.inbox.pending_count(SID) == 0  # both stragglers drained by the free-lock reopen
    assert await runtime.lock.is_complete(SID)


async def test_mgr_backstop_respawns_on_straggler_landing_in_check_release_gap(fake_clock: FakeClock) -> None:
    # The completing orchestrator reads pending_count (W2) and then, in a SEPARATE step, releases the lock.
    # A cross-pod deliver that appends a straggler in the [W2, release] gap is invisible to W2, so the
    # orchestrator's own pre-release view is clean — nothing in-run can catch it. The manager holds no epoch
    # and reads the inbox AFTER the run returns (post-release), so its pending_count probe DOES observe the
    # straggler and must re-spawn to fold it rather than orphan it. This reproduces the exact NAME/WORK_EMAIL
    # late-deliver orphan: without the post-release probe the straggler is durably in the inbox but nothing
    # ever drains it.
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    # Unique session id + durable table + operator id so the test is hermetic under any ordering.
    sid = SessionId('mgr-check-release-gap-session')

    async def write(ctx: OperatorContext) -> None:
        assert ctx.aggregation is not None
        emails = sorted(dp.value for dp in ctx.store.of_type(EmailDataPoint))
        await ctx.aggregation.upsert('gap_reports', 'report', {'emails': emails})

    reporter = make_aggregator('gap_reporter', depends_on={EmailDataPoint}, on_aggregate=write)
    flow = _flow(reporter, completes_when=EmailDataPoint)

    # Inject the straggler during lock.release — the single-event-loop interleaving point of the [W2, release]
    # gap: it lands strictly after the completing run's finally-block pending_count read (W2) yet at/before the
    # actual release, so the run returns a clean COMPLETED but the inbox is durably non-empty once released.
    real_release = runtime.lock.release
    injected = {'done': False}

    async def release_with_straggler(session_id: SessionId, *, epoch: Epoch) -> None:
        if session_id == sid and not injected['done']:
            injected['done'] = True
            await runtime.inbox.append(sid, work_email('late@work.example'))
        await real_release(session_id, epoch=epoch)

    runtime.lock.release = release_with_straggler  # type: ignore[method-assign]

    result = await manager.start_session(session_id=sid, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])

    assert result is not None and result.status is SessionStatus.COMPLETED
    # The post-release probe re-spawned and folded the straggler: the durable doc carries BOTH values and the
    # inbox is fully drained. Without the fix the straggler would sit orphaned in the inbox (pending_count==1)
    # and the doc would hold only the seed email.
    doc = await runtime.durable.read('gap_reports', 'report')
    assert doc is not None and sorted(doc.document['emails']) == ['alice@work.example', 'late@work.example']
    assert await runtime.inbox.pending_count(sid) == 0  # fully drained by the backstop


class TestInMemoryCooldownGate(CooldownGateConformance):
    @pytest.fixture
    def gate(self, fake_clock: FakeClock) -> InMemoryCooldownGate:
        return InMemoryCooldownGate(fake_clock)

    @pytest.fixture
    def advance_time(self, fake_clock: FakeClock) -> Callable[[float], None]:
        return fake_clock.advance

    async def test_gate_04_cooldown_window_is_half_open_at_the_exact_expiry_tie(
        self, gate: InMemoryCooldownGate, advance_time: Callable[[float], None]
    ) -> None:
        # The window is [T, T+C): at exactly now == expires_at the cooldown has lapsed and a new start
        # is admitted (and re-arms); one tick earlier it is still closed. With a FakeClock this exact
        # equality is reachable, so the off-by-one boundary must have a defined, tested outcome.
        assert await gate.try_acquire('tie', 60.0) is True  # arms expires_at = T0 + 60
        advance_time(59.999)
        assert await gate.try_acquire('tie', 60.0) is False  # 59.999 < 60.0 — still inside the closed front
        advance_time(0.001)  # now == expires_at exactly (cumulative monotonic lands on 60.0)
        assert await gate.try_acquire('tie', 60.0) is True  # now == expires_at admits (strict `<`, not `<=`)


async def test_mgr_a_straggler_delivered_to_a_held_session_that_then_completes_is_not_lost(
    fake_clock: FakeClock,
) -> None:
    # `deliver` leaves a held session to its live owner and schedules a deferred re-drive — but that
    # re-drive is `resume`, and `resume` refuses a session that has since COMPLETED. So an entry appended
    # while the owner still held the lock is dropped the moment that owner finishes: the recheck fires,
    # sees the completion flag, and stands down. Nothing else re-drives it, the caller was already told the
    # delivery succeeded, and every stand-down on the path logs at DEBUG — which is off in production.
    #
    # Real shape: a participant's name/email delivered just as their collection run is finishing. The
    # record keeps its `final` status, so nothing looks broken; the late identity is simply never folded.
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    flow = _flow(_scorer())
    epoch = await runtime.lock.acquire(SID)  # a live owner is mid-run

    result = await manager.deliver(
        session_id=SID,
        namespace_id=NAMESPACE,
        data_point=work_email(),
        flow=flow,
        reopen_if_complete=True,
    )

    assert result is None  # left to the owner
    assert await runtime.inbox.pending_count(SID) == 1  # durably appended

    # The owner finishes: marks complete, then releases — exactly what a completing collection run does.
    await runtime.lock.mark_complete(SID, epoch=epoch)
    await runtime.lock.release(SID, epoch=epoch)

    await asyncio.gather(*manager._recheck_tasks)  # noqa: SLF001

    # The caller was told this was delivered, so it has to actually land.
    assert await runtime.inbox.pending_count(SID) == 0
