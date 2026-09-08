"""Live interim aggregation — an opt-in aggregator scheduled during gathering."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import timedelta
from itertools import count

import pytest

from orcastork.adapters.memory import InMemoryCapabilityCatalog, InMemorySessionLock
from orcastork.exceptions import CompletionTailTimeoutError, StaleEpochError
from orcastork.flow import FlowDefinition
from orcastork.ids import Epoch, NamespaceId, OperatorId, SessionId
from orcastork.manager import SessionOrchestrationManager
from orcastork.operators import OperatorContext
from orcastork.orchestrator import Orchestrator, SessionStatus
from orcastork.runtime import build_in_memory_runtime

from .doubles.clock import FakeClock
from .doubles.datapoints import (
    ChatAnswerDataPoint,
    IpDataPoint,
    RiskDataPoint,
    TriggerDataPoint,
    chat_answer,
    ip,
    risk,
)
from .doubles.operators import make_aggregator, make_operator

NAMESPACE = NamespaceId('namespace')
REPORTS = 'reports'


class _FenceOnRenewLock(InMemorySessionLock):
    """In-memory lock whose renew always reports a takeover (models being fenced mid-run)."""

    async def renew(self, session_id: SessionId, *, epoch: Epoch) -> None:  # noqa: ARG002
        raise StaleEpochError('a higher epoch took over')


async def _write_score(ctx: OperatorContext) -> None:
    count = sum(1 for _ in ctx.store.of_type(RiskDataPoint))
    assert ctx.aggregation is not None
    await ctx.aggregation.upsert(REPORTS, str(ctx.session_id), {'risk_count': count, 'is_final': ctx.is_final})


@pytest.mark.asyncio
async def test_interim_aggregator_writes_in_progress_during_gathering() -> None:
    clock = FakeClock()
    runtime = build_in_memory_runtime(clock)
    report = make_aggregator(
        'report', depends_on={RiskDataPoint}, rerun_on_new_data=True, interim_refresh=True, on_aggregate=_write_score
    )
    # park_after=0.0 makes the session park immediately once it reaches the inbox wait, so the
    # test does not spend real wall-clock time waiting for the session deadline.
    flow = FlowDefinition(name='live', operators=(report,), completes_when=ChatAnswerDataPoint, park_after=0.0)
    sid = SessionId('s1')
    manager = SessionOrchestrationManager(runtime)

    # No ChatAnswer yet → completes_when unsatisfied → the session parks, but the interim
    # aggregator ran during gathering and wrote an in_progress document.
    await manager.start_session(session_id=sid, namespace_id=NAMESPACE, flow=flow, seed=[risk(0.4)])
    doc = await runtime.durable.read(REPORTS, str(sid))
    assert doc is not None and doc.document['risk_count'] == 1
    assert doc.status == 'in_progress'
    assert doc.document['is_final'] is False  # the orchestrated gather pass binds ctx.is_final=False end-to-end
    assert await runtime.durable.is_contribution_marked(sid, OperatorId('report')) is False


@pytest.mark.asyncio
async def test_interim_then_finalize_flips_to_final_and_marks_contribution() -> None:
    clock = FakeClock()
    runtime = build_in_memory_runtime(clock)
    report = make_aggregator(
        'report', depends_on={RiskDataPoint}, rerun_on_new_data=True, interim_refresh=True, on_aggregate=_write_score
    )
    flow = FlowDefinition(name='live', operators=(report,), completes_when=ChatAnswerDataPoint, park_after=0.0)
    sid = SessionId('s2')
    manager = SessionOrchestrationManager(runtime)

    await manager.start_session(session_id=sid, namespace_id=NAMESPACE, flow=flow, seed=[risk(0.4)])
    interim = await runtime.durable.read(REPORTS, str(sid))
    assert interim is not None and interim.status == 'in_progress'

    # The completing DataPoint arrives → the session resumes, satisfies completes_when, and the
    # finalize pass re-runs the aggregator authoritatively.
    await manager.deliver(session_id=sid, namespace_id=NAMESPACE, flow=flow, data_point=chat_answer('done'))
    doc = await runtime.durable.read(REPORTS, str(sid))
    assert doc is not None and doc.status == 'final' and doc.document['is_final'] is True
    assert await runtime.durable.is_contribution_marked(sid, OperatorId('report')) is True


@pytest.mark.asyncio
async def test_interim_aggregator_reruns_as_new_data_arrives() -> None:
    clock = FakeClock()
    runtime = build_in_memory_runtime(clock)
    report = make_aggregator(
        'report', depends_on={RiskDataPoint}, rerun_on_new_data=True, interim_refresh=True, on_aggregate=_write_score
    )
    flow = FlowDefinition(name='live', operators=(report,), completes_when=ChatAnswerDataPoint, park_after=0.0)
    sid = SessionId('s3')
    manager = SessionOrchestrationManager(runtime)

    await manager.start_session(session_id=sid, namespace_id=NAMESPACE, flow=flow, seed=[risk(0.1)])
    first = await runtime.durable.read(REPORTS, str(sid))
    assert first is not None and first.document['risk_count'] == 1

    await manager.deliver(session_id=sid, namespace_id=NAMESPACE, flow=flow, data_point=risk(0.9))
    refreshed = await runtime.durable.read(REPORTS, str(sid))
    assert refreshed is not None and refreshed.document['risk_count'] == 2 and refreshed.status == 'in_progress'


async def _write_counts(ctx: OperatorContext) -> None:
    risks = sum(1 for _ in ctx.store.of_type(RiskDataPoint))
    chats = sum(1 for _ in ctx.store.of_type(ChatAnswerDataPoint))
    assert ctx.aggregation is not None
    await ctx.aggregation.upsert(REPORTS, str(ctx.session_id), {'total': risks + chats, 'is_final': ctx.is_final})


@pytest.mark.asyncio
async def test_uses_reruns_interim_without_gating_readiness() -> None:
    # `uses` is a rerun trigger, NOT a readiness requirement: the aggregator is ready off its
    # depends_on (RiskDataPoint) even though the used type (ChatAnswer) is absent, and a later
    # ChatAnswer ADD re-fires the interim aggregator anyway.
    clock = FakeClock()
    runtime = build_in_memory_runtime(clock)
    report = make_aggregator(
        'report',
        depends_on={RiskDataPoint},
        uses={ChatAnswerDataPoint},
        rerun_on_new_data=True,
        interim_refresh=True,
        on_aggregate=_write_counts,
    )
    # completes_when is a type never delivered → the session parks and stays interim.
    flow = FlowDefinition(name='live', operators=(report,), completes_when=TriggerDataPoint, park_after=0.0)
    sid = SessionId('uses')
    manager = SessionOrchestrationManager(runtime)

    # Ready off RiskDataPoint alone (ChatAnswer, a `uses` type, is absent and does NOT block it).
    await manager.start_session(session_id=sid, namespace_id=NAMESPACE, flow=flow, seed=[risk(0.4)])
    first = await runtime.durable.read(REPORTS, str(sid))
    assert first is not None and first.document['total'] == 1 and first.status == 'in_progress'

    # A `uses` type arrives → the interim aggregator RE-RUNS and folds it, though ChatAnswer is
    # neither a dependency nor a readiness gate.
    await manager.deliver(session_id=sid, namespace_id=NAMESPACE, flow=flow, data_point=chat_answer('hi'))
    refreshed = await runtime.durable.read(REPORTS, str(sid))
    assert refreshed is not None and refreshed.document['total'] == 2 and refreshed.status == 'in_progress'


@pytest.mark.asyncio
async def test_undeclared_non_dependency_data_does_not_rerun() -> None:
    # Control: with the same shape but ChatAnswer in neither depends_on nor uses, its arrival is not
    # rerun-worthy, so the interim doc is not refreshed.
    clock = FakeClock()
    runtime = build_in_memory_runtime(clock)
    report = make_aggregator(
        'report',
        depends_on={RiskDataPoint},
        rerun_on_new_data=True,
        interim_refresh=True,
        on_aggregate=_write_counts,
    )
    flow = FlowDefinition(name='live', operators=(report,), completes_when=TriggerDataPoint, park_after=0.0)
    sid = SessionId('no-uses')
    manager = SessionOrchestrationManager(runtime)

    await manager.start_session(session_id=sid, namespace_id=NAMESPACE, flow=flow, seed=[risk(0.4)])
    await manager.deliver(session_id=sid, namespace_id=NAMESPACE, flow=flow, data_point=chat_answer('hi'))
    doc = await runtime.durable.read(REPORTS, str(sid))
    assert doc is not None and doc.document['total'] == 1  # the ChatAnswer never triggered a rerun


@pytest.mark.asyncio
async def test_non_interim_aggregator_does_not_run_during_gathering() -> None:
    clock = FakeClock()
    runtime = build_in_memory_runtime(clock)
    # interim_refresh defaults to False — the aggregator must NOT run until the finalize phase.
    report = make_aggregator('report', depends_on={RiskDataPoint}, on_aggregate=_write_score)
    flow = FlowDefinition(name='batch', operators=(report,), completes_when=ChatAnswerDataPoint, park_after=0.0)
    sid = SessionId('s4')
    manager = SessionOrchestrationManager(runtime)

    await manager.start_session(session_id=sid, namespace_id=NAMESPACE, flow=flow, seed=[risk(0.4)])
    assert await runtime.durable.read(REPORTS, str(sid)) is None  # nothing written until completion


@pytest.mark.asyncio
async def test_failing_interim_run_is_best_effort() -> None:
    clock = FakeClock()
    runtime = build_in_memory_runtime(clock)
    calls: dict[str, int] = {'n': 0}

    async def flaky(ctx: OperatorContext) -> None:
        calls['n'] += 1
        if not ctx.is_final:  # interim runs raise; the finalize pass succeeds
            raise RuntimeError('interim boom')
        assert ctx.aggregation is not None
        await ctx.aggregation.upsert(REPORTS, str(ctx.session_id), {'ok': True})

    report = make_aggregator(
        'report', depends_on={RiskDataPoint}, rerun_on_new_data=True, interim_refresh=True, on_aggregate=flaky
    )
    flow = FlowDefinition(name='live', operators=(report,), completes_when=ChatAnswerDataPoint, park_after=0.0)
    sid = SessionId('s5')
    manager = SessionOrchestrationManager(runtime)

    # Interim run raises; fault-isolation swallows it without dead-lettering or wedging the session.
    await manager.start_session(session_id=sid, namespace_id=NAMESPACE, flow=flow, seed=[risk(0.4)])
    result = await manager.deliver(session_id=sid, namespace_id=NAMESPACE, flow=flow, data_point=chat_answer('done'))
    assert result is not None and result.dead_letters == ()  # interim failure never dead-letters
    doc = await runtime.durable.read(REPORTS, str(sid))
    assert doc is not None and doc.status == 'final'  # finalize still ran and committed


@pytest.mark.asyncio
async def test_resume_after_interim_still_finalizes() -> None:
    clock = FakeClock()
    runtime = build_in_memory_runtime(clock)
    report = make_aggregator(
        'report', depends_on={RiskDataPoint}, rerun_on_new_data=True, interim_refresh=True, on_aggregate=_write_score
    )
    flow = FlowDefinition(name='live', operators=(report,), completes_when=ChatAnswerDataPoint, park_after=0.0)
    sid = SessionId('s6')
    manager = SessionOrchestrationManager(runtime)

    await manager.start_session(session_id=sid, namespace_id=NAMESPACE, flow=flow, seed=[risk(0.4)])
    result = await manager.deliver(session_id=sid, namespace_id=NAMESPACE, flow=flow, data_point=chat_answer('done'))
    assert result is not None and result.status is SessionStatus.COMPLETED
    final_doc = await runtime.durable.read(REPORTS, str(sid))
    assert final_doc is not None and final_doc.status == 'final'


@pytest.mark.asyncio
async def test_superseded_run_with_interim_write_ends_without_finalization() -> None:
    # A fenced run (higher epoch took over mid-gather) ends SUPERSEDED: the interim write that
    # already landed in the durable store is the last record, contribution is never marked, and
    # the finalize pass never ran — the successor (a higher epoch) is responsible for completing.
    clock = FakeClock()
    runtime = replace(build_in_memory_runtime(clock), lock=_FenceOnRenewLock(clock))
    sid = SessionId('s7')
    report = make_aggregator(
        'report', depends_on={RiskDataPoint}, rerun_on_new_data=True, interim_refresh=True, on_aggregate=_write_score
    )
    # A self-cycling operator with a long debounce forces a loop sleep that crosses the renew
    # interval, triggering the fenced renew after the interim aggregator has already written.
    ticker = make_operator(
        'ticker',
        depends_on={RiskDataPoint},
        rerun_on_new_data=True,
        debounce=timedelta(seconds=20),
    )
    result = await Orchestrator(
        session_id=sid,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[report, ticker],
        seed=[risk(0.4)],
        completes_when=ChatAnswerDataPoint,
        session_deadline=300.0,
    ).run()
    assert result.status is SessionStatus.SUPERSEDED  # fenced mid-run → clean stop, not a raise
    # The finalize pass never ran: contribution was not marked and the durable doc stays in_progress
    # (the interim write the fenced run already landed is its last word; a successor overwrites at a
    # higher epoch when it finalizes).
    assert await runtime.durable.is_contribution_marked(sid, OperatorId('report')) is False
    superseded_doc = await runtime.durable.read(REPORTS, str(sid))
    assert superseded_doc is not None and superseded_doc.status == 'in_progress'


@pytest.mark.asyncio
async def test_quiescence_not_blocked_by_interim_aggregator() -> None:
    # An interim aggregator with rerun_on_new_data=True that has caught up (no new data) must
    # let the session quiesce and park — it must NOT spin indefinitely or block start_session.
    clock = FakeClock()
    runtime = build_in_memory_runtime(clock)
    report = make_aggregator(
        'report', depends_on={RiskDataPoint}, rerun_on_new_data=True, interim_refresh=True, on_aggregate=_write_score
    )
    flow = FlowDefinition(name='live', operators=(report,), completes_when=ChatAnswerDataPoint, park_after=0.0)
    sid = SessionId('s8')
    manager = SessionOrchestrationManager(runtime)

    # start_session must return (not hang): after the aggregator processes the seed data and
    # finds no further changes, the session reaches the inbox wait and parks immediately.
    result = await manager.start_session(session_id=sid, namespace_id=NAMESPACE, flow=flow, seed=[risk(0.4)])
    assert result.status is SessionStatus.PARKED  # parked, not hung
    interim_doc = await runtime.durable.read(REPORTS, str(sid))
    assert interim_doc is not None and interim_doc.status == 'in_progress'  # interim write happened


@pytest.mark.asyncio
async def test_namespace_gated_interim_aggregator_writes_nothing_in_either_phase() -> None:
    # An interim_refresh aggregator the namespace does not permit must be dropped from BOTH the gather set
    # and the aggregation set: no in_progress write during gathering and no final write at completion.
    clock = FakeClock()
    catalog = InMemoryCapabilityCatalog(permitted_operators={NAMESPACE: {OperatorId('kept')}})
    runtime = build_in_memory_runtime(clock, catalog=catalog)
    kept = make_operator(
        'kept', depends_on={RiskDataPoint}, produces={ChatAnswerDataPoint}, emits=[chat_answer('done')]
    )
    report = make_aggregator(
        'report', depends_on={RiskDataPoint}, rerun_on_new_data=True, interim_refresh=True, on_aggregate=_write_score
    )
    flow = FlowDefinition(name='gated', operators=(kept, report), completes_when=ChatAnswerDataPoint, park_after=0.0)
    sid = SessionId('s10')
    manager = SessionOrchestrationManager(runtime)

    result = await manager.start_session(session_id=sid, namespace_id=NAMESPACE, flow=flow, seed=[risk(0.4)])
    assert result.status is SessionStatus.COMPLETED  # 'kept' satisfies completes_when; the session finishes
    assert await runtime.durable.read(REPORTS, str(sid)) is None  # the gated report never wrote, interim or final
    assert await runtime.durable.is_contribution_marked(sid, OperatorId('report')) is False


@pytest.mark.asyncio
async def test_persistently_failing_interim_aggregator_never_blocks_or_dead_letters() -> None:
    # An interim aggregator that raises on EVERY interim run must still let the session park (its watermark
    # advances on failure, so it does not loop) and must never dead-letter — only the final pass has finality.
    clock = FakeClock()
    runtime = build_in_memory_runtime(clock)
    interim_attempts = {'n': 0}

    async def flaky(ctx: OperatorContext) -> None:
        if not ctx.is_final:
            interim_attempts['n'] += 1
            raise RuntimeError('interim boom')
        assert ctx.aggregation is not None
        await ctx.aggregation.upsert(REPORTS, str(ctx.session_id), {'ok': True})

    report = make_aggregator(
        'report', depends_on={RiskDataPoint}, rerun_on_new_data=True, interim_refresh=True, on_aggregate=flaky
    )
    flow = FlowDefinition(name='flaky', operators=(report,), completes_when=ChatAnswerDataPoint, park_after=0.0)
    sid = SessionId('s11')
    manager = SessionOrchestrationManager(runtime)

    first = await manager.start_session(session_id=sid, namespace_id=NAMESPACE, flow=flow, seed=[risk(0.1)])
    assert first.status is SessionStatus.PARKED and first.dead_letters == ()
    second = await manager.deliver(session_id=sid, namespace_id=NAMESPACE, flow=flow, data_point=risk(0.9))
    assert second is not None and second.status is SessionStatus.PARKED and second.dead_letters == ()
    assert interim_attempts['n'] >= 2  # reran on the new data, each failing — never looping forever

    final = await manager.deliver(session_id=sid, namespace_id=NAMESPACE, flow=flow, data_point=chat_answer('done'))
    assert final is not None and final.status is SessionStatus.COMPLETED and final.dead_letters == ()
    doc = await runtime.durable.read(REPORTS, str(sid))
    assert doc is not None and doc.status == 'final'


class _FenceOnCompleteLock(InMemorySessionLock):
    """Lets a run finish its finalize, then fences it at ``mark_complete``.

    The shape of a predecessor that lost its lease in the completion tail after its finalize pass had
    already committed ``final``. That tail runs unrenewed, so a durable write stalling there for longer
    than the lock's TTL produces exactly this: a live owner fenced by a supervisor that read the session
    as orphaned.
    """

    async def mark_complete(self, session_id: SessionId, *, epoch: Epoch) -> None:  # noqa: ARG002
        raise StaleEpochError('a higher epoch took over')


@pytest.mark.asyncio
async def test_a_resumed_epoch_never_downgrades_an_already_final_record() -> None:
    # The failure this pins, seen on dev2: epoch 1 finalized (status `final`, contribution marked) but was
    # fenced in its completion tail before marking the session complete. The successor rehydrated, its
    # interim_refresh aggregator re-ran during gather and overwrote `final` with `in_progress`, and its
    # finalize pass was then SKIPPED as already-contributed — so the record was stranded at `in_progress`
    # for good. Every consumer polls for `final`, so a complete session read as "no data at all".
    clock = FakeClock()
    runtime = replace(build_in_memory_runtime(clock), lock=_FenceOnCompleteLock(clock))
    sid = SessionId('s-downgrade')
    report = make_aggregator(
        'report', depends_on={RiskDataPoint}, rerun_on_new_data=True, interim_refresh=True, on_aggregate=_write_score
    )

    fenced = await Orchestrator(
        session_id=sid,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[report],
        seed=[risk(0.4), chat_answer('done')],
        completes_when=ChatAnswerDataPoint,
        session_deadline=300.0,
    ).run()

    # Precondition: the predecessor DID finalize, and only its completion mark was lost.
    assert fenced.status is SessionStatus.SUPERSEDED
    assert await runtime.durable.is_contribution_marked(sid, OperatorId('report')) is True
    finalized = await runtime.durable.read(REPORTS, str(sid))
    assert finalized is not None and finalized.status == 'final'

    # The successor: fresh data makes the interim aggregator re-run during gather, and the finalize pass
    # is skipped because the contribution is already marked.
    resumed = await Orchestrator(
        session_id=sid,
        namespace_id=NAMESPACE,
        runtime=replace(runtime, lock=InMemorySessionLock(clock)),
        operators=[report],
        seed=[risk(0.9)],
        completes_when=ChatAnswerDataPoint,
        session_deadline=300.0,
    ).run()

    assert resumed.status is SessionStatus.COMPLETED
    still_final = await runtime.durable.read(REPORTS, str(sid))
    assert still_final is not None
    # `final` is terminal for an aggregator's output: an interim refresh that lands afterwards must not
    # be able to walk it back, whatever the scheduling.
    assert still_final.status == 'final'


# The lock's TTL is decided against the injected clock, so the production failure — a lease lapsing
# under a live owner because the completion tail runs unrenewed — is reproducible on virtual time.
_LOCK_TTL_SECONDS = 30.0


@pytest.mark.asyncio
async def test_a_lease_lost_in_the_completion_tail_cannot_strand_the_record() -> None:
    # The whole failure, end to end, on the real mechanism rather than a stand-in fence: the tail stalls
    # past the lock's TTL, so the lease lapses while its owner is still working; the supervisor reads the
    # session as genuinely orphaned and resumes it at a higher epoch, fencing the original mid-finalize.
    # That takeover is tolerated — the tail being unrenewed is a known, documented gap — but it must not
    # walk the record back. On dev2 it did: the successor's interim pass overwrote `final` with
    # `in_progress` and skipped its own finalize as already-contributed, stranding a complete session
    # where every consumer polls for `final` and so reads it as holding no data at all.
    clock = FakeClock()
    runtime = build_in_memory_runtime(clock)
    sid = SessionId('s-tail-stall')
    manager = SessionOrchestrationManager(runtime)
    report = make_aggregator(
        'report', depends_on={RiskDataPoint}, rerun_on_new_data=True, interim_refresh=True, on_aggregate=_write_score
    )
    flow = FlowDefinition(name='live', operators=(report,), completes_when=ChatAnswerDataPoint)

    tail_blocked = asyncio.Event()
    release_tail = asyncio.Event()
    real_pending_count = runtime.inbox.pending_count
    stalls_left = 1

    async def stalling_pending_count(session_id: SessionId) -> int:
        # Only the FIRST caller stalls: the successor's own tail must run normally, or it would wedge too.
        nonlocal stalls_left
        if stalls_left:
            stalls_left -= 1
            tail_blocked.set()
            await release_tail.wait()
        return await real_pending_count(session_id)

    runtime.inbox.pending_count = stalling_pending_count  # type: ignore[method-assign]

    stalled = asyncio.create_task(
        Orchestrator(
            session_id=sid,
            namespace_id=NAMESPACE,
            runtime=runtime,
            operators=[report],
            seed=[risk(0.4), chat_answer('done')],
            completes_when=ChatAnswerDataPoint,
            session_deadline=300.0,
        ).run()
    )
    await tail_blocked.wait()

    # Its finalize already committed; only the completion mark is still outstanding.
    finalized = await runtime.durable.read(REPORTS, str(sid))
    assert finalized is not None and finalized.status == 'final'

    # Carry the clock past the TTL. Nothing renews in the tail, so the lease lapses under a LIVE owner —
    # this is the gap itself, asserted rather than described.
    clock.advance(_LOCK_TTL_SECONDS + 1.0)
    assert await runtime.lock.is_held(sid) is False
    assert await manager.is_orphaned(sid) is True

    # A late deliver lands while the owner is stalled — the shape that produced this on dev2, where
    # participant info arrived during the run. With the lock free and the session not complete, the
    # supervisor resumes it at a higher epoch, and the successor's gather sees the new data, so its
    # interim_refresh aggregator re-runs (the write that used to walk `final` back).
    resumed = await manager.deliver(session_id=sid, namespace_id=NAMESPACE, flow=flow, data_point=risk(0.9))
    assert resumed is not None and resumed.status is SessionStatus.COMPLETED
    assert int(resumed.epoch) > 1  # the takeover really happened
    # The successor ran the aggregator during gather but NOT as its finalize: the contribution was already
    # marked by the fenced predecessor, so `_aggregate` skipped it. Its last word was an interim write.
    assert await runtime.durable.is_contribution_marked(sid, OperatorId('report')) is True

    release_tail.set()
    assert (await stalled).status is SessionStatus.SUPERSEDED  # fenced by the successor, as designed

    survived = await runtime.durable.read(REPORTS, str(sid))
    assert survived is not None
    assert survived.status == 'final'


async def test_a_hung_completion_tail_fails_the_run_instead_of_holding_the_epoch() -> None:
    # The tail is the one stretch the gathering loop's renewals do not cover, and the lock's TTL is
    # specified to exceed the longest single unrenewed await. A durable flush that blocks indefinitely
    # broke that promise silently: the lease lapsed under a live owner and a supervisor resumed the
    # session on top of it, mid-finalize. Bounding the step keeps the lease honest — the run fails, the
    # epoch is released, and the successor re-drives from durable state.
    clock = FakeClock()
    runtime = build_in_memory_runtime(clock)
    sid = SessionId('s-hung-tail')
    hung = asyncio.Event()  # never set: models a write that simply never returns

    async def never_returns(session_id: SessionId) -> int:  # noqa: ARG001
        await hung.wait()
        return 0

    runtime.inbox.pending_count = never_returns  # type: ignore[method-assign]
    report = make_aggregator(
        'report', depends_on={RiskDataPoint}, rerun_on_new_data=True, interim_refresh=True, on_aggregate=_write_score
    )

    with pytest.raises(CompletionTailTimeoutError):
        await Orchestrator(
            session_id=sid,
            namespace_id=NAMESPACE,
            runtime=runtime,
            operators=[report],
            seed=[risk(0.4), chat_answer('done')],
            completes_when=ChatAnswerDataPoint,
            session_deadline=300.0,
            operation_timeout=0.05,
        ).run()

    # The epoch is released even though the tail blew up, so a successor can take the session over
    # immediately rather than waiting out the TTL.
    assert await runtime.lock.is_held(sid) is False
    # And it is NOT marked complete: the run did not finish, so the session stays re-drivable.
    assert await runtime.lock.is_complete(sid) is False


# The coalescing window an interim aggregator arms. Its exact width is irrelevant to the behaviour
# under test — what matters is that the completion tail no longer scales with it.
_INTERIM_WINDOW = timedelta(milliseconds=250)


async def _record_fold(
    folds: list[bool], write: Callable[[OperatorContext], Awaitable[None]], ctx: OperatorContext
) -> None:
    """Note whether each fold was the authoritative one, then delegate the durable write."""
    folds.append(ctx.is_final)
    await write(ctx)


@pytest.mark.asyncio
async def test_a_satisfied_completion_does_not_charge_the_interim_window_to_the_tail() -> None:
    # The tail is the one stretch where an interim refold can buy nothing: with the completion
    # condition already satisfied and nothing left running, the next thing that happens is the
    # finalize pass, which rewrites the same document. Waiting the window out there produced a
    # second `in_progress` write that `final` overwrote in the same instant — no reader could ever
    # observe it — while charging the window's full width to every session that completes.
    clock = FakeClock()
    runtime = build_in_memory_runtime(clock)
    folds: list[bool] = []
    report = make_aggregator(
        'report',
        depends_on={RiskDataPoint},
        rerun_on_new_data=True,
        interim_refresh=True,
        debounce=_INTERIM_WINDOW,
        on_aggregate=lambda ctx: _record_fold(folds, _write_score, ctx),
    )
    # One late detector: ready off the already-present completing DataPoint, and its emission is
    # what arms the aggregator's window with nothing else left to run.
    late = make_operator('late', depends_on={ChatAnswerDataPoint}, produces={RiskDataPoint}, emits=[risk(0.9)])
    sid = SessionId('s-tail-charge')

    result = await Orchestrator(
        session_id=sid,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[report, late],
        seed=[risk(0.4), chat_answer('done')],
        completes_when=ChatAnswerDataPoint,
        session_deadline=300.0,
    ).run()

    assert result.status is SessionStatus.COMPLETED
    # FakeClock advances only where the gathering loop itself waits, so this IS the simulated time
    # the session spent in its tail — and it must not scale with the window.
    assert clock.monotonic() == 0.0, 'the completion tail waited out the interim window'
    assert folds == [False, True], 'the tail ran an interim refold the finalize immediately overwrote'
    doc = await runtime.durable.read(REPORTS, str(sid))
    # Nothing is lost by abandoning the refold: the finalize folds the late Risk authoritatively.
    assert doc is not None and doc.status == 'final' and doc.document['risk_count'] == 2


@pytest.mark.asyncio
async def test_an_interim_window_still_collapses_a_burst_arriving_while_work_runs() -> None:
    # The coalescing itself is the point of the window and must survive: without it every arrival
    # forces a full rebuild-reseal-rewrite on the event loop, competing with operators still on the
    # critical path. A self-cycling ticker delivers its arrivals in separate gather passes with work
    # still in flight — exactly the burst the window exists to collapse.
    clock = FakeClock()
    runtime = build_in_memory_runtime(clock)
    refolded_at: list[float] = []

    async def note_refold(ctx: OperatorContext) -> None:
        if not ctx.is_final:
            refolded_at.append(clock.monotonic())
        await _write_score(ctx)

    report = make_aggregator(
        'report',
        depends_on={IpDataPoint},
        rerun_on_new_data=True,
        interim_refresh=True,
        debounce=timedelta(seconds=3),
        on_aggregate=note_refold,
    )
    counter = count()
    ticker = make_operator(
        'ticker',
        depends_on={IpDataPoint},
        produces={IpDataPoint},
        rerun_on_new_data=True,
        max_cycles=6,
        debounce=timedelta(seconds=1),  # one arrival per simulated second, for six seconds
        emit_factory=lambda _ctx: [ip(f'198.51.100.{next(counter)}')],  # noqa: ARG005
    )
    sid = SessionId('s-burst')

    result = await Orchestrator(
        session_id=sid,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[report, ticker],
        seed=[ip('198.51.100.254')],
        session_deadline=300.0,
    ).run()

    assert result.status is SessionStatus.COMPLETED
    stored = (await runtime.store.snapshot(sid)).of_type(IpDataPoint)
    assert len(stored) == 7  # seven arrivals, one per simulated second, while the ticker cycles
    # One refold per window, not one per arrival: the burst still collapses while work is in flight.
    # A zero-width window would refold on every one of the seven.
    assert refolded_at[:2] == [0.0, 3.0], 'the window stopped collapsing arrivals into one refold'
    # The ticker's last cycle lands at 5.0s and nothing else can run after it, so any later refold is
    # one the imminent finalize would overwrite — and the wait for it is charged to the tail.
    assert max(refolded_at) < 5.0, 'an interim refold ran once the work had drained, ahead of the finalize'
    assert clock.monotonic() == 5.0, 'the completion tail waited out the interim window'


async def _write_totals(ctx: OperatorContext) -> None:
    total = sum(1 for _ in ctx.store.of_type(RiskDataPoint)) + sum(1 for _ in ctx.store.of_type(IpDataPoint))
    assert ctx.aggregation is not None
    await ctx.aggregation.upsert(REPORTS, str(ctx.session_id), {'total': total, 'is_final': ctx.is_final})


@pytest.mark.asyncio
async def test_an_unsatisfied_completion_still_waits_out_the_window_before_going_idle() -> None:
    # The mirror image, and the reason the exemption is conditional rather than blanket: with the
    # completion condition unsatisfied, a would-be-quiescent session is heading for an inbox wait of
    # unbounded length, where the interim document is the only live view a reader gets. Abandoning
    # the window there would trade a bounded tail charge for a stale record over an open-ended wait.
    clock = FakeClock()
    runtime = build_in_memory_runtime(clock)
    folds: list[bool] = []
    report = make_aggregator(
        'report',
        depends_on={RiskDataPoint},
        uses={IpDataPoint},
        rerun_on_new_data=True,
        interim_refresh=True,
        debounce=timedelta(seconds=2),
        on_aggregate=lambda ctx: _record_fold(folds, _write_totals, ctx),
    )
    late = make_operator('late', depends_on={RiskDataPoint}, produces={IpDataPoint}, emits=[ip('198.51.100.7')])
    # completes_when never arrives, so the session reaches the inbox wait and parks immediately.
    flow = FlowDefinition(name='live', operators=(report, late), completes_when=ChatAnswerDataPoint, park_after=0.0)
    sid = SessionId('s-idle-window')
    manager = SessionOrchestrationManager(runtime)

    result = await manager.start_session(session_id=sid, namespace_id=NAMESPACE, flow=flow, seed=[risk(0.4)])

    assert result.status is SessionStatus.PARKED
    assert clock.monotonic() == 2.0, 'the interim window was abandoned before an idle wait'
    assert folds == [False, False], 'the refold that carries the live view never ran'
    doc = await runtime.durable.read(REPORTS, str(sid))
    # The refold landed before the session went idle, so a reader polling the record sees the late
    # arrival rather than a snapshot frozen at the first fold.
    assert doc is not None and doc.status == 'in_progress' and doc.document['total'] == 2
