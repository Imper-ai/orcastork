"""AGG — aggregation phase: idempotency, OCC, contribution markers, dead-letter, backoff."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import timedelta
from itertools import count

import pytest

from orcastork.adapters.memory import InMemoryDurableStore, InMemorySessionLock
from orcastork.aggregation import AggregationHelpers, RetryPolicy, backoff_delays, run_with_retry
from orcastork.audit import AuditLogEntry, OperatorOutcome
from orcastork.exceptions import AggregatorDeadLetteredError, OptimisticConcurrencyError, StaleEpochError
from orcastork.ids import Epoch, NamespaceId, OperatorId, SessionId
from orcastork.operators import OperatorContext
from orcastork.orchestrator import Orchestrator
from orcastork.orchestrator.orchestrator import SessionStatus
from orcastork.runtime import build_in_memory_runtime

from .doubles.clock import FakeClock
from .doubles.conformance import DurableStoreConformance
from .doubles.datapoints import DEFAULT_OP, T0, EmailDataPoint, RiskDataPoint, TriggerDataPoint, risk, work_email
from .doubles.logs import capture_logs
from .doubles.operators import make_aggregator, make_operator

SID = SessionId('agg-session')
NAMESPACE = NamespaceId('agg-namespace')
FAST_RETRY = RetryPolicy(max_attempts=2, base_delay=0.0)  # no real sleeping in tests


class TestInMemoryDurableStore(DurableStoreConformance):
    @pytest.fixture
    def durable(self) -> InMemoryDurableStore:
        return InMemoryDurableStore()


async def _no_sleep(_delay: float) -> None:  # noqa: ARG001
    return None


async def test_agg_01_runs_in_aggregation_phase_and_writes_durable(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)

    async def write(ctx: OperatorContext) -> None:
        assert ctx.aggregation is not None
        await ctx.aggregation.upsert('reports', 'report', {'risk_count': len(ctx.store.of_type(RiskDataPoint))})

    scorer = make_operator('s', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    reporter = make_aggregator('rep', depends_on={RiskDataPoint}, on_aggregate=write)
    await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[scorer, reporter], seed=[work_email()]
    ).run()
    document = await runtime.durable.read('reports', 'report')
    assert document is not None and document.document == {'risk_count': 1}


async def test_agg_02_contribution_marker_makes_rerun_a_noop(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    calls: list[int] = []

    async def write(ctx: OperatorContext) -> None:  # noqa: ARG001
        calls.append(1)

    scorer = make_operator('s', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    reporter = make_aggregator('rep', depends_on={RiskDataPoint}, on_aggregate=write)
    for _ in range(2):  # a redundant re-drive of the same session
        await Orchestrator(
            session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[scorer, reporter], seed=[work_email()]
        ).run()
    assert calls == [1]  # contributed at most once


async def test_agg_03_dead_letter_flags_domain_and_peers_unaffected(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)

    async def boom(ctx: OperatorContext) -> None:  # noqa: ARG001
        raise ValueError('cannot aggregate')

    async def healthy(ctx: OperatorContext) -> None:
        assert ctx.aggregation is not None
        await ctx.aggregation.upsert('reports', 'healthy', {'ok': True})

    scorer = make_operator('s', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    failing = make_aggregator('failing', depends_on={RiskDataPoint}, on_aggregate=boom)
    good = make_aggregator('healthy', depends_on={RiskDataPoint}, on_aggregate=healthy)
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[scorer, failing, good],
        seed=[work_email()],
        retry_policy=FAST_RETRY,
    ).run()

    assert result.status is SessionStatus.COMPLETED  # session still completes
    assert {dead.operator_id for dead in result.dead_letters} == {OperatorId('failing')}  # recorded for re-drive
    assert await runtime.durable.read('reports', 'healthy') is not None  # the peer aggregator is unaffected


async def test_agg_04_add_to_set_is_idempotent() -> None:
    durable = InMemoryDurableStore()
    await durable.add_to_set('profiles', 'profile', 'sessions', 'session-1', epoch=Epoch(1))
    size = await durable.add_to_set('profiles', 'profile', 'sessions', 'session-1', epoch=Epoch(1))  # again
    assert size == 1  # set-cardinality, never a double-counting increment


async def test_agg_05_occ_upsert_does_not_lose_updates() -> None:
    durable = InMemoryDurableStore()
    session_a = AggregationHelpers(
        durable,
        session_id=SessionId('a'),
        operator_id=OperatorId('profile'),
        epoch=Epoch(1),
        clock=FakeClock(),
        is_final=True,
    )
    session_b = AggregationHelpers(
        durable,
        session_id=SessionId('b'),
        operator_id=OperatorId('profile'),
        epoch=Epoch(1),
        clock=FakeClock(),
        is_final=True,
    )

    await session_a.upsert('profiles', 'profile', {'a': 1})  # version 1
    await session_b.upsert('profiles', 'profile', {'a': 1, 'b': 2})  # reads v1, writes v2 — no lost update
    document = await durable.read('profiles', 'profile')
    assert document is not None and document.version == 2 and document.document == {'a': 1, 'b': 2}

    with pytest.raises(OptimisticConcurrencyError):  # a stale-version write conflicts
        await durable.upsert('profiles', 'profile', {}, expected_version=1, epoch=Epoch(1))


async def test_agg_05a_helper_add_to_set_forwards_bound_epoch_and_returns_size() -> None:
    # The helper is the aggregator-facing set-cardinality surface, and it injects its bound epoch so
    # the write fences exactly like upsert. A predecessor bound to a lower epoch must be rejected once
    # a higher epoch has mutated the same (table, key) — proving the helper threads self._epoch through,
    # not a hardcoded or defaulted value — while a fresh add returns the true set size.
    durable = InMemoryDurableStore()
    helper = AggregationHelpers(
        durable, session_id=SID, operator_id=OperatorId('profile'), epoch=Epoch(1), clock=FakeClock(), is_final=True
    )

    assert await helper.add_to_set('profiles', 'profile', 'sessions', 'session-1') == 1  # set size, not a count
    assert await helper.add_to_set('profiles', 'profile', 'sessions', 'session-2') == 2  # union grows

    await durable.add_to_set('profiles', 'profile', 'sessions', 'session-3', epoch=Epoch(2))  # successor takeover
    with pytest.raises(StaleEpochError):  # the epoch-1 helper is now fenced on this key
        await helper.add_to_set('profiles', 'profile', 'sessions', 'session-4')


async def test_agg_05b_helper_mark_contribution_dedups_and_stamps_under_bound_epoch() -> None:
    # mark_contribution must forward the bound (session_id, operator_id, epoch): the first call newly
    # marks, the second is a stable False (at-most-once dedup), and the mark advances the session's
    # contribution fence to the bound epoch — so a later lower-epoch marker for a DIFFERENT operator is
    # rejected as stale. That last assertion is what pins the *epoch* (not just the ids) is threaded.
    durable = InMemoryDurableStore()
    helper = AggregationHelpers(
        durable, session_id=SID, operator_id=OperatorId('profile'), epoch=Epoch(2), clock=FakeClock(), is_final=True
    )

    assert await helper.mark_contribution() is True  # newly recorded under the bound (session, operator)
    assert await helper.mark_contribution() is False  # already recorded — at-most-once
    assert await durable.is_contribution_marked(SID, OperatorId('profile'))  # stamped for the bound operator

    with pytest.raises(StaleEpochError):  # the epoch-2 mark advanced the session fence past 1
        await durable.mark_contribution(SID, OperatorId('other'), epoch=Epoch(1))


async def test_agg_06_occ_conflict_retries_then_dead_letters() -> None:
    attempts = count()

    async def succeeds_on_third() -> None:
        if next(attempts) < 2:
            raise OptimisticConcurrencyError('version conflict')

    await run_with_retry(
        succeeds_on_third, policy=RetryPolicy(max_attempts=3, base_delay=0.0), seed=1, sleep=_no_sleep
    )

    async def always_conflicts() -> None:
        raise OptimisticConcurrencyError('version conflict')

    with pytest.raises(AggregatorDeadLetteredError):
        await run_with_retry(
            always_conflicts, policy=RetryPolicy(max_attempts=3, base_delay=0.0), seed=1, sleep=_no_sleep
        )


async def test_agg_07_aggregator_skips_ephemeral(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    captured: dict[str, int] = {}

    async def write(ctx: OperatorContext) -> None:
        captured['persisted'] = len([dp for dp in ctx.store if not dp.is_ephemeral])

    trigger = TriggerDataPoint(value='go', retrieved_by=DEFAULT_OP, first_retrieved=T0, last_retrieved=T0)
    reporter = make_aggregator('rep', depends_on={RiskDataPoint}, on_aggregate=write)
    await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[reporter], seed=[risk(), trigger]
    ).run()
    assert captured['persisted'] == 1  # the ephemeral Trigger is excluded


async def test_agg_08_aggregators_are_independent(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)

    async def boom(ctx: OperatorContext) -> None:  # noqa: ARG001
        raise ValueError('boom')

    async def healthy(ctx: OperatorContext) -> None:
        assert ctx.aggregation is not None
        await ctx.aggregation.upsert('reports', 'healthy', {'ok': True})

    scorer = make_operator('s', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    failing = make_aggregator('failing', depends_on={RiskDataPoint}, on_aggregate=boom)
    good = make_aggregator('healthy', depends_on={RiskDataPoint}, on_aggregate=healthy)
    await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[scorer, failing, good],
        seed=[work_email()],
        retry_policy=FAST_RETRY,
    ).run()
    assert await runtime.durable.read('reports', 'healthy') is not None  # the failing peer did not block this one


async def test_agg_09_resume_reruns_only_unfinished(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    calls = {'a': 0, 'b': 0}

    async def run_a(ctx: OperatorContext) -> None:  # noqa: ARG001
        calls['a'] += 1

    async def run_b(ctx: OperatorContext) -> None:  # noqa: ARG001
        calls['b'] += 1

    scorer = make_operator('s', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    aggregator_a = make_aggregator('agg_a', depends_on={RiskDataPoint}, on_aggregate=run_a)
    await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[scorer, aggregator_a], seed=[work_email()]
    ).run()
    assert calls == {'a': 1, 'b': 0}

    # "Resume": agg_a already completed; a newly-added agg_b is the only unfinished one.
    aggregator_b = make_aggregator('agg_b', depends_on={RiskDataPoint}, on_aggregate=run_b)
    await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[scorer, aggregator_a, aggregator_b],
        seed=[work_email()],
    ).run()
    assert calls == {'a': 1, 'b': 1}  # completed aggregator skipped, unfinished one re-driven


async def test_agg_10_aggregator_sink_never_retriggers_gathering(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    gathering_runs = count()

    def emit(ctx: OperatorContext) -> list[RiskDataPoint]:  # noqa: ARG001
        next(gathering_runs)
        return [risk()]

    async def noop(ctx: OperatorContext) -> None:  # noqa: ARG001
        return None

    scorer = make_operator('s', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emit_factory=emit)
    reporter = make_aggregator('rep', depends_on={RiskDataPoint}, on_aggregate=noop)  # produces nothing → sink
    await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[scorer, reporter], seed=[work_email()]
    ).run()
    assert next(gathering_runs) == 1  # the gathering operator ran once; the sink did not re-trigger it


async def test_agg_11_dead_letter_records_failure_for_redrive(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)

    async def boom(ctx: OperatorContext) -> None:  # noqa: ARG001
        raise ValueError('boom')

    scorer = make_operator('s', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    failing = make_aggregator('failing', depends_on={RiskDataPoint}, on_aggregate=boom)
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[scorer, failing],
        seed=[work_email()],
        retry_policy=FAST_RETRY,
    ).run()
    assert [dead.operator_id for dead in result.dead_letters] == [OperatorId('failing')]


def test_agg_12_backoff_is_jittered_and_seeded() -> None:
    policy = RetryPolicy(max_attempts=5, base_delay=0.05, jitter=0.2)
    delays = backoff_delays(policy, seed=42)
    assert len(set(delays)) > 1  # jittered — not a fixed schedule
    assert delays == backoff_delays(policy, seed=42)  # deterministic for a seed
    assert delays != [0.05 * (2**attempt) for attempt in range(5)]  # differs from the un-jittered schedule


def _aggregator_runs(entries: tuple[AuditLogEntry, ...], operator_id: OperatorId) -> list[OperatorOutcome]:
    return [
        entry.operator.outcome for entry in entries if entry.operator is not None and entry.operator_id == operator_id
    ]


async def test_agg_13_audits_each_retry_then_success(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    attempts = count(1)

    async def fail_once(ctx: OperatorContext) -> None:
        if next(attempts) == 1:
            raise OptimisticConcurrencyError('version conflict')  # first attempt loses the OCC race
        assert ctx.aggregation is not None
        await ctx.aggregation.upsert('reports', 'report', {'ok': True})

    scorer = make_operator('scorer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    reporter = make_aggregator('reporter', depends_on={RiskDataPoint}, on_aggregate=fail_once)
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[scorer, reporter],
        seed=[work_email()],
        retry_policy=FAST_RETRY,
    ).run()

    assert not result.dead_letters  # it recovered on retry
    assert _aggregator_runs(await runtime.audit.replay(SID), OperatorId('reporter')) == [
        OperatorOutcome.FAILED,
        OperatorOutcome.SUCCEEDED,
    ]


async def test_agg_14_audits_dead_letter_after_exhausting_retries(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)

    async def always_fail(ctx: OperatorContext) -> None:  # noqa: ARG001
        raise OptimisticConcurrencyError('version conflict')

    scorer = make_operator('scorer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    reporter = make_aggregator('reporter', depends_on={RiskDataPoint}, on_aggregate=always_fail)
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[scorer, reporter],
        seed=[work_email()],
        retry_policy=FAST_RETRY,
    ).run()

    assert {dead.operator_id for dead in result.dead_letters} == {OperatorId('reporter')}  # exhausted retries
    # Every bounded attempt is audited, then a terminal dead-letter disposition.
    assert _aggregator_runs(await runtime.audit.replay(SID), OperatorId('reporter')) == [
        OperatorOutcome.FAILED,
        OperatorOutcome.FAILED,
        OperatorOutcome.DEAD_LETTERED,
    ]


class _RenewCountingLock(InMemorySessionLock):
    """In-memory lock that counts lease renewals (to observe the per-attempt renew)."""

    def __init__(self, clock: FakeClock) -> None:
        super().__init__(clock)
        self.renews = 0

    async def renew(self, session_id: SessionId, *, epoch: Epoch) -> None:
        self.renews += 1
        await super().renew(session_id, epoch=epoch)


class _FenceAfterRenewsLock(InMemorySessionLock):
    """In-memory lock that fences after N successful renews (models a takeover mid-aggregation)."""

    def __init__(self, clock: FakeClock, *, fence_after: int) -> None:
        super().__init__(clock)
        self._remaining = fence_after

    async def renew(self, session_id: SessionId, *, epoch: Epoch) -> None:
        if self._remaining == 0:
            raise StaleEpochError('a higher epoch took over')
        self._remaining -= 1
        await super().renew(session_id, epoch=epoch)


async def test_agg_aggregators_run_concurrently_so_a_retrying_peer_does_not_delay_completion(
    fake_clock: FakeClock,
) -> None:
    # Aggregators are independent: the healthy one must finish while the failing one is still working
    # through its retry backoff — sequential execution would order it strictly after the last attempt.
    runtime = build_in_memory_runtime(fake_clock)
    order: list[str] = []

    async def boom(ctx: OperatorContext) -> None:  # noqa: ARG001
        order.append('failing-attempt')
        raise ValueError('cannot aggregate')

    async def healthy(ctx: OperatorContext) -> None:
        assert ctx.aggregation is not None
        await ctx.aggregation.upsert('reports', 'healthy', {'ok': True})
        order.append('healthy-done')

    scorer = make_operator('s', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    failing = make_aggregator('failing', depends_on={RiskDataPoint}, on_aggregate=boom)
    good = make_aggregator('healthy', depends_on={RiskDataPoint}, on_aggregate=healthy)
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[scorer, failing, good],
        seed=[work_email()],
        retry_policy=RetryPolicy(max_attempts=2, base_delay=5.0, jitter=0.0),  # a real backoff window
    ).run()

    assert result.status is SessionStatus.COMPLETED
    assert {dead.operator_id for dead in result.dead_letters} == {OperatorId('failing')}
    assert await runtime.durable.read('reports', 'healthy') is not None
    last_failing_attempt = max(index for index, step in enumerate(order) if step == 'failing-attempt')
    assert order.index('healthy-done') < last_failing_attempt  # finished while the peer was still retrying


async def test_agg_hung_aggregator_times_out_per_attempt_and_dead_letters(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)

    async def hangs(ctx: OperatorContext) -> None:  # noqa: ARG001
        await asyncio.Event().wait()  # a durable write that never returns

    scorer = make_operator('s', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    hung = make_aggregator('hung', depends_on={RiskDataPoint}, on_aggregate=hangs)
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[scorer, hung],
        seed=[work_email()],
        operation_timeout=0.02,  # bounds each aggregation attempt, exactly like the gathering bound
        retry_policy=FAST_RETRY,
    ).run()

    assert result.status is SessionStatus.COMPLETED  # the session was not hung forever
    assert {dead.operator_id for dead in result.dead_letters} == {OperatorId('hung')}
    assert _aggregator_runs(await runtime.audit.replay(SID), OperatorId('hung')) == [
        OperatorOutcome.FAILED,
        OperatorOutcome.FAILED,
        OperatorOutcome.DEAD_LETTERED,
    ]


async def test_agg_policy_timeout_bounds_aggregator_attempts(fake_clock: FakeClock) -> None:
    # The per-operator policy timeout (not just the orchestrator-wide default, which stays at its 30s
    # default here and never fires) is what cuts each attempt short.
    runtime = build_in_memory_runtime(fake_clock)

    async def hangs(ctx: OperatorContext) -> None:  # noqa: ARG001
        await asyncio.Event().wait()

    hung = make_aggregator('hung', depends_on={RiskDataPoint}, on_aggregate=hangs, timeout=timedelta(seconds=0.02))
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[hung],
        seed=[risk()],
        retry_policy=FAST_RETRY,
    ).run()

    assert result.status is SessionStatus.COMPLETED
    assert {dead.operator_id for dead in result.dead_letters} == {OperatorId('hung')}


async def test_agg_lease_is_renewed_between_retry_attempts(fake_clock: FakeClock) -> None:
    counting = _RenewCountingLock(fake_clock)
    runtime = replace(build_in_memory_runtime(fake_clock), lock=counting)
    renews_seen: list[int] = []

    async def flaky(ctx: OperatorContext) -> None:  # noqa: ARG001
        renews_seen.append(counting.renews)
        if len(renews_seen) < 3:
            raise OptimisticConcurrencyError('version conflict')

    reporter = make_aggregator('rep', depends_on={RiskDataPoint}, on_aggregate=flaky)
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[reporter],
        seed=[risk()],
        retry_policy=RetryPolicy(max_attempts=3, base_delay=0.0),
    ).run()

    assert result.status is SessionStatus.COMPLETED and not result.dead_letters
    # Each attempt observed exactly one more renewal than its predecessor: the lease was extended
    # between attempts, not just once at phase start.
    assert renews_seen[1] == renews_seen[0] + 1
    assert renews_seen[2] == renews_seen[1] + 1


async def test_agg_fenced_renew_between_attempts_stops_cleanly_as_superseded(fake_clock: FakeClock) -> None:
    # The phase-start renew succeeds; the per-attempt renew reveals a takeover. That must propagate
    # out of the retry loop (the hook runs outside the operation's try/except) into a clean
    # SUPERSEDED stop — never be retried as if the aggregator itself had failed.
    runtime = replace(build_in_memory_runtime(fake_clock), lock=_FenceAfterRenewsLock(fake_clock, fence_after=1))

    async def noop(ctx: OperatorContext) -> None:  # noqa: ARG001
        return None

    reporter = make_aggregator('rep', depends_on={RiskDataPoint}, on_aggregate=noop)
    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[reporter], seed=[risk()]
    ).run()

    assert result.status is SessionStatus.SUPERSEDED
    assert not result.dead_letters  # the takeover is not an aggregator failure


async def test_agg_never_ready_aggregator_is_audited_as_skipped(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    # RiskDataPoint is never produced, so the reporter never becomes ready: the session still
    # completes, but the unwritten output domain must be visible in the logs and the audit trail.
    reporter = make_aggregator('reporter', depends_on={RiskDataPoint})
    with capture_logs() as records:
        result = await Orchestrator(
            session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[reporter], seed=[work_email()]
        ).run()
    assert result.status is SessionStatus.COMPLETED
    assert _aggregator_runs(await runtime.audit.replay(SID), OperatorId('reporter')) == [OperatorOutcome.SKIPPED]
    skipped = next(record for record in records if record['extra'].get('operator_id') == OperatorId('reporter'))
    assert skipped['extra']['missing_data_points'] == ['RiskDataPoint']


async def test_aggregation_helpers_stamps_status_and_updated_at() -> None:
    durable = InMemoryDurableStore()
    clock = FakeClock()
    helper = AggregationHelpers(
        durable, session_id=SID, operator_id=OperatorId('profile'), epoch=Epoch(1), clock=clock, is_final=False
    )
    await helper.upsert('reports', 'k', {'score': 1})
    interim = await durable.read('reports', 'k')
    assert interim is not None and interim.document == {'score': 1} and interim.status == 'in_progress'
    assert interim.updated_at == clock.now()
    interim_stamp = interim.updated_at

    clock.advance(60.0)  # the finalize pass re-stamps updated_at, so it must move forward
    final_helper = AggregationHelpers(
        durable, session_id=SID, operator_id=OperatorId('profile'), epoch=Epoch(1), clock=clock, is_final=True
    )
    await final_helper.upsert('reports', 'k', {'score': 2})
    finalized = await durable.read('reports', 'k')
    assert finalized is not None and finalized.status == 'final'
    assert finalized.updated_at == clock.now() and finalized.updated_at != interim_stamp


def test_operator_context_is_final_defaults_false() -> None:
    from orcastork.datapoints import DataPointView
    from orcastork.operators.context import CapabilityView, InvocationDelta, OperatorContext
    from orcastork.operators.effects import EffectGuard

    delta = InvocationDelta(frozenset(), frozenset(), frozenset(), is_first_invocation=True)
    ctx = OperatorContext(
        session_id=SID,
        epoch=Epoch(1),
        store=DataPointView(()),
        capabilities=CapabilityView(),
        delta=delta,
        effects=EffectGuard.__new__(EffectGuard),
    )
    assert ctx.is_final is False


def test_aggregator_interim_refresh_defaults_false_and_is_settable() -> None:
    from orcastork.operators import Aggregator, OperatorPolicy

    class _Plain(Aggregator):
        operator_id = OperatorId('plain_agg')
        policy = OperatorPolicy(rerun_on_new_data=False)

        async def aggregate(self, ctx: OperatorContext) -> None: ...

    class _Live(Aggregator):
        operator_id = OperatorId('live_agg')
        policy = OperatorPolicy(rerun_on_new_data=True)
        interim_refresh = True

        async def aggregate(self, ctx: OperatorContext) -> None: ...

    assert _Plain.interim_refresh is False
    assert _Live.interim_refresh is True
