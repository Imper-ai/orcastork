"""Gathering-operator retry: loop-scheduled backoff relaunch of failed runs.

A gathering operator whose policy declares ``retry`` is relaunched by the gather loop on a
backoff window (the same due-time mechanics as a debounced rerun) instead of being abandoned
on its first failure. The loop owns the window — no task ever sleeps the backoff away in-line
— so lease renewal, the session deadline, and inbox draining stay live throughout, and
quiescence waits for an armed retry rather than breaking to aggregation.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from datetime import timedelta
from itertools import count

import pytest

from orcastork.aggregation import RetryPolicy, backoff_delays, run_with_retry
from orcastork.audit import AuditLogEntry, OperatorAuditInfo, OperatorOutcome
from orcastork.datapoints import DataPointEmission
from orcastork.exceptions import AggregatorDeadLetteredError, StaleEpochError
from orcastork.ids import NamespaceId, OperatorId, SessionId
from orcastork.operators import Operator, OperatorContext, OperatorPolicy
from orcastork.orchestrator import Orchestrator, SessionStatus
from orcastork.runtime import build_in_memory_runtime

from .doubles.clock import FakeClock
from .doubles.datapoints import EmailDataPoint, IpDataPoint, RiskDataPoint, ip, risk, work_email
from .doubles.logs import capture_logs
from .doubles.operators import make_aggregator, make_operator

SID = SessionId('retry-session')
NAMESPACE = NamespaceId('retry-namespace')


async def _no_sleep(_delay: float) -> None:  # noqa: ARG001
    return None


def _operator_runs(entries: tuple[AuditLogEntry, ...], operator_id: OperatorId) -> list[OperatorAuditInfo]:
    return [entry.operator for entry in entries if entry.operator is not None and entry.operator_id == operator_id]


async def test_retry_relaunches_failed_operator_until_success(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    attempts = count(1)

    def flaky(ctx: OperatorContext) -> Iterator[DataPointEmission]:  # noqa: ARG001
        yield RiskDataPoint.emit(0.5)  # emitted on every attempt — the keyed-merge makes the retry idempotent
        if next(attempts) < 3:
            raise ValueError('transient blip')

    operator = make_operator(
        'flaky',
        depends_on={EmailDataPoint},
        produces={RiskDataPoint},
        emit_factory=flaky,
        rerun_on_new_data=False,  # never reruns on data — the retry must relaunch it anyway
        retry=RetryPolicy(max_attempts=3, base_delay=0.0),
    )
    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[operator], seed=[work_email()]
    ).run()

    assert result.status is SessionStatus.COMPLETED
    assert result.operator_runs[OperatorId('flaky')] == 3  # two failures + the recovering attempt
    runs = _operator_runs(await runtime.audit.replay(SID), OperatorId('flaky'))
    assert [run.attempt for run in runs] == [1, 2, 3]  # every attempt is audited with its number
    assert [run.outcome for run in runs] == [
        OperatorOutcome.FAILED,
        OperatorOutcome.FAILED,
        OperatorOutcome.SUCCEEDED,
    ]
    # The failed attempts' emissions were merged and re-emitted idempotently: one identity, not three.
    assert len((await runtime.store.snapshot(SID)).of_type(RiskDataPoint)) == 1


async def test_no_retry_policy_keeps_single_failure_behavior(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    failer = make_operator('failer', depends_on={EmailDataPoint}, raise_error=ValueError('boom'))
    with capture_logs(level='ERROR') as records:
        result = await Orchestrator(
            session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[failer], seed=[work_email()]
        ).run()

    assert result.status is SessionStatus.COMPLETED
    assert result.operator_runs[OperatorId('failer')] == 1  # abandoned after one attempt, exactly as before
    runs = _operator_runs(await runtime.audit.replay(SID), OperatorId('failer'))
    assert [(run.outcome, run.attempt) for run in runs] == [(OperatorOutcome.FAILED, 1)]
    failure = next(record for record in records if record['extra'].get('operator_id') == OperatorId('failer'))
    assert failure['exception'] is not None  # still an ERROR with the traceback, not a retry WARNING


async def test_retry_waits_out_the_backoff_window(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    run_times: list[float] = []

    def record_then_recover(ctx: OperatorContext) -> list[RiskDataPoint]:  # noqa: ARG001
        run_times.append(fake_clock.monotonic())
        if len(run_times) == 1:
            raise ValueError('transient blip')
        return [risk()]

    operator = make_operator(
        'flaky',
        depends_on={EmailDataPoint},
        produces={RiskDataPoint},
        emit_factory=record_then_recover,
        retry=RetryPolicy(max_attempts=2, base_delay=8.0, jitter=0.0),  # deterministic 8s first backoff
    )
    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[operator], seed=[work_email()]
    ).run()

    assert result.status is SessionStatus.COMPLETED
    assert result.operator_runs[OperatorId('flaky')] == 2
    assert run_times[1] - run_times[0] >= 8.0  # the relaunch waited out the full backoff window


async def test_retry_exhaustion_proceeds_without_wedging(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    hopeless = make_operator(
        'hopeless',
        depends_on={EmailDataPoint},
        raise_error=ValueError('persistent failure'),
        retry=RetryPolicy(max_attempts=2, base_delay=0.0),
    )
    healthy = make_operator('healthy', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    with capture_logs() as records:
        result = await Orchestrator(
            session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[hopeless, healthy], seed=[work_email()]
        ).run()

    assert result.status is SessionStatus.COMPLETED  # no wedge
    assert result.operator_runs[OperatorId('hopeless')] == 2  # bounded by max_attempts
    runs = _operator_runs(await runtime.audit.replay(SID), OperatorId('hopeless'))
    assert [(run.outcome, run.attempt) for run in runs] == [
        (OperatorOutcome.FAILED, 1),
        (OperatorOutcome.FAILED, 2),
    ]
    assert len((await runtime.store.snapshot(SID)).of_type(RiskDataPoint)) == 1  # the scheduler proceeded
    exhausted = [
        record
        for record in records
        if record['extra'].get('operator_id') == OperatorId('hopeless') and 'no retry remaining' in record['message']
    ]
    assert len(exhausted) == 1 and exhausted[0]['level'].name == 'WARNING'


async def test_quiescence_waits_for_an_armed_retry_before_aggregating(fake_clock: FakeClock) -> None:
    # When the failure is consumed, nothing is running and nothing else is due: the loop must wait
    # out the armed retry (via the injected clock) rather than declare quiescence and aggregate.
    runtime = build_in_memory_runtime(fake_clock)
    attempts = count(1)
    seen: dict[str, int] = {}

    def flaky(ctx: OperatorContext) -> list[RiskDataPoint]:  # noqa: ARG001
        if next(attempts) == 1:
            raise ValueError('transient blip')
        return [risk()]

    async def aggregate(ctx: OperatorContext) -> None:
        seen['risk_count'] = len(ctx.store.of_type(RiskDataPoint))

    operator = make_operator(
        'flaky',
        depends_on={EmailDataPoint},
        produces={RiskDataPoint},
        emit_factory=flaky,
        retry=RetryPolicy(max_attempts=2, base_delay=5.0, jitter=0.0),
    )
    reporter = make_aggregator('rep', depends_on={RiskDataPoint}, on_aggregate=aggregate)
    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[operator, reporter], seed=[work_email()]
    ).run()

    assert result.status is SessionStatus.COMPLETED
    assert seen == {'risk_count': 1}  # aggregation only ran after the due retry produced its output
    assert fake_clock.monotonic() >= 5.0  # the loop genuinely waited out the backoff window


# --- run_with_retry: the per-attempt hook contract -----------------------------------


async def test_retry_failing_hook_on_a_successful_attempt_propagates_without_replay() -> None:
    # The hook runs OUTSIDE the operation's try/except and before the success return. A hook that
    # raises on the SUCCESS attempt (e.g. a per-attempt lease renew that finds the epoch fenced)
    # must propagate unchanged — not be swallowed, not be counted as an operation failure, and above
    # all not replay an already-successful operation (which would double-apply non-idempotent effects).
    operation_calls = count(1)

    async def succeeds_once() -> None:
        next(operation_calls)  # records the call; the operation itself never fails

    async def hook_fails_on_success(_attempt: int, error: Exception | None) -> None:  # noqa: ARG001
        if error is None:
            raise StaleEpochError('lease fenced on the success attempt')

    with pytest.raises(StaleEpochError):
        await run_with_retry(
            succeeds_once,
            policy=RetryPolicy(max_attempts=3, base_delay=0.0),
            seed=1,
            sleep=_no_sleep,
            on_attempt=hook_fails_on_success,
        )
    # The operation ran exactly once — a swallowed hook error would have replayed the success path.
    assert next(operation_calls) == 2  # one real call (count started at 1, advanced once)


async def test_retry_failing_hook_does_not_dead_letter() -> None:
    # A hook failure is the caller's concern, distinct from operation failure: it must not be
    # mistaken for an exhausted-retry dead-letter.
    async def always_succeeds() -> None:
        return None

    async def hook_always_raises(_attempt: int, _error: Exception | None) -> None:  # noqa: ARG001
        raise StaleEpochError('fenced')

    with pytest.raises(StaleEpochError):  # the hook's error, not AggregatorDeadLetteredError
        await run_with_retry(
            always_succeeds,
            policy=RetryPolicy(max_attempts=2, base_delay=0.0),
            seed=1,
            sleep=_no_sleep,
            on_attempt=hook_always_raises,
        )


# --- RetryPolicy.__post_init__: fail-fast validation ---------------------------------


@pytest.mark.parametrize(
    ('kwargs', 'field'),
    [
        ({'max_attempts': 0}, 'max_attempts'),
        ({'max_attempts': -1}, 'max_attempts'),
        ({'base_delay': -0.1}, 'base_delay'),
        ({'base_delay': -1.0}, 'base_delay'),
        ({'jitter': 1.5}, 'jitter'),
        ({'jitter': -0.1}, 'jitter'),
    ],
)
def test_retry_policy_rejects_misconfiguration(kwargs: dict[str, float], field: str) -> None:
    # A misconfigured policy must fail fast at construction rather than dead-lettering immediately
    # (max_attempts < 1 makes the loop run zero times) or producing a negative/degenerate schedule.
    with pytest.raises(ValueError, match=field):  # the message names the offending field
        RetryPolicy(**kwargs)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    'kwargs',
    [
        {'max_attempts': 1},  # the single-attempt boundary is valid
        {'base_delay': 0.0},  # an all-zero schedule is valid
        {'jitter': 0.0},  # un-jittered is valid
        {'jitter': 1.0},  # full jitter is the inclusive upper bound
    ],
)
def test_retry_policy_accepts_boundary_values(kwargs: dict[str, float]) -> None:
    RetryPolicy(**kwargs)  # type: ignore[arg-type]  # constructs without raising


# --- backoff_delays / run_with_retry: boundary schedules -----------------------------


def test_backoff_unjittered_is_exact_exponential() -> None:
    # jitter=0.0 must yield exactly the un-jittered base * 2**n schedule (no random perturbation).
    policy = RetryPolicy(max_attempts=4, base_delay=0.05, jitter=0.0)
    assert backoff_delays(policy, seed=1) == [0.05 * 2**n for n in range(4)]


def test_backoff_zero_base_is_all_zero_regardless_of_jitter() -> None:
    # base_delay=0.0 must yield all zeros even with jitter armed (0 * anything == 0).
    policy = RetryPolicy(max_attempts=5, base_delay=0.0, jitter=0.5)
    assert backoff_delays(policy, seed=1) == [0.0] * policy.max_attempts


async def test_single_attempt_dead_letters_on_first_failure_without_sleeping() -> None:
    # max_attempts=1: the `attempt < max_attempts - 1` guard is `0 < 0` == False, so a failing
    # operation dead-letters on the FIRST failure WITHOUT ever sleeping the backoff.
    slept: list[float] = []

    async def spy_sleep(delay: float) -> None:
        slept.append(delay)

    async def always_fails() -> None:
        raise ValueError('boom')

    with pytest.raises(AggregatorDeadLetteredError):
        await run_with_retry(always_fails, policy=RetryPolicy(max_attempts=1), seed=1, sleep=spy_sleep)
    assert slept == []  # no spurious sleep at the single-attempt boundary


# --- orchestrator interplay: retry x new-data, retry x cycle-breaker, retry x timeout -


async def test_armed_retry_relaunches_once_even_as_new_data_arrives(fake_clock: FakeClock) -> None:
    # A rerun_on_new_data operator fails attempt 1 and arms a retry. While the backoff window is
    # armed, a peer merges a new IpDataPoint it depends on. When the window comes due the operator
    # must relaunch EXACTLY once via the armed-retry path — the rerun/debounce path stands aside —
    # and the keyed-merge collapses the repeated emission to a single identity (no racing double).
    runtime = build_in_memory_runtime(fake_clock)
    attempts = count(1)

    def flaky(ctx: OperatorContext) -> Iterator[DataPointEmission]:  # noqa: ARG001
        yield RiskDataPoint.emit(0.5)  # emitted every attempt; keyed-merge makes the relaunch idempotent
        if next(attempts) == 1:
            raise ValueError('transient blip')

    # A peer turns the seed email into a fresh Ip (new relevant data) for the flaky operator.
    peer = make_operator('peer', depends_on={EmailDataPoint}, produces={IpDataPoint}, emits=[ip('203.0.113.9')])
    operator = make_operator(
        'flaky',
        depends_on={IpDataPoint},
        produces={RiskDataPoint},
        emit_factory=flaky,
        rerun_on_new_data=True,
        debounce=timedelta(0),
        retry=RetryPolicy(max_attempts=2, base_delay=5.0, jitter=0.0),  # deterministic armed window
    )
    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[peer, operator], seed=[work_email()]
    ).run()

    assert result.status is SessionStatus.COMPLETED
    assert result.operator_runs[OperatorId('flaky')] == 2  # exactly one relaunch, not a second racing one
    runs = _operator_runs(await runtime.audit.replay(SID), OperatorId('flaky'))
    assert [run.outcome for run in runs] == [OperatorOutcome.FAILED, OperatorOutcome.SUCCEEDED]
    assert len((await runtime.store.snapshot(SID)).of_type(RiskDataPoint)) == 1  # de-duplicated identity


async def test_tripped_breaker_makes_an_armed_retry_terminal_on_a_cycle(fake_clock: FakeClock) -> None:
    # A self-cycle operator with BOTH a retry policy (max_attempts > max_cycles) and a persistent
    # failure: its retried runs consume breaker budget; once the breaker trips (max_cycles), the
    # retry must become terminal even with attempts remaining — otherwise it ping-pongs forever.
    runtime = build_in_memory_runtime(fake_clock)

    def emit_then_fail(ctx: OperatorContext) -> Iterator[DataPointEmission]:  # noqa: ARG001
        yield IpDataPoint.emit('203.0.113.1')
        raise ValueError('persistent failure')

    self_cycle = make_operator(
        'cyclic',
        depends_on={IpDataPoint},
        produces={IpDataPoint},
        rerun_on_new_data=True,
        max_cycles=2,
        emit_factory=emit_then_fail,
        retry=RetryPolicy(max_attempts=5, base_delay=0.0),  # far more attempts than max_cycles
    )
    with capture_logs() as records:
        result = await Orchestrator(
            session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[self_cycle], seed=[ip('seed')]
        ).run()

    assert result.status is SessionStatus.COMPLETED  # bounded, not looping forever
    assert result.operator_runs[OperatorId('cyclic')] <= 2  # bounded by max_cycles, not max_attempts
    terminal = [
        record
        for record in records
        if record['extra'].get('operator_id') == OperatorId('cyclic') and 'no retry remaining' in record['message']
    ]
    assert terminal and terminal[-1]['level'].name == 'WARNING'  # logged terminal, not retry-scheduled


async def test_timeout_failure_arms_a_retry_that_relaunches(fake_clock: FakeClock) -> None:
    # A per-operator timeout that fires on attempt 1 must be treated exactly like an exception
    # failure: asyncio.wait_for raises TimeoutError, the fault boundary records it as a FAILED
    # attempt, and that arms a retry. Attempt 2 (fast this time) succeeds and persists its emission.
    # The sleep here is a REAL asyncio.sleep so the REAL wait_for timeout actually fires.
    runtime = build_in_memory_runtime(fake_clock)
    attempts = count(1)

    async def slow_then_fast(ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:  # noqa: ARG001
        yield RiskDataPoint.emit(0.7)
        if next(attempts) == 1:
            await asyncio.sleep(0.5)  # blows the 0.02s policy timeout on the first attempt only

    class _Timed(Operator):
        operator_id = OperatorId('timed')
        policy = OperatorPolicy(
            rerun_on_new_data=False,
            timeout=timedelta(seconds=0.02),
            retry=RetryPolicy(max_attempts=2, base_delay=0.0),
        )
        depends_on = frozenset({EmailDataPoint})
        produces = frozenset({RiskDataPoint})

        def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:
            return slow_then_fast(ctx)

    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[_Timed], seed=[work_email()]
    ).run()

    assert result.status is SessionStatus.COMPLETED
    assert result.operator_runs[OperatorId('timed')] == 2
    runs = _operator_runs(await runtime.audit.replay(SID), OperatorId('timed'))
    assert [(run.outcome, run.attempt) for run in runs] == [
        (OperatorOutcome.FAILED, 1),
        (OperatorOutcome.SUCCEEDED, 2),
    ]
    assert len((await runtime.store.snapshot(SID)).of_type(RiskDataPoint)) == 1  # the recovering emission persisted


# --- dead-lettered aggregator that wrote durable output: resume must re-drive idempotently --


async def test_dead_lettered_upsert_survives_then_resume_redrives_idempotently(fake_clock: FakeClock) -> None:
    # An aggregator upserts a durable doc on each attempt, then raises; retries exhaust → dead-letter.
    # mark_contribution runs only on a fully successful run, so the dead-lettered aggregator is NOT
    # marked — and its durable write survives the dead-letter. On resume it is re-launched (still
    # unmarked) and, because upsert re-reads the OCC version each attempt, the re-drive is idempotent:
    # the final document is correct, not double-counted or conflicting.
    runtime = build_in_memory_runtime(fake_clock)
    scorer = make_operator('s', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    # The same aggregator identity must be re-driven across both runs (the contribution marker keys on
    # it), so one class is reused; a counter makes session 1's attempts raise and session 2's succeed.
    raise_until = count(1)

    async def upsert_then_maybe_boom(ctx: OperatorContext) -> None:
        assert ctx.aggregation is not None
        # OCC upsert re-reads the current version each attempt, so re-running over a surviving doc cannot
        # double-count or conflict — the value is recomputed from the (unchanged) store snapshot.
        await ctx.aggregation.upsert('reports', 'x', {'risk_count': len(ctx.store.of_type(RiskDataPoint))})
        if next(raise_until) <= 2:  # session 1's two attempts raise → dead-letter; session 2 succeeds
            raise ValueError('boom after the durable write')

    aggregator = make_aggregator('agg', depends_on={RiskDataPoint}, on_aggregate=upsert_then_maybe_boom)
    first = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[scorer, aggregator],
        seed=[work_email()],
        retry_policy=RetryPolicy(max_attempts=2, base_delay=0.0),
    ).run()

    assert first.status is SessionStatus.COMPLETED
    assert {dead.operator_id for dead in first.dead_letters} == {OperatorId('agg')}
    assert not await runtime.durable.is_contribution_marked(SID, OperatorId('agg'))  # never marked
    survived = await runtime.durable.read('reports', 'x')
    assert survived is not None and survived.document == {'risk_count': 1}  # the durable write survived

    # Resume: the same (now unmarked) aggregator re-runs (not skipped); its OCC upsert re-reads the
    # surviving version — no version conflict — leaving the document correct.
    second = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[scorer, aggregator], seed=[work_email()]
    ).run()

    assert second.status is SessionStatus.COMPLETED
    assert not second.dead_letters  # the re-drive succeeded this time
    assert await runtime.durable.is_contribution_marked(SID, OperatorId('agg'))  # now marked
    final = await runtime.durable.read('reports', 'x')
    assert final is not None and final.document == {'risk_count': 1}  # idempotent: not double-counted


async def test_dead_lettered_add_to_set_resume_keeps_set_cardinality(fake_clock: FakeClock) -> None:
    # The add_to_set variant: each attempt adds the same set member then raises → dead-letter. Because
    # add_to_set is set-keyed (not an increment), the dead-lettered attempts plus the resume re-drive
    # must leave the set at exactly the intended cardinality, never a duplicated/double-counted value.
    runtime = build_in_memory_runtime(fake_clock)
    scorer = make_operator('s', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    raise_until = count(1)
    cardinality: dict[str, int] = {}

    async def add_then_maybe_boom(ctx: OperatorContext) -> None:
        assert ctx.aggregation is not None
        cardinality['size'] = await ctx.aggregation.add_to_set('profiles', 'profile', 'sessions', str(SID))
        if next(raise_until) <= 2:  # session 1's two attempts raise → dead-letter; session 2 succeeds
            raise ValueError('boom after the set-add')

    aggregator = make_aggregator('agg', depends_on={RiskDataPoint}, on_aggregate=add_then_maybe_boom)
    first = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[scorer, aggregator],
        seed=[work_email()],
        retry_policy=RetryPolicy(max_attempts=2, base_delay=0.0),
    ).run()
    assert {dead.operator_id for dead in first.dead_letters} == {OperatorId('agg')}

    await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[scorer, aggregator], seed=[work_email()]
    ).run()
    assert cardinality == {'size': 1}  # exactly the intended single member despite retry + dead-letter + resume
