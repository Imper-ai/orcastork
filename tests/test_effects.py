"""FX — the ``ctx.once`` effect guard: claim/commit/revert side effects across reruns, retries, resumes.

Emissions, archive writes, and aggregator outputs are all safe to repeat; an external side
effect (an OTP, an ITSM ticket) is not — these tests pin that ``async with ctx.once(key)``
fires such an effect exactly once per (operator, key) per session when attempts succeed,
re-runs it when the attempt that claimed it failed (the claim is reverted, so a retry is not
silently skipped), and applies an explicit recovery policy when a predecessor died mid-effect.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import timedelta
from itertools import count

import pytest

from orcastork.adapters.memory import InMemoryDataPointStore
from orcastork.aggregation import RetryPolicy
from orcastork.datapoints import DataPointEmission
from orcastork.exceptions import StaleEpochError
from orcastork.ids import Epoch, NamespaceId, OperatorId, SessionId
from orcastork.operators import EffectGuard, EffectRecovery, Operator, OperatorContext, OperatorPolicy
from orcastork.orchestrator import Orchestrator, SessionStatus
from orcastork.ports import EffectClaim
from orcastork.ports.datapoint_store import effect_pending_epoch, effect_pending_state
from orcastork.runtime import build_in_memory_runtime

from .doubles.clock import FakeClock
from .doubles.datapoints import EmailDataPoint, IpDataPoint, RiskDataPoint, ip, work_email
from .doubles.logs import capture_logs
from .doubles.operators import make_aggregator

SID = SessionId('fx-session')
NAMESPACE = NamespaceId('fx-namespace')
OP = OperatorId('fx-op')


def _guard(store: InMemoryDataPointStore, *, epoch: int = 1) -> EffectGuard:
    return EffectGuard(store, session_id=SID, operator_id=OP, epoch=Epoch(epoch))


async def test_fx_01_effect_fires_once_across_data_driven_reruns(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    fired: list[int] = []
    counter = count()

    class _OtpSender(Operator):
        operator_id = OperatorId('otp_sender')
        # A bounded self-cycle: each run's own emission re-triggers it, so it genuinely reruns.
        policy = OperatorPolicy(rerun_on_new_data=True, max_cycles=3, debounce=timedelta(0))
        depends_on = frozenset({IpDataPoint})
        produces = frozenset({IpDataPoint})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:
            async with ctx.once('send-otp') as acquired:
                if acquired:
                    fired.append(1)
            yield IpDataPoint.emit(f'ip-{next(counter)}')

    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[_OtpSender], seed=[ip('seed')]
    ).run()

    assert result.status is SessionStatus.COMPLETED
    assert result.operator_runs[OperatorId('otp_sender')] >= 2  # the operator really reran on new data
    assert len(fired) == 1  # but the OTP went out exactly once


async def test_fx_02_committed_effect_does_not_refire_on_crash_resume(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    fired: list[int] = []

    class _OtpSender(Operator):
        operator_id = OperatorId('otp_sender')
        policy = OperatorPolicy(rerun_on_new_data=False)
        depends_on = frozenset({EmailDataPoint})
        produces = frozenset({RiskDataPoint})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:
            async with ctx.once('send-otp') as acquired:
                if acquired:
                    fired.append(1)
            yield RiskDataPoint.emit(0.5)

    # The predecessor seeded the store and its operator ran the effect to completion (the durable
    # commit landed), then the pod died before the watermark write — a successor re-runs the
    # operator from scratch.
    epoch = await runtime.lock.acquire(SID)
    await runtime.store.write(SID, [work_email()], epoch=epoch)
    claim = await runtime.store.claim_effect(SID, 'otp_sender:send-otp', epoch=epoch, reclaim_stale=False)
    assert claim is EffectClaim.ACQUIRED
    await runtime.store.commit_effect(SID, 'otp_sender:send-otp', epoch=epoch)
    fake_clock.advance(31.0)  # the predecessor's lease expires

    result = await Orchestrator(session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[_OtpSender]).run()

    assert result.status is SessionStatus.COMPLETED
    assert result.operator_runs[OperatorId('otp_sender')] == 1  # the successor re-drove the operator
    assert fired == []  # but the committed claim stopped the OTP from re-firing


async def test_fx_03_same_effect_key_is_namespaced_per_operator(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    fired: list[str] = []

    class _FirstNotifier(Operator):
        operator_id = OperatorId('first_notifier')
        policy = OperatorPolicy(rerun_on_new_data=False)
        depends_on = frozenset({EmailDataPoint})
        produces = frozenset({IpDataPoint})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:
            async with ctx.once('notify') as acquired:
                if acquired:
                    fired.append('first')
            yield IpDataPoint.emit('203.0.113.7')

    class _SecondNotifier(Operator):
        operator_id = OperatorId('second_notifier')
        policy = OperatorPolicy(rerun_on_new_data=False)
        depends_on = frozenset({EmailDataPoint})
        produces = frozenset({RiskDataPoint})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:
            async with ctx.once('notify') as acquired:
                if acquired:
                    fired.append('second')
            yield RiskDataPoint.emit(0.5)

    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[_FirstNotifier, _SecondNotifier],
        seed=[work_email()],
    ).run()

    assert result.status is SessionStatus.COMPLETED
    assert sorted(fired) == ['first', 'second']  # the shared key never collided across operators


async def test_fx_04_aggregator_context_exposes_the_effect_guard(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    outcomes: list[bool] = []

    async def aggregate(ctx: OperatorContext) -> None:
        async with ctx.once('final-notification') as first:
            outcomes.append(first)
        async with ctx.once('final-notification') as second:
            outcomes.append(second)

    reporter = make_aggregator('rep', depends_on={EmailDataPoint}, on_aggregate=aggregate)
    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[reporter], seed=[work_email()]
    ).run()

    assert result.status is SessionStatus.COMPLETED
    assert outcomes == [True, False]  # claimed and committed once; the second claim in the same run deduped


async def test_fx_05_failed_effect_attempt_is_reverted_and_the_retry_refires(fake_clock: FakeClock) -> None:
    # THE scenario motivating claim/commit/revert: the third-party call raises on attempt 1, so the
    # operator attempt fails. A mark-before-run design would leave the mark in place and the
    # loop-scheduled retry would SKIP the effect ("at most once, possibly zero"); the revert on the
    # failing exit means the retry re-acquires and the effect actually happens exactly once overall.
    runtime = build_in_memory_runtime(fake_clock)
    attempts = count(1)
    acquisitions: list[bool] = []
    sends: list[int] = []

    class _OtpSender(Operator):
        operator_id = OperatorId('otp_sender')
        policy = OperatorPolicy(rerun_on_new_data=False, retry=RetryPolicy(max_attempts=2, base_delay=0.0))
        depends_on = frozenset({EmailDataPoint})
        produces = frozenset({RiskDataPoint})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:
            attempt = next(attempts)
            async with ctx.once('send-otp') as acquired:
                acquisitions.append(acquired)
                if acquired:
                    if attempt == 1:
                        raise ValueError('otp provider returned 503')  # the call failed: no OTP went out
                    sends.append(attempt)
            yield RiskDataPoint.emit(0.5)

    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[_OtpSender], seed=[work_email()]
    ).run()

    assert result.status is SessionStatus.COMPLETED
    assert result.operator_runs[OperatorId('otp_sender')] == 2  # the loop-scheduled retry re-drove the operator
    assert acquisitions == [True, True]  # the failed attempt's claim was reverted, so the retry re-acquired
    assert sends == [2]  # and the OTP went out exactly once overall — on the attempt that succeeded
    assert await runtime.store.get_effect_state(SID, 'otp_sender:send-otp') == 'committed'


async def test_fx_06_predecessor_mid_effect_crash_reruns_on_resume_by_default(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    fired: list[int] = []

    class _OtpSender(Operator):
        operator_id = OperatorId('otp_sender')
        policy = OperatorPolicy(rerun_on_new_data=False)
        depends_on = frozenset({EmailDataPoint})
        produces = frozenset({RiskDataPoint})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:
            async with ctx.once('send-otp') as acquired:
                if acquired:
                    fired.append(1)
            yield RiskDataPoint.emit(0.5)

    # The predecessor claimed the effect and died mid-call — whether the OTP went out is unknowable.
    epoch = await runtime.lock.acquire(SID)
    await runtime.store.write(SID, [work_email()], epoch=epoch)
    claim = await runtime.store.claim_effect(SID, 'otp_sender:send-otp', epoch=epoch, reclaim_stale=False)
    assert claim is EffectClaim.ACQUIRED
    fake_clock.advance(31.0)  # the predecessor's lease expires

    with capture_logs() as records:
        result = await Orchestrator(
            session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[_OtpSender]
        ).run()

    assert result.status is SessionStatus.COMPLETED
    assert fired == [1]  # the at-least-once default re-ran the unknown-outcome effect
    warning = next(record for record in records if record['extra'].get('effect_key') == 'otp_sender:send-otp')
    assert warning['level'].name == 'WARNING'
    assert warning['extra']['stale_epoch'] == int(epoch)  # the warning names the predecessor's epoch


async def test_fx_07_happy_path_commits_and_a_second_claim_dedupes() -> None:
    store = InMemoryDataPointStore()
    guard = _guard(store)
    fired: list[int] = []

    async with guard.once('send-otp') as acquired:
        assert acquired is True
        fired.append(1)
    assert await store.get_effect_state(SID, f'{OP}:send-otp') == 'committed'

    async with guard.once('send-otp') as acquired:
        assert acquired is False
    assert fired == [1]


async def test_fx_08_same_epoch_nested_claim_is_refused_and_does_not_resolve_the_outer() -> None:
    store = InMemoryDataPointStore()
    guard = _guard(store)

    async with guard.once('send-otp') as outer:
        assert outer is True
        async with guard.once('send-otp') as inner:
            assert inner is False  # a duplicate claim within this run must not re-run the effect
        # The non-owning inner exit neither committed nor reverted the outer claim.
        assert await store.get_effect_state(SID, f'{OP}:send-otp') == 'pending:1'
    assert await store.get_effect_state(SID, f'{OP}:send-otp') == 'committed'


async def test_fx_09_stale_pending_rerun_policy_reclaims_with_a_warning() -> None:
    store = InMemoryDataPointStore()
    await store.claim_effect(SID, f'{OP}:send-otp', epoch=Epoch(1), reclaim_stale=False)  # predecessor died mid-effect

    with capture_logs() as records:
        async with _guard(store, epoch=2).once('send-otp') as acquired:
            assert acquired is True
    assert await store.get_effect_state(SID, f'{OP}:send-otp') == 'committed'
    warning = next(record for record in records if record['extra'].get('effect_key') == f'{OP}:send-otp')
    assert warning['level'].name == 'WARNING'
    assert warning['extra']['stale_epoch'] == 1


async def test_fx_10_stale_pending_skip_policy_leaves_the_unknown_state_in_place() -> None:
    store = InMemoryDataPointStore()
    await store.claim_effect(SID, f'{OP}:send-otp', epoch=Epoch(1), reclaim_stale=False)

    async with _guard(store, epoch=2).once('send-otp', on_unknown=EffectRecovery.SKIP) as acquired:
        assert acquired is False
    # The stale mark survives, so a later resume sees the same unknown state — not a fabricated outcome.
    assert await store.get_effect_state(SID, f'{OP}:send-otp') == 'pending:1'


async def test_fx_11_committed_under_one_epoch_dedupes_under_the_next() -> None:
    store = InMemoryDataPointStore()
    async with _guard(store, epoch=1).once('send-otp') as acquired:
        assert acquired is True
    async with _guard(store, epoch=2).once('send-otp') as acquired:
        assert acquired is False  # the commit is durable across epochs — a resume never re-fires it


async def test_fx_12_exception_in_the_body_reverts_the_claim_for_a_same_epoch_retry() -> None:
    store = InMemoryDataPointStore()
    guard = _guard(store)

    with pytest.raises(ValueError, match='otp provider'):
        async with guard.once('send-otp') as acquired:
            assert acquired is True
            raise ValueError('otp provider returned 503')
    assert await store.get_effect_state(SID, f'{OP}:send-otp') is None  # reverted, not stuck pending

    async with guard.once('send-otp') as acquired:
        assert acquired is True  # the same-epoch retry owns the effect again


async def test_fx_13_task_cancellation_inside_the_body_reverts_the_claim() -> None:
    store = InMemoryDataPointStore()
    guard = _guard(store)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def attempt() -> None:
        async with guard.once('send-otp') as acquired:
            assert acquired is True
            entered.set()
            await release.wait()  # a real in-flight await for the cancellation to land in

    task = asyncio.create_task(attempt())
    await entered.wait()
    assert await store.get_effect_state(SID, f'{OP}:send-otp') == 'pending:1'
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert await store.get_effect_state(SID, f'{OP}:send-otp') is None  # reverted during the unwind
    async with guard.once('send-otp') as acquired:
        assert acquired is True  # a relaunched attempt owns the effect again


async def test_fx_14_cancellation_at_the_commit_boundary_still_commits_the_claim() -> None:
    # The body completed — the effect DID run — so a cancellation landing on the post-body commit
    # must not strand the claim as pending: a same-epoch retry would then SKIP an effect that ran.
    store = InMemoryDataPointStore()
    guard = _guard(store)
    body_finished = asyncio.Event()

    async def attempt() -> None:
        async with guard.once('send-otp') as acquired:
            assert acquired is True
            body_finished.set()

    task = asyncio.create_task(attempt())
    await body_finished.wait()
    task.cancel()  # lands on the shielded commit await — the first suspension after the body
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0)  # let the shielded commit run to completion

    assert await store.get_effect_state(SID, f'{OP}:send-otp') == 'committed'
    async with guard.once('send-otp') as acquired:
        assert acquired is False  # the committed effect never re-fires


def test_fx_15_effect_pending_epoch_treats_a_malformed_mark_as_unknown() -> None:
    # Corrupt persisted state must not take down claim/recovery — an unparseable owner is
    # reported exactly like a non-pending state.
    assert effect_pending_epoch('pending:not-an-int') is None
    assert effect_pending_epoch('pending:') is None
    assert effect_pending_epoch('committed') is None
    assert effect_pending_epoch(effect_pending_state(Epoch(7))) == Epoch(7)


async def test_fx_16_revert_failure_on_a_failing_exit_propagates_the_body_error_not_the_revert() -> None:
    # The rare double failure: the effect body raised AND the revert ITSELF raises (a higher epoch
    # superseded this run between claim and revert, so revert's fence raises StaleEpochError). The
    # guard must swallow the revert error, log the documented WARNING, and re-raise the ORIGINAL body
    # exception — not the revert error, which would mask the real failure from the retry machinery.
    class _RevertRaisesStore(InMemoryDataPointStore):
        async def revert_effect(self, session_id: SessionId, effect_key: str, *, epoch: Epoch) -> None:  # noqa: ARG002
            raise StaleEpochError('a higher epoch superseded this run')

    store = _RevertRaisesStore()
    guard = _guard(store)

    with capture_logs() as records, pytest.raises(ValueError, match='boom'):
        async with guard.once('send-otp') as acquired:
            assert acquired is True
            raise ValueError('boom')

    warning = next(record for record in records if record['extra'].get('effect_key') == f'{OP}:send-otp')
    assert warning['level'].name == 'WARNING'
    assert 'revert failed' in warning['message']
    # The mark stays pending under this epoch, so a same-epoch retry sees PENDING_SAME_EPOCH → skips.
    assert await store.get_effect_state(SID, f'{OP}:send-otp') == 'pending:1'
    async with guard.once('send-otp') as acquired:
        assert acquired is False  # prefer a possibly-skipped effect over a possibly-double-fired one


async def test_fx_17_commit_failure_on_a_clean_exit_does_not_turn_a_success_into_a_failure() -> None:
    # The effect DID run (clean body), but the commit ITSELF raises (a higher epoch superseded this
    # run). The guard must swallow it, log the WARNING, and let the `async with` exit NORMALLY — a
    # commit failure must not surface as an operator failure that would retry and re-fire the effect.
    class _CommitRaisesStore(InMemoryDataPointStore):
        async def commit_effect(self, session_id: SessionId, effect_key: str, *, epoch: Epoch) -> None:  # noqa: ARG002
            raise StaleEpochError('a higher epoch superseded this run')

    store = _CommitRaisesStore()
    guard = _guard(store)
    fired: list[int] = []

    with capture_logs() as records:
        async with guard.once('send-otp') as acquired:
            assert acquired is True
            fired.append(1)  # the effect ran to completion; the body did not raise

    assert fired == [1]  # the block exited without raising despite the commit failure
    warning = next(record for record in records if record['extra'].get('effect_key') == f'{OP}:send-otp')
    assert warning['level'].name == 'WARNING'
    assert 'commit failed' in warning['message']
    # The claim is stranded as pending by design — not committed, not deleted.
    assert await store.get_effect_state(SID, f'{OP}:send-otp') == 'pending:1'


async def test_fx_18_exception_in_a_non_owning_body_leaves_the_owners_claim_untouched() -> None:
    # A non-owning attempt (acquired False) does `yield False; return` and never enters the
    # commit/revert block. If its body raises, the exception must propagate WITHOUT the guard
    # touching the owner's mark — only the attempt that ACQUIRED may resolve the claim.
    store = InMemoryDataPointStore()
    guard = _guard(store)

    async with guard.once('send-otp') as owner:
        assert owner is True
        with pytest.raises(ValueError, match='unrelated'):
            async with guard.once('send-otp') as duplicate:
                assert duplicate is False  # a same-epoch duplicate within this run
                raise ValueError('unrelated work in the duplicate block failed')
        # The non-owner's failure neither committed nor reverted the still-open owner claim.
        assert await store.get_effect_state(SID, f'{OP}:send-otp') == 'pending:1'
    # The owner exited cleanly afterward and committed normally.
    assert await store.get_effect_state(SID, f'{OP}:send-otp') == 'committed'


async def test_fx_19_rerun_reclaim_losing_the_race_to_a_commit_yields_not_acquired() -> None:
    # Under RERUN the guard sees PENDING_STALE_EPOCH and reclaims with reclaim_stale=True. The
    # documented race: a concurrent commit landed between the stale observation and the reclaim, so
    # the reclaim returns ALREADY_COMMITTED — anything but ACQUIRED means the effect must NOT run.
    class _CommitDuringReclaimStore(InMemoryDataPointStore):
        async def claim_effect(
            self, session_id: SessionId, effect_key: str, *, epoch: Epoch, reclaim_stale: bool
        ) -> EffectClaim:
            if reclaim_stale:
                # The predecessor (or another writer) committed the effect just before the reclaim.
                state = self._state(session_id)
                state.effects[effect_key] = 'committed'
                return EffectClaim.ALREADY_COMMITTED
            return await super().claim_effect(session_id, effect_key, epoch=epoch, reclaim_stale=reclaim_stale)

    store = _CommitDuringReclaimStore()
    await store.claim_effect(SID, f'{OP}:send-otp', epoch=Epoch(1), reclaim_stale=False)  # predecessor died mid-effect
    fired: list[int] = []

    async with _guard(store, epoch=2).once('send-otp') as acquired:
        assert acquired is False  # the reclaim lost the race to a commit — this attempt must not fire
        if acquired:
            fired.append(1)

    assert fired == []  # the effect body did not run
    assert await store.get_effect_state(SID, f'{OP}:send-otp') == 'committed'  # the committed state stands
