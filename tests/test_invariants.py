"""INV — cross-cutting invariants, as parametrized example tables over adversarial inputs.

No property-based testing library is used; each invariant is exercised over a small
hand-picked set of sequences/interleavings.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import replace

import pytest

from orcastork.adapters.memory import (
    InMemoryAuditSink,
    InMemoryDataPointStore,
    InMemoryDurableStore,
    InMemoryInbox,
)
from orcastork.aggregation import RetryPolicy
from orcastork.audit import AuditKind, AuditLogEntry
from orcastork.capabilities import compute_available
from orcastork.datapoints import DataPointEmission
from orcastork.exceptions import StaleEpochError
from orcastork.ids import CapabilityId, Epoch, NamespaceId, OperatorId, SessionId
from orcastork.operators import Operator, OperatorContext, OperatorPolicy
from orcastork.orchestrator import Orchestrator
from orcastork.orchestrator.orchestrator import SessionStatus
from orcastork.runtime import build_in_memory_runtime

from .doubles.capabilities import make_capability
from .doubles.clock import FakeClock
from .doubles.datapoints import (
    ChatAnswerDataPoint,
    EmailDataPoint,
    GeoDataPoint,
    IpDataPoint,
    RiskDataPoint,
    WorkEmailDataPoint,
    chat_answer,
    ip,
    personal_email,
    risk,
    work_email,
)
from .doubles.logs import capture_logs
from .doubles.operators import make_aggregator, make_operator
from .doubles.otel import TelemetryProbe

SID = SessionId('inv-session')
NAMESPACE = NamespaceId('inv-namespace')


@pytest.mark.parametrize('order', [[0, 1, 2], [2, 1, 0], [1, 0, 2], [0, 0, 1, 2, 1], [2, 2, 2, 0, 1]])
async def test_inv_01_dedup_idempotent_under_reordering(order: list[int]) -> None:
    points = [work_email('a@e.example'), personal_email('p@e.example'), ip('203.0.113.1')]
    store = InMemoryDataPointStore()
    for index in order:
        await store.write(SID, [points[index]], epoch=Epoch(1))
    identities = {(dp.type, dp.value) for dp in (await store.snapshot(SID)).all()}
    assert identities == {('work_email', 'a@e.example'), ('personal_email', 'p@e.example'), ('ip', '203.0.113.1')}


@pytest.mark.parametrize('deliveries', [1, 2, 5])
async def test_inv_02_at_least_once_delivery_does_not_change_output(deliveries: int, fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)

    async def write(ctx: OperatorContext) -> None:
        assert ctx.aggregation is not None
        await ctx.aggregation.upsert('reports', 'report', {'answers': len(ctx.store.of_type(ChatAnswerDataPoint))})

    for _ in range(deliveries):  # the same user action delivered N times
        await runtime.inbox.append(SID, chat_answer('same-answer'))
    reporter = make_aggregator('rep', depends_on={ChatAnswerDataPoint}, on_aggregate=write)
    await Orchestrator(session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[reporter]).run()
    document = await runtime.durable.read('reports', 'report')
    assert document is not None and document.document == {'answers': 1}  # deliveries collapse to one identity


@pytest.mark.parametrize('interleaving', [[1, 2, 1], [2, 1], [1, 1, 2, 1], [2, 2, 1]])
async def test_inv_03_fencing_rejects_superseded_epoch(interleaving: list[int]) -> None:
    store = InMemoryDataPointStore()
    highest = 0
    for position, epoch in enumerate(interleaving):
        if epoch < highest:
            with pytest.raises(StaleEpochError):  # a write from a superseded epoch never lands
                await store.write(SID, [ip(f'{position}')], epoch=Epoch(epoch))
        else:
            await store.write(SID, [ip(f'{position}')], epoch=Epoch(epoch))
            highest = epoch


@pytest.mark.parametrize(
    'additions',
    [
        [WorkEmailDataPoint],
        [IpDataPoint, WorkEmailDataPoint],
        [WorkEmailDataPoint, IpDataPoint, GeoDataPoint],
        [GeoDataPoint, GeoDataPoint, WorkEmailDataPoint],
    ],
)
def test_inv_04_availability_is_monotonic(additions: list[type]) -> None:
    email_cap = make_capability('email_cap', depends_on={WorkEmailDataPoint})
    ip_cap = make_capability('ip_cap', depends_on={IpDataPoint})
    registered = {email_cap.capability_id: email_cap, ip_cap.capability_id: ip_cap}
    permitted = frozenset({CapabilityId('email_cap'), CapabilityId('ip_cap')})

    present: set[type] = set()
    previous: frozenset[CapabilityId] = frozenset()
    for added in additions:
        present.add(added)
        available = compute_available(registered=registered, permitted=permitted, present_types=frozenset(present))
        assert previous <= available  # adding DataPoints only ever adds capabilities
        previous = available


@pytest.mark.parametrize('count', [1, 3, 5])
async def test_inv_05_revision_strictly_increases(count: int) -> None:
    store = InMemoryDataPointStore()
    last = 0
    for index in range(count):
        revision = await store.write(SID, [ip(f'203.0.113.{index}')], epoch=Epoch(1))
        assert revision > last
        last = revision


@pytest.mark.parametrize('extra_arrivals', [0, 1, 3])
async def test_inv_06_running_operator_is_never_cancelled(extra_arrivals: int, fake_clock: FakeClock) -> None:

    runtime = build_in_memory_runtime(fake_clock)

    def emit_three(ctx: OperatorContext) -> list[IpDataPoint]:  # noqa: ARG001
        return [ip('a'), ip('b'), ip('c')]

    operator = make_operator('multi', depends_on={EmailDataPoint}, produces={IpDataPoint}, emit_factory=emit_three)
    for index in range(extra_arrivals):  # new data arriving must not cancel the running operator
        await runtime.inbox.append(SID, chat_answer(f'arrival-{index}'))
    await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[operator], seed=[work_email()]
    ).run()
    emitted = {dp.value for dp in (await runtime.store.snapshot(SID)).of_type(IpDataPoint)}
    assert emitted == {'a', 'b', 'c'}  # every emission landed → the operator ran to completion


@pytest.mark.parametrize('flow', ['simple', 'failing_aggregator'])
async def test_inv_07_session_always_completes(flow: str, fake_clock: FakeClock) -> None:

    runtime = build_in_memory_runtime(fake_clock)
    if flow == 'simple':
        operators: list[type] = [
            make_operator('s', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
        ]
    else:

        async def boom(ctx: OperatorContext) -> None:  # noqa: ARG001
            raise ValueError('permanently failing')

        operators = [make_aggregator('failing', depends_on={EmailDataPoint}, on_aggregate=boom)]

    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=operators,
        seed=[work_email()],
        retry_policy=RetryPolicy(max_attempts=2, base_delay=0.0),
    ).run()
    assert result.status is SessionStatus.COMPLETED  # never wedges


async def test_inv_08_every_write_path_honors_the_epoch() -> None:
    # Store CAS
    store = InMemoryDataPointStore()
    await store.write(SID, [work_email()], epoch=Epoch(2))
    with pytest.raises(StaleEpochError):
        await store.write(SID, [ip()], epoch=Epoch(1))

    # Durable OCC upsert
    durable = InMemoryDurableStore()
    await durable.upsert('docs', 'k', {}, expected_version=0, epoch=Epoch(2))
    with pytest.raises(StaleEpochError):
        await durable.upsert('docs', 'k', {'x': 1}, expected_version=1, epoch=Epoch(1))

    # Audit append
    audit = InMemoryAuditSink()
    await audit.append(
        AuditLogEntry(
            session_id=SID, epoch=Epoch(2), timestamp=work_email().first_retrieved, kind=AuditKind.DATA_POINT_ADDED
        )
    )
    with pytest.raises(StaleEpochError):
        await audit.append(
            AuditLogEntry(
                session_id=SID, epoch=Epoch(1), timestamp=work_email().first_retrieved, kind=AuditKind.DATA_POINT_ADDED
            )
        )

    # Inbox ack
    inbox = InMemoryInbox()
    first = await inbox.append(SID, work_email())
    second = await inbox.append(SID, personal_email())
    await inbox.consume(SID)
    await inbox.ack(SID, first, epoch=Epoch(2))
    with pytest.raises(StaleEpochError):
        await inbox.ack(SID, second, epoch=Epoch(1))


class _TieInbox(InMemoryInbox):
    """Inbox whose FIRST ``wait_for_entry`` makes an entry land in the SAME pass the park window elapses.

    The orchestrator's first inbox wait creates this waiter; when it runs it appends an entry to
    itself and returns at once (entries now pending). Meanwhile the wait's park-bounded timer
    fast-forwards the fake clock to exactly ``park_at``. Both complete in the same pass, so the
    NEXT ``while not waiter.done()`` check sees a done waiter and resolves the tie as 'data beats
    parking' — the wakeup wins over the just-elapsed park window.
    """

    def __init__(self) -> None:
        super().__init__()
        self._tied = False

    async def wait_for_entry(self, session_id: SessionId) -> None:
        if not self._tied:
            self._tied = True
            await self.append(session_id, chat_answer('arrived-at-the-tie'))  # an entry lands as the wait begins
        await super().wait_for_entry(session_id)


async def test_inv_09_inbox_wakeup_at_the_park_tie_resumes_instead_of_parking(fake_clock: FakeClock) -> None:
    # The exact tie: an inbox entry is pending in the same pass that `now >= park_at`. The loop
    # checks `waiter.done()` first, so the wakeup wins — the session must resume and process the
    # entry rather than PARK a session that actually had input ready. park_after is kept under the
    # lease renew interval so the clock fast-forward to park_at does not trip a renew fence.
    probe = TelemetryProbe()
    park_after = 5.0
    runtime = replace(build_in_memory_runtime(fake_clock), inbox=_TieInbox(), telemetry=probe.telemetry)
    scorer = make_operator('scorer', depends_on={ChatAnswerDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[scorer],
        completes_when=RiskDataPoint,  # only the inbox answer can satisfy this
        session_deadline=300.0,
        park_after=park_after,
    ).run()

    assert result.status is SessionStatus.COMPLETED  # resumed on the wakeup, NOT parked
    assert result.operator_runs.get(OperatorId('scorer')) == 1  # the tie-arriving entry was processed
    assert {dp.value for dp in (await runtime.store.snapshot(SID)).of_type(ChatAnswerDataPoint)} == {
        'arrived-at-the-tie'
    }
    assert fake_clock.monotonic() >= park_after  # the park window genuinely elapsed — a real tie, not an early wakeup
    wait_spans = [span for span in probe.spans('session.inbox_wait')]
    assert wait_spans and wait_spans[-1].attributes is not None
    assert wait_spans[-1].attributes.get('outcome') == 'wakeup'  # the tie resolved as a wakeup, not 'parked'


class _FenceOnRevertEffectStore(InMemoryDataPointStore):
    """Store whose ``revert_effect`` is fenced — a takeover lands before the failing body's revert."""

    async def revert_effect(self, session_id: SessionId, effect_key: str, *, epoch: Epoch) -> None:  # noqa: ARG002
        raise StaleEpochError('a higher epoch took over before the effect revert landed')


async def test_inv_10_effect_revert_fenced_on_unwind_degrades_and_preserves_original_failure(
    fake_clock: FakeClock,
) -> None:
    # ctx.once body raises, so EffectGuard reverts the claim during the exception unwind — but the
    # revert is fenced by a takeover. EffectGuard shields and ABSORBS the failed revert (a logged
    # warning, leaving the mark pending for the successor's recovery policy) and RE-RAISES the
    # ORIGINAL body exception — the cleanup-path failure under fencing must not mask the real error
    # nor crash the operator task with a secondary exception. The operator's failure is isolated, so
    # the session still reaches its normal disposition.
    runtime = replace(build_in_memory_runtime(fake_clock), store=_FenceOnRevertEffectStore())
    seen_acquired: dict[str, bool] = {}

    class _FailingSender(Operator):
        operator_id = OperatorId('failing_sender')
        policy = OperatorPolicy(rerun_on_new_data=False)
        depends_on = frozenset({EmailDataPoint})
        produces = frozenset({RiskDataPoint})

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:
            async with ctx.once('send-otp') as acquired:
                seen_acquired['value'] = acquired
                if acquired:
                    raise ValueError('effect body blew up')  # forces the revert on the unwind
                yield DataPointEmission(RiskDataPoint, 0.1)  # unreachable; keeps this an async generator

    with capture_logs(level='WARNING') as records:
        result = await Orchestrator(
            session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[_FailingSender], seed=[work_email()]
        ).run()

    assert result.status is SessionStatus.COMPLETED  # the operator's failure (and the fenced revert) are isolated
    assert seen_acquired['value'] is True  # the claim was acquired, so the revert path was taken on the raise
    # The fenced revert is absorbed (logged), leaving the claim as this epoch's pending mark for the
    # successor's RERUN/SKIP recovery — never deleted under a stale epoch, never a secondary crash.
    assert await runtime.store.get_effect_state(SID, 'failing_sender:send-otp') == 'pending:1'
    assert any('revert failed' in r['message'] for r in records)  # degraded, not raised as a new error
    # The ORIGINAL body failure is what was logged as the operator failure (its emissions persisted path).
    assert any(r['extra'].get('operator_id') == OperatorId('failing_sender') for r in records)


def _slow_durable_aggregator(
    op_id: str, *, table: str, key: str, dead_letter: bool, clock: FakeClock
) -> type[Operator]:
    """A representative aggregator whose durable write straddles an injected await (a read→write gap).

    Half the cohort raises (dead-letters); half writes a durable doc. The injected ``clock.sleep``
    between observing and writing forces a real interleaving of the concurrently-gathered
    aggregator coroutines, stressing the 'single loop, no locking' claim for shared orchestrator
    state (``self._dead_letters`` / ``self._runs``).
    """

    async def _aggregate(ctx: OperatorContext) -> None:
        await clock.sleep(0.0)  # yields control — peers' mutations can interleave here
        if dead_letter:
            raise ValueError(f'{op_id} cannot aggregate')
        assert ctx.aggregation is not None
        await ctx.aggregation.upsert(table, key, {'op': op_id})

    return make_aggregator(op_id, depends_on={RiskDataPoint}, on_aggregate=_aggregate)


async def test_inv_11_concurrent_aggregators_keep_exact_dead_letter_and_run_counts(fake_clock: FakeClock) -> None:
    # The 'single event loop, no locking' claim for shared orchestrator state, stress-asserted: 20
    # aggregators run concurrently (each yielding mid-aggregate so they genuinely interleave), half
    # dead-lettering and half succeeding. Every mutation of self._dead_letters / self._runs is one
    # non-awaiting step, so NO interleaving may drop a dead-letter or a run count.
    runtime = build_in_memory_runtime(fake_clock)
    scorer = make_operator('scorer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    aggregators: list[type[Operator]] = [
        _slow_durable_aggregator(
            f'agg-{index}', table='reports', key=f'doc-{index}', dead_letter=(index % 2 == 0), clock=fake_clock
        )
        for index in range(20)
    ]
    result = await Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[scorer, *aggregators],
        seed=[work_email()],
        retry_policy=RetryPolicy(max_attempts=1, base_delay=0.0),  # one shot: a failure dead-letters at once
    ).run()

    assert result.status is SessionStatus.COMPLETED
    dead_ids = {dead.operator_id for dead in result.dead_letters}
    assert dead_ids == {OperatorId(f'agg-{index}') for index in range(20) if index % 2 == 0}  # exactly the 10
    assert len(result.dead_letters) == 10  # no dead-letter dropped or double-counted under interleaving
    succeeded = {index for index in range(20) if index % 2 == 1}
    assert {result.operator_runs.get(OperatorId(f'agg-{index}'), 0) for index in succeeded} == {1}  # each ran once
    for index in succeeded:
        document = await runtime.durable.read('reports', f'doc-{index}')
        assert document is not None and document.document == {'op': f'agg-{index}'}  # every success persisted
