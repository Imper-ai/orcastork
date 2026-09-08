"""A re-observed DataPoint identity costs one counted row, not a row per sighting.

Operators that rerun on new data re-emit their whole output every pass, and a converter can run
hundreds of times in one session, so the same unchanged value was recorded hundreds of times. On a real
dev2 session that was ~18k of ~24.5k audit rows. The value is already in the store, its latest sighting
is on the DataPoint, and the count is reported once — what a per-row trail adds is the timing of each
individual re-sighting.
"""

from orcastork.audit import AuditKind
from orcastork.ids import NamespaceId, SessionId
from orcastork.orchestrator.orchestrator import Orchestrator, SessionStatus
from orcastork.runtime import build_in_memory_runtime
from tests.doubles.clock import FakeClock
from tests.doubles.datapoints import ChatAnswerDataPoint, EmailDataPoint, work_email
from tests.doubles.operators import make_operator

SID = SessionId('reobs-session')
NAMESPACE = NamespaceId('reobs-namespace')


async def test_a_re_observed_identity_is_counted_not_re_rowed(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    # The same (type, value) emitted three times: one ADDED, two re-observations.
    emitter = make_operator(
        'reobs_emitter', produces={EmailDataPoint}, emits=[work_email(), work_email(), work_email()]
    )
    orchestrator = Orchestrator(session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[emitter])

    await orchestrator.run()

    trail = await runtime.audit.replay(SID)
    added = [e for e in trail if e.kind is AuditKind.DATA_POINT_ADDED]
    coalesced = [e for e in trail if e.kind is AuditKind.DATA_POINTS_COALESCED]

    assert len(added) == 1, f'expected one row for the new identity, got {len(added)}'
    assert len(coalesced) == 1, f'expected one counted row for the re-observations, got {len(coalesced)}'
    assert coalesced[0].data_point is not None
    assert coalesced[0].data_point.summary == '2 re-observed'


async def test_distinct_values_each_still_get_their_own_row(fake_clock: FakeClock) -> None:
    # Coalescing must not hide real change: three different values are three identities, not one.
    runtime = build_in_memory_runtime(fake_clock)
    emitter = make_operator(
        'distinct_emitter',
        produces={EmailDataPoint},
        emits=[work_email('a@e.example'), work_email('b@e.example'), work_email('c@e.example')],
    )
    orchestrator = Orchestrator(session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[emitter])

    await orchestrator.run()

    trail = await runtime.audit.replay(SID)
    added = [e for e in trail if e.kind is AuditKind.DATA_POINT_ADDED]

    assert len(added) == 3, f'each distinct value must keep its own row, got {len(added)}'
    assert not [e for e in trail if e.kind is AuditKind.DATA_POINTS_COALESCED]


async def test_a_parked_session_commits_its_counts_before_it_releases(fake_clock: FakeClock) -> None:
    """Parking releases cleanly, so the counts have to reach the trail exactly as a completion's do.

    The counters live only on the parked instance and the resume that follows starts them at zero, so
    counts skipped at the park are gone from the committed trail for good — the session's merges would
    then be unaccounted for even though every DataPoint behind them is durable.
    """
    runtime = build_in_memory_runtime(fake_clock)
    emitter = make_operator(
        'park_emitter', produces={EmailDataPoint}, emits=[work_email(), work_email(), work_email()]
    )
    orchestrator = Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[emitter],
        completes_when=ChatAnswerDataPoint,  # never arrives — the session parks instead of completing
        session_deadline=300.0,
        park_after=30.0,
    )

    result = await orchestrator.run()

    trail = await runtime.audit.replay(SID)
    coalesced = [e for e in trail if e.kind is AuditKind.DATA_POINTS_COALESCED]

    assert result.status is SessionStatus.PARKED
    assert len(coalesced) == 1, f'the park dropped the coalesced counts, got {len(coalesced)} rows'
    assert coalesced[0].data_point is not None
    assert coalesced[0].data_point.summary == '2 re-observed'
