"""End-to-end sessions on the in-memory runtime, driven by ``FakeClock``."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest
from loguru import logger

from orcastork_lite import (
    CapabilityActivated,
    DataPoint,
    DataPointMerged,
    DuplicateIdError,
    InMemoryCapabilityCatalog,
    InvalidOperatorError,
    NamespaceId,
    Operator,
    OperatorContext,
    OperatorId,
    OperatorPolicy,
    OperatorRunCompleted,
    Orchestrator,
    RerunOn,
    RetryPolicy,
    SessionCompleted,
    SessionEvent,
    UnboundedCycleError,
    build_runtime,
)
from orcastork_lite.adapters.memory import InMemorySessionEventSink
from orcastork_lite.ids import CapabilityId

from ..doubles.clock import FakeClock
from .conftest import (
    NAMESPACE,
    SESSION,
    Email,
    Flag,
    Ip,
    Risk,
    WorkEmail,
    dp,
    make_capability,
    make_operator,
    run_session,
)


def _values(result_points: Any, leaf: type[Any]) -> list[Any]:
    return sorted(point.value for point in result_points.of_type(leaf))


async def test_chain_runs_each_operator_once_in_data_order(fake_clock: FakeClock) -> None:
    order: list[str] = []

    def emitter(name: str, emission: Any) -> Any:
        def factory(_ctx: OperatorContext) -> list[Any]:
            order.append(name)
            return [emission]

        return factory

    a = make_operator('a', depends_on={Flag}, produces={Ip}, emit_factory=emitter('a', Ip.emit('1.1.1.1')))
    b = make_operator('b', depends_on={Ip}, produces={Risk}, emit_factory=emitter('b', Risk.emit(0.9)))
    c = make_operator('c', depends_on={Risk}, produces={WorkEmail}, emit_factory=emitter('c', WorkEmail.emit('w')))

    result = await run_session(fake_clock, [c, b, a], seed=[dp(Flag, True, fake_clock.now())])

    assert order == ['a', 'b', 'c']
    assert result.operator_runs == {'a': 1, 'b': 1, 'c': 1}
    assert result.failures == {}
    assert result.data_points.present_types() == {Flag, Ip, Risk, WorkEmail}
    (email,) = result.data_points.of_type(Email)
    assert email.retrieved_by == 'c' and email.first_retrieved == fake_clock.now()


async def test_never_ready_operator_never_runs_and_session_still_completes(fake_clock: FakeClock) -> None:
    orphan = make_operator('orphan', depends_on={Risk}, emits=[Ip.emit('x')])
    result = await run_session(fake_clock, [orphan], seed=[dp(Flag, True, fake_clock.now())])
    assert result.operator_runs == {} and len(result.data_points) == 1


async def test_rerun_on_new_data_reruns_with_only_the_new_delta_and_uses_triggers_it(fake_clock: FakeClock) -> None:
    seen: list[OperatorContext] = []
    producer = make_operator('producer', depends_on={Flag}, produces={Risk}, emits=[Risk.emit(0.5)])
    folder = make_operator('folder', depends_on={Flag}, uses={Risk}, rerun_on_new_data=True, seen=seen)
    once = make_operator('once', depends_on={Flag}, uses={Risk}, rerun_on_new_data=False)

    result = await run_session(fake_clock, [producer, folder, once], seed=[dp(Flag, True, fake_clock.now())])

    assert result.operator_runs == {'producer': 1, 'folder': 2, 'once': 1}
    first, second = seen
    assert first.delta.is_first_invocation and first.delta.added == {dp(Flag, True, fake_clock.now())}
    assert not second.delta.is_first_invocation
    assert second.delta.added == {dp(Risk, 0.5, fake_clock.now())} and not second.delta.updated
    assert second.store.of_type(Risk)  # the rerun sees the store that triggered it


async def test_reruns_coalesce_within_the_debounce_window(fake_clock: FakeClock) -> None:
    seen: list[OperatorContext] = []
    first_hop = make_operator('first_hop', depends_on={Flag}, produces={Ip}, emits=[Ip.emit('1.1.1.1')])
    second_hop = make_operator('second_hop', depends_on={Ip}, produces={Risk}, emits=[Risk.emit(0.1)])
    folder = make_operator(
        'folder', depends_on={Flag}, uses={Ip, Risk}, rerun_on_new_data=True, debounce=timedelta(seconds=5), seen=seen
    )

    result = await run_session(fake_clock, [first_hop, second_hop, folder], seed=[dp(Flag, True, fake_clock.now())])

    # First run at seed; the Ip arrival arms a 5s window, the Risk arrival lands inside it, one rerun folds both.
    assert result.operator_runs[OperatorId('folder')] == 2
    assert seen[1].delta.added == {dp(Ip, '1.1.1.1', fake_clock.now()), dp(Risk, 0.1, fake_clock.now())}
    assert fake_clock.monotonic() == 5.0  # the window was waited out on the injected clock, not abandoned


@pytest.mark.parametrize(('rerun_on', 'expected_runs'), [(RerunOn.ADDED_OR_UPDATED, 2), (RerunOn.ADDED_ONLY, 1)])
async def test_freshness_only_reobservation_reruns_only_under_added_or_updated(
    fake_clock: FakeClock, rerun_on: RerunOn, expected_runs: int
) -> None:
    earlier = fake_clock.now() - timedelta(minutes=1)
    reobserver = make_operator('reobserver', depends_on={Flag}, produces={Ip}, emits=[Ip.emit('1.1.1.1')])
    watcher = make_operator('watcher', depends_on={Ip}, rerun_on_new_data=True, rerun_on=rerun_on)

    result = await run_session(
        fake_clock, [reobserver, watcher], seed=[dp(Flag, True, earlier), dp(Ip, '1.1.1.1', earlier)]
    )

    assert result.operator_runs[OperatorId('watcher')] == expected_runs
    (ip,) = result.data_points.of_type(Ip)
    assert (ip.first_retrieved, ip.last_retrieved) == (earlier, fake_clock.now())  # merged, not duplicated


async def test_failure_is_isolated_emissions_kept_and_peers_run(fake_clock: FakeClock) -> None:
    broken = make_operator(
        'broken', depends_on={Flag}, produces={Ip}, emits=[Ip.emit('kept')], raise_error=ValueError('boom')
    )
    peer = make_operator('peer', depends_on={Ip}, produces={Risk}, emits=[Risk.emit(1.0)])

    result = await run_session(fake_clock, [broken, peer], seed=[dp(Flag, True, fake_clock.now())])

    assert result.failures == {'broken': 'boom'}
    assert result.operator_runs == {'broken': 1, 'peer': 1}
    assert _values(result.data_points, Ip) == ['kept'] and _values(result.data_points, Risk) == [1.0]


async def test_timeout_is_isolated_like_any_failure(fake_clock: FakeClock) -> None:
    slow = make_operator('slow', depends_on={Flag}, sleep_after=5.0, timeout=timedelta(milliseconds=20))
    global_slow = make_operator('global_slow', depends_on={Flag}, sleep_after=5.0)

    result = await run_session(
        fake_clock, [slow, global_slow], seed=[dp(Flag, True, fake_clock.now())], operation_timeout=0.02
    )

    assert set(result.failures) == {'slow', 'global_slow'}


async def test_retry_relaunches_on_backoff_with_the_same_delta_then_succeeds(fake_clock: FakeClock) -> None:
    seen: list[OperatorContext] = []
    flaky = make_operator(
        'flaky',
        depends_on={Flag},
        produces={Ip},
        emits=[Ip.emit('1.1.1.1')],
        fail_first=2,
        retry=RetryPolicy(max_attempts=5, base_delay=1.0, jitter=0.0),
        seen=seen,
    )

    result = await run_session(fake_clock, [flaky], seed=[dp(Flag, True, fake_clock.now())])

    assert result.operator_runs == {'flaky': 3} and result.failures == {}
    assert all(ctx.delta.is_first_invocation for ctx in seen)  # the watermark never advanced while retrying
    assert fake_clock.monotonic() == 1.0 + 2.0  # the two backoff windows, waited on the injected clock
    assert _values(result.data_points, Ip) == ['1.1.1.1']


async def test_retry_budget_exhausted_is_terminal(fake_clock: FakeClock) -> None:
    doomed = make_operator(
        'doomed',
        depends_on={Flag},
        raise_error=RuntimeError('always'),
        retry=RetryPolicy(max_attempts=3, base_delay=1.0, jitter=0.0),
    )
    result = await run_session(fake_clock, [doomed], seed=[dp(Flag, True, fake_clock.now())])
    assert result.operator_runs == {'doomed': 3} and result.failures == {'doomed': 'always'}


async def test_max_cycles_bounds_a_self_feeding_operator_and_unbounded_cycle_is_rejected(
    fake_clock: FakeClock,
) -> None:
    def next_ip(ctx: OperatorContext) -> list[Any]:
        return [Ip.emit(f'10.0.0.{len(ctx.store.of_type(Ip)) + 1}')]

    looper = make_operator(
        'looper', depends_on={Ip}, produces={Ip}, rerun_on_new_data=True, max_cycles=3, emit_factory=next_ip
    )
    result = await run_session(fake_clock, [looper], seed=[dp(Ip, '10.0.0.1', fake_clock.now())])
    assert result.operator_runs == {'looper': 3}
    assert _values(result.data_points, Ip) == ['10.0.0.1', '10.0.0.2', '10.0.0.3', '10.0.0.4']

    with pytest.raises(UnboundedCycleError):
        await run_session(fake_clock, [make_operator('loop', depends_on={Ip}, produces={Ip}, rerun_on_new_data=True)])


async def test_tripped_breaker_makes_a_retry_terminal(fake_clock: FakeClock) -> None:
    looper = make_operator(
        'looper',
        depends_on={Ip},
        produces={Ip},
        max_cycles=2,
        raise_error=RuntimeError('x'),
        retry=RetryPolicy(max_attempts=10, base_delay=0.0, jitter=0.0),
    )
    result = await run_session(fake_clock, [looper], seed=[dp(Ip, '1', fake_clock.now())])
    assert result.operator_runs == {'looper': 2} and 'looper' in result.failures


async def test_consumes_prunes_operators_nothing_needs(fake_clock: FakeClock) -> None:
    needed = make_operator('needed', depends_on={Flag}, produces={Ip}, emits=[Ip.emit('1')])
    upstream = make_operator('upstream', depends_on={Ip}, produces={Risk}, emits=[Risk.emit(0.2)])
    dead = make_operator('dead', depends_on={Flag}, produces={WorkEmail}, emits=[WorkEmail.emit('w')])
    sink = make_operator('sink', depends_on={Risk}, consumes={Risk})

    result = await run_session(fake_clock, [needed, upstream, dead, sink], seed=[dp(Flag, True, fake_clock.now())])

    assert set(result.operator_runs) == {'needed', 'upstream', 'sink'}
    assert not result.data_points.of_type(WorkEmail)


@pytest.mark.parametrize(
    ('permitted', 'expected'),
    [(None, {'x', 'y'}), ({'x'}, {'x'}), (set(), set())],
    ids=['unrestricted', 'gated', 'run-nothing'],
)
async def test_namespace_operator_gating(
    fake_clock: FakeClock, permitted: set[str] | None, expected: set[str]
) -> None:
    x = make_operator('x', depends_on={Flag})
    y = make_operator('y', depends_on={Flag})
    catalog = InMemoryCapabilityCatalog()
    if permitted is not None:
        catalog.set_permitted_operators(NAMESPACE, [OperatorId(op) for op in permitted])

    result = await run_session(fake_clock, [x, y], seed=[dp(Flag, True, fake_clock.now())], catalog=catalog)
    assert set(result.operator_runs) == expected

    other = await run_session(
        fake_clock, [x, y], seed=[dp(Flag, True, fake_clock.now())], catalog=catalog, namespace_id=NamespaceId('other')
    )
    assert set(other.operator_runs) == {'x', 'y'}  # gating is per namespace


async def test_operator_requiring_a_capability_runs_only_once_it_is_activated_from_credentials(
    fake_clock: FakeClock,
) -> None:
    cap = make_capability('intel', depends_on={Email})
    seen: list[OperatorContext] = []
    checker = make_operator(
        'checker', depends_on={Email}, requires={cap}, produces={Risk}, emits=[Risk.emit(0.7)], seen=seen
    )
    catalog = InMemoryCapabilityCatalog(
        permitted={NAMESPACE: {CapabilityId('intel')}},
        credentials={(NAMESPACE, CapabilityId('intel')): {'token': 'secret'}},
    )
    seed = [dp(WorkEmail, 'a@x', fake_clock.now())]

    result = await run_session(fake_clock, [checker], capabilities=[cap], seed=seed, catalog=catalog)
    assert result.operator_runs == {'checker': 1}
    assert seen[0].capabilities.available_ids() == {'intel'}
    assert await seen[0].capabilities.require(cap).token() == 'secret'

    not_permitted = await run_session(fake_clock, [checker], capabilities=[cap], seed=seed)
    assert not_permitted.operator_runs == {}


async def test_newly_available_capability_reruns_a_rerun_operator(fake_clock: FakeClock) -> None:
    cap = make_capability('late', depends_on={Ip})
    unlock = make_operator('unlock', depends_on={Flag}, produces={Ip}, emits=[Ip.emit('1')])
    seen: list[OperatorContext] = []
    watcher = make_operator('watcher', depends_on={Flag}, rerun_on_new_data=True, seen=seen)
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {CapabilityId('late')}})

    result = await run_session(
        fake_clock, [unlock, watcher], capabilities=[cap], seed=[dp(Flag, True, fake_clock.now())], catalog=catalog
    )

    assert result.operator_runs[OperatorId('watcher')] == 2
    assert seen[1].delta.newly_available_caps == {'late'}


async def test_duplicate_ids_and_missing_policy_are_rejected(fake_clock: FakeClock) -> None:
    with pytest.raises(DuplicateIdError, match='operator_id'):
        await run_session(fake_clock, [make_operator('dup'), make_operator('dup')])
    with pytest.raises(DuplicateIdError, match='capability_id'):
        await run_session(fake_clock, [], capabilities=[make_capability('dup'), make_capability('dup')])
    with pytest.raises(InvalidOperatorError):

        class NoPolicy(Operator):
            operator_id = OperatorId('no_policy')

            async def run(self, _ctx: OperatorContext) -> Any:
                yield Ip.emit('x')

    assert OperatorPolicy(rerun_on_new_data=False).retry is None


async def test_undeclared_emission_is_merged_and_logged_once(fake_clock: FakeClock) -> None:
    records: list[str] = []
    sink_id = logger.add(lambda message: records.append(str(message)), level='ERROR')
    try:
        sneaky = make_operator('sneaky', depends_on={Flag}, emits=[Ip.emit('1'), Ip.emit('2')])
        result = await run_session(fake_clock, [sneaky], seed=[dp(Flag, True, fake_clock.now())])
    finally:
        logger.remove(sink_id)

    assert _values(result.data_points, Ip) == ['1', '2']
    assert len([record for record in records if 'produces declaration' in record]) == 1


async def test_unhashable_emission_fails_only_its_operator(fake_clock: FakeClock) -> None:
    class Raw(DataPoint[Any]): ...

    careless = make_operator('careless', depends_on={Flag}, produces={Raw}, emits=[Raw.emit(bytearray(b'x'))])
    peer = make_operator('peer', depends_on={Flag}, produces={Ip}, emits=[Ip.emit('1')])

    result = await run_session(fake_clock, [careless, peer], seed=[dp(Flag, True, fake_clock.now())])

    assert 'bytearray' in result.failures[OperatorId('careless')]
    assert result.operator_runs == {'careless': 1, 'peer': 1}
    assert _values(result.data_points, Ip) == ['1']


async def test_session_deadline_bounds_a_flow_that_keeps_retriggering_itself(fake_clock: FakeClock) -> None:
    def next_ip(ctx: OperatorContext) -> list[Any]:
        return [Ip.emit(f'10.0.0.{len(ctx.store.of_type(Ip)) + 1}')]

    looper = make_operator(
        'looper',
        depends_on={Ip},
        produces={Ip},
        rerun_on_new_data=True,
        max_cycles=1000,
        debounce=timedelta(seconds=1),
        emit_factory=next_ip,
    )
    result = await Orchestrator(
        session_id=SESSION,
        namespace_id=NAMESPACE,
        runtime=build_runtime(fake_clock),
        operators=[looper],
        seed=[dp(Ip, '10.0.0.1', fake_clock.now())],
        session_deadline=5.0,
    ).run()

    assert result.deadline_hit
    assert result.operator_runs == {'looper': 5}  # runs at t=0..4; the pass at t=5 is refused by the deadline
    assert fake_clock.monotonic() == 5.0  # the last window was clipped to the deadline, never past it
    assert result.failures == {}


async def test_session_deadline_cancels_in_flight_operators_but_keeps_their_emissions(fake_clock: FakeClock) -> None:
    def long_run(_ctx: OperatorContext) -> list[Any]:
        fake_clock.advance(10.0)  # this run alone eats the whole budget
        return [Risk.emit(0.5)]

    hog = make_operator('hog', depends_on={Flag}, produces={Risk}, emit_factory=long_run)
    slow = make_operator('slow', depends_on={Flag}, produces={Ip}, emits=[Ip.emit('early')], sleep_after=5.0)
    events = InMemorySessionEventSink()

    result = await Orchestrator(
        session_id=SESSION,
        namespace_id=NAMESPACE,
        runtime=build_runtime(fake_clock, events=events),
        operators=[hog, slow],
        seed=[dp(Flag, True, fake_clock.now())],
        session_deadline=5.0,
    ).run()

    assert result.deadline_hit
    assert result.operator_runs == {'hog': 1}  # slow never finished a run
    assert 'deadline' in result.failures[OperatorId('slow')]
    assert _values(result.data_points, Ip) == ['early'] and _values(result.data_points, Risk) == [0.5]
    cancelled = [e for e in events.events if isinstance(e, OperatorRunCompleted) and e.outcome == 'cancelled']
    assert [e.operator_id for e in cancelled] == ['slow']


async def test_unbounded_session_runs_to_quiescence(fake_clock: FakeClock) -> None:
    a = make_operator('a', depends_on={Flag}, produces={Ip}, emits=[Ip.emit('1')])
    result = await Orchestrator(
        session_id=SESSION,
        namespace_id=NAMESPACE,
        runtime=build_runtime(fake_clock),
        operators=[a],
        seed=[dp(Flag, True, fake_clock.now())],
        session_deadline=None,
    ).run()
    assert not result.deadline_hit and result.operator_runs == {'a': 1}


async def test_every_change_is_published_to_the_event_sink_in_order(fake_clock: FakeClock) -> None:
    cap = make_capability('intel', depends_on={Flag})
    earlier = fake_clock.now() - timedelta(minutes=1)
    producer = make_operator(
        'producer', depends_on={Flag}, requires={cap}, produces={Ip}, emits=[Ip.emit('1'), Ip.emit('seeded')]
    )
    broken = make_operator('broken', depends_on={Flag}, raise_error=RuntimeError('boom'))
    events = InMemorySessionEventSink()
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {CapabilityId('intel')}})

    result = await Orchestrator(
        session_id=SESSION,
        namespace_id=NAMESPACE,
        runtime=build_runtime(fake_clock, catalog=catalog, events=events),
        operators=[producer, broken],
        capabilities=[cap],
        seed=[dp(Flag, True, earlier), dp(Ip, 'seeded', earlier)],
    ).run()

    kinds = [event.kind for event in events.events]
    assert kinds[:2] == ['data_point_merged', 'data_point_merged']  # the seed, before anything runs
    assert kinds[2] == 'capability_activated'
    assert kinds[-1] == 'session_completed'
    assert all(event.session_id == SESSION and event.namespace_id == NAMESPACE for event in events.events)

    merged = [e for e in events.events if isinstance(e, DataPointMerged)]
    assert [(e.data_point_type, e.value, e.merge) for e in merged] == [
        ('Flag', True, 'added'),
        ('Ip', 'seeded', 'added'),
        ('Ip', '1', 'added'),
        ('Ip', 'seeded', 'updated'),
    ]
    assert merged[-1].retrieved_by == 'seed' and merged[-1].revision == 2  # an update keeps the first observer

    runs = {(e.operator_id, e.outcome, e.error) for e in events.events if isinstance(e, OperatorRunCompleted)}
    assert runs == {('producer', 'succeeded', None), ('broken', 'failed', 'boom')}
    activated = [e for e in events.events if isinstance(e, CapabilityActivated)]
    assert [(e.capability_id, e.outcome) for e in activated] == [('intel', 'activated')]
    completed = events.events[-1]
    assert isinstance(completed, SessionCompleted)
    assert (completed.operator_runs, completed.failures, completed.deadline_hit) == (
        result.operator_runs,
        result.failures,
        False,
    )


async def test_a_failing_event_sink_never_stops_the_session(fake_clock: FakeClock) -> None:
    class Exploding:
        async def publish(self, _event: SessionEvent) -> None:
            raise ConnectionError('redis is down')

    a = make_operator('a', depends_on={Flag}, produces={Ip}, emits=[Ip.emit('1')])
    result = await Orchestrator(
        session_id=SESSION,
        namespace_id=NAMESPACE,
        runtime=build_runtime(fake_clock, events=Exploding()),
        operators=[a],
        seed=[dp(Flag, True, fake_clock.now())],
    ).run()
    assert result.operator_runs == {'a': 1} and _values(result.data_points, Ip) == ['1']


async def test_armed_retry_becomes_terminal_when_the_operator_loses_readiness(fake_clock: FakeClock) -> None:
    cap = make_capability('intel')
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {CapabilityId('intel')}})

    def revoke(_ctx: OperatorContext) -> list[Any]:
        catalog.set_permitted(NAMESPACE, [])  # the namespace drops the capability while the retry window is armed
        return []

    flaky = make_operator(
        'flaky',
        depends_on={Flag},
        requires={cap},
        raise_error=RuntimeError('boom'),
        retry=RetryPolicy(max_attempts=3, base_delay=1.0, jitter=0.0),
    )
    revoker = make_operator('revoker', depends_on={Flag}, emit_factory=revoke)
    events = InMemorySessionEventSink()

    result = await Orchestrator(
        session_id=SESSION,
        namespace_id=NAMESPACE,
        runtime=build_runtime(fake_clock, catalog=catalog, events=events),
        operators=[flaky, revoker],
        capabilities=[cap],
        seed=[dp(Flag, True, fake_clock.now())],
    ).run()

    # The relaunch can never run, so the failure it was retrying is terminal — not silently forgotten.
    assert result.operator_runs == {'flaky': 1, 'revoker': 1}
    assert result.failures == {'flaky': 'boom'}
    outcomes = [e.outcome for e in events.events if isinstance(e, OperatorRunCompleted) and e.operator_id == 'flaky']
    assert outcomes == ['retrying', 'failed']
    assert fake_clock.monotonic() == 0.0  # nothing waited out a window that could never fire


async def test_a_hanging_event_sink_is_bounded_by_the_publish_timeout(fake_clock: FakeClock) -> None:
    class Stalled:
        attempts = 0

        async def publish(self, _event: SessionEvent) -> None:
            self.attempts += 1
            await asyncio.sleep(5.0)  # a connection that never answers

    sink = Stalled()
    a = make_operator('a', depends_on={Flag}, produces={Ip}, emits=[Ip.emit('1')])
    result = await Orchestrator(
        session_id=SESSION,
        namespace_id=NAMESPACE,
        runtime=build_runtime(fake_clock, events=sink),
        operators=[a],
        seed=[dp(Flag, True, fake_clock.now())],
        publish_timeout=0.01,
    ).run()

    assert result.operator_runs == {'a': 1} and _values(result.data_points, Ip) == ['1']
    assert sink.attempts == 4  # seed merge, emission merge, run completed, session completed — each bounded
