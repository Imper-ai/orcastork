"""INTRO — describe_session / render_text: read-only stuck-session forensics from persisted state.

The description is computed with the same pure functions the scheduler uses, so what it
reports as missing/ready is exactly what the engine would act on.
"""

from __future__ import annotations

from typing import Any

from orcastork.adapters.memory import InMemoryCapabilityCatalog, InMemoryInbox
from orcastork.flow import FlowDefinition
from orcastork.ids import CapabilityId, Epoch, NamespaceId, OperatorId, SessionId
from orcastork.introspection import OperatorState, SessionDescription, describe_session, render_text
from orcastork.manager import SessionOrchestrationManager
from orcastork.operators import Operator
from orcastork.orchestrator import SessionStatus
from orcastork.runtime import build_in_memory_runtime

from .doubles.capabilities import make_capability
from .doubles.clock import FakeClock
from .doubles.datapoints import (
    ChatAnswerDataPoint,
    EmailDataPoint,
    IpDataPoint,
    RiskDataPoint,
    chat_answer,
    risk,
    work_email,
)
from .doubles.operators import make_aggregator, make_operator

SID = SessionId('intro-session')
NAMESPACE = NamespaceId('intro-namespace')


def _flow(*operators: type[Operator], **kwargs: Any) -> FlowDefinition:
    return FlowDefinition(name='intro-flow', operators=tuple(operators), **kwargs)


def _state(description: SessionDescription, operator_id: str) -> OperatorState:
    return next(state for state in description.operators if state.operator_id == OperatorId(operator_id))


async def test_intro_stuck_operator_reports_exactly_its_missing_dependency_type(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    answer_handler = make_operator(
        'answer_handler', depends_on={ChatAnswerDataPoint}, produces={RiskDataPoint}, emits=[risk()]
    )
    flow = _flow(answer_handler, completes_when=RiskDataPoint, park_after=20.0)
    parked = await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])
    assert parked.status is SessionStatus.PARKED  # the answer never arrived — this session is "stuck"

    description = await describe_session(runtime, session_id=SID, namespace_id=NAMESPACE, flow=flow)

    assert description.is_complete is False
    assert description.is_owned is False  # parked sessions hold no lease
    assert description.current_epoch == 1
    assert description.deadline is not None  # the wall-clock budget persisted at the first gather
    assert description.pending_inbox == 0
    assert description.present_types == {'WorkEmailDataPoint': 1}
    state = _state(description, 'answer_handler')
    assert state.has_run is False and state.watermark is None
    assert state.is_ready_now is False
    assert state.missing_data_points == ('ChatAnswerDataPoint',)  # exactly the one missing type
    assert state.missing_capabilities == ()
    assert state.is_gated is False
    assert state.contribution_marked is None  # not an aggregator


async def test_intro_ready_but_unrun_operator_reports_ready_with_no_missing_pieces(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    # Persisted state only — no orchestrator has driven this session yet.
    await runtime.store.write(SID, [work_email()], epoch=Epoch(1))
    operator = make_operator('op', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])

    description = await describe_session(runtime, session_id=SID, namespace_id=NAMESPACE, flow=_flow(operator))

    state = _state(description, 'op')
    assert state.is_ready_now is True  # the abstract EmailDataPoint dependency is satisfied by the leaf
    assert state.has_run is False
    assert state.missing_data_points == () and state.missing_capabilities == ()
    assert description.is_complete is False
    assert description.deadline is None  # no gather ever persisted a budget


async def test_intro_completed_session_reports_complete_and_all_run(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    scorer = make_operator('scorer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    reporter = make_aggregator('rep', depends_on={RiskDataPoint})
    flow = _flow(scorer, reporter)
    result = await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])
    assert result.status is SessionStatus.COMPLETED

    description = await describe_session(runtime, session_id=SID, namespace_id=NAMESPACE, flow=flow)

    assert description.is_complete is True
    assert description.is_owned is False
    scorer_state = _state(description, 'scorer')
    assert scorer_state.has_run is True and scorer_state.watermark is not None
    reporter_state = _state(description, 'rep')
    assert reporter_state.contribution_marked is True  # the aggregator's durable "ran" flag
    assert 'all operators have run' in render_text(description)


async def test_intro_quarantined_entries_and_pending_inbox_are_surfaced(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    inbox = runtime.inbox
    assert isinstance(inbox, InMemoryInbox)
    poison_id = await inbox.append_serialized(SID, 'not-json{')
    operator = make_operator('op', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    flow = _flow(operator)
    await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])
    # A late entry appended after completion stays pending forever — exactly what an operator
    # inspecting a session needs to see.
    await runtime.inbox.append(SID, chat_answer('too late'))

    description = await describe_session(runtime, session_id=SID, namespace_id=NAMESPACE, flow=flow)

    assert description.pending_inbox == 1
    (quarantined,) = description.quarantined
    assert quarantined.entry_id == poison_id


async def test_intro_gated_operator_reports_is_gated(fake_clock: FakeClock) -> None:
    catalog = InMemoryCapabilityCatalog(permitted_operators={NAMESPACE: {OperatorId('kept')}})
    runtime = build_in_memory_runtime(fake_clock, catalog=catalog)
    manager = SessionOrchestrationManager(runtime)
    kept = make_operator('kept', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    gated = make_operator('gated', depends_on={EmailDataPoint}, produces={IpDataPoint})
    flow = _flow(kept, gated)
    await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])

    description = await describe_session(runtime, session_id=SID, namespace_id=NAMESPACE, flow=flow)

    gated_state = _state(description, 'gated')
    assert gated_state.is_gated is True
    assert gated_state.has_run is False  # the engine never launched it, although its input was present
    assert gated_state.is_ready_now is True  # data-ready, namespace-forbidden — the gate is the blocker
    assert _state(description, 'kept').is_gated is False
    assert 'gated for this namespace' in render_text(description)


async def test_intro_namespace_forbidden_capability_is_missing_for_its_dependent(fake_clock: FakeClock) -> None:
    # netcap is registered in the flow but the namespace's catalog does not permit it: availability
    # (computed with the engine's own fixpoint) excludes it, so its dependent reports it missing.
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: set()})
    runtime = build_in_memory_runtime(fake_clock, catalog=catalog)
    netcap = make_capability('netcap')
    consumer = make_operator('consumer', depends_on={EmailDataPoint}, requires={netcap}, produces={RiskDataPoint})
    flow = _flow(consumer, capabilities=(netcap,))
    await runtime.store.write(SID, [work_email()], epoch=Epoch(1))

    description = await describe_session(runtime, session_id=SID, namespace_id=NAMESPACE, flow=flow)

    state = _state(description, 'consumer')
    assert state.is_ready_now is False
    assert state.missing_capabilities == (netcap.__name__,)
    assert state.missing_data_points == ()  # only the capability blocks it
    assert netcap.__name__ in render_text(description)


async def test_intro_permitted_capability_with_present_deps_is_not_missing(fake_clock: FakeClock) -> None:
    catalog = InMemoryCapabilityCatalog(permitted={NAMESPACE: {CapabilityId('netcap')}})
    runtime = build_in_memory_runtime(fake_clock, catalog=catalog)
    netcap = make_capability('netcap', depends_on={EmailDataPoint})
    consumer = make_operator('consumer', depends_on={EmailDataPoint}, requires={netcap}, produces={RiskDataPoint})
    flow = _flow(consumer, capabilities=(netcap,))
    await runtime.store.write(SID, [work_email()], epoch=Epoch(1))

    description = await describe_session(runtime, session_id=SID, namespace_id=NAMESPACE, flow=flow)

    state = _state(description, 'consumer')
    assert state.is_ready_now is True
    assert state.missing_capabilities == ()


async def test_intro_fingerprint_mismatch_reported_when_the_flow_changed(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    answer_handler = make_operator(
        'answer_handler', depends_on={ChatAnswerDataPoint}, produces={RiskDataPoint}, emits=[risk()]
    )
    late_addition = make_operator('late_addition', depends_on={IpDataPoint}, produces={RiskDataPoint})
    original = _flow(answer_handler, completes_when=RiskDataPoint, park_after=20.0)
    changed = _flow(answer_handler, late_addition, completes_when=RiskDataPoint, park_after=20.0)
    parked = await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=original, seed=[work_email()])
    assert parked.status is SessionStatus.PARKED

    same = await describe_session(runtime, session_id=SID, namespace_id=NAMESPACE, flow=original)
    assert same.fingerprint_matches is True
    assert same.stored_flow_fingerprint == original.fingerprint()

    drifted = await describe_session(runtime, session_id=SID, namespace_id=NAMESPACE, flow=changed)
    assert drifted.fingerprint_matches is False
    assert drifted.stored_flow_fingerprint == original.fingerprint()  # the persisted one, not ours
    assert 'DRIFTED' in render_text(drifted)


async def test_intro_describe_is_read_only_even_while_the_session_is_owned(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    epoch = await runtime.lock.acquire(SID)  # a live orchestrator owns the session right now
    await runtime.store.write(SID, [work_email()], epoch=epoch)
    operator = make_operator('op', depends_on={EmailDataPoint}, produces={RiskDataPoint})
    flow = _flow(operator)
    revision_before = await runtime.store.revision(SID)

    description = await describe_session(runtime, session_id=SID, namespace_id=NAMESPACE, flow=flow)

    assert description.is_owned is True
    assert description.current_epoch == epoch
    # Nothing moved: no epoch was minted, no write happened, the owner is undisturbed.
    assert await runtime.lock.current_epoch(SID) == epoch
    assert await runtime.lock.is_held(SID) is True
    assert await runtime.store.revision(SID) == revision_before


async def test_intro_render_text_summarizes_the_stuck_session(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    answer_handler = make_operator(
        'answer_handler', depends_on={ChatAnswerDataPoint}, produces={RiskDataPoint}, emits=[risk()]
    )
    ready_waiter = make_operator('ready_waiter', depends_on={EmailDataPoint}, produces={IpDataPoint})
    flow = _flow(answer_handler, completes_when=RiskDataPoint, park_after=20.0)
    await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])
    # Describe against a wider flow so the rendering covers both a blocked and a ready operator.
    description = await describe_session(
        runtime, session_id=SID, namespace_id=NAMESPACE, flow=_flow(answer_handler, ready_waiter)
    )

    text = render_text(description)
    assert f'session {SID}' in text
    assert 'incomplete' in text
    assert 'unowned' in text
    assert 'WorkEmailDataPoint=1' in text
    assert 'answer_handler' in text and 'missing data: ChatAnswerDataPoint' in text
    assert 'ready_waiter' in text and 'ready, not yet run' in text
    assert len(text.splitlines()) >= 4  # status line + deadline/present + one line per pending operator


async def test_intro_empty_session_reports_no_present_types_and_renders_none(fake_clock: FakeClock) -> None:
    # The empty boundary: a never-touched session has zero DataPoints, so present_types is {},
    # every operator reports its dependency missing, deadline is None (no gather ever ran), and
    # render_text emits the 'present: (none)' branch — the literal that never renders when a seed
    # is written. describe_session must stay total over this brand-new/empty state.
    runtime = build_in_memory_runtime(fake_clock)
    operator = make_operator('op', depends_on={EmailDataPoint}, produces={RiskDataPoint})
    flow = _flow(operator)

    description = await describe_session(runtime, session_id=SID, namespace_id=NAMESPACE, flow=flow)

    assert description.present_types == {}
    assert description.revision == 0
    assert description.current_epoch == 0
    assert description.deadline is None
    state = _state(description, 'op')
    assert state.has_run is False
    assert state.is_ready_now is False  # its dependency is absent
    assert state.missing_data_points == ('EmailDataPoint',)
    text = render_text(description)
    assert 'present: (none)' in text


async def test_intro_present_types_counts_duplicate_concrete_types(fake_clock: FakeClock) -> None:
    # Two DataPoints of the SAME concrete leaf (distinct values) must accumulate to a count of 2 —
    # the get(...,0)+1 path past its first increment, which a single-value seed never exercises.
    runtime = build_in_memory_runtime(fake_clock)
    await runtime.store.write(SID, [risk(0.1), risk(0.9)], epoch=Epoch(1))
    operator = make_operator('op', depends_on={RiskDataPoint}, produces={IpDataPoint})

    description = await describe_session(runtime, session_id=SID, namespace_id=NAMESPACE, flow=_flow(operator))

    assert description.present_types['RiskDataPoint'] == 2  # multiplicity, not collapsed to 1
    assert 'RiskDataPoint=2' in render_text(description)


async def test_intro_describe_session_is_byte_identical_and_leaves_durable_state_untouched(
    fake_clock: FakeClock,
) -> None:
    # SYSTEMIC: describe_session is strictly read-only and deterministic. Run a session to
    # COMPLETED, snapshot the entire durable/lock/audit state, then call describe_session twice and
    # assert (1) the two renderings are byte-identical and (2) no store revision, lock epoch, lock
    # state, or audit length moved — describe minted no epoch and mutated no port.
    runtime = build_in_memory_runtime(fake_clock)
    manager = SessionOrchestrationManager(runtime)
    scorer = make_operator('scorer', depends_on={EmailDataPoint}, produces={RiskDataPoint}, emits=[risk()])
    reporter = make_aggregator('rep', depends_on={RiskDataPoint})
    flow = _flow(scorer, reporter)
    result = await manager.start_session(session_id=SID, namespace_id=NAMESPACE, flow=flow, seed=[work_email()])
    assert result.status is SessionStatus.COMPLETED

    revision_before = await runtime.store.revision(SID)
    epoch_before = await runtime.lock.current_epoch(SID)
    held_before = await runtime.lock.is_held(SID)
    complete_before = await runtime.lock.is_complete(SID)
    audit_len_before = len(await runtime.audit.replay(SID))

    first = render_text(await describe_session(runtime, session_id=SID, namespace_id=NAMESPACE, flow=flow))
    second = render_text(await describe_session(runtime, session_id=SID, namespace_id=NAMESPACE, flow=flow))

    assert first == second  # deterministic — describe is a pure function of persisted state
    assert await runtime.store.revision(SID) == revision_before  # no write
    assert await runtime.lock.current_epoch(SID) == epoch_before  # no epoch minted
    assert await runtime.lock.is_held(SID) == held_before
    assert await runtime.lock.is_complete(SID) == complete_before
    assert len(await runtime.audit.replay(SID)) == audit_len_before  # no audit entry appended
