"""COMP — declarative completion conditions: the AST itself + the orchestrator-level wiring."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from orcastork.datapoints import BaseDataPoint, DataPointView
from orcastork.exceptions import InvalidCompletionConditionError
from orcastork.ids import NamespaceId, SessionId
from orcastork.orchestrator import Orchestrator, SessionStatus
from orcastork.runtime import build_in_memory_runtime
from orcastork.scheduling import (
    AllOf,
    AnyOf,
    TypePresent,
    all_of,
    any_of,
    describe_condition,
    normalize_completion,
)

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

SID = SessionId('comp-session')
NAMESPACE = NamespaceId('comp-namespace')


# --- the AST --------------------------------------------------------------------------


def test_comp_01_type_present_requires_an_instance_of_the_type() -> None:
    condition = TypePresent(RiskDataPoint)
    assert condition.is_satisfied(DataPointView([risk()])) is True
    assert condition.is_satisfied(DataPointView([ip()])) is False
    assert condition.is_satisfied(DataPointView()) is False


def test_comp_02_type_present_is_subtype_aware() -> None:
    # Consistent with readiness: a present leaf satisfies a base-type condition.
    assert TypePresent(EmailDataPoint).is_satisfied(DataPointView([work_email()])) is True


def test_comp_03_all_of_requires_every_child() -> None:
    condition = all_of(RiskDataPoint, ChatAnswerDataPoint)
    assert condition.is_satisfied(DataPointView([risk()])) is False
    assert condition.is_satisfied(DataPointView([risk(), chat_answer()])) is True


def test_comp_04_any_of_requires_at_least_one_child() -> None:
    condition = any_of(RiskDataPoint, ChatAnswerDataPoint)
    assert condition.is_satisfied(DataPointView([ip()])) is False
    assert condition.is_satisfied(DataPointView([chat_answer()])) is True


def test_comp_05_conditions_nest() -> None:
    # "A risk score AND (an answer OR an ip)" — the report-and-all-answers-scored shape.
    condition = all_of(RiskDataPoint, any_of(ChatAnswerDataPoint, IpDataPoint))
    assert condition.is_satisfied(DataPointView([risk()])) is False
    assert condition.is_satisfied(DataPointView([risk(), ip()])) is True
    assert condition.is_satisfied(DataPointView([risk(), chat_answer()])) is True
    assert condition.is_satisfied(DataPointView([chat_answer(), ip()])) is False


def test_comp_06_empty_combinators_follow_the_conventional_identities() -> None:
    # AllOf(()) is the empty conjunction (true); AnyOf(()) is the empty disjunction (false).
    assert all_of().is_satisfied(DataPointView()) is True
    assert any_of().is_satisfied(DataPointView()) is False


def test_comp_07_constructors_normalize_bare_types_and_pass_conditions_through() -> None:
    inner = any_of(ChatAnswerDataPoint)
    condition = all_of(RiskDataPoint, inner)
    assert condition == AllOf((TypePresent(RiskDataPoint), inner))
    assert any_of(RiskDataPoint, TypePresent(IpDataPoint)) == AnyOf(
        (TypePresent(RiskDataPoint), TypePresent(IpDataPoint))
    )


def test_comp_08_normalize_completion_is_the_single_compatibility_seam() -> None:
    assert normalize_completion(None) is None
    assert normalize_completion(RiskDataPoint) == TypePresent(RiskDataPoint)
    condition = any_of(RiskDataPoint, ChatAnswerDataPoint)
    assert normalize_completion(condition) is condition


def test_comp_09_non_datapoint_items_are_rejected_at_construction() -> None:
    with pytest.raises(InvalidCompletionConditionError):
        all_of(str)  # type: ignore[arg-type]  # a class, but not a DataPoint type
    with pytest.raises(InvalidCompletionConditionError):
        any_of('risk')  # type: ignore[arg-type]  # a value, not a type or condition


def test_comp_09b_custom_condition_without_describe_falls_back_to_qualified_name() -> None:
    # A user-supplied condition is only obligated to implement the Protocol's is_satisfied;
    # describe() is optional. Its fingerprint must still be deterministic across pods/deploys,
    # so the fallback uses module.qualname — never id() or a default object repr.
    class CustomCondition:
        def is_satisfied(self, view: DataPointView) -> bool:  # noqa: ARG002
            return False

    text = describe_condition(CustomCondition())
    assert text == f'{CustomCondition.__module__}.{CustomCondition.__qualname__}'
    assert '<class' not in text and ' at 0x' not in text  # no class-object or default-instance reprs
    assert describe_condition(CustomCondition()) == text  # process-stable: independent of instance identity


# --- orchestrator-level wiring ----------------------------------------------------------


@pytest.mark.parametrize('arrival', [chat_answer('it was me'), ip('198.51.100.7')])
async def test_comp_10_any_of_completes_when_either_branch_arrives(
    fake_clock: FakeClock, arrival: BaseDataPoint[Any]
) -> None:
    # A "verdict OR user-abandoned" flow: whichever of the two types lands first completes it.
    runtime = build_in_memory_runtime(fake_clock)

    async def user_acts() -> None:
        for _ in range(5):
            await asyncio.sleep(0)  # give the session time to reach the inbox wait
        await runtime.inbox.append(SID, arrival)

    orchestrator = Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[],
        seed=[work_email()],
        completes_when=any_of(ChatAnswerDataPoint, IpDataPoint),
    )
    result, _ = await asyncio.gather(orchestrator.run(), user_acts())

    assert result.status is SessionStatus.COMPLETED
    assert arrival in set((await runtime.store.snapshot(SID)).all())  # the arrival was folded in
    assert fake_clock.monotonic() < 300.0  # one branch satisfied the condition — no wait to the deadline


async def test_comp_11_all_of_keeps_waiting_on_the_inbox_until_every_branch_is_present(
    fake_clock: FakeClock,
) -> None:
    runtime = build_in_memory_runtime(fake_clock)

    async def user_acts() -> None:
        for _ in range(5):
            await asyncio.sleep(0)  # give the session time to reach the inbox wait
        await runtime.inbox.append(SID, chat_answer('first'))
        for _ in range(10):
            await asyncio.sleep(0)  # let the session apply the entry and re-evaluate completion
        assert not await runtime.lock.is_complete(SID)  # one of two branches present → still waiting
        await runtime.inbox.append(SID, ip('198.51.100.7'))

    orchestrator = Orchestrator(
        session_id=SID,
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[],
        seed=[work_email()],
        completes_when=all_of(ChatAnswerDataPoint, IpDataPoint),
    )
    result, _ = await asyncio.gather(orchestrator.run(), user_acts())

    assert result.status is SessionStatus.COMPLETED
    types = {dp.type for dp in (await runtime.store.snapshot(SID)).all()}
    assert {'chat_answer', 'ip'} <= types  # both arrivals were folded in before completion
    assert await runtime.inbox.pending_count(SID) == 0  # both applied and acked
    assert fake_clock.monotonic() < 300.0  # completed by satisfaction, not by the session deadline
