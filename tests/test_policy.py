"""POLICY — rerun eligibility and debounce/coalescing.

POLICY-03 (no cancellation), POLICY-06 (stopped only by timeout/deadline) and POLICY-07
(rerun processes only ctx.delta) are orchestrator behaviours and are exercised in M7
(test_orchestrator / ACC); this suite covers the rerun decision + the debounce primitive.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta

import pytest

from orcastork.datapoints import DataPointEmission
from orcastork.exceptions import InvalidOperatorError
from orcastork.ids import OperatorId
from orcastork.operators import Operator, OperatorContext, OperatorPolicy
from orcastork.scheduling import DebounceController, rerun_eligible, window_defers_to_finalize

from .doubles.clock import FakeClock
from .doubles.datapoints import RiskDataPoint, WorkEmailDataPoint
from .doubles.operators import make_aggregator, make_operator


def test_policy_01_rerun_on_new_data_true_is_eligible_when_data_arrives() -> None:
    policy = OperatorPolicy(rerun_on_new_data=True)
    assert rerun_eligible(policy, has_relevant_new_data=True)
    assert not rerun_eligible(policy, has_relevant_new_data=False)  # nothing new → no rerun


def test_policy_02_rerun_on_new_data_false_never_reinvokes() -> None:
    policy = OperatorPolicy(rerun_on_new_data=False)
    assert not rerun_eligible(policy, has_relevant_new_data=True)


def test_policy_04_arrivals_within_window_coalesce_into_single_rerun(fake_clock: FakeClock) -> None:
    controller = DebounceController(fake_clock, default_window=timedelta(seconds=10))
    chatty = OperatorId('chatty')

    controller.schedule(chatty)  # arrival 1
    fake_clock.advance(5)
    controller.schedule(chatty)  # arrival 2 within the window → coalesces
    fake_clock.advance(5)
    assert not controller.is_due(chatty)  # 10s since arrival 1 but only 5s since arrival 2
    fake_clock.advance(5)
    assert controller.is_due(chatty)  # one rerun becomes due for the whole burst
    controller.clear(chatty)
    assert not controller.is_due(chatty)  # no second rerun for the same burst


def test_policy_05_per_operator_override_beats_global_default(fake_clock: FakeClock) -> None:
    controller = DebounceController(fake_clock, default_window=timedelta(seconds=10))
    chatty, latency_sensitive = OperatorId('chatty'), OperatorId('latency_sensitive')

    controller.schedule(chatty)  # global 10s
    controller.schedule(latency_sensitive, window=timedelta(seconds=1))  # per-op override
    fake_clock.advance(1)
    assert controller.is_due(latency_sensitive)
    assert not controller.is_due(chatty)


def test_policy_08_zero_window_reruns_immediately(fake_clock: FakeClock) -> None:
    controller = DebounceController(fake_clock, default_window=timedelta(seconds=10))
    immediate = OperatorId('immediate')
    controller.schedule(immediate, window=timedelta(0))
    assert controller.is_due(immediate)  # no coalescing delay


def test_policy_09_fixed_window_does_not_slide_when_re_arm_is_gated(fake_clock: FakeClock) -> None:
    # The orchestrator arms a window once and never re-arms while is_scheduled is True, so a burst
    # collapses into a FIXED window measured from the FIRST arrival. This pins the behaviour the
    # orchestrator actually relies on (is_scheduled gating) rather than the controller's own sliding.
    controller = DebounceController(fake_clock, default_window=timedelta(seconds=10))
    chatty = OperatorId('chatty')

    assert not controller.is_scheduled(chatty)  # nothing armed yet
    assert controller.due_at(chatty) is None  # and so no due time
    controller.schedule(chatty)  # arrival 1 arms the window
    assert controller.is_scheduled(chatty)
    assert isinstance(controller.due_at(chatty), float)
    fake_clock.advance(5)
    # A second arrival during the window: the orchestrator does NOT call schedule again because
    # is_scheduled() is already True — so the window must NOT slide.
    assert controller.is_scheduled(chatty)
    fake_clock.advance(5)
    assert controller.is_due(chatty)  # due 10s after arrival 1, NOT 10s after the 5s-later arrival


def test_policy_10_clear_disarms_so_is_scheduled_and_due_at_reset(fake_clock: FakeClock) -> None:
    controller = DebounceController(fake_clock, default_window=timedelta(seconds=10))
    op = OperatorId('op')

    controller.schedule(op)
    assert controller.is_scheduled(op) and controller.due_at(op) is not None
    controller.clear(op)
    assert not controller.is_scheduled(op)  # consumed: a fresh burst must re-arm from scratch
    assert controller.due_at(op) is None  # no stale due time lingers


def test_policy_11_due_at_is_none_for_an_unknown_operator(fake_clock: FakeClock) -> None:
    controller = DebounceController(fake_clock, default_window=timedelta(seconds=10))
    assert controller.due_at(OperatorId('never-scheduled')) is None


def test_policy_12_concrete_operator_without_policy_is_rejected() -> None:
    # A concrete operator (has operator_id, implements run, no abstract methods left) that forgets
    # the `policy` ClassVar must be rejected at class-definition time — distinct from the abstract-base
    # skip (no operator_id) and from OperatorPolicy()'s missing-arg TypeError.
    with pytest.raises(InvalidOperatorError, match='scheduling `policy`'):

        class _NoPolicyOp(Operator):
            operator_id = OperatorId('no_policy')

            async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:  # noqa: ARG002
                # A concrete async generator (so no abstractmethods remain) — never actually run; the
                # registration guard rejects the class at definition time, before any instantiation.
                yield WorkEmailDataPoint.emit('x@e.example')


def test_policy_13_only_an_interim_window_defers_to_an_imminent_finalize() -> None:
    # An armed window normally holds the gathering loop open — that is what makes the coalescing
    # real. The single exemption is an interim refold whose output the finalize pass is about to
    # rewrite anyway; nothing else may be abandoned, whatever the completion state.
    interim = make_aggregator('interim', depends_on={RiskDataPoint}, rerun_on_new_data=True, interim_refresh=True)
    batch = make_aggregator('batch', depends_on={RiskDataPoint}, rerun_on_new_data=True)
    plain = make_operator('plain', depends_on={RiskDataPoint}, rerun_on_new_data=True)

    assert window_defers_to_finalize(interim, completion_satisfied=True)
    # Unsatisfied: the session is heading for an inbox wait instead, where the interim write is the
    # only live view a reader gets — so the window still holds.
    assert not window_defers_to_finalize(interim, completion_satisfied=False)
    # An ordinary operator's rerun feeds the finalize data it would otherwise never see, and a
    # non-interim aggregator never reruns during gathering at all.
    assert not window_defers_to_finalize(plain, completion_satisfied=True)
    assert not window_defers_to_finalize(batch, completion_satisfied=True)
