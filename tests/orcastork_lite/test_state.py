"""SessionState: keyed-merge, revisions, and the per-operator delta."""

from __future__ import annotations

from datetime import timedelta

from orcastork_lite import OperatorId
from orcastork_lite.state import SessionState

from ..doubles.clock import FakeClock
from .conftest import Ip, Risk, dp

OP = OperatorId('op')


def test_merge_adds_then_updates_and_advances_revision_only_on_change(fake_clock: FakeClock) -> None:
    now = fake_clock.now()
    state = SessionState()
    first = state.merge([dp(Ip, 'a', now)])
    assert (first.revision, first.added, first.updated) == (1, (dp(Ip, 'a', now),), ())
    same = state.merge([dp(Ip, 'a', now)])  # identical re-observation: nothing changed
    assert same.revision == 1 and not same.changed
    fresher = state.merge([dp(Ip, 'a', now + timedelta(seconds=1))])  # fresher sighting: updated
    assert fresher.revision == 2 and fresher.updated == (dp(Ip, 'a', now),) and not fresher.added
    assert state.merge([]).revision == 2

    (only,) = state.view().all()
    assert (only.first_retrieved, only.last_retrieved) == (now, now + timedelta(seconds=1))


def test_first_invocation_delta_is_the_whole_set(fake_clock: FakeClock) -> None:
    now = fake_clock.now()
    state = SessionState()
    state.merge([dp(Ip, 'a', now), dp(Risk, 0.1, now)])

    delta = state.delta_for(OP, previous_caps=frozenset(), available_caps=frozenset())
    assert delta.is_first_invocation
    assert delta.added == {dp(Ip, 'a', now), dp(Risk, 0.1, now)} and not delta.updated
    assert not state.has_run(OP)


def test_delta_after_watermark_splits_added_and_updated(fake_clock: FakeClock) -> None:
    now = fake_clock.now()
    state = SessionState()
    state.advance_watermark(OP, state.merge([dp(Ip, 'a', now)]).revision)
    state.merge([dp(Ip, 'a', now + timedelta(seconds=1)), dp(Ip, 'b', now)])

    delta = state.delta_for(OP, previous_caps=frozenset({'x'}), available_caps=frozenset({'x', 'y'}))  # type: ignore[arg-type]
    assert not delta.is_first_invocation
    assert delta.added == {dp(Ip, 'b', now)}
    assert delta.updated == {dp(Ip, 'a', now)}
    assert delta.newly_available_caps == {'y'}
    assert state.has_run(OP)
