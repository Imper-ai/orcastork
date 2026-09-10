"""DataPoint identity, the view's subtype-aware queries, and value-only emissions."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from orcastork_lite import DataPoint, DataPointView, OperatorId

from ..doubles.clock import FakeClock
from .conftest import Email, Ip, PersonalEmail, Risk, WorkEmail, dp


def test_identity_is_class_and_value_excluding_timestamps(fake_clock: FakeClock) -> None:
    now = fake_clock.now()
    a = dp(Ip, '1.1.1.1', now)
    b = dp(Ip, '1.1.1.1', now + timedelta(hours=1), by=OperatorId('other'))
    assert a == b and hash(a) == hash(b)
    assert dp(Ip, '2.2.2.2', now) != a
    assert dp(WorkEmail, 'x', now) != dp(PersonalEmail, 'x', now)  # same value, different class


def test_dict_values_are_order_insensitive_for_identity(fake_clock: FakeClock) -> None:
    class Blob(DataPoint[dict[str, Any]]): ...

    now = fake_clock.now()
    assert dp(Blob, {'a': 1, 'b': [1, 2]}, now) == dp(Blob, {'b': [1, 2], 'a': 1}, now)


def test_view_queries_are_subtype_aware_and_latest_picks_newest(fake_clock: FakeClock) -> None:
    now = fake_clock.now()
    work, personal, risk = (
        dp(WorkEmail, 'w', now),
        dp(PersonalEmail, 'p', now + timedelta(seconds=5)),
        dp(Risk, 0.5, now),
    )
    view = DataPointView([work, personal, risk])

    assert set(view.of_type(Email)) == {work, personal}
    assert view.of_type(WorkEmail) == (work,)
    assert view.latest(Email) == personal
    assert view.latest(Ip) is None
    assert view.present_types() == {WorkEmail, PersonalEmail, Risk}
    assert len(view) == 3 and list(view) == [work, personal, risk]


def test_emit_is_value_only_and_finalize_stamps_provenance(fake_clock: FakeClock) -> None:
    emission = Ip.emit('9.9.9.9')
    assert (emission.leaf_type, emission.value) == (Ip, '9.9.9.9')

    finalized = emission.finalize(retrieved_by=OperatorId('scanner'), at=fake_clock.now())
    assert isinstance(finalized, Ip)
    assert finalized.retrieved_by == 'scanner'
    assert finalized.first_retrieved == finalized.last_retrieved == fake_clock.now()


def test_reobserved_only_advances_last_retrieved(fake_clock: FakeClock) -> None:
    now = fake_clock.now()
    point = dp(Ip, '1.1.1.1', now)
    later = point.reobserved(now + timedelta(seconds=30))
    assert (later.first_retrieved, later.last_retrieved) == (now, now + timedelta(seconds=30))
    assert point.reobserved(now - timedelta(seconds=30)).last_retrieved == now  # never moves backwards
