"""Readiness, coalescing windows, the circuit breaker, and the backoff schedule."""

from __future__ import annotations

from datetime import timedelta

import pytest

from orcastork_lite import OperatorId, RetryPolicy
from orcastork_lite.scheduling import CircuitBreaker, DebounceController, backoff_delays, is_ready, seed_for

from ..doubles.clock import FakeClock
from .conftest import Email, Ip, Risk, WorkEmail, make_capability, make_operator

OP = OperatorId('op')


def test_readiness_needs_every_dependency_and_capability_subtype_aware() -> None:
    cap = make_capability('geo')
    operator = make_operator('op', depends_on={Email, Ip}, requires={cap})

    assert not is_ready(operator, present_types=frozenset({WorkEmail}), available_capability_types=frozenset({cap}))
    assert not is_ready(operator, present_types=frozenset({WorkEmail, Ip}), available_capability_types=frozenset())
    assert is_ready(operator, present_types=frozenset({WorkEmail, Ip}), available_capability_types=frozenset({cap}))
    assert is_ready(make_operator('free'), present_types=frozenset(), available_capability_types=frozenset())


def test_uses_never_gates_readiness() -> None:
    operator = make_operator('op', depends_on={Ip}, uses={Risk})
    assert is_ready(operator, present_types=frozenset({Ip}), available_capability_types=frozenset())


def test_debounce_window_becomes_due_when_the_clock_advances(fake_clock: FakeClock) -> None:
    debounce = DebounceController(fake_clock)
    assert not debounce.is_scheduled(OP) and debounce.due_at(OP) is None

    debounce.schedule(OP, window=timedelta(seconds=2))
    assert debounce.is_scheduled(OP) and not debounce.is_due(OP)
    fake_clock.advance(2)
    assert debounce.is_due(OP)
    debounce.clear(OP)
    assert not debounce.is_scheduled(OP)

    debounce.schedule(OP)  # the default window is zero: due immediately
    assert debounce.is_due(OP)


def test_circuit_breaker_trips_at_the_cap_and_ignores_uncapped_operators() -> None:
    breaker = CircuitBreaker({OP: 2})
    breaker.record_run(OP)
    assert not breaker.is_tripped(OP)
    breaker.record_run(OP)
    assert breaker.is_tripped(OP)
    for _ in range(10):
        breaker.record_run(OperatorId('free'))
    assert not breaker.is_tripped(OperatorId('free'))


def test_backoff_is_exponential_deterministic_and_bounded_by_jitter() -> None:
    policy = RetryPolicy(max_attempts=4, base_delay=1.0, jitter=0.0)
    assert backoff_delays(policy, seed=seed_for('s', 'op')) == [1.0, 2.0, 4.0, 8.0]

    jittered = RetryPolicy(max_attempts=3, base_delay=1.0, jitter=0.5)
    first = backoff_delays(jittered, seed=seed_for('s', 'op'))
    assert first == backoff_delays(jittered, seed=seed_for('s', 'op'))
    assert all(0.5 * 2**i <= delay <= 1.5 * 2**i for i, delay in enumerate(first))


@pytest.mark.parametrize('kwargs', [{'max_attempts': 0}, {'base_delay': -1.0}, {'jitter': 1.5}])
def test_retry_policy_rejects_degenerate_values(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)  # type: ignore[arg-type]
