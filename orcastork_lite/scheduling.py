"""Scheduling primitives: readiness, coalescing windows, the cycle breaker, retry backoff.

Readiness is the data-driven trigger: an operator is ready iff all its ``depends_on`` types are
present (subtype-aware) and all its ``requires`` capabilities are available. Reruns and retries
are both **loop-scheduled windows** on the injected clock (:class:`DebounceController`) — never
an in-task sleep — so the gathering loop stays in charge of time.
"""

from __future__ import annotations

import random
import zlib
from datetime import timedelta
from typing import Any

from .capabilities import Capability
from .clock import Clock
from .datapoints import DataPoint
from .ids import OperatorId
from .operators import Operator, RetryPolicy

DEFAULT_DEBOUNCE = timedelta(0)


def is_ready(
    operator: type[Operator],
    *,
    present_types: frozenset[type[DataPoint[Any]]],
    available_capability_types: frozenset[type[Capability]],
) -> bool:
    """Whether ``operator`` can run given the present DataPoint types + available capabilities."""
    data_ready = all(
        any(issubclass(present, required) for present in present_types) for required in operator.depends_on
    )
    caps_ready = all(
        any(issubclass(available, required) for available in available_capability_types)
        for required in operator.requires
    )
    return data_ready and caps_ready


class DebounceController:
    """Tracks, per operator, the monotonic time at which a coalesced rerun (or a retry) becomes due."""

    def __init__(self, clock: Clock, *, default_window: timedelta = DEFAULT_DEBOUNCE) -> None:
        self._clock = clock
        self._default_window = default_window
        self._due_at: dict[OperatorId, float] = {}

    def schedule(self, operator_id: OperatorId, *, window: timedelta | None = None) -> None:
        """(Re)arm a window for ``operator_id``; arrivals within it coalesce."""
        effective = self._default_window if window is None else window
        self._due_at[operator_id] = self._clock.monotonic() + effective.total_seconds()

    def is_scheduled(self, operator_id: OperatorId) -> bool:
        return operator_id in self._due_at

    def due_at(self, operator_id: OperatorId) -> float | None:
        return self._due_at.get(operator_id)

    def is_due(self, operator_id: OperatorId) -> bool:
        due = self._due_at.get(operator_id)
        return due is not None and self._clock.monotonic() >= due

    def clear(self, operator_id: OperatorId) -> None:
        """Consume a window (once the orchestrator has launched the operator)."""
        self._due_at.pop(operator_id, None)


class CircuitBreaker:
    """Bounds a permitted cycle per session: an operator at its ``max_cycles`` cap is *tripped*."""

    def __init__(self, caps: dict[OperatorId, int]) -> None:
        self._caps = dict(caps)
        self._counts: dict[OperatorId, int] = {}

    def record_run(self, operator_id: OperatorId) -> None:
        self._counts[operator_id] = self._counts.get(operator_id, 0) + 1

    def is_tripped(self, operator_id: OperatorId) -> bool:
        cap = self._caps.get(operator_id)
        return cap is not None and self._counts.get(operator_id, 0) >= cap


def seed_for(*parts: str) -> int:
    """A stable jitter seed derived from e.g. ``(session_id, operator_id)``."""
    return zlib.crc32('|'.join(parts).encode())


def backoff_delays(policy: RetryPolicy, *, seed: int) -> list[float]:
    """The jittered exponential backoff schedule — deterministic for a given seed."""
    rng = random.Random(seed)
    return [
        policy.base_delay * (2**attempt) * (1 + rng.uniform(-policy.jitter, policy.jitter))
        for attempt in range(policy.max_attempts)
    ]
