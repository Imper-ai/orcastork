"""LOCK — SessionLock contract, run against the in-memory adapter (via CNF mixin).

Time-dependent contracts advance the injected ``FakeClock`` through ``advance_time``.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from orcastork.adapters.memory import InMemorySessionLock
from orcastork.ports import SessionLock

from .doubles.clock import FakeClock
from .doubles.conformance import LockConformance


class TestInMemorySessionLock(LockConformance):
    @pytest.fixture
    def clock(self) -> FakeClock:
        return FakeClock()

    @pytest.fixture
    def lock(self, clock: FakeClock) -> SessionLock:
        return InMemorySessionLock(clock)

    @pytest.fixture
    def advance_time(self, clock: FakeClock) -> Callable[[float], None]:
        return clock.advance
