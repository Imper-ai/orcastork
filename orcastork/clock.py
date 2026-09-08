"""Injected time source.

All time-dependent logic (debounce windows, timeouts, lock TTLs, backoff) reads the
clock through the :class:`Clock` protocol so tests can drive a deterministic
``FakeClock`` instead of the wall clock. Core code must never call
``datetime.now()`` / ``time.monotonic`` / ``asyncio.sleep`` directly.
"""

import asyncio
import time
from datetime import datetime, timezone
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    """A source of wall-clock timestamps, a monotonic counter, and an awaitable sleep."""

    def now(self) -> datetime:
        """Current timezone-aware UTC time (used for DataPoint timestamps, audit)."""
        ...

    def monotonic(self) -> float:
        """Monotonic seconds (used for debounce/timeout/backoff arithmetic)."""
        ...

    async def sleep(self, seconds: float) -> None:
        """Wait ``seconds`` of monotonic time (the engine's only wait — never ``asyncio.sleep``)."""
        ...


class SystemClock:
    """Real clock — the production default."""

    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    def monotonic(self) -> float:
        return time.monotonic()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)
