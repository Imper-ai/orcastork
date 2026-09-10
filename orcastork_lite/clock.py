"""Injected time source.

All time-dependent logic (debounce windows, timeouts, retry backoff) reads the clock through
the :class:`Clock` protocol so tests can drive a deterministic fake instead of the wall clock.
Core code never calls ``datetime.now()`` / ``time.monotonic`` / ``asyncio.sleep`` directly.
"""

import asyncio
import time
from datetime import datetime, timezone
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    """A source of wall-clock timestamps, a monotonic counter, and an awaitable sleep."""

    def now(self) -> datetime:
        """Current timezone-aware UTC time (DataPoint observation timestamps)."""
        ...

    def monotonic(self) -> float:
        """Monotonic seconds (debounce / backoff arithmetic)."""
        ...

    async def sleep(self, seconds: float) -> None:
        """Wait ``seconds`` of monotonic time — the engine's only wait."""
        ...


class SystemClock:
    """Real clock — the production default."""

    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    def monotonic(self) -> float:
        return time.monotonic()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)
