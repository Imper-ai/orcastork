"""``FakeClock`` — a deterministic :class:`~orcastork.clock.Clock`.

Time only moves when a test advances it, so debounce windows, timeouts, lock TTLs and
backoff are exercised without any wall-clock dependency.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone


class FakeClock:
    def __init__(self, start: datetime | None = None) -> None:
        self._now = start or datetime(2026, 1, 1, tzinfo=timezone.utc)
        self._monotonic = 0.0

    def now(self) -> datetime:
        return self._now

    def monotonic(self) -> float:
        return self._monotonic

    def advance(self, seconds: float) -> None:
        """Move both the wall clock and the monotonic counter forward by ``seconds``."""
        self._now = self._now + timedelta(seconds=seconds)
        self._monotonic += seconds

    async def sleep(self, seconds: float) -> None:
        """Deterministic sleep: fast-forward the clock instead of waiting on the wall clock."""
        self.advance(seconds)
        await asyncio.sleep(0)  # yield, so tasks running concurrently with the sleeper interleave
