"""In-memory ``RateLimiter`` — a classic token bucket per key on the injected clock.

Single-process only (the Redis adapter coordinates across pods); deterministic under a
``FakeClock`` because both the refill arithmetic (``monotonic``) and the wait (``sleep``)
go through the injected clock.
"""

from __future__ import annotations

from dataclasses import dataclass

from ...clock import Clock


@dataclass
class _Bucket:
    tokens: float
    refilled_at: float  # monotonic time of the last refill computation


class InMemoryRateLimiter:
    def __init__(self, clock: Clock, *, rate_per_second: float, burst: int) -> None:
        # Fail fast on a misconfigured limiter rather than dividing by zero (or never refilling) on
        # the first contended acquire.
        if rate_per_second <= 0.0:
            raise ValueError(f'rate_per_second must be > 0.0, got {rate_per_second}')
        if burst < 1:
            raise ValueError(f'burst must be >= 1, got {burst}')
        self._clock = clock
        self._rate = rate_per_second
        self._burst = burst
        self._buckets: dict[str, _Bucket] = {}

    async def acquire(self, key: str) -> None:
        # Re-check after every sleep: a concurrent waiter may have taken the token this one slept for.
        while True:
            now = self._clock.monotonic()
            bucket = self._buckets.setdefault(key, _Bucket(tokens=float(self._burst), refilled_at=now))
            bucket.tokens = min(float(self._burst), bucket.tokens + (now - bucket.refilled_at) * self._rate)
            bucket.refilled_at = now
            if bucket.tokens >= 1.0:
                bucket.tokens -= 1.0
                return
            await self._clock.sleep((1.0 - bucket.tokens) / self._rate)
