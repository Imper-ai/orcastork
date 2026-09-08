"""The ``RateLimiter`` port — fleet-level pacing of capability actions.

One namespace's many concurrent sessions all call the same third-party APIs; nothing inside a
single session can see that pressure, so pacing is a shared, injected concern. ``acquire``
is a token bucket per key: it returns immediately while budget remains and otherwise waits
(through the backend's own notion of time) until the action may proceed — it never fails,
it only paces. The capability base class acquires at the audited action seam, so an
operator cannot bypass it.
"""

from __future__ import annotations

from typing import Protocol


class RateLimiter(Protocol):
    async def acquire(self, key: str) -> None:
        """Wait until one action for ``key`` may proceed (token bucket; returns immediately when allowed)."""
        ...


class NullRateLimiter:
    """No-op limiter — the default when a deployment opts out of fleet-level pacing."""

    async def acquire(self, key: str) -> None:  # noqa: ARG002
        return None
