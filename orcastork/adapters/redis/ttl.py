"""Sliding-TTL helper shared by the Redis adapters.

Every per-session key — the store hashes (``dp``/``added``/``upd``), the revision/epoch counters,
the inbox stream, the ``mint``/``complete`` lock counters — carries a long TTL that is refreshed on
each write. An active session therefore never expires, while a finished one is reclaimed, bounding
Redis growth. The TTL (24h by default) is far larger than the second-scale fencing and recovery
windows, so it never interferes with lease takeover, epoch monotonicity, or completion reads.
"""

from __future__ import annotations

from typing import Any

DEFAULT_STATE_TTL_MS = 86_400_000  # 24 hours


async def slide_ttl(redis: Any, ttl_ms: int, *keys: str) -> None:
    """Refresh the TTL on each key in one round-trip (``pexpire`` on a missing key is a no-op)."""
    async with redis.pipeline(transaction=False) as pipe:
        for key in keys:
            pipe.pexpire(key, ttl_ms)
        await pipe.execute()
