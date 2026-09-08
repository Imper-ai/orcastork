"""Redis ``CooldownGate`` — ``SET NX PX``: the key exists exactly while the cooldown runs.

Redis owns the expiry, so the cooldown holds across pods and restarts by construction; the
``NX`` set is the atomic check-and-arm.
"""

from __future__ import annotations

from typing import Any

from redis.asyncio import Redis


class RedisCooldownGate:
    def __init__(self, redis: Redis) -> None:
        self._redis: Any = redis  # see RedisDataPointStore: redis-py's sync/async union typing

    async def try_acquire(self, key: str, cooldown_seconds: float) -> bool:
        return bool(await self._redis.set(f'cooldown:{key}', '1', nx=True, px=max(int(cooldown_seconds * 1000), 1)))
