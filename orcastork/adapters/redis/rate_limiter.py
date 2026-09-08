"""Redis ``RateLimiter`` — an atomic Lua token bucket shared by every pod of the fleet.

The bucket state (``tokens`` + ``refill_ms``) lives in a hash keyed ``ratelimit:{key}``, and
one Lua script refills, takes a token, and reports the wait — atomically, so concurrent
sessions across pods can never overdraw the bucket. The script computes the refill from a
caller-supplied now-ms taken off the **injected clock** (wall time, comparable across pods),
so tests drive it deterministically with a ``FakeClock``; waiting also goes through the
injected clock. The key expires once the bucket would be full again — an absent key reads
as a full bucket, so the expiry is semantically lossless and state stays bounded.
"""

from __future__ import annotations

import math
from typing import Any

from redis.asyncio import Redis

from ...clock import Clock

# KEYS[1] = bucket hash; ARGV = now_ms, rate_per_ms, burst, ttl_ms. Returns 0 (token taken,
# proceed) or the ms until the next token becomes available. A negative elapsed (another
# pod's later write, clock skew) is clamped so the bucket never refills backwards.
_ACQUIRE_SCRIPT = """
local now_ms = tonumber(ARGV[1])
local rate_per_ms = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])
local state = redis.call('HMGET', KEYS[1], 'tokens', 'refill_ms')
local tokens = tonumber(state[1])
local refill_ms = tonumber(state[2])
if tokens == nil then
  tokens = burst
  refill_ms = now_ms
end
local elapsed = math.max(now_ms - refill_ms, 0)
tokens = math.min(burst, tokens + elapsed * rate_per_ms)
local wait_ms = 0
if tokens >= 1 then
  tokens = tokens - 1
else
  wait_ms = math.ceil((1 - tokens) / rate_per_ms)
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'refill_ms', now_ms)
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return wait_ms
"""


class RedisRateLimiter:
    def __init__(self, redis: Redis, clock: Clock, *, rate_per_second: float, burst: int) -> None:
        if rate_per_second <= 0.0:
            raise ValueError(f'rate_per_second must be > 0.0, got {rate_per_second}')
        if burst < 1:
            raise ValueError(f'burst must be >= 1, got {burst}')
        # See RedisDataPointStore: redis-py types its commands as a sync/async union, so the client
        # is held as Any internally while the public param stays typed.
        self._redis: Any = redis
        self._clock = clock
        self._rate_per_ms = rate_per_second / 1000.0
        self._burst = burst
        # Once a full refill's worth of time has passed, the stored state equals the absent-key
        # default (a full bucket), so expiring then loses nothing and bounds Redis growth.
        self._ttl_ms = math.ceil(burst / self._rate_per_ms)

    async def acquire(self, key: str) -> None:
        # Re-run the script after every sleep: another session may have taken the token this one
        # slept for, in which case the script reports the next wait.
        while True:
            now_ms = int(self._clock.now().timestamp() * 1000)
            wait_ms = int(
                await self._redis.eval(
                    _ACQUIRE_SCRIPT,
                    1,
                    f'ratelimit:{key}',
                    str(now_ms),
                    str(self._rate_per_ms),
                    str(self._burst),
                    str(self._ttl_ms),
                )
            )
            if wait_ms == 0:
                return
            await self._clock.sleep(wait_ms / 1000.0)
