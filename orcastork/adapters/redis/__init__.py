"""Redis adapters (extra ``redis``) — Lua-CAS store, Streams inbox, fencing lock, cooldown gate,
token-bucket rate limiter.

Each adapter takes a ``redis.asyncio.Redis`` client (constructed with
``decode_responses=True``); the framework owns this code directly and does not depend on
``common``.
"""

from .gate import RedisCooldownGate
from .inbox import RedisStreamsInbox
from .lock import RedisSessionLock
from .rate_limiter import RedisRateLimiter
from .store import RedisDataPointStore

__all__ = ['RedisCooldownGate', 'RedisDataPointStore', 'RedisRateLimiter', 'RedisSessionLock', 'RedisStreamsInbox']
