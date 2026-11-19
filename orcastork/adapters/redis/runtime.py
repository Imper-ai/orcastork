"""``build_redis_runtime`` — one Redis client, one key prefix, one state TTL, every Redis port wired.

The store, inbox and lock share the client, the prefix and the state TTL, so a session's keys all
expire together and sit under one prefix in a shared database. The ports Redis does not back are
the no-op ones (audit, archive, durable store) unless the caller passes its own; the capability
catalog defaults to an empty in-memory one.
"""

from __future__ import annotations

from redis.asyncio import Redis

from ...clock import Clock, SystemClock
from ...ports import (
    AuditSink,
    CapabilityCatalog,
    NullAuditSink,
    NullDataPointArchive,
    NullDurableStore,
    NullRateLimiter,
    RateLimiter,
)
from ...runtime import OrchestratorRuntime
from ...telemetry import Telemetry
from ..memory import InMemoryCapabilityCatalog
from .inbox import RedisStreamsInbox
from .lock import DEFAULT_TTL_MS, RedisSessionLock
from .store import RedisDataPointStore
from .ttl import DEFAULT_STATE_TTL_MS


def build_redis_runtime(
    redis: Redis,
    *,
    key_prefix: str = '',
    state_ttl_ms: int = DEFAULT_STATE_TTL_MS,
    lock_ttl_ms: int = DEFAULT_TTL_MS,
    clock: Clock | None = None,
    catalog: CapabilityCatalog | None = None,
    rate_limiter: RateLimiter | None = None,
    audit: AuditSink | None = None,
    telemetry: Telemetry | None = None,
) -> OrchestratorRuntime:
    """Build a runtime whose store, inbox and lock live in Redis under ``key_prefix``.

    ``redis`` must decode responses (``decode_responses=True``). ``state_ttl_ms`` is the sliding TTL of
    every per-session key, so it must outlast the longest a session may sit parked.
    """
    return OrchestratorRuntime(
        clock=clock or SystemClock(),
        store=RedisDataPointStore(redis, state_ttl_ms=state_ttl_ms, key_prefix=key_prefix),
        inbox=RedisStreamsInbox(redis, state_ttl_ms=state_ttl_ms, key_prefix=key_prefix),
        lock=RedisSessionLock(redis, ttl_ms=lock_ttl_ms, state_ttl_ms=state_ttl_ms, key_prefix=key_prefix),
        audit=audit or NullAuditSink(),
        archive=NullDataPointArchive(),
        durable=NullDurableStore(),
        catalog=catalog or InMemoryCapabilityCatalog(),
        rate_limiter=rate_limiter or NullRateLimiter(),
        telemetry=telemetry or Telemetry(),
    )
