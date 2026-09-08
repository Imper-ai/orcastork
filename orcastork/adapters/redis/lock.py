"""Redis ``SessionLock`` — a ``PX`` liveness lease whose value is a monotonic minted epoch.

The lock value is the minted epoch, so acquire/release/renew are compare-by-epoch (Lua) — a stale
holder cannot release or extend a successor's lease. The epoch counter (``mint:{session}``)
only ever increases and advances only when a lease is actually granted, so a non-zero epoch means
the session was started at some point; lease expiry is Redis's own TTL (deterministic under
fakeredis time control in tests).
"""

from __future__ import annotations

from typing import Any

from redis.asyncio import Redis

from ...exceptions import LockHeldError, StaleEpochError
from ...ids import Epoch, SessionId
from .ttl import DEFAULT_STATE_TTL_MS, slide_ttl

DEFAULT_TTL_MS = 30_000

# KEYS[1]=lock key, KEYS[2]=mint key, ARGV[1]=lease TTL ms. Minting and taking the lease must be one
# atomic step, not INCR-then-SET: between those two calls the mint reads as advanced while no lease is
# held, which is indistinguishable from a started session whose owner died — and a caller that treats a
# minted epoch as "already started" (the manager's resume guard) would act on a session mid-acquire.
# Returns the minted epoch, or 0 when a live lease already holds the lock.
_ACQUIRE = (
    "if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end\n"
    "local epoch = redis.call('INCR', KEYS[2])\n"
    "redis.call('SET', KEYS[1], epoch, 'PX', ARGV[1])\nreturn epoch"
)
_RELEASE = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end\nreturn 0"
_RENEW = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) end\nreturn 0"
# KEYS[1]=mint key, KEYS[2]=complete key, ARGV[1]=epoch. Compare against the live epoch and set the
# completion flag in one atomic script — a writer below the current epoch (a fenced predecessor) loses.
_COMPLETE = (
    "if tonumber(redis.call('GET', KEYS[1]) or '0') > tonumber(ARGV[1]) then return 0 end\n"
    "redis.call('SET', KEYS[2], '1')\nreturn 1"
)


class RedisSessionLock:
    def __init__(
        self, redis: Redis, *, ttl_ms: int = DEFAULT_TTL_MS, state_ttl_ms: int = DEFAULT_STATE_TTL_MS
    ) -> None:
        self._redis: Any = redis  # see RedisDataPointStore: redis-py's sync/async union typing
        self._ttl_ms = ttl_ms
        # The lease (`lock` key) lives for ttl_ms; the mint/complete counters outlive it by state_ttl_ms
        # so fencing stays monotonic and completion stays readable well beyond any recovery window.
        self._state_ttl_ms = state_ttl_ms

    @staticmethod
    def _lock_key(session_id: SessionId) -> str:
        return f'lock:{session_id}'

    @staticmethod
    def _mint_key(session_id: SessionId) -> str:
        return f'mint:{session_id}'

    @staticmethod
    def _complete_key(session_id: SessionId) -> str:
        return f'complete:{session_id}'

    async def acquire(self, session_id: SessionId) -> Epoch:
        epoch = int(
            await self._redis.eval(
                _ACQUIRE, 2, self._lock_key(session_id), self._mint_key(session_id), str(self._ttl_ms)
            )
        )
        if epoch == 0:
            raise LockHeldError(f'session {session_id} is already locked')
        await slide_ttl(self._redis, self._state_ttl_ms, self._mint_key(session_id))
        return Epoch(epoch)

    async def renew(self, session_id: SessionId, *, epoch: Epoch) -> None:
        extended = await self._redis.eval(_RENEW, 1, self._lock_key(session_id), str(int(epoch)), str(self._ttl_ms))
        if not extended:
            raise StaleEpochError(f'epoch {epoch} does not hold the lock for session {session_id}')

    async def release(self, session_id: SessionId, *, epoch: Epoch) -> None:
        await self._redis.eval(_RELEASE, 1, self._lock_key(session_id), str(int(epoch)))

    async def current_epoch(self, session_id: SessionId) -> Epoch:
        return Epoch(int(await self._redis.get(self._mint_key(session_id)) or 0))

    async def is_held(self, session_id: SessionId) -> bool:
        return bool(await self._redis.exists(self._lock_key(session_id)))

    async def mark_complete(self, session_id: SessionId, *, epoch: Epoch) -> None:
        marked = await self._redis.eval(
            _COMPLETE, 2, self._mint_key(session_id), self._complete_key(session_id), str(int(epoch))
        )
        if not marked:
            raise StaleEpochError(f'epoch {epoch} is stale for session {session_id}')
        await slide_ttl(self._redis, self._state_ttl_ms, self._mint_key(session_id), self._complete_key(session_id))

    async def is_complete(self, session_id: SessionId) -> bool:
        return bool(await self._redis.exists(self._complete_key(session_id)))

    async def clear_complete(self, session_id: SessionId) -> None:
        await self._redis.delete(self._complete_key(session_id))
