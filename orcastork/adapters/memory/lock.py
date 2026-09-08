"""In-memory ``SessionLock`` — a TTL lease (liveness) plus a monotonic epoch (correctness).

The epoch counter only ever increases, so every grant (including taking over an expired
lease) mints a strictly higher epoch. Liveness is decided against the injected clock, so
TTL expiry is deterministic in tests.
"""

from __future__ import annotations

from dataclasses import dataclass

from ...clock import Clock
from ...exceptions import LockHeldError, StaleEpochError
from ...ids import Epoch, SessionId

DEFAULT_TTL_SECONDS = 30.0


@dataclass
class _Lease:
    epoch: int
    expires_at: float


@dataclass
class _LockState:
    epoch_counter: int = 0
    lease: _Lease | None = None
    complete: bool = False


class InMemorySessionLock:
    def __init__(self, clock: Clock, *, ttl_seconds: float = DEFAULT_TTL_SECONDS) -> None:
        self._clock = clock
        self._ttl = ttl_seconds
        self._sessions: dict[SessionId, _LockState] = {}

    def _state(self, session_id: SessionId) -> _LockState:
        return self._sessions.setdefault(session_id, _LockState())

    def _is_live(self, state: _LockState) -> bool:
        return state.lease is not None and self._clock.monotonic() < state.lease.expires_at

    async def acquire(self, session_id: SessionId) -> Epoch:
        state = self._state(session_id)
        if self._is_live(state):
            raise LockHeldError(f'session {session_id} is already locked')
        state.epoch_counter += 1
        state.lease = _Lease(epoch=state.epoch_counter, expires_at=self._clock.monotonic() + self._ttl)
        return Epoch(state.epoch_counter)

    async def renew(self, session_id: SessionId, *, epoch: Epoch) -> None:
        state = self._state(session_id)
        if state.lease is None or state.lease.epoch != epoch or not self._is_live(state):
            raise StaleEpochError(f'epoch {epoch} does not hold the lock for session {session_id}')
        state.lease.expires_at = self._clock.monotonic() + self._ttl

    async def release(self, session_id: SessionId, *, epoch: Epoch) -> None:
        state = self._state(session_id)
        if state.lease is not None and state.lease.epoch == epoch:
            state.lease = None

    async def current_epoch(self, session_id: SessionId) -> Epoch:
        return Epoch(self._state(session_id).epoch_counter)

    async def is_held(self, session_id: SessionId) -> bool:
        return self._is_live(self._state(session_id))

    async def mark_complete(self, session_id: SessionId, *, epoch: Epoch) -> None:
        # The flag and the epoch counter live in one state object mutated under the single async
        # loop, so this compare-and-set is atomic: a writer below the current epoch (a fenced
        # predecessor superseded by a takeover) is rejected and cannot finalize the session.
        state = self._state(session_id)
        if int(epoch) < state.epoch_counter:
            raise StaleEpochError(f'epoch {epoch} is stale for session {session_id}')
        state.complete = True

    async def is_complete(self, session_id: SessionId) -> bool:
        return self._state(session_id).complete

    async def clear_complete(self, session_id: SessionId) -> None:
        self._state(session_id).complete = False
