"""The ``SessionLock`` port — liveness lock + the fencing epoch (correctness) + completion.

Acquiring ownership grants a lock (mutual exclusion, TTL-bounded) **and** mints an epoch
strictly greater than any prior epoch for the session. The lock is only a liveness hint;
the epoch is the correctness primitive that every write path validates. The lock expires
after its TTL when the holder dies, letting a successor acquire (with a higher epoch). The
session-completion flag is co-located with the epoch counter, so marking a session done is a
single compare-and-set against the live epoch — a fenced predecessor can never finalize.
"""

from __future__ import annotations

from typing import Protocol

from ..ids import Epoch, SessionId


class SessionLock(Protocol):
    async def acquire(self, session_id: SessionId) -> Epoch:
        """Grant the lock and mint a strictly-increasing epoch.

        Raises :class:`~orcastork.exceptions.LockHeldError` if a live lease
        is already held (mutual exclusion).
        """
        ...

    async def renew(self, session_id: SessionId, *, epoch: Epoch) -> None:
        """Extend the lease TTL while held (epoch must be current)."""
        ...

    async def release(self, session_id: SessionId, *, epoch: Epoch) -> None:
        """Release the lock for reuse (epoch must be current)."""
        ...

    async def current_epoch(self, session_id: SessionId) -> Epoch:
        """The highest epoch minted for the session (``0`` if never acquired)."""
        ...

    async def is_held(self, session_id: SessionId) -> bool:
        """Whether a live (un-expired) lease currently exists."""
        ...

    async def mark_complete(self, session_id: SessionId, *, epoch: Epoch) -> None:
        """Record that the session finished, gated atomically on the fencing epoch.

        The completion flag is co-located with the epoch counter, so the mark is a single
        compare-and-set: a writer whose ``epoch`` is below the current one (a fenced
        predecessor superseded by a takeover) is rejected with
        :class:`~orcastork.exceptions.StaleEpochError`. Idempotent for the
        current holder. A supervisor — even on another pod — reads :meth:`is_complete` to skip
        a finished session.
        """
        ...

    async def is_complete(self, session_id: SessionId) -> bool:
        """Whether the session has been marked complete (a supervisor must not resume it)."""
        ...

    async def clear_complete(self, session_id: SessionId) -> None:
        """Clear the completion flag so a completed session can be re-opened.

        Called by the manager before re-opening a session for late non-ephemeral data: the
        flag is removed so the next acquire succeeds and the re-opening orchestrator can
        mark_complete again once it has re-aggregated. This does NOT reset or alter the epoch
        counter — the re-open still acquires a fresh, strictly higher epoch so the re-aggregation
        write is fenced against any stale predecessor.
        """
        ...
