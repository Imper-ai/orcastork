"""The ``CooldownGate`` port — an atomic, durable per-key cooldown.

Backs the manager's ``SchedulingGate``: may a new session for this key (a namespace, a device)
start now? ``try_acquire`` is check-and-arm in one atomic step — a ``True`` return
immediately starts the cooldown, so two racing callers can never both win — and the state
is owned by the backing store, so the cooldown holds across pods and restarts.
"""

from __future__ import annotations

from typing import Protocol


class CooldownGate(Protocol):
    async def try_acquire(self, key: str, cooldown_seconds: float) -> bool:
        """Atomically start a cooldown for ``key`` iff none is active; ``True`` means proceed."""
        ...
