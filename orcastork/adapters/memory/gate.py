"""In-memory ``CooldownGate`` — per-key expiry decided against the injected clock."""

from __future__ import annotations

from ...clock import Clock


class InMemoryCooldownGate:
    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._expires_at: dict[str, float] = {}

    async def try_acquire(self, key: str, cooldown_seconds: float) -> bool:
        now = self._clock.monotonic()
        expires_at = self._expires_at.get(key)
        if expires_at is not None and now < expires_at:
            return False
        self._expires_at[key] = now + cooldown_seconds
        return True
