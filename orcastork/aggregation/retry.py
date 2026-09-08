"""Bounded retry with jittered backoff, then dead-letter.

An aggregator gets bounded retries (e.g. on an OCC version conflict) with a jittered
exponential backoff; after ``max_attempts`` it is dead-lettered (the failure surfaces as
:class:`AggregatorDeadLetteredError`) rather than wedging the session. The jitter is
seeded for reproducibility, and the sleeper is injectable so tests run instantly.
"""

from __future__ import annotations

import asyncio
import random
import zlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from ..exceptions import AggregatorDeadLetteredError

# Notified after each attempt with its 1-based number and the error it raised (None on success).
OnAttempt = Callable[[int, Exception | None], Awaitable[None]]


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 5
    base_delay: float = 0.05  # seconds; the schedule is base * 2**attempt
    jitter: float = 0.2  # ±20% multiplicative jitter

    def __post_init__(self) -> None:
        # Fail fast on a misconfigured policy rather than dead-lettering immediately (max_attempts < 1)
        # or producing a negative/degenerate backoff schedule.
        if self.max_attempts < 1:
            raise ValueError(f'max_attempts must be >= 1, got {self.max_attempts}')
        if self.base_delay < 0.0:
            raise ValueError(f'base_delay must be >= 0.0, got {self.base_delay}')
        if not 0.0 <= self.jitter <= 1.0:
            raise ValueError(f'jitter must be within [0.0, 1.0], got {self.jitter}')


def seed_for(*parts: str) -> int:
    """A stable (cross-process) jitter seed derived from e.g. (session_id, operator_id)."""
    return zlib.crc32('|'.join(parts).encode())


def backoff_delays(policy: RetryPolicy, *, seed: int) -> list[float]:
    """The jittered exponential backoff schedule — deterministic for a given seed."""
    rng = random.Random(seed)
    return [
        policy.base_delay * (2**attempt) * (1 + rng.uniform(-policy.jitter, policy.jitter))
        for attempt in range(policy.max_attempts)
    ]


async def run_with_retry(
    operation: Callable[[], Awaitable[None]],
    *,
    policy: RetryPolicy,
    seed: int,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    on_attempt: OnAttempt | None = None,
) -> None:
    """Run ``operation``, retrying on any failure with jittered backoff; dead-letter after N.

    ``on_attempt`` (if given) is notified after every attempt with its 1-based number and the
    error it raised (``None`` on success) — the caller's hook for auditing each retry.
    """
    delays = backoff_delays(policy, seed=seed)
    last_error: Exception | None = None
    for attempt in range(policy.max_attempts):
        attempt_error: Exception | None = None
        try:
            await operation()
        except Exception as error:  # aggregator retry boundary — bounded then dead-lettered
            attempt_error = last_error = error
        # The hook runs outside the try/except so a failing hook propagates rather than being mistaken
        # for an operation failure — which would replay an already-successful operation's side effects.
        if on_attempt is not None:
            await on_attempt(attempt + 1, attempt_error)
        if attempt_error is None:
            return
        if attempt < policy.max_attempts - 1:
            await sleep(delays[attempt])
    raise AggregatorDeadLetteredError(f'dead-lettered after {policy.max_attempts} attempts') from last_error
