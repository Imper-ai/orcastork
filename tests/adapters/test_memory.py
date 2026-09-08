"""MEM — in-memory-adapter mechanics: determinism, infra-free, FakeClock-driven."""

from __future__ import annotations

import asyncio

import pytest

from orcastork.adapters.memory import (
    InMemoryDataPointStore,
    InMemoryInbox,
    InMemoryRateLimiter,
    InMemorySessionLock,
)
from orcastork.adapters.redis import RedisRateLimiter
from orcastork.ids import Epoch, SessionId, new_session_id
from orcastork.ports import NullRateLimiter
from orcastork.ports.datapoint_store import EffectClaim, effect_pending_state
from orcastork.runtime import build_in_memory_runtime

from ..doubles.clock import FakeClock
from ..doubles.datapoints import work_email


async def test_mem_01_runtime_round_trip_is_infra_free() -> None:
    # A fully in-memory runtime exposes all its ports and round-trips a DataPoint with no infra.
    runtime = build_in_memory_runtime(FakeClock())
    session = new_session_id()
    await runtime.store.write(session, [work_email('a@e.example')], epoch=Epoch(1))
    stored = (await runtime.store.snapshot(session)).all()
    assert len(stored) == 1
    assert all(
        adapter is not None
        for adapter in (
            runtime.store,
            runtime.inbox,
            runtime.lock,
            runtime.audit,
            runtime.durable,
            runtime.catalog,
            runtime.rate_limiter,
        )
    )


async def test_mem_02_lock_expiry_governed_by_fake_clock() -> None:
    clock = FakeClock()
    runtime = build_in_memory_runtime(clock)
    session = SessionId('mem-session')

    await runtime.lock.acquire(session)
    assert await runtime.lock.is_held(session)  # no wall-clock time passes on its own

    clock.advance(31.0)
    assert not await runtime.lock.is_held(session)  # expiry is driven purely by the FakeClock


async def test_mem_03_rate_limiter_burst_token_is_immediate_then_paced() -> None:
    clock = FakeClock()
    limiter = InMemoryRateLimiter(clock, rate_per_second=1.0, burst=1)

    await limiter.acquire('namespace-1:idp')
    assert clock.monotonic() == 0.0  # the burst token → immediate

    await limiter.acquire('namespace-1:idp')
    assert clock.monotonic() == 1.0  # waited (via clock.sleep) until the next token accrued


async def test_mem_04_rate_limiter_buckets_are_per_key() -> None:
    clock = FakeClock()
    limiter = InMemoryRateLimiter(clock, rate_per_second=1.0, burst=1)

    await limiter.acquire('namespace-1:idp')
    await limiter.acquire('namespace-2:idp')  # a different key draws from its own untouched bucket

    assert clock.monotonic() == 0.0


async def test_mem_05_rate_limiter_refill_caps_at_burst() -> None:
    clock = FakeClock()
    limiter = InMemoryRateLimiter(clock, rate_per_second=1.0, burst=2)
    await limiter.acquire('k')
    await limiter.acquire('k')  # the bucket is empty at t=0

    clock.advance(60.0)  # a long idle period refills at most `burst` tokens, never more

    await limiter.acquire('k')
    await limiter.acquire('k')
    assert clock.monotonic() == 60.0  # both came from the capped refill — immediate
    await limiter.acquire('k')
    assert clock.monotonic() == 61.0  # the third had to wait out a full period again


async def test_mem_06_null_rate_limiter_passes_straight_through() -> None:
    limiter = NullRateLimiter()

    # A short wall-clock bound proves each acquire returns immediately — the null limiter never
    # waits and never accumulates state across calls.
    for _ in range(3):
        await asyncio.wait_for(limiter.acquire('namespace-1:idp'), timeout=0.1)


async def test_mem_07_stale_release_does_not_free_successors_live_lease() -> None:
    # A fenced predecessor (epoch 1) must not be able to drop a successor's (epoch 2) live lease:
    # release is a no-op unless the stored lease belongs to the calling epoch (no split-brain).
    clock = FakeClock()
    lock = InMemorySessionLock(clock, ttl_seconds=30.0)
    session = SessionId('mem-stale-release')

    epoch1 = await lock.acquire(session)
    clock.advance(31.0)  # epoch 1's lease expires
    epoch2 = await lock.acquire(session)  # epoch 2 takes over, now the live holder
    assert int(epoch2) > int(epoch1)

    await lock.release(session, epoch=epoch1)  # the stale predecessor tries to release

    assert await lock.is_held(session) is True  # the successor's lease survived
    assert int(await lock.current_epoch(session)) == int(epoch2)


async def test_mem_08_rate_limiter_rejects_nonpositive_rate_and_subunit_burst() -> None:
    # The fail-fast guards protect the 'waits, never fails' runtime invariant: a zero/negative rate
    # would never refill (hang) and a sub-1 burst would divide-by-zero on the first contended acquire.
    clock = FakeClock()
    with pytest.raises(ValueError, match='rate_per_second'):
        InMemoryRateLimiter(clock, rate_per_second=0.0, burst=1)
    with pytest.raises(ValueError, match='rate_per_second'):
        InMemoryRateLimiter(clock, rate_per_second=-1.0, burst=1)
    with pytest.raises(ValueError, match='burst'):
        InMemoryRateLimiter(clock, rate_per_second=1.0, burst=0)
    with pytest.raises(ValueError, match='burst'):
        InMemoryRateLimiter(clock, rate_per_second=1.0, burst=-1)


async def test_mem_09_redis_rate_limiter_constructor_at_parity(redis_client: object) -> None:
    # The two adapters share the 'waits, never fails' contract, so they must reject the same
    # misconfig identically (the guards run before any I/O, so a live client is incidental here).
    with pytest.raises(ValueError, match='rate_per_second'):
        RedisRateLimiter(redis_client, FakeClock(), rate_per_second=0.0, burst=1)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match='burst'):
        RedisRateLimiter(redis_client, FakeClock(), rate_per_second=1.0, burst=0)  # type: ignore[arg-type]


async def test_mem_10_concurrent_waiters_never_share_one_token() -> None:
    # burst=1, rate=1.0: two coroutines both find the bucket empty and sleep. When the clock yields
    # exactly one token, only one waiter may take it; the other must re-check, find nothing, and
    # sleep a further full period. Total simulated wait is two periods — no over-admission.
    clock = FakeClock()
    limiter = InMemoryRateLimiter(clock, rate_per_second=1.0, burst=1)

    await limiter.acquire('k')  # drain the single burst token at t=0
    assert clock.monotonic() == 0.0

    finish_times: list[float] = []

    async def contend() -> None:
        await limiter.acquire('k')
        finish_times.append(clock.monotonic())

    # Both tasks start contended; each clock.sleep fast-forwards time and yields, so the two
    # interleave through the `while True` re-check rather than both grabbing the same token.
    await asyncio.gather(contend(), contend())

    assert sorted(finish_times) == [1.0, 2.0]  # one token per full period, the second waited again
    assert clock.monotonic() == 2.0  # two full periods of simulated wait — the token was not double-spent


async def test_mem_11_repeated_quarantine_is_idempotent_and_never_double_records() -> None:
    # A retried disposal (e.g. on resume) of an already-quarantined entry must be a safe no-op:
    # the message is already gone, so nothing is removed and no second record is appended.
    inbox = InMemoryInbox()
    session = SessionId('mem-quarantine-idem')

    await inbox.append(session, work_email('a@work.example'))
    (delivered,) = await inbox.consume(session)  # claim it so quarantine actually removes it

    await inbox.quarantine(session, delivered.entry_id, reason='poison', epoch=Epoch(1))
    assert len(await inbox.quarantined(session)) == 1

    await inbox.quarantine(session, delivered.entry_id, reason='poison', epoch=Epoch(1))  # retry

    assert len(await inbox.quarantined(session)) == 1  # no duplicate record
    assert await inbox.pending_count(session) == 0


async def test_mem_12_commit_over_foreign_pending_mark_is_a_no_op() -> None:
    # A successor at epoch 2 that has NOT reclaimed a predecessor's 'pending:1' must never fabricate
    # 'committed' for that unowned claim — commit only transitions this epoch's own pending mark.
    store = InMemoryDataPointStore()
    session = SessionId('mem-commit-foreign')
    key = 'send-email'

    assert await store.claim_effect(session, key, epoch=Epoch(1), reclaim_stale=False) == EffectClaim.ACQUIRED

    await store.commit_effect(session, key, epoch=Epoch(2))  # successor, no reclaim

    assert await store.get_effect_state(session, key) == effect_pending_state(Epoch(1))  # left intact
    # The recovery policy is still in force: the predecessor's mid-effect claim is reported as stale.
    assert (
        await store.claim_effect(session, key, epoch=Epoch(2), reclaim_stale=False) == EffectClaim.PENDING_STALE_EPOCH
    )
