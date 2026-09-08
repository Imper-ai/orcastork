"""RZ — Redis adapters: the CNF contracts bound to Redis + backend-specific mechanics.

These run against in-process ``fakeredis`` (which supports Lua, Streams + consumer groups,
``XAUTOCLAIM`` and ``INCR``), so they execute in the default test run and give CNF parity
between the in-memory and Redis adapters. RZ-07 (transient-connection retry) is a thin
production wrapper and is deferred.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from typing import Any

import pytest
from fakeredis.aioredis import FakeRedis
from redis.exceptions import ResponseError

from orcastork.adapters.redis import (
    RedisCooldownGate,
    RedisDataPointStore,
    RedisRateLimiter,
    RedisSessionLock,
    RedisStreamsInbox,
)
from orcastork.exceptions import StaleEpochError
from orcastork.ids import Epoch, OperatorId, Revision, SessionId
from orcastork.ports import CooldownGate, DataPointStore, Inbox, InboxEntry, SessionLock

from ..doubles.clock import FakeClock
from ..doubles.conformance import (
    AppendRaw,
    CooldownGateConformance,
    InboxConformance,
    LockConformance,
    StoreConformance,
)
from ..doubles.datapoints import T0, personal_email, work_email
from ..doubles.redis_support import TimeControlledServer

RZ = SessionId('rz-session')
RZ_OTHER = SessionId('rz-other-session')
RZ_OP = OperatorId('rz-op')
T2 = T0 + timedelta(hours=2)


class TestRedisStore(StoreConformance):
    @pytest.fixture
    def store(self, redis_client: FakeRedis) -> DataPointStore:
        return RedisDataPointStore(redis_client)


class TestRedisInbox(InboxConformance):
    @pytest.fixture
    def inbox(self, redis_client: FakeRedis) -> Inbox:
        return RedisStreamsInbox(redis_client)

    @pytest.fixture
    def append_raw(self, redis_client: FakeRedis) -> AppendRaw:
        # A foreign producer writes straight to the stream — exactly the front door's XADD seam.
        async def _append(session_id: SessionId, payload: str) -> str:
            return str(await redis_client.xadd(f'inbox:{session_id}', {'data': payload}))

        return _append


class TestRedisSessionLock(LockConformance):
    @pytest.fixture
    def lock(self, redis_client: FakeRedis) -> SessionLock:
        return RedisSessionLock(redis_client)

    @pytest.fixture
    def advance_time(self, redis_server: TimeControlledServer) -> Callable[[float], None]:
        return redis_server.advance


async def test_rz_02_lua_cas_rejects_stale_epoch_without_partial_write(redis_client: FakeRedis) -> None:
    store = RedisDataPointStore(redis_client)
    await store.write(RZ, [work_email('a@e.example')], epoch=Epoch(2))
    with pytest.raises(StaleEpochError):
        await store.write(RZ, [personal_email('b@e.example')], epoch=Epoch(1))
    values = {dp.value for dp in (await store.snapshot(RZ)).all()}
    assert 'b@e.example' not in values  # the rejected write left no partial state


async def test_rz_03_streams_redelivery_on_crash_before_ack(redis_client: FakeRedis) -> None:
    inbox = RedisStreamsInbox(redis_client)
    await inbox.append(RZ, work_email('a@e.example'))
    assert len(await inbox.consume(RZ)) == 1  # claimed (XREADGROUP), but not acked
    reclaimed = await inbox.reclaim(RZ)  # XAUTOCLAIM re-presents it
    assert len(reclaimed) == 1 and reclaimed[0].delivery_count == 2


async def test_rz_04_epoch_incr_is_atomic_and_monotonic(
    redis_client: FakeRedis, redis_server: TimeControlledServer
) -> None:
    lock = RedisSessionLock(redis_client)
    first = await lock.acquire(RZ)
    redis_server.advance(31.0)  # lease expires
    second = await lock.acquire(RZ)
    assert int(first) == 1 and int(second) == 2


async def test_rz_06_consumer_group_delivers_each_entry_once(redis_client: FakeRedis) -> None:
    inbox = RedisStreamsInbox(redis_client)
    await inbox.append(RZ, work_email('a@e.example'))
    first = await inbox.consume(RZ)
    second = await inbox.consume(RZ)  # already claimed → not re-delivered by XREADGROUP '>'
    assert len(first) == 1 and len(second) == 0


async def test_rz_08_session_keys_carry_a_ttl_so_state_is_bounded(redis_client: FakeRedis) -> None:
    # Session state must not accumulate in Redis forever: a write/append sets (and slides) a TTL on
    # the per-session keys. pttl returns ms-remaining (-1 = no expiry, -2 = missing), so > 0 means set.
    store = RedisDataPointStore(redis_client, state_ttl_ms=60_000)
    inbox = RedisStreamsInbox(redis_client, state_ttl_ms=60_000)
    await store.write(RZ, [work_email('a@e.example')], epoch=Epoch(1))
    await inbox.append(RZ, work_email('a@e.example'))
    assert await redis_client.pttl(f'dp:{RZ}') > 0  # the store hash is bounded by a sliding TTL
    assert await redis_client.pttl(f'rev:{RZ}') > 0
    assert await redis_client.pttl(f'inbox:{RZ}') > 0  # the inbox stream is bounded too


class TestRedisCooldownGate(CooldownGateConformance):
    @pytest.fixture
    def gate(self, redis_client: FakeRedis) -> CooldownGate:
        return RedisCooldownGate(redis_client)

    @pytest.fixture
    def advance_time(self, redis_server: TimeControlledServer) -> Callable[[float], None]:
        return redis_server.advance


@pytest.mark.integration
async def test_rz_09_lua_token_bucket_paces_the_whole_fleet(redis_client: FakeRedis) -> None:
    clock = FakeClock()
    limiter = RedisRateLimiter(redis_client, clock, rate_per_second=1.0, burst=1)

    await limiter.acquire('namespace-1:idp')
    assert clock.monotonic() == 0.0  # the burst token → immediate

    # A second limiter instance (another pod / another session) shares the same ratelimit:{key}
    # bucket, so it is paced by the first instance's take — fleet-wide, not per process.
    other_pod = RedisRateLimiter(redis_client, clock, rate_per_second=1.0, burst=1)
    await other_pod.acquire('namespace-1:idp')
    assert clock.monotonic() == 1.0  # waited (via the injected clock) for the shared next token

    assert await redis_client.pttl('ratelimit:namespace-1:idp') > 0  # bucket state is TTL-bounded


# === reclaim re-presents in-flight entries only (edge case 1) =========================


async def test_rz_10_reclaim_re_presents_only_claimed_entries_not_a_fresh_one(redis_client: FakeRedis) -> None:
    # AT-LEAST-ONCE: reclaim re-presents in-flight (claimed-but-unacked) entries only. A bounded
    # consume claims just the first of a backlog; the never-claimed remainder must stay a fresh
    # consume candidate, neither reclaimed nor dropped.
    inbox = RedisStreamsInbox(redis_client)
    e1 = await inbox.append(RZ, work_email('e1@e.example'))
    await inbox.append(RZ, personal_email('e2@e.example'))  # e2 stays unclaimed

    (claimed,) = await inbox.consume(RZ, max_entries=1)  # XREADGROUP COUNT 1 claims only e1
    assert claimed.entry_id == e1

    reclaimed = await inbox.reclaim(RZ)  # XAUTOCLAIM re-presents only the pending (claimed) e1
    assert [entry.entry_id for entry in reclaimed] == [e1]
    assert reclaimed[0].delivery_count == 2  # only the reclaimed entry's count is bumped

    remaining = await inbox.consume(RZ)  # e2 was never claimed → still a fresh consume candidate
    assert len(remaining) == 1
    entry = remaining[0]
    assert isinstance(entry, InboxEntry) and entry.data_point.value == 'e2@e.example'  # surfaced intact
    assert entry.delivery_count == 1  # first delivery, not bumped by the reclaim of its peer


# === inbox ack stale-epoch rejection (edge cases 2 + 6) ===============================


async def test_rz_11_ack_is_epoch_guarded_and_keeps_the_entry_pending(redis_client: FakeRedis) -> None:
    # FENCING EPOCH: ack is a mutating write path. A fenced predecessor (lower epoch) must be
    # rejected with StaleEpochError after a successor bumped the inbox epoch — and must NOT remove
    # the entry it tried to ack, so no split-brain ack-away of an entry the successor now owns.
    inbox = RedisStreamsInbox(redis_client)
    entry_id = await inbox.append(RZ, work_email('a@e.example'))
    await inbox.consume(RZ)
    # A successor acks an unrelated id at epoch 2, bumping inbox_epoch to 2 (the _ACK_SCRIPT's
    # `epoch > stored → SET` branch); the real entry is untouched, still pending.
    await inbox.ack(RZ, 'no-such-entry', epoch=Epoch(2))

    with pytest.raises(StaleEpochError):
        await inbox.ack(RZ, entry_id, epoch=Epoch(1))  # the _ACK_SCRIPT returns -1 for epoch < stored

    assert await inbox.pending_count(RZ) == 1  # the fenced ack removed nothing


# === _ensure_group: swallow BUSYGROUP, re-raise everything else (edge cases 3 + 7) ====


async def test_rz_12_ensure_group_swallows_busygroup_on_re_entry(redis_client: FakeRedis) -> None:
    # The group-create runs on every consume/reclaim/quarantine; the first creates the group and
    # subsequent calls hit BUSYGROUP, which must be swallowed silently (idempotent ensure) so a
    # second consume after the group exists succeeds and simply delivers nothing new.
    inbox = RedisStreamsInbox(redis_client)
    await inbox.append(RZ, work_email('a@e.example'))
    first = await inbox.consume(RZ)  # creates the group
    second = await inbox.consume(RZ)  # re-entry: xgroup_create raises BUSYGROUP, swallowed
    assert len(first) == 1 and len(second) == 0


async def test_rz_13_ensure_group_reraises_a_non_busygroup_response_error(
    redis_client: FakeRedis, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Fail-fast: a genuine group-creation failure (anything but BUSYGROUP) must surface, not be
    # masked as a benign 'group already exists'. Masking it would let consume run against a missing
    # group and silently deliver nothing.
    inbox = RedisStreamsInbox(redis_client)

    async def _raise_other(*_args: object, **_kwargs: object) -> None:
        raise ResponseError('ERR something else entirely')

    monkeypatch.setattr(redis_client, 'xgroup_create', _raise_other)
    with pytest.raises(ResponseError, match='something else entirely'):
        await inbox.consume(RZ)


# === quarantine of an already-acked entry is a safe no-op (edge case 4) ===============


async def test_rz_14_quarantine_of_an_already_acked_entry_does_not_resurrect_or_record(
    redis_client: FakeRedis,
) -> None:
    # Quarantine idempotency: an acked entry is durably applied and removed; quarantining it
    # afterward must not create a spurious record or resurrect it (the _QUARANTINE_SCRIPT's XACK
    # returns 0, so no RPUSH).
    inbox = RedisStreamsInbox(redis_client)
    entry_id = await inbox.append(RZ, work_email('a@e.example'))
    await inbox.consume(RZ)
    await inbox.ack(RZ, entry_id, epoch=Epoch(1))  # entry XACK'd out of pending

    await inbox.quarantine(RZ, entry_id, reason='late', epoch=Epoch(1))  # XACK now returns 0 → no record
    assert await inbox.quarantined(RZ) == ()
    assert await inbox.pending_count(RZ) == 0  # not resurrected, not double-counted


# === write() re-observing an identity with an equal/older timestamp is a no-op (edge case 5) ===


async def test_rz_15_write_with_equal_or_older_timestamp_is_a_noop_merge(redis_client: FakeRedis) -> None:
    # KEYED-MERGE DEDUP: an at-least-once redelivery of an OLDER (or equal) snapshot of the same
    # identity must leave last_retrieved untouched, emit no 'u' triple, and not advance the
    # revision — the `last_retrieved > existing` guard is strict, so equality is also a no-op.
    store = RedisDataPointStore(redis_client)
    base = await store.write(RZ, [work_email('a@e.example', last=T2)], epoch=Epoch(1))

    older = await store.write(RZ, [work_email('a@e.example', last=T0)], epoch=Epoch(1))  # stale re-emission
    assert older == base  # the no-op merge burned no revision
    equal = await store.write(RZ, [work_email('a@e.example', last=T2)], epoch=Epoch(1))  # exactly equal
    assert equal == base  # strict `>` means an equal re-observation is also a no-op

    snapshot = await store.snapshot(RZ)
    assert len(snapshot) == 1
    assert snapshot.all()[0].last_retrieved == T2  # never regressed below the higher stored value
    assert await store.revision(RZ) == base
    change = await store.change_set_since(RZ, base)
    assert change.added == () and change.updated == ()  # neither added nor updated by the stale re-emissions


# === set_watermark round-trip + fence + isolation (edge case 8) =======================


async def test_rz_16_watermark_round_trips_under_its_epoch(redis_client: FakeRedis) -> None:
    # The positive guarded-field path: get is None until set, then set under an epoch round-trips.
    store = RedisDataPointStore(redis_client)
    assert await store.get_watermark(RZ, RZ_OP) is None
    await store.set_watermark(RZ, RZ_OP, Revision(7), epoch=Epoch(1))
    assert await store.get_watermark(RZ, RZ_OP) == Revision(7)


async def test_rz_17_set_watermark_is_epoch_guarded_and_leaves_prior_value(redis_client: FakeRedis) -> None:
    # A broken set_watermark that still rejects stale epochs would pass — so assert BOTH the
    # rejection and that the prior value is intact after a fenced call.
    store = RedisDataPointStore(redis_client)
    await store.set_watermark(RZ, RZ_OP, Revision(7), epoch=Epoch(1))
    await store.write(RZ, [work_email('a@e.example')], epoch=Epoch(2))  # a successor bumps the fence to 2
    with pytest.raises(StaleEpochError):
        await store.set_watermark(RZ, RZ_OP, Revision(99), epoch=Epoch(1))
    assert await store.get_watermark(RZ, RZ_OP) == Revision(7)  # the fenced write left the prior value


async def test_rz_18_watermarks_are_per_session_and_operator(redis_client: FakeRedis) -> None:
    store = RedisDataPointStore(redis_client)
    await store.set_watermark(RZ, RZ_OP, Revision(7), epoch=Epoch(1))
    assert await store.get_watermark(RZ_OTHER, RZ_OP) is None  # another session is isolated
    assert await store.get_watermark(RZ, OperatorId('rz-other-op')) is None  # another operator is isolated


# === rate limiter: refill, burst, skew clamp, wait, fractional rate, guards (edge case 9) ===


async def test_rz_19_rate_limiter_burst_token_immediate_then_paced(redis_client: FakeRedis) -> None:
    clock = FakeClock()
    limiter = RedisRateLimiter(redis_client, clock, rate_per_second=1.0, burst=1)
    await limiter.acquire('namespace-1:idp')
    assert clock.monotonic() == 0.0  # the burst token → immediate
    await limiter.acquire('namespace-1:idp')
    assert clock.monotonic() == 1.0  # second take waits exactly 1/rate for the next token


async def test_rz_20_rate_limiter_long_idle_refills_at_most_burst(redis_client: FakeRedis) -> None:
    # The math.min(burst, ...) clamp: a long idle accrues at most `burst` tokens, never more.
    clock = FakeClock()
    limiter = RedisRateLimiter(redis_client, clock, rate_per_second=1.0, burst=2)
    await limiter.acquire('k')
    await limiter.acquire('k')  # bucket empty at t=0
    clock.advance(60.0)  # 60 tokens' worth of time, but the bucket caps at burst=2
    await limiter.acquire('k')
    await limiter.acquire('k')
    assert clock.monotonic() == 60.0  # both came from the capped refill — immediate
    await limiter.acquire('k')
    assert clock.monotonic() == 61.0  # the third had to wait a full period — no extra tokens were hoarded


async def test_rz_21_rate_limiter_clock_skew_does_not_refill_backwards(redis_client: FakeRedis) -> None:
    # The math.max(now_ms - refill_ms, 0) clamp: if another pod stamped refill_ms into the FUTURE
    # (cross-pod clock skew), this pod's elapsed is clamped to 0 so the bucket never refills
    # backwards — the empty bucket still has to wait the full period.
    clock = FakeClock()
    limiter = RedisRateLimiter(redis_client, clock, rate_per_second=1.0, burst=1)
    await limiter.acquire('skew')  # takes the burst token, leaving ~0 tokens at t=0

    now_ms = int(clock.now().timestamp() * 1000)
    # redis-py types its commands as a sync/async union, so the standalone await needs the same Any
    # seam the adapters use; this stamps refill_ms into the future as a skewed peer pod would.
    client: Any = redis_client
    await client.hset('ratelimit:skew', mapping={'tokens': '0', 'refill_ms': str(now_ms + 5_000)})

    await limiter.acquire('skew')
    assert clock.monotonic() == 1.0  # elapsed clamped to 0 → waited a full 1/rate, not refilled from the future


async def test_rz_22_rate_limiter_second_pod_take_forces_a_reloop_wait(redis_client: FakeRedis) -> None:
    # acquire() re-runs the script after every sleep: another pod can grab the token this one slept
    # for, so the script reports a fresh wait and this pod loops again. Drain to empty, then race two
    # acquires concurrently on the shared bucket: between them they consume two tokens, so total
    # elapsed is two full refill periods — proving neither slipped through on a single wait.
    clock = FakeClock()
    pod_a = RedisRateLimiter(redis_client, clock, rate_per_second=1.0, burst=1)
    pod_b = RedisRateLimiter(redis_client, clock, rate_per_second=1.0, burst=1)
    await pod_a.acquire('namespace:cap')  # consume the burst token; bucket now empty at t=0

    await pod_a.acquire('namespace:cap')  # waits 1s
    assert clock.monotonic() == 1.0
    await pod_b.acquire('namespace:cap')  # the shared bucket is empty again → waits another full period
    assert clock.monotonic() == 2.0  # two takes after the burst cost two full periods, never one


async def test_rz_23_rate_limiter_fractional_rate_and_partial_tokens(redis_client: FakeRedis) -> None:
    # Fractional rate_per_ms + burst>1 partial-token arithmetic: at 2.5/s the next token after
    # draining accrues in ceil(1 / 0.0025) = 400 ms, exercising the wait_ms ceil computation.
    clock = FakeClock()
    limiter = RedisRateLimiter(redis_client, clock, rate_per_second=2.5, burst=2)
    await limiter.acquire('frac')  # 2 → 1 token
    await limiter.acquire('frac')  # 1 → 0 tokens, still immediate
    assert clock.monotonic() == 0.0
    await limiter.acquire('frac')  # empty: wait ceil(1 / (2.5/1000)) ms = 400 ms
    assert clock.monotonic() == 0.4


@pytest.mark.parametrize('rate', [0.0, -1.0])
async def test_rz_24_rate_limiter_rejects_non_positive_rate(redis_client: FakeRedis, rate: float) -> None:
    clock = FakeClock()
    with pytest.raises(ValueError, match='rate_per_second must be > 0.0'):
        RedisRateLimiter(redis_client, clock, rate_per_second=rate, burst=1)


@pytest.mark.parametrize('burst', [0, -1])
async def test_rz_25_rate_limiter_rejects_burst_below_one(redis_client: FakeRedis, burst: int) -> None:
    clock = FakeClock()
    with pytest.raises(ValueError, match='burst must be >= 1'):
        RedisRateLimiter(redis_client, clock, rate_per_second=1.0, burst=burst)


# === reclaim maps per-entry delivery counts across an XPENDING id-range (edge case 10) ===


async def test_rz_26_reclaim_maps_delivery_counts_per_entry_across_multiple_pending(
    redis_client: FakeRedis,
) -> None:
    # Poison surfacing: with >1 pending entry, reclaim must map times_delivered back to EACH entry
    # by message_id (the XPENDING range from claimed_ids[0]..[-1]), never share a single count. Drive
    # the two entries to DIFFERENT counts, then assert the returned tuple carries each entry's own.
    inbox = RedisStreamsInbox(redis_client)
    e1 = await inbox.append(RZ, work_email('e1@e.example'))
    e2 = await inbox.append(RZ, personal_email('e2@e.example'))
    await inbox.consume(RZ)  # both claimed, delivery_count 1

    await inbox.reclaim(RZ)  # both now delivery_count 2
    await inbox.ack(RZ, e1, epoch=Epoch(1))  # remove e1 from pending so the next reclaim only re-counts e2
    second = await inbox.reclaim(RZ)  # only e2 remains pending → reaches delivery_count 3

    counts = {entry.entry_id: entry.delivery_count for entry in second}
    assert counts == {e2: 3}  # e2 carries its own count
    assert e1 not in counts  # e1 was acked away, not re-presented with a stale shared count


async def test_rz_27_reclaim_keeps_distinct_counts_for_concurrently_pending_entries(
    redis_client: FakeRedis,
) -> None:
    # The multi-id XPENDING range + per-entry dict mapping: two entries reclaimed in ONE XAUTOCLAIM
    # with different histories must each keep their own times_delivered, not a single shared value.
    inbox = RedisStreamsInbox(redis_client)
    e1 = await inbox.append(RZ, work_email('e1@e.example'))
    await inbox.consume(RZ, max_entries=1)  # claim only e1 (delivery_count 1)
    await inbox.reclaim(RZ)  # e1 → 2
    e2 = await inbox.append(RZ, personal_email('e2@e.example'))
    await inbox.consume(RZ, max_entries=1)  # claim e2 (delivery_count 1); e1 still pending at 2

    reclaimed = await inbox.reclaim(RZ)  # one XAUTOCLAIM re-presents both, across the id range
    counts = {entry.entry_id: entry.delivery_count for entry in reclaimed}
    assert counts == {e1: 3, e2: 2}  # each entry mapped to its OWN times_delivered, never a shared count
