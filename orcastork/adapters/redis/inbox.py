"""Redis ``Inbox`` — a per-session Stream + consumer group (at-least-once, ordered).

``append`` → ``XADD`` + a pub/sub wakeup that ``wait_for_entry`` subscribes to (push, not
polling; the durable entry is the XADD — pub/sub only bounds wake latency); ``consume`` →
``XREADGROUP`` claims new entries; ``reclaim`` → ``XAUTOCLAIM`` re-presents pending
(un-acked) entries after a crash, with the delivery count from ``XPENDING`` (poison
surfacing); ``ack`` → epoch-guarded ``XACK`` after the durable apply. Sessions are isolated
by their stream key.

Delivery tolerates a bad wire payload only: a missing payload field or bytes that do not
decode as JSON are surfaced as a ``PoisonInboxEntry`` instead of raising, while semantic
deserialization failures (an unknown DataPoint type, a validation error) propagate.
``quarantine`` disposes of an entry atomically
(epoch-guarded ``XACK`` + a JSON record pushed onto ``quarantine:{session}``), and
``quarantined`` reads those records back.
"""

from __future__ import annotations

from typing import Any

import orjson
from redis.asyncio import Redis
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from ...datapoints import BaseDataPoint, parse_data_point
from ...exceptions import StaleEpochError
from ...ids import Epoch, SessionId
from ...ports.change_set import DeliveredInboxEntry, InboxEntry, PoisonInboxEntry, QuarantinedEntry
from .ttl import DEFAULT_STATE_TTL_MS, slide_ttl

_GROUP = 'orchestrator'

# Bounded so an idle wakeup channel cannot outlive the client's socket read deadline; see
# ``wait_for_entry``. Comfortably below RedisConfig.socket_timeout.
_WAKEUP_POLL_TIMEOUT_SECONDS = 1.0
_CONSUMER = 'orchestrator'

# Atomically epoch-guard the ack: reject a stale epoch, else bump the inbox epoch and XACK,
# counting the ack only if it removed a pending entry. KEYS = epoch, stream, acked; ARGV =
# epoch, group, entry_id.
_ACK_SCRIPT = """
local epoch = tonumber(ARGV[1])
local stored = tonumber(redis.call('GET', KEYS[1]) or '0')
if epoch < stored then return -1 end
if epoch > stored then redis.call('SET', KEYS[1], epoch) end
local acked = redis.call('XACK', KEYS[2], ARGV[2], ARGV[3])
if acked == 1 then redis.call('INCR', KEYS[3]) end
return acked
"""

# Atomically epoch-guard the quarantine: reject a stale epoch, else bump the inbox epoch and
# XACK the entry out of pending delivery; iff that removed it, count the ack (pending_count
# bookkeeping) and push the durable quarantine record — so a fenced predecessor can neither
# drop the entry nor double-record it. KEYS = epoch, stream, acked, quarantine; ARGV = epoch,
# group, entry_id, record_json.
_QUARANTINE_SCRIPT = """
local epoch = tonumber(ARGV[1])
local stored = tonumber(redis.call('GET', KEYS[1]) or '0')
if epoch < stored then return -1 end
if epoch > stored then redis.call('SET', KEYS[1], epoch) end
local acked = redis.call('XACK', KEYS[2], ARGV[2], ARGV[3])
if acked == 1 then
  redis.call('INCR', KEYS[3])
  redis.call('RPUSH', KEYS[4], ARGV[4])
end
return acked
"""


class RedisStreamsInbox:
    def __init__(self, redis: Redis, *, state_ttl_ms: int = DEFAULT_STATE_TTL_MS) -> None:
        self._redis: Any = redis  # see RedisDataPointStore: redis-py's sync/async union typing
        self._state_ttl_ms = state_ttl_ms

    @staticmethod
    def _stream(session_id: SessionId) -> str:
        return f'inbox:{session_id}'

    async def _ensure_group(self, session_id: SessionId) -> None:
        try:
            await self._redis.xgroup_create(self._stream(session_id), _GROUP, id='0', mkstream=True)
        except ResponseError as error:
            if 'BUSYGROUP' not in str(error):
                raise

    @staticmethod
    def _wakeup_channel(session_id: SessionId) -> str:
        return f'inbox-wakeup:{session_id}'

    @staticmethod
    def _quarantine_key(session_id: SessionId) -> str:
        return f'quarantine:{session_id}'

    async def append(self, session_id: SessionId, data_point: BaseDataPoint[Any]) -> str:
        # The client decodes responses, so payloads flow as str — decode the orjson bytes here.
        payload = orjson.dumps(data_point.model_dump(mode='json')).decode()
        entry_id = await self._redis.xadd(self._stream(session_id), {'data': payload})
        # Publish after the XADD: a waiter woken by this nudge must find the entry pending.
        await self._redis.publish(self._wakeup_channel(session_id), '1')
        await slide_ttl(self._redis, self._state_ttl_ms, self._stream(session_id))
        return str(entry_id)

    def _to_entry(self, entry_id: str, fields: dict[str, str], delivery_count: int) -> DeliveredInboxEntry:
        # Poison is reserved for a bad wire payload: only a missing payload field (a foreign
        # producer's malformed XADD) or bytes that fail to decode as JSON. Semantic
        # deserialization failures from parse_data_point (an unknown type, a validation error, a
        # registry bug) propagate and fail fast — a quiet quarantine would hide a parser
        # regression, and redelivery on resume lets a newer deployment parse what an older one
        # could not.
        try:
            decoded = orjson.loads(fields['data'])
        except (orjson.JSONDecodeError, KeyError) as error:
            return PoisonInboxEntry(
                str(entry_id), f'{type(error).__name__}: {error}', delivery_count, fields.get('data')
            )
        return InboxEntry(str(entry_id), parse_data_point(decoded), delivery_count)

    async def consume(
        self, session_id: SessionId, *, max_entries: int | None = None
    ) -> tuple[DeliveredInboxEntry, ...]:
        if max_entries is not None and max_entries <= 0:
            return ()  # XREADGROUP treats COUNT 0 as unbounded; keep parity with the in-memory adapter
        await self._ensure_group(session_id)
        response = await self._redis.xreadgroup(_GROUP, _CONSUMER, {self._stream(session_id): '>'}, count=max_entries)
        if not response:
            return ()
        return tuple(self._to_entry(entry_id, fields, 1) for entry_id, fields in response[0][1])

    async def reclaim(self, session_id: SessionId) -> tuple[DeliveredInboxEntry, ...]:
        await self._ensure_group(session_id)
        _, claimed, _ = await self._redis.xautoclaim(self._stream(session_id), _GROUP, _CONSUMER, min_idle_time=0)
        if not claimed:
            return ()
        # Fetch delivery counts only for the entries XAUTOCLAIM actually re-presented, bounded by their
        # id range (claimed is ascending), so poison counts stay complete regardless of backlog size — a
        # fixed cap over all pending would silently drop counts for entries beyond it.
        claimed_ids = [entry_id for entry_id, _ in claimed]
        delivery_counts = {
            pending['message_id']: int(pending['times_delivered'])
            for pending in await self._redis.xpending_range(
                self._stream(session_id), _GROUP, min=claimed_ids[0], max=claimed_ids[-1], count=len(claimed_ids)
            )
        }
        return tuple(
            self._to_entry(entry_id, fields, delivery_counts.get(entry_id, 1)) for entry_id, fields in claimed
        )

    async def ack(self, session_id: SessionId, entry_id: str, *, epoch: Epoch) -> None:
        result = await self._redis.eval(
            _ACK_SCRIPT,
            3,
            f'inbox_epoch:{session_id}',
            self._stream(session_id),
            f'acked:{session_id}',
            str(int(epoch)),
            _GROUP,
            entry_id,
        )
        if int(result) == -1:
            raise StaleEpochError(f'epoch {epoch} is stale for session {session_id}')
        await slide_ttl(
            self._redis,
            self._state_ttl_ms,
            self._stream(session_id),
            f'acked:{session_id}',
            f'inbox_epoch:{session_id}',
        )

    async def quarantine(self, session_id: SessionId, entry_id: str, *, reason: str, epoch: Epoch) -> None:
        # The group must exist for XPENDING/XACK: a quarantine racing ahead of any delivery
        # (nothing consumed yet) must be a safe no-op, not a NOGROUP error.
        await self._ensure_group(session_id)
        # The delivery count and raw payload are read before the atomic step: the record content
        # is advisory (ops inspection), while removal-plus-record must be atomic and epoch-guarded
        # — the Lua script only pushes the record iff the XACK actually removed the entry.
        pending = await self._redis.xpending_range(
            self._stream(session_id), _GROUP, min=entry_id, max=entry_id, count=1
        )
        delivery_count = int(pending[0]['times_delivered']) if pending else 0
        entries = await self._redis.xrange(self._stream(session_id), min=entry_id, max=entry_id)
        raw_payload = entries[0][1].get('data') if entries else None
        record = orjson.dumps(
            {'entry_id': entry_id, 'reason': reason, 'delivery_count': delivery_count, 'raw_payload': raw_payload}
        ).decode()
        result = await self._redis.eval(
            _QUARANTINE_SCRIPT,
            4,
            f'inbox_epoch:{session_id}',
            self._stream(session_id),
            f'acked:{session_id}',
            self._quarantine_key(session_id),
            str(int(epoch)),
            _GROUP,
            entry_id,
            record,
        )
        if int(result) == -1:
            raise StaleEpochError(f'epoch {epoch} is stale for session {session_id}')
        await slide_ttl(
            self._redis,
            self._state_ttl_ms,
            self._stream(session_id),
            f'acked:{session_id}',
            f'inbox_epoch:{session_id}',
            self._quarantine_key(session_id),
        )

    async def quarantined(self, session_id: SessionId) -> tuple[QuarantinedEntry, ...]:
        records = await self._redis.lrange(self._quarantine_key(session_id), 0, -1)
        entries: list[QuarantinedEntry] = []
        for raw in records:
            record = orjson.loads(raw)
            entries.append(
                QuarantinedEntry(
                    entry_id=record['entry_id'],
                    reason=record['reason'],
                    delivery_count=record['delivery_count'],
                    raw_payload=record['raw_payload'],
                )
            )
        return tuple(entries)

    async def pending_count(self, session_id: SessionId) -> int:
        total = int(await self._redis.xlen(self._stream(session_id)))
        acked = int(await self._redis.get(f'acked:{session_id}') or 0)
        return total - acked

    async def wait_for_entry(self, session_id: SessionId) -> None:
        pubsub = self._redis.pubsub()
        try:
            # Subscribe BEFORE checking pending: an append before the subscribe is visible as a
            # pending entry; one after it publishes to the live subscription — no gap either way.
            await pubsub.subscribe(self._wakeup_channel(session_id))
            if await self.pending_count(session_id) > 0:
                return
            # Polled on a bounded wait rather than an unbounded ``listen()``: the client carries a socket
            # read deadline, so a session that simply receives no input for longer than that deadline
            # would have this waiter raise instead of keeping its place — and the orchestrator propagates
            # a failed waiter. An idle poll returns None and is re-looped; the deadline firing anyway is
            # treated the same, so correctness does not rest on two timeouts staying ordered.
            while True:
                try:
                    message = await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=_WAKEUP_POLL_TIMEOUT_SECONDS
                    )
                except RedisTimeoutError:
                    continue
                if message is not None and message['type'] == 'message':
                    return
        finally:
            await pubsub.unsubscribe(self._wakeup_channel(session_id))
            await pubsub.aclose()
