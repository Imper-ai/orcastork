"""Redis Stream ``SessionEventSink`` — one stream per session, one entry per event.

Consumers ``XREAD``/``XRANGE`` ``<key_prefix><session_id>`` to follow a session while it runs
(e.g. to act on a DataPoint the moment it lands rather than when the session ends). Each entry
carries the event ``kind`` as its own field, so a consumer can filter without parsing, plus the
full event as JSON. Two bounds keep the keyspace finite: the stream is capped at ``maxlen``
entries (approximate trimming, the cheap kind), and every publish re-arms a sliding ``ttl`` on the
key, so a live session keeps its stream while a finished or abandoned one disappears on its own —
without the TTL, one key per session ever run would stay behind forever. Both land in one
pipelined round trip.
"""

from __future__ import annotations

from datetime import timedelta

from redis.asyncio import Redis

from ..events import SessionEvent
from ..ids import SessionId

DEFAULT_KEY_PREFIX = 'orcastork_lite:events:'
DEFAULT_MAXLEN = 10_000
DEFAULT_TTL = timedelta(hours=24)


class RedisSessionEventSink:
    def __init__(
        self,
        redis: Redis,
        *,
        key_prefix: str = DEFAULT_KEY_PREFIX,
        maxlen: int | None = DEFAULT_MAXLEN,
        ttl: timedelta | None = DEFAULT_TTL,
    ) -> None:
        self._redis = redis
        self._key_prefix = key_prefix
        self._maxlen = maxlen
        self._ttl = ttl  # None opts out, for a deployment that trims the keyspace itself

    def stream_key(self, session_id: SessionId) -> str:
        return f'{self._key_prefix}{session_id}'

    async def publish(self, event: SessionEvent) -> None:
        key = self.stream_key(event.session_id)
        # A DataPoint value is whatever the flow chose; anything JSON cannot express is rendered with
        # repr rather than failing the publish, since the stream is a view of the session, not its record.
        payload = event.model_dump_json(fallback=repr)
        async with self._redis.pipeline(transaction=False) as pipe:
            pipe.xadd(key, {'kind': event.kind, 'event': payload}, maxlen=self._maxlen, approximate=True)
            if self._ttl is not None:
                pipe.expire(key, self._ttl)
            await pipe.execute()
