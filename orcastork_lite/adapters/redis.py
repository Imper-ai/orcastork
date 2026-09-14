"""Redis Stream ``SessionEventSink`` — one stream per session, one entry per event.

Consumers ``XREAD``/``XRANGE`` ``<key_prefix><session_id>`` to follow a session while it runs
(e.g. to act on a DataPoint the moment it lands rather than when the session ends). Each entry
carries the event ``kind`` as its own field, so a consumer can filter without parsing, plus the
full event as JSON. The stream is capped at ``maxlen`` entries (approximate trimming, the cheap
kind) so a chatty session cannot grow it without bound.
"""

from __future__ import annotations

from redis.asyncio import Redis

from ..events import SessionEvent
from ..ids import SessionId

DEFAULT_KEY_PREFIX = 'orcastork_lite:events:'
DEFAULT_MAXLEN = 10_000


class RedisSessionEventSink:
    def __init__(
        self, redis: Redis, *, key_prefix: str = DEFAULT_KEY_PREFIX, maxlen: int | None = DEFAULT_MAXLEN
    ) -> None:
        self._redis = redis
        self._key_prefix = key_prefix
        self._maxlen = maxlen

    def stream_key(self, session_id: SessionId) -> str:
        return f'{self._key_prefix}{session_id}'

    async def publish(self, event: SessionEvent) -> None:
        # A DataPoint value is whatever the flow chose; anything JSON cannot express is rendered with
        # repr rather than failing the publish, since the stream is a view of the session, not its record.
        payload = event.model_dump_json(fallback=repr)
        await self._redis.xadd(
            self.stream_key(event.session_id),
            {'kind': event.kind, 'event': payload},
            maxlen=self._maxlen,
            approximate=True,
        )
