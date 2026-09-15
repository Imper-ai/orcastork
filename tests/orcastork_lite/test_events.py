"""The session event sinks: the Redis stream adapter against fakeredis, and the in-memory one."""

from __future__ import annotations

import json
from datetime import timedelta, timezone

from fakeredis.aioredis import FakeRedis

from orcastork_lite import DataPointMerged, NamespaceId, OperatorId, SessionCompleted, SessionId
from orcastork_lite.adapters.memory import InMemorySessionEventSink
from orcastork_lite.adapters.redis import DEFAULT_TTL, RedisSessionEventSink

from ..doubles.clock import FakeClock


def _merged(session: str, value: object, clock: FakeClock) -> DataPointMerged:
    return DataPointMerged(
        session_id=SessionId(session),
        namespace_id=NamespaceId('ns'),
        at=clock.now(),
        data_point_type='Ip',
        value=value,
        retrieved_by=OperatorId('op'),
        merge='added',
        revision=1,
    )


async def test_redis_sink_appends_one_entry_per_event_to_the_session_stream(
    redis_client: FakeRedis, fake_clock: FakeClock
) -> None:
    sink = RedisSessionEventSink(redis_client, maxlen=100)
    await sink.publish(_merged('s1', {'a': [1, 2]}, fake_clock))
    await sink.publish(
        SessionCompleted(
            session_id=SessionId('s1'),
            namespace_id=NamespaceId('ns'),
            at=fake_clock.now(),
            deadline_hit=False,
            operator_runs={OperatorId('op'): 1},
            failures={},
        )
    )
    await sink.publish(_merged('s2', 'other session', fake_clock))

    entries = await redis_client.xrange(sink.stream_key(SessionId('s1')))
    assert [fields['kind'] for _entry_id, fields in entries] == ['data_point_merged', 'session_completed']
    first = json.loads(entries[0][1]['event'])
    assert first['value'] == {'a': [1, 2]} and first['merge'] == 'added' and first['session_id'] == 's1'
    assert first['at'] == fake_clock.now().astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
    assert len(await redis_client.xrange(sink.stream_key(SessionId('s2')))) == 1


async def test_redis_sink_renders_values_json_cannot_express_and_caps_the_stream(
    redis_client: FakeRedis, fake_clock: FakeClock
) -> None:
    class Opaque:
        def __repr__(self) -> str:
            return 'Opaque()'

    sink = RedisSessionEventSink(redis_client, key_prefix='x:', maxlen=1)
    for value in (Opaque(), 'second'):
        await sink.publish(_merged('s', value, fake_clock))

    entries = await redis_client.xrange('x:s')
    assert len(entries) <= 2  # approximate trimming honours the cap loosely, but never grows unbounded
    payloads = [json.loads(fields['event'])['value'] for _entry_id, fields in entries]
    assert payloads[-1] == 'second'
    if len(payloads) == 2:
        assert payloads[0] == 'Opaque()'


async def test_redis_sink_arms_a_sliding_ttl_on_the_stream_key(redis_client: FakeRedis, fake_clock: FakeClock) -> None:
    sink = RedisSessionEventSink(redis_client, ttl=timedelta(minutes=10))
    key = sink.stream_key(SessionId('s'))
    await sink.publish(_merged('s', 1, fake_clock))
    first_ttl = await redis_client.ttl(key)
    assert 0 < first_ttl <= 600

    await redis_client.expire(key, 5)  # simulate time passing: the key is close to expiring
    await sink.publish(_merged('s', 2, fake_clock))
    assert await redis_client.ttl(key) > 5  # a live session re-arms the full window
    assert timedelta(hours=24) == DEFAULT_TTL

    forever = RedisSessionEventSink(redis_client, key_prefix='keep:', ttl=None)
    await forever.publish(_merged('s', 3, fake_clock))
    assert await redis_client.ttl('keep:s') == -1  # opted out: no expiry at all


async def test_in_memory_sink_filters_by_session(fake_clock: FakeClock) -> None:
    sink = InMemorySessionEventSink()
    await sink.publish(_merged('a', 1, fake_clock))
    await sink.publish(_merged('b', 2, fake_clock))
    assert [event.session_id for event in sink.for_session(SessionId('a'))] == ['a']
    assert len(sink.events) == 2
