"""The inbox waiter must outlive a session that receives no input for a while.

``wait_for_entry`` blocks on a pub/sub wakeup, and its client carries a socket read deadline
(``RedisConfig.socket_timeout``, 30s in this fleet). An unbounded blocking read on a quiet channel hits
that deadline and raises ``redis.exceptions.TimeoutError`` — and the orchestrator's ``_wait_for_inbox``
propagates a failed waiter, so a session simply waiting for mid-session input would fail once it waited
longer than the deadline.

Needs a real Redis: fakeredis has no socket, so it cannot express a read deadline at all, which is
exactly why this class of bug survives an otherwise thorough fake-backed suite.
"""

import asyncio
import os
from collections.abc import AsyncGenerator

import pytest
from redis.asyncio import Redis
from redis.exceptions import RedisError

from orcastork.adapters.redis.inbox import RedisStreamsInbox
from orcastork.ids import SessionId
from tests.doubles.datapoints import work_email

_SESSION = SessionId('inbox-idle-session')
# Far below the fleet's 30s so the idle stretch below stays a couple of seconds.
_SOCKET_TIMEOUT_SECONDS = 1


@pytest.fixture
async def real_redis() -> AsyncGenerator[Redis, None]:
    url = os.environ.get('REDIS_URL', 'redis://localhost:6379/15')
    client: Redis = Redis.from_url(url, decode_responses=True, socket_timeout=_SOCKET_TIMEOUT_SECONDS)
    try:
        await client.ping()
    except (RedisError, OSError) as exc:
        await client.aclose()
        pytest.skip(f'real redis unreachable at {url}: {exc}')
    await client.flushdb()
    try:
        yield client
    finally:
        await client.flushdb()
        await client.aclose()


async def test_the_waiter_survives_a_session_idle_longer_than_the_socket_timeout(real_redis: Redis) -> None:
    inbox = RedisStreamsInbox(real_redis)

    waiter = asyncio.create_task(inbox.wait_for_entry(_SESSION))
    await asyncio.sleep(_SOCKET_TIMEOUT_SECONDS * 2.5)

    if waiter.done():
        waiter.result()  # re-raise so the failure names the cause rather than just "done"
        pytest.fail('the waiter returned without any entry being appended')
    waiter.cancel()


async def test_an_entry_appended_after_a_long_idle_stretch_still_wakes_the_waiter(real_redis: Redis) -> None:
    # Surviving is only half of it: the wakeup has to still work after the idle stretch.
    inbox = RedisStreamsInbox(real_redis)
    waiter = asyncio.create_task(inbox.wait_for_entry(_SESSION))
    await asyncio.sleep(_SOCKET_TIMEOUT_SECONDS * 2.5)

    await inbox.append(_SESSION, work_email())

    await asyncio.wait_for(waiter, timeout=5)
