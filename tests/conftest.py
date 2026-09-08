"""Shared fixtures for the orcastork test suite.

Everything the suite needs is declared here or in ``tests/doubles`` — the package pulls in
no external test plugins of its own.

The autouse ``registry_isolation`` fixture is load-bearing: ``DataPoint`` (and, later,
``Operator``/``Capability``) subclasses self-register into module-global registries at
class-definition time, and the lazy discriminated-union ``TypeAdapter`` caches off them.
Without snapshot/restore, leaves defined inside one test would leak into others and make
duplicate-registration and union tests order-dependent.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from typing import Any

import mongomock.collection
import pytest
import pytest_asyncio
from fakeredis.aioredis import FakeRedis
from mongomock_motor import AsyncMongoMockClient
from pymongo import AsyncMongoClient
from pytest_mock_resources import MongoConfig, create_mongo_fixture

from orcastork.capabilities.base import Capability
from orcastork.datapoints import base as dp_base
from orcastork.datapoints import registry as dp_registry
from orcastork.operators.base import Operator

# Importing the doubles registers the standard DataPoint zoo once, before any test runs;
# the isolation fixture treats that as the per-test baseline.
from .doubles import datapoints as _zoo  # noqa: F401
from .doubles.clock import FakeClock
from .doubles.redis_support import TimeControlledServer


def _let_the_double_accept_a_null_sort() -> None:
    """Teach the Mongo double to ignore the `sort` argument pymongo sends on every bulk update.

    `UpdateOne._add_to_bulk` passes `sort=self._sort` unconditionally, and the double's bulk builder
    predates that parameter, so ANY `bulk_write` of update operations raises a `TypeError` about a
    keyword it has never heard of — which is not a disagreement about behaviour, just a version skew.

    Dropped only when it is None, which is what pymongo sends unless a caller asked for a sort. A real
    sort still raises, so the double cannot quietly ignore an instruction that would have changed the
    result. Behaviour that the double can only approximate is verified against a real server instead
    (see `real_mongo_database`).
    """
    builder = mongomock.collection.BulkOperationBuilder
    if getattr(builder.add_update, '_drops_null_sort', False):
        return
    original = builder.add_update

    def add_update(self: Any, *args: Any, **kwargs: Any) -> Any:
        if kwargs.get('sort') is None:
            kwargs.pop('sort', None)
        return original(self, *args, **kwargs)

    add_update._drops_null_sort = True  # type: ignore[attr-defined]
    builder.add_update = add_update  # type: ignore[method-assign]


_let_the_double_accept_a_null_sort()


@pytest.fixture(autouse=True)
def registry_isolation() -> Iterator[None]:
    """Restore the DataPoint / Operator / Capability registries after each test.

    All three self-register at class-definition time; without snapshot/restore, leaves and
    stubs defined inside one test would leak into others (and break duplicate-registration
    and union tests).
    """
    leaves = dict(dp_base._REGISTRY)
    abstracts = set(dp_base._ABSTRACT_TYPES)
    version = dp_base.registry_version()
    operators = dict(Operator._registry)
    capabilities = dict(Capability._registry)
    try:
        yield
    finally:
        dp_base._REGISTRY.clear()
        dp_base._REGISTRY.update(leaves)
        dp_base._ABSTRACT_TYPES.clear()
        dp_base._ABSTRACT_TYPES.update(abstracts)
        dp_base._registry_version = version
        dp_registry.reset_cache()
        Operator._registry.clear()
        Operator._registry.update(operators)
        Capability._registry.clear()
        Capability._registry.update(capabilities)


@pytest.fixture
def fake_clock() -> FakeClock:
    return FakeClock()


@pytest_asyncio.fixture
async def redis_server() -> AsyncIterator[TimeControlledServer]:
    """A time-controlled in-process fakeredis server (advance to expire lock leases)."""
    server = TimeControlledServer()
    try:
        yield server
    finally:
        server.reset()
        server.connected = False


@pytest_asyncio.fixture
async def redis_client(redis_server: TimeControlledServer) -> AsyncIterator[FakeRedis]:
    """A fresh fakeredis client per test (decode_responses, isolated, flushed)."""
    client: FakeRedis = FakeRedis(server=redis_server, decode_responses=True)
    try:
        yield client
    finally:
        await client.flushall()
        await client.aclose()


@pytest.fixture
def mongo_database() -> Any:
    """A fresh in-process async Mongo database per test (mongomock-motor)."""
    return AsyncMongoMockClient()['orcastork_test']


@pytest.fixture(scope='session')
def pmr_mongo_config() -> MongoConfig:
    """Container settings for the real-Mongo fixture below.

    The image is pinned because the default is ``mongo:3.6``, whose wire version predates the minimum
    the installed pymongo speaks — the container starts and accepts connections, then every command
    fails, which surfaces as an unhelpful "unable to connect".

    The port is pinned away from the default for the same reason: the container is named after its
    port and reused across runs, so a stale one left by the old default would be adopted rather than
    replaced, reintroducing that failure on a machine that had once run it.
    """
    return MongoConfig(image='mongo:7', port=28018, ci_port=27017)


_pmr_mongo = create_mongo_fixture()


@asynccontextmanager
async def opened_real_database(minted: Any) -> AsyncIterator[Any]:
    """Connect to a PMR-minted database, and drop it again on the way out.

    The body of ``real_mongo_database``, kept reachable so a test can drive the teardown: a fixture
    cleans up after its test has finished, so nothing inside that test can observe it, and test order
    here is randomized so the test that runs next cannot be relied on either.

    Dropped rather than merely disconnected from. ``create_mongo_fixture`` mints an ObjectId-named
    database per test and removes none of them, on the assumption that the container is thrown away
    when the session ends — which does not hold whenever one is already answering on the port and is
    adopted instead of created (a run interrupted before its own cleanup, a second session
    overlapping this one, a CI-provided server on ``ci_port``). ``tools/migrate``'s migration fixture
    drops its database for the same reason.
    """
    host, port = minted.client.address
    client: Any = AsyncMongoClient(f'mongodb://{host}:{port}')
    try:
        yield client[minted.name]
    finally:
        await client.drop_database(minted.name)
        await client.close()


@pytest_asyncio.fixture
async def real_mongo_database(_pmr_mongo: Any) -> AsyncIterator[Any]:
    """A database on a REAL MongoDB, for behaviour the in-process double cannot answer for.

    Reach for this only where the server itself is the subject — an aggregation stage, a bulk write's
    merge semantics, an index actually being used — and stay on ``mongo_database`` otherwise, since
    the double needs no Docker and costs milliseconds. The double is a reimplementation: it is
    faithful enough for shape, and silently unfaithful about the operators it never grew, so a test
    that passes against it proves less than it appears to about anything server-side.

    A fresh database per test is what keeps one test's ``max_epoch`` out of the next one's fence, and
    it is dropped again afterwards rather than left behind (see :func:`opened_real_database`).

    Tests using this are marked ``integration``, so they are excluded from the default run.
    """
    async with opened_real_database(_pmr_mongo) as database:
        yield database
