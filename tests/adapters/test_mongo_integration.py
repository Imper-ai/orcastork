"""MG-INT — the Mongo adapters against a REAL MongoDB, for what the in-process double cannot answer.

Everything in `test_mongo.py` runs on `mongomock-motor`, which is a reimplementation: it emulates
what it has grown, and does not have everything. Where it agrees with a server it is genuinely
useful and costs no Docker, which is why it stays the default. What it cannot do is tell you which
of those two states you are in.

These tests exist to hold the two Python folds honest. `flush` merges with `$setOnInsert` + `$max`
in the database, and both adapters reimplement that merge in Python (the in-memory one to be the
merge, the Mongo one to fold buffered rows into a read before they are flushed). Those mirrors are
only trustworthy if the thing they mirror is pinned somewhere the server is real — the double's
agreement is worth having, but two reimplementations agreeing is not evidence about the original.

The double's emulation of that particular pair was measured and is faithful. What it cannot execute
at all is a `bulk_write` of `UpdateOne` (its bulk builder predates pymongo's `sort` argument), a
`$unionWith`, or a `collMod` — and it stores Python datetimes as they were handed to it rather than
as BSON dates, so it cannot see a resolution the wire format does not carry. Anything reaching for
those has to be verified here or not at all. The last section covers the `real_mongo_database`
fixture itself, which only a real server can hold to account.

Marked `integration`, so the default run excludes them; they need Docker (`pytest -m integration`).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import pytest

from orcastork.adapters.mongo import MongoDataPointArchive
from orcastork.archive import ArchivedDataPoint
from orcastork.ids import Epoch, SessionId

from ..conftest import opened_real_database
from ..doubles.cipher import ReversingCipher
from ..doubles.conformance import NAMESPACE, T2
from ..doubles.datapoints import T0, work_email

MGI = SessionId('mg-integration-session')

# Sub-millisecond, because that is the resolution a BSON date drops. `SystemClock.now()` yields
# microseconds and the orchestrator stamps first/last_retrieved straight from it, so this is the
# production shape rather than a contrived one; a whole-second fixture cannot see the difference.
SUB_MS_T0 = T0 + timedelta(microseconds=123456)
SUB_MS_T2 = T2 + timedelta(microseconds=654321)

pytestmark = pytest.mark.integration


def _entry(*, last: Any, epoch: int, first: datetime = T0) -> ArchivedDataPoint:
    """The same identity every time — one `(type, value_hash)`, so re-archiving re-observes it."""
    return ArchivedDataPoint.from_data_point(
        work_email('a@e.example', first=first, last=last),
        session_id=MGI,
        namespace_id=NAMESPACE,
        epoch=Epoch(epoch),
    )


async def test_mgi_01_the_server_merge_keeps_the_first_sighting_and_advances_the_rest(
    real_mongo_database: Any,
) -> None:
    # The keyed upsert the Python folds mirror, executed by a real server: the immutable fields come
    # from the first observation and never move, while last_retrieved and epoch advance. The double
    # does emulate this pair correctly today — measured, not assumed — so this is an anchor rather
    # than a correction: it is what makes the two Python folds mirrors of the server's behaviour
    # instead of mirrors of another reimplementation that happens to agree.
    archive = MongoDataPointArchive(real_mongo_database, cipher=ReversingCipher())

    await archive.archive(_entry(last=T0, epoch=1))
    await archive.archive(_entry(last=T2, epoch=2))
    await archive.flush(MGI)

    committed = await archive.read(MGI)
    assert len(committed) == 1  # one identity, one row
    assert committed[0].first_retrieved == T0
    assert committed[0].last_retrieved == T2
    assert int(committed[0].epoch) == 2
    assert committed[0].value == 'a@e.example'  # sealed on write, unsealed on read


async def test_mgi_02_a_read_matches_the_server_merge_before_the_flush_runs(
    real_mongo_database: Any,
) -> None:
    # The property the buffered read is built on, checked against the real merge rather than against
    # the Python one that stands in for it. If the fold ever drifts from what the server would have
    # produced, the two reads disagree here — which the double cannot notice, because the same Python
    # fold produces both of its answers.
    #
    # Stamped below the millisecond deliberately: a buffered row validates from an ISO string and a
    # committed one round-trips through a BSON date, so this is where the two sides can carry
    # different instants for the same observation and "a read is buffer-transparent" quietly stops
    # being true on the production clock.
    archive = MongoDataPointArchive(real_mongo_database, cipher=ReversingCipher())

    await archive.archive(_entry(first=SUB_MS_T0, last=SUB_MS_T0, epoch=1))
    await archive.archive(_entry(first=SUB_MS_T0, last=SUB_MS_T2, epoch=2))

    before = await archive.read(MGI)
    await archive.flush(MGI)
    after = await archive.read(MGI)

    assert before == after
    # Both sides at the resolution the store can actually hold, rather than one of them pretending.
    assert after[0].first_retrieved.microsecond % 1000 == 0
    assert after[0].last_retrieved.microsecond % 1000 == 0


async def test_mgi_03_a_reflushed_buffer_does_not_duplicate_on_the_server(
    real_mongo_database: Any,
) -> None:
    # A pass that commits and then dies before clearing must be safely replayable: the id is derived
    # from the value, so the re-commit lands on the same row. Worth pinning on a real server because
    # what stops the duplicate is the unique `_id`, which is the server's rule to enforce.
    archive = MongoDataPointArchive(real_mongo_database, cipher=ReversingCipher())

    await archive.archive(_entry(last=T0, epoch=1))
    await archive.flush(MGI)
    await archive.archive(_entry(last=T2, epoch=1))
    await archive.flush(MGI)

    assert len(await archive.read(MGI)) == 1
    assert await archive.buffered_count(MGI) == 0


async def test_mgi_04_one_page_carrying_the_same_identity_twice_commits_without_colliding(
    real_mongo_database: Any,
) -> None:
    # The buffer does not deduplicate, so re-archiving one identity before a flush puts two upserts
    # for the SAME `_id` in a single batch — the first has to insert and the second merge into it,
    # rather than both attempting an insert. Verified here because how a batch of upserts onto one id
    # resolves is the server's own scheduling, and the double can only approximate it.
    #
    # This does not discriminate `ordered`: unordered was measured and resolves the same way on a
    # current server. What it pins is that a page carrying a repeated identity commits at all and
    # merges correctly, which is the property the flush depends on.
    archive = MongoDataPointArchive(real_mongo_database, cipher=ReversingCipher())

    await archive.archive(_entry(last=T0, epoch=1))
    await archive.archive(_entry(last=T2, epoch=2))
    assert await archive.buffered_count(MGI) == 2  # both still buffered, so one page carries both

    assert await archive.flush(MGI) == 2

    committed = await archive.read(MGI)
    assert len(committed) == 1
    assert committed[0].first_retrieved == T0  # the first upsert supplied the immutable fields
    assert committed[0].last_retrieved == T2  # the second merged into it rather than colliding
    assert int(committed[0].epoch) == 2


async def test_mgi_05_retuning_the_retention_window_reaches_the_server(real_mongo_database: Any) -> None:
    # `archive_retention_seconds` is operator-tunable, so the deployed value has to be the one the
    # reaper uses. The index already exists under the same name after the first deploy, and an
    # already-existing index is otherwise left exactly as it is — so without reconciliation a
    # shortened window is a retention promise the archive quietly stops keeping. Only a real server
    # can answer this: the double has no `collMod`.
    thirty_days, seven_days = 2592000, 604800
    await MongoDataPointArchive(real_mongo_database, cipher=ReversingCipher(), retention_seconds=thirty_days).archive(
        _entry(last=T0, epoch=1)
    )

    await MongoDataPointArchive(real_mongo_database, cipher=ReversingCipher(), retention_seconds=seven_days).archive(
        _entry(last=T2, epoch=1)
    )

    indexes = await real_mongo_database[ArchivedDataPoint.__table_name__].index_information()
    assert indexes['retention_reaper']['expireAfterSeconds'] == seven_days


async def test_mgi_06_enabling_retention_over_a_plain_index_still_expires(real_mongo_database: Any) -> None:
    # The mirror case, and the worse one. Mongo allows only one index per key pattern, so a plain
    # `last_retrieved` index an operator built for their own query matches on the key pattern and
    # takes the name the reaper would have used — leaving a first-time retention rollout expiring
    # nothing at all, with no error anywhere to say so.
    seven_days = 604800
    committed = real_mongo_database[ArchivedDataPoint.__table_name__]
    await committed.create_index([('last_retrieved', 1)], name='built_by_ops')

    await MongoDataPointArchive(real_mongo_database, cipher=ReversingCipher(), retention_seconds=seven_days).archive(
        _entry(last=T0, epoch=1)
    )

    indexes = await committed.index_information()
    assert indexes['built_by_ops']['expireAfterSeconds'] == seven_days  # the operator's index, now reaping
    on_last_retrieved = [name for name, spec in indexes.items() if spec['key'] == [('last_retrieved', 1)]]
    assert on_last_retrieved == ['built_by_ops']  # converted in place, not competed with by a second index


async def test_mgi_07_a_per_session_read_is_an_indexed_range_not_a_scan(real_mongo_database: Any) -> None:
    # `orcastork-datapoints` is one shared collection holding every namespace's and every
    # session's rows for the whole retention window, and nothing indexes `session_id`. Read through
    # the profiler rather than by explaining a filter written out again here, so what is asserted is
    # the query the adapter actually sent.
    archive = MongoDataPointArchive(real_mongo_database, cipher=ReversingCipher())
    await archive.archive(_entry(last=T0, epoch=1))
    await archive.flush(MGI)
    other = MongoDataPointArchive(real_mongo_database, cipher=ReversingCipher())
    for address in ('b@e.example', 'c@e.example', 'd@e.example'):
        await other.archive(
            ArchivedDataPoint.from_data_point(
                work_email(address, first=T0, last=T0),
                session_id=SessionId('mg-integration-other'),
                namespace_id=NAMESPACE,
                epoch=Epoch(1),
            )
        )
    await other.flush(SessionId('mg-integration-other'))

    await real_mongo_database.command('profile', 2)
    try:
        assert len(await archive.read(MGI)) == 1
    finally:
        await real_mongo_database.command('profile', 0)

    namespace = f'{real_mongo_database.name}.{ArchivedDataPoint.__table_name__}'
    reads = [doc async for doc in real_mongo_database['system.profile'].find({'op': 'query', 'ns': namespace})]
    assert reads, 'the profiler saw no read of the committed collection'
    assert all('COLLSCAN' not in read['planSummary'] for read in reads), [read['planSummary'] for read in reads]
    # The other session's three rows were never touched: an unindexed equality would have read them.
    assert sum(read['docsExamined'] for read in reads) == 1


# ---------------------------------------------------------------------------
# The `real_mongo_database` fixture itself.
# ---------------------------------------------------------------------------


@dataclass
class _Minted:
    """A stand-in for what `create_mongo_fixture` hands over: a client, and a database name."""

    client: Any
    name: str


async def test_mgi_08_the_real_mongo_fixture_drops_the_database_it_used(_pmr_mongo: Any) -> None:
    # Driven over a throwaway name rather than observed through the fixture, because a fixture tidies
    # up after its test returns: no test can watch its own teardown, and the order tests run in here
    # is randomized, so the one that happens to run next cannot watch it either.
    #
    # `create_mongo_fixture` mints a database per test and drops none of them, assuming the container
    # goes away with the session — which it does not whenever one is already answering on the port
    # and gets adopted rather than created. Left alone they pile up in a server that outlives the run.
    throwaway = _Minted(client=_pmr_mongo.client, name=f'{_pmr_mongo.name}-teardown-probe')

    async with opened_real_database(throwaway) as database:
        await database['probe'].insert_one({'seen': True})
        assert throwaway.name in _pmr_mongo.client.list_database_names()  # exists while in use

    assert throwaway.name not in _pmr_mongo.client.list_database_names()  # and is gone afterwards
