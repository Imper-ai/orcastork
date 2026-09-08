"""MG — Mongo adapters: the CNF contracts bound to Mongo + backend-specific mechanics.

These run against in-process ``mongomock-motor`` (OCC ``find_one_and_update``, ``$addToSet``,
ordered ``find``), so they execute in the default run and give CNF parity between the
in-memory and Mongo durable-store / audit-sink adapters. MG-05 (encrypted-field round-trip)
is out of scope — the framework does not encrypt; that is a flow/persistence concern.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from pymongo.errors import BulkWriteError, DuplicateKeyError, OperationFailure

from orcastork.adapters.mongo import (
    MongoAuditSink,
    MongoDataPointArchive,
    MongoDurableStore,
    audit_sink,
    datapoint_archive,
)
from orcastork.adapters.mongo.audit_sink import AUDIT_LOG_COLLECTION
from orcastork.adapters.mongo.datapoint_archive import _fold_in as mongo_fold_in
from orcastork.archive import ArchivedDataPoint
from orcastork.audit import AuditKind, AuditLogEntry
from orcastork.exceptions import (
    OptimisticConcurrencyError,
    PiiKeyUnavailableError,
    StaleEpochError,
    UnprotectedPiiError,
)
from orcastork.ids import Epoch, SessionId
from orcastork.ports import AuditSink, DataPointArchive, DurableStore

from ..doubles.cipher import ReversingCipher
from ..doubles.conformance import (
    NAMESPACE,
    TBL,
    AuditSinkConformance,
    DataPointArchiveConformance,
    DurableStoreConformance,
)
from ..doubles.datapoints import T0, risk, work_email

MG = SessionId('mg-session')


class _CountingCollection:
    """Wraps a collection to count the round-trips the batched paths must collapse."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.find_one_and_updates = 0
        self.insert_manys = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def find_one_and_update(self, *args: Any, **kwargs: Any) -> Any:
        self.find_one_and_updates += 1
        return await self._inner.find_one_and_update(*args, **kwargs)

    async def insert_many(self, *args: Any, **kwargs: Any) -> Any:
        self.insert_manys += 1
        return await self._inner.insert_many(*args, **kwargs)


class _InsertOneFailingCollection:
    """Lets the per-(table, key) epoch-guard update_one and the absence probe through, but fails
    insert_one with a DuplicateKeyError — the loser of a concurrent first-insert race (two writers
    both saw the key absent at expected_version 0)."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def insert_one(self, *_args: Any, **_kwargs: Any) -> Any:
        raise DuplicateKeyError('E11000 duplicate key error: _id')


class _InsertOneFailingDatabase:
    """Routes one table's collection through _InsertOneFailingCollection; the epoch guard's update_one
    on that same collection and every other collection behave normally."""

    def __init__(self, inner: Any, table: str) -> None:
        self._inner = inner
        self._table = table

    def __getitem__(self, name: str) -> Any:
        collection = self._inner[name]
        return _InsertOneFailingCollection(collection) if name == self._table else collection


class _ConcurrentAddOnReadCollection:
    """Lands another writer's set member at the moment this collection is read back.

    Stands in for a concurrent ``add_to_set`` on the same key: harmless if the size is reported by the
    write itself, and visible in the returned count if the size comes from a follow-up read.
    """

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def find_one(self, query: Any, *args: Any, **kwargs: Any) -> Any:
        await self._inner.update_one(query, {'$addToSet': {'members': 'another-writers-value'}})
        return await self._inner.find_one(query, *args, **kwargs)


class _ConcurrentAddOnReadDatabase:
    """Routes one table's collection through _ConcurrentAddOnReadCollection; others behave normally."""

    def __init__(self, inner: Any, table: str) -> None:
        self._inner = inner
        self._table = table

    def __getitem__(self, name: str) -> Any:
        collection = self._inner[name]
        return _ConcurrentAddOnReadCollection(collection) if name == self._table else collection


def _audit_entry(epoch: int = 1, *, kind: AuditKind = AuditKind.DATA_POINT_ADDED) -> AuditLogEntry:
    return AuditLogEntry(session_id=MG, epoch=Epoch(epoch), timestamp=T0, kind=kind)


class TestMongoDurableStore(DurableStoreConformance):
    @pytest.fixture
    def durable(self, mongo_database: Any) -> DurableStore:
        return MongoDurableStore(mongo_database)

    async def test_mg_durable_concurrent_insert_race_surfaces_as_occ(self, mongo_database: Any) -> None:
        # Two writers both observe the key absent (find_one None, expected_version 0) and both insert_one;
        # the loser collides on _id. A lost first-insert race is an OCC conflict, not a raw adapter leak, so
        # the pymongo DuplicateKeyError must surface as OptimisticConcurrencyError with the original chained.
        durable = MongoDurableStore(_InsertOneFailingDatabase(mongo_database, TBL))
        with pytest.raises(OptimisticConcurrencyError, match='concurrent insert') as excinfo:
            await durable.upsert(TBL, 'k', {'a': 1}, expected_version=0, epoch=Epoch(1))
        assert isinstance(excinfo.value.__cause__, DuplicateKeyError)

    async def test_mg_durable_add_to_set_size_is_the_set_as_of_this_write(self, mongo_database: Any) -> None:
        # A caller uses the returned size to decide something about its own membership, so it has to
        # describe the set including this write and nothing that landed after it. Reporting from a
        # follow-up read instead lets a concurrent add on the same key inflate the answer, which
        # corresponds to no single writer's view. The epoch guard does not help — it fences a superseded
        # predecessor, not two writers at the same epoch.
        durable = MongoDurableStore(_ConcurrentAddOnReadDatabase(mongo_database, TBL))
        size = await durable.add_to_set(TBL, 'k', 'sessions', 'mine', epoch=Epoch(1))
        assert size == 1


class TestMongoAuditSink(AuditSinkConformance):
    @pytest.fixture
    def audit(self, mongo_database: Any) -> AuditSink:
        return MongoAuditSink(mongo_database)


class TestMongoDataPointArchive(DataPointArchiveConformance):
    @pytest.fixture
    def archive(self, mongo_database: Any) -> DataPointArchive:
        # A cipher is wired so PII conformance entries pass the production fail-closed guard.
        return MongoDataPointArchive(mongo_database, cipher=ReversingCipher())


class _CommitFailingAfterCollection:
    """Fails the commit once a given number of pages have succeeded — a flush interrupted partway.

    Counted in pages rather than rows, because a page is one round trip: the flush commits a whole
    batch per call, so a page is the smallest unit an interruption can land between.
    """

    def __init__(self, inner: Any, succeed_first: int) -> None:
        self._inner = inner
        self._remaining = succeed_first

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def bulk_write(self, *args: Any, **kwargs: Any) -> Any:
        if self._remaining <= 0:
            raise RuntimeError('commit exploded partway through the flush')
        self._remaining -= 1
        return await self._inner.bulk_write(*args, **kwargs)


class _CommitFailingAfterDatabase:
    """Routes the archive's committed collection through _CommitFailingAfterCollection."""

    def __init__(self, inner: Any, succeed_first: int) -> None:
        self._inner = inner
        self._succeed_first = succeed_first
        self._wrapped: Any = None

    def __getitem__(self, name: str) -> Any:
        collection = self._inner[name]
        if name != ArchivedDataPoint.__table_name__:
            return collection
        if self._wrapped is None:
            self._wrapped = _CommitFailingAfterCollection(collection, self._succeed_first)
        return self._wrapped


async def test_mg_archive_flush_interrupted_partway_keeps_the_batches_it_committed(
    mongo_database: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The flush shares the bounded tail's timeout with everything else there, so it has to be
    # resumable: clearing the buffer only after every row is committed would leave a timed-out pass
    # with rows committed and none cleared, and the next attempt would re-read the identical buffer
    # and die in the same place — each pass making the next one slower. Clearing per batch is what
    # turns that into forward progress.
    monkeypatch.setattr(datapoint_archive, '_PAGE_SIZE', 2)
    seeded = MongoDataPointArchive(mongo_database, cipher=ReversingCipher())
    for address in ('a@e.example', 'b@e.example', 'c@e.example', 'd@e.example'):
        await seeded.archive(
            ArchivedDataPoint.from_data_point(
                work_email(address), session_id=MG, namespace_id=NAMESPACE, epoch=Epoch(1)
            )
        )
    assert await seeded.buffered_count(MG) == 4

    # The first page of two commits, then the second page's commit raises.
    interrupted = MongoDataPointArchive(
        _CommitFailingAfterDatabase(mongo_database, succeed_first=1), cipher=ReversingCipher()
    )
    with pytest.raises(RuntimeError, match='commit exploded'):
        await interrupted.flush(MG)

    # buffered_count is the probe for progress, not read(): a read is buffer-transparent, so it
    # reports all four either way. Two rows having LEFT the buffer is what says the interrupted pass
    # committed them and cleared only those — had it committed nothing, this would still be 4.
    assert await seeded.buffered_count(MG) == 2  # the committed batch was cleared, not re-presented
    assert len(await seeded.read(MG)) == 4  # and nothing was lost: 2 committed + 2 still buffered

    assert await seeded.flush(MG) == 2  # the resume does only the work that remains
    assert await seeded.buffered_count(MG) == 0
    assert len(await seeded.read(MG)) == 4


class _IndexRefusingCollection:
    """Refuses ``create_index``, by default the way a server does when an incompatible one exists.

    The refusal message is a parameter because the two ways this happens are different situations
    that have to degrade identically: an incompatible index already present, and a credential that
    is not allowed to create one at all.
    """

    def __init__(self, inner: Any, error: str = 'Index with pattern already exists with a different name') -> None:
        self._inner = inner
        self._error = error

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def create_index(self, *_args: Any, **_kwargs: Any) -> Any:
        raise OperationFailure(self._error)


class _IndexRefusingDatabase:
    """Routes one collection's create_index into a refusal; everything else behaves normally."""

    def __init__(self, inner: Any, table: str) -> None:
        self._inner = inner
        self._table = table

    def __getitem__(self, name: str) -> Any:
        collection = self._inner[name]
        return _IndexRefusingCollection(collection) if name == self._table else collection


class _EveryIndexRefusingDatabase:
    """Refuses ``create_index`` on every collection — the shape of a read-only credential.

    Whole-database rather than per-table because a single archive call ensures indexes on the
    committed collection AND the buffer, so refusing only one of them leaves the other free to
    succeed and the degraded path only half-exercised.
    """

    def __init__(self, inner: Any, error: str) -> None:
        self._inner = inner
        self._error = error

    def __getitem__(self, name: str) -> Any:
        return _IndexRefusingCollection(self._inner[name], self._error)


def _index_keys(information: dict[str, Any]) -> list[list[tuple[str, int]]]:
    return [[tuple(entry) for entry in spec.get('key', [])] for spec in information.values()]


async def test_mg_audit_log_is_indexed_for_the_only_way_it_is_read(mongo_database: Any) -> None:
    # Every reader of this collection — `replay`, and any timeline view built over it — asks for one session's
    # trail in sequence order. Unindexed that is a scan of every session's rows plus an in-memory
    # sort, so the cost of reading one session grows with the size of the whole log. There is no
    # migration step that could have created it: the collection appears on first write.
    sink = MongoAuditSink(mongo_database)
    await sink.append(_audit_entry())

    assert [('session_id', 1), ('sequence', 1)] in _index_keys(
        await mongo_database[AUDIT_LOG_COLLECTION].index_information()
    )


async def test_mg_archive_buffer_is_indexed_for_the_paged_flush(mongo_database: Any) -> None:
    # The flush reads the buffer by session in sequence order a page at a time, and `buffered_count`
    # counts by session. Unindexed, every page scans every session's buffered rows.
    archive = MongoDataPointArchive(mongo_database, cipher=ReversingCipher())
    await archive.archive(
        ArchivedDataPoint.from_data_point(
            work_email('a@e.example'), session_id=MG, namespace_id=NAMESPACE, epoch=Epoch(1)
        )
    )

    assert [('session_id', 1), ('sequence', 1)] in _index_keys(
        await mongo_database['datapoint_archive_buffer'].index_information()
    )


async def test_mg_an_equivalent_index_under_another_name_is_left_alone(mongo_database: Any) -> None:
    # Mongo refuses a same-keys index under a different name, so creating unconditionally would turn
    # an index someone already made by hand into an exception on the append path. Gating on the key
    # pattern as well as the name makes that case a no-op, and leaves the operator's index untouched
    # rather than competing with it.
    collection = mongo_database[AUDIT_LOG_COLLECTION]
    await collection.create_index([('session_id', 1), ('sequence', 1)], name='made_by_ops')

    await MongoAuditSink(mongo_database).append(_audit_entry())

    assert sorted((await collection.index_information()).keys()) == ['_id_', 'made_by_ops']


async def test_mg_a_refused_index_does_not_fail_the_append(mongo_database: Any) -> None:
    # An index is an optimization: its absence is a slow read, so a process that cannot create one
    # must still complete the write it was actually doing. The entry has to land regardless.
    sink = MongoAuditSink(_IndexRefusingDatabase(mongo_database, AUDIT_LOG_COLLECTION))
    await sink.append(_audit_entry())

    assert len(await sink.replay(MG)) == 1


_ARCHIVE_COLLECTIONS = (ArchivedDataPoint.__table_name__, 'datapoint_archive_buffer')
_INDEX_NOT_AUTHORIZED = 'not authorized on test to execute command createIndexes'


async def _seed_an_unindexed_archive(mongo_database: Any) -> None:
    """Leave one committed row and one buffered row behind, on collections carrying no index.

    Rows present and indexes absent is what a reader finds when it is the first process onto a
    restored collection, and it is the only state in which a refusal is reachable at all: with the
    indexes already in place ``ensure_index`` stops at the listing and never calls ``create_index``.
    """
    seeded = MongoDataPointArchive(mongo_database, cipher=ReversingCipher())

    def entry(value: str) -> ArchivedDataPoint:
        return ArchivedDataPoint.from_data_point(
            work_email(value, first=T0, last=T0), session_id=MG, namespace_id=NAMESPACE, epoch=Epoch(1)
        )

    await seeded.archive(entry('a@e.example'))
    await seeded.flush(MG)
    await seeded.archive(entry('b@e.example'))
    for name in _ARCHIVE_COLLECTIONS:
        await mongo_database[name].drop_indexes()


async def _assert_nothing_was_indexed(mongo_database: Any) -> None:
    """Guard against the refusal never having been reached, which would pass for the wrong reason."""
    for name in _ARCHIVE_COLLECTIONS:
        assert set(await mongo_database[name].index_information()) - {'_id_'} == set()


async def test_mg_a_refused_index_does_not_fail_an_archive_read(mongo_database: Any) -> None:
    # `read` ensures its indexes too, so a credential that may not create one reaches `create_index`
    # on the way to every answer the archive gives, not just on the way to a write. The same tolerance
    # an append gets has to hold here — an absent index is a collection scan, so the rows still come
    # back, unindexed and slower — and nothing else pins it: the audit sink's reader ensures nothing.
    await _seed_an_unindexed_archive(mongo_database)
    reader = MongoDataPointArchive(
        _EveryIndexRefusingDatabase(mongo_database, _INDEX_NOT_AUTHORIZED), cipher=ReversingCipher()
    )

    # Both sides of the fold, so the refusal on the buffer's index is exercised as well as the one on
    # the committed collection's.
    assert {point.value for point in await reader.read(MG)} == {'a@e.example', 'b@e.example'}
    await _assert_nothing_was_indexed(mongo_database)


async def test_mg_a_refused_index_does_not_fail_an_archive_flush(mongo_database: Any) -> None:
    # The same for the drain. Pinned separately from the read rather than asserted after it in one
    # test, because the ensure is one-shot per adapter: whichever call ran first would spend the
    # refusal and leave the other running with the flag already set, which is not reaching the
    # refusal rather than surviving it.
    await _seed_an_unindexed_archive(mongo_database)
    flusher = MongoDataPointArchive(
        _EveryIndexRefusingDatabase(mongo_database, _INDEX_NOT_AUTHORIZED), cipher=ReversingCipher()
    )

    assert await flusher.flush(MG) == 1
    await _assert_nothing_was_indexed(mongo_database)


class _CommitCountingCollection:
    """Counts commits, to show a page costs one round trip rather than one per row."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.bulk_writes = 0
        self.update_ones = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def bulk_write(self, *args: Any, **kwargs: Any) -> Any:
        self.bulk_writes += 1
        return await self._inner.bulk_write(*args, **kwargs)

    async def update_one(self, *args: Any, **kwargs: Any) -> Any:
        self.update_ones += 1
        return await self._inner.update_one(*args, **kwargs)


class _CommitCountingDatabase:
    """Routes the archive's committed collection through _CommitCountingCollection."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.committed: Any = None

    def __getitem__(self, name: str) -> Any:
        collection = self._inner[name]
        if name != ArchivedDataPoint.__table_name__:
            return collection
        if self.committed is None:
            self.committed = _CommitCountingCollection(collection)
        return self.committed


async def test_mg_archive_flush_commits_a_page_in_one_round_trip(
    mongo_database: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A round trip against a remote primary dominates the cost of a flush, so the page — not the row —
    # has to be the unit. Counted rather than asserted on the call shape, because what matters is how
    # many times the driver goes to the server.
    monkeypatch.setattr(datapoint_archive, '_PAGE_SIZE', 3)
    counting = _CommitCountingDatabase(mongo_database)
    archive = MongoDataPointArchive(counting, cipher=ReversingCipher())
    for address in ('a@e.example', 'b@e.example', 'c@e.example', 'd@e.example'):
        await archive.archive(
            ArchivedDataPoint.from_data_point(
                work_email(address), session_id=MG, namespace_id=NAMESPACE, epoch=Epoch(1)
            )
        )

    assert await archive.flush(MG) == 4

    # Four rows over a page size of three: two pages, so two commits — not four.
    assert counting.committed.bulk_writes == 2
    assert counting.committed.update_ones == 0


async def test_mg_20_a_replay_pages_the_trail_rather_than_draining_it_whole(
    mongo_database: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A trail is unbounded by construction — it grows with everything the session did, and the chatty
    # shapes (a per-frame probe, a page-view stream) are exactly the ones worth replaying. One cursor
    # over all of it materializes the whole trail; the sibling archive beside it pages for the same
    # reason. The walk is on `sequence`, which the session_trail index serves after the equality.
    monkeypatch.setattr(audit_sink, '_PAGE_SIZE', 2)
    counting = _FindCountingDatabase(mongo_database)
    sink = MongoAuditSink(counting)

    for _ in range(5):
        await sink.append(_audit_entry())
    committed = counting.collections[AUDIT_LOG_COLLECTION]
    committed.finds = 0

    entries = await sink.replay(MG)

    assert committed.finds == 3  # five rows over a page of two: 2 + 2 + 1
    assert len(entries) == 5  # nothing dropped or repeated at a page boundary


async def test_mg_04_archive_committed_survives_restart(mongo_database: Any) -> None:
    archive = MongoDataPointArchive(mongo_database, cipher=ReversingCipher())
    entry = ArchivedDataPoint.from_data_point(
        work_email('a@e.example'), session_id=MG, namespace_id=NAMESPACE, epoch=Epoch(1)
    )
    await archive.archive(entry)
    assert await archive.buffered_count(MG) == 1
    assert len(await archive.read(MG)) == 1  # buffer-transparent: readable before any flush
    assert await archive.flush(MG) == 1

    # A fresh adapter on the same database (a "restart") still sees the committed, deduped archive.
    restarted = MongoDataPointArchive(mongo_database, cipher=ReversingCipher())
    committed = await restarted.read(MG)
    assert len(committed) == 1
    assert committed[0].value == 'a@e.example'  # PII decrypts on read


async def test_mg_06_pii_without_a_cipher_fails_closed(mongo_database: Any) -> None:
    archive = MongoDataPointArchive(mongo_database)  # no cipher → NullCipher
    pii = ArchivedDataPoint.from_data_point(
        work_email('a@e.example'), session_id=MG, namespace_id=NAMESPACE, epoch=Epoch(1)
    )
    with pytest.raises(UnprotectedPiiError):  # refuses to persist cleartext PII at rest
        await archive.archive(pii)


class _PerNamespaceProvider:
    """A test NamespaceCipherProvider: returns a cipher for known namespaces, raises (no key) for the rest."""

    def __init__(self, known: dict[str, ReversingCipher]) -> None:
        self._known = known

    async def for_namespace(self, namespace_id: str) -> ReversingCipher:
        try:
            return self._known[namespace_id]
        except KeyError as error:
            raise LookupError(f'no key for {namespace_id}') from error


async def test_mg_06_archive_seals_pii_under_the_per_namespace_cipher(mongo_database: Any) -> None:
    archive = MongoDataPointArchive(
        mongo_database, cipher_provider=_PerNamespaceProvider({NAMESPACE: ReversingCipher()})
    )
    entry = ArchivedDataPoint.from_data_point(
        work_email('a@e.example'), session_id=MG, namespace_id=NAMESPACE, epoch=Epoch(1)
    )
    await archive.archive(entry)
    await archive.flush(MG)

    committed = await archive.read(MG)
    assert committed[0].value == 'a@e.example'  # sealed under the namespace's key, decrypts on read
    raw = await mongo_database[ArchivedDataPoint.__table_name__].find_one({'session_id': MG})
    assert 'a@e.example' not in raw['value']  # ciphertext at rest, never the plaintext value


async def test_mg_06_archive_pii_fails_closed_when_namespace_has_no_key(mongo_database: Any) -> None:
    archive = MongoDataPointArchive(mongo_database, cipher_provider=_PerNamespaceProvider({}))  # no key for NAMESPACE
    pii = ArchivedDataPoint.from_data_point(
        work_email('a@e.example'), session_id=MG, namespace_id=NAMESPACE, epoch=Epoch(1)
    )
    with pytest.raises(UnprotectedPiiError):  # a provider failure degrades to NullCipher → PII refused
        await archive.archive(pii)


async def test_mg_06_read_raises_clearly_when_namespace_key_unavailable_for_sealed_pii(mongo_database: Any) -> None:
    # Seal PII under the namespace's real key, then read back through a provider that can no longer resolve
    # the key: rather than crashing opaquely inside unseal, read() surfaces PiiKeyUnavailableError.
    cipher = ReversingCipher()
    writer = MongoDataPointArchive(mongo_database, cipher_provider=_PerNamespaceProvider({NAMESPACE: cipher}))
    pii = ArchivedDataPoint.from_data_point(
        work_email('a@e.example'), session_id=MG, namespace_id=NAMESPACE, epoch=Epoch(1)
    )
    await writer.archive(pii)
    await writer.flush(MG)

    reader = MongoDataPointArchive(mongo_database, cipher_provider=_PerNamespaceProvider({}))  # key now unavailable
    with pytest.raises(PiiKeyUnavailableError):
        await reader.read(MG)


async def test_mg_03_audit_appends_are_readable_by_another_process_with_no_recovery_step(
    mongo_database: Any,
) -> None:
    sink = MongoAuditSink(mongo_database)
    for _ in range(3):
        await sink.append(AuditLogEntry(session_id=MG, epoch=Epoch(1), timestamp=T0, kind=AuditKind.DATA_POINT_ADDED))

    # The writer is abandoned here — no completion tail, no resume. A fresh sink on the same database
    # (another pod, or the timeline reader) still sees the whole ordered trail, because the appends
    # went to the committed log rather than to a staging area only the writer would have drained.
    restarted = MongoAuditSink(mongo_database)
    assert len(await restarted.replay(MG)) == 3
    rows = mongo_database[AUDIT_LOG_COLLECTION].find({'session_id': MG}).sort('sequence', 1)
    assert [doc['sequence'] async for doc in rows] == [1, 2, 3]  # contiguous, in the committed log itself


async def test_mg_audit_timestamp_persisted_as_date_not_string(mongo_database: Any) -> None:
    sink = MongoAuditSink(mongo_database)
    await sink.append(_audit_entry())
    # The committed entry's timestamp is a BSON Date (queryable by time in Mongo), not an ISO string.
    committed = await mongo_database[AUDIT_LOG_COLLECTION].find_one({'session_id': MG})
    assert isinstance(committed['entry']['timestamp'], datetime)
    assert not isinstance(committed['entry']['timestamp'], str)
    # And it round-trips through replay back to the original tz-aware instant.
    (entry,) = await sink.replay(MG)
    assert entry.timestamp == T0


async def test_mg_07_audit_append_many_allocates_one_contiguous_range_under_one_meta_cas(mongo_database: Any) -> None:
    sink = MongoAuditSink(mongo_database)
    await sink.append(_audit_entry(kind=AuditKind.OPERATOR_INVOKED))  # sequence 1 — the range continues after it
    meta, committed = _CountingCollection(sink._meta), _CountingCollection(sink._committed)  # noqa: SLF001
    sink._meta, sink._committed = meta, committed  # type: ignore[assignment]

    batch = [_audit_entry(kind=kind) for kind in (AuditKind.DATA_POINT_ADDED,) * 2 + (AuditKind.CAPABILITY_ACTIVATED,)]
    await sink.append_many(batch)

    assert meta.find_one_and_updates == 1  # ONE CAS allocated the whole range
    assert committed.insert_manys == 1  # and ONE ordered insert committed it — two round-trips for the batch
    rows = [doc async for doc in sink._committed.find({'session_id': MG}).sort('sequence', 1)]  # noqa: SLF001
    assert [doc['sequence'] for doc in rows] == [1, 2, 3, 4]  # contiguous, continuing the single append's
    replayed = await sink.replay(MG)
    assert [entry.entry_id for entry in replayed[1:]] == [entry.entry_id for entry in batch]  # order preserved


async def test_mg_08_audit_append_many_stale_epoch_rejects_the_whole_batch_atomically(mongo_database: Any) -> None:
    sink = MongoAuditSink(mongo_database)
    await sink.append(_audit_entry(epoch=2))
    with pytest.raises(StaleEpochError):
        await sink.append_many([_audit_entry(epoch=1), _audit_entry(epoch=1)])
    assert len(await sink.replay(MG)) == 1  # nothing from the fenced batch was written
    meta = await sink._meta.find_one({'_id': MG})  # noqa: SLF001
    assert meta['sequence'] == 1  # and no sequence numbers were burned


async def test_mg_09_archive_many_allocates_one_range_and_folds_exactly_like_singles(mongo_database: Any) -> None:
    archive = MongoDataPointArchive(mongo_database, cipher=ReversingCipher())
    meta, buffer = _CountingCollection(archive._meta), _CountingCollection(archive._buffer)  # noqa: SLF001
    archive._meta, archive._buffer = meta, buffer  # type: ignore[assignment]

    def entry(point: Any) -> ArchivedDataPoint:
        return ArchivedDataPoint.from_data_point(point, session_id=MG, namespace_id=NAMESPACE, epoch=Epoch(1))

    await archive.archive_many(
        [
            entry(work_email('a@e.example', first=T0, last=T0)),
            entry(work_email('b@e.example')),
            entry(work_email('a@e.example', first=T0, last=T0 + timedelta(hours=2))),  # re-observation in-batch
        ]
    )

    assert meta.find_one_and_updates == 1  # ONE CAS allocated the whole range
    assert buffer.insert_manys == 1
    buffered = [doc async for doc in archive._buffer.find({'session_id': MG}).sort('sequence', 1)]  # noqa: SLF001
    assert [doc['sequence'] for doc in buffered] == [1, 2, 3]  # per-observation granularity, contiguous
    assert await archive.flush(MG) == 3
    committed = {doc.value: doc for doc in await archive.read(MG)}
    assert set(committed) == {'a@e.example', 'b@e.example'}  # the keyed-upsert fold still dedups
    assert committed['a@e.example'].last_retrieved == T0 + timedelta(hours=2)  # and still bumps last_retrieved


async def test_mg_10_archive_many_pii_fail_closed_burns_no_sequence(mongo_database: Any) -> None:
    archive = MongoDataPointArchive(mongo_database)  # no cipher → NullCipher → PII must fail closed

    def entry(point: Any) -> ArchivedDataPoint:
        return ArchivedDataPoint.from_data_point(point, session_id=MG, namespace_id=NAMESPACE, epoch=Epoch(1))

    with pytest.raises(UnprotectedPiiError):  # the PII check runs per entry BEFORE the meta allocation
        await archive.archive_many([entry(risk(0.5)), entry(work_email('a@e.example'))])
    assert await archive.buffered_count(MG) == 0  # the whole batch was refused — no partial buffer

    await archive.archive(entry(risk(0.5)))  # a later valid write starts at sequence 1: nothing was burned
    buffered = [doc async for doc in archive._buffer.find({'session_id': MG})]  # noqa: SLF001
    assert [doc['sequence'] for doc in buffered] == [1]


async def _suppress_buffer_delete(buffer: Any) -> Callable[[], None]:
    """Stub the buffer's ``delete_many`` to a no-op, simulating a crash AFTER the committing
    upsert loop but BEFORE the buffer is drained. Returns a restorer so the resume can re-flush."""
    original = buffer.delete_many

    async def _noop(*args: Any, **kwargs: Any) -> None:  # noqa: ARG001
        return None

    buffer.delete_many = _noop  # type: ignore[method-assign]

    def restore() -> None:
        buffer.delete_many = original  # type: ignore[method-assign]

    return restore


async def test_mg_11_audit_empty_batch_writes_nothing_and_burns_no_sequence(mongo_database: Any) -> None:
    sink = MongoAuditSink(mongo_database)
    await sink.append_many([])  # the empty-batch boundary short-circuits before the meta CAS
    assert await sink.replay(MG) == ()  # nothing written
    assert await mongo_database['audit_meta'].find_one({'_id': MG}) is None  # and no range allocated


async def test_mg_12_audit_a_reused_sequence_number_surfaces_instead_of_being_absorbed(
    mongo_database: Any,
) -> None:
    # The CAS hands out each sequence exactly once, so a document already sitting on a (session,
    # sequence) _id means two writers claimed the same number — a real fault. Absorbing the duplicate
    # key would report a durable append that never landed, and hide the collision behind it.
    await mongo_database[AUDIT_LOG_COLLECTION].insert_one({'_id': f'{MG}\x001', 'session_id': MG, 'sequence': 1})

    sink = MongoAuditSink(mongo_database)
    with pytest.raises(BulkWriteError):
        await sink.append(_audit_entry())


async def test_mg_13_archive_reflush_after_crash_before_buffer_delete_does_not_duplicate(mongo_database: Any) -> None:
    # Same crash-window invariant for the archive: the committed (session, type, value_hash) _id plus
    # $setOnInsert dedups a redundant re-flush, and $max keeps last_retrieved from regressing
    # (datapoint_archive.py :117-142).
    archive = MongoDataPointArchive(mongo_database, cipher=ReversingCipher())

    def entry(point: Any) -> ArchivedDataPoint:
        return ArchivedDataPoint.from_data_point(point, session_id=MG, namespace_id=NAMESPACE, epoch=Epoch(1))

    await archive.archive(entry(work_email('a@e.example', first=T0, last=T0)))
    await archive.archive(entry(work_email('b@e.example', first=T0, last=T0 + timedelta(hours=2))))
    assert await archive.buffered_count(MG) == 2

    restore = await _suppress_buffer_delete(archive._buffer)  # noqa: SLF001
    assert await archive.flush(MG) == 2  # commits, but the simulated crash drops the buffer delete
    assert await archive.buffered_count(MG) == 2  # the buffer survived — nothing is dropped
    committed = {entry.value: entry for entry in await archive.read(MG)}
    assert set(committed) == {'a@e.example', 'b@e.example'}  # folded once

    restore()
    assert await archive.flush(MG) == 2  # the resume re-flushes the still-buffered observations
    reread = {entry.value: entry for entry in await archive.read(MG)}
    assert set(reread) == {'a@e.example', 'b@e.example'}  # keyed upsert deduped on read
    # Count the raw committed docs, not the by-value read fold: the deterministic _id must keep the
    # re-flush from inserting fresh documents, so the collection holds exactly two, never four.
    committed_ids = [doc['_id'] async for doc in archive._committed.find({})]  # noqa: SLF001
    assert len(committed_ids) == len(set(committed_ids)) == 2
    assert reread['b@e.example'].last_retrieved == T0 + timedelta(hours=2)  # $max kept last from regressing
    assert await archive.buffered_count(MG) == 0  # and the buffer is finally drained


async def test_mg_14_archive_retention_ttl_index_and_tz_aware_read_round_trip(mongo_database: Any) -> None:
    # The retention branch (datapoint_archive.py :60-62) only runs when retention_seconds is set, and a
    # real BSON date round-trip must come back tz-AWARE (the _aware fix-up, :151-159). A recent
    # last_retrieved is used so the live TTL reaper does not age the doc out before the assertion.
    now = datetime.now(timezone.utc)
    archive = MongoDataPointArchive(mongo_database, cipher=ReversingCipher(), retention_seconds=3600)
    entry = ArchivedDataPoint.from_data_point(
        work_email('a@e.example', first=now, last=now), session_id=MG, namespace_id=NAMESPACE, epoch=Epoch(1)
    )
    await archive.archive(entry)
    assert await archive.flush(MG) == 1

    committed = await archive.read(MG)
    assert len(committed) == 1
    assert committed[0].value == 'a@e.example'  # PII still decrypts on read through the cipher seam
    assert committed[0].last_retrieved.tzinfo is not None  # the BSON-naive date was restored to tz-aware UTC
    assert committed[0].last_retrieved.utcoffset() == timezone.utc.utcoffset(None)

    indexes = await archive._committed.index_information()  # noqa: SLF001
    ttl = next(spec for name, spec in indexes.items() if spec.get('key') == [('last_retrieved', 1)])
    assert ttl['expireAfterSeconds'] == 3600  # the TTL ages the raw archive out on last_retrieved


async def test_mg_15_archive_ensure_indexes_is_idempotent_across_archive_calls(mongo_database: Any) -> None:
    # _ensure_indexes guards on a one-shot flag, so a second archive_many must NOT rebuild indexes.
    archive = MongoDataPointArchive(mongo_database, cipher=ReversingCipher(), retention_seconds=3600)
    create_index_calls = 0
    original_create_index = archive._committed.create_index  # noqa: SLF001

    async def _counting_create_index(*args: Any, **kwargs: Any) -> Any:
        nonlocal create_index_calls
        create_index_calls += 1
        return await original_create_index(*args, **kwargs)

    archive._committed.create_index = _counting_create_index  # type: ignore[method-assign]  # noqa: SLF001

    def entry(value: str) -> ArchivedDataPoint:
        return ArchivedDataPoint.from_data_point(
            work_email(value), session_id=MG, namespace_id=NAMESPACE, epoch=Epoch(1)
        )

    await archive.archive(entry('a@e.example'))
    after_first = create_index_calls
    assert after_first > 0  # the first write built the analytics + TTL indexes
    await archive.archive(entry('b@e.example'))
    assert create_index_calls == after_first  # the one-shot flag short-circuited the second build


async def test_mg_16_same_value_under_two_sessions_stays_isolated_in_one_collection(mongo_database: Any) -> None:
    # PII value_hash mixes in session_id AND the committed _id is session-prefixed, so the same email
    # archived under two sessions into the single __table_name__ collection lands as two distinct docs
    # (datapoint_archive.py :122). A regression dropping session from the _id would collapse them.
    other = SessionId('mg-other-session')
    archive = MongoDataPointArchive(mongo_database, cipher=ReversingCipher())

    def entry(session: SessionId) -> ArchivedDataPoint:
        return ArchivedDataPoint.from_data_point(
            work_email('a@e.example'), session_id=session, namespace_id=NAMESPACE, epoch=Epoch(1)
        )

    await archive.archive(entry(MG))
    await archive.flush(MG)
    await archive.archive(entry(other))
    await archive.flush(other)

    for_mg = await archive.read(MG)
    for_other = await archive.read(other)
    assert [point.value for point in for_mg] == ['a@e.example']  # each session reads exactly its own
    assert [point.value for point in for_other] == ['a@e.example']
    committed_ids = [doc['_id'] async for doc in archive._committed.find({})]  # noqa: SLF001
    assert len(committed_ids) == len(set(committed_ids)) == 2  # two distinct keys in one collection


# ---------------------------------------------------------------------------
# MG-17 — fencing collection layout + TTL index tests
# ---------------------------------------------------------------------------


async def test_mg_17_durable_guard_lands_in_fencing_collection_not_data(mongo_database: Any) -> None:
    # After an upsert, the epoch-guard doc lives in <table>-fencing (_id = key), NOT in the data
    # collection. The data collection must contain exactly one document (the versioned payload) with
    # no \x00durable-epoch\x00 sentinel docs.
    durable = MongoDurableStore(mongo_database)
    await durable.upsert(TBL, 'k', {'a': 1}, expected_version=0, epoch=Epoch(1))

    # The guard is in <table>-fencing with _id = key, epoch, updated_at.
    fencing_name = f'{TBL}-fencing'
    guard = await mongo_database[fencing_name].find_one({'_id': 'k'})
    assert guard is not None
    assert guard['epoch'] == 1
    # mongomock strips tzinfo on round-trip (same as the data-collection read path); we assert the
    # field is a datetime (proving it was stored) — the production code writes tz-aware UTC.
    assert isinstance(guard['updated_at'], datetime)

    # The data collection has only the versioned payload — no \x00durable-epoch\x00 sentinel.
    all_data_ids = [doc['_id'] async for doc in mongo_database[TBL].find({})]
    assert all_data_ids == ['k']
    assert not any('\x00durable-epoch\x00' in str(doc_id) for doc_id in all_data_ids)


async def test_mg_17b_lower_epoch_raises_stale_epoch_across_new_collection(mongo_database: Any) -> None:
    # The fence still works from the new fencing collection: a lower-epoch write is rejected.
    durable = MongoDurableStore(mongo_database)
    await durable.upsert(TBL, 'k', {'a': 1}, expected_version=0, epoch=Epoch(2))
    with pytest.raises(StaleEpochError):
        await durable.upsert(TBL, 'k', {'a': 2}, expected_version=1, epoch=Epoch(1))


async def test_mg_17c_add_to_set_shares_fencing_collection_fence_with_upsert(mongo_database: Any) -> None:
    # add_to_set and upsert write their guards to the same <table>-fencing document (_id = key),
    # so they share one fence — a prior add_to_set at epoch 2 fences a lower-epoch upsert.
    durable = MongoDurableStore(mongo_database)
    await durable.add_to_set(TBL, 'k', 'sessions', 'v1', epoch=Epoch(2))

    fencing_name = f'{TBL}-fencing'
    guard = await mongo_database[fencing_name].find_one({'_id': 'k'})
    assert guard is not None and guard['epoch'] == 2  # guard is in the fencing collection

    with pytest.raises(StaleEpochError):
        await durable.upsert(TBL, 'k', {'a': 1}, expected_version=0, epoch=Epoch(1))


async def test_mg_17d_contribution_guard_in_contributions_fencing_markers_in_contributions(
    mongo_database: Any,
) -> None:
    # The contribution epoch guard must land in contributions-fencing (_id = session_id), while the
    # real marker (_id = session\x00operator) stays in contributions.
    durable = MongoDurableStore(mongo_database)
    marked = await durable.mark_contribution(MG, SessionId('op-a'), epoch=Epoch(1))  # type: ignore[arg-type]
    assert marked is True

    # Guard in contributions-fencing.
    cf_guard = await mongo_database['contributions-fencing'].find_one({'_id': MG})
    assert cf_guard is not None
    assert cf_guard['epoch'] == 1
    # mongomock strips tzinfo on round-trip; assert the field is a datetime (production code writes tz-aware UTC).
    assert isinstance(cf_guard['updated_at'], datetime)

    # Real marker in contributions, NOT contributions-fencing.
    marker = await mongo_database['contributions'].find_one({'_id': f'{MG}\x00op-a'})
    assert marker is not None

    # No \x00contribution-epoch\x00 sentinel in contributions.
    all_contrib_ids = [doc['_id'] async for doc in mongo_database['contributions'].find({})]
    assert not any('\x00contribution-epoch\x00' in str(doc_id) for doc_id in all_contrib_ids)


async def test_mg_17e_ttl_index_exists_on_fencing_collection_with_configured_seconds(mongo_database: Any) -> None:
    # The TTL index must be created on <table>-fencing with the configured expireAfterSeconds.
    ttl_seconds = 3600
    durable = MongoDurableStore(mongo_database, fencing_ttl_seconds=ttl_seconds)
    await durable.upsert(TBL, 'k', {'a': 1}, expected_version=0, epoch=Epoch(1))

    fencing_name = f'{TBL}-fencing'
    indexes = await mongo_database[fencing_name].index_information()
    ttl_index = next(
        (spec for spec in indexes.values() if spec.get('key') == [('updated_at', 1)]),
        None,
    )
    assert ttl_index is not None, f'No TTL index on updated_at found in {fencing_name}'
    assert ttl_index['expireAfterSeconds'] == ttl_seconds


async def test_mg_17f_ttl_index_on_contributions_fencing_with_configured_seconds(mongo_database: Any) -> None:
    # The contributions-fencing collection also gets a TTL index with the configured TTL.
    ttl_seconds = 1800
    durable = MongoDurableStore(mongo_database, fencing_ttl_seconds=ttl_seconds)
    await durable.mark_contribution(MG, SessionId('op-b'), epoch=Epoch(1))  # type: ignore[arg-type]

    indexes = await mongo_database['contributions-fencing'].index_information()
    ttl_index = next(
        (spec for spec in indexes.values() if spec.get('key') == [('updated_at', 1)]),
        None,
    )
    assert ttl_index is not None, 'No TTL index on updated_at found in contributions-fencing'
    assert ttl_index['expireAfterSeconds'] == ttl_seconds


async def test_mg_18_the_read_fold_refuses_to_walk_a_stored_epoch_back() -> None:
    # The Mongo fold is `read`'s stand-in for the server's `$max`, and has to refuse an out-of-order
    # pair for the same reason the server does. Asserted here rather than through the port: the meta
    # CAS rejects an epoch below the session's high water mark, so an older-epoch re-observation can
    # never be buffered and no sequence of archive/flush/read calls can tell `max(existing, sealed)`
    # apart from taking whichever one arrived last.
    def entry(epoch: int, last: datetime) -> ArchivedDataPoint:
        return ArchivedDataPoint.from_data_point(
            work_email('a@e.example', first=T0, last=last), session_id=MG, namespace_id=NAMESPACE, epoch=Epoch(epoch)
        )

    folded: dict[tuple[str, str], ArchivedDataPoint] = {}
    mongo_fold_in(folded, entry(2, T0 + timedelta(hours=2)))
    mongo_fold_in(folded, entry(1, T0))

    (merged,) = folded.values()
    assert int(merged.epoch) == 2  # not 1 — the out-of-order arrival does not lower it
    assert merged.last_retrieved == T0 + timedelta(hours=2)  # nor does it walk the timestamp back


class _FindCountingCollection:
    """Counts ``find`` calls, so a read paging its way through a collection is visible as many."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.finds = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def find(self, *args: Any, **kwargs: Any) -> Any:
        self.finds += 1
        return self._inner.find(*args, **kwargs)


class _FindCountingDatabase:
    """Routes every collection through _FindCountingCollection, keeping one wrapper per name."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.collections: dict[str, _FindCountingCollection] = {}

    def __getitem__(self, name: str) -> Any:
        return self.collections.setdefault(name, _FindCountingCollection(self._inner[name]))


async def test_mg_19_a_read_pages_both_sides_rather_than_draining_them_whole(
    mongo_database: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A session's committed rows are its distinct observed VALUES — value_hash is a MAC of the value,
    # so a chatty session runs to tens of thousands — and the buffer carries one row per observation
    # on top of that. Draining either side in a single cursor is the memory growth the page size
    # exists to prevent during a flush, so the read has to page the same way.
    monkeypatch.setattr(datapoint_archive, '_PAGE_SIZE', 2)
    counting = _FindCountingDatabase(mongo_database)
    archive = MongoDataPointArchive(counting, cipher=ReversingCipher())

    def entry(value: str, last: datetime = T0) -> ArchivedDataPoint:
        return ArchivedDataPoint.from_data_point(
            work_email(value, first=T0, last=last), session_id=MG, namespace_id=NAMESPACE, epoch=Epoch(1)
        )

    for address in ('a@e.example', 'b@e.example', 'c@e.example', 'd@e.example', 'e@e.example'):
        await archive.archive(entry(address))
    await archive.flush(MG)  # five committed rows
    for address in ('f@e.example', 'g@e.example', 'a@e.example'):  # and three buffered, one a re-observation
        await archive.archive(entry(address, last=T0 + timedelta(hours=2)))

    committed_collection = counting.collections[ArchivedDataPoint.__table_name__]
    buffer_collection = counting.collections['datapoint_archive_buffer']
    committed_collection.finds = 0
    buffer_collection.finds = 0

    entries = await archive.read(MG)

    assert committed_collection.finds == 3  # five rows over a page of two: 2 + 2 + 1
    assert buffer_collection.finds == 2  # three rows: 2 + 1
    by_value = {entry.value: entry for entry in entries}
    assert len(by_value) == 7  # nothing dropped or duplicated by the paging
    assert by_value['a@e.example'].last_retrieved == T0 + timedelta(hours=2)  # and the fold still merged
