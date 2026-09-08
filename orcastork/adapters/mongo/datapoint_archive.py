"""Mongo ``DataPointArchive`` — a durable per-observation buffer folded into raw docs.

``archive_many`` allocates one contiguous sequence range under a single meta CAS
(rejecting a stale-epoch writer for the whole batch, exactly as the audit sink does) and
buffers the epoch-stamped, sequence-numbered documents in one ordered ``insert_many``;
``archive`` is the batch of one. ``flush``
folds the buffered observations into the ``orcastork-datapoints`` collection via
**keyed-upsert** on a composite ``_id`` of ``(session, type, value_hash)``: ``$setOnInsert``
fixes the immutable fields and ``$max`` advances ``last_retrieved``/``epoch``, so
redelivery and replay are idempotent and the buffer survives a crash before flush.

PII protection: the value is sealed via a :class:`ValueCipher`, and the ``value_hash`` key
is a **keyed** MAC of the value (mixing in ``session_id``) — never a bare plaintext digest.
The cipher is resolved per batch: an injected :class:`NamespaceCipherProvider` keys each namespace's PII
under its own key (resolved from the entries' ``namespace_id``), or a single static
cipher applies to all namespaces. This adapter is the *production*, PII-heavy store, so it **fails
closed**: archiving an ``is_pii`` value while only the passthrough ``NullCipher`` is wired (or
the provider could not resolve a key) raises :class:`UnprotectedPiiError` rather than persisting
cleartext. An optional retention TTL ages the raw archive out on ``last_retrieved`` (a native
BSON date).
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

from loguru import logger
from pymongo import ReturnDocument, UpdateOne
from pymongo.errors import DuplicateKeyError

from ...archive import ArchivedDataPoint, NamespaceCipherProvider, NullCipher, ValueCipher
from ...archive.sealing import seal, unseal, value_hash
from ...exceptions import PiiKeyUnavailableError, StaleEpochError, UnprotectedPiiError
from ...ids import SessionId
from .indexes import ensure_index

_DATE_FIELDS = ('first_retrieved', 'last_retrieved')


# Rows carried per round-trip, by a flush draining the buffer and by a read paging either side. The
# audit sink writes at append and so needs no equivalent; this one is the archive's own.
_PAGE_SIZE = 500


def _to_stored_resolution(moment: datetime) -> datetime:
    """Drop a timestamp to the millisecond a BSON date can hold.

    Applied on the way into the buffer so both sides of a flush carry the same instant. A BSON date
    is milliseconds since the epoch, so a committed row can never return the microseconds the clock
    produced — while a buffered row validates from an ISO string and keeps all six digits. Left
    alone, a read before a flush differs from a read after one by up to a millisecond, which is the
    port's buffer-transparency promise broken on the production clock; ``$max`` and the fold's mirror
    of it drift the same way, since only one of them can see the finer value.
    """
    return moment.replace(microsecond=moment.microsecond // 1000 * 1000)


def _fold_in(folded: dict[tuple[str, str], ArchivedDataPoint], sealed: ArchivedDataPoint) -> None:
    """Apply one sealed observation the way the committed keyed-upsert would.

    Mirrors the ``$setOnInsert`` + ``$max`` merge ``flush`` performs in Mongo: the first observation
    of a ``(type, value_hash)`` supplies every field, and a re-observation only advances
    ``last_retrieved`` and ``epoch``. This is a second expression of that merge, in Python, so it is
    pinned to the first by the conformance property that a read before a flush equals a read after
    one — a divergence fails the suite rather than silently returning what no flush would produce.
    """
    key = (sealed.type, sealed.value_hash)
    existing = folded.get(key)
    if existing is None:
        folded[key] = sealed
        return
    folded[key] = existing.model_copy(
        update={
            'last_retrieved': max(existing.last_retrieved, sealed.last_retrieved),
            'epoch': max(existing.epoch, sealed.epoch),
        }
    )


class MongoDataPointArchive:
    def __init__(
        self,
        database: Any,
        *,
        cipher: ValueCipher | None = None,
        cipher_provider: NamespaceCipherProvider | None = None,
        retention_seconds: int | None = None,
    ) -> None:
        self._buffer = database['datapoint_archive_buffer']
        # The committed collection is named by the model's __table_name__ (the destination decider),
        # not a hardcoded constant — a flow that subclasses ArchivedDataPoint reroutes by overriding it.
        self._committed = database[ArchivedDataPoint.__table_name__]
        self._meta = database['datapoint_archive_meta']
        # A per-namespace provider (PII sealed under a per-namespace key) takes precedence; otherwise a single
        # static cipher applies to every namespace. With neither, the passthrough refuses PII (fails closed).
        self._cipher = cipher or NullCipher()
        self._cipher_provider = cipher_provider
        self._retention_seconds = retention_seconds
        self._indexes_ready = False

    async def _cipher_for(self, namespace_id: str) -> ValueCipher:
        """Resolve the cipher for a namespace. Without a provider, the static cipher applies to all namespaces.
        A provider failure degrades to ``NullCipher`` so the PII gate refuses rather than sealing
        under a wrong/shared key — fail closed."""
        if self._cipher_provider is None:
            return self._cipher
        try:
            return await self._cipher_provider.for_namespace(namespace_id)
        except Exception:  # noqa: BLE001 — provider-defined failures; degrade to the refusing passthrough
            logger.warning(
                'per-namespace cipher provider failed to resolve key; degrading to fail-closed',
                namespace_id=namespace_id,
            )
            return NullCipher()

    async def _ensure_indexes(self) -> None:
        if self._indexes_ready:
            return
        # Secondary index for analytics scans by namespace/type; the unique (session, type, value_hash)
        # key is the composite `_id`, so it needs no separate index.
        await ensure_index(self._committed, [('namespace_id', 1), ('type', 1)], name='namespace_type_scan')
        # The buffer is read by session in sequence order, a page at a time, and counted by session —
        # so it wants the same shape as the audit log's. Unindexed, each page of a flush scans every
        # session's buffered rows, which is the cost that made an interrupted flush compound.
        await ensure_index(self._buffer, [('session_id', 1), ('sequence', 1)], name='session_trail')
        if self._retention_seconds is not None:
            # last_retrieved is a native BSON date, so the TTL reaper can act on it directly.
            await ensure_index(
                self._committed,
                [('last_retrieved', 1)],
                name='retention_reaper',
                expireAfterSeconds=self._retention_seconds,
            )
        self._indexes_ready = True

    async def archive(self, entry: ArchivedDataPoint) -> None:
        await self.archive_many([entry])

    async def archive_many(self, entries: Sequence[ArchivedDataPoint]) -> None:
        if not entries:
            return
        await self._ensure_indexes()
        # A batch is one writer's entries for one session, hence one namespace — resolve its cipher once.
        cipher = await self._cipher_for(str(entries[0].namespace_id))
        for entry in entries:
            if entry.is_pii and isinstance(cipher, NullCipher):
                # Fail closed, and BEFORE any sequence allocation: never persist PII at rest without
                # a real cipher (a wiring mistake must not leak), and a refused batch must not burn
                # sequence numbers either.
                raise UnprotectedPiiError(
                    f'refusing to archive PII {entry.type!r} for {entry.session_id} without a cipher'
                )
        # Fence the epoch and allocate the whole sequence range atomically (mirrors the audit sink):
        # the conditional upsert matches only when max_epoch is not newer, so a stale writer fails the
        # predicate and the upsert insert then collides on _id → DuplicateKeyError — the entire batch
        # is rejected before anything is buffered. A batch is one writer's entries for one session,
        # so the first entry carries the epoch/session for the whole range, numbered last-len+1..last.
        first = entries[0]
        current_epoch = int(first.epoch)
        not_superseded = [{'max_epoch': {'$exists': False}}, {'max_epoch': {'$lte': current_epoch}}]
        try:
            meta = await self._meta.find_one_and_update(
                {'_id': first.session_id, '$or': not_superseded},
                {'$inc': {'sequence': len(entries)}, '$max': {'max_epoch': current_epoch}},
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
        except DuplicateKeyError as error:
            raise StaleEpochError(f'epoch {first.epoch} is stale for session {first.session_id}') from error
        if meta is None:
            raise StaleEpochError(f'epoch {first.epoch} is stale for session {first.session_id}')
        last = int(meta['sequence'])
        documents = []
        for index, entry in enumerate(entries):
            # Derive the key from the plaintext value, then seal the value — order matters.
            keyed = seal(entry, cipher).model_copy(
                update={
                    'value_hash': value_hash(entry, cipher),
                    'first_retrieved': _to_stored_resolution(entry.first_retrieved),
                    'last_retrieved': _to_stored_resolution(entry.last_retrieved),
                }
            )
            documents.append(
                {
                    'session_id': entry.session_id,
                    'sequence': last - len(entries) + 1 + index,
                    'entry': keyed.model_dump(mode='json'),
                }
            )
        await self._buffer.insert_many(documents, ordered=True)

    async def flush(self, session_id: SessionId) -> int:
        """Drain this session's buffered rows into the committed collection, a page at a time.

        Paged, committed and cleared per batch so the cost of a flush is bounded rather than
        proportional to the session's whole trail: reading it all at once grows memory with the trail,
        and clearing only after every row is committed means an interrupted pass leaves work committed
        but nothing cleared, so the next attempt re-reads the identical buffer and fails in the same
        place — each pass making the following one slower. This shares ``_bounded_tail_step``'s timeout
        with the rest of the tail, so leaving durable progress behind is what lets a resume finish.

        The page advances on ``sequence`` rather than on the delete having landed, so a pass that
        commits and then dies before clearing still terminates; re-committing is harmless because the
        ``_id`` is derived from the value and the merge is ``$setOnInsert`` plus ``$max``.

        A page is one round trip rather than one per row, which is the whole cost of a flush against
        a remote primary.

        Ordered, for equivalence rather than safety: the buffer does not deduplicate, so a page can
        carry two upserts for one ``_id``, and applying them in sequence is exactly what the previous
        row-at-a-time loop did. Unordered was measured against a real server and does NOT collide on
        that pair, and ``$max`` makes the merged result order-independent in any case — so ordered
        costs nothing here and removes a question rather than answering one. Revisit it if a page ever
        grows large enough for the server's parallelism to matter.
        """
        await self._ensure_indexes()
        flushed = 0
        after_sequence = -1
        while True:
            batch = [
                doc
                async for doc in self._buffer.find({'session_id': session_id, 'sequence': {'$gt': after_sequence}})
                .sort('sequence', 1)
                .limit(_PAGE_SIZE)
            ]
            if not batch:
                return flushed
            operations = []
            for doc in batch:
                # Round-trip the buffered JSON back to the model so the committed doc carries native BSON
                # dates: `$max` then orders chronologically regardless of string formatting, and the TTL
                # reaper can act on last_retrieved. The sealed value/keyed value_hash ride along unchanged.
                entry = ArchivedDataPoint.model_validate(doc['entry'])
                operations.append(
                    UpdateOne(
                        {'_id': f'{session_id}\x00{entry.type}\x00{entry.value_hash}'},
                        {
                            '$setOnInsert': {
                                'session_id': entry.session_id,
                                'namespace_id': entry.namespace_id,
                                'type': entry.type,
                                'value_hash': entry.value_hash,
                                'value': entry.value,
                                'retrieved_by': entry.retrieved_by,
                                'first_retrieved': entry.first_retrieved,
                                'is_pii': entry.is_pii,
                                'schema_version': entry.schema_version,
                            },
                            '$max': {'last_retrieved': entry.last_retrieved, 'epoch': int(entry.epoch)},
                        },
                        upsert=True,
                    )
                )
            await self._committed.bulk_write(operations, ordered=True)
            await self._buffer.delete_many({'_id': {'$in': [doc['_id'] for doc in batch]}})
            after_sequence = int(batch[-1]['sequence'])
            flushed += len(batch)

    async def read(self, session_id: SessionId) -> tuple[ArchivedDataPoint, ...]:
        """Committed rows folded together with any still-buffered ones, in first-observation order.

        Folded rather than concatenated: the buffer keeps per-observation granularity, so appending it
        raw would surface duplicates the keyed-upsert collapses. Committed rows fold in first and the
        buffer follows in sequence order — the order a flush would apply them — so an in-flight
        session reads as what a flush-then-read would return, without the read writing anything.

        Both sides are paged, and neither is materialized whole. A session's committed row count is
        the number of distinct VALUES it observed — ``value_hash`` is a MAC of the value, not of the
        leaf type — so a chatty session (a per-frame probe, a page-view stream) reaches tens of
        thousands of rows, and the buffer holds one row per observation on top of that until a flush
        drains it. Folding a page at a time keeps the peak at one page plus the answer itself. The
        answer is the cap that remains: one entry per identity is what an "everything for this
        session" contract owes, and ``replay_session`` re-runs a flow over exactly these rows, so a
        limit here would silently replay a different session.
        """
        await self._ensure_indexes()
        folded: dict[tuple[str, str], ArchivedDataPoint] = {}
        # Ranged over the composite `_id` rather than filtered on `session_id`, which has no index on
        # the committed collection — an equality there scans every namespace's and every session's rows
        # inside the retention window. The `_id` is `session\x00type\x00value_hash` and a session id
        # holds no NUL, so `[sid\x00, sid\x01)` is exactly this session's keys and rides the `_id`
        # index. Paging on the same key needs no second sort.
        upper_bound = f'{session_id}\x01'
        after_id = f'{session_id}\x00'
        while True:
            page = [
                self._aware(row)
                async for row in self._committed.find({'_id': {'$gt': after_id, '$lt': upper_bound}})
                .sort('_id', 1)
                .limit(_PAGE_SIZE)
            ]
            for row in page:
                _fold_in(folded, ArchivedDataPoint.model_validate(row))
            if len(page) < _PAGE_SIZE:
                break
            after_id = str(page[-1]['_id'])
        # The buffer holds each observation as a JSON dump under `entry`, so it validates directly —
        # its datetimes round-trip as ISO strings, and only the committed rows need `_aware`. Paged on
        # `sequence`, the way `flush` drains it, so the two agree on the order they apply.
        after_sequence = -1
        while True:
            page = [
                row
                async for row in self._buffer.find({'session_id': session_id, 'sequence': {'$gt': after_sequence}})
                .sort('sequence', 1)
                .limit(_PAGE_SIZE)
            ]
            for row in page:
                _fold_in(folded, ArchivedDataPoint.model_validate(row['entry']))
            if len(page) < _PAGE_SIZE:
                break
            after_sequence = int(page[-1]['sequence'])
        if not folded:
            return ()
        # Ties broken on the identity key, not on the order the pages happened to arrive in: rows
        # sharing a first_retrieved sort one way when they are read from the buffer and another once
        # they are committed, which would make a read before a flush differ from a read after one for
        # no reason a caller could see.
        entries = sorted(folded.values(), key=lambda entry: (entry.first_retrieved, entry.type, entry.value_hash))
        # All rows for a session share its namespace, so resolve the unsealing cipher once.
        namespace_id = str(entries[0].namespace_id)
        cipher = await self._cipher_for(namespace_id)
        # PII is only ever persisted under a real cipher (write fails closed), so a passthrough cipher
        # against sealed PII rows means the namespace key is unavailable — surface it clearly instead of
        # letting unseal recover garbage (or crash opaquely) with no context.
        if isinstance(cipher, NullCipher) and any(entry.is_pii for entry in entries):
            logger.error(
                'per-namespace cipher unavailable; cannot unseal sealed PII on read', namespace_id=namespace_id
            )
            raise PiiKeyUnavailableError(f'cannot unseal PII for session {session_id}: namespace key unavailable')
        return tuple(unseal(entry, cipher) for entry in entries)

    async def buffered_count(self, session_id: SessionId) -> int:
        return int(await self._buffer.count_documents({'session_id': session_id}))

    @staticmethod
    def _aware(row: dict[str, Any]) -> dict[str, Any]:
        # Mongo returns BSON dates tz-naive (UTC); restore the timezone so timestamps round-trip exactly.
        normalized = dict(row)
        for field_name in _DATE_FIELDS:
            value = normalized.get(field_name)
            if isinstance(value, datetime) and value.tzinfo is None:
                normalized[field_name] = value.replace(tzinfo=timezone.utc)
        return normalized
