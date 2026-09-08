"""Mongo ``AuditSink`` — a durable, epoch-fenced, append-only event log.

``append_many`` allocates one contiguous sequence range under a single meta CAS (rejecting a
stale-epoch writer for the whole batch) and writes the epoch-stamped, sequence-numbered documents
straight to the ``orcastork-audit-log`` collection in one ordered ``insert_many``; ``append`` is
the batch of one. An entry is durable and visible to ``replay`` as soon as its append returns, so
the trail of a session that crashes — or one still running — is readable without any further step.
The audit is independent of the live store.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from ...audit import AuditLogEntry
from ...exceptions import StaleEpochError
from ...ids import SessionId
from .indexes import ensure_index

AUDIT_LOG_COLLECTION = 'orcastork-audit-log'

# Rows fetched per round trip when replaying a trail. A session's audit is unbounded by construction —
# it grows with everything the flow did, and a chatty one (a per-frame probe, a page-view stream) runs
# to tens of thousands of entries — so the read pages rather than opening one cursor over all of it.
_PAGE_SIZE = 500


class MongoAuditSink:
    def __init__(self, database: Any) -> None:
        self._committed = database[AUDIT_LOG_COLLECTION]
        self._meta = database['audit_meta']
        self._indexes_ready = False

    async def _ensure_indexes(self) -> None:
        if self._indexes_ready:
            return
        # Every read of this collection is one session's trail in sequence order — `replay` here, and
        # whatever timeline view the embedding application builds over the same collection. Without
        # this the trail is a collection scan across every session's rows plus an in-memory sort,
        # which grows with the whole log rather than with the session being read. `audit_meta` needs
        # nothing: it is keyed by session id, so `_id` already serves it.
        await ensure_index(self._committed, [('session_id', 1), ('sequence', 1)], name='session_trail')
        self._indexes_ready = True

    async def append(self, entry: AuditLogEntry) -> None:
        await self.append_many([entry])

    async def append_many(self, entries: Sequence[AuditLogEntry]) -> None:
        if not entries:
            return
        await self._ensure_indexes()
        # Fence the epoch and allocate the whole sequence RANGE in one atomic step: the conditional
        # upsert only matches when the session's max_epoch is not newer, so concurrent appends can
        # neither both pass a stale check nor collide on sequence numbers. A stale writer fails the
        # predicate, and the upsert insert then collides on _id → DuplicateKeyError — the entire
        # batch is rejected before anything is written. A batch is one writer's events for one
        # session (the sole mutator batches its own appends), so the first entry carries the
        # epoch/session for the whole range; entries take the range's numbers in input order.
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
        first_sequence = int(meta['sequence']) - len(entries) + 1
        # The ``_id`` is derived from the session and the sequence, and the CAS above hands out each
        # sequence number exactly once, so a duplicate key here means two writers claimed the same
        # number — a fault worth surfacing, not a re-presented row to absorb. The session id comes
        # from the same entry the CAS fenced on: a sequence number only means anything within the
        # session whose meta allocated it.
        await self._committed.insert_many(
            [
                {
                    '_id': f'{first.session_id}\x00{first_sequence + index}',
                    'session_id': first.session_id,
                    'sequence': first_sequence + index,
                    'entry': self._entry_document(entry),
                }
                for index, entry in enumerate(entries)
            ],
            ordered=True,
        )

    @staticmethod
    def _entry_document(entry: AuditLogEntry) -> dict[str, Any]:
        # JSON-safe primitives for Mongo, EXCEPT ``timestamp``: persist it as a BSON Date (not an ISO
        # string) so the audit log is queryable by time in Mongo (range filters, aggregation). It is
        # the only datetime field on the entry.
        document = entry.model_dump(mode='json')
        document['timestamp'] = entry.timestamp
        return document

    @staticmethod
    def _restore_timestamp(entry: dict[str, Any]) -> dict[str, Any]:
        # Mongo returns BSON dates tz-naive (UTC); restore the timezone so a replayed entry round-trips
        # to the same tz-aware datetime it was appended with.
        timestamp = entry.get('timestamp')
        if isinstance(timestamp, datetime) and timestamp.tzinfo is None:
            return {**entry, 'timestamp': timestamp.replace(tzinfo=timezone.utc)}
        return entry

    async def replay(self, session_id: SessionId) -> tuple[AuditLogEntry, ...]:
        """Every entry this session appended, in the order the sequence numbers were allocated.

        Paged rather than drained in one cursor. A trail is not bounded by anything the flow declares:
        it grows with what the session actually did, and the chatty shapes are the ones that most need
        replaying. Paging keeps the peak at one page rather than the whole trail; the answer is
        necessarily the whole of it, since replaying a subset would reconstruct a different session.

        The page walks `sequence` forward, which the `(session_id, sequence)` index serves directly —
        an equality on the session then a range on the sequence — so no page costs a sort.
        """
        entries: list[AuditLogEntry] = []
        after_sequence = -1
        while True:
            page = [
                row
                async for row in self._committed.find({'session_id': session_id, 'sequence': {'$gt': after_sequence}})
                .sort('sequence', 1)
                .limit(_PAGE_SIZE)
            ]
            entries.extend(AuditLogEntry.model_validate(self._restore_timestamp(row['entry'])) for row in page)
            if len(page) < _PAGE_SIZE:
                return tuple(entries)
            after_sequence = int(page[-1]['sequence'])
