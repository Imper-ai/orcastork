"""Mongo ``DurableStore`` — OCC version-guarded upsert, idempotent set-add, markers.

A document carries ``version`` + ``epoch``; ``upsert`` succeeds only when the stored
version equals ``expected_version`` (else ``OptimisticConcurrencyError``) and the stored
epoch is not newer (else ``StaleEpochError``). Set-add uses ``$addToSet`` (idempotent);
contribution markers are insert-once. Each durable output is routed to the collection named
by its ``table`` (the output model's ``__table_name__``) — versioned docs (``_id = key``)
and set docs (``_id = key\x00field``) coexist there. The database is duck-typed so a real
``AsyncMongoClient`` database or an in-process mock both work.

Fencing epoch-guard docs live in dedicated companion collections (``<table>-fencing`` for
per-key durable guards, ``contributions-fencing`` for contribution guards) rather than the
data collections. A TTL index on ``updated_at`` (default 7 days) bounds growth: active
sessions keep refreshing the timestamp and are never reaped; dead sessions' fencing docs
expire naturally after ``fencing_ttl_seconds``. TTL >> max session lifetime (≤ ~15 min),
so a fence is only reaped long after its session is dead — the TTL never touches a live fence.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from loguru import logger
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError, OperationFailure

from ...exceptions import OptimisticConcurrencyError, StaleEpochError
from ...ids import Epoch, OperatorId, SessionId
from ...ports.change_set import VersionedDocument


class MongoDurableStore:
    def __init__(self, database: Any, *, fencing_ttl_seconds: int = 604800) -> None:
        self._database = database  # collection chosen per call by `table` (the destination decider)
        self._contributions = database['contributions']
        self._fencing_ttl_seconds = fencing_ttl_seconds
        # Names of fencing collections that already have the TTL index ensured (at most once per process).
        self._ttl_ensured: set[str] = set()

    async def read(self, table: str, key: str) -> VersionedDocument | None:
        stored = await self._database[table].find_one({'_id': key})
        if stored is None:
            return None
        raw_updated_at = stored.get('updated_at')
        # Mongo returns BSON dates tz-naive (UTC); restore the timezone so timestamps round-trip exactly.
        updated_at = (
            raw_updated_at.replace(tzinfo=timezone.utc)
            if isinstance(raw_updated_at, datetime) and raw_updated_at.tzinfo is None
            else raw_updated_at
        )
        return VersionedDocument(dict(stored['document']), int(stored['version']), stored.get('status'), updated_at)

    async def upsert(
        self,
        table: str,
        key: str,
        document: dict[str, Any],
        *,
        expected_version: int,
        epoch: Epoch,
        status: str | None = None,
        updated_at: datetime | None = None,
    ) -> int:
        collection = self._database[table]
        # Fence on the shared per-(table, key) guard, NOT the versioned doc's own epoch: upsert and
        # add_to_set both mutate this key, so they must consult and advance one fence. A per-doc check
        # would let a set-add and a versioned write to the same key be superseded behind each other.
        await self._guard_durable_epoch(table, key, epoch)
        existing = await collection.find_one({'_id': key})
        if existing is None:
            if expected_version != 0:
                raise OptimisticConcurrencyError(
                    f'expected version {expected_version} but {table}/{key!r} does not exist'
                )
            try:
                await collection.insert_one(
                    {
                        '_id': key,
                        'document': document,
                        'version': 1,
                        'epoch': int(epoch),
                        'status': status,
                        'updated_at': updated_at,
                    }
                )
            except DuplicateKeyError as error:
                raise OptimisticConcurrencyError(f'concurrent insert of {table}/{key!r}') from error
            return 1
        if int(existing['version']) != expected_version:
            raise OptimisticConcurrencyError(
                f'version conflict on {table}/{key!r}: expected {expected_version}, got {existing["version"]}'
            )
        new_version = expected_version + 1
        updated = await collection.find_one_and_update(
            {'_id': key, 'version': expected_version},
            {
                '$set': {
                    'document': document,
                    'version': new_version,
                    'epoch': int(epoch),
                    'status': status,
                    'updated_at': updated_at,
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        if updated is None:
            raise OptimisticConcurrencyError(f'version conflict on {table}/{key!r} (lost the race)')
        return new_version

    async def add_to_set(self, table: str, key: str, field_name: str, value: str, *, epoch: Epoch) -> int:
        collection = self._database[table]
        # Share the upsert fence scope: the guard is per (table, key), so a superseded predecessor
        # cannot mutate ANY set on a key a higher epoch already touched — and cannot diverge from a
        # versioned-doc write to the same key. The set doc itself only carries membership.
        await self._guard_durable_epoch(table, key, epoch)
        set_id = f'{key}\x00{field_name}'
        # One round-trip that both adds and reports, so the size is the set as of THIS write. Adding and
        # then reading back separately lets a concurrent add_to_set on the same key land in the gap, and
        # the caller is handed a count matching no single writer's view. The epoch guard above does not
        # cover this: it fences a superseded predecessor, not two writers within the same epoch.
        stored = await collection.find_one_and_update(
            {'_id': set_id},
            {'$addToSet': {'members': value}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        return len(stored['members'])

    async def _ensure_fencing_ttl(self, fencing_collection_name: str) -> None:
        # Lazily create the TTL index on updated_at the first time we write to a fencing collection.
        # The cache avoids a round-trip on every guard call. A pre-existing index with a different
        # expireAfterSeconds is an ops concern (requires a manual drop+recreate); we log and proceed
        # rather than failing writes over an index conflict.
        if fencing_collection_name in self._ttl_ensured:
            return
        collection = self._database[fencing_collection_name]
        try:
            await collection.create_index('updated_at', expireAfterSeconds=self._fencing_ttl_seconds)
        except OperationFailure as exc:
            logger.warning(
                'Could not ensure TTL index on fencing collection — index may already exist with a different TTL',
                fencing_collection=fencing_collection_name,
                error=str(exc),
            )
        self._ttl_ensured.add(fencing_collection_name)

    async def _guard_durable_epoch(self, table: str, key: str, epoch: Epoch) -> None:
        # Per-(table, key) epoch fence shared by upsert and add_to_set (mirrors the in-memory adapter,
        # which fences per (table, key)). The guard lives in a dedicated companion collection
        # (<table>-fencing) so it never pollutes the data collection. _id = key (no namespacing
        # needed in a dedicated collection). updated_at enables TTL reaping after sessions are dead.
        current = int(epoch)
        fencing_name = f'{table}-fencing'
        await self._ensure_fencing_ttl(fencing_name)
        fencing = self._database[fencing_name]
        now = datetime.now(timezone.utc)
        not_superseded = [{'epoch': {'$exists': False}}, {'epoch': {'$lte': current}}]
        try:
            result = await fencing.update_one(
                {'_id': key, '$or': not_superseded},
                {'$max': {'epoch': current}, '$set': {'updated_at': now}},
                upsert=True,
            )
        except DuplicateKeyError as error:
            raise StaleEpochError(f'epoch {epoch} is stale for durable {key!r}') from error
        if result.matched_count == 0 and result.upserted_id is None:
            raise StaleEpochError(f'epoch {epoch} is stale for durable {key!r}')

    async def mark_contribution(self, session_id: SessionId, operator_id: OperatorId, *, epoch: Epoch) -> bool:
        await self._guard_contribution_epoch(session_id, epoch)
        try:
            await self._contributions.insert_one({'_id': f'{session_id}\x00{operator_id}', 'epoch': int(epoch)})
            return True
        except DuplicateKeyError:
            return False

    async def _guard_contribution_epoch(self, session_id: SessionId, epoch: Epoch) -> None:
        # Session-scope epoch fence (mirrors the in-memory adapter): a fenced predecessor must not be able
        # to insert a contribution marker that a higher epoch would then read as already done. The guard
        # lives in contributions-fencing (_id = session_id) separate from the real markers in contributions
        # (_id = session\x00operator), so the data collection stays clean. updated_at enables TTL reaping.
        current = int(epoch)
        await self._ensure_fencing_ttl('contributions-fencing')
        contributions_fencing = self._database['contributions-fencing']
        now = datetime.now(timezone.utc)
        not_superseded = [{'epoch': {'$exists': False}}, {'epoch': {'$lte': current}}]
        try:
            result = await contributions_fencing.update_one(
                {'_id': session_id, '$or': not_superseded},
                {'$max': {'epoch': current}, '$set': {'updated_at': now}},
                upsert=True,
            )
        except DuplicateKeyError as error:
            raise StaleEpochError(f'epoch {epoch} is stale for contributions in session {session_id}') from error
        if result.matched_count == 0 and result.upserted_id is None:
            raise StaleEpochError(f'epoch {epoch} is stale for contributions in session {session_id}')

    async def is_contribution_marked(self, session_id: SessionId, operator_id: OperatorId) -> bool:
        return await self._contributions.find_one({'_id': f'{session_id}\x00{operator_id}'}) is not None

    async def clear_contribution(self, session_id: SessionId, operator_id: OperatorId) -> None:
        await self._contributions.delete_one({'_id': f'{session_id}\x00{operator_id}'})
