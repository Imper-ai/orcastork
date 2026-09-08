"""``AggregationHelpers`` — the durable-write API handed to an aggregator via its context.

Bound to one ``(session_id, operator_id, epoch)``, it offers an OCC upsert (read the
current version, write guarded by it), an idempotent set-add, and a contribution marker
(so a session contributes at most once). Bounded-retry / dead-letter wrapping is layered on
in M8; the writes here are already epoch-fenced and version-guarded.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from ..clock import Clock
from ..ids import Epoch, OperatorId, SessionId
from ..ports.durable_store import DurableStore


class AggregateStatus(StrEnum):
    IN_PROGRESS = 'in_progress'
    FINAL = 'final'


class AggregationHelpers:
    def __init__(
        self,
        durable: DurableStore,
        *,
        session_id: SessionId,
        operator_id: OperatorId,
        epoch: Epoch,
        clock: Clock,
        is_final: bool,
    ) -> None:
        self._durable = durable
        self._session_id = session_id
        self._operator_id = operator_id
        self._epoch = epoch
        self._clock = clock
        self._is_final = is_final

    async def upsert(self, table: str, key: str, document: dict[str, Any]) -> int:
        """OCC upsert into ``table`` (the output model's ``__table_name__``): read the current version,
        then write guarded by it. Returns the new version."""
        existing = await self._durable.read(table, key)
        expected_version = 0 if existing is None else existing.version
        if not self._is_final and existing is not None and existing.status == AggregateStatus.FINAL.value:
            # FINAL is terminal for this output: a later interim refresh must never walk it back. A resumed
            # or reopened epoch re-runs `interim_refresh` aggregators during gathering, and its finalize
            # pass is skipped once the contribution is marked — so without this guard the record is left at
            # `in_progress` with no finalize left to restore it, and every consumer that waits for `final`
            # reads a finished session as having produced nothing. The version is returned unchanged: the
            # caller's contract is "the record's current version", and nothing was written.
            return existing.version
        status = (AggregateStatus.FINAL if self._is_final else AggregateStatus.IN_PROGRESS).value
        return await self._durable.upsert(
            table,
            key,
            document,
            expected_version=expected_version,
            epoch=self._epoch,
            status=status,
            updated_at=self._clock.now(),
        )

    async def add_to_set(self, table: str, key: str, field_name: str, value: str) -> int:
        """Idempotent set-add into ``table`` keyed by id (set-cardinality, never a double-counting increment)."""
        return await self._durable.add_to_set(table, key, field_name, value, epoch=self._epoch)

    async def mark_contribution(self) -> bool:
        """Record this session's contribution; ``False`` if it was already recorded."""
        return await self._durable.mark_contribution(self._session_id, self._operator_id, epoch=self._epoch)
