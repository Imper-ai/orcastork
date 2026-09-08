"""In-memory ``DurableStore`` — OCC upsert, idempotent set-add, contribution markers.

Optimistic concurrency: a document carries a version; ``upsert`` succeeds only when the
caller's ``expected_version`` matches, otherwise raises ``OptimisticConcurrencyError``.
Set-add is idempotent; contribution markers make an aggregator's effect at-most-once. Each
durable output is namespaced by its ``table`` (the destination decider), and every mutation
is epoch-guarded per ``(table, key)``.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ...exceptions import OptimisticConcurrencyError, StaleEpochError
from ...ids import Epoch, OperatorId, SessionId
from ...ports.change_set import VersionedDocument


@dataclass
class _StoredDocument:
    document: dict[str, Any]
    version: int
    status: str | None = None
    updated_at: datetime | None = None


class InMemoryDurableStore:
    def __init__(self) -> None:
        self._documents: dict[tuple[str, str], _StoredDocument] = {}
        self._sets: dict[tuple[str, str, str], set[str]] = {}
        self._contributions: set[tuple[SessionId, OperatorId]] = set()
        self._max_epoch: dict[str, int] = {}

    def _guard_epoch(self, scope: str, epoch: Epoch) -> None:
        if epoch < self._max_epoch.get(scope, 0):
            raise StaleEpochError(f'epoch {epoch} is stale for {scope!r}')
        self._max_epoch[scope] = epoch

    async def read(self, table: str, key: str) -> VersionedDocument | None:
        stored = self._documents.get((table, key))
        if stored is None:
            return None
        return VersionedDocument(deepcopy(stored.document), stored.version, stored.status, stored.updated_at)

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
        self._guard_epoch(f'{table}\x00{key}', epoch)
        current = self._documents.get((table, key))
        current_version = 0 if current is None else current.version
        if current_version != expected_version:
            raise OptimisticConcurrencyError(
                f'version conflict on {table}/{key!r}: expected {expected_version}, got {current_version}'
            )
        new_version = current_version + 1
        self._documents[(table, key)] = _StoredDocument(deepcopy(document), new_version, status, updated_at)
        return new_version

    async def add_to_set(self, table: str, key: str, field_name: str, value: str, *, epoch: Epoch) -> int:
        self._guard_epoch(f'{table}\x00{key}', epoch)
        members = self._sets.setdefault((table, key, field_name), set())
        members.add(value)
        return len(members)

    async def mark_contribution(self, session_id: SessionId, operator_id: OperatorId, *, epoch: Epoch) -> bool:
        self._guard_epoch(f'contribution:{session_id}', epoch)
        marker = (session_id, operator_id)
        if marker in self._contributions:
            return False
        self._contributions.add(marker)
        return True

    async def is_contribution_marked(self, session_id: SessionId, operator_id: OperatorId) -> bool:
        return (session_id, operator_id) in self._contributions

    async def clear_contribution(self, session_id: SessionId, operator_id: OperatorId) -> None:
        self._contributions.discard((session_id, operator_id))
