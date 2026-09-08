"""In-memory ``AuditSink`` — a durable, append-only event log, epoch-fenced.

``append`` commits the entry where ``replay`` can see it (rejecting a stale-epoch writer's
entries so a fenced writer produces no authoritative audit), preserving order and per-event
granularity. A run that stops partway still has every entry it appended, with no recovery
step standing between the append and the read.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

from ...audit import AuditLogEntry
from ...exceptions import StaleEpochError
from ...ids import SessionId


@dataclass
class _AuditState:
    committed: list[AuditLogEntry] = field(default_factory=list)
    max_epoch: int = 0


class InMemoryAuditSink:
    def __init__(self) -> None:
        self._sessions: dict[SessionId, _AuditState] = {}

    def _state(self, session_id: SessionId) -> _AuditState:
        return self._sessions.setdefault(session_id, _AuditState())

    async def append(self, entry: AuditLogEntry) -> None:
        state = self._state(entry.session_id)
        if entry.epoch < state.max_epoch:
            raise StaleEpochError(f'epoch {entry.epoch} is stale (current {state.max_epoch})')
        state.max_epoch = entry.epoch
        state.committed.append(entry)

    async def append_many(self, entries: Sequence[AuditLogEntry]) -> None:
        # In memory a batch costs the same as N appends, and a (contract-homogeneous) stale
        # batch is rejected by its first entry before anything lands — atomic either way.
        for entry in entries:
            await self.append(entry)

    async def replay(self, session_id: SessionId) -> tuple[AuditLogEntry, ...]:
        return tuple(self._state(session_id).committed)
