"""The ``AuditSink`` port — a durable, append-only event log.

Entries are appended per-event (epoch-guarded — a fenced writer's entries are rejected),
preserving order and per-event granularity (N events → N documents). An append is durable
and replayable by the time it returns, so a session that dies mid-run leaves everything it
appended readable with no recovery step to run first. An adapter may collapse a batch into
one round-trip, but never defers durability past the append. The audit is independent of
the live store.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from ..audit import AuditLogEntry
from ..ids import SessionId


class AuditSink(Protocol):
    async def append(self, entry: AuditLogEntry) -> None:
        """Append one event durably.

        The entry carries the writer's ``epoch``; an entry whose epoch is below the
        session's highest accepted epoch is rejected (no authoritative audit for a fenced
        writer) with :class:`~orcastork.exceptions.StaleEpochError`.
        """
        ...

    async def append_many(self, entries: Sequence[AuditLogEntry]) -> None:
        """Append a batch of events durably, preserving order.

        Observably identical to appending each entry in sequence — same ordering, same
        per-event granularity on replay — but an adapter may allocate the whole batch in
        one round-trip. The batch is one writer's events for one session, so the epoch
        predicate is a single append's: a stale epoch rejects the entire batch atomically
        with :class:`~orcastork.exceptions.StaleEpochError` and writes
        nothing.
        """
        ...

    async def replay(self, session_id: SessionId) -> tuple[AuditLogEntry, ...]:
        """Every entry appended for the session, in append order."""
        ...
