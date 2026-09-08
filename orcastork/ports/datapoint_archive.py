"""The ``DataPointArchive`` port — a second durable write path for raw DataPoints.

Distinct from ``DurableStore`` (curated aggregate outputs, written only by aggregators)
and ``AuditSink`` (events, not values), but built on the **same write-behind machinery**:
``archive`` appends to a durable, epoch-guarded buffer off the hot path; ``flush`` folds
the buffer into the committed store in a batch via **keyed-upsert** on
``(session, type, value_hash)`` — re-observing an identity bumps ``last_retrieved`` and
never duplicates. ``read`` folds the buffer over the committed rows under that same rule, so
what it returns does not depend on whether a flush has run yet. The archive lags the live
store (eventually consistent) and is never read on a decision path.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from ..archive import ArchivedDataPoint
from ..ids import SessionId


class DataPointArchive(Protocol):
    async def archive(self, entry: ArchivedDataPoint) -> None:
        """Append one raw DataPoint to the durable buffer.

        Epoch-guarded: an entry whose epoch is below the session's highest accepted epoch
        is rejected with :class:`~orcastork.exceptions.StaleEpochError` (a
        fenced writer never reaches the archive).
        """
        ...

    async def archive_many(self, entries: Sequence[ArchivedDataPoint]) -> None:
        """Append a batch of raw DataPoints to the durable buffer, preserving order.

        Observably identical to archiving each entry in sequence — same per-observation
        buffer granularity, same keyed-upsert fold on flush — but an adapter may allocate
        the whole batch in one round-trip. The batch is one writer's entries for one
        session: a stale epoch (or any per-entry refusal, e.g. unprotected PII) rejects the
        entire batch atomically and buffers nothing.
        """
        ...

    async def flush(self, session_id: SessionId) -> int:
        """Fold buffered entries into the committed store (keyed-upsert); return the count flushed."""
        ...

    async def read(self, session_id: SessionId) -> tuple[ArchivedDataPoint, ...]:
        """Every archived DataPoint for the session, deduped — still-buffered rows included.

        Committed rows and any not-yet-flushed buffered rows are folded together under the same
        keyed-upsert rule ``flush`` applies, so a read is buffer-transparent: it returns what a
        read after a flush would return, and a session whose entries are still buffered — in
        flight, or dead before its flush ran — is readable rather than invisible. Reading never
        flushes; the buffer is left intact.
        """
        ...

    async def buffered_count(self, session_id: SessionId) -> int:
        """Entries appended but not yet flushed."""
        ...
