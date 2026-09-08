"""The ``DurableStore`` port — idempotent, OCC-guarded durable outputs + contribution markers.

Aggregators are the sole writers of durable *outputs*: shared aggregates use optimistic
concurrency — read a :class:`VersionedDocument`, compute the next document, then ``upsert``
with the expected version (a mismatch raises :class:`OptimisticConcurrencyError` → caller
retries). The **destination is decided by the written model's** ``table`` (its
``__table_name__``) — the adapter routes each write to that collection rather than a single
shared one. The store also holds a ``(session_id, operator_id)`` contribution marker that
makes an aggregator's effect at-most-once. Every mutating method is epoch-guarded. Session
completion lives on the :class:`~orcastork.ports.session_lock.SessionLock`,
co-located with the fencing epoch.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol

from ..ids import Epoch, OperatorId, SessionId
from .change_set import VersionedDocument


class DurableStore(Protocol):
    async def read(self, table: str, key: str) -> VersionedDocument | None:
        """Read a document + its version from ``table``, or ``None`` if absent."""
        ...

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
        """Insert/replace ``key`` in ``table`` iff its stored version equals ``expected_version``.

        ``table`` is the destination collection (the output model's ``__table_name__``). Returns
        the new version. ``status``/``updated_at`` are framework-owned record metadata stored
        alongside ``version``/``epoch`` (the business ``document`` is never mutated). Raises
        :class:`OptimisticConcurrencyError` on a version mismatch and :class:`StaleEpochError`
        on a stale epoch.
        """
        ...

    async def add_to_set(self, table: str, key: str, field_name: str, value: str, *, epoch: Epoch) -> int:
        """Idempotently add ``value`` to a set-valued field of ``key`` in ``table``; returns the size.

        Set-cardinality union (never a double-counting increment): re-adding a member leaves the
        size unchanged, a distinct member grows it. Epoch-fenced on the SAME per-``(table, key)``
        scope as :meth:`upsert` — once any epoch has mutated a key, a strictly-lower-epoch write to
        that key (any field, or its versioned document) is rejected with :class:`StaleEpochError`,
        so the two write paths can never be superseded behind each other (no split-brain).
        """
        ...

    async def mark_contribution(self, session_id: SessionId, operator_id: OperatorId, *, epoch: Epoch) -> bool:
        """Record an aggregator's contribution; ``True`` if newly marked, ``False`` if already done."""
        ...

    async def is_contribution_marked(self, session_id: SessionId, operator_id: OperatorId) -> bool:
        """Whether an aggregator has already contributed for this session (resume skips it)."""
        ...

    async def clear_contribution(self, session_id: SessionId, operator_id: OperatorId) -> None:
        """Remove an aggregator's contribution marker so a re-open can re-aggregate.

        Called by the manager before re-opening a completed session for late non-ephemeral data:
        clearing the marker lets the next orchestrator run the aggregator again, folding the new
        data into the result. The new orchestrator acquires a fresh, strictly higher epoch, so
        the re-aggregation's mark_contribution call is epoch-fenced against any stale predecessor.
        """
        ...
