"""The ``Inbox`` port — durable, ordered, at-least-once session ingestion.

A stateless front door appends user-action DataPoints; the orchestrator claims them,
applies them to the store, then acks. Crash-before-ack leaves the entry reclaimable, so
delivery is at-least-once (the store's keyed-merge makes redelivery safe). Append also
publishes a push wakeup that ``wait_for_entry`` blocks on, but the wakeup only bounds
latency — delivery correctness never depends on it.

Delivery tolerates malformed wire bytes: an entry whose payload cannot be decoded is
presented as a :class:`~orcastork.ports.change_set.PoisonInboxEntry` instead
of raising, so one malformed message never blocks its batch (or crash-loops a resuming
session). Semantic deserialization failures (an unknown DataPoint type, a validation error)
propagate instead — those must fail fast, and redelivery on resume lets a newer deployment
parse them. The consumer disposes of poison entries via ``quarantine``, which removes them
from delivery while keeping them durably inspectable through ``quarantined``.
"""

from __future__ import annotations

from typing import Any, Protocol

from ..datapoints import BaseDataPoint
from ..ids import Epoch, SessionId
from .change_set import DeliveredInboxEntry, QuarantinedEntry


class Inbox(Protocol):
    async def append(self, session_id: SessionId, data_point: BaseDataPoint[Any]) -> str:
        """Append a DataPoint to the session inbox (+publish wakeup); returns the entry id."""
        ...

    async def consume(
        self, session_id: SessionId, *, max_entries: int | None = None
    ) -> tuple[DeliveredInboxEntry, ...]:
        """Claim the not-yet-delivered entries, in append order (marks them in-flight).

        An entry whose wire payload cannot be decoded is delivered as a ``PoisonInboxEntry``,
        never raised — the other entries in the batch still flow.
        """
        ...

    async def reclaim(self, session_id: SessionId) -> tuple[DeliveredInboxEntry, ...]:
        """Re-present in-flight entries that were never acked (crash recovery / redelivery).

        Undecodable entries surface as ``PoisonInboxEntry`` here too (same tolerance as
        ``consume``), so a poison entry left by a crashed predecessor cannot crash the resume.
        """
        ...

    async def ack(self, session_id: SessionId, entry_id: str, *, epoch: Epoch) -> None:
        """Acknowledge an entry after its durable apply (epoch-guarded; no-op if unknown)."""
        ...

    async def quarantine(self, session_id: SessionId, entry_id: str, *, reason: str, epoch: Epoch) -> None:
        """Remove an entry from pending delivery AND record it durably for operator inspection.

        Epoch-guarded like ``ack``; an unknown / already-disposed entry is a safe no-op (so a
        repeated quarantine never double-records).
        """
        ...

    async def quarantined(self, session_id: SessionId) -> tuple[QuarantinedEntry, ...]:
        """The session's quarantined entries, in quarantine order (a read — ops and tests)."""
        ...

    async def pending_count(self, session_id: SessionId) -> int:
        """Number of entries not yet acked (claimed or unclaimed)."""
        ...

    async def wait_for_entry(self, session_id: SessionId) -> None:
        """Block until an entry may be available — a push nudge, not delivery.

        Spurious wakeups are allowed; missed wakeups are not: an append after the call
        begins MUST wake it, and entries already pending when it is called return it
        immediately (implementations subscribe before checking, closing the race).
        Returning claims nothing — callers still drain via consume/reclaim/ack.
        """
        ...
