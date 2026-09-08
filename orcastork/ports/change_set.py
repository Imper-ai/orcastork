"""Value types shared across ports."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, TypeAlias

from ..datapoints import BaseDataPoint


@dataclass(frozen=True)
class ChangeSet:
    """The DataPoints that changed since a given store revision.

    ``added`` are new ``(type, value)`` identities; ``updated`` are existing identities
    re-observed since the revision (``last_retrieved`` advanced).
    """

    added: tuple[BaseDataPoint[Any], ...]
    updated: tuple[BaseDataPoint[Any], ...]


@dataclass(frozen=True)
class InboxEntry:
    """A claimed inbox message: a DataPoint plus its delivery bookkeeping."""

    entry_id: str
    data_point: BaseDataPoint[Any]
    delivery_count: int


@dataclass(frozen=True)
class PoisonInboxEntry:
    """An inbox entry whose wire payload is malformed (bytes that do not decode).

    Adapters deliver this instead of raising for wire-format decode failures ONLY — semantic
    deserialization failures (an unknown DataPoint type, a validation error) propagate so a
    parser/registry regression surfaces loudly. Redelivery can never fix malformed bytes, so
    the orchestrator quarantines these on sight.
    """

    entry_id: str
    error: str
    delivery_count: int
    raw_payload: str | None = None  # the undecodable wire payload, when cheaply available


# What ``consume``/``reclaim`` deliver: a parsed entry, or the poison representation of one
# that could not be parsed — adapters never raise per entry, so one bad message cannot block
# the rest of its batch.
DeliveredInboxEntry: TypeAlias = InboxEntry | PoisonInboxEntry


@dataclass(frozen=True)
class QuarantinedEntry:
    """A durably-recorded quarantined inbox entry (operator inspection / manual re-drive)."""

    entry_id: str
    reason: str
    delivery_count: int
    raw_payload: str | None = None  # the original wire payload, when cheaply available


@dataclass(frozen=True)
class VersionedDocument:
    """A durable document plus its optimistic-concurrency version and live-status metadata."""

    document: dict[str, Any]
    version: int
    status: str | None = None  # 'in_progress' during interim aggregation, 'final' once finalized
    updated_at: datetime | None = None  # when the framework last stamped the record
