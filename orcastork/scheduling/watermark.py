"""Per-operator watermark → change delta.

The store carries a monotonic revision; each operator has a watermark (the revision at its
last run). The delta since that watermark is what the operator processes on a rerun. The
first run (no watermark) presents the whole current set as ``added``; a lost watermark
degrades safely to first-invocation semantics (sound because operators are idempotent).
The caller supplies the watermark — the orchestrator caches them (it is the session's sole
watermark writer while it holds the epoch), so the hot loop never re-reads them per operator.
Reads go through the narrow :class:`DataPointReader` surface, so the orchestrator can hand in
its local session mirror instead of the backend store.
"""

from __future__ import annotations

from typing import Any

from ..datapoints import BaseDataPoint
from ..ids import CapabilityId, Revision, SessionId
from ..operators.context import InvocationDelta
from ..ports.change_set import ChangeSet
from ..ports.datapoint_store import DataPointReader


def compute_delta(
    *,
    change_set: ChangeSet,
    current_set: tuple[BaseDataPoint[Any], ...],
    previous_caps: frozenset[CapabilityId],
    available_caps: frozenset[CapabilityId],
    is_first_invocation: bool,
) -> InvocationDelta:
    """Build the :class:`InvocationDelta` from a change-set (or the full set on first run)."""
    newly_available = available_caps - previous_caps
    if is_first_invocation:
        return InvocationDelta(
            added=frozenset(current_set),
            updated=frozenset(),
            newly_available_caps=newly_available,
            is_first_invocation=True,
        )
    return InvocationDelta(
        added=frozenset(change_set.added),
        updated=frozenset(change_set.updated),
        newly_available_caps=newly_available,
        is_first_invocation=False,
    )


async def operator_delta(
    store: DataPointReader,
    session_id: SessionId,
    *,
    watermark: Revision | None,
    available_caps: frozenset[CapabilityId],
    previous_caps: frozenset[CapabilityId],
) -> InvocationDelta:
    """Compute an operator's delta from its watermark (None/lost → first-invocation)."""
    if watermark is None:
        current = (await store.snapshot(session_id)).all()
        empty = ChangeSet(added=(), updated=())
        return compute_delta(
            change_set=empty,
            current_set=current,
            previous_caps=previous_caps,
            available_caps=available_caps,
            is_first_invocation=True,
        )
    change = await store.change_set_since(session_id, watermark)
    return compute_delta(
        change_set=change,
        current_set=(),
        previous_caps=previous_caps,
        available_caps=available_caps,
        is_first_invocation=False,
    )
