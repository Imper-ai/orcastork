"""``SessionState`` — the session's in-memory DataPoint set, revisions and watermarks.

Keyed-merge on ``(class, value)``: an equal DataPoint **merges** into the existing entry
(keeping ``first_retrieved``, advancing ``last_retrieved``) rather than duplicating. Every
batch that changes something advances one monotonic revision, and each entry remembers the
revision it was added at and last freshened at; an operator's **watermark** is the revision its
last run observed, so the delta since then splits cleanly into added vs updated.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from pydantic import BaseModel, ConfigDict

from .datapoints import DataPoint, DataPointView, identity_key
from .ids import CapabilityId, OperatorId
from .operators import InvocationDelta


class _Entry(BaseModel):
    data_point: DataPoint  # bare: see InvocationDelta for why the annotation is not parametrized
    added_rev: int
    updated_rev: int


class MergeOutcome(BaseModel):
    """What one ``merge`` did: the resulting revision and which DataPoints were new vs freshened."""

    model_config = ConfigDict(frozen=True)

    revision: int
    added: tuple[DataPoint, ...]
    updated: tuple[DataPoint, ...]

    @property
    def changed(self) -> bool:
        return bool(self.added or self.updated)


class SessionState:
    def __init__(self) -> None:
        self._entries: dict[tuple[type[DataPoint[Any]], Any], _Entry] = {}
        self._revision = 0
        self._watermarks: dict[OperatorId, int] = {}

    @property
    def revision(self) -> int:
        return self._revision

    def merge(self, data_points: Iterable[DataPoint[Any]]) -> MergeOutcome:
        """Keyed-merge a batch; the revision advances iff something was added or freshened."""
        next_revision = self._revision + 1
        added: list[DataPoint[Any]] = []
        updated: list[DataPoint[Any]] = []
        for data_point in data_points:
            key = identity_key(data_point)
            existing = self._entries.get(key)
            if existing is None:
                self._entries[key] = _Entry(data_point=data_point, added_rev=next_revision, updated_rev=next_revision)
                added.append(data_point)
            elif data_point.last_retrieved > existing.data_point.last_retrieved:
                existing.data_point = existing.data_point.reobserved(data_point.last_retrieved)
                existing.updated_rev = next_revision
                updated.append(existing.data_point)
        if added or updated:
            self._revision = next_revision
        return MergeOutcome(revision=self._revision, added=tuple(added), updated=tuple(updated))

    def view(self) -> DataPointView:
        return DataPointView(entry.data_point for entry in self._entries.values())

    def has_run(self, operator_id: OperatorId) -> bool:
        return operator_id in self._watermarks

    def advance_watermark(self, operator_id: OperatorId, revision: int) -> None:
        self._watermarks[operator_id] = revision

    def delta_for(
        self,
        operator_id: OperatorId,
        *,
        previous_caps: frozenset[CapabilityId],
        available_caps: frozenset[CapabilityId],
    ) -> InvocationDelta:
        """What changed since ``operator_id`` last ran; a first run presents the whole set as ``added``."""
        newly_available = available_caps - previous_caps
        watermark = self._watermarks.get(operator_id)
        entries = self._entries.values()
        if watermark is None:
            return InvocationDelta(
                added=frozenset(entry.data_point for entry in entries),
                updated=frozenset(),
                newly_available_caps=newly_available,
                is_first_invocation=True,
            )
        return InvocationDelta(
            added=frozenset(entry.data_point for entry in entries if entry.added_rev > watermark),
            updated=frozenset(
                entry.data_point for entry in entries if entry.added_rev <= watermark < entry.updated_rev
            ),
            newly_available_caps=newly_available,
            is_first_invocation=False,
        )
