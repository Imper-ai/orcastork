"""``DataPointSet`` — a keyed collection with merge-on-add (the dedup primitive).

Identity is ``(type, value)`` (timestamps excluded), so adding an equal DataPoint
**merges** into the existing entry — keeping ``first_retrieved`` and advancing
``last_retrieved`` — rather than duplicating or dropping the new timestamp. The
in-memory store and delta machinery build on this.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from enum import Enum
from typing import Any

from .base import BaseDataPoint, identity_key


class MergeKind(Enum):
    ADDED = 'added'  # a new (type, value) identity
    UPDATED = 'updated'  # an existing identity re-observed (last_retrieved bumped)


@dataclass(frozen=True)
class MergeResult:
    kind: MergeKind
    data_point: BaseDataPoint[Any]


class DataPointSet:
    def __init__(self, items: Iterable[BaseDataPoint[Any]] = ()) -> None:
        self._items: dict[tuple[str, Any], BaseDataPoint[Any]] = {}
        for item in items:
            self.add(item)

    @staticmethod
    def _key(data_point: BaseDataPoint[Any]) -> tuple[str, Any]:
        return identity_key(data_point)

    def add(self, data_point: BaseDataPoint[Any]) -> MergeResult:
        """Insert a new identity, or merge into an existing one (bumping ``last_retrieved``)."""
        key = self._key(data_point)
        existing = self._items.get(key)
        if existing is None:
            self._items[key] = data_point
            return MergeResult(MergeKind.ADDED, data_point)
        merged = existing.reobserved(data_point.last_retrieved)
        self._items[key] = merged
        return MergeResult(MergeKind.UPDATED, merged)

    def all(self) -> tuple[BaseDataPoint[Any], ...]:
        return tuple(self._items.values())

    def __iter__(self) -> Iterator[BaseDataPoint[Any]]:
        return iter(self._items.values())

    def __len__(self) -> int:
        return len(self._items)

    def __contains__(self, data_point: object) -> bool:
        return isinstance(data_point, BaseDataPoint) and self._key(data_point) in self._items
