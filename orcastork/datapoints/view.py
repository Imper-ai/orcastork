"""``DataPointView`` — the read-only, subtype-aware query facade handed to operators.

Queries are polymorphic (``isinstance``/MRO): asking for an abstract intermediate
(``EmailDataPoint``) returns every concrete leaf below it. ``latest(Type)`` returns the
newest matching DataPoint by ``last_retrieved`` while the full set stays queryable.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any

from .base import BaseDataPoint


class DataPointView:
    def __init__(self, items: Iterable[BaseDataPoint[Any]] = ()) -> None:
        self._items: tuple[BaseDataPoint[Any], ...] = tuple(items)

    def all(self) -> tuple[BaseDataPoint[Any], ...]:
        return self._items

    def of_type[T: BaseDataPoint[Any]](self, data_point_type: type[T]) -> tuple[T, ...]:
        """All DataPoints that are instances of ``data_point_type`` (subtype-aware)."""
        return tuple(item for item in self._items if isinstance(item, data_point_type))

    def latest[T: BaseDataPoint[Any]](self, data_point_type: type[T]) -> T | None:
        """The newest DataPoint of ``data_point_type`` by ``last_retrieved``, or ``None``."""
        matches = self.of_type(data_point_type)
        if not matches:
            return None
        return max(matches, key=lambda item: item.last_retrieved)

    def __iter__(self) -> Iterator[BaseDataPoint[Any]]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)
