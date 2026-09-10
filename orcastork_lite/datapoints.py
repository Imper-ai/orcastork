"""The ``DataPoint`` model — the unit of data — and its read-only query view.

A ``DataPoint`` is a frozen pydantic model whose **identity is ``(class, value)``**, timestamps
excluded, so re-observing a value merges (bumping ``last_retrieved``) instead of duplicating.
There is no registry and no discriminator: nothing is ever serialized, so the class itself is
the type. The identity is computed once, when the DataPoint is built, so a value that cannot be
made hashable is rejected there (:class:`~orcastork_lite.exceptions.UnhashableValueError`). Any
subclass — however abstract — may appear in an operator's ``depends_on``; a present leaf satisfies
a base-type dependency through plain ``isinstance``.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, PrivateAttr

from .exceptions import UnhashableValueError
from .ids import OperatorId


def _make_hashable(obj: Any) -> Any:
    """Recursively coerce a value into a hashable, order-insensitive form (lists/dicts/sets → tuples).

    A pydantic model is reduced through its dump so two models with equal fields share an identity.
    Anything else must already be hashable: a value that is not raises :class:`UnhashableValueError`
    instead of surfacing later as an opaque ``TypeError`` from inside the session state.
    """
    if isinstance(obj, (list, tuple)):
        return tuple(_make_hashable(item) for item in obj)
    if isinstance(obj, dict):
        return tuple(sorted(((key, _make_hashable(value)) for key, value in obj.items()), key=repr))
    if isinstance(obj, (set, frozenset)):
        return tuple(sorted((_make_hashable(item) for item in obj), key=repr))
    if isinstance(obj, BaseModel):
        return _make_hashable(obj.model_dump())
    try:
        hash(obj)
    except TypeError as error:
        raise UnhashableValueError(f'{type(obj).__name__} value cannot be part of a DataPoint identity') from error
    return obj


def identity_key(data_point: DataPoint[Any]) -> tuple[type[DataPoint[Any]], Any]:
    """The keyed-merge identity of a DataPoint — ``(class, normalized-value)``, timestamps excluded."""
    return data_point.identity


class DataPoint[ValueT](BaseModel):
    model_config = ConfigDict(frozen=True)

    value: ValueT
    retrieved_by: OperatorId
    first_retrieved: datetime
    last_retrieved: datetime

    # Computed once at construction: the model is frozen, so the identity can never change, and every
    # keyed-merge, hash and equality check reads it back instead of re-normalizing the value. Computing
    # it eagerly also means an unhashable value fails where the DataPoint is built — inside the emitting
    # operator's fault boundary — rather than deep in the session state.
    _identity: tuple[type[DataPoint[Any]], Any] = PrivateAttr()

    def model_post_init(self, _context: Any, /) -> None:
        self._identity = (type(self), _make_hashable(self.value))

    @property
    def identity(self) -> tuple[type[DataPoint[Any]], Any]:
        """The keyed-merge identity — ``(class, normalized-value)``, timestamps excluded."""
        return self._identity

    def __hash__(self) -> int:
        return hash(self._identity)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DataPoint):
            return NotImplemented
        return self._identity == other._identity

    def reobserved(self, at: datetime) -> Self:
        """A copy with ``last_retrieved`` advanced to ``at`` (``first_retrieved`` kept)."""
        return self.model_copy(update={'last_retrieved': max(self.last_retrieved, at)})

    @classmethod
    def emit(cls, value: ValueT) -> DataPointEmission:
        """Emit this DataPoint type carrying ``value`` — an operator's sole responsibility.

        The orchestrator stamps provenance (``retrieved_by``) and the observation time when it
        writes the result to the session, so operators never fabricate bookkeeping fields.
        """
        return DataPointEmission(cls, value)


@dataclass(frozen=True)
class DataPointEmission:
    """A value-only DataPoint emitted by an operator (see :meth:`DataPoint.emit`)."""

    leaf_type: type[DataPoint[Any]]
    value: Any

    def finalize(self, *, retrieved_by: OperatorId, at: datetime) -> DataPoint[Any]:
        """Build the full DataPoint, stamping provenance and observation time."""
        return self.leaf_type(value=self.value, retrieved_by=retrieved_by, first_retrieved=at, last_retrieved=at)


class DataPointView:
    """The read-only, subtype-aware query facade handed to operators and capabilities."""

    def __init__(self, items: Iterable[DataPoint[Any]] = ()) -> None:
        self._items: tuple[DataPoint[Any], ...] = tuple(items)

    def all(self) -> tuple[DataPoint[Any], ...]:
        return self._items

    def of_type[T: DataPoint[Any]](self, data_point_type: type[T]) -> tuple[T, ...]:
        """All DataPoints that are instances of ``data_point_type`` (subtype-aware)."""
        return tuple(item for item in self._items if isinstance(item, data_point_type))

    def latest[T: DataPoint[Any]](self, data_point_type: type[T]) -> T | None:
        """The newest DataPoint of ``data_point_type`` by ``last_retrieved``, or ``None``."""
        matches = self.of_type(data_point_type)
        if not matches:
            return None
        return max(matches, key=lambda item: item.last_retrieved)

    def present_types(self) -> frozenset[type[DataPoint[Any]]]:
        """The concrete classes present — what readiness and availability are computed from."""
        return frozenset(type(item) for item in self._items)

    def __iter__(self) -> Iterator[DataPoint[Any]]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)
