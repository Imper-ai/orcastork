"""The ``DataPoint`` model — the framework's unit of data.

A ``DataPoint`` is a frozen pydantic model whose **identity is ``(type, value)``**,
excluding timestamps, so re-observing a value dedups (keyed-merge). A leaf may narrow the
identity to named fields of its value (``DataPointTypeConfig.identity_fields``); the first value
for such an identity is kept. The identity is computed once, at construction, so a value that
cannot be made hashable is refused there (:class:`UnhashableValueError`). Concrete leaves pin
a ``type`` ``Literal`` discriminator and carry a class-level :class:`DataPointTypeConfig`
(``pii`` / ``ephemeral``); abstract intermediates group leaves for subtype substitution
but are not union members.

Registration happens in :meth:`BaseDataPoint.__pydantic_init_subclass__` (pydantic's
post-build hook — ``model_fields`` is populated there, unlike plain ``__init_subclass__``):
concrete leaves self-register by their discriminator (raising on a duplicate), abstract
intermediates are tracked for substitution. A monotonically increasing version counter
lets :mod:`.registry` rebuild the discriminated-union ``TypeAdapter`` lazily.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar, Self

from pydantic import BaseModel, ConfigDict, PrivateAttr
from pydantic_core import PydanticUndefined

from ..exceptions import DuplicateRegistrationError, InvalidDataPointError, UnhashableValueError
from ..ids import DataPointType, OperatorRef

# Registry of concrete leaves keyed by discriminator value, the set of abstract
# intermediates (for substitution), and a version bumped on every change so the lazy
# union adapter (see :mod:`.registry`) knows when to rebuild.
_REGISTRY: dict[DataPointType, type['BaseDataPoint[Any]']] = {}
_ABSTRACT_TYPES: set[type['BaseDataPoint[Any]']] = set()
_registry_version: int = 0


@dataclass(frozen=True)
class DataPointTypeConfig:
    """Class-level classification of a DataPoint type.

    ``pii`` drives encryption/redaction/retention; ``ephemeral`` means "never written to
    the durable store" (a transient DataPoint emitted only to trigger another operator).

    ``audit_every_emission`` is the escape hatch for high-volume types. A per-frame debugger probe or a
    page-view stream emits hundreds of DataPoints in one session, and a row each dominates the trail —
    two such types were ~78% of it — while every row is also a durable buffer write, so the cost lands
    on the database twice and the completion flush inherits all of it. Opting a type out replaces its
    per-emission rows with one counted summary, so the trail still says how many were merged.

    ``identity_fields`` names the fields of the value that make a DataPoint's identity, for a value
    that is a mapping or a pydantic model. Two values that agree on those fields are one DataPoint:
    the first value merged is kept, and a later value that differs elsewhere is dropped (and
    logged) rather than stored beside it. Empty, the default, keeps the whole value as identity.
    """

    pii: bool
    ephemeral: bool
    audit_every_emission: bool = True
    identity_fields: tuple[str, ...] = ()


def _sorted(items: Iterable[Any]) -> list[Any]:
    # Natural order first, so a homogeneous value keeps the canonical form (and the Redis field) it had;
    # repr is the fallback for a mix of types that cannot be compared with each other.
    materialized = list(items)
    try:
        return sorted(materialized)
    except TypeError:
        return sorted(materialized, key=repr)


def _make_hashable(obj: Any) -> Any:
    """Recursively coerce a value into a hashable, order-insensitive form (lists/dicts/sets → tuples).

    A pydantic model is reduced through its dump, so two models with equal fields share an identity.
    Anything else must already be hashable: a value that is not raises :class:`UnhashableValueError`
    where the DataPoint is built, instead of a ``TypeError`` from deep inside the session state.
    """
    if isinstance(obj, (list, tuple)):
        return tuple(_make_hashable(item) for item in obj)
    if isinstance(obj, dict):
        return tuple(_sorted((key, _make_hashable(value)) for key, value in obj.items()))
    if isinstance(obj, (set, frozenset)):
        return tuple(_sorted(_make_hashable(item) for item in obj))
    if isinstance(obj, BaseModel):
        return _make_hashable(obj.model_dump())
    try:
        hash(obj)
    except TypeError as error:
        raise UnhashableValueError(f'{type(obj).__name__} value cannot be part of a DataPoint identity') from error
    return obj


def _identity_value(value: Any, fields: tuple[str, ...]) -> Any:
    """The part of ``value`` that makes its identity: the declared fields, else all of it."""
    if not fields:
        return _make_hashable(value)
    if isinstance(value, BaseModel):
        return _make_hashable({name: getattr(value, name) for name in fields})
    if isinstance(value, dict):
        return _make_hashable({name: value[name] for name in fields})
    raise InvalidDataPointError(
        f'identity_fields {fields} need a mapping or pydantic model value, not {type(value).__name__}'
    )


def canonical_value(value: Any) -> str:
    """A process-stable canonical string for a JSON-native DataPoint value.

    Mirrors the keyed-merge identity normalization (``_make_hashable`` — dict/set order
    insensitive), so a durable archive key built from it is stable across processes (the
    builtin ``hash`` is per-process salted). The archive adapter feeds this to
    :meth:`ValueCipher.mac` (PII — a keyed digest) or a plain SHA-256 (non-PII) to derive
    the key; the canonical form itself is never persisted for PII.
    """
    return repr(_make_hashable(value))


def identity_key(data_point: 'BaseDataPoint[Any]') -> tuple[DataPointType, Any]:
    """The keyed-merge identity of a DataPoint — ``(type, normalized-value)``, timestamps excluded.

    The single source of truth for "the same DataPoint": the in-memory store and the keyed-merge
    set key on this tuple, and the Redis store keys its hash fields on :func:`identity_string`. Both
    build on the one ``_make_hashable`` normalization, so dedup decisions never diverge across adapters.
    """
    return data_point.identity


def identity_string(data_point: 'BaseDataPoint[Any]') -> str:
    """The process-stable string form of :func:`identity_key`, for string-keyed stores.

    For a type with no ``identity_fields`` it is :func:`canonical_value` of the whole value, the
    form 0.1.1 stored, so existing sessions keep their keys.
    """
    return repr(data_point.identity[1])


def registry_version() -> int:
    """Current registry version — bumped on every (de)registration."""
    return _registry_version


def registered_leaves() -> tuple[type['BaseDataPoint[Any]'], ...]:
    """All registered concrete leaf classes (the discriminated-union members)."""
    return tuple(_REGISTRY.values())


def subtypes_of(data_point_type: type['BaseDataPoint[Any]']) -> tuple[type['BaseDataPoint[Any]'], ...]:
    """Registered concrete leaves that satisfy ``data_point_type`` (subtype substitution).

    A dependency on an abstract intermediate (e.g. ``EmailDataPoint``) is satisfied by any
    concrete leaf below it; a dependency on a concrete leaf is satisfied by itself.
    """
    return tuple(leaf for leaf in _REGISTRY.values() if issubclass(leaf, data_point_type))


class BaseDataPoint[ValueT](BaseModel):
    # Tolerant reader: unknown/extra fields are ignored on read (rolling deploy / resume).
    model_config = ConfigDict(frozen=True, extra='ignore')

    type: DataPointType  # discriminator — every concrete leaf pins a ``Literal``
    value: ValueT  # value type fixed by each leaf via the generic parameter
    retrieved_by: OperatorRef
    first_retrieved: datetime
    last_retrieved: datetime

    config: ClassVar[DataPointTypeConfig]  # set on an intermediate (inherited) or per leaf
    __abstract__: ClassVar[bool] = True  # the base itself is abstract; leaves are concrete

    # Computed once at construction: the model is frozen, so neither can change, and every keyed-merge,
    # hash and equality check reads them back instead of re-normalizing the value. ``model_copy`` (a
    # re-observation) carries them over.
    _identity: tuple[DataPointType, Any] = PrivateAttr()
    _value_key: Any = PrivateAttr()

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        global _registry_version

        # Skip pydantic's internal generic parametrizations (e.g. the ``BaseDataPoint[str]``
        # submodel created when a leaf subclasses ``BaseDataPoint[str]``) — only real
        # user-defined subclasses carry an ``origin`` of ``None``.
        if cls.__pydantic_generic_metadata__.get('origin') is not None:
            return

        # A class is abstract only if it sets ``__abstract__`` in its OWN body — the flag
        # never leaks to leaves via inheritance.
        if cls.__dict__.get('__abstract__', False):
            _ABSTRACT_TYPES.add(cls)
            _registry_version += 1
            return

        type_default = cls.model_fields['type'].default
        if type_default is PydanticUndefined or type_default is None:
            raise InvalidDataPointError(f'{cls.__name__} is a concrete DataPoint but does not pin a `type` Literal')
        if not isinstance(getattr(cls, 'config', None), DataPointTypeConfig):
            raise InvalidDataPointError(f'{cls.__name__} must declare a class-level `config: DataPointTypeConfig`')

        discriminator: DataPointType = type_default
        existing = _REGISTRY.get(discriminator)
        if existing is not None and existing is not cls:
            raise DuplicateRegistrationError(
                f'DataPoint type {discriminator!r} is already registered to {existing.__name__}'
            )
        _REGISTRY[discriminator] = cls
        _registry_version += 1

    def model_post_init(self, _context: Any, /) -> None:
        self._value_key = _make_hashable(self.value)
        fields = self.config.identity_fields
        self._identity = (self.type, self._value_key if not fields else _identity_value(self.value, fields))

    @property
    def identity(self) -> tuple[DataPointType, Any]:
        """The keyed-merge identity — ``(type, normalized identity value)``, timestamps excluded."""
        return self._identity

    def same_value(self, other: 'BaseDataPoint[Any]') -> bool:
        """Whether ``other`` carries this exact value — the test that tells a refresh from a conflicting value."""
        return self._value_key == other._value_key

    def __hash__(self) -> int:
        return hash(self._identity)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, BaseDataPoint):
            return NotImplemented
        return self._identity == other._identity

    @property
    def is_pii(self) -> bool:
        return self.config.pii

    @property
    def is_ephemeral(self) -> bool:
        return self.config.ephemeral

    @property
    def audits_every_emission(self) -> bool:
        return self.config.audit_every_emission

    def audit_summary(self) -> str | None:
        """A non-PII summary for the audit trail, used in place of the value.

        Defaults to ``None`` — the orchestrator then redacts a PII value and stringifies a
        non-PII one. A PII DataPoint may override this to surface a non-sensitive identifier
        (e.g. a collector name) so the audit can attribute the entry — and time it — without
        ever leaking the redacted payload.
        """
        return None

    def reobserved(self, at: datetime) -> Self:
        """A copy with ``last_retrieved`` advanced to ``at`` (``first_retrieved`` kept)."""
        bumped = max(self.last_retrieved, at)
        return self.model_copy(update={'last_retrieved': bumped})

    @classmethod
    def emit(cls, value: ValueT) -> DataPointEmission:
        """Emit this DataPoint type carrying ``value`` — an operator's sole responsibility.

        Returns a value-only :class:`DataPointEmission`; the orchestrator stamps provenance
        (``retrieved_by``) and the observation time (``first_retrieved``/``last_retrieved``)
        when it writes the result to the store. Operators never fabricate provenance.
        """
        if cls not in _REGISTRY.values():
            raise InvalidDataPointError(f'{cls.__name__} is not a concrete DataPoint leaf and cannot emit')
        return DataPointEmission(cls, value)


@dataclass(frozen=True)
class DataPointEmission:
    """A value-only DataPoint emitted by an operator (see :meth:`BaseDataPoint.emit`).

    Operators own only *what* they observed — the concrete leaf type and its value. The
    orchestrator owns provenance: it stamps ``retrieved_by`` (the emitting operator) and the
    observation time via :meth:`finalize` before the DataPoint is written to the store, so
    an operator never sees or fabricates the bookkeeping fields.
    """

    leaf_type: type[BaseDataPoint[Any]]
    value: Any

    def finalize(self, *, retrieved_by: OperatorRef, at: datetime) -> BaseDataPoint[Any]:
        """Build the full DataPoint, stamping provenance (``retrieved_by``) and observation time."""
        # ``leaf_type`` is always a concrete leaf (which pins ``type`` with a default); mypy only
        # sees the abstract base, whose ``type`` has no default — hence the call-arg ignore.
        return self.leaf_type(  # type: ignore[call-arg]
            value=self.value, retrieved_by=retrieved_by, first_retrieved=at, last_retrieved=at
        )
