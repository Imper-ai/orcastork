"""Lazy discriminated-union assembly for DataPoints.

The canonical ``DataPoint`` type is a discriminated union over the registered concrete
leaves, discriminated on ``type``. It is assembled **lazily** from the registry (no
hand-maintained list) and rebuilt only when the registry changes — so leaf import order
is irrelevant (DP-07). Deserializing an unknown discriminator raises a clear
:class:`UnknownDataPointTypeError` rather than silently producing the wrong class.
"""

from __future__ import annotations

from typing import Annotated, Any, Union

from pydantic import Field, TypeAdapter, ValidationError

from ..exceptions import UnknownDataPointTypeError
from . import base
from .base import BaseDataPoint

_cached_adapter: TypeAdapter[BaseDataPoint[Any]] | None = None
_cached_version: int = -1


def data_point_adapter() -> TypeAdapter[BaseDataPoint[Any]]:
    """The discriminated-union ``TypeAdapter`` over all registered leaves (rebuilt on change)."""
    global _cached_adapter, _cached_version
    version = base.registry_version()
    if _cached_adapter is not None and _cached_version == version:
        return _cached_adapter

    leaves = base.registered_leaves()
    if not leaves:
        raise UnknownDataPointTypeError('no DataPoint leaves are registered')

    union_type: Any = leaves[0] if len(leaves) == 1 else Annotated[Union[tuple(leaves)], Field(discriminator='type')]
    adapter: TypeAdapter[BaseDataPoint[Any]] = TypeAdapter(union_type)
    _cached_adapter, _cached_version = adapter, version
    return adapter


def parse_data_point(raw: Any) -> BaseDataPoint[Any]:
    """Deserialize a raw mapping (or revalidate a model) into its concrete leaf class."""
    try:
        return data_point_adapter().validate_python(raw)
    except ValidationError as error:
        discriminator = raw.get('type') if isinstance(raw, dict) else getattr(raw, 'type', None)
        known = {leaf.model_fields['type'].default for leaf in base.registered_leaves()}
        if discriminator in known:
            raise  # a known type with a malformed payload — preserve the real validation error
        raise UnknownDataPointTypeError(f'cannot deserialize DataPoint with type={discriminator!r}') from error


def reset_cache() -> None:
    """Drop the cached adapter — used by the test registry-isolation fixture."""
    global _cached_adapter, _cached_version
    _cached_adapter, _cached_version = None, -1
