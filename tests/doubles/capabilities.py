"""Capability test doubles — a factory creating fresh registered Capability subclasses.

Each stub records the credentials it was activated with (on a class-level list), so tests
can assert lazy activation: a never-available capability is never constructed (its list
stays empty), and an activated one captures its catalog-supplied credentials.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, ClassVar

from orcastork.capabilities import Capability, CapabilityContext
from orcastork.datapoints import BaseDataPoint
from orcastork.ids import CapabilityId


def make_capability(
    capability_id: str,
    *,
    depends_on: Iterable[type[BaseDataPoint[Any]]] = (),
    requires: Iterable[type[Capability]] = (),
    record_order: list[CapabilityId] | None = None,
    activate_error: Exception | None = None,
    activate_errors: Iterable[Exception] = (),
) -> type[Capability]:
    """Create (and register) a stub Capability subclass with the given declarations.

    ``record_order``, when shared across capabilities, captures the global activation
    order (for asserting base-before-layer layering). ``activate_error`` raises from
    every ``activate`` (to exercise the activation-failure isolation path);
    ``activate_errors`` is a queue raised one per attempt, then activation succeeds
    (to exercise the cool-off retry path). ``attempts`` counts every activation attempt,
    successful or not.
    """
    _id, _depends, _requires, _order, _error = (
        CapabilityId(capability_id),
        frozenset(depends_on),
        frozenset(requires),
        record_order,
        activate_error,
    )
    _error_queue = list(activate_errors)

    class _StubCapability(Capability):
        capability_id = _id
        depends_on = _depends
        requires = _requires
        activations: ClassVar[list[Mapping[str, Any]]] = []  # credentials captured per successful activation
        attempts: ClassVar[int] = 0  # every activation attempt, including the ones that raised

        async def activate(self, ctx: CapabilityContext) -> None:
            type(self).attempts += 1
            if _error is not None:
                raise _error
            if _error_queue:
                raise _error_queue.pop(0)
            type(self).activations.append(dict(ctx.credentials))
            if _order is not None:
                _order.append(_id)

    return _StubCapability
