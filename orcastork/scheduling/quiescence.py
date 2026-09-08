"""Graph-aware quiescence.

Gathering is quiescent when **no not-yet-run operator has a producible-input path
remaining** — i.e. no pending operator could ever become ready, even after every other
pending operator that could run produces its outputs. This is a producibility fixpoint:
starting from the present types, repeatedly admit pending operators whose inputs are
reachable (and whose capabilities are available), accumulating what they produce. If none
are reachable, gathering is quiescent and the aggregation phase may begin.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from ..capabilities.base import Capability
from ..datapoints import BaseDataPoint
from ..graph.builder import effective_produces
from ..operators.base import Operator


def _deps_reachable(operator: type[Operator], producible: set[type[BaseDataPoint[Any]]]) -> bool:
    return all(any(issubclass(p, required) for p in producible) for required in operator.depends_on)


def _caps_available(operator: type[Operator], available_capability_types: frozenset[type[Capability]]) -> bool:
    return all(
        any(issubclass(available, required) for available in available_capability_types)
        for required in operator.requires
    )


def reachable_pending(
    pending_operators: Iterable[type[Operator]],
    *,
    present_types: frozenset[type[BaseDataPoint[Any]]],
    available_capability_types: frozenset[type[Capability]],
) -> frozenset[type[Operator]]:
    """The pending operators that could still become ready from the current state (the fixpoint)."""
    pending = list(pending_operators)
    producible: set[type[BaseDataPoint[Any]]] = set(present_types)
    reachable: set[type[Operator]] = set()
    changed = True
    while changed:
        changed = False
        for operator in pending:
            if operator in reachable:
                continue
            if _deps_reachable(operator, producible) and _caps_available(operator, available_capability_types):
                reachable.add(operator)
                producible |= effective_produces(operator)
                changed = True
    return frozenset(reachable)


def is_quiescent(
    pending_operators: Iterable[type[Operator]],
    *,
    present_types: frozenset[type[BaseDataPoint[Any]]],
    available_capability_types: frozenset[type[Capability]],
) -> bool:
    """True iff no pending operator can ever become ready from the current state."""
    return not reachable_pending(
        pending_operators, present_types=present_types, available_capability_types=available_capability_types
    )
