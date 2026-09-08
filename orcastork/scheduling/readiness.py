"""Operator readiness — the data-driven trigger.

An operator is **ready** iff all its ``depends_on`` DataPoint types are present
(subtype-aware: a present leaf satisfies a base-type dependency) and all its ``requires``
capabilities are available. An operator with empty ``depends_on`` is ready at session
start. Readiness is recomputed whenever the present types or available capabilities change
(operator emissions and inbox arrivals both feed this), so the last missing dependency
triggers the operator immediately — no phase wait.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from ..capabilities.base import Capability
from ..datapoints import BaseDataPoint
from ..operators.base import Operator


@dataclass(frozen=True)
class ReadinessGap:
    """Exactly which declared inputs are keeping an operator from running.

    An operator that never ran is otherwise indistinguishable from one that ran and found nothing,
    which matters when the output is a verdict: "not checked" and "checked, clean" are very
    different claims. This carries the gate's own reason so a report can state which.
    """

    missing_data_points: tuple[type[BaseDataPoint[Any]], ...]
    missing_capabilities: tuple[type[Capability], ...]

    @property
    def is_ready(self) -> bool:
        return not self.missing_data_points and not self.missing_capabilities


def readiness_gap(
    operator: type[Operator],
    *,
    present_types: frozenset[type[BaseDataPoint[Any]]],
    available_capability_types: frozenset[type[Capability]],
) -> ReadinessGap:
    """The unsatisfied half of ``operator``'s declared inputs, subtype-aware like the gate itself.

    ``is_ready`` is defined in terms of this rather than the two conditions being written twice, so an
    explanation can never disagree with the decision it explains. Sorted by name so a stored report
    diffs cleanly between runs.
    """
    return ReadinessGap(
        missing_data_points=tuple(
            sorted(
                (
                    required
                    for required in operator.depends_on
                    if not any(issubclass(present, required) for present in present_types)
                ),
                key=lambda leaf: leaf.__name__,
            )
        ),
        missing_capabilities=tuple(
            sorted(
                (
                    required
                    for required in operator.requires
                    if not any(issubclass(available, required) for available in available_capability_types)
                ),
                key=lambda capability: capability.__name__,
            )
        ),
    )


def is_ready(
    operator: type[Operator],
    *,
    present_types: frozenset[type[BaseDataPoint[Any]]],
    available_capability_types: frozenset[type[Capability]],
) -> bool:
    """Whether ``operator`` can run given the present DataPoint types + available capabilities."""
    return readiness_gap(
        operator, present_types=present_types, available_capability_types=available_capability_types
    ).is_ready


def ready_operators(
    operators: Iterable[type[Operator]],
    *,
    present_types: frozenset[type[BaseDataPoint[Any]]],
    available_capability_types: frozenset[type[Capability]],
    already_run: frozenset[str] = frozenset(),
) -> list[type[Operator]]:
    """All not-yet-run operators that are currently ready (the orchestrator runs them together)."""
    return [
        operator
        for operator in operators
        if operator.operator_id not in already_run
        and is_ready(operator, present_types=present_types, available_capability_types=available_capability_types)
    ]
