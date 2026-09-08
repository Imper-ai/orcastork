"""Debounce / coalescing of reruns + the rerun-eligibility decision.

When depended-on data arrives for a ``rerun_on_new_data`` operator, the rerun is debounced
on a coalescing window — a small global default, overridable per operator
(``OperatorPolicy.debounce``). Multiple arrivals within the window collapse into a single
rerun; a window of zero reruns immediately. Time is read from the injected clock.

An armed window normally holds the gathering loop open until it comes due — that is what makes
the coalescing real rather than advisory. :func:`window_defers_to_finalize` names the one case
where it must not.
"""

from __future__ import annotations

from datetime import timedelta

from ..clock import Clock
from ..ids import OperatorId
from ..operators.aggregator import Aggregator
from ..operators.base import Operator, OperatorPolicy

DEFAULT_DEBOUNCE = timedelta(0)


def rerun_eligible(policy: OperatorPolicy, *, has_relevant_new_data: bool) -> bool:
    """A completed operator reruns only if its policy opts in AND relevant data arrived."""
    return policy.rerun_on_new_data and has_relevant_new_data


def window_defers_to_finalize(operator: type[Operator], *, completion_satisfied: bool) -> bool:
    """Whether an armed-but-not-due window may be abandoned rather than hold gathering open.

    True for exactly one shape: an ``interim_refresh`` aggregator whose session already satisfies
    its completion condition. Reaching a would-be-quiescent pass in that state means the next thing
    that happens is the aggregation phase, whose finalize pass rewrites the very document the
    refold would produce — so waiting out the window buys a durable write no reader can observe
    while charging its full width to every session's completion tail. Nothing is lost by
    abandoning it: the finalize folds the same data, authoritatively.

    Every other window still holds. An ordinary operator's rerun feeds the finalize with data it
    would otherwise never see, and a retry's window IS its backoff — collapsing it would spend the
    retry budget before the fault it is waiting out could clear. An interim window under an
    *unsatisfied* condition holds too: that session is heading for an inbox wait of unbounded
    length, where the interim write is the only live view a reader gets.
    """
    return completion_satisfied and issubclass(operator, Aggregator) and operator.interim_refresh


class DebounceController:
    """Tracks, per operator, the monotonic time at which a coalesced rerun becomes due."""

    def __init__(self, clock: Clock, *, default_window: timedelta = DEFAULT_DEBOUNCE) -> None:
        self._clock = clock
        self._default_window = default_window
        self._due_at: dict[OperatorId, float] = {}

    def schedule(self, operator_id: OperatorId, *, window: timedelta | None = None) -> None:
        """(Re)arm a rerun for ``operator_id``; arrivals within the window coalesce."""
        effective = self._default_window if window is None else window
        self._due_at[operator_id] = self._clock.monotonic() + effective.total_seconds()

    def is_scheduled(self, operator_id: OperatorId) -> bool:
        """Whether a (coalesced) rerun is already armed — so arrivals within the window don't re-arm it."""
        return operator_id in self._due_at

    def due_at(self, operator_id: OperatorId) -> float | None:
        return self._due_at.get(operator_id)

    def is_due(self, operator_id: OperatorId) -> bool:
        due = self._due_at.get(operator_id)
        return due is not None and self._clock.monotonic() >= due

    def clear(self, operator_id: OperatorId) -> None:
        """Consume a due rerun (called once the orchestrator has re-invoked the operator)."""
        self._due_at.pop(operator_id, None)
