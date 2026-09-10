"""``Operator`` — the unit of work — plus its scheduling policy and per-invocation context.

An operator declares its data dependencies (``depends_on`` in, ``produces`` out, ``uses`` as
read-only rerun triggers, ``consumes`` as the sinks it exists to feed), its capability needs
(``requires``), and an explicit scheduling ``policy`` (no default — the author must choose
``rerun_on_new_data``). It implements one async generator, ``run``, that emits DataPoints by
yielding them. There is no registry: the classes are handed to the orchestrator directly.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Any, ClassVar

from .capabilities import Capability, CapabilityView
from .datapoints import DataPoint, DataPointEmission, DataPointView
from .exceptions import InvalidOperatorError
from .ids import CapabilityId, NamespaceId, OperatorId, SessionId


class RerunOn(Enum):
    """Which delta kinds count as rerun-worthy new data.

    ``ADDED_OR_UPDATED`` also reruns on freshness-only re-observations of an existing identity
    (``delta.updated``); ``ADDED_ONLY`` ignores those, so chatty re-observation cannot keep
    re-triggering a pure value-computation operator. A newly-available capability always
    warrants a rerun, under either mode.
    """

    ADDED_OR_UPDATED = 'added_or_updated'
    ADDED_ONLY = 'added_only'


@dataclass(frozen=True)
class RetryPolicy:
    """Bounded relaunch-on-failure with a jittered exponential backoff (``base_delay * 2**attempt``)."""

    max_attempts: int = 5
    base_delay: float = 0.05  # seconds
    jitter: float = 0.2  # ±20% multiplicative jitter

    def __post_init__(self) -> None:
        # Fail fast on a misconfigured policy rather than abandoning the operator on its first failure
        # (max_attempts < 1) or producing a negative/degenerate backoff schedule.
        if self.max_attempts < 1:
            raise ValueError(f'max_attempts must be >= 1, got {self.max_attempts}')
        if self.base_delay < 0.0:
            raise ValueError(f'base_delay must be >= 0.0, got {self.base_delay}')
        if not 0.0 <= self.jitter <= 1.0:
            raise ValueError(f'jitter must be within [0.0, 1.0], got {self.jitter}')


@dataclass(frozen=True)
class OperatorPolicy:
    rerun_on_new_data: bool  # NO DEFAULT — the author must decide whether new data re-triggers this operator
    # Consulted only when rerun_on_new_data=True; inert otherwise (deliberately not a validation error).
    rerun_on: RerunOn = RerunOn.ADDED_OR_UPDATED
    debounce: timedelta | None = None  # coalescing window for reruns; None → the scheduler default (zero)
    max_cycles: int | None = None  # circuit-breaker run cap; required to sit on a graph cycle
    timeout: timedelta | None = None  # per-operator override of the orchestrator's operation timeout
    # Bounded relaunch-on-failure: the loop relaunches a failed run on a jittered backoff window;
    # None abandons the operator after a single failed run.
    retry: RetryPolicy | None = None


class Operator(ABC):
    operator_id: ClassVar[OperatorId]
    policy: ClassVar[OperatorPolicy]  # explicit; no default
    # Readiness gate: EVERY ``depends_on`` type must be present (subtype-aware) before the operator can
    # run at all; new data of these types also re-triggers a ``rerun_on_new_data`` operator.
    depends_on: ClassVar[frozenset[type[DataPoint[Any]]]] = frozenset()
    # Data the operator READS but does not require: new data of a ``uses`` type re-triggers a
    # ``rerun_on_new_data`` operator exactly like ``depends_on``, but its ABSENCE never blocks readiness.
    uses: ClassVar[frozenset[type[DataPoint[Any]]]] = frozenset()
    produces: ClassVar[frozenset[type[DataPoint[Any]]]] = frozenset()  # outputs (graph edges)
    requires: ClassVar[frozenset[type[Capability]]] = frozenset()  # capability deps (subtype-aware)
    # The DataPoint types this operator is the sink for. When any operator in a session declares
    # ``consumes``, the orchestrator runs ONLY the operators whose output is transitively needed to
    # produce those sinks and prunes the rest. Empty everywhere (the default) opts out of pruning.
    consumes: ClassVar[frozenset[type[DataPoint[Any]]]] = frozenset()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Abstract intermediates (no id) and not-yet-concrete classes (`run` not implemented) are exempt.
        if not hasattr(cls, 'operator_id') or getattr(cls, '__abstractmethods__', None):
            return
        if not hasattr(cls, 'policy'):
            raise InvalidOperatorError(f'{cls.__name__} must declare a scheduling `policy`')

    @abstractmethod
    def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:
        """Emit DataPoints value-only by yielding ``SomeDataPoint.emit(value)``.

        Implement as an ``async def`` generator. The orchestrator stamps provenance and
        observation time on each emission as it is written.
        """
        ...


@dataclass(frozen=True)
class InvocationDelta:
    """What changed since *this* operator last ran — so a rerun does incremental work."""

    added: frozenset[DataPoint[Any]]  # new identities
    updated: frozenset[DataPoint[Any]]  # existing identities re-observed (last_retrieved bumped)
    newly_available_caps: frozenset[CapabilityId]  # capabilities that came online since the last run
    is_first_invocation: bool  # first run → `added` is the full current set


@dataclass(frozen=True)
class OperatorContext:
    session_id: SessionId
    namespace_id: NamespaceId
    store: DataPointView
    capabilities: CapabilityView
    delta: InvocationDelta

    def latest[T: DataPoint[Any]](self, data_point_type: type[T]) -> T | None:
        """The newest DataPoint of ``data_point_type`` by ``last_retrieved`` (ergonomic single-read)."""
        return self.store.latest(data_point_type)
