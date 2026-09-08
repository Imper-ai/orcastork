"""``Operator`` — the single base unifying the old collector / enricher / detector.

An operator declares its data dependencies (``depends_on`` in, ``produces`` out), its
capability needs (``requires``), and an explicit scheduling ``policy`` (no default — the
author must choose ``rerun_on_new_data``). It implements one coroutine, ``run``, an async
generator that emits DataPoints by yielding them. Concrete operators self-register under
``operator_id``; a duplicate id raises (unlike a silently-overwriting factory dict).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Any, ClassVar

from ..aggregation.retry import RetryPolicy
from ..capabilities.base import Capability
from ..datapoints import BaseDataPoint, DataPointEmission
from ..exceptions import DuplicateRegistrationError, InvalidOperatorError
from ..ids import OperatorId
from .context import OperatorContext


class RerunOn(Enum):
    """Which delta kinds count as rerun-worthy new data.

    ``ADDED_OR_UPDATED`` also reruns on freshness-only re-observations of an existing
    ``(type, value)`` identity (``delta.updated``); ``ADDED_ONLY`` ignores those, so chatty
    re-observation cannot keep re-triggering a pure value-computation operator. A newly-available
    capability always warrants a rerun, under either mode.
    """

    ADDED_OR_UPDATED = 'added_or_updated'
    ADDED_ONLY = 'added_only'


@dataclass(frozen=True)
class OperatorPolicy:
    rerun_on_new_data: bool  # NO DEFAULT — the author must decide whether new data re-triggers this operator
    # Consulted only when rerun_on_new_data=True; inert otherwise (deliberately not a validation error).
    rerun_on: RerunOn = RerunOn.ADDED_OR_UPDATED
    debounce: timedelta | None = None  # per-operator override of the scheduler's global default
    max_cycles: int | None = None  # circuit-breaker iteration cap; required to sit on a graph cycle
    timeout: timedelta | None = None  # per-operator override of the orchestrator's global operation timeout
    # Bounded relaunch-on-failure: the gather loop relaunches a failed run on a jittered backoff
    # window; None abandons the operator after a single failed run.
    retry: RetryPolicy | None = None


class Operator(ABC):
    operator_id: ClassVar[OperatorId]  # registry key AND provenance identity
    policy: ClassVar[OperatorPolicy]  # explicit; no default
    # Readiness gate: EVERY ``depends_on`` type must be present (subtype-aware) before the operator can
    # run at all, and new data of these types also re-triggers a ``rerun_on_new_data`` operator.
    depends_on: ClassVar[frozenset[type[BaseDataPoint[Any]]]] = frozenset()
    # Data the operator READS but does not require to run: new data of a ``uses`` type re-triggers a
    # ``rerun_on_new_data`` operator (subtype-aware), exactly like ``depends_on`` — but its ABSENCE never
    # blocks readiness. Declare here every consumed type that is optional / may arrive late (e.g. an
    # aggregator folds many optional attributes it must re-fold on arrival, yet must run without them).
    uses: ClassVar[frozenset[type[BaseDataPoint[Any]]]] = frozenset()
    produces: ClassVar[frozenset[type[BaseDataPoint[Any]]]] = frozenset()  # outputs (graph edges)
    requires: ClassVar[frozenset[type[Capability]]] = frozenset()  # capability deps (subtype-aware)

    _registry: ClassVar[dict[OperatorId, type['Operator']]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Skip abstract bases/intermediates (no id) and not-yet-concrete classes (e.g. `run`
        # not implemented, or the Aggregator base whose `aggregate` is abstract).
        if not hasattr(cls, 'operator_id') or getattr(cls, '__abstractmethods__', None):
            return
        if not hasattr(cls, 'policy'):
            raise InvalidOperatorError(f'{cls.__name__} must declare a scheduling `policy`')
        existing = Operator._registry.get(cls.operator_id)
        if existing is not None and existing is not cls:
            raise DuplicateRegistrationError(
                f'operator_id {cls.operator_id!r} is already registered to {existing.__name__}'
            )
        Operator._registry[cls.operator_id] = cls

    @classmethod
    def registered(cls) -> dict[OperatorId, type['Operator']]:
        return dict(Operator._registry)

    @abstractmethod
    def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:
        """Emit DataPoints value-only by yielding ``SomeDataPoint.emit(value)``.

        Implement as an ``async def`` generator. An operator declares *what* it observed (the
        leaf type + value); the orchestrator stamps provenance and observation time on write.
        """
        ...
