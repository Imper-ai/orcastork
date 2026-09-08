"""``Aggregator`` — an Operator the orchestrator runs in the aggregation phase.

It is an ordinary Operator (one component model, one scheduler, one registry) distinguished
only by being an ``Aggregator`` subclass: the orchestrator runs it once gathering quiesces,
and it is the sole writer of durable state. Aggregators emit no DataPoints (``produces`` is
typically empty), so ``run`` drives the ``aggregate`` hook and yields nothing.
"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import AsyncIterator
from typing import Any, ClassVar

from ..datapoints import BaseDataPoint, DataPointEmission
from .base import Operator
from .context import OperatorContext


class Aggregator(Operator):
    interim_refresh: ClassVar[bool] = False  # opt-in: also run during gathering, stamping status='in_progress'
    # The DataPoint types this aggregator actually folds/persists. When declared (non-empty), the
    # orchestrator runs ONLY the operators whose output is transitively needed to produce these (the
    # backward-reachable closure) and prunes the rest — so an operator whose output nothing considers
    # never runs. Empty (the default) opts out: every operator runs, exactly as before.
    consumes: ClassVar[frozenset[type[BaseDataPoint[Any]]]] = frozenset()

    @abstractmethod
    async def aggregate(self, ctx: OperatorContext) -> None:
        """Fold the gathered DataPoints into durable output (idempotently)."""
        ...

    async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:
        await self.aggregate(ctx)
        nothing: tuple[DataPointEmission, ...] = ()  # an aggregator is a sink — it emits no DataPoints
        for emitted in nothing:
            yield emitted
