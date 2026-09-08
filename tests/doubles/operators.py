"""Operator/Aggregator test doubles.

Because ``operator_id`` and the dependency sets are ClassVars (the registry keys off
them), a configurable stub is a *factory* that creates a fresh registered subclass per
call. The autouse registry-isolation fixture removes them after each test.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from datetime import timedelta
from typing import Any

from orcastork.aggregation import RetryPolicy
from orcastork.capabilities import Capability
from orcastork.datapoints import BaseDataPoint, DataPointEmission
from orcastork.ids import OperatorId
from orcastork.operators import Aggregator, Operator, OperatorContext, OperatorPolicy, RerunOn


def make_operator(
    operator_id: str,
    *,
    depends_on: Iterable[type[BaseDataPoint[Any]]] = (),
    uses: Iterable[type[BaseDataPoint[Any]]] = (),
    produces: Iterable[type[BaseDataPoint[Any]]] = (),
    requires: Iterable[type[Capability]] = (),
    rerun_on_new_data: bool = False,
    rerun_on: RerunOn = RerunOn.ADDED_OR_UPDATED,
    debounce: timedelta | None = None,
    max_cycles: int | None = None,
    timeout: timedelta | None = None,
    retry: RetryPolicy | None = None,
    emits: Iterable[DataPointEmission | BaseDataPoint[Any]] = (),
    emit_factory: Callable[[OperatorContext], Iterable[DataPointEmission | BaseDataPoint[Any]]] | None = None,
    sleep_after: float | None = None,
    raise_error: Exception | None = None,
) -> type[Operator]:
    """Create (and register) a stub Operator subclass with the given declarations.

    ``emits``/``emit_factory`` yield value-only ``DataPointEmission``s (``Leaf.emit(value)``);
    a full DataPoint is also accepted and normalized to an emission, so the orchestrator stamps
    its provenance just like a real operator's. ``emit_factory`` derives emissions from the
    context (e.g. from the delta); ``sleep_after`` sleeps (to trigger per-operation timeouts);
    ``raise_error`` raises after emitting (to exercise the failure-isolation path).
    """
    _id, _depends, _uses, _produces, _requires = (
        OperatorId(operator_id),
        frozenset(depends_on),
        frozenset(uses),
        frozenset(produces),
        frozenset(requires),
    )
    _policy = OperatorPolicy(
        rerun_on_new_data=rerun_on_new_data,
        rerun_on=rerun_on,
        debounce=debounce,
        max_cycles=max_cycles,
        timeout=timeout,
        retry=retry,
    )
    _emitted, _factory, _sleep, _error = tuple(emits), emit_factory, sleep_after, raise_error

    class _StubOperator(Operator):
        operator_id = _id
        policy = _policy
        depends_on = _depends
        uses = _uses
        produces = _produces
        requires = _requires

        async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:
            for item in _emitted if _factory is None else _factory(ctx):
                yield item if isinstance(item, DataPointEmission) else DataPointEmission(type(item), item.value)
            if _sleep is not None:
                await asyncio.sleep(_sleep)
            if _error is not None:
                raise _error

    return _StubOperator


def make_aggregator(
    operator_id: str,
    *,
    depends_on: Iterable[type[BaseDataPoint[Any]]] = (),
    uses: Iterable[type[BaseDataPoint[Any]]] = (),
    requires: Iterable[type[Capability]] = (),
    consumes: Iterable[type[BaseDataPoint[Any]]] = (),
    rerun_on_new_data: bool = False,
    rerun_on: RerunOn = RerunOn.ADDED_OR_UPDATED,
    debounce: timedelta | None = None,
    timeout: timedelta | None = None,
    interim_refresh: bool = False,
    on_aggregate: Callable[[OperatorContext], Awaitable[None]] | None = None,
) -> type[Aggregator]:
    """Create (and register) a stub Aggregator subclass."""
    _id, _depends, _uses, _requires = (
        OperatorId(operator_id),
        frozenset(depends_on),
        frozenset(uses),
        frozenset(requires),
    )
    _policy = OperatorPolicy(
        rerun_on_new_data=rerun_on_new_data, rerun_on=rerun_on, debounce=debounce, timeout=timeout
    )
    _hook, _interim, _consumes = on_aggregate, interim_refresh, frozenset(consumes)

    class _StubAggregator(Aggregator):
        operator_id = _id
        policy = _policy
        depends_on = _depends
        uses = _uses
        requires = _requires
        interim_refresh = _interim
        consumes = _consumes

        async def aggregate(self, ctx: OperatorContext) -> None:
            if _hook is not None:
                await _hook(ctx)

    return _StubAggregator
