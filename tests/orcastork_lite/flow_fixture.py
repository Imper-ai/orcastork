"""A tiny flow module the graph-tool tests import through ``-m`` (module-namespace discovery)."""

from __future__ import annotations

from collections.abc import AsyncIterator

from orcastork_lite import (
    Capability,
    CapabilityContext,
    DataPoint,
    DataPointEmission,
    Operator,
    OperatorContext,
    OperatorId,
    OperatorPolicy,
)
from orcastork_lite.ids import CapabilityId


class Seed(DataPoint[str]): ...


class Derived(DataPoint[int]): ...


class Lookup(Capability):
    capability_id = CapabilityId('fixture_lookup')

    async def activate(self, _ctx: CapabilityContext) -> None:
        return None


class Producer(Operator):
    operator_id = OperatorId('fixture_producer')
    policy = OperatorPolicy(rerun_on_new_data=False)
    depends_on = frozenset({Seed})
    produces = frozenset({Derived})
    requires = frozenset({Lookup})

    async def run(self, ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:
        yield Derived.emit(len(ctx.store.of_type(Seed)))


class Reporter(Operator):
    operator_id = OperatorId('fixture_reporter')
    policy = OperatorPolicy(rerun_on_new_data=True)
    depends_on = frozenset({Derived})
    consumes = frozenset({Derived})

    async def run(self, _ctx: OperatorContext) -> AsyncIterator[DataPointEmission]:
        return
        yield  # an async generator that emits nothing


class _AbstractHelper(Operator):
    """No operator_id: an intermediate the tool must not pick up."""


NOT_A_CLASS = Producer  # a second binding to the same class must not duplicate the node
