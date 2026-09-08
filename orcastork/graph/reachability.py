"""Backward reachability — the operators/capabilities whose output is actually consumed.

Given the **sink** DataPoint types a flow ultimately considers/persists (an aggregator's declared
``consumes``, plus its own gate inputs and the completion condition), this computes the operators
whose output is transitively needed to produce those sinks, and the capabilities those operators
require. Everything outside that closure is *dead*: its output feeds neither the aggregator nor any
operator the aggregator (transitively) needs, so running it only wastes work and can needlessly hold
the session open. The orchestrator prunes the dead operators before gathering.

Matching is subtype-aware and mirrors :func:`build_graph`'s edge rule exactly — a producer of ``P``
feeds a need for ``T`` iff ``issubclass(P, T)`` — so abstract intermediates and concrete leaves line
up the same way the live scheduler resolves readiness.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from ..capabilities.base import Capability
from ..datapoints import BaseDataPoint
from ..operators.base import Operator
from .builder import Node, effective_produces


def backward_reachable(
    operators: Iterable[type[Operator]],
    capabilities: Iterable[type[Capability]],
    sinks: frozenset[type[BaseDataPoint[Any]]],
) -> frozenset[Node]:
    """The operators producing (transitively) toward ``sinks`` + the capabilities they require.

    Empty ``sinks`` yields the empty set (the caller treats "no declared sinks" as "no pruning").
    """
    operator_list = list(operators)
    capability_list = list(capabilities)

    # Backward fixpoint over needed DataPoint types: keep an operator once it produces a needed type,
    # then its own inputs (``depends_on``) become needed too — pulling its upstream producers in.
    needed: set[type[BaseDataPoint[Any]]] = set(sinks)
    kept_operators: set[type[Operator]] = set()
    changed = True
    while changed:
        changed = False
        for operator in operator_list:
            if operator in kept_operators:
                continue
            if any(issubclass(produced, need) for produced in effective_produces(operator) for need in needed):
                kept_operators.add(operator)
                needed |= operator.depends_on
                changed = True

    # A capability is kept iff a kept node requires it (subtype-aware), transitively across layering.
    kept_capabilities: set[type[Capability]] = set()
    changed = True
    while changed:
        changed = False
        requirers: list[Node] = [*kept_operators, *kept_capabilities]
        for requirer in requirers:
            for required in requirer.requires:
                for provider in capability_list:
                    if provider not in kept_capabilities and issubclass(provider, required):
                        kept_capabilities.add(provider)
                        changed = True

    return frozenset(kept_operators | kept_capabilities)
