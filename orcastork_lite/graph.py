"""The static dependency graph over operator + capability classes.

A directed edge ``X → Y`` means "Y depends on something X provides": an operator *produces* a
DataPoint type that Y *depends_on*, or a capability is *required* by Y. Matching is
subtype-aware in both directions — a producer of ``P`` feeds a need for ``T`` iff one is a
subclass of the other — so an abstract ``produces`` reaches a leaf consumer and a leaf producer
reaches an abstract consumer without any registry of concrete leaves.

Cycles are detected with Tarjan's SCC algorithm and permitted only if **bounded**: every
operator on the cycle declares a ``max_cycles`` cap. Backward reachability from a set of sink
types tells the orchestrator which operators are worth running at all.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from .capabilities import Capability
from .datapoints import DataPoint
from .exceptions import UnboundedCycleError
from .ids import CapabilityId, OperatorId
from .operators import Operator

Node = type[Operator] | type[Capability]


def _overlaps(produced: type[DataPoint[Any]], needed: type[DataPoint[Any]]) -> bool:
    return issubclass(produced, needed) or issubclass(needed, produced)


def _feeds(producer: type[Operator], needed_types: Iterable[type[DataPoint[Any]]]) -> bool:
    return any(_overlaps(produced, needed) for produced in producer.produces for needed in needed_types)


def build_edges(
    operators: Iterable[type[Operator]], capabilities: Iterable[type[Capability]]
) -> dict[Node, set[Node]]:
    """The readiness graph (adjacency map) over operators + capabilities."""
    operator_list, capability_list = list(operators), list(capabilities)
    nodes: list[Node] = [*operator_list, *capability_list]
    edges: dict[Node, set[Node]] = {node: set() for node in nodes}
    for consumer in nodes:
        for producer in operator_list:
            if _feeds(producer, consumer.depends_on):
                edges[producer].add(consumer)
        for required in consumer.requires:
            for provider in capability_list:
                if issubclass(provider, required):
                    edges[provider].add(consumer)
    return edges


def build_uses_edges(operators: Iterable[type[Operator]]) -> dict[Node, set[Node]]:
    """Producer→consumer edges for the weaker ``uses`` relation (rerun triggers, not readiness gates).

    Kept apart from :func:`build_edges` so the cycle rule reasons over readiness edges only; the
    graph tool renders these dotted. Self-edges are dropped.
    """
    operator_list = list(operators)
    edges: dict[Node, set[Node]] = {operator: set() for operator in operator_list}
    for consumer in operator_list:
        for producer in operator_list:
            if producer is not consumer and _feeds(producer, consumer.uses):
                edges[producer].add(consumer)
    return edges


def restrict_to_permitted(edges: dict[Node, set[Node]], permitted: frozenset[CapabilityId]) -> dict[Node, set[Node]]:
    """The per-namespace subgraph: drop capability nodes the namespace does not permit (operators stay)."""

    def keep(node: Node) -> bool:
        return node.capability_id in permitted if issubclass(node, Capability) else True

    return {node: {target for target in targets if keep(target)} for node, targets in edges.items() if keep(node)}


def find_cycles(edges: dict[Node, set[Node]]) -> list[frozenset[Node]]:
    """Each strongly-connected component that forms a cycle (size > 1, or a self-loop)."""
    index_counter = 0
    indices: dict[Node, int] = {}
    lowlinks: dict[Node, int] = {}
    stack: list[Node] = []
    on_stack: set[Node] = set()
    cycles: list[frozenset[Node]] = []

    def strongconnect(node: Node) -> None:
        nonlocal index_counter
        indices[node] = lowlinks[node] = index_counter
        index_counter += 1
        stack.append(node)
        on_stack.add(node)
        for successor in edges.get(node, set()):
            if successor not in indices:
                strongconnect(successor)
                lowlinks[node] = min(lowlinks[node], lowlinks[successor])
            elif successor in on_stack:
                lowlinks[node] = min(lowlinks[node], indices[successor])
        if lowlinks[node] == indices[node]:
            component: list[Node] = []
            while True:
                member = stack.pop()
                on_stack.discard(member)
                component.append(member)
                if member is node:
                    break
            if len(component) > 1 or node in edges.get(node, set()):
                cycles.append(frozenset(component))

    for node in edges:
        if node not in indices:
            strongconnect(node)
    return cycles


def validate_acyclic_or_bounded(edges: dict[Node, set[Node]]) -> None:
    """Raise :class:`UnboundedCycleError` unless every cycle is bounded by ``max_cycles`` caps."""
    for cycle in find_cycles(edges):
        operators_on_cycle = [node for node in cycle if issubclass(node, Operator)]
        if not operators_on_cycle:
            raise UnboundedCycleError(f'cycle with no boundable operator: {sorted(n.__name__ for n in cycle)}')
        unbounded = [op for op in operators_on_cycle if op.policy.max_cycles is None]
        if unbounded:
            raise UnboundedCycleError(
                f'cycle has operators without a max_cycles circuit-breaker: {sorted(op.__name__ for op in unbounded)}'
            )


def cycle_caps(edges: dict[Node, set[Node]]) -> dict[OperatorId, int]:
    """The ``max_cycles`` cap of every operator that sits on a cycle — what the circuit breaker enforces."""
    caps: dict[OperatorId, int] = {}
    for cycle in find_cycles(edges):
        for node in cycle:
            if issubclass(node, Operator) and node.policy.max_cycles is not None:
                caps[node.operator_id] = node.policy.max_cycles
    return caps


def backward_reachable(
    operators: Iterable[type[Operator]],
    capabilities: Iterable[type[Capability]],
    sinks: frozenset[type[DataPoint[Any]]],
) -> frozenset[Node]:
    """The operators producing (transitively) toward ``sinks`` + the capabilities they require.

    Empty ``sinks`` yields the empty set (the caller treats "no declared sinks" as "no pruning").
    """
    operator_list, capability_list = list(operators), list(capabilities)
    needed: set[type[DataPoint[Any]]] = set(sinks)
    kept_operators: set[type[Operator]] = set()
    changed = True
    while changed:
        changed = False
        for operator in operator_list:
            if operator not in kept_operators and _feeds(operator, needed):
                kept_operators.add(operator)
                needed |= operator.depends_on
                changed = True
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
