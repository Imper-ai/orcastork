"""Cycle detection (Tarjan SCC) + the deploy-time bounded-cycle rule.

CI builds the graph and flags every cycle. A cycle is **permitted only if bounded** —
every operator on it must declare a ``max_cycles`` circuit-breaker cap (and the cycle must
contain at least one operator to bound). An unbounded cycle raises
:class:`UnboundedCycleError`, the same posture as rejecting bad config at startup but earlier.
"""

from __future__ import annotations

from ..exceptions import UnboundedCycleError
from ..operators.base import Operator
from .builder import Node


def find_cycles(edges: dict[Node, set[Node]]) -> list[frozenset[Node]]:
    """Return each strongly-connected component that forms a cycle (size > 1, or a self-loop)."""
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
    """Raise :class:`UnboundedCycleError` unless every cycle is bounded by circuit-breaker caps."""
    for cycle in find_cycles(edges):
        operators_on_cycle = [node for node in cycle if issubclass(node, Operator)]
        if not operators_on_cycle:
            raise UnboundedCycleError(f'cycle with no boundable operator: {sorted(n.__name__ for n in cycle)}')
        unbounded = [op for op in operators_on_cycle if op.policy.max_cycles is None]
        if unbounded:
            raise UnboundedCycleError(
                f'cycle has operators without a max_cycles circuit-breaker: {sorted(op.__name__ for op in unbounded)}'
            )
