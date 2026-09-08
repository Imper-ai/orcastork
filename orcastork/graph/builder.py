"""Build the static dependency graph from the operator + capability registries.

Nodes are operator and capability **classes**. A directed edge ``X → Y`` means "Y depends
on something X provides":

* operator A *produces* a DataPoint type that operator/capability B *depends_on*
  (subtype-aware — a ``WorkEmail`` producer feeds an ``EmailDataPoint`` consumer);
* capability C is *required* by operator/capability B (layering / capability use).

The **abstract-produces rule**: an operator that declares an abstract DataPoint in
``produces`` is treated as producing every registered concrete leaf of that type, so no
real edge is missed. Aggregators (empty ``produces``) emit nothing and so are pure sinks.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from ..capabilities.base import Capability
from ..datapoints import BaseDataPoint, subtypes_of
from ..datapoints.base import _ABSTRACT_TYPES
from ..ids import CapabilityId
from ..operators.base import Operator

# A graph node is an Operator subclass or a Capability subclass (both declare depends_on /
# requires; only operators declare produces).
Node = type[Operator] | type[Capability]


def effective_produces(operator: type[Operator]) -> set[type[BaseDataPoint[Any]]]:
    produced: set[type[BaseDataPoint[Any]]] = set()
    for declared in operator.produces:
        if declared in _ABSTRACT_TYPES:
            produced.update(subtypes_of(declared))  # abstract → all concrete leaves
        else:
            produced.add(declared)
    return produced


def build_graph(
    operators: Iterable[type[Operator]], capabilities: Iterable[type[Capability]]
) -> dict[Node, set[Node]]:
    """Construct the dependency graph (adjacency map) over operators + capabilities."""
    operator_list, capability_list = list(operators), list(capabilities)
    nodes: list[Node] = [*operator_list, *capability_list]
    edges: dict[Node, set[Node]] = {node: set() for node in nodes}

    # DataPoint dependencies: an operator producing a subtype of a consumer's input feeds it.
    producers = [(operator, effective_produces(operator)) for operator in operator_list]
    for consumer in nodes:
        for required_type in consumer.depends_on:
            for producer, produced_types in producers:
                if any(issubclass(produced, required_type) for produced in produced_types):
                    edges[producer].add(consumer)

    # Capability requirements (layering / use): a provider feeds whatever requires it.
    for requirer in nodes:
        for required_capability in requirer.requires:
            for provider in capability_list:
                if issubclass(provider, required_capability):
                    edges[provider].add(requirer)

    return edges


def build_uses_edges(operators: Iterable[type[Operator]]) -> dict[Node, set[Node]]:
    """Producer→consumer edges for the ``uses`` relation, mirroring ``build_graph``'s subtype matching.

    ``uses`` inputs are rerun triggers, not readiness gates — an operator does not wait on them, it
    just re-runs when they change (an aggregator folds them in). Only operators declare ``uses``;
    capabilities have none, and only operators produce, so this is operator→operator. Self-edges (an
    operator using what it produces) are dropped. Kept separate from ``build_graph`` so the cycle
    checker's strong-edge semantics are unchanged — this exists to render the weaker ``uses`` links.
    """
    operator_list = list(operators)
    producers = [(operator, effective_produces(operator)) for operator in operator_list]
    edges: dict[Node, set[Node]] = {operator: set() for operator in operator_list}
    for consumer in operator_list:
        for used_type in consumer.uses:
            for producer, produced_types in producers:
                if producer is not consumer and any(issubclass(p, used_type) for p in produced_types):
                    edges[producer].add(consumer)
    return edges


def restrict_to_permitted(edges: dict[Node, set[Node]], permitted: frozenset[CapabilityId]) -> dict[Node, set[Node]]:
    """The per-session subgraph: drop capability nodes the namespace does not permit (operators stay)."""

    def keep(node: Node) -> bool:
        return node.capability_id in permitted if issubclass(node, Capability) else True

    return {node: {target for target in targets if keep(target)} for node, targets in edges.items() if keep(node)}
