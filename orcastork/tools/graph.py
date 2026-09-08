"""``orcastork-graph`` — deploy-time dependency-graph validation + Mermaid rendering.

The design promises that CI builds the dependency graph and flags every cycle at
deploy-time; without this tool the bounded-cycle rule only runs inside
``Orchestrator.__init__``, once per session. This entrypoint imports the flow modules
(so their operators/capabilities/datapoints self-register — the same import-at-startup
convention the orchestrator relies on), builds the full registry graph, validates the
cycle policy, and renders the graph as Mermaid so humans can see what CI is checking.

Run as ``orcastork-graph`` (console script) or ``python -m orcastork.tools.graph``.
"""

from __future__ import annotations

import argparse
import importlib
import re
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

from ..capabilities.base import Capability
from ..exceptions import UnboundedCycleError
from ..graph import build_graph, build_uses_edges, find_cycles, restrict_to_permitted, validate_acyclic_or_bounded
from ..graph.builder import Node
from ..ids import CapabilityId
from ..operators.aggregator import Aggregator
from ..operators.base import Operator

_ID_SANITIZER = re.compile(r'[^0-9A-Za-z_]')


@dataclass(frozen=True)
class GraphCheck:
    """Outcome of one cycle-policy validation run (full graph or per-namespace subgraph)."""

    findings: list[str]  # informational lines: dropped capability nodes, every cycle found
    error: str | None  # the UnboundedCycleError message when the cycle policy fails


def _registered_capabilities() -> list[type[Capability]]:
    # Capability (unlike Operator) exposes no public `registered()` accessor; this read-only
    # tool deliberately reads the private registry directly rather than widening Capability's
    # public API for one consumer.
    return list(Capability._registry.values())


def _node_id(node: Node) -> str:
    # The op_/cap_ prefixes keep ids unique across the two registries (an operator_id and
    # a capability_id may collide); sanitizing keeps the ids parseable by Mermaid.
    if issubclass(node, Capability):
        return f'cap_{_ID_SANITIZER.sub("_", node.capability_id)}'
    return f'op_{_ID_SANITIZER.sub("_", node.operator_id)}'


def _declaration(node: Node) -> str:
    if issubclass(node, Capability):
        return f'{_node_id(node)}{{{{"{node.__name__}"}}}}'  # hexagon
    if issubclass(node, Aggregator):
        return f'{_node_id(node)}[/"{node.__name__} (aggregator)"/]'  # parallelogram
    return f'{_node_id(node)}["{node.__name__}"]'  # rectangle


def render_mermaid(operators: Iterable[type[Operator]], capabilities: Iterable[type[Capability]]) -> str:
    """Render the dependency graph as a Mermaid ``flowchart LR``.

    Nodes and edge targets are sorted so the output is deterministic — a committed
    diagram only diffs when the graph actually changes.
    """
    edges = build_graph(operators, capabilities)
    uses_edges = build_uses_edges(operators)
    lines = ['flowchart LR']
    lines.extend(f'    {_declaration(node)}' for node in sorted(edges, key=_node_id))

    strong = [
        f'    {_node_id(source)} --> {_node_id(target)}'
        for source in sorted(edges, key=_node_id)
        for target in sorted(edges[source], key=_node_id)
    ]
    # Weaker `uses` links (rerun triggers, not readiness gates) render dotted. Skip any pair a strong
    # readiness edge already connects so the two never draw over each other.
    weak = [
        f'    {_node_id(source)} -. uses .-> {_node_id(target)}'
        for source in sorted(uses_edges, key=_node_id)
        for target in sorted(uses_edges[source], key=_node_id)
        if target not in edges.get(source, set())
    ]
    lines.extend(strong)
    lines.extend(weak)
    # Fade the `uses` links so the eye follows the solid readiness DAG and the (often large) fan-in
    # into an aggregator recedes into the background. Mermaid indexes links in definition order, so
    # the weak edges occupy the contiguous range right after the strong ones.
    if weak:
        weak_indices = ','.join(str(index) for index in range(len(strong), len(strong) + len(weak)))
        lines.append(f'    linkStyle {weak_indices} stroke:#9aa0a6,stroke-width:1px,opacity:0.5')
    return '\n'.join(lines) + '\n'


def _describe_cycle(cycle: frozenset[Node]) -> str:
    operators_on_cycle = sorted(
        (node for node in cycle if issubclass(node, Operator)),
        key=lambda operator: (operator.__name__, operator.operator_id),
    )
    bounded = bool(operators_on_cycle) and all(
        operator.policy.max_cycles is not None for operator in operators_on_cycle
    )
    members = ', '.join(sorted(node.__name__ for node in cycle))
    caps = '; '.join(f'{op.__name__}.max_cycles={op.policy.max_cycles}' for op in operators_on_cycle)
    return f'cycle ({"bounded" if bounded else "UNBOUNDED"}): {members} [{caps}]'


def check_graph(
    operators: Iterable[type[Operator]],
    capabilities: Iterable[type[Capability]],
    permitted: frozenset[CapabilityId] | None = None,
) -> GraphCheck:
    """Validate the cycle policy; with ``permitted``, validate the per-namespace subgraph instead."""
    edges = build_graph(operators, capabilities)
    findings: list[str] = []
    if permitted is not None:
        restricted = restrict_to_permitted(edges, permitted)
        dropped = sorted(node.__name__ for node in set(edges) - set(restricted))
        findings.extend(f'dropped capability (not permitted): {name}' for name in dropped)
        edges = restricted
    findings.extend(_describe_cycle(cycle) for cycle in find_cycles(edges))
    try:
        validate_acyclic_or_bounded(edges)
    except UnboundedCycleError as error:
        return GraphCheck(findings=findings, error=str(error))
    return GraphCheck(findings=findings, error=None)


def _report(label: str, result: GraphCheck) -> int:
    for finding in result.findings:
        print(f'{label}: {finding}')
    if result.error is not None:
        print(f'{label}: {result.error}', file=sys.stderr)
        return 1
    print(f'{label}: OK')
    return 0


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog='orcastork-graph',
        description=(
            'Build the operator/capability dependency graph from the registries, '
            'validate its cycle policy, and render it as Mermaid.'
        ),
    )
    parser.add_argument(
        '-m',
        '--import-module',
        action='append',
        default=[],
        metavar='MODULE',
        help='module to import first so its operators/capabilities/datapoints self-register (repeatable)',
    )
    parser.add_argument(
        '--check',
        action='store_true',
        help='validate the bounded-cycle policy; exit 1 on an unbounded cycle',
    )
    parser.add_argument(
        '--mermaid',
        metavar='PATH',
        help="write the graph as a Mermaid flowchart to PATH ('-' for stdout)",
    )
    parser.add_argument(
        '--permitted',
        metavar='IDS',
        help='comma-separated CapabilityIds; also validate the per-namespace subgraph restricted to these',
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    if not args.check and args.mermaid is None and args.permitted is None:
        print('nothing to do: pass --check, --mermaid and/or --permitted', file=sys.stderr)
        return 2
    for module_name in args.import_module:
        importlib.import_module(module_name)
    # Sorted by registry id so findings come out in a stable order across runs.
    operators = sorted(Operator.registered().values(), key=lambda operator: operator.operator_id)
    capabilities = sorted(_registered_capabilities(), key=lambda capability: capability.capability_id)

    exit_code = 0
    if args.check:
        exit_code = max(exit_code, _report('full graph', check_graph(operators, capabilities)))
    if args.permitted is not None:
        permitted = frozenset(CapabilityId(part.strip()) for part in args.permitted.split(',') if part.strip())
        exit_code = max(exit_code, _report('permitted subgraph', check_graph(operators, capabilities, permitted)))
    if args.mermaid is not None:
        rendered = render_mermaid(operators, capabilities)
        if args.mermaid == '-':
            sys.stdout.write(rendered)
        else:
            Path(args.mermaid).write_text(rendered)
    return exit_code


if __name__ == '__main__':
    sys.exit(main())
