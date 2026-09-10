"""The ``orcastork-lite-graph`` CLI: discovery by module namespace, checks, Mermaid, and its separation."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

import orcastork_lite
from orcastork_lite.tools.graph import check_graph, discover, main, render_mermaid

from . import flow_fixture
from .conftest import Ip, Risk, make_capability, make_operator

FIXTURE = 'tests.orcastork_lite.flow_fixture'


def test_discover_collects_concrete_classes_once_sorted_by_id() -> None:
    graph = discover([flow_fixture])
    assert graph.operators == [flow_fixture.Producer, flow_fixture.Reporter]
    assert graph.capabilities == [flow_fixture.Lookup]


def test_mermaid_renders_nodes_edges_sink_shape_and_dotted_uses() -> None:
    cap = make_capability('geo')
    producer = make_operator('producer', produces={Ip})
    consumer = make_operator('consumer', depends_on={Ip}, requires={cap})
    scorer = make_operator('scorer', produces={Risk})
    sink = make_operator('sink', depends_on={Ip}, uses={Risk}, consumes={Ip, Risk})

    rendered = render_mermaid([producer, consumer, scorer, sink], [cap])
    lines = rendered.splitlines()
    assert lines[0] == 'flowchart LR'
    assert '    op_producer["Producer"]' in lines
    assert '    op_sink[/"Sink (sink)"/]' in lines
    assert '    cap_geo{{"Geo"}}' in lines
    assert '    op_producer --> op_consumer' in lines
    assert '    cap_geo --> op_consumer' in lines
    assert '    op_scorer -. uses .-> op_sink' in lines
    assert lines[-1].startswith('    linkStyle ')
    assert rendered == render_mermaid([sink, scorer, consumer, producer], [cap])  # deterministic


def test_check_graph_reports_cycles_and_permitted_drops() -> None:
    cap = make_capability('geo')
    a = make_operator('a', depends_on={Ip}, produces={Risk}, requires={cap}, max_cycles=2)
    b = make_operator('b', depends_on={Risk}, produces={Ip})

    unbounded = check_graph([a, b], [cap])
    assert unbounded.error is not None and 'max_cycles' in unbounded.error
    assert any(finding.startswith('cycle (UNBOUNDED)') for finding in unbounded.findings)

    restricted = check_graph([a, b], [cap], permitted=frozenset())
    assert restricted.findings[0] == 'dropped capability (not permitted): Geo'


def test_main_check_and_mermaid_on_the_fixture_module(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    target = tmp_path / 'graph.mmd'
    assert main(['-m', FIXTURE, '--check', '--mermaid', str(target), '--permitted', 'fixture_lookup, ']) == 0
    out = capsys.readouterr().out
    assert 'full graph: OK' in out and 'permitted subgraph: OK' in out
    rendered = target.read_text()
    assert '    op_fixture_producer --> op_fixture_reporter' in rendered
    assert '    cap_fixture_lookup --> op_fixture_producer' in rendered

    assert main(['-m', FIXTURE, '--mermaid', '-']) == 0
    assert capsys.readouterr().out == rendered


def test_main_exit_codes(capsys: pytest.CaptureFixture[str]) -> None:
    assert main([]) == 2
    assert 'nothing to do' in capsys.readouterr().err
    with pytest.raises(SystemExit) as raised:
        main(['--help'])
    assert raised.value.code == 0


def test_the_library_never_imports_the_tools_package() -> None:
    root = Path(orcastork_lite.__file__).parent
    offenders = []
    for file in sorted(root.rglob('*.py')):
        if file.relative_to(root).parts[0] == 'tools':
            continue
        for node in ast.walk(ast.parse(file.read_text())):
            names = (
                [alias.name for alias in node.names]
                if isinstance(node, ast.Import)
                else [node.module or '']
                if isinstance(node, ast.ImportFrom)
                else []
            )
            if any('tools' in name.split('.') for name in names):
                offenders.append(file.relative_to(root).as_posix())
    assert offenders == []
