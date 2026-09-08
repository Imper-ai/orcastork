"""TOOL — the ``orcastork-graph`` CLI: registry graph build, cycle-policy check, Mermaid rendering.

Stubs are created inside each test (via the doubles factories / local class definitions),
so the autouse ``registry_isolation`` fixture in ``conftest.py`` removes them afterwards;
``main()`` then sees exactly the stubs the test registered.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import pytest

from orcastork.capabilities import Capability
from orcastork.datapoints import BaseDataPoint, DataPointTypeConfig
from orcastork.operators import Aggregator, Operator
from orcastork.tools.graph import check_graph, main, render_mermaid

from .doubles.capabilities import make_capability
from .doubles.datapoints import IpDataPoint, RiskDataPoint
from .doubles.operators import make_aggregator, make_operator

# Mermaid labels render the class name; the shared stub factories reuse one class name,
# so these wrappers give each stub a distinct, assertable one.


def _operator(name: str, operator_id: str, **declarations: Any) -> type[Operator]:
    operator = make_operator(operator_id, **declarations)
    operator.__name__ = name
    return operator


def _aggregator(name: str, operator_id: str, **declarations: Any) -> type[Aggregator]:
    aggregator = make_aggregator(operator_id, **declarations)
    aggregator.__name__ = name
    return aggregator


def _capability(name: str, capability_id: str, **declarations: Any) -> type[Capability]:
    capability = make_capability(capability_id, **declarations)
    capability.__name__ = name
    return capability


def test_tool_01_mermaid_contains_nodes_and_edges_and_is_deterministic() -> None:
    capability = _capability('GeoLookup', 'tool_geo_lookup')
    producer = _operator('IpProducer', 'tool_ip_producer', produces={IpDataPoint})
    consumer = _operator('IpConsumer', 'tool_ip_consumer', depends_on={IpDataPoint}, requires={capability})
    sink = _aggregator('RiskSink', 'tool_risk_sink', depends_on={IpDataPoint})

    rendered = render_mermaid([producer, consumer, sink], [capability])

    assert rendered.splitlines()[0] == 'flowchart LR'
    assert '    op_tool_ip_producer["IpProducer"]' in rendered
    assert '    op_tool_risk_sink[/"RiskSink (aggregator)"/]' in rendered
    assert '    cap_tool_geo_lookup{{"GeoLookup"}}' in rendered
    assert '    op_tool_ip_producer --> op_tool_ip_consumer' in rendered
    assert '    op_tool_ip_producer --> op_tool_risk_sink' in rendered
    assert '    cap_tool_geo_lookup --> op_tool_ip_consumer' in rendered
    # Input order must not affect the output (stable diffs for a committed diagram).
    assert render_mermaid([sink, consumer, producer], [capability]) == rendered


def test_tool_01a_uses_renders_as_weaker_dotted_arrow() -> None:
    # A `uses` input is a rerun trigger, not a readiness gate. It must render as a weaker (dotted)
    # link so an aggregator that folds inputs via `uses` is no longer a disconnected sink.
    attr_producer = _operator('AttrProducer', 'tool_attr_producer', produces={IpDataPoint})
    risk_producer = _operator('RiskProducer', 'tool_risk_producer', produces={RiskDataPoint})
    folder = _aggregator('Folder', 'tool_folder', depends_on={RiskDataPoint}, uses={IpDataPoint})

    rendered = render_mermaid([attr_producer, risk_producer, folder], [])

    # Strong readiness edge (RiskProducer -> Folder via depends_on) stays a solid arrow.
    assert '    op_tool_risk_producer --> op_tool_folder' in rendered
    # The `uses` relationship (AttrProducer produces what Folder uses) is a weaker dotted arrow...
    assert '    op_tool_attr_producer -. uses .-> op_tool_folder' in rendered
    # ...and never a solid one.
    assert '    op_tool_attr_producer --> op_tool_folder' not in rendered
    # The weaker links are faded via a linkStyle targeting exactly the uses-edge indices (they are
    # emitted after the solid edges: 1 solid edge here → the single uses edge is index 1).
    assert any(line.startswith('    linkStyle 1 ') and 'stroke' in line for line in rendered.splitlines())
    # Byte-stable regardless of input order (a committed diagram only diffs on real changes).
    assert render_mermaid([folder, risk_producer, attr_producer], []) == rendered


def test_tool_01b_uses_edge_deduped_when_a_strong_edge_already_connects_the_pair() -> None:
    # When a producer feeds a consumer through BOTH depends_on and uses, only the strong solid edge
    # is drawn — the weaker dotted duplicate is suppressed.
    producer = _operator('Dp', 'tool_dedupe_producer', produces={IpDataPoint})
    consumer = _operator('Dc', 'tool_dedupe_consumer', depends_on={IpDataPoint}, uses={IpDataPoint})

    rendered = render_mermaid([producer, consumer], [])

    assert '    op_tool_dedupe_producer --> op_tool_dedupe_consumer' in rendered
    assert 'op_tool_dedupe_producer -. uses .-> op_tool_dedupe_consumer' not in rendered
    # No weak edges survive the dedupe, so no fade directive is emitted.
    assert 'linkStyle' not in rendered


def test_tool_02_check_acyclic_graph_exits_zero(capsys: pytest.CaptureFixture[str]) -> None:
    make_operator('tool_acyclic_producer', produces={IpDataPoint})
    make_operator('tool_acyclic_consumer', depends_on={IpDataPoint})

    # `--import-module` exercises the import-at-startup convention (already-imported is a no-op).
    assert main(['--import-module', 'tests.doubles.datapoints', '--check']) == 0

    captured = capsys.readouterr()
    assert 'full graph: OK' in captured.out
    assert captured.err == ''


def test_tool_03_check_unbounded_cycle_exits_one(capsys: pytest.CaptureFixture[str]) -> None:
    _operator('SelfLoop', 'tool_self_loop', produces={IpDataPoint}, depends_on={IpDataPoint})  # no max_cycles

    assert main(['--check']) == 1

    captured = capsys.readouterr()
    assert 'cycle (UNBOUNDED): SelfLoop' in captured.out
    assert 'SelfLoop' in captured.err
    assert 'max_cycles' in captured.err
    assert 'OK' not in captured.out


def test_tool_04_bounded_cycle_reported_but_exits_zero(capsys: pytest.CaptureFixture[str]) -> None:
    _operator('LoopX', 'tool_loop_x', produces={IpDataPoint}, depends_on={RiskDataPoint}, max_cycles=3)
    _operator('LoopY', 'tool_loop_y', produces={RiskDataPoint}, depends_on={IpDataPoint}, max_cycles=2)

    assert main(['--check']) == 0

    captured = capsys.readouterr()
    assert 'full graph: cycle (bounded): LoopX, LoopY [LoopX.max_cycles=3; LoopY.max_cycles=2]' in captured.out
    assert 'full graph: OK' in captured.out
    assert captured.err == ''


def test_tool_05_permitted_drops_capability_and_reports_it(capsys: pytest.CaptureFixture[str]) -> None:
    _capability('AllowedCap', 'tool_allowed_cap')
    gated = _capability('GatedCap', 'tool_gated_cap')
    _operator('CapUser', 'tool_cap_user', requires={gated})

    assert main(['--check', '--permitted', 'tool_allowed_cap']) == 0

    captured = capsys.readouterr()
    assert 'full graph: OK' in captured.out
    assert 'permitted subgraph: dropped capability (not permitted): GatedCap' in captured.out
    assert 'permitted subgraph: OK' in captured.out


def test_tool_05a_permitted_ids_are_whitespace_stripped(capsys: pytest.CaptureFixture[str]) -> None:
    _capability('CapA', 'cap_a')
    required = _capability('CapB', 'cap_b')
    _operator('CapUser', 'tool_cap_user', requires={required})

    assert main(['--permitted', 'cap_a, cap_b']) == 0

    captured = capsys.readouterr()
    # Without stripping, ' cap_b' would not match and CapB would be reported as dropped.
    assert 'dropped capability (not permitted): CapB' not in captured.out
    assert 'dropped capability (not permitted): CapA' not in captured.out
    assert 'permitted subgraph: OK' in captured.out


def test_tool_06_check_graph_with_locally_defined_datapoint() -> None:
    class ToolMetricDataPoint(BaseDataPoint[int]):
        type: Literal['tool_metric'] = 'tool_metric'
        config = DataPointTypeConfig(pii=False, ephemeral=False)

    producer = make_operator('tool_metric_producer', produces={ToolMetricDataPoint})
    consumer = make_operator('tool_metric_consumer', depends_on={ToolMetricDataPoint})

    result = check_graph([producer, consumer], [])

    assert result.error is None
    assert result.findings == []


def test_tool_07_mermaid_to_stdout_and_file_match(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _operator('SoloOp', 'tool_solo_op', produces={IpDataPoint})

    assert main(['--mermaid', '-']) == 0
    stdout = capsys.readouterr().out
    assert stdout.startswith('flowchart LR')
    assert '    op_tool_solo_op["SoloOp"]' in stdout

    target = tmp_path / 'graph.mmd'
    assert main(['--mermaid', str(target)]) == 0
    assert target.read_text() == stdout


def test_tool_08_help_exits_zero_and_no_action_exits_two(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(['--help'])
    assert excinfo.value.code == 0
    assert 'orcastork-graph' in capsys.readouterr().out

    assert main([]) == 2
    assert 'nothing to do' in capsys.readouterr().err


def test_tool_09_permitted_dropping_cap_breaks_the_cycle() -> None:
    # The cycle runs operator -> cap -> operator (cap depends_on a DataPoint the operator
    # produces; operator requires the cap). Restricting the permitted set must be applied
    # BEFORE cycle detection so the per-namespace subgraph reflects only permitted nodes: dropping
    # the capability node breaks the cycle, so the subgraph reports no cycle while the full
    # graph still does.
    cap = _capability('GeoNeedsIp', 'tool_geo_needs_ip', depends_on={IpDataPoint})
    operator = _operator('CapLooper', 'tool_cap_looper', produces={IpDataPoint}, requires={cap}, max_cycles=2)

    full = check_graph([operator], [cap])
    assert any(finding.startswith('cycle (') for finding in full.findings)

    restricted = check_graph([operator], [cap], permitted=frozenset())
    assert 'dropped capability (not permitted): GeoNeedsIp' in restricted.findings
    assert not any(finding.startswith('cycle (') for finding in restricted.findings)
    assert restricted.error is None


def test_tool_10_permitted_drop_can_still_expose_an_unbounded_cycle() -> None:
    # Dropping a capability must change which cycles are validated, not just node listing:
    # here the operator also has an unbounded self-loop, so even with the capability cycle
    # gone the per-namespace subgraph must still raise on the remaining unbounded cycle.
    cap = _capability('SafeCap', 'tool_safe_cap', depends_on={RiskDataPoint})
    # Bounded leg through the cap (produces RiskDataPoint -> cap -> operator) plus an
    # unbounded self-loop on IpDataPoint with no max_cycles.
    operator = _operator(
        'SelfLooper',
        'tool_self_looper',
        produces={IpDataPoint, RiskDataPoint},
        depends_on={IpDataPoint},
        requires={cap},
    )

    restricted = check_graph([operator], [cap], permitted=frozenset())
    assert 'dropped capability (not permitted): SafeCap' in restricted.findings
    assert restricted.error is not None
    assert 'SelfLooper' in restricted.error


def test_tool_11_capability_only_cycle_reports_unbounded_and_exits_one(
    capsys: pytest.CaptureFixture[str],
) -> None:
    # A cycle of only capabilities has no operator to bound: _describe_cycle hits its
    # bool(operators_on_cycle) guard (rendering an empty '[]' caps segment) and validate
    # rejects it, so --check exits 1 with the 'no boundable operator' error on stderr.
    provider = _capability('CapDee', 'tool_cap_dee')
    consumer = _capability('CapCee', 'tool_cap_cee', requires={provider})
    provider.requires = frozenset({consumer})  # close the requires loop  # type: ignore[misc]

    assert main(['--check']) == 1

    captured = capsys.readouterr()
    assert 'cycle (UNBOUNDED): CapCee, CapDee []' in captured.out
    assert 'OK' not in captured.out
    assert 'no boundable operator' in captured.err
    assert 'CapCee' in captured.err
    assert 'CapDee' in captured.err


def test_tool_12_mermaid_is_deterministic_under_same_name_nodes() -> None:
    # Two distinct operators share a __name__ but have distinct ids. render_mermaid sorts by
    # _node_id (which embeds the unique id), so the declarations stay distinct and the output
    # is byte-stable regardless of input order — a guarantee __name__-based sorting would lose.
    a = _operator('Dup', 'dup_a', produces={IpDataPoint})
    b = _operator('Dup', 'dup_b', depends_on={RiskDataPoint})

    rendered = render_mermaid([a, b], [])
    assert render_mermaid([b, a], []) == rendered  # byte-identical across input orders

    lines = rendered.splitlines()
    assert '    op_dup_a["Dup"]' in lines
    assert '    op_dup_b["Dup"]' in lines
    # Declarations are emitted in id-sorted order (dup_a before dup_b).
    assert lines.index('    op_dup_a["Dup"]') < lines.index('    op_dup_b["Dup"]')


def test_tool_13_check_failure_does_not_suppress_mermaid_and_max_exit(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # A failing --check must surface exit 1 even though --mermaid also runs; the max()
    # accumulation across independent actions must not be clobbered by the later mermaid write.
    _operator('SelfLoop', 'tool_max_self_loop', produces={IpDataPoint}, depends_on={IpDataPoint})
    target = tmp_path / 'g.mmd'

    assert main(['--check', '--mermaid', str(target)]) == 1

    capsys.readouterr()  # drain the check output
    written = target.read_text()
    assert written.startswith('flowchart LR')
    assert '    op_tool_max_self_loop["SelfLoop"]' in written


def test_tool_14_failing_permitted_keeps_max_exit_with_mermaid(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Variant: a failing --permitted subgraph (unbounded self-loop survives the restriction)
    # plus a successful --mermaid write must still yield exit 1.
    _operator('PermLoop', 'tool_perm_self_loop', produces={IpDataPoint}, depends_on={IpDataPoint})
    target = tmp_path / 'perm.mmd'

    assert main(['--permitted', '', '--mermaid', str(target)]) == 1

    capsys.readouterr()
    assert target.read_text().startswith('flowchart LR')
