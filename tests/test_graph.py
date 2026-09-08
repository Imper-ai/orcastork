"""GRAPH — dependency graph construction, cycle detection, bounded-cycle rule, breaker."""

from __future__ import annotations

import pytest

from orcastork.capabilities import Capability
from orcastork.datapoints import BaseDataPoint, DataPointTypeConfig
from orcastork.exceptions import UnboundedCycleError
from orcastork.graph import (
    CircuitBreaker,
    build_graph,
    find_cycles,
    restrict_to_permitted,
    validate_acyclic_or_bounded,
)
from orcastork.graph.builder import effective_produces
from orcastork.ids import CapabilityId, OperatorId
from orcastork.operators import Operator

from .doubles.capabilities import make_capability
from .doubles.datapoints import EmailDataPoint, IpDataPoint, RiskDataPoint, WorkEmailDataPoint
from .doubles.operators import make_aggregator, make_operator


def _build_breaker_for(operators: list[type[Operator]], capabilities: list[type[Capability]]) -> CircuitBreaker:
    """Mirror ``Orchestrator._build_circuit_breaker`` over a (possibly pruned) gathering set.

    The orchestrator recomputes cycles on the run-time gathering set and caps only the
    operators that survive; reproducing that here exercises the graph primitives the live
    breaker is built from without standing up a whole orchestrator.
    """
    caps: dict[OperatorId, int] = {}
    for cycle in find_cycles(build_graph(operators, capabilities)):
        for node in cycle:
            if issubclass(node, Operator) and node.policy.max_cycles is not None:
                caps[node.operator_id] = node.policy.max_cycles
    return CircuitBreaker(caps)


def test_graph_01_nodes_and_subtype_aware_edges() -> None:
    producer = make_operator('produces_work', produces={WorkEmailDataPoint})
    consumer = make_operator('consumes_email', depends_on={EmailDataPoint})  # subtype-aware
    edges = build_graph([producer, consumer], [])
    assert consumer in edges[producer]


def test_graph_02_tarjan_detects_acyclic_and_cyclic() -> None:
    a = make_operator('acyc_a', produces={IpDataPoint})
    b = make_operator('acyc_b', depends_on={IpDataPoint})
    assert find_cycles(build_graph([a, b], [])) == []

    x = make_operator('cyc_x', produces={IpDataPoint}, depends_on={RiskDataPoint})
    y = make_operator('cyc_y', produces={RiskDataPoint}, depends_on={IpDataPoint})
    assert find_cycles(build_graph([x, y], []))


def test_graph_03_bounded_cycle_permitted_unbounded_fails() -> None:
    bounded_x = make_operator('b_x', produces={IpDataPoint}, depends_on={RiskDataPoint}, max_cycles=3)
    bounded_y = make_operator('b_y', produces={RiskDataPoint}, depends_on={IpDataPoint}, max_cycles=3)
    validate_acyclic_or_bounded(build_graph([bounded_x, bounded_y], []))  # no raise

    unbounded_y = make_operator('u_y', produces={RiskDataPoint}, depends_on={IpDataPoint})  # no max_cycles
    with pytest.raises(UnboundedCycleError):
        validate_acyclic_or_bounded(build_graph([bounded_x, unbounded_y], []))


def test_graph_04_cycle_through_capabilities_is_detected() -> None:
    provider = make_capability('cap_d')
    consumer = make_capability('cap_c', requires={provider})
    provider.requires = frozenset({consumer})  # close the requires loop  # type: ignore[misc]
    assert find_cycles(build_graph([], [provider, consumer]))


def test_graph_05_cycle_via_capability_required_datapoint_is_detected() -> None:
    cap = make_capability('cap_needs_ip', depends_on={IpDataPoint})
    operator = make_operator('op_needs_cap', produces={IpDataPoint}, requires={cap}, max_cycles=2)
    cycles = find_cycles(build_graph([operator], [cap]))
    assert any(operator in cycle and cap in cycle for cycle in cycles)


def test_graph_06_aggregators_are_sinks() -> None:
    producer = make_operator('feeds_agg', produces={IpDataPoint})
    aggregator = make_aggregator('sink_agg', depends_on={IpDataPoint})  # produces nothing
    edges = build_graph([producer, aggregator], [])
    assert edges[aggregator] == set()  # no out-edges
    assert all(aggregator not in cycle for cycle in find_cycles(edges))


def test_graph_07_abstract_produces_edge_rule_resolves_to_concrete_leaves() -> None:
    producer = make_operator('produces_email', produces={EmailDataPoint})  # abstract
    consumer = make_operator('needs_work', depends_on={WorkEmailDataPoint})  # concrete leaf
    edges = build_graph([producer, consumer], [])
    assert consumer in edges[producer]


def test_graph_08_per_session_subgraph_drops_unpermitted_capabilities() -> None:
    cap = make_capability('gated_cap')
    operator = make_operator('uses_cap', requires={cap})
    edges = build_graph([operator], [cap])

    without = restrict_to_permitted(edges, frozenset())
    assert cap not in without
    with_permission = restrict_to_permitted(edges, frozenset({CapabilityId('gated_cap')}))
    assert cap in with_permission


def test_graph_09_circuit_breaker_trips_after_cap() -> None:
    breaker = CircuitBreaker({OperatorId('loop'): 2})
    breaker.record_run(OperatorId('loop'))
    assert not breaker.is_tripped(OperatorId('loop'))
    breaker.record_run(OperatorId('loop'))
    assert breaker.is_tripped(OperatorId('loop'))


def test_graph_10_breaker_counter_is_per_session() -> None:
    first = CircuitBreaker({OperatorId('loop'): 1})
    second = CircuitBreaker({OperatorId('loop'): 1})
    first.record_run(OperatorId('loop'))
    assert first.is_tripped(OperatorId('loop'))
    assert not second.is_tripped(OperatorId('loop'))  # independent across sessions


def test_graph_11_self_edge_is_a_one_node_cycle_requiring_a_cap() -> None:
    unbounded = make_operator('self_loop', produces={IpDataPoint}, depends_on={IpDataPoint})
    edges = build_graph([unbounded], [])
    assert any(unbounded in cycle for cycle in find_cycles(edges))
    with pytest.raises(UnboundedCycleError):
        validate_acyclic_or_bounded(edges)

    bounded = make_operator('self_loop_capped', produces={IpDataPoint}, depends_on={IpDataPoint}, max_cycles=1)
    validate_acyclic_or_bounded(build_graph([bounded], []))  # no raise


def test_graph_12_abstract_produces_with_no_concrete_leaves_yields_no_edges() -> None:
    # An abstract DataPoint with zero registered leaves: the abstract-produces rule must
    # degrade to "produces nothing" — never match the abstract itself or unrelated leaves.
    class OrphanAbstractDataPoint(BaseDataPoint[str]):
        __abstract__ = True
        config = DataPointTypeConfig(pii=False, ephemeral=False)

    producer = make_operator('orphan_producer', produces={OrphanAbstractDataPoint})
    consumer = make_operator('orphan_consumer', depends_on={OrphanAbstractDataPoint})
    edges = build_graph([producer, consumer], [])

    assert effective_produces(producer) == set()
    assert consumer not in edges[producer]


def test_graph_13_capability_only_cycle_has_no_boundable_operator() -> None:
    # A cycle made of only capabilities has no operator to attach a max_cycles cap to, so
    # it can never be bounded and must be rejected at deploy time.
    provider = make_capability('cap_d')
    consumer = make_capability('cap_c', requires={provider})
    provider.requires = frozenset({consumer})  # type: ignore[misc]

    with pytest.raises(UnboundedCycleError) as excinfo:
        validate_acyclic_or_bounded(build_graph([], [provider, consumer]))

    message = str(excinfo.value)
    assert 'no boundable operator' in message
    assert provider.__name__ in message
    assert consumer.__name__ in message


def test_graph_14_pruned_gathering_set_drops_breaker_cap_for_broken_cycle() -> None:
    # S1: gating one operator out of the run-time gathering set breaks the cycle, so the
    # breaker recomputed on the pruned set must NOT carry a cap for the now-broken cycle.
    x = make_operator('s1_x', produces={IpDataPoint}, depends_on={RiskDataPoint}, max_cycles=2)
    y = make_operator('s1_y', produces={RiskDataPoint}, depends_on={IpDataPoint}, max_cycles=2)

    full_breaker = _build_breaker_for([x, y], [])
    assert full_breaker._caps == {OperatorId('s1_x'): 2, OperatorId('s1_y'): 2}

    # Namespace gating excludes Y; X no longer participates in any cycle on the pruned set.
    pruned_breaker = _build_breaker_for([x], [])
    assert pruned_breaker._caps == {}
    pruned_breaker.record_run(OperatorId('s1_x'))
    pruned_breaker.record_run(OperatorId('s1_x'))
    assert not pruned_breaker.is_tripped(OperatorId('s1_x'))  # no spurious trip without a cap


def test_graph_15_gating_capability_entry_leaves_no_dangling_breaker_cap() -> None:
    # S1 (capability-availability variant): the cycle enters through a capability the namespace
    # does not provide. With that capability pruned the cycle is gone, so its operator must
    # not keep a max_cycles cap that would never arm.
    cap = make_capability('s1_cap_needs_ip', depends_on={IpDataPoint})
    looped = make_operator('s1_op_needs_cap', produces={IpDataPoint}, requires={cap}, max_cycles=3)

    with_cap = _build_breaker_for([looped], [cap])
    assert with_cap._caps == {OperatorId('s1_op_needs_cap'): 3}

    # Capability unavailable for this namespace: pruned graph has no cycle, hence no cap.
    without_cap = _build_breaker_for([looped], [])
    assert without_cap._caps == {}
    without_cap.record_run(OperatorId('s1_op_needs_cap'))
    assert not without_cap.is_tripped(OperatorId('s1_op_needs_cap'))
