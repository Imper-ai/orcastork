"""REACHABILITY — backward-reachable operator/capability pruning from a sink (consumed) type set.

The closure: keep an operator iff its output is (transitively, subtype-aware) consumed toward the
sinks; keep a capability iff a kept node requires it. Operators producing only datapoints nothing
in the closure consumes are dropped, so they never run.
"""

from __future__ import annotations

from orcastork.graph import backward_reachable

from .doubles.capabilities import make_capability
from .doubles.datapoints import EmailDataPoint, IpDataPoint, RiskDataPoint, WorkEmailDataPoint
from .doubles.operators import make_operator


def test_keeps_operator_producing_a_sink_type() -> None:
    producer = make_operator('r_keep_direct', produces={IpDataPoint})
    assert producer in backward_reachable([producer], [], frozenset({IpDataPoint}))


def test_prunes_operator_whose_output_feeds_nothing() -> None:
    consumed = make_operator('r_consumed', produces={IpDataPoint})
    dead = make_operator('r_dead', produces={RiskDataPoint})  # nothing in the closure reads RiskDataPoint
    kept = backward_reachable([consumed, dead], [], frozenset({IpDataPoint}))
    assert consumed in kept
    assert dead not in kept


def test_keeps_transitive_upstream_chain() -> None:
    # sink = RiskDataPoint; detector produces Risk depends_on Ip; collector produces Ip; unrelated is dead.
    detector = make_operator('r_detector', produces={RiskDataPoint}, depends_on={IpDataPoint})
    collector = make_operator('r_collector', produces={IpDataPoint})
    unrelated = make_operator('r_unrelated', produces={EmailDataPoint})
    kept = backward_reachable([detector, collector, unrelated], [], frozenset({RiskDataPoint}))
    assert detector in kept and collector in kept
    assert unrelated not in kept


def test_sink_match_is_subtype_aware() -> None:
    # An abstract sink (EmailDataPoint) is satisfied by a concrete-leaf (WorkEmail) producer.
    producer = make_operator('r_subtype', produces={WorkEmailDataPoint})
    assert producer in backward_reachable([producer], [], frozenset({EmailDataPoint}))


def test_keeps_only_capabilities_required_by_kept_operators() -> None:
    cap_kept = make_capability('r_cap_kept')
    cap_dead = make_capability('r_cap_dead')
    keeper = make_operator('r_uses_cap', produces={IpDataPoint}, requires={cap_kept})
    dead = make_operator('r_dead_uses_cap', produces={RiskDataPoint}, requires={cap_dead})
    kept = backward_reachable([keeper, dead], [cap_kept, cap_dead], frozenset({IpDataPoint}))
    assert keeper in kept and cap_kept in kept
    assert dead not in kept and cap_dead not in kept


def test_keeps_transitively_layered_capability() -> None:
    base_cap = make_capability('r_cap_base')
    layer_cap = make_capability('r_cap_layer', requires={base_cap})
    op = make_operator('r_uses_layer', produces={IpDataPoint}, requires={layer_cap})
    kept = backward_reachable([op], [base_cap, layer_cap], frozenset({IpDataPoint}))
    assert {op, layer_cap, base_cap} <= kept


def test_empty_sinks_keeps_nothing() -> None:
    op = make_operator('r_anything', produces={IpDataPoint})
    assert backward_reachable([op], [], frozenset()) == frozenset()
