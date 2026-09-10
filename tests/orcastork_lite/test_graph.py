"""The static graph: edges, cycles, the bounded-cycle rule, permitted subgraphs, backward reachability."""

from __future__ import annotations

import pytest

from orcastork_lite import UnboundedCycleError
from orcastork_lite.graph import (
    backward_reachable,
    build_edges,
    build_uses_edges,
    cycle_caps,
    find_cycles,
    restrict_to_permitted,
    validate_acyclic_or_bounded,
)
from orcastork_lite.ids import CapabilityId

from .conftest import Email, Flag, Ip, Risk, WorkEmail, make_capability, make_operator


def test_edges_follow_produces_to_depends_on_in_both_subtype_directions() -> None:
    abstract_producer = make_operator('abstract', produces={Email})
    leaf_producer = make_operator('leaf', produces={WorkEmail})
    leaf_consumer = make_operator('leaf_consumer', depends_on={WorkEmail})
    abstract_consumer = make_operator('abstract_consumer', depends_on={Email})
    unrelated = make_operator('unrelated', depends_on={Ip})

    edges = build_edges([abstract_producer, leaf_producer, leaf_consumer, abstract_consumer, unrelated], [])
    assert edges[abstract_producer] == {leaf_consumer, abstract_consumer}
    assert edges[leaf_producer] == {leaf_consumer, abstract_consumer}
    assert edges[unrelated] == set()


def test_capability_edges_feed_requirers_and_layered_capabilities() -> None:
    base = make_capability('base')
    layer = make_capability('layer', requires={base})
    user = make_operator('user', requires={layer})

    edges = build_edges([user], [base, layer])
    assert edges[base] == {layer} and edges[layer] == {user}


def test_uses_edges_are_separate_and_drop_self_edges() -> None:
    producer = make_operator('producer', produces={Risk})
    folder = make_operator('folder', depends_on={Ip}, uses={Risk}, produces={Risk})
    assert build_uses_edges([producer, folder]) == {producer: {folder}, folder: set()}
    assert build_edges([producer, folder], [])[producer] == set()  # `uses` is not a readiness edge


def test_unbounded_cycle_raises_and_bounded_cycle_yields_caps() -> None:
    a = make_operator('a', depends_on={Ip}, produces={Risk})
    b = make_operator('b', depends_on={Risk}, produces={Ip})
    with pytest.raises(UnboundedCycleError):
        validate_acyclic_or_bounded(build_edges([a, b], []))

    a_bounded = make_operator('a', depends_on={Ip}, produces={Risk}, max_cycles=3)
    b_bounded = make_operator('b', depends_on={Risk}, produces={Ip}, max_cycles=5)
    edges = build_edges([a_bounded, b_bounded], [])
    validate_acyclic_or_bounded(edges)
    assert find_cycles(edges) == [frozenset({a_bounded, b_bounded})]
    assert cycle_caps(edges) == {'a': 3, 'b': 5}
    assert cycle_caps(build_edges([make_operator('lonely', depends_on={Ip}, produces={Risk})], [])) == {}


def test_self_loop_is_a_cycle_and_capability_only_cycle_is_unboundable() -> None:
    selfie = make_operator('selfie', depends_on={Ip}, produces={Ip}, max_cycles=2)
    assert find_cycles(build_edges([selfie], [])) == [frozenset({selfie})]

    x = make_capability('x')
    y = make_capability('y', requires={x})
    x.requires = frozenset({y})
    with pytest.raises(UnboundedCycleError, match='no boundable operator'):
        validate_acyclic_or_bounded(build_edges([], [x, y]))


def test_restrict_to_permitted_drops_capability_nodes_only() -> None:
    cap = make_capability('cap')
    user = make_operator('user', requires={cap})
    edges = build_edges([user], [cap])
    restricted = restrict_to_permitted(edges, frozenset())
    assert set(restricted) == {user} and restricted[user] == set()
    assert restrict_to_permitted(edges, frozenset({CapabilityId('cap')})) == edges


def test_backward_reachable_keeps_upstream_chain_and_required_capabilities() -> None:
    cap = make_capability('cap')
    root = make_operator('root', depends_on={Flag}, produces={Ip}, requires={cap})
    mid = make_operator('mid', depends_on={Ip}, produces={Risk})
    dead = make_operator('dead', depends_on={Ip}, produces={WorkEmail})
    assert backward_reachable([root, mid, dead], [cap], frozenset({Risk})) == {root, mid, cap}
    assert backward_reachable([root, mid, dead], [cap], frozenset()) == frozenset()
