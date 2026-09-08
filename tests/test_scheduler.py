"""SCHED — data-driven readiness and graph-aware quiescence."""

from __future__ import annotations

from orcastork.capabilities import Capability
from orcastork.scheduling import (
    is_quiescent,
    is_ready,
    reachable_pending,
    readiness_gap,
    ready_operators,
)

from .doubles.capabilities import make_capability
from .doubles.datapoints import (
    ChatAnswerDataPoint,
    EmailDataPoint,
    IpDataPoint,
    RiskDataPoint,
    WorkEmailDataPoint,
)
from .doubles.operators import make_operator

NO_CAPS: frozenset[type[Capability]] = frozenset()


def test_sched_01_ready_iff_deps_present_and_caps_available() -> None:
    cap = make_capability('idp')
    operator = make_operator('o', depends_on={WorkEmailDataPoint}, requires={cap})
    assert not is_ready(operator, present_types=frozenset(), available_capability_types=NO_CAPS)
    assert not is_ready(operator, present_types=frozenset({WorkEmailDataPoint}), available_capability_types=NO_CAPS)
    assert is_ready(
        operator, present_types=frozenset({WorkEmailDataPoint}), available_capability_types=frozenset({cap})
    )


def test_sched_02_last_missing_dependency_triggers_immediately() -> None:
    operator = make_operator('o', depends_on={WorkEmailDataPoint, IpDataPoint})
    assert not is_ready(operator, present_types=frozenset({WorkEmailDataPoint}), available_capability_types=NO_CAPS)
    assert is_ready(
        operator, present_types=frozenset({WorkEmailDataPoint, IpDataPoint}), available_capability_types=NO_CAPS
    )


def test_sched_03_quiescent_when_no_producible_input_path() -> None:
    blocked = make_operator('needs_risk', depends_on={RiskDataPoint})  # nothing produces Risk
    assert is_quiescent([blocked], present_types=frozenset(), available_capability_types=NO_CAPS)


def test_sched_04_empty_depends_on_ready_at_start() -> None:
    seed_only = make_operator('seed', depends_on=set())
    assert is_ready(seed_only, present_types=frozenset(), available_capability_types=NO_CAPS)


def test_sched_05_subtype_satisfies_base_dependency() -> None:
    operator = make_operator('needs_email', depends_on={EmailDataPoint})
    assert is_ready(operator, present_types=frozenset({WorkEmailDataPoint}), available_capability_types=NO_CAPS)


def test_sched_06_all_simultaneously_ready_operators_are_returned() -> None:
    operators = [make_operator(f'seed_{index}') for index in range(3)]
    ready = ready_operators(operators, present_types=frozenset(), available_capability_types=NO_CAPS)
    assert len(ready) == 3


def test_sched_07_operator_waits_until_capability_online() -> None:
    cap = make_capability('idp')
    operator = make_operator('needs_cap', requires={cap})
    assert not is_ready(operator, present_types=frozenset(), available_capability_types=NO_CAPS)
    assert is_ready(operator, present_types=frozenset(), available_capability_types=frozenset({cap}))


def test_sched_08_not_quiescent_while_a_satisfiable_path_remains() -> None:
    producer = make_operator('produces_ip', produces={IpDataPoint})  # empty deps → runnable now
    consumer = make_operator('needs_ip', depends_on={IpDataPoint})
    assert not is_quiescent([producer, consumer], present_types=frozenset(), available_capability_types=NO_CAPS)


def test_sched_09_quiescent_when_remaining_permanently_unsatisfiable() -> None:
    consumer = make_operator('needs_ip', depends_on={IpDataPoint})  # no producer present or pending
    assert is_quiescent([consumer], present_types=frozenset(), available_capability_types=NO_CAPS)


def test_sched_10_emission_retriggers_readiness() -> None:
    consumer = make_operator('needs_ip', depends_on={IpDataPoint})
    assert not is_ready(consumer, present_types=frozenset(), available_capability_types=NO_CAPS)
    # An upstream operator emitted an IpDataPoint → readiness re-evaluates to ready.
    assert is_ready(consumer, present_types=frozenset({IpDataPoint}), available_capability_types=NO_CAPS)


def test_sched_11_inbox_arrival_retriggers_readiness() -> None:
    scorer = make_operator('scores_chat', depends_on={ChatAnswerDataPoint}, rerun_on_new_data=True)
    assert not is_ready(scorer, present_types=frozenset(), available_capability_types=NO_CAPS)
    # A chat answer arrived via the inbox and was merged → readiness re-evaluates to ready.
    assert is_ready(scorer, present_types=frozenset({ChatAnswerDataPoint}), available_capability_types=NO_CAPS)


def test_sched_12_reachable_pending_names_the_operators_behind_quiescence() -> None:
    producer = make_operator('produces_ip', produces={IpDataPoint})  # empty deps → runnable now
    consumer = make_operator('needs_ip', depends_on={IpDataPoint})  # reachable through the producer
    orphan = make_operator('needs_chat', depends_on={ChatAnswerDataPoint})  # nothing can ever produce this
    reachable = reachable_pending(
        [producer, consumer, orphan], present_types=frozenset(), available_capability_types=NO_CAPS
    )
    assert reachable == frozenset({producer, consumer})
    assert is_quiescent([orphan], present_types=frozenset(), available_capability_types=NO_CAPS)


def test_sched_13_reachable_pending_respects_capability_gates() -> None:
    cap = make_capability('idp')
    gated = make_operator('gated', requires={cap})
    assert reachable_pending([gated], present_types=frozenset(), available_capability_types=NO_CAPS) == frozenset()
    assert reachable_pending(
        [gated], present_types=frozenset(), available_capability_types=frozenset({cap})
    ) == frozenset({gated})


def test_sched_14_readiness_gap_names_the_missing_inputs() -> None:
    # "Never ran" and "ran and found nothing" are indistinguishable without this, and downstream that
    # is the difference between "not checked" and "checked, clean".
    cap = make_capability('idp')
    operator = make_operator('o', depends_on={WorkEmailDataPoint, IpDataPoint}, requires={cap})

    gap = readiness_gap(operator, present_types=frozenset({IpDataPoint}), available_capability_types=NO_CAPS)

    assert not gap.is_ready
    assert gap.missing_data_points == (WorkEmailDataPoint,)
    assert gap.missing_capabilities == (cap,)


def test_sched_15_readiness_gap_is_empty_when_ready() -> None:
    cap = make_capability('idp')
    operator = make_operator('o', depends_on={WorkEmailDataPoint}, requires={cap})

    gap = readiness_gap(
        operator, present_types=frozenset({WorkEmailDataPoint}), available_capability_types=frozenset({cap})
    )

    assert gap.is_ready
    assert gap.missing_data_points == ()
    assert gap.missing_capabilities == ()


def test_sched_16_readiness_gap_is_subtype_aware_like_the_gate() -> None:
    operator = make_operator('needs_email', depends_on={EmailDataPoint})

    gap = readiness_gap(operator, present_types=frozenset({WorkEmailDataPoint}), available_capability_types=NO_CAPS)

    assert gap.missing_data_points == ()


def test_sched_17_readiness_gap_never_disagrees_with_is_ready() -> None:
    """The explanation and the decision must not drift: is_ready is defined in terms of the gap, and
    this pins that for every combination of satisfied/unsatisfied data and capability inputs."""
    cap = make_capability('idp')
    operator = make_operator('o', depends_on={WorkEmailDataPoint, IpDataPoint}, requires={cap})
    for present in (frozenset(), frozenset({IpDataPoint}), frozenset({WorkEmailDataPoint, IpDataPoint})):
        for caps in (NO_CAPS, frozenset({cap})):
            gap = readiness_gap(operator, present_types=present, available_capability_types=caps)
            assert gap.is_ready == is_ready(operator, present_types=present, available_capability_types=caps), (
                f'disagreement for present={present} caps={caps}'
            )


def test_sched_18_readiness_gap_ordering_is_stable() -> None:
    # The gap is stored in a report, so it has to diff cleanly rather than following set iteration order.
    operator = make_operator('o', depends_on={WorkEmailDataPoint, IpDataPoint, RiskDataPoint})

    gap = readiness_gap(operator, present_types=frozenset(), available_capability_types=NO_CAPS)

    assert [leaf.__name__ for leaf in gap.missing_data_points] == sorted(
        leaf.__name__ for leaf in (WorkEmailDataPoint, IpDataPoint, RiskDataPoint)
    )
