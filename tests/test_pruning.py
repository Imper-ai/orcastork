"""PRUNING — the orchestrator runs only the backward-reachable closure of the aggregator's consumes.

When an aggregator declares ``consumes``, operators whose output nothing in that closure considers
must never run. With no ``consumes`` declared, every operator runs (backward-compatible).
"""

from __future__ import annotations

from orcastork.datapoints import DataPointView
from orcastork.ids import NamespaceId, OperatorId, SessionId
from orcastork.orchestrator import Orchestrator, SessionStatus
from orcastork.runtime import build_in_memory_runtime
from orcastork.scheduling import all_of, any_of, normalize_completion, referenced_types

from .doubles.clock import FakeClock
from .doubles.datapoints import EmailDataPoint, IpDataPoint, RiskDataPoint, ip, risk, work_email
from .doubles.operators import make_aggregator, make_operator

NAMESPACE = NamespaceId('prune-namespace')


def test_referenced_types_none_is_empty() -> None:
    assert referenced_types(None) == frozenset()


def test_referenced_types_unwraps_type_present() -> None:
    assert referenced_types(normalize_completion(IpDataPoint)) == frozenset({IpDataPoint})


def test_referenced_types_walks_nested_combinators() -> None:
    condition = all_of(IpDataPoint, any_of(RiskDataPoint, EmailDataPoint))
    assert referenced_types(condition) == frozenset({IpDataPoint, RiskDataPoint, EmailDataPoint})


def test_referenced_types_returns_none_for_opaque_custom_condition() -> None:
    # A custom CompletionCondition the AST can't introspect — the caller must treat this as
    # "types unknown" and refuse to prune rather than risk dropping a completion producer.
    class _Opaque:
        def is_satisfied(self, view: DataPointView) -> bool:  # noqa: ARG002
            return True

    assert referenced_types(_Opaque()) is None


def _useful() -> type:
    return make_operator('prune_useful', produces={IpDataPoint}, emits=[ip()])


def _detector() -> type:
    return make_operator('prune_detector', depends_on={IpDataPoint}, produces={RiskDataPoint}, emits=[risk()])


def _dead() -> type:
    # Produces a type nothing in the aggregator's closure consumes.
    return make_operator('prune_dead', produces={EmailDataPoint}, emits=[work_email()])


async def test_dead_operator_is_pruned_when_aggregator_declares_consumes() -> None:
    runtime = build_in_memory_runtime(FakeClock())
    useful, detector, dead = _useful(), _detector(), _dead()
    agg = make_aggregator('prune_agg', depends_on={RiskDataPoint}, consumes={RiskDataPoint})
    result = await Orchestrator(
        session_id=SessionId('prune-on'),
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[useful, detector, dead, agg],
        capabilities=[],
        seed=[],
    ).run()

    assert result.status is SessionStatus.COMPLETED
    assert result.operator_runs.get(OperatorId('prune_useful')) == 1  # feeds the detector → kept
    assert result.operator_runs.get(OperatorId('prune_detector')) == 1  # produces the consumed Risk → kept
    assert OperatorId('prune_dead') not in result.operator_runs  # output consumed by nothing → pruned


async def test_no_consumes_runs_every_operator_backward_compatible() -> None:
    runtime = build_in_memory_runtime(FakeClock())
    useful, detector, dead = _useful(), _detector(), _dead()
    agg = make_aggregator('compat_agg', depends_on={RiskDataPoint})  # consumes unset → no pruning
    result = await Orchestrator(
        session_id=SessionId('prune-off'),
        namespace_id=NAMESPACE,
        runtime=runtime,
        operators=[useful, detector, dead, agg],
        capabilities=[],
        seed=[],
    ).run()

    assert result.status is SessionStatus.COMPLETED
    assert result.operator_runs.get(OperatorId('prune_dead')) == 1  # nothing declared → dead still runs
