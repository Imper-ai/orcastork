"""FLOW — FlowDefinition: one named definition per flow + a stable graph-shape fingerprint."""

from __future__ import annotations

import re
from abc import abstractmethod
from typing import Any

from orcastork.aggregation import RetryPolicy
from orcastork.capabilities import Capability
from orcastork.flow import FlowDefinition, _capability_name
from orcastork.ids import OperatorId
from orcastork.operators import Operator, RerunOn
from orcastork.scheduling import TypePresent, all_of, any_of, describe_condition

from .doubles.capabilities import make_capability
from .doubles.datapoints import ChatAnswerDataPoint, EmailDataPoint, IpDataPoint, RiskDataPoint
from .doubles.operators import make_operator


def _redefined(operator_id: str, **kwargs: Any) -> type[Operator]:
    """Re-register ``operator_id`` with new declarations — the deploy-changed-the-operator case."""
    Operator._registry.pop(OperatorId(operator_id), None)  # noqa: SLF001
    return make_operator(operator_id, **kwargs)


def test_flow_fingerprint_is_a_stable_sha256_hexdigest() -> None:
    flow = FlowDefinition(name='f', operators=(make_operator('op', depends_on={EmailDataPoint}),))
    assert flow.fingerprint() == flow.fingerprint()
    assert re.fullmatch(r'[0-9a-f]{64}', flow.fingerprint())


def test_flow_fingerprint_is_declaration_order_insensitive() -> None:
    op_a = make_operator('a', depends_on={EmailDataPoint}, produces={RiskDataPoint})
    op_b = make_operator('b', depends_on={RiskDataPoint}, produces={IpDataPoint})
    cap_a = make_capability('cap-a', depends_on={EmailDataPoint})
    cap_b = make_capability('cap-b', depends_on={IpDataPoint})
    forward = FlowDefinition(name='f', operators=(op_a, op_b), capabilities=(cap_a, cap_b))
    backward = FlowDefinition(name='f', operators=(op_b, op_a), capabilities=(cap_b, cap_a))
    assert forward.fingerprint() == backward.fingerprint()


def test_flow_fingerprint_changes_when_an_operator_is_added_or_removed() -> None:
    op_a = make_operator('a', depends_on={EmailDataPoint})
    op_b = make_operator('b', depends_on={RiskDataPoint})
    smaller = FlowDefinition(name='f', operators=(op_a,))
    larger = FlowDefinition(name='f', operators=(op_a, op_b))
    assert smaller.fingerprint() != larger.fingerprint()


def test_flow_fingerprint_changes_when_a_dependency_changes() -> None:
    original = FlowDefinition(name='f', operators=(make_operator('op', depends_on={EmailDataPoint}),)).fingerprint()
    redeployed = FlowDefinition(name='f', operators=(_redefined('op', depends_on={IpDataPoint}),)).fingerprint()
    assert original != redeployed


def test_flow_fingerprint_changes_when_a_scheduling_policy_knob_changes() -> None:
    variants = [
        FlowDefinition(
            name='f', operators=(_redefined('op', depends_on={IpDataPoint}, rerun_on_new_data=False),)
        ).fingerprint(),
        FlowDefinition(
            name='f', operators=(_redefined('op', depends_on={IpDataPoint}, rerun_on_new_data=True),)
        ).fingerprint(),
        FlowDefinition(
            name='f',
            operators=(
                _redefined('op', depends_on={IpDataPoint}, rerun_on_new_data=True, rerun_on=RerunOn.ADDED_ONLY),
            ),
        ).fingerprint(),
        FlowDefinition(
            name='f',
            operators=(_redefined('op', depends_on={IpDataPoint}, rerun_on_new_data=True, max_cycles=3),),
        ).fingerprint(),
    ]
    assert len(set(variants)) == len(variants)  # every scheduling knob is part of the identity


def test_flow_fingerprint_ignores_runtime_configuration() -> None:
    operator = make_operator('op', depends_on={EmailDataPoint})
    plain = FlowDefinition(name='f', operators=(operator,))
    tuned = FlowDefinition(
        name='renamed',  # the name identifies the flow to humans, not to the graph
        operators=(operator,),
        retry_policy=RetryPolicy(max_attempts=2, base_delay=0.0),
        park_after=20.0,
        operation_timeout=1.0,
        session_deadline=60.0,
        max_inbox_deliveries=2,
        emission_queue_size=16,
    )
    assert plain.fingerprint() == tuned.fingerprint()


def test_flow_fingerprint_changes_with_capability_declarations() -> None:
    operator = make_operator('op', depends_on={EmailDataPoint})
    cap_a = make_capability('cap-a', depends_on={EmailDataPoint})
    cap_b = make_capability('cap-b', depends_on={IpDataPoint})
    one = FlowDefinition(name='f', operators=(operator,), capabilities=(cap_a,))
    two = FlowDefinition(name='f', operators=(operator,), capabilities=(cap_a, cap_b))
    assert one.fingerprint() != two.fingerprint()


def test_flow_fingerprint_normalizes_bare_completion_types() -> None:
    operator = make_operator('op', depends_on={EmailDataPoint})
    bare = FlowDefinition(name='f', operators=(operator,), completes_when=RiskDataPoint)
    explicit = FlowDefinition(name='f', operators=(operator,), completes_when=TypePresent(RiskDataPoint))
    unconditioned = FlowDefinition(name='f', operators=(operator,))
    assert bare.fingerprint() == explicit.fingerprint()  # the shorthand and the AST are the same condition
    assert bare.fingerprint() != unconditioned.fingerprint()


def test_flow_fingerprint_distinguishes_completion_conditions() -> None:
    operator = make_operator('op', depends_on={EmailDataPoint})
    conjunction = FlowDefinition(
        name='f', operators=(operator,), completes_when=all_of(RiskDataPoint, ChatAnswerDataPoint)
    )
    disjunction = FlowDefinition(
        name='f', operators=(operator,), completes_when=any_of(RiskDataPoint, ChatAnswerDataPoint)
    )
    assert conjunction.fingerprint() != disjunction.fingerprint()


def test_condition_describe_renders_class_names_without_object_reprs() -> None:
    condition = all_of(RiskDataPoint, any_of(ChatAnswerDataPoint, TypePresent(EmailDataPoint)))
    text = describe_condition(condition)
    assert text == (
        'AllOf(TypePresent(RiskDataPoint), AnyOf(TypePresent(ChatAnswerDataPoint), TypePresent(EmailDataPoint)))'
    )
    assert '<class' not in text and ' at 0x' not in text  # no class-object or default-instance reprs


def test_flow_identity_carries_name_and_fingerprint() -> None:
    flow = FlowDefinition(name='sample-flow', operators=(make_operator('op'),))
    identity = flow.identity()
    assert identity.name == 'sample-flow'
    assert identity.fingerprint == flow.fingerprint()


def test_flow_fingerprint_changes_when_an_operator_requires_a_different_capability() -> None:
    # The capability *family* an operator consumes is part of the graph-shape identity: a deploy that
    # rewires `requires=` while the operator id/depends_on/produces stay identical must change the digest
    # (so a resume audits FLOW_DRIFT_DETECTED), and two flows differing only in `requires` must not collide.
    cap_a = make_capability('cap-a', depends_on={EmailDataPoint})
    cap_b = make_capability('cap-b', depends_on={IpDataPoint})
    # Both capabilities are registered in BOTH flows, so the capability section is identical — the ONLY
    # difference is which capability family the operator declares it `requires`. This isolates flow.py:91.
    caps = (cap_a, cap_b)
    consumes_a = FlowDefinition(
        name='f', operators=(_redefined('op', depends_on={EmailDataPoint}, requires={cap_a}),), capabilities=caps
    ).fingerprint()
    consumes_a_again = FlowDefinition(
        name='f', operators=(_redefined('op', depends_on={EmailDataPoint}, requires={cap_a}),), capabilities=caps
    ).fingerprint()
    consumes_b = FlowDefinition(
        name='f', operators=(_redefined('op', depends_on={EmailDataPoint}, requires={cap_b}),), capabilities=caps
    ).fingerprint()
    assert consumes_a == consumes_a_again  # same `requires` → stable digest, every process
    assert consumes_a != consumes_b  # the only difference is the consumed capability family


def test_flow_fingerprint_changes_when_a_capabilitys_own_wiring_changes() -> None:
    # A capability's own depends_on/requires are part of the graph shape too: changing only a
    # capability's declarations (operators untouched) must still move the digest.
    leaf_a = make_capability('leaf-a', depends_on={EmailDataPoint})
    leaf_b = make_capability('leaf-b', depends_on={EmailDataPoint})
    operator = make_operator('op', depends_on={EmailDataPoint})

    depends_on_email = make_capability('wired', depends_on={EmailDataPoint})
    depends_on_email_fp = FlowDefinition(
        name='f', operators=(operator,), capabilities=(depends_on_email,)
    ).fingerprint()

    Capability._registry.pop(depends_on_email.capability_id, None)  # noqa: SLF001
    depends_on_ip = make_capability('wired', depends_on={IpDataPoint})  # only the capability's depends_on changed
    depends_on_ip_fp = FlowDefinition(name='f', operators=(operator,), capabilities=(depends_on_ip,)).fingerprint()
    assert depends_on_email_fp != depends_on_ip_fp  # a changed capability depends_on is drift

    requires_a = make_capability('wired2', depends_on={EmailDataPoint}, requires={leaf_a})
    requires_a_fp = FlowDefinition(
        name='f', operators=(operator,), capabilities=(requires_a, leaf_a, leaf_b)
    ).fingerprint()

    Capability._registry.pop(requires_a.capability_id, None)  # noqa: SLF001
    requires_b = make_capability('wired2', depends_on={EmailDataPoint}, requires={leaf_b})  # only requires changed
    requires_b_fp = FlowDefinition(
        name='f', operators=(operator,), capabilities=(requires_b, leaf_a, leaf_b)
    ).fingerprint()
    assert requires_a_fp != requires_b_fp  # a changed capability `requires` is drift too


def test_flow_fingerprint_names_an_abstract_capability_by_qualname_stably() -> None:
    # An abstract intermediate (an ABC with no capability_id, deliberately unregistered) must contribute a
    # stable name via __qualname__ — never an id()-derived per-process repr — so a cross-pod resume of a
    # flow listing it never falsely reads as drift.
    class AbstractProvider(Capability):
        @abstractmethod
        async def fetch(self) -> None: ...

    assert not hasattr(AbstractProvider, 'capability_id')  # the __qualname__ fallback branch is the one exercised
    operator = make_operator('op', depends_on={EmailDataPoint})
    first = FlowDefinition(name='f', operators=(operator,), capabilities=(AbstractProvider,))
    second = FlowDefinition(name='f', operators=(operator,), capabilities=(AbstractProvider,))
    assert first.fingerprint() == second.fingerprint()  # two instances ⇒ identical (cross-process stability)

    # Passing an abstract intermediate is exactly the case under test — the no-id `__qualname__` fallback.
    fallback_name = _capability_name(AbstractProvider)  # type: ignore[type-abstract]
    assert fallback_name.endswith('AbstractProvider')  # named by its qualified class name, not an id()-derived repr
    assert '0x' not in fallback_name and '<class' not in fallback_name  # never an id()/class-object repr
    # Including the abstract capability must actually move the digest off the no-capability baseline, proving
    # the fallback name is what reached the hashed lines (not silently dropped).
    baseline = FlowDefinition(name='f', operators=(operator,)).fingerprint()
    assert first.fingerprint() != baseline
