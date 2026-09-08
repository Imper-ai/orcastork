"""First-class flow definitions.

A ``FlowDefinition`` names a flow once — its operators, capabilities, completion condition,
retry/parking policy and orchestrator tuning — so the manager's ``start_session`` /
``resume`` / ``deliver`` all drive the *same* definition instead of re-threading loose
kwargs whose docstrings could only ask callers to keep them consistent across calls.

Its :meth:`FlowDefinition.fingerprint` is a stable digest of the flow's **graph-shape
identity**. The orchestrator persists it per session and flags a resume whose flow no longer
matches (``AuditKind.FLOW_DRIFT_DETECTED``) — so a deploy that changes the operator set can
never *silently* change quiescence/graph semantics mid-session.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

from .aggregation.retry import RetryPolicy
from .capabilities.base import Capability
from .datapoints import BaseDataPoint
from .operators.base import Operator
from .scheduling import CompletionCondition, describe_condition, normalize_completion

__all__ = ['FlowDefinition', 'FlowIdentity']


@dataclass(frozen=True)
class FlowIdentity:
    """What the orchestrator needs for drift detection: the flow's name + fingerprint."""

    name: str
    fingerprint: str


def _data_point_names(types: frozenset[type[BaseDataPoint[Any]]]) -> str:
    return ','.join(sorted(data_point_type.__qualname__ for data_point_type in types))


def _capability_name(capability: type[Capability]) -> str:
    # A concrete capability is named by its registry identity; an abstract intermediate (no id)
    # falls back to its qualified class name. Both are stable across processes — never an
    # `id()`-derived default repr.
    return str(capability.capability_id) if hasattr(capability, 'capability_id') else capability.__qualname__


def _capability_names(types: frozenset[type[Capability]]) -> str:
    return ','.join(sorted(_capability_name(capability) for capability in types))


@dataclass(frozen=True)
class FlowDefinition:
    name: str
    operators: tuple[type[Operator], ...]
    capabilities: tuple[type[Capability], ...] = ()
    completes_when: type[BaseDataPoint[Any]] | CompletionCondition | None = None
    retry_policy: RetryPolicy | None = None
    park_after: float | None = None
    # Orchestrator tuning the flow may pin; ``None`` falls back to the orchestrator's defaults.
    operation_timeout: float | None = None
    session_deadline: float | None = None
    max_inbox_deliveries: int | None = None
    emission_queue_size: int | None = None

    def identity(self) -> FlowIdentity:
        return FlowIdentity(name=self.name, fingerprint=self.fingerprint())

    def fingerprint(self) -> str:
        """A stable sha256 hexdigest of the flow's graph-shape identity.

        Covers exactly what changes the scheduler's graph reasoning: each operator's id, its
        declared ``depends_on``/``produces``/``requires`` and the policy knobs that affect
        scheduling semantics (``rerun_on_new_data``, ``rerun_on``, ``max_cycles``); each
        capability's id and declarations; and the completion condition's canonical text.
        Every section is sorted, so declaration order never matters, and every name is a
        registry id or a qualified class name, so the digest is identical across processes.

        Deliberately NOT covered: the flow name, retries/parking/tuning (runtime behavior,
        not graph shape) and per-namespace operator gating — gating is runtime configuration the
        catalog applies per grant, not flow code identity, so a namespace config change must never
        read as flow drift.
        """
        lines: list[str] = []
        for operator in sorted(self.operators, key=lambda declared: declared.operator_id):
            policy = operator.policy
            lines.append(
                f'operator {operator.operator_id}'
                f' depends_on=[{_data_point_names(operator.depends_on)}]'
                f' produces=[{_data_point_names(operator.produces)}]'
                f' requires=[{_capability_names(operator.requires)}]'
                f' rerun_on_new_data={policy.rerun_on_new_data}'
                f' rerun_on={policy.rerun_on.value}'
                f' max_cycles={policy.max_cycles}'
            )
        for capability in sorted(self.capabilities, key=_capability_name):
            lines.append(
                f'capability {_capability_name(capability)}'
                f' depends_on=[{_data_point_names(capability.depends_on)}]'
                f' requires=[{_capability_names(capability.requires)}]'
            )
        condition = normalize_completion(self.completes_when)
        lines.append(f'completes_when {"None" if condition is None else describe_condition(condition)}')
        return hashlib.sha256('\n'.join(lines).encode()).hexdigest()
