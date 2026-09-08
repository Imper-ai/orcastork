"""Read-only session introspection — stuck-session forensics from persisted state alone.

``describe_session`` answers "why is this session stuck / why this verdict" without driving
the engine: it reads only the persisted ports (store, lock, inbox, durable contribution
markers) plus the flow definition. It is **strictly read-only** — no epoch is minted and no
port is mutated — so it is safe to call at any time, including while a live orchestrator
owns the session.

Readiness and missing-dependency names are computed with the SAME pure functions the
scheduler runs (:func:`~orcastork.scheduling.is_ready`, subtype-aware, and
:func:`~orcastork.capabilities.availability.compute_available` over the namespace's
catalog permissions), so the description can never disagree with what the engine would do.
A capability that is registered in the flow but namespace-forbidden is simply never available, so
every operator requiring it reports that capability as missing.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .capabilities.availability import compute_available
from .capabilities.base import Capability
from .datapoints import BaseDataPoint
from .flow import FlowDefinition
from .ids import Epoch, NamespaceId, OperatorId, Revision, SessionId
from .operators import Aggregator, Operator
from .ports.change_set import QuarantinedEntry
from .runtime import OrchestratorRuntime
from .scheduling import is_ready

__all__ = ['OperatorState', 'SessionDescription', 'describe_session', 'render_text']


@dataclass(frozen=True)
class OperatorState:
    operator_id: OperatorId
    has_run: bool  # a persisted watermark exists (gathering operators advance one per completed run)
    watermark: Revision | None
    is_ready_now: bool  # the scheduler's readiness check over the current snapshot (gating aside)
    missing_data_points: tuple[str, ...]  # depends_on type names with no present (sub)type
    missing_capabilities: tuple[str, ...]  # requires type names with no available (sub)type
    is_gated: bool  # excluded by the namespace's permitted_operators — it can never launch for this namespace
    contribution_marked: bool | None  # aggregators only (their durable "ran" flag); None otherwise


@dataclass(frozen=True)
class SessionDescription:
    session_id: SessionId
    namespace_id: NamespaceId
    flow_name: str
    is_complete: bool
    is_owned: bool  # a live (un-expired) ownership lease exists right now
    current_epoch: Epoch
    revision: Revision
    deadline: datetime | None  # the persisted wall-clock session deadline (set at the first gather)
    stored_flow_fingerprint: str | None
    fingerprint_matches: bool
    pending_inbox: int
    quarantined: tuple[QuarantinedEntry, ...]
    present_types: Mapping[str, int]  # concrete DataPoint type name → count in the snapshot
    operators: tuple[OperatorState, ...]


def _missing_data_points(
    operator: type[Operator], present_types: frozenset[type[BaseDataPoint[Any]]]
) -> tuple[str, ...]:
    return tuple(
        sorted(
            required.__name__
            for required in operator.depends_on
            if not any(issubclass(present, required) for present in present_types)
        )
    )


def _missing_capabilities(operator: type[Operator], available_types: frozenset[type[Capability]]) -> tuple[str, ...]:
    return tuple(
        sorted(
            required.__name__
            for required in operator.requires
            if not any(issubclass(available, required) for available in available_types)
        )
    )


async def describe_session(
    runtime: OrchestratorRuntime,
    *,
    session_id: SessionId,
    namespace_id: NamespaceId,
    flow: FlowDefinition,
) -> SessionDescription:
    """Describe a session's persisted state against ``flow`` (strictly read-only)."""
    view = await runtime.store.snapshot(session_id)
    present_type_set = frozenset(type(data_point) for data_point in view.all())
    registered = {capability.capability_id: capability for capability in flow.capabilities}
    permitted = await runtime.catalog.permitted_capabilities(namespace_id)
    available_ids = compute_available(registered=registered, permitted=permitted, present_types=present_type_set)
    available_types = frozenset(registered[capability_id] for capability_id in available_ids)
    permitted_operators = await runtime.catalog.permitted_operators(namespace_id)

    operators: list[OperatorState] = []
    for operator in flow.operators:
        watermark = await runtime.store.get_watermark(session_id, operator.operator_id)
        contribution_marked = (
            await runtime.durable.is_contribution_marked(session_id, operator.operator_id)
            if issubclass(operator, Aggregator)
            else None
        )
        operators.append(
            OperatorState(
                operator_id=operator.operator_id,
                has_run=watermark is not None,
                watermark=watermark,
                is_ready_now=is_ready(
                    operator, present_types=present_type_set, available_capability_types=available_types
                ),
                missing_data_points=_missing_data_points(operator, present_type_set),
                missing_capabilities=_missing_capabilities(operator, available_types),
                is_gated=permitted_operators is not None and operator.operator_id not in permitted_operators,
                contribution_marked=contribution_marked,
            )
        )

    present_counts: dict[str, int] = {}
    for data_point in view.all():
        type_name = type(data_point).__name__
        present_counts[type_name] = present_counts.get(type_name, 0) + 1

    stored_fingerprint = await runtime.store.get_flow_fingerprint(session_id)
    return SessionDescription(
        session_id=session_id,
        namespace_id=namespace_id,
        flow_name=flow.name,
        is_complete=await runtime.lock.is_complete(session_id),
        is_owned=await runtime.lock.is_held(session_id),
        current_epoch=await runtime.lock.current_epoch(session_id),
        revision=await runtime.store.revision(session_id),
        deadline=await runtime.store.get_session_deadline(session_id),
        stored_flow_fingerprint=stored_fingerprint,
        # A session with no persisted fingerprint has nothing to drift from — the same reading
        # the orchestrator's drift detection takes when it persists the first fingerprint silently.
        fingerprint_matches=stored_fingerprint is None or stored_fingerprint == flow.fingerprint(),
        pending_inbox=await runtime.inbox.pending_count(session_id),
        quarantined=await runtime.inbox.quarantined(session_id),
        present_types=present_counts,
        operators=tuple(operators),
    )


def render_text(description: SessionDescription) -> str:
    """A compact, log-friendly rendering: a status line, then one line per not-yet-run operator."""
    status = 'complete' if description.is_complete else 'incomplete'
    ownership = 'owned' if description.is_owned else 'unowned'
    fingerprint = 'match' if description.fingerprint_matches else 'DRIFTED'
    lines = [
        f'session {description.session_id} (flow {description.flow_name}): {status}, {ownership}, '
        f'epoch={description.current_epoch}, revision={description.revision}, '
        f'pending_inbox={description.pending_inbox}, quarantined={len(description.quarantined)}, '
        f'fingerprint={fingerprint}'
    ]
    if description.deadline is not None:
        lines.append(f'deadline: {description.deadline.isoformat()}')
    present = ', '.join(f'{name}={count}' for name, count in sorted(description.present_types.items()))
    lines.append(f'present: {present if present else "(none)"}')
    # An aggregator never advances a watermark; its contribution marker is its "ran" flag.
    pending = [state for state in description.operators if not state.has_run and not state.contribution_marked]
    if not pending:
        lines.append('all operators have run')
        return '\n'.join(lines)
    lines.extend(f'- {state.operator_id}: {_describe_blockers(state)}' for state in pending)
    return '\n'.join(lines)


def _describe_blockers(state: OperatorState) -> str:
    blockers: list[str] = []
    if state.is_gated:
        blockers.append('gated for this namespace')
    if state.missing_data_points:
        blockers.append(f'missing data: {", ".join(state.missing_data_points)}')
    if state.missing_capabilities:
        blockers.append(f'missing capabilities: {", ".join(state.missing_capabilities)}')
    # No blockers means the readiness check passes — the operator simply has not launched yet
    # (e.g. the description was taken mid-run, or the session is parked/unowned).
    return '; '.join(blockers) if blockers else 'ready, not yet run'
