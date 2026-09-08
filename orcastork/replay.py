"""Session replay — re-run a flow over a previously-archived session's raw DataPoints.

The archive (the second durable write path) persists every non-ephemeral DataPoint of a
session as an :class:`ArchivedDataPoint`. :func:`replay_session` makes the promise behind
it executable: reconstruct the raw DataPoints from those documents and drive a (possibly
different/newer) flow over them on a **fresh in-memory runtime** — re-deriving aggregates
and reprocessing raw signals without re-running collection. Use it for regression-testing
a graph change against a recorded session, what-if analysis, and computing new aggregates
that did not exist when the session originally ran (the framework's analog of Temporal
replay testing / Flink savepoint reprocessing).

Like :mod:`.runtime`, this module is a wiring seam: replay always runs on a fresh,
isolated in-memory substrate, so it deliberately builds on the in-memory adapters.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from .adapters.memory import InMemoryCapabilityCatalog
from .archive import ArchivedDataPoint
from .clock import Clock
from .datapoints import BaseDataPoint, parse_data_point
from .exceptions import ReplayError
from .flow import FlowDefinition
from .ids import NamespaceId, SessionId
from .orchestrator import Orchestrator, OrchestratorResult
from .ports import CapabilityCatalog
from .runtime import OrchestratorRuntime, build_in_memory_runtime

__all__ = ['ReplayResult', 'replay_session']


@dataclass(frozen=True)
class ReplayResult:
    """What a replay produced.

    ``runtime`` is the fresh in-memory runtime the replay ran on, so callers can inspect
    the durable store / audit / archive ports directly; ``data_points`` is the final
    store snapshot (the reconstructed seed plus everything the replayed flow derived).
    """

    result: OrchestratorResult
    runtime: OrchestratorRuntime
    data_points: tuple[BaseDataPoint[Any], ...]


def _reconstruct(entry: ArchivedDataPoint) -> BaseDataPoint[Any]:
    # Registry-driven: the discriminated union picks the concrete leaf, so the rebuilt DataPoint
    # carries its real class (subtype-aware queries, pii/ephemeral config) — not a generic shell.
    return parse_data_point(
        {
            'type': entry.type,
            'value': entry.value,
            'retrieved_by': entry.retrieved_by,
            'first_retrieved': entry.first_retrieved,
            'last_retrieved': entry.last_retrieved,
        }
    )


async def replay_session(
    archived: Iterable[ArchivedDataPoint],
    *,
    flow: FlowDefinition,
    session_id: SessionId | None = None,
    namespace_id: NamespaceId | None = None,
    catalog: CapabilityCatalog | None = None,
    clock: Clock | None = None,
) -> ReplayResult:
    """Reconstruct an archived session's raw DataPoints and re-run ``flow`` over them.

    Each entry is rebuilt into its concrete DataPoint leaf via the registry (provenance and
    first/last_retrieved timestamps preserved) and used as the seed of a fresh
    :class:`Orchestrator` on a fresh in-memory runtime — collection is not re-run; the data
    arrives as it was recorded. The flow may differ from the one that produced the archive
    (new operators, new aggregators, removed operators).

    Values are passed through exactly as stored: the caller is responsible for unsealing
    PII values before replay (the in-memory archive's ``read`` already returns plaintext;
    entries pulled from a production backend must be unsealed with that backend's cipher).

    ``session_id`` / ``namespace_id`` default to those of the first archived entry.

    Raises:
        UnknownDataPointTypeError: an entry's ``type`` has no registered leaf in the
            current code — a meaningful replay failure (the flow no longer ships that
            DataPoint), deliberately not skipped.
        ReplayError: ``archived`` is empty and no explicit session/namespace ids were
            supplied, so there is nothing to derive them from.
        pydantic.ValidationError: an entry's ``type`` is still a registered leaf but its
            archived ``value`` no longer matches that leaf's schema (value-shape drift after
            a flow change) — the real validation error is preserved rather than re-wrapped,
            so replay-as-regression-testing surfaces schema drift with its precise cause.
    """
    entries = tuple(archived)
    if not entries and (session_id is None or namespace_id is None):
        raise ReplayError('cannot replay an empty archive without explicit ids — pass session_id and namespace_id')
    resolved_session = session_id if session_id is not None else entries[0].session_id
    resolved_namespace = namespace_id if namespace_id is not None else entries[0].namespace_id
    seed = tuple(_reconstruct(entry) for entry in entries)

    if catalog is None:
        # Replay reprocesses data that already arrived, so every flow capability is permitted
        # rather than re-checked against live namespace entitlements; credentials are empty because a
        # replayed flow should not need a live backend.
        permitted = frozenset(capability.capability_id for capability in flow.capabilities)
        catalog = InMemoryCapabilityCatalog(
            permitted={resolved_namespace: permitted},
            credentials={(resolved_namespace, capability_id): {} for capability_id in permitted},
        )
    runtime = build_in_memory_runtime(clock, catalog=catalog)

    result = await Orchestrator(
        session_id=resolved_session,
        namespace_id=resolved_namespace,
        runtime=runtime,
        operators=flow.operators,
        capabilities=flow.capabilities,
        seed=seed,
        completes_when=flow.completes_when,
        retry_policy=flow.retry_policy,
        park_after=flow.park_after,
        operation_timeout=flow.operation_timeout,
        session_deadline=flow.session_deadline,
        max_inbox_deliveries=flow.max_inbox_deliveries,
        emission_queue_size=flow.emission_queue_size,
        flow_identity=flow.identity(),
    ).run()
    snapshot = await runtime.store.snapshot(resolved_session)
    return ReplayResult(result=result, runtime=runtime, data_points=snapshot.all())
