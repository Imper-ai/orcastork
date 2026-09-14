"""Session events — the live view of a running session, and the port they are published through.

``SessionState`` is private to the orchestrator, so without this nothing outside the process could
see a DataPoint before ``run()`` returned. The loop publishes one event per change — a batch of
DataPoints merged, an operator run finished, a capability activated, the session completed — through the
injected :class:`SessionEventSink`. The shipped sinks are a no-op (the default), an in-memory list
(tests) and a Redis stream (``adapters/redis.py``). A sink that raises is logged and the event is
dropped: the live view is a convenience, never a reason to wedge the session.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict

from .ids import CapabilityId, NamespaceId, OperatorId, SessionId


class _SessionEventBase(BaseModel):
    model_config = ConfigDict(frozen=True)

    session_id: SessionId
    namespace_id: NamespaceId
    at: datetime  # the session clock's ``now()`` when the change happened


class MergedDataPoint(BaseModel):
    """One DataPoint as it stands in the session after a merge.

    ``retrieved_by`` is the stored DataPoint's provenance — its first observer. An ``updated`` merge only
    advances ``last_retrieved``; the operator that re-observed the value is not recorded on the identity.
    """

    model_config = ConfigDict(frozen=True)

    data_point_type: str  # the DataPoint class name
    value: Any
    retrieved_by: OperatorId


class DataPointsMerged(_SessionEventBase):
    """One merge landed in the session: new identities (``added``) and fresher sightings (``updated``).

    A merge is one batch — the seed, or every emission drained from the queue in one pass — so a burst
    of DataPoints costs the sink one event (one round trip) rather than one per DataPoint.
    """

    kind: Literal['data_points_merged'] = 'data_points_merged'
    added: tuple[MergedDataPoint, ...]
    updated: tuple[MergedDataPoint, ...]
    revision: int  # the session revision the merge produced


class OperatorRunCompleted(_SessionEventBase):
    """One run of an operator finished, in whatever way."""

    kind: Literal['operator_run_completed'] = 'operator_run_completed'
    operator_id: OperatorId
    outcome: Literal['succeeded', 'failed', 'retrying', 'cancelled']
    attempt: int
    error: str | None = None


class CapabilityActivated(_SessionEventBase):
    """A capability became available, or its activation failed terminally for the session."""

    kind: Literal['capability_activated'] = 'capability_activated'
    capability_id: CapabilityId
    outcome: Literal['activated', 'failed']


class SessionCompleted(_SessionEventBase):
    """The session returned; the last event a sink sees for a session."""

    kind: Literal['session_completed'] = 'session_completed'
    deadline_hit: bool
    operator_runs: dict[OperatorId, int]
    failures: dict[OperatorId, str]


SessionEvent = DataPointsMerged | OperatorRunCompleted | CapabilityActivated | SessionCompleted


@runtime_checkable
class SessionEventSink(Protocol):
    """Where the orchestrator publishes session events. Must be fast or buffer internally."""

    async def publish(self, event: SessionEvent) -> None: ...


class NullSessionEventSink:
    """The default: events go nowhere, so a runtime that wires nothing loses nothing."""

    async def publish(self, _event: SessionEvent) -> None:
        return None
