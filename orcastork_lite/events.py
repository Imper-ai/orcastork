"""Session events — the live view of a running session, and the port they are published through.

``SessionState`` is private to the orchestrator, so without this nothing outside the process could
see a DataPoint before ``run()`` returned. The loop publishes one event per change — a DataPoint
merged, an operator run finished, a capability activated, the session completed — through the
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


class DataPointMerged(_SessionEventBase):
    """A DataPoint landed in the session: a new identity (``added``) or a fresher sighting (``updated``).

    One event per DataPoint on purpose: a stream entry is the unit a consumer filters and acts on, so
    "react the moment this DataPoint lands" needs no unpacking of a batch. ``retrieved_by`` is the stored
    DataPoint's provenance — its first observer. An ``updated`` merge only advances ``last_retrieved``; the
    operator that re-observed the value is not recorded on the identity.
    """

    kind: Literal['data_point_merged'] = 'data_point_merged'
    data_point_type: str  # the DataPoint class name
    value: Any
    retrieved_by: OperatorId
    merge: Literal['added', 'updated']
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


SessionEvent = DataPointMerged | OperatorRunCompleted | CapabilityActivated | SessionCompleted


@runtime_checkable
class SessionEventSink(Protocol):
    """Where the orchestrator publishes session events. Must be fast or buffer internally."""

    async def publish(self, event: SessionEvent) -> None: ...


class NullSessionEventSink:
    """The default: events go nowhere, so a runtime that wires nothing loses nothing."""

    async def publish(self, _event: SessionEvent) -> None:
        return None
