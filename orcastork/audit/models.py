"""The audit log entry model — pure pydantic, framework-owned (``common``-free).

One entry is emitted per event — a DataPoint added, an operator invoked (per run/retry,
with its outcome), or a capability activated/invoked — epoch-stamped. PII is redacted into
a non-PII summary per the DataPoint's ``is_pii``, and capability parameters are redacted at
the invocation seam. The Mongo adapter serializes this model directly to its collection;
the binding to any persistence base class lives in that adapter, never here.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

from ..ids import CapabilityId, Epoch, NamespaceId, OperatorId, SessionId


class AuditKind(Enum):
    DATA_POINT_ADDED = 'data_point_added'
    OPERATOR_INVOKED = 'operator_invoked'  # one per operator run / aggregator retry, carrying its outcome
    CAPABILITY_ACTIVATED = 'capability_activated'
    CAPABILITY_INVOKED = 'capability_invoked'  # one per action performed on a capability (redacted parameters)
    CAPABILITY_ACTIVATION_FAILED = 'capability_activation_failed'  # activation retries exhausted (terminal)
    INBOX_ENTRY_QUARANTINED = 'inbox_entry_quarantined'  # an entry removed from delivery (poison / apply cap)
    SESSION_PARKED = 'session_parked'  # idle inbox-wait exceeded park_after; the run returned without finalizing
    FLOW_DRIFT_DETECTED = 'flow_drift_detected'  # a spawn's flow fingerprint differs from the session's persisted one
    # One row standing in for many merges of a type that opted out of per-emission auditing, carrying the
    # count so the trail still accounts for every DataPoint.
    DATA_POINTS_COALESCED = 'data_points_coalesced'


class OperatorOutcome(Enum):
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'  # the run/attempt raised or timed out (isolated; the session proceeds)
    DEAD_LETTERED = 'dead_lettered'  # an aggregator exhausted its retries; its output domain is flagged
    SKIPPED = 'skipped'  # an aggregator whose dependencies never materialized; it wrote nothing durable


class DataPointAuditInfo(BaseModel):
    model_config = ConfigDict(frozen=True)

    data_point_type: str
    summary: str  # non-PII summary; the raw value is redacted when the DataPoint is PII


class OperatorAuditInfo(BaseModel):
    model_config = ConfigDict(frozen=True)

    outcome: OperatorOutcome
    run_count: int = 1  # which scheduling invocation of this operator in the session (its rerun number)
    attempt: int = 1  # which retry attempt within this invocation (aggregator retries; 1 when not retried)
    run_seconds: float | None = None  # the operator's own run time (invoked -> completed); None for non-run events
    error: str | None = None  # the failure reason when the outcome is not SUCCEEDED


class CapabilityAuditInfo(BaseModel):
    model_config = ConfigDict(frozen=True)

    capability_id: CapabilityId
    action: str | None = None  # the invoked action (None for an activation event)
    parameters: dict[str, str] = Field(default_factory=dict)  # redacted parameters
    error: str | None = None  # the failure reason when the event records a terminal activation failure


class InboxAuditInfo(BaseModel):
    model_config = ConfigDict(frozen=True)

    entry_id: str
    reason: str  # why the entry was quarantined (parse failure, or the apply error at the delivery cap)
    delivery_count: int


class FlowAuditInfo(BaseModel):
    model_config = ConfigDict(frozen=True)

    flow_name: str
    stored_fingerprint: str  # the flow that last drove the session
    current_fingerprint: str  # the flow that just resumed it (persisted as the new baseline)


class AuditLogEntry(BaseModel):
    model_config = ConfigDict(frozen=True)

    entry_id: str = Field(default_factory=lambda: str(uuid4()))
    session_id: SessionId
    namespace_id: NamespaceId | None = None
    epoch: Epoch
    timestamp: datetime
    operator_id: OperatorId | None = None
    kind: AuditKind
    data_point: DataPointAuditInfo | None = None
    operator: OperatorAuditInfo | None = None
    capability: CapabilityAuditInfo | None = None
    inbox: InboxAuditInfo | None = None
    flow: FlowAuditInfo | None = None
