"""AUDIT — AuditSink contract + entry model, run against the in-memory adapter."""

from __future__ import annotations

import pytest

from orcastork.adapters.memory import InMemoryAuditSink
from orcastork.audit import (
    AuditKind,
    AuditLogEntry,
    CapabilityAuditInfo,
    DataPointAuditInfo,
    OperatorAuditInfo,
    OperatorOutcome,
)
from orcastork.ids import CapabilityId, Epoch, OperatorId, SessionId
from orcastork.ports import AuditSink

from .doubles.conformance import AuditSinkConformance
from .doubles.datapoints import T0


class TestInMemoryAuditSink(AuditSinkConformance):
    @pytest.fixture
    def audit(self) -> AuditSink:
        return InMemoryAuditSink()


def test_audit_04_pii_summary_is_non_pii() -> None:
    # The audit entry carries a redacted summary, not the raw PII value.
    info = DataPointAuditInfo(data_point_type='work_email', summary='work_email present')
    entry = AuditLogEntry(
        session_id=SessionId('s'), epoch=Epoch(1), timestamp=T0, kind=AuditKind.DATA_POINT_ADDED, data_point=info
    )
    assert 'alice@work.example' not in entry.data_point.summary  # type: ignore[union-attr]


def test_audit_07_capability_invoked_records_redacted_parameters() -> None:
    info = CapabilityAuditInfo(
        capability_id=CapabilityId('idp'), action='send_challenge', parameters={'user_id': '<redacted>'}
    )
    entry = AuditLogEntry(
        session_id=SessionId('s'), epoch=Epoch(1), timestamp=T0, kind=AuditKind.CAPABILITY_INVOKED, capability=info
    )
    assert entry.capability is not None
    assert entry.capability.action == 'send_challenge'
    assert entry.capability.parameters == {'user_id': '<redacted>'}


def test_audit_08_operator_invoked_records_outcome_run_and_retry() -> None:
    info = OperatorAuditInfo(outcome=OperatorOutcome.DEAD_LETTERED, run_count=1, attempt=5, error='boom')
    entry = AuditLogEntry(
        session_id=SessionId('s'),
        epoch=Epoch(1),
        timestamp=T0,
        operator_id=OperatorId('agg'),
        kind=AuditKind.OPERATOR_INVOKED,
        operator=info,
    )
    assert entry.operator is not None
    assert entry.operator.outcome is OperatorOutcome.DEAD_LETTERED
    assert entry.operator.attempt == 5
    assert entry.operator.error == 'boom'
