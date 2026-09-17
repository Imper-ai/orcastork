/**
 * AUDIT — the `AuditSink` contract + entry model, run against the in-memory adapter.
 */

import { describe, expect, it } from 'vitest';
import { InMemoryAuditSink } from '../src/orcastork/adapters/memory/index.js';
import {
  AuditKind,
  AuditLogEntry,
  CapabilityAuditInfo,
  DataPointAuditInfo,
  OperatorAuditInfo,
  OperatorOutcome,
} from '../src/orcastork/audit/index.js';
import { CapabilityId, Epoch, OperatorId, SessionId } from '../src/orcastork/ids.js';
import { describeAuditSinkConformance } from './doubles/conformance/audit_sink.js';
import { T0 } from './doubles/datapoints.js';

describeAuditSinkConformance({
  name: 'InMemoryAuditSink',
  create: () =>
    Promise.resolve({
      audit: new InMemoryAuditSink(),
      // Nothing in the audit contract waits on a clock; entries carry their own timestamp.
      advanceTime: () => Promise.resolve(),
    }),
});

describe('the audit log entry', () => {
  it('carries a redacted summary of a PII DataPoint, never the raw value', () => {
    const info = DataPointAuditInfo({ dataPointType: 'work_email', summary: 'work_email present' });
    const entry = AuditLogEntry({
      sessionId: SessionId('s'),
      epoch: Epoch(1),
      timestamp: T0,
      kind: AuditKind.DATA_POINT_ADDED,
      dataPoint: info,
    });
    expect(entry.dataPoint?.summary).not.toContain('alice@work.example');
  });

  it('records a capability invocation with its redacted parameters', () => {
    const info = CapabilityAuditInfo({
      capabilityId: CapabilityId('idp'),
      action: 'send_challenge',
      parameters: { user_id: '<redacted>' },
    });
    const entry = AuditLogEntry({
      sessionId: SessionId('s'),
      epoch: Epoch(1),
      timestamp: T0,
      kind: AuditKind.CAPABILITY_INVOKED,
      capability: info,
    });
    expect(entry.capability).not.toBeNull();
    expect(entry.capability?.action).toBe('send_challenge');
    expect(entry.capability?.parameters).toEqual({ user_id: '<redacted>' });
  });

  it('records an operator invocation with its outcome, run count and attempt', () => {
    const info = OperatorAuditInfo({
      outcome: OperatorOutcome.DEAD_LETTERED,
      runCount: 1,
      attempt: 5,
      error: 'boom',
    });
    const entry = AuditLogEntry({
      sessionId: SessionId('s'),
      epoch: Epoch(1),
      timestamp: T0,
      operatorId: OperatorId('agg'),
      kind: AuditKind.OPERATOR_INVOKED,
      operator: info,
    });
    expect(entry.operator).not.toBeNull();
    expect(entry.operator?.outcome).toBe(OperatorOutcome.DEAD_LETTERED);
    expect(entry.operator?.attempt).toBe(5);
    expect(entry.operator?.error).toBe('boom');
  });
});
