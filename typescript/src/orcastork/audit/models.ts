/**
 * The audit log entry model — framework-owned, domain-free.
 *
 * One entry is emitted per event — a DataPoint added, an operator invoked (per run/retry, with its
 * outcome), or a capability activated/invoked — epoch-stamped. PII is redacted into a non-PII
 * summary per the DataPoint's `isPii`, and capability parameters are redacted at the invocation
 * seam. The Mongo adapter serializes this model straight to its collection, so the wire shape is
 * Python's `model_dump(mode='json')` verbatim: snake_case keys, enum *values*, ISO instants, and an
 * explicit `null` for every field the entry does not carry. The binding to any persistence base
 * class lives in that adapter, never here.
 *
 * @module
 */

import { randomUUID } from 'node:crypto';
import { z } from 'zod';
import type { CapabilityId, Epoch, NamespaceId, OperatorId, SessionId } from '../ids.js';

/** What an audit entry records. The strings are the wire values a reader filters on. */
export const AuditKind = {
  DATA_POINT_ADDED: 'data_point_added',

  /** One per operator run / aggregator retry, carrying its outcome. */
  OPERATOR_INVOKED: 'operator_invoked',

  CAPABILITY_ACTIVATED: 'capability_activated',

  /** One per action performed on a capability (redacted parameters). */
  CAPABILITY_INVOKED: 'capability_invoked',

  /** Activation retries exhausted (terminal). */
  CAPABILITY_ACTIVATION_FAILED: 'capability_activation_failed',

  /** An entry removed from delivery (poison / apply cap). */
  INBOX_ENTRY_QUARANTINED: 'inbox_entry_quarantined',

  /** Idle inbox-wait exceeded `parkAfterMs`; the run returned without finalizing. */
  SESSION_PARKED: 'session_parked',

  /** A spawn's flow fingerprint differs from the session's persisted one. */
  FLOW_DRIFT_DETECTED: 'flow_drift_detected',

  /**
   * One row standing in for many merges of a type that opted out of per-emission auditing,
   * carrying the count so the trail still accounts for every DataPoint.
   */
  DATA_POINTS_COALESCED: 'data_points_coalesced',
} as const;

/** One of the {@link AuditKind} values. */
export type AuditKind = (typeof AuditKind)[keyof typeof AuditKind];

/** How one operator invocation ended. */
export const OperatorOutcome = {
  SUCCEEDED: 'succeeded',

  /** The run/attempt raised or timed out (isolated; the session proceeds). */
  FAILED: 'failed',

  /** An aggregator exhausted its retries; its output domain is flagged. */
  DEAD_LETTERED: 'dead_lettered',

  /** An aggregator whose dependencies never materialized; it wrote nothing durable. */
  SKIPPED: 'skipped',
} as const;

/** One of the {@link OperatorOutcome} values. */
export type OperatorOutcome = (typeof OperatorOutcome)[keyof typeof OperatorOutcome];

// --- the per-event detail blocks -------------------------------------------------------

/** What was merged, summarized so the trail carries no PII. */
export interface DataPointAuditInfo {
  readonly dataPointType: string;

  /** Non-PII summary; the raw value is redacted when the DataPoint is PII. */
  readonly summary: string;
}

/** The fields a {@link DataPointAuditInfo} is built from. */
export type DataPointAuditInfoInit = DataPointAuditInfo;

/** How one operator invocation went. */
export interface OperatorAuditInfo {
  readonly outcome: OperatorOutcome;

  /** Which scheduling invocation of this operator in the session (its rerun number). */
  readonly runCount: number;

  /** Which retry attempt within this invocation (aggregator retries; 1 when not retried). */
  readonly attempt: number;

  /**
   * The operator's own run time, in **seconds**, or `null` for a non-run event.
   *
   * Seconds, not milliseconds, against the port's usual rule: the field is persisted as
   * `run_seconds` and read back by Python workers and by whatever dashboards sit on the
   * collection, so its unit belongs to the wire format rather than to this language.
   */
  readonly runSeconds: number | null;

  /** The failure reason when the outcome is not `SUCCEEDED`. */
  readonly error: string | null;
}

/** The fields an {@link OperatorAuditInfo} is built from; only `outcome` is required. */
export interface OperatorAuditInfoInit {
  readonly outcome: OperatorOutcome;
  readonly runCount?: number;
  readonly attempt?: number;
  readonly runSeconds?: number | null;
  readonly error?: string | null;
}

/** Which capability did what, with its parameters already redacted. */
export interface CapabilityAuditInfo {
  readonly capabilityId: CapabilityId;

  /** The invoked action (`null` for an activation event). */
  readonly action: string | null;

  /** Redacted parameters. */
  readonly parameters: Readonly<Record<string, string>>;

  /** The failure reason when the event records a terminal activation failure. */
  readonly error: string | null;
}

/** The fields a {@link CapabilityAuditInfo} is built from; only `capabilityId` is required. */
export interface CapabilityAuditInfoInit {
  readonly capabilityId: CapabilityId;
  readonly action?: string | null;
  readonly parameters?: Readonly<Record<string, string>>;
  readonly error?: string | null;
}

/** Why an inbox entry left delivery. */
export interface InboxAuditInfo {
  readonly entryId: string;

  /** Why the entry was quarantined (parse failure, or the apply error at the delivery cap). */
  readonly reason: string;

  readonly deliveryCount: number;
}

/** The fields an {@link InboxAuditInfo} is built from. */
export type InboxAuditInfoInit = InboxAuditInfo;

/** Which flow drove the session, and which one just resumed it. */
export interface FlowAuditInfo {
  readonly flowName: string;

  /** The flow that last drove the session. */
  readonly storedFingerprint: string;

  /** The flow that just resumed it (persisted as the new baseline). */
  readonly currentFingerprint: string;
}

/** The fields a {@link FlowAuditInfo} is built from. */
export type FlowAuditInfoInit = FlowAuditInfo;

// --- the entry --------------------------------------------------------------------------

/** One epoch-stamped event in a session's trail. */
export interface AuditLogEntry {
  readonly entryId: string;

  readonly sessionId: SessionId;

  readonly namespaceId: NamespaceId | null;

  readonly epoch: Epoch;

  readonly timestamp: Date;

  readonly operatorId: OperatorId | null;

  readonly kind: AuditKind;

  readonly dataPoint: DataPointAuditInfo | null;

  readonly operator: OperatorAuditInfo | null;

  readonly capability: CapabilityAuditInfo | null;

  readonly inbox: InboxAuditInfo | null;

  readonly flow: FlowAuditInfo | null;
}

/** The fields an {@link AuditLogEntry} is built from; every detail block is optional. */
export interface AuditLogEntryInit {
  /** A fresh UUID when omitted, as Python's `default_factory` mints one. */
  readonly entryId?: string;
  readonly sessionId: SessionId;
  readonly namespaceId?: NamespaceId | null;
  readonly epoch: Epoch;
  readonly timestamp: Date;
  readonly operatorId?: OperatorId | null;
  readonly kind: AuditKind;
  readonly dataPoint?: DataPointAuditInfo | null;
  readonly operator?: OperatorAuditInfo | null;
  readonly capability?: CapabilityAuditInfo | null;
  readonly inbox?: InboxAuditInfo | null;
  readonly flow?: FlowAuditInfo | null;
}

// --- the wire form ----------------------------------------------------------------------

/** `DataPointAuditInfo` as persisted. */
export interface DataPointAuditInfoWire {
  readonly data_point_type: string;
  readonly summary: string;
}

/** `OperatorAuditInfo` as persisted. */
export interface OperatorAuditInfoWire {
  readonly outcome: OperatorOutcome;
  readonly run_count: number;
  readonly attempt: number;
  readonly run_seconds: number | null;
  readonly error: string | null;
}

/** `CapabilityAuditInfo` as persisted. */
export interface CapabilityAuditInfoWire {
  readonly capability_id: string;
  readonly action: string | null;
  readonly parameters: Readonly<Record<string, string>>;
  readonly error: string | null;
}

/** `InboxAuditInfo` as persisted. */
export interface InboxAuditInfoWire {
  readonly entry_id: string;
  readonly reason: string;
  readonly delivery_count: number;
}

/** `FlowAuditInfo` as persisted. */
export interface FlowAuditInfoWire {
  readonly flow_name: string;
  readonly stored_fingerprint: string;
  readonly current_fingerprint: string;
}

/** `AuditLogEntry` as persisted — Python's `model_dump(mode='json')`, key for key. */
export interface AuditLogEntryWire {
  readonly entry_id: string;
  readonly session_id: string;
  readonly namespace_id: string | null;
  readonly epoch: number;
  readonly timestamp: string;
  readonly operator_id: string | null;
  readonly kind: AuditKind;
  readonly data_point: DataPointAuditInfoWire | null;
  readonly operator: OperatorAuditInfoWire | null;
  readonly capability: CapabilityAuditInfoWire | null;
  readonly inbox: InboxAuditInfoWire | null;
  readonly flow: FlowAuditInfoWire | null;
}

// --- construction -----------------------------------------------------------------------

const auditKinds = Object.values(AuditKind);
const operatorOutcomes = Object.values(OperatorOutcome);

/** Branded ids are plain strings at runtime, so the schemas validate them as strings. */
const dataPointInfoSchema = z.object({ dataPointType: z.string(), summary: z.string() });

const operatorInfoSchema = z.object({
  outcome: z.enum(operatorOutcomes),
  runCount: z.number().int().optional(),
  attempt: z.number().int().optional(),
  runSeconds: z.number().nullish(),
  error: z.string().nullish(),
});

const capabilityInfoSchema = z.object({
  capabilityId: z.string(),
  action: z.string().nullish(),
  parameters: z.record(z.string(), z.string()).optional(),
  error: z.string().nullish(),
});

const inboxInfoSchema = z.object({ entryId: z.string(), reason: z.string(), deliveryCount: z.number().int() });

const flowInfoSchema = z.object({
  flowName: z.string(),
  storedFingerprint: z.string(),
  currentFingerprint: z.string(),
});

const entrySchema = z.object({
  entryId: z.string().optional(),
  sessionId: z.string(),
  namespaceId: z.string().nullish(),
  epoch: z.number().int(),
  timestamp: z.date(),
  operatorId: z.string().nullish(),
  kind: z.enum(auditKinds),
  dataPoint: dataPointInfoSchema.nullish(),
  operator: operatorInfoSchema.nullish(),
  capability: capabilityInfoSchema.nullish(),
  inbox: inboxInfoSchema.nullish(),
  flow: flowInfoSchema.nullish(),
});

/** Build a {@link DataPointAuditInfo}. */
export const DataPointAuditInfo = (init: DataPointAuditInfoInit): DataPointAuditInfo => {
  dataPointInfoSchema.parse(init);
  return Object.freeze({ dataPointType: init.dataPointType, summary: init.summary });
};

/** Build an {@link OperatorAuditInfo}; the counters default to a first, unretried run. */
export const OperatorAuditInfo = (init: OperatorAuditInfoInit): OperatorAuditInfo => {
  operatorInfoSchema.parse(init);
  return Object.freeze({
    outcome: init.outcome,
    runCount: init.runCount ?? 1,
    attempt: init.attempt ?? 1,
    runSeconds: init.runSeconds ?? null,
    error: init.error ?? null,
  });
};

/** Build a {@link CapabilityAuditInfo}; `parameters` are copied, so the entry cannot drift. */
export const CapabilityAuditInfo = (init: CapabilityAuditInfoInit): CapabilityAuditInfo => {
  capabilityInfoSchema.parse(init);
  return Object.freeze({
    capabilityId: init.capabilityId,
    action: init.action ?? null,
    parameters: Object.freeze({ ...(init.parameters ?? {}) }),
    error: init.error ?? null,
  });
};

/** Build an {@link InboxAuditInfo}. */
export const InboxAuditInfo = (init: InboxAuditInfoInit): InboxAuditInfo => {
  inboxInfoSchema.parse(init);
  return Object.freeze({ entryId: init.entryId, reason: init.reason, deliveryCount: init.deliveryCount });
};

/** Build a {@link FlowAuditInfo}. */
export const FlowAuditInfo = (init: FlowAuditInfoInit): FlowAuditInfo => {
  flowInfoSchema.parse(init);
  return Object.freeze({
    flowName: init.flowName,
    storedFingerprint: init.storedFingerprint,
    currentFingerprint: init.currentFingerprint,
  });
};

/**
 * Build an {@link AuditLogEntry}.
 *
 * Every optional field lands as `null` rather than absent: the entry is persisted field for field,
 * and a row whose shape depends on what happened is a row a query has to special-case.
 */
export const AuditLogEntry = (init: AuditLogEntryInit): AuditLogEntry => {
  entrySchema.parse(init);
  return Object.freeze({
    entryId: init.entryId ?? randomUUID(),
    sessionId: init.sessionId,
    namespaceId: init.namespaceId ?? null,
    epoch: init.epoch,
    // `Date` is mutable, and a frozen entry that kept the caller's instance could have its
    // timestamp moved after the fact.
    timestamp: new Date(init.timestamp.getTime()),
    operatorId: init.operatorId ?? null,
    kind: init.kind,
    dataPoint: init.dataPoint ?? null,
    operator: init.operator ?? null,
    capability: init.capability ?? null,
    inbox: init.inbox ?? null,
    flow: init.flow ?? null,
  });
};

// --- serialization ----------------------------------------------------------------------

const dataPointInfoToWire = (info: DataPointAuditInfo): DataPointAuditInfoWire => ({
  data_point_type: info.dataPointType,
  summary: info.summary,
});

const operatorInfoToWire = (info: OperatorAuditInfo): OperatorAuditInfoWire => ({
  outcome: info.outcome,
  run_count: info.runCount,
  attempt: info.attempt,
  run_seconds: info.runSeconds,
  error: info.error,
});

const capabilityInfoToWire = (info: CapabilityAuditInfo): CapabilityAuditInfoWire => ({
  capability_id: info.capabilityId,
  action: info.action,
  parameters: { ...info.parameters },
  error: info.error,
});

const inboxInfoToWire = (info: InboxAuditInfo): InboxAuditInfoWire => ({
  entry_id: info.entryId,
  reason: info.reason,
  delivery_count: info.deliveryCount,
});

const flowInfoToWire = (info: FlowAuditInfo): FlowAuditInfoWire => ({
  flow_name: info.flowName,
  stored_fingerprint: info.storedFingerprint,
  current_fingerprint: info.currentFingerprint,
});

/** The persisted form of an entry — what the audit sink writes and `replay` reads back. */
export const auditLogEntryToWire = (entry: AuditLogEntry): AuditLogEntryWire => ({
  entry_id: entry.entryId,
  session_id: entry.sessionId,
  namespace_id: entry.namespaceId,
  epoch: entry.epoch,
  timestamp: entry.timestamp.toISOString(),
  operator_id: entry.operatorId,
  kind: entry.kind,
  data_point: entry.dataPoint === null ? null : dataPointInfoToWire(entry.dataPoint),
  operator: entry.operator === null ? null : operatorInfoToWire(entry.operator),
  capability: entry.capability === null ? null : capabilityInfoToWire(entry.capability),
  inbox: entry.inbox === null ? null : inboxInfoToWire(entry.inbox),
  flow: entry.flow === null ? null : flowInfoToWire(entry.flow),
});

/**
 * Instants are read tolerantly: an ISO-8601 string with either spelling of UTC, or the `Date` a
 * driver hands back for a BSON date (the Mongo sink persists `timestamp` as a real date so the
 * trail is queryable by time).
 */
const instantSchema = z.union([z.date(), z.string()]).transform((value, ctx) => {
  const parsed = value instanceof Date ? new Date(value.getTime()) : new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    ctx.addIssue({ code: 'custom', message: 'expected an ISO-8601 instant' });
    return z.NEVER;
  }
  return parsed;
});

const wireSchema = z.object({
  entry_id: z.string(),
  session_id: z.string(),
  namespace_id: z.string().nullish(),
  epoch: z.number().int(),
  timestamp: instantSchema,
  operator_id: z.string().nullish(),
  kind: z.enum(auditKinds),
  data_point: z.object({ data_point_type: z.string(), summary: z.string() }).nullish(),
  operator: z
    .object({
      outcome: z.enum(operatorOutcomes),
      run_count: z.number().int(),
      attempt: z.number().int(),
      run_seconds: z.number().nullish(),
      error: z.string().nullish(),
    })
    .nullish(),
  capability: z
    .object({
      capability_id: z.string(),
      action: z.string().nullish(),
      parameters: z.record(z.string(), z.string()).optional(),
      error: z.string().nullish(),
    })
    .nullish(),
  inbox: z.object({ entry_id: z.string(), reason: z.string(), delivery_count: z.number().int() }).nullish(),
  flow: z.object({ flow_name: z.string(), stored_fingerprint: z.string(), current_fingerprint: z.string() }).nullish(),
});

/** Read a persisted entry back — the counterpart of {@link auditLogEntryToWire}. */
export const parseAuditLogEntry = (raw: unknown): AuditLogEntry => {
  const wire = wireSchema.parse(raw);
  return AuditLogEntry({
    entryId: wire.entry_id,
    sessionId: wire.session_id as SessionId,
    namespaceId: (wire.namespace_id ?? null) as NamespaceId | null,
    epoch: wire.epoch as Epoch,
    timestamp: wire.timestamp,
    operatorId: (wire.operator_id ?? null) as OperatorId | null,
    kind: wire.kind,
    dataPoint:
      wire.data_point == null
        ? null
        : DataPointAuditInfo({ dataPointType: wire.data_point.data_point_type, summary: wire.data_point.summary }),
    operator:
      wire.operator == null
        ? null
        : OperatorAuditInfo({
            outcome: wire.operator.outcome,
            runCount: wire.operator.run_count,
            attempt: wire.operator.attempt,
            runSeconds: wire.operator.run_seconds ?? null,
            error: wire.operator.error ?? null,
          }),
    capability:
      wire.capability == null
        ? null
        : CapabilityAuditInfo({
            capabilityId: wire.capability.capability_id as CapabilityId,
            action: wire.capability.action ?? null,
            parameters: wire.capability.parameters ?? {},
            error: wire.capability.error ?? null,
          }),
    inbox:
      wire.inbox == null
        ? null
        : InboxAuditInfo({
            entryId: wire.inbox.entry_id,
            reason: wire.inbox.reason,
            deliveryCount: wire.inbox.delivery_count,
          }),
    flow:
      wire.flow == null
        ? null
        : FlowAuditInfo({
            flowName: wire.flow.flow_name,
            storedFingerprint: wire.flow.stored_fingerprint,
            currentFingerprint: wire.flow.current_fingerprint,
          }),
  });
};
