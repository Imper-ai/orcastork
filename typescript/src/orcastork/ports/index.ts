/**
 * The framework ports (interfaces) the core depends on, plus shared value types.
 *
 * Concrete backends (in-memory, Redis, Mongo) implement these under `adapters/`; the core never
 * imports a backend.
 *
 * @module
 */

export type { AuditSink } from './audit_sink.js';
export type { CapabilityCatalog } from './capability_catalog.js';
export {
  ChangeSet,
  type ChangeSetInit,
  type DeliveredInboxEntry,
  InboxEntry,
  type InboxEntryInit,
  PoisonInboxEntry,
  type PoisonInboxEntryInit,
  QuarantinedEntry,
  type QuarantinedEntryInit,
  VersionedDocument,
  type VersionedDocumentInit,
} from './change_set.js';
export type { CooldownGate } from './cooldown_gate.js';
export type { DataPointArchive } from './datapoint_archive.js';
export {
  type ApplyResolvedOptions,
  type ClaimEffectOptions,
  type DataPointReader,
  type DataPointStore,
  EFFECT_COMMITTED,
  EFFECT_PENDING_PREFIX,
  EffectClaim,
  effectPendingEpoch,
  effectPendingState,
} from './datapoint_store.js';
export type { DurableStore, DurableUpsertOptions } from './durable_store.js';
export type { ConsumeOptions, Inbox, QuarantineOptions } from './inbox.js';
export { NullRateLimiter, type RateLimiter } from './rate_limiter.js';
export type { SessionLock } from './session_lock.js';
