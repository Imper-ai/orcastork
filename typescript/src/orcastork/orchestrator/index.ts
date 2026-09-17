/**
 * The engine itself: the session-scoped orchestrator and its sole-mutator state mirror.
 *
 * @module
 */

export { type MirrorWriteResult, SessionStateMirror } from './mirror.js';
export {
  DEFAULT_EMISSION_QUEUE_SIZE,
  DEFAULT_LEASE_RENEW_INTERVAL_MS,
  DEFAULT_MAX_INBOX_DELIVERIES,
  DEFAULT_OPERATION_TIMEOUT_MS,
  DEFAULT_SESSION_DEADLINE_MS,
  type DeadLetter,
  Orchestrator,
  type OrchestratorOptions,
  type OrchestratorResult,
  SessionStatus,
  type Signal,
} from './orchestrator.js';
