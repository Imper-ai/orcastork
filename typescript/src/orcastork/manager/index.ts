/**
 * Fleet-level supervision: spawn, orphan-resume, epoch fencing, scheduling gate.
 *
 * @module
 */

export type {
  DeliverOptions,
  Redrive,
  ResumeOptions,
  SessionOrchestrationManagerOptions,
  StartSessionOptions,
} from './manager.js';
export {
  DEFAULT_REDELIVERY_RECHECK_MS,
  MAX_BACKSTOP_RESPAWNS,
  SchedulingGate,
  SessionOrchestrationManager,
} from './manager.js';
