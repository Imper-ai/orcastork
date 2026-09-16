/**
 * orcastork — a reusable, standalone dataflow orchestration framework.
 *
 * A blackboard/dataflow engine: `Operator`s consume and produce `DataPoint`s, `Capability`s
 * provide actions, `Aggregator`s write durable outputs, and a session-scoped `Orchestrator`
 * (supervised by a `SessionOrchestrationManager`) schedules work by data readiness rather than
 * fixed phases.
 *
 * The core is infrastructure- and domain-agnostic: it depends only on a set of `ports`
 * (interfaces); concrete backends (in-memory, Redis, Mongo) are injected as `adapters`. It is a
 * library, not a service: an embedding application runs it in its own event loop.
 *
 * @module
 */

export { type Clock, SystemClock } from './clock.js';
export {
  AggregatorDeadLetteredError,
  CapabilityUnavailableError,
  CompletionTailTimeoutError,
  DuplicateRegistrationError,
  InvalidCapabilityError,
  InvalidCompletionConditionError,
  InvalidDataPointError,
  InvalidOperatorError,
  LockHeldError,
  OptimisticConcurrencyError,
  OrchestrationError,
  PiiKeyUnavailableError,
  ReplayError,
  SchedulingGateBlockedError,
  StaleEpochError,
  StateMirrorError,
  UnboundedCycleError,
  UnknownDataPointTypeError,
  UnprotectedPiiError,
  UnstableValueError,
} from './exceptions.js';
export {
  type Brand,
  CapabilityId,
  type DataPointType,
  Epoch,
  NamespaceId,
  newSessionId,
  OperatorId,
  OperatorRef,
  Revision,
  SessionId,
} from './ids.js';
export { ConsoleJsonLogger, getLogger, type LogFields, type Logger, type LogLevel, setLogger } from './logging.js';
