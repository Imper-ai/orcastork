/**
 * orcastork_lite — the scheduling core of orcastork, and nothing else.
 *
 * `Operator`s consume and produce `DataPoint`s, `Capability`s are injected action providers, and a
 * session-scoped `Orchestrator` runs the operators by **data readiness**: an operator runs when
 * its inputs exist, reruns (debounced) when relevant new data lands, is retried on a backoff
 * window if its policy asks for it, and is timeout-bounded and fault-isolated throughout. When
 * nothing can run any more, the session is complete and the gathered DataPoints are returned.
 *
 * Deliberately absent: durability, resumability, fencing epochs, locks, inbox, parking,
 * aggregators, audit, archive, telemetry. A session lives and dies in one process.
 *
 * @module
 */

export {
  Capability,
  CapabilityActivator,
  type CapabilityActivatorOptions,
  type CapabilityCatalog,
  type CapabilityClass,
  CapabilityContext,
  type CapabilityContextInit,
  type CapabilityStatics,
  CapabilityView,
  type ComputeAvailableOptions,
  type ConcreteCapabilityClass,
  type CredentialEntry,
  type Credentials,
  computeAvailable,
  InMemoryCapabilityCatalog,
  type InMemoryCapabilityCatalogInit,
} from './capabilities.js';
export { type Clock, SleepAbortedError, SystemClock } from './clock.js';
export {
  type AnyClass,
  type AnyDataPoint,
  type ConcreteDataPointClass,
  DataPoint,
  type DataPointClass,
  DataPointEmission,
  type DataPointInit,
  DataPointView,
  identityKey,
  isSubclass,
} from './datapoints.js';
export {
  type ActivationOutcome,
  CapabilityActivated,
  type CapabilityActivatedInit,
  DataPointMerged,
  type DataPointMergedInit,
  type MergeKind,
  NullSessionEventSink,
  OperatorRunCompleted,
  type OperatorRunCompletedInit,
  type RunOutcome,
  SESSION_EVENT_KIND,
  SessionCompleted,
  type SessionCompletedInit,
  type SessionEvent,
  type SessionEventBase,
  type SessionEventKind,
  type SessionEventSink,
} from './events.js';
export {
  CapabilityUnavailableError,
  DuplicateIdError,
  InvalidOperatorError,
  OrcastorkLiteError,
  UnboundedCycleError,
  UnhashableValueError,
} from './exceptions.js';
export { type Brand, CapabilityId, NamespaceId, OperatorId, SessionId } from './ids.js';
export { ConsoleJsonLogger, getLogger, type LogFields, type Logger, type LogLevel, setLogger } from './logging.js';
export {
  type ConcreteOperatorClass,
  InvocationDelta,
  type InvocationDeltaInit,
  Operator,
  type OperatorClass,
  OperatorContext,
  type OperatorContextInit,
  OperatorPolicy,
  type OperatorPolicyInit,
  type OperatorStatics,
  RerunOn,
  RetryPolicy,
  type RetryPolicyInit,
} from './operators.js';
export {
  DEFAULT_OPERATION_TIMEOUT_MS,
  DEFAULT_PUBLISH_TIMEOUT_MS,
  DEFAULT_SESSION_DEADLINE_MS,
  Orchestrator,
  type OrchestratorOptions,
  SessionResult,
  type SessionResultInit,
} from './orchestrator.js';
export { type BuildRuntimeOptions, buildRuntime, Runtime, type RuntimeInit } from './runtime.js';
