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

export { type Clock, SystemClock } from './clock.js';
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
