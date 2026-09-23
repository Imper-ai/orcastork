/**
 * Scheduling: readiness, graph-aware quiescence, debounce, watermark/delta, completion.
 *
 * @module
 */

export {
  AllOf,
  AnyOf,
  allOf,
  anyOf,
  type CompletionCondition,
  type CompletionItem,
  describeCondition,
  normalizeCompletion,
  referencedTypes,
  TypePresent,
} from './completion.js';
export {
  DEFAULT_DEBOUNCE_MS,
  DebounceController,
  type DebounceControllerOptions,
  type FinalizeProximity,
  type RerunEligibilityInputs,
  rerunEligible,
  type WindowOptions,
  windowDefersToFinalize,
} from './debounce.js';
export { isQuiescent, reachablePending } from './quiescence.js';
export {
  isReady,
  type ReadinessGap,
  type ReadinessInputs,
  type ReadyOperatorsOptions,
  readinessGap,
  readyOperators,
} from './readiness.js';
export {
  type ComputeDeltaOptions,
  computeDelta,
  type OperatorDeltaOptions,
  operatorDelta,
} from './watermark.js';
