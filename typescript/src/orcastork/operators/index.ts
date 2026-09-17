/**
 * Operators: the unified component base, the aggregation-phase marker, and the context.
 *
 * @module
 */

export {
  Aggregator,
  type AggregatorClass,
  type AggregatorStatics,
  aggregator,
  type ConcreteAggregatorClass,
} from './aggregator.js';
export {
  type ConcreteOperatorClass,
  Operator,
  type OperatorClass,
  OperatorPolicy,
  type OperatorPolicyInit,
  type OperatorStatics,
  operator,
  RerunOn,
} from './base.js';
export {
  CapabilityView,
  InvocationDelta,
  type InvocationDeltaInit,
  OperatorContext,
  type OperatorContextInit,
} from './context.js';
export { type EffectBody, EffectGuard, type EffectGuardOptions, EffectRecovery, type OnceOptions } from './effects.js';
