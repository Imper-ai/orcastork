/**
 * Aggregation: the idempotency/OCC helpers + bounded-retry/dead-letter machinery.
 *
 * @module
 */

export { AggregateStatus, AggregationHelpers, type AggregationHelpersOptions } from './helpers.js';
export {
  type BackoffOptions,
  backoffDelays,
  type OnAttempt,
  RetryPolicy,
  type RetryPolicyInit,
  type RunWithRetryOptions,
  retryPolicyBounds,
  runWithRetry,
  seedFor,
} from './retry.js';
