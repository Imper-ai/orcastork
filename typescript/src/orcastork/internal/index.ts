/**
 * Cross-cutting helpers this package needs and nothing outside it should reach for.
 *
 * Deliberately per-package: `orcastork_lite` keeps its own copies rather than sharing a `utils`
 * module, because the two packages must stay independently importable.
 *
 * @module
 */

export { BoundedQueue } from './bounded_queue.js';
export { Deferred } from './deferred.js';
export {
  type ManagedRegistry,
  type RegistriesSnapshot,
  Registry,
  registries,
  resetAllRegistries,
  restoreAllRegistries,
  snapshotAllRegistries,
} from './registry.js';
export { canonicalValue, stableStringify } from './stable_json.js';
