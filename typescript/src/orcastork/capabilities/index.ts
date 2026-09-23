/**
 * Capabilities: injected action providers with their own DataPoint/Capability deps.
 *
 * @module
 */

export {
  CapabilityActivator,
  type CapabilityActivatorOptions,
  type ComputeAvailableOptions,
  computeAvailable,
  type OnTerminalFailure,
} from './availability.js';
export {
  Capability,
  type CapabilityClass,
  CapabilityContext,
  type CapabilityContextInit,
  type CapabilityStatics,
  type ConcreteCapabilityClass,
  type Credentials,
  capability,
  type InvocationAuditor,
} from './base.js';
