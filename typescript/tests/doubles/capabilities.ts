/**
 * Capability test doubles — a factory creating fresh registered Capability subclasses.
 *
 * Each stub records the credentials it was activated with (on a class-level list), so tests can
 * assert lazy activation: a never-available capability is never constructed (its list stays empty),
 * and an activated one captures its catalog-supplied credentials.
 *
 * A class *expression* cannot carry a decorator, so the factory registers its stub by applying the
 * `capability` decorator by hand — the manual form the decorator exists to support.
 *
 * @module
 */

import type {
  CapabilityClass,
  CapabilityContext,
  ConcreteCapabilityClass,
  Credentials,
} from '../../src/orcastork/capabilities/index.js';
import { Capability, capability } from '../../src/orcastork/capabilities/index.js';
import type { AnyDataPoint, DataPointClass } from '../../src/orcastork/datapoints/index.js';
import { CapabilityId } from '../../src/orcastork/ids.js';

/** What {@link makeCapability} hands back: the class plus the counters a test asserts on. */
export type StubCapabilityClass = ConcreteCapabilityClass & {
  /** Credentials captured per successful activation. */
  activations: Credentials[];

  /** Every activation attempt, including the ones that threw. */
  attempts: number;
};

/** How a stub capability differs from the plain one. */
export interface MakeCapabilityOptions {
  readonly dependsOn?: readonly DataPointClass<AnyDataPoint>[];
  readonly requires?: readonly CapabilityClass[];

  /** Shared across capabilities, this captures the global activation order (base-before-layer). */
  readonly recordOrder?: CapabilityId[];

  /** Thrown from every `activate` (to exercise the activation-failure isolation path). */
  readonly activateError?: Error | null;

  /** A queue thrown one per attempt, after which activation succeeds (the cool-off retry path). */
  readonly activateErrors?: Iterable<Error>;
}

/** The class name Python's `make_capability` gives a stub: `auth_browser` → `AuthBrowser`. */
const titleCase = (identifier: string): string =>
  identifier
    .split('_')
    .map((part) => (part === '' ? part : `${part[0]?.toUpperCase() ?? ''}${part.slice(1)}`))
    .join('');

/** Create (and register) a stub Capability subclass with the given declarations. */
export const makeCapability = (capabilityId: string, options: MakeCapabilityOptions = {}): StubCapabilityClass => {
  const id = CapabilityId(capabilityId);
  const { activateError = null, recordOrder } = options;
  const errorQueue = [...(options.activateErrors ?? [])];

  class StubCapability extends Capability {
    public static readonly capabilityId = id;
    public static readonly dependsOn: readonly DataPointClass<AnyDataPoint>[] = options.dependsOn ?? [];
    public static readonly requires: readonly CapabilityClass[] = options.requires ?? [];
    public static activations: Credentials[] = [];
    public static attempts = 0;

    public async activate(ctx: CapabilityContext): Promise<void> {
      const own = this.constructor as typeof StubCapability;
      own.attempts += 1;
      if (activateError !== null) {
        throw activateError;
      }
      const queued = errorQueue.shift();
      if (queued !== undefined) {
        throw queued;
      }
      own.activations.push({ ...ctx.credentials });
      recordOrder?.push(id);
    }
  }

  // Named like the Python stub, so an error message or a log names the capability a reader knows.
  Object.defineProperty(StubCapability, 'name', { value: titleCase(capabilityId) });
  capability(StubCapability);
  return StubCapability;
};
