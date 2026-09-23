/**
 * `Runtime` — the injected bundle the orchestrator depends on: the clock, the catalog, the event sink.
 *
 * @module
 */

import type { CapabilityCatalog } from './capabilities.js';
import { InMemoryCapabilityCatalog } from './capabilities.js';
import type { Clock } from './clock.js';
import { SystemClock } from './clock.js';
import type { SessionEventSink } from './events.js';
import { NullSessionEventSink } from './events.js';

/** Everything the engine reaches the outside world through. */
export interface Runtime {
  /** The only time source: every window, deadline and timestamp in the session comes from here. */
  readonly clock: Clock;

  /** Per-namespace permissions, credentials and provider preference. */
  readonly catalog: CapabilityCatalog;

  /** Where the live view of the session goes; the default drops it. */
  readonly events: SessionEventSink;
}

/** The parts of a {@link Runtime}; each one left out falls back to the shipped default. */
export interface RuntimeInit {
  readonly clock?: Clock | undefined;
  readonly catalog?: CapabilityCatalog | undefined;
  readonly events?: SessionEventSink | undefined;
}

/** Build a {@link Runtime}, filling in the system clock, an empty catalog and no event sink. */
export const Runtime = (init: RuntimeInit = {}): Runtime =>
  Object.freeze({
    clock: init.clock ?? new SystemClock(),
    catalog: init.catalog ?? new InMemoryCapabilityCatalog(),
    events: init.events ?? new NullSessionEventSink(),
  });

/** The parts of a {@link Runtime} other than the clock, which {@link buildRuntime} takes first. */
export type BuildRuntimeOptions = Omit<RuntimeInit, 'clock'>;

/**
 * Build a runtime; unspecified parts fall back to the system clock, an empty catalog and no sink.
 *
 * The clock comes first because it is the part a test almost always replaces; a call that only
 * wires a sink passes `undefined` for it (`buildRuntime(undefined, { events: sink })`).
 */
export const buildRuntime = (clock?: Clock | undefined, options: BuildRuntimeOptions = {}): Runtime =>
  Runtime({ clock, catalog: options.catalog, events: options.events });
