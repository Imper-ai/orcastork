/**
 * `Capability` — an injected provider of typed actions (vs. inert DataPoints).
 *
 * Like operators, a capability declares the DataPoints it needs (`dependsOn`) and the capabilities
 * it builds on (`requires`); the orchestrator decides availability and activates it lazily. An
 * operator gets the activated instance from `ctx.capabilities` and calls its **action methods
 * directly**, so call sites keep full static typing (real parameters, real return type — no
 * stringly-typed dispatch).
 *
 * Auditing is intrinsic, not a call-site concern: at registration every **public async method**
 * (other than `activate`) is wrapped so each call is recorded through the capability's injected
 * auditor *before* the body runs. An author cannot forget to audit an action and an operator cannot
 * bypass it; internal helpers are simply named with a leading underscore. A public method the
 * wrapper cannot cover (synchronous, or an async generator) is rejected at class definition with
 * {@link InvalidCapabilityError}, so no action ever escapes the audited seam. The same seam also
 * paces each action through the injected fleet `RateLimiter` (when one is bound) before the
 * invocation is recorded, and opens a telemetry span around the whole call — so every external
 * action is traced without the author instrumenting anything. Concrete capabilities register under
 * their `capabilityId` through the {@link capability} decorator; a duplicate id raises.
 *
 * **Where the port differs from Python, and why.** Python runs this at `__init_subclass__`, so
 * *every* subclass — abstract family included — is wrapped and checked. A TypeScript class runs no
 * code at definition time unless it is decorated, so the decorator walks the whole prototype chain
 * up to `Capability` instead: a concrete provider below an undecorated abstract family still has
 * that family's actions audited and its public synchronous methods rejected. Two further
 * consequences of the runtime, both documented on {@link capability}: "public" is decided by the
 * *name* (a leading underscore means internal), because TypeScript's `private`/`protected` do not
 * exist at runtime, and "async" is decided by the function's constructor name, which a downlevel
 * transpiler would erase.
 *
 * @module
 */

import type { AnyDataPoint, DataPointClass, DataPointView } from '../datapoints/index.js';
import { DuplicateRegistrationError, InvalidCapabilityError } from '../exceptions.js';
import type { CapabilityId } from '../ids.js';
import { Registry } from '../internal/registry.js';
import type { RateLimiter } from '../ports/rate_limiter.js';
import { Telemetry, withSpan } from '../telemetry.js';

/**
 * Registry of concrete capabilities keyed by `capabilityId`.
 *
 * Module-level, as Python's `Capability._registry` is: registering is a side effect of declaring
 * the class. It is a {@link Registry}, so the test setup snapshots and restores it around every
 * test without knowing it exists. Exported from this module only (not from the barrel), like the
 * DataPoint registry — the graph tool reads it, nothing else should.
 */
export const capabilityRegistry = new Registry<CapabilityId, ConcreteCapabilityClass>('orcastork.capabilities');

/** The secrets a capability is built from — whatever the deployment's catalog stores for it. */
export type Credentials = Readonly<Record<string, unknown>>;

/**
 * Records one capability invocation (capability id, action name, bound arguments) at the audited
 * seam.
 */
export type InvocationAuditor = (
  capabilityId: CapabilityId,
  action: string,
  parameters: Readonly<Record<string, unknown>>,
) => Promise<void>;

/**
 * What a capability declares about itself, as `static readonly` fields on the class:
 *
 * ```ts
 * @capability
 * class BreachIntel extends Capability {
 *   static readonly capabilityId = CapabilityId('breach_intel');
 *   static readonly dependsOn = [EmailDataPoint];
 *   async activate(ctx: CapabilityContext): Promise<void> { … }
 *   async breachCount(options: { email: string }): Promise<number> { … }
 * }
 * ```
 *
 * They are declared here rather than on {@link Capability} itself so that a subclass setting one
 * does not have to write `static override` for a field the base only holds a default of — the
 * engine reads an absent `dependsOn`/`requires` as empty, exactly as Python's class defaults do.
 */
export interface CapabilityStatics {
  /** Registry key + identity of this provider. */
  readonly capabilityId: CapabilityId;

  /** DataPoint types that must be present (subtype-aware) before this can be built. */
  readonly dependsOn?: readonly DataPointClass<AnyDataPoint>[];

  /** Capabilities this one builds on; they activate first. */
  readonly requires?: readonly CapabilityClass[];

  /** Every class has one; the framework uses it in error messages and logs. */
  readonly name: string;
}

/**
 * A capability class as the framework handles it — Python's `type[Capability]`.
 *
 * The constructor is abstract and the statics are absent on purpose: a class type is used here only
 * to *match* (`instanceof`, `isSubclass`, a `requires` entry, a `resolve` argument), and a provider
 * *family* that declares no id and is never instantiated must be as usable there as a leaf — exactly
 * as an abstract Python class is. {@link ConcreteCapabilityClass} is the registered, constructible
 * counterpart, and the one the declarations are read off.
 */
export type CapabilityClass<T extends Capability = Capability> = abstract new () => T;

/** A constructible, registered capability class: what the activator builds. */
export type ConcreteCapabilityClass<T extends Capability = Capability> = CapabilityStatics & (new () => T);

/** The fields a {@link CapabilityContext} is built from. */
export interface CapabilityContextInit {
  readonly credentials: Credentials;

  readonly store: DataPointView;
}

/** What a capability sees when it activates: its credentials + the current DataPoints. */
export class CapabilityContext {
  /** What the `CapabilityCatalog` holds for `(namespace, capabilityId)`. */
  public readonly credentials: Credentials;

  /** The session's current DataPoints, read-only. */
  public readonly store: DataPointView;

  public constructor(init: CapabilityContextInit) {
    this.credentials = init.credentials;
    this.store = init.store;
    Object.freeze(this);
  }
}

/**
 * The shared default telemetry — the process globals, inert until a deployment installs an SDK.
 *
 * One instance for the whole module, as Python's class-level `Telemetry()` default is: building a
 * fresh one per capability would create a duplicate set of instruments for every provider.
 */
const DEFAULT_TELEMETRY = new Telemetry();

/** An injected provider of typed actions; subclass it, declare {@link CapabilityStatics}, add actions. */
export abstract class Capability {
  /** Injected on activation; `null` → invocations are not recorded. */
  private auditor: InvocationAuditor | null = null;

  /** Injected on activation; `null` → actions are not paced. */
  private rateLimiter: RateLimiter | null = null;

  private rateLimitKey = '';

  /** Injected on activation; the default rides the global providers. */
  private telemetry: Telemetry = DEFAULT_TELEMETRY;

  /** This provider's id, read off the class — the counterpart of Python's `self.capability_id`. */
  public get capabilityId(): CapabilityId {
    return (this.constructor as unknown as CapabilityStatics).capabilityId;
  }

  /** Inject the invocation auditor — the orchestrator wires this when the capability activates. */
  public bindAuditor(auditor: InvocationAuditor | null): void {
    this.auditor = auditor;
  }

  /** Inject the fleet rate limiter — the orchestrator wires this when the capability activates. */
  public bindRateLimit(limiter: RateLimiter | null, key: string): void {
    this.rateLimiter = limiter;
    this.rateLimitKey = key;
  }

  /** Inject the telemetry backend — the orchestrator wires this when the capability activates. */
  public bindTelemetry(telemetry: Telemetry): void {
    this.telemetry = telemetry;
  }

  /**
   * Run one action through the audited seam: span → pace → record → body.
   *
   * @internal The wrapper the {@link capability} decorator installs is the only caller. It is a
   * method (and not three exposed fields) so everything it needs stays private, and it carries the
   * underscore that marks it internal to the decorator's own "is this an action?" rule.
   */
  public async _invokeAction<T>(
    actionName: string,
    parameters: Readonly<Record<string, unknown>>,
    body: () => Promise<T>,
  ): Promise<T> {
    // The span covers the caller-observed call — pacing, the audit record and the action body — so
    // time queued behind the fleet limiter is visible in the trace too. An action that raises
    // propagates through the span, which records it and marks it failed.
    return await withSpan(
      this.telemetry.tracer,
      `capability.action ${this.capabilityId}.${actionName}`,
      async () => {
        // Pace before recording: the audit trail must hold only actions that actually proceeded,
        // not ones still queued behind the fleet's rate limit.
        await this._acquireRateLimit();
        await this._recordInvocation(actionName, parameters);
        return await body();
      },
      { attributes: { capability_id: this.capabilityId, action: actionName } },
    );
  }

  /** Build the underlying client from catalog-supplied credentials (lazy). */
  public abstract activate(ctx: CapabilityContext): Promise<void>;

  private async _recordInvocation(actionName: string, parameters: Readonly<Record<string, unknown>>): Promise<void> {
    if (this.auditor !== null) {
      await this.auditor(this.capabilityId, actionName, parameters);
    }
  }

  private async _acquireRateLimit(): Promise<void> {
    if (this.rateLimiter !== null) {
      await this.rateLimiter.acquire(this.rateLimitKey);
    }
  }
}

/** Set on a wrapper so an inherited action is never double-wrapped (Python's `__capability_audited__`). */
const AUDITED_MARKER = Symbol('orcastork.capability.audited');

/** A prototype method the wrapper may stand in for. */
type Action = (this: Capability, ...args: unknown[]) => Promise<unknown>;

/** The wrapper's own marker, readable back off it. */
type MarkedAction = Action & { readonly [AUDITED_MARKER]?: true };

/** Lifecycle, not an action: `activate` is called by the framework, never by an operator. */
const LIFECYCLE_METHOD = 'activate';

/** Names the base itself defines (`bindAuditor`, …) stay overridable without becoming actions. */
const baseMethodNames = (): ReadonlySet<string> => new Set(Object.getOwnPropertyNames(Capability.prototype));

/**
 * Whether `value` is an `async function` — the only shape the audit wrapper can cover.
 *
 * The check reads the function's constructor name, which is the one runtime signal JavaScript
 * offers: a `function` returning a promise is indistinguishable from a synchronous one, and an
 * `async function*` reports `AsyncGeneratorFunction` and is (correctly) rejected, exactly as
 * Python's `inspect.iscoroutinefunction` rejects an async generator. The signal survives only while
 * the code is not downleveled — a build targeting a runtime without native `async` would turn every
 * action into a plain function and reject the whole class. The package targets modern Node, where
 * `async` is native, and says so in its `engines`.
 */
const isAsyncFunction = (value: unknown): value is Action =>
  typeof value === 'function' && value.constructor.name === 'AsyncFunction';

/**
 * Wrap an action so the invocation is recorded (capability id, action, bound args) before it runs.
 *
 * The arguments are named as well as JavaScript allows (see {@link bindParameters}); the auditor —
 * not this wrapper — decides what to redact.
 */
const audited = (method: Action, actionName: string): Action => {
  const wrapper = async function (this: Capability, ...args: unknown[]): Promise<unknown> {
    return await this._invokeAction(actionName, bindParameters(args), async () => await method.apply(this, args));
  };
  // The counterpart of `functools.wraps`: a stack trace and a log naming the action, not `wrapper`.
  Object.defineProperty(wrapper, 'name', { value: actionName });
  Object.defineProperty(wrapper, AUDITED_MARKER, { value: true });
  return wrapper;
};

/**
 * Name the arguments of one action call for the audit trail.
 *
 * Python binds them against the signature, so every parameter is recorded by name with its defaults
 * filled in. JavaScript exposes neither parameter names nor default values at runtime, so the port
 * uses the convention this codebase already follows for Python's keyword-only arguments: an action
 * called with **exactly one plain object** is an options object, and its keys are the parameter
 * names. Anything else is recorded positionally as `arg0`, `arg1`, … — which is what the audit
 * consumer needs either way, since it redacts every value and keeps only the keys.
 *
 * "Plain object" excludes a class instance (a DataPoint, a `Date`), whose fields would otherwise be
 * spread into the trail as if they were parameters.
 */
const bindParameters = (args: readonly unknown[]): Readonly<Record<string, unknown>> => {
  const [only] = args;
  if (args.length === 1 && isPlainObject(only)) {
    return { ...only };
  }
  const positional: Record<string, unknown> = {};
  args.forEach((value, index) => {
    positional[`arg${index}`] = value;
  });
  return positional;
};

const isPlainObject = (value: unknown): value is Record<string, unknown> => {
  if (typeof value !== 'object' || value === null) {
    return false;
  }
  const prototype: unknown = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
};

/** Every prototype in the chain that belongs to the capability itself, leaf first. */
const declaredPrototypes = (target: ConcreteCapabilityClass): readonly object[] => {
  const levels: object[] = [];
  let current: unknown = target.prototype;
  while (typeof current === 'object' && current !== null && current !== Capability.prototype) {
    levels.push(current);
    current = Object.getPrototypeOf(current);
  }
  return levels;
};

/**
 * Wrap this class's action methods and reject the ones the wrapper cannot cover.
 *
 * Walks the whole chain (see the module docstring) and wraps each method **on the prototype that
 * declares it**, as Python wraps on the defining class — so two leaves below one family share the
 * family's single wrapper, and the marker keeps a second pass from double-wrapping it.
 */
const installAuditedSeam = (target: ConcreteCapabilityClass): void => {
  const fromBase = baseMethodNames();
  for (const level of declaredPrototypes(target)) {
    for (const name of Object.getOwnPropertyNames(level)) {
      if (name === 'constructor' || name === LIFECYCLE_METHOD || name.startsWith('_') || fromBase.has(name)) {
        continue;
      }
      const descriptor = Object.getOwnPropertyDescriptor(level, name);
      // An accessor has no `value`: a property is not a plain method, so it is neither wrapped nor
      // rejected — the same exemption Python's `inspect.isfunction` gives a `property`.
      if (descriptor === undefined || typeof descriptor.value !== 'function') {
        continue;
      }
      const method = descriptor.value as MarkedAction;
      if (method[AUDITED_MARKER] === true) {
        continue; // already wrapped on this (inherited) class
      }
      if (!isAsyncFunction(method)) {
        // Only an async function can be wrapped, so any other public method would be an action that
        // silently bypasses the audited seam — rejected at definition rather than discovered in
        // production.
        throw new InvalidCapabilityError(
          `${target.name}.${name} is a public non-async method: it would bypass the audited action seam. ` +
            `Make it async, or prefix it with an underscore if it is an internal helper.`,
        );
      }
      Object.defineProperty(level, name, { ...descriptor, value: audited(method, name) });
    }
  }
};

/** Wrap this class's actions, then register it under its `capabilityId`. */
const declareCapability = (target: ConcreteCapabilityClass): void => {
  installAuditedSeam(target);
  // Typed as possibly absent because only an untyped JavaScript caller can get here without one.
  const capabilityId: CapabilityId | undefined = target.capabilityId;
  if (capabilityId === undefined) {
    // Python simply skips a class with no id (an abstract family). Here a decorator IS the
    // declaration of intent to register, so a missing id is a mistake worth naming — and the type
    // system already demands one, so only an untyped JavaScript caller can reach this.
    throw new InvalidCapabilityError(`${target.name} must declare a \`capabilityId\``);
  }
  capabilityRegistry.set(capabilityId, target, (key, existing) => {
    if (existing !== target) {
      throw new DuplicateRegistrationError(`capability_id '${key}' is already registered to ${existing.name}`);
    }
  });
};

/**
 * Declare a concrete capability: audit its actions and register it under its `capabilityId`.
 *
 * ```ts
 * @capability
 * class BreachIntel extends Capability { … }
 * ```
 *
 * The port's `__init_subclass__`. It runs at class-definition time, so a duplicate id raises
 * {@link DuplicateRegistrationError} there, as in Python; re-applying it to the same class (a module
 * reload) is benign. An abstract family is simply not decorated — its actions are still audited,
 * through the concrete provider below it.
 *
 * Two rules the runtime imposes on what counts as an action:
 *
 * - **Public means "not underscore-prefixed".** TypeScript's `private`/`protected` are erased at
 *   compile time, so a `private async lookup()` is an ordinary prototype method and would be
 *   audited like any other action. Name an internal helper `_lookup`, or make it a `#private`
 *   method — a `#` method is not on the prototype at all, so the seam never sees it.
 * - **Async means "declared `async`."** See {@link isAsyncFunction}: a plain function that returns a
 *   promise is rejected, because the wrapper cannot tell it from a synchronous one.
 */
export const capability = <T extends ConcreteCapabilityClass>(target: T, context?: ClassDecoratorContext): void => {
  if (context !== undefined) {
    // A class decorator runs BEFORE the class's static fields are initialized, so `capabilityId`
    // would still be undefined here; a class-decorator initializer runs after they are, which is
    // where the declaration can actually read what the class declared. Both compilers this package
    // is built and tested with agree on that order.
    context.addInitializer(function (this: unknown) {
      declareCapability(this as ConcreteCapabilityClass);
    });
    return;
  }
  // Applied by hand — `capability(SomeClass)`, which is how a class *expression* (a test factory)
  // registers. The class is fully initialized by then, so its statics are readable straight away.
  declareCapability(target);
};
