/**
 * CAP — capability availability fixpoint, lazy activation, layering, revocation, invocation.
 *
 * The catalog is the in-memory `CapabilityCatalog` adapter, as in Python — `setPermitted` is how a
 * test models a namespace changing its config between grants.
 */

import { SpanStatusCode } from '@opentelemetry/api';
import { describe, expect, it } from 'vitest';
import { InMemoryCapabilityCatalog, InMemoryRateLimiter } from '../src/orcastork/adapters/memory/index.js';
import { RetryPolicy } from '../src/orcastork/aggregation/index.js';
import { AuditKind } from '../src/orcastork/audit/index.js';
import { capabilityRegistry } from '../src/orcastork/capabilities/base.js';
import type {
  CapabilityContext,
  ConcreteCapabilityClass,
  InvocationAuditor,
} from '../src/orcastork/capabilities/index.js';
import { Capability, CapabilityActivator, capability, computeAvailable } from '../src/orcastork/capabilities/index.js';
import type { DataPointEmission } from '../src/orcastork/datapoints/index.js';
import { DataPointView } from '../src/orcastork/datapoints/index.js';
import {
  CapabilityUnavailableError,
  DuplicateRegistrationError,
  InvalidCapabilityError,
} from '../src/orcastork/exceptions.js';
import type { CapabilityId, NamespaceId } from '../src/orcastork/ids.js';
import {
  OperatorId,
  SessionId,
  CapabilityId as toCapabilityId,
  NamespaceId as toNamespaceId,
} from '../src/orcastork/ids.js';
import { Deferred } from '../src/orcastork/internal/deferred.js';
import type { OperatorContext } from '../src/orcastork/operators/index.js';
import { CapabilityView, Operator, OperatorPolicy, operator } from '../src/orcastork/operators/index.js';
import { Orchestrator, SessionStatus } from '../src/orcastork/orchestrator/index.js';
import type { RateLimiter } from '../src/orcastork/ports/index.js';
import { buildInMemoryRuntime } from '../src/orcastork/runtime.js';
import { makeCapability } from './doubles/capabilities.js';
import { FakeClock } from './doubles/clock.js';
import {
  ChatAnswerDataPoint,
  EmailDataPoint,
  personalEmail,
  WorkEmailDataPoint,
  workEmail,
} from './doubles/datapoints.js';
import { captureLogs } from './doubles/logs.js';
import { TelemetryProbe } from './doubles/otel.js';

const NAMESPACE: NamespaceId = toNamespaceId('namespace-1');
const IDP_ID: CapabilityId = toCapabilityId('idp');

const registered = (
  ...capabilities: readonly ConcreteCapabilityClass[]
): ReadonlyMap<CapabilityId, ConcreteCapabilityClass> =>
  new Map(capabilities.map((entry) => [entry.capabilityId, entry]));

const permitting = (...capabilities: readonly CapabilityId[]): InMemoryCapabilityCatalog =>
  new InMemoryCapabilityCatalog({ permitted: [[NAMESPACE, capabilities]] });

/** Abstract capability family (no id → never registered) for resolution-cardinality tests. */
abstract class IdpFamily extends Capability {}

@capability
class Entra extends IdpFamily {
  public static readonly capabilityId = toCapabilityId('entra');

  public async activate(_ctx: CapabilityContext): Promise<void> {
    return;
  }
}

@capability
class Google extends IdpFamily {
  public static readonly capabilityId = toCapabilityId('google');

  public async activate(_ctx: CapabilityContext): Promise<void> {
    return;
  }
}

describe('capability availability and activation', () => {
  it('is available iff permitted, deps present and requires available', () => {
    const cap = makeCapability('idp', { dependsOn: [WorkEmailDataPoint] });
    const table = registered(cap);
    const permitted = new Set([IDP_ID]);
    expect(computeAvailable({ registered: table, permitted, presentTypes: new Set() })).toEqual(new Set());
    expect(computeAvailable({ registered: table, permitted, presentTypes: new Set([WorkEmailDataPoint]) })).toEqual(
      new Set([IDP_ID]),
    );
    expect(
      computeAvailable({ registered: table, permitted: new Set(), presentTypes: new Set([WorkEmailDataPoint]) }),
    ).toEqual(new Set());
  });

  it('activates lazily, only once available', async () => {
    const cap = makeCapability('idp', { dependsOn: [WorkEmailDataPoint] });
    const activator = new CapabilityActivator(registered(cap), permitting(IDP_ID), NAMESPACE, new FakeClock());

    await activator.refresh(new DataPointView([])); // deps missing → not constructed
    expect(cap.activations).toEqual([]);
    await activator.refresh(new DataPointView([workEmail()]));
    expect(cap.activations).toHaveLength(1);
  });

  it('activates a layer only after its base and its deps', async () => {
    const browser = makeCapability('browser');
    const auth = makeCapability('auth_browser', { dependsOn: [WorkEmailDataPoint], requires: [browser] });
    const catalog = permitting(toCapabilityId('browser'), toCapabilityId('auth_browser'));
    const activator = new CapabilityActivator(registered(browser, auth), catalog, NAMESPACE, new FakeClock());

    const withoutEmail = await activator.refresh(new DataPointView([]));
    expect(withoutEmail.isAvailable(toCapabilityId('browser'))).toBe(true);
    expect(withoutEmail.isAvailable(toCapabilityId('auth_browser'))).toBe(false);

    const withEmail = await activator.refresh(new DataPointView([workEmail()]));
    expect(withEmail.isAvailable(toCapabilityId('auth_browser'))).toBe(true);
  });

  it('keeps availability monotonic', () => {
    const cap = makeCapability('idp', { dependsOn: [WorkEmailDataPoint] });
    const table = registered(cap);
    const permitted = new Set([IDP_ID]);
    const empty = computeAvailable({ registered: table, permitted, presentTypes: new Set() });
    const withEmail = computeAvailable({
      registered: table,
      permitted,
      presentTypes: new Set([WorkEmailDataPoint]),
    });
    expect([...empty].every((id) => withEmail.has(id))).toBe(true);
  });

  it('resolves one provider for a capability and every match for a DataPoint type', () => {
    // requires → the single preferred available provider
    const view = new CapabilityView(
      new Map([
        [toCapabilityId('entra'), new Entra()],
        [toCapabilityId('google'), new Google()],
      ]),
    );
    const provider = view.resolve(IdpFamily);
    expect(provider?.capabilityId).toBe(toCapabilityId('entra'));
    // dependsOn → ALL matching DataPoints
    expect(new DataPointView([workEmail(), personalEmail()]).ofType(EmailDataPoint)).toHaveLength(2);
  });

  it('blocks new acquisitions on revocation but keeps the in-flight instance', async () => {
    const cap = makeCapability('idp', { dependsOn: [WorkEmailDataPoint] });
    const catalog = permitting(IDP_ID);
    const activator = new CapabilityActivator(registered(cap), catalog, NAMESPACE, new FakeClock());
    expect((await activator.refresh(new DataPointView([workEmail()]))).isAvailable(IDP_ID)).toBe(true);

    catalog.setPermitted(NAMESPACE, []); // mid-session revocation
    const revoked = await activator.refresh(new DataPointView([workEmail()]));
    expect(revoked.isAvailable(IDP_ID)).toBe(false); // new acquisitions blocked
    expect(activator.activatedIds().has(IDP_ID)).toBe(true); // in-flight instance not destroyed
  });

  it('never flips availability on a value change', async () => {
    const cap = makeCapability('idp', { dependsOn: [WorkEmailDataPoint] });
    const activator = new CapabilityActivator(registered(cap), permitting(IDP_ID), NAMESPACE, new FakeClock());
    const first = await activator.refresh(new DataPointView([workEmail('a@e.example')]));
    const second = await activator.refresh(new DataPointView([workEmail('b@e.example')]));
    expect(first.availableIds()).toEqual(second.availableIds());
  });

  it('brings a capability online when its dep arrives', async () => {
    const cap = makeCapability('idp', { dependsOn: [WorkEmailDataPoint] });
    const activator = new CapabilityActivator(registered(cap), permitting(IDP_ID), NAMESPACE, new FakeClock());
    expect((await activator.refresh(new DataPointView([]))).isAvailable(IDP_ID)).toBe(false);
    expect((await activator.refresh(new DataPointView([workEmail()]))).isAvailable(IDP_ID)).toBe(true);
  });

  it('activates with the catalog credentials', async () => {
    const cap = makeCapability('idp', { dependsOn: [WorkEmailDataPoint] });
    const catalog = new InMemoryCapabilityCatalog({
      permitted: [[NAMESPACE, [IDP_ID]]],
      credentials: [{ namespaceId: NAMESPACE, capabilityId: IDP_ID, credentials: { token: 'secret' } }],
    });
    const activator = new CapabilityActivator(registered(cap), catalog, NAMESPACE, new FakeClock());
    await activator.refresh(new DataPointView([workEmail()]));
    expect(cap.activations).toEqual([{ token: 'secret' }]);
  });

  it('never activates a capability whose prereqs stay unmet', async () => {
    const cap = makeCapability('idp', { dependsOn: [WorkEmailDataPoint] });
    const activator = new CapabilityActivator(registered(cap), permitting(IDP_ID), NAMESPACE, new FakeClock());
    for (let attempt = 0; attempt < 3; attempt += 1) {
      await activator.refresh(new DataPointView([personalEmail()])); // wrong leaf, never satisfies
    }
    expect(cap.activations).toEqual([]);
  });

  it('terminates a layered chain in dependency order', async () => {
    const order: CapabilityId[] = [];
    const a = makeCapability('a', { recordOrder: order });
    const b = makeCapability('b', { requires: [a], recordOrder: order });
    const c = makeCapability('c', { requires: [b], recordOrder: order });
    const catalog = permitting(toCapabilityId('a'), toCapabilityId('b'), toCapabilityId('c'));
    const activator = new CapabilityActivator(registered(a, b, c), catalog, NAMESPACE, new FakeClock());

    const view = await activator.refresh(new DataPointView([]));
    expect(view.availableIds()).toEqual(new Set([toCapabilityId('a'), toCapabilityId('b'), toCapabilityId('c')]));
    expect(order.indexOf(toCapabilityId('a'))).toBeLessThan(order.indexOf(toCapabilityId('b')));
    expect(order.indexOf(toCapabilityId('b'))).toBeLessThan(order.indexOf(toCapabilityId('c')));
  });

  it('selects the preferred provider deterministically', () => {
    // Insertion order varies; the preferred provider (stable tie-break by id) does not.
    const view = new CapabilityView(
      new Map([
        [toCapabilityId('google'), new Google()],
        [toCapabilityId('entra'), new Entra()],
      ]),
    );
    const first = view.resolve(IdpFamily);
    const second = view.resolve(IdpFamily);
    expect(first?.capabilityId).toBe(toCapabilityId('entra'));
    expect(second?.capabilityId).toBe(toCapabilityId('entra'));
  });

  it('lets the namespace preference override the alphabetical resolution', () => {
    // 'entra' sorts first alphabetically, but the namespace prefers google — the preference wins.
    const view = new CapabilityView(
      new Map([
        [toCapabilityId('entra'), new Entra()],
        [toCapabilityId('google'), new Google()],
      ]),
      [toCapabilityId('google')],
    );
    expect(view.resolve(IdpFamily)?.capabilityId).toBe(toCapabilityId('google'));
  });

  it('ranks unlisted providers after listed ones', () => {
    // A listed provider beats every unlisted one, even when an absent id leads the preference list
    // and the unlisted provider would win the alphabetical tie-break.
    const view = new CapabilityView(
      new Map([
        [toCapabilityId('entra'), new Entra()],
        [toCapabilityId('google'), new Google()],
      ]),
      [toCapabilityId('okta'), toCapabilityId('google')],
    );
    expect(view.resolve(IdpFamily)?.capabilityId).toBe(toCapabilityId('google'));
  });

  it('keeps the alphabetical tie-break when the preference is empty', () => {
    const view = new CapabilityView(
      new Map([
        [toCapabilityId('google'), new Google()],
        [toCapabilityId('entra'), new Entra()],
      ]),
      [],
    );
    expect(view.resolve(IdpFamily)?.capabilityId).toBe(toCapabilityId('entra'));
  });

  it('threads the namespace preference from the catalog into the view', async () => {
    /** Abstract family (no id → never registered) the namespace's providers share. */
    abstract class Family extends Capability {}

    @capability
    class GoogleIdp extends Family {
      public static readonly capabilityId = toCapabilityId('idp.google');

      public async activate(_ctx: CapabilityContext): Promise<void> {
        return;
      }
    }

    @capability
    class EntraIdp extends Family {
      public static readonly capabilityId = toCapabilityId('idp.entra');

      public async activate(_ctx: CapabilityContext): Promise<void> {
        return;
      }
    }

    const catalog = new InMemoryCapabilityCatalog({
      permitted: [[NAMESPACE, [toCapabilityId('idp.google'), toCapabilityId('idp.entra')]]],
      preferred: [[NAMESPACE, [toCapabilityId('idp.google')]]],
    });
    const activator = new CapabilityActivator(registered(GoogleIdp, EntraIdp), catalog, NAMESPACE, new FakeClock());

    const provider = (await activator.refresh(new DataPointView([]))).resolve(Family);

    // Preferred, although 'idp.entra' sorts first.
    expect(provider?.capabilityId).toBe(toCapabilityId('idp.google'));
  });

  it('keeps a requires-blocked layer unavailable independently of its own deps', () => {
    // Condition (3) of the fixpoint gates a layer on its requires being AVAILABLE — not merely on
    // its own deps being present. A is registered but NOT permitted, so it can never enter
    // `available`; B (permitted, deps satisfied) must therefore stay perpetually out of the result.
    // This isolates the requires-blocked branch from the deps-missing branch.
    const base = makeCapability('base');
    const layer = makeCapability('layer', { dependsOn: [WorkEmailDataPoint], requires: [base] });
    const table = registered(base, layer);

    // A registered-but-unpermitted base keeps the layer out even with the layer's own deps present.
    const blocked = computeAvailable({
      registered: table,
      permitted: new Set([toCapabilityId('layer')]),
      presentTypes: new Set([WorkEmailDataPoint]),
    });
    expect(blocked).toEqual(new Set()); // the requires gate alone excludes the layer

    // Permitting the base (its deps are empty, so present) lets the fixpoint admit both, base first.
    const both = computeAvailable({
      registered: table,
      permitted: new Set([toCapabilityId('base'), toCapabilityId('layer')]),
      presentTypes: new Set([WorkEmailDataPoint]),
    });
    expect(both).toEqual(new Set([toCapabilityId('base'), toCapabilityId('layer')]));
  });
});

describe('the audited action seam', () => {
  it('calls an action directly, typed, and audits it at the seam', async () => {
    const performed: { action: string; parameters: Record<string, unknown> }[] = [];
    const audited: { capabilityId: CapabilityId; action: string; parameters: Record<string, unknown> }[] = [];

    @capability
    class Idp extends Capability {
      public static readonly capabilityId = IDP_ID;

      public async activate(_ctx: CapabilityContext): Promise<void> {
        return;
      }

      /** A real, typed action method. */
      public async sendChallenge(options: { readonly userId: string }): Promise<string> {
        performed.push({ action: 'sendChallenge', parameters: { ...options } });
        return 'challenge-sent';
      }
    }

    const record: InvocationAuditor = async (capabilityId, action, parameters) => {
      audited.push({ capabilityId, action, parameters: { ...parameters } });
    };

    const idp = new Idp();
    idp.bindAuditor(record); // the orchestrator wires this when the capability activates
    const result = await idp.sendChallenge({ userId: 'u-1' }); // direct, statically-typed call

    expect(result).toBe('challenge-sent'); // the action's result is returned to the operator
    expect(performed).toEqual([{ action: 'sendChallenge', parameters: { userId: 'u-1' } }]);
    // The seam sees raw parameters by name; redacting is the orchestrator's job at the audit boundary.
    expect(audited).toEqual([{ capabilityId: IDP_ID, action: 'sendChallenge', parameters: { userId: 'u-1' } }]);
  });

  it('raises when a required capability is unavailable', () => {
    @capability
    class Idp extends Capability {
      public static readonly capabilityId = IDP_ID;

      public async activate(_ctx: CapabilityContext): Promise<void> {
        return;
      }
    }

    expect(() => new CapabilityView().require(Idp)).toThrow(CapabilityUnavailableError);
  });

  it('audits only public actions, never private helpers', async () => {
    const audited: string[] = [];

    @capability
    class Idp extends Capability {
      public static readonly capabilityId = IDP_ID;

      public async activate(_ctx: CapabilityContext): Promise<void> {
        return;
      }

      /** Public → an audited action. */
      public async lookup(options: { readonly key: string }): Promise<string> {
        return await this._fetch(options.key);
      }

      /** Underscored helper → not an audited action. */
      private async _fetch(key: string): Promise<string> {
        return `value-${key}`;
      }
    }

    const idp = new Idp();
    idp.bindAuditor(async (_capabilityId, action) => {
      audited.push(action);
    });
    expect(await idp.lookup({ key: 'k' })).toBe('value-k');
    expect(audited).toEqual(['lookup']); // only the public action; the private helper it called did not
  });

  it('rejects a public sync method at definition', () => {
    expect(() => {
      @capability
      class Leaky extends Capability {
        public static readonly capabilityId = toCapabilityId('leaky');

        public async activate(_ctx: CapabilityContext): Promise<void> {
          return;
        }

        /** Public sync → would silently bypass the audit wrapper. */
        public fetchToken(): string {
          return 'token';
        }
      }
      return Leaky;
    }).toThrow(/fetchToken/);
    expect(() => {
      @capability
      class AlsoLeaky extends Capability {
        public static readonly capabilityId = toCapabilityId('also_leaky');

        public async activate(_ctx: CapabilityContext): Promise<void> {
          return;
        }

        public fetchToken(): string {
          return 'token';
        }
      }
      return AlsoLeaky;
    }).toThrow(InvalidCapabilityError);
  });

  it('allows an underscored sync helper', () => {
    @capability
    class WithHelper extends Capability {
      public static readonly capabilityId = toCapabilityId('with_helper');

      public async activate(_ctx: CapabilityContext): Promise<void> {
        return;
      }

      /** Internal helper → not an action, never audited. */
      public _normalize(raw: string): string {
        return raw.trim();
      }
    }

    expect(new WithHelper()._normalize(' x ')).toBe('x');
  });

  it('allows overriding a base-defined method', () => {
    @capability
    class Override extends Capability {
      public static readonly capabilityId = toCapabilityId('override');

      public async activate(_ctx: CapabilityContext): Promise<void> {
        return;
      }

      /** Framework lifecycle, synchronous on the base. */
      public override bindAuditor(auditor: InvocationAuditor | null): void {
        super.bindAuditor(auditor);
      }
    }

    expect(capabilityRegistry.get(toCapabilityId('override'))).toBe(Override);
  });

  it('does not trip the sync check on accessors, statics or class fields', () => {
    @capability
    class Descriptors extends Capability {
      public static readonly capabilityId = toCapabilityId('descriptors');

      /** Plain instance field — never on the prototype, so never scanned. */
      public region = 'eu';

      public async activate(_ctx: CapabilityContext): Promise<void> {
        return;
      }

      public get endpoint(): string {
        return 'https://idp.example';
      }

      public static familyName(): string {
        return Descriptors.name;
      }

      public static version(): number {
        return 1;
      }
    }

    expect(capabilityRegistry.get(toCapabilityId('descriptors'))).toBe(Descriptors);
    expect(new Descriptors().endpoint).toBe('https://idp.example');
    expect(Descriptors.familyName()).toBe('Descriptors');
    expect(Descriptors.version()).toBe(1);
  });

  it('acquires the rate limit before auditing and before the body', async () => {
    const order: string[] = [];

    class RecordingLimiter implements RateLimiter {
      public async acquire(key: string): Promise<void> {
        order.push(`limit:${key}`);
      }
    }

    @capability
    class Idp extends Capability {
      public static readonly capabilityId = IDP_ID;

      public async activate(_ctx: CapabilityContext): Promise<void> {
        return;
      }

      public async ping(): Promise<string> {
        order.push('body');
        return 'pong';
      }
    }

    const idp = new Idp();
    idp.bindAuditor(async () => {
      order.push('audit');
    });
    idp.bindRateLimit(new RecordingLimiter(), 'namespace-1:idp');

    expect(await idp.ping()).toBe('pong');
    // The action is paced first, recorded second, executed last — the audit trail holds only
    // invocations that actually got past the fleet's rate limit.
    expect(order).toEqual(['limit:namespace-1:idp', 'audit', 'body']);
  });

  it('binds the namespace-scoped rate-limit key at activation', async () => {
    const acquired: string[] = [];

    class RecordingLimiter implements RateLimiter {
      public async acquire(key: string): Promise<void> {
        acquired.push(key);
      }
    }

    @capability
    class Idp extends Capability {
      public static readonly capabilityId = IDP_ID;

      public async activate(_ctx: CapabilityContext): Promise<void> {
        return;
      }

      public async ping(): Promise<string> {
        return 'pong';
      }
    }

    const activator = new CapabilityActivator(registered(Idp), permitting(IDP_ID), NAMESPACE, new FakeClock(), {
      rateLimiter: new RecordingLimiter(),
    });

    const view = await activator.refresh(new DataPointView([]));
    await view.require(Idp).ping();

    // The fleet bucket is keyed per (namespace, capability).
    expect(acquired).toEqual([`${NAMESPACE}:${IDP_ID}`]);
  });

  it('still paces and audits before the body of an action that throws', async () => {
    // The audited seam is `span -> acquireRateLimit -> recordInvocation -> body`. A failing body
    // must still have consumed a token and written the audit record (pacing and recording happen
    // BEFORE the body), and the exception must propagate out through the span (marked failed).
    const order: string[] = [];

    class RecordingLimiter implements RateLimiter {
      public async acquire(key: string): Promise<void> {
        order.push(`limit:${key}`);
      }
    }

    class BoomError extends Error {}

    @capability
    class Idp extends Capability {
      public static readonly capabilityId = IDP_ID;

      public async activate(_ctx: CapabilityContext): Promise<void> {
        return;
      }

      public async act(): Promise<string> {
        order.push('body');
        throw new BoomError('downstream failed');
      }
    }

    const probe = new TelemetryProbe();
    const idp = new Idp();
    idp.bindAuditor(async () => {
      order.push('audit');
    });
    idp.bindRateLimit(new RecordingLimiter(), 'namespace-1:idp');
    idp.bindTelemetry(probe.telemetry);

    await expect(idp.act()).rejects.toThrow(BoomError);

    // A failed external action still costs a token and a recorded invocation: the trail holds every
    // action that actually proceeded past the limiter, even one whose body then threw.
    expect(order).toEqual(['limit:namespace-1:idp', 'audit', 'body']);
    const spans = probe.spans('capability.action idp.act');
    expect(spans).toHaveLength(1);
    expect(spans[0]?.status.code).toBe(SpanStatusCode.ERROR); // the throw propagated through the span
  });

  it.skip('test_cap_36_audit_binds_positional_args_by_name_and_fills_defaults — JavaScript has neither', () => {
    // Python binds the call against the signature, so `query('k')` is recorded as
    // `{'key': 'k', 'limit': 5}`. JavaScript exposes neither parameter names nor default values at
    // runtime; the rule the port uses instead is covered by the next case.
  });

  it('names parameters from a single options object and falls back to positions otherwise', async () => {
    // The port's replacement for Python's signature binding: an action called with exactly one plain
    // object is an options object (the shape this codebase gives Python's keyword-only arguments),
    // so its keys are the parameter names. Anything else is recorded positionally — which is all the
    // audit consumer needs, since it redacts every value and keeps only the keys.
    const audited: Record<string, unknown>[] = [];

    @capability
    class Idp extends Capability {
      public static readonly capabilityId = IDP_ID;

      public async activate(_ctx: CapabilityContext): Promise<void> {
        return;
      }

      public async query(options: { readonly key: string; readonly limit?: number }): Promise<string> {
        return `${options.key}:${options.limit ?? 5}`;
      }

      public async positional(key: string, limit: number): Promise<string> {
        return `${key}:${limit}`;
      }
    }

    const idp = new Idp();
    idp.bindAuditor(async (_capabilityId, _action, parameters) => {
      audited.push({ ...parameters });
    });

    expect(await idp.query({ key: 'k' })).toBe('k:5');
    expect(await idp.positional('k', 7)).toBe('k:7');
    expect(audited).toEqual([{ key: 'k' }, { arg0: 'k', arg1: 7 }]);
  });

  it('rejects a duplicate capabilityId but not a re-declaration of the same class', () => {
    @capability
    class Foo extends Capability {
      public static readonly capabilityId = toCapabilityId('dup');

      public async activate(_ctx: CapabilityContext): Promise<void> {
        return;
      }
    }

    // A second, DISTINCT class colliding on the same id is rejected at class-definition time.
    expect(() => {
      @capability
      class Bar extends Capability {
        public static readonly capabilityId = toCapabilityId('dup');

        public async activate(_ctx: CapabilityContext): Promise<void> {
          return;
        }
      }
      return Bar;
    }).toThrow(DuplicateRegistrationError);

    // Re-declaring the SAME class (an idempotent re-import) is allowed.
    capability(Foo);
    expect(capabilityRegistry.get(toCapabilityId('dup'))).toBe(Foo);
  });
});

describe('activation failure, cool-off and terminal disposition', () => {
  it('isolates an activation failure and logs it with the error', async () => {
    const cap = makeCapability('idp', {
      dependsOn: [WorkEmailDataPoint],
      activateError: new Error('no creds'),
    });
    const activator = new CapabilityActivator(registered(cap), permitting(IDP_ID), NAMESPACE, new FakeClock());

    const { records, result: view } = await captureLogs(
      async () => await activator.refresh(new DataPointView([workEmail()])),
    );

    expect(view.isAvailable(IDP_ID)).toBe(false); // the failure is isolated: unavailable, nothing thrown
    const failure = records.find((record) => record.fields.capability_id === IDP_ID);
    expect(failure).toBeDefined();
    // The log carries the error itself, not just the capability id — the bridge lifts it onto the
    // record's `exception.*` attributes.
    const logged = failure?.fields.error;
    expect(logged).toBeInstanceOf(Error);
    expect((logged as Error).message).toContain('no creds');
    const retry = await activator.refresh(new DataPointView([workEmail()]));
    expect(retry.isAvailable(IDP_ID)).toBe(false); // still inside the cool-off → not re-attempted yet
    expect(cap.attempts).toBe(1);
  });

  it('retries a failed activation after the cool-off and succeeds', async () => {
    const clock = new FakeClock();
    const cap = makeCapability('idp', {
      dependsOn: [WorkEmailDataPoint],
      activateErrors: [new Error('transient')],
    });
    const activator = new CapabilityActivator(registered(cap), permitting(IDP_ID), NAMESPACE, clock, {
      activationRetry: RetryPolicy({ maxAttempts: 3, baseDelayMs: 1000, jitter: 0 }),
    });

    const first = await activator.refresh(new DataPointView([workEmail()]));
    expect(first.isAvailable(IDP_ID)).toBe(false); // the transient failure leaves it unavailable for now

    const stillCooling = await activator.refresh(new DataPointView([workEmail()]));
    expect(stillCooling.isAvailable(IDP_ID)).toBe(false);
    expect(cap.attempts).toBe(1); // inside the cool-off → no re-attempt

    clock.advance(1000); // past the first cool-off (baseDelayMs * 2**0, jitter disabled)
    const recovered = await activator.refresh(new DataPointView([workEmail()]));
    expect(recovered.isAvailable(IDP_ID)).toBe(true); // the re-attempt succeeded — the subgraph is back
    expect(cap.attempts).toBe(2);
  });

  it('reports exhausted activation retries once and never re-attempts', async () => {
    const clock = new FakeClock();
    const terminal: { capabilityId: CapabilityId; message: string }[] = [];

    const cap = makeCapability('idp', {
      dependsOn: [WorkEmailDataPoint],
      activateError: new Error('no creds'),
    });
    const activator = new CapabilityActivator(registered(cap), permitting(IDP_ID), NAMESPACE, clock, {
      activationRetry: RetryPolicy({ maxAttempts: 2, baseDelayMs: 1000, jitter: 0 }),
      onTerminalFailure: async (capabilityId, error) => {
        terminal.push({ capabilityId, message: (error as Error).message });
      },
    });

    await activator.refresh(new DataPointView([workEmail()])); // attempt 1 fails → cooling off
    expect(terminal).toEqual([]);
    clock.advance(1000);
    await activator.refresh(new DataPointView([workEmail()])); // attempt 2 fails → retries exhausted
    expect(terminal).toEqual([{ capabilityId: IDP_ID, message: 'no creds' }]);

    clock.advance(3_600_000); // however long the session runs, a terminal failure is never re-attempted
    const final = await activator.refresh(new DataPointView([workEmail()]));
    expect(final.isAvailable(IDP_ID)).toBe(false);
    expect(cap.attempts).toBe(2);
    expect(terminal).toHaveLength(1); // the terminal callback fired exactly once
  });

  it('leaves a first-try success unaffected by the retry machinery', async () => {
    const clock = new FakeClock();
    const cap = makeCapability('idp', { dependsOn: [WorkEmailDataPoint] });
    const activator = new CapabilityActivator(registered(cap), permitting(IDP_ID), NAMESPACE, clock);

    const view = await activator.refresh(new DataPointView([workEmail()]));
    expect(view.isAvailable(IDP_ID)).toBe(true);
    clock.advance(3_600_000);
    const again = await activator.refresh(new DataPointView([workEmail()]));
    expect(again.isAvailable(IDP_ID)).toBe(true);
    expect(cap.attempts).toBe(1); // activated once, never re-attempted
  });

  it('activates a layer only once its failed base recovers on a later refresh', async () => {
    // Base A fails activation once (then succeeds); layer B requires A. On the first refresh A fails
    // and B (requires not yet activated) is never built — the activation fixpoint terminates via the
    // `ready.length === 0` break, no exception. After A's cool-off elapses, the next refresh
    // activates A and THEN B in the same pass.
    const clock = new FakeClock();
    const order: CapabilityId[] = [];
    const base = makeCapability('base_layer', { recordOrder: order, activateErrors: [new Error('transient')] });
    const layer = makeCapability('top_layer', { requires: [base], recordOrder: order });
    const catalog = permitting(toCapabilityId('base_layer'), toCapabilityId('top_layer'));
    const activator = new CapabilityActivator(registered(base, layer), catalog, NAMESPACE, clock, {
      activationRetry: RetryPolicy({ maxAttempts: 3, baseDelayMs: 1000, jitter: 0 }),
    });

    const first = await activator.refresh(new DataPointView([]));
    expect(first.isAvailable(toCapabilityId('base_layer'))).toBe(false); // A's first attempt failed
    expect(first.isAvailable(toCapabilityId('top_layer'))).toBe(false); // B never built without A activated
    expect(activator.activatedIds().has(toCapabilityId('top_layer'))).toBe(false); // not stuck-activated
    expect(base.attempts).toBe(1);
    expect(layer.attempts).toBe(0); // the layer was never even constructed

    clock.advance(1000); // past A's first cool-off (baseDelayMs * 2**0, jitter disabled)
    const recovered = await activator.refresh(new DataPointView([]));
    expect(recovered.isAvailable(toCapabilityId('base_layer'))).toBe(true);
    expect(recovered.isAvailable(toCapabilityId('top_layer'))).toBe(true); // B finally activates in the same refresh
    expect(order).toEqual([toCapabilityId('base_layer'), toCapabilityId('top_layer')]); // base before layer
  });

  it('keeps a layer unavailable forever when its base is terminal', async () => {
    // A's retry budget is exhausted on the first attempt (maxAttempts=1 → terminal immediately); B
    // requires A. B must remain permanently unavailable across many refreshes without wedging, and
    // the loop must terminate every pass (no busy-loop on the unready layer).
    const clock = new FakeClock();
    const base = makeCapability('dead_base', { activateError: new Error('no creds') });
    const layer = makeCapability('dead_top', { requires: [base] });
    const catalog = permitting(toCapabilityId('dead_base'), toCapabilityId('dead_top'));
    const activator = new CapabilityActivator(registered(base, layer), catalog, NAMESPACE, clock, {
      activationRetry: RetryPolicy({ maxAttempts: 1 }),
    });

    for (let pass = 0; pass < 5; pass += 1) {
      clock.advance(3_600_000); // however long the session runs, a terminal base never recovers
      const view = await activator.refresh(new DataPointView([]));
      expect(view.isAvailable(toCapabilityId('dead_base'))).toBe(false);
      expect(view.isAvailable(toCapabilityId('dead_top'))).toBe(false); // the layer is permanently blocked
    }
    expect(base.attempts).toBe(1); // terminal after one attempt, never re-tried
    expect(layer.attempts).toBe(0); // the layer was never constructed
  });

  it('does not crash on a terminal activation with no callback wired', async () => {
    // With onTerminalFailure left unset (the orchestrator wired no notifier), exhausting the retry
    // budget must record the terminal disposition and return — never try to call the missing
    // callback, and stay terminal forever.
    const clock = new FakeClock();
    const cap = makeCapability('idp', {
      dependsOn: [WorkEmailDataPoint],
      activateError: new Error('no creds'),
    });
    const activator = new CapabilityActivator(registered(cap), permitting(IDP_ID), NAMESPACE, clock, {
      activationRetry: RetryPolicy({ maxAttempts: 2, baseDelayMs: 1000, jitter: 0 }),
      // onTerminalFailure deliberately omitted
    });

    await activator.refresh(new DataPointView([workEmail()])); // attempt 1 fails → cooling off
    expect(cap.attempts).toBe(1);
    clock.advance(1000);
    const final = await activator.refresh(new DataPointView([workEmail()])); // attempt 2 fails → terminal
    expect(final.isAvailable(IDP_ID)).toBe(false);
    expect(cap.attempts).toBe(2);

    clock.advance(3_600_000); // a terminal failure is never re-attempted, even with no notifier wired
    const again = await activator.refresh(new DataPointView([workEmail()]));
    expect(again.isAvailable(IDP_ID)).toBe(false);
    expect(cap.attempts).toBe(2); // no re-attempt; the missing callback never crashed the run
  });
});

describe('a session activating and calling capabilities', () => {
  it('audits a terminal activation failure exactly once', async () => {
    const session = SessionId('cap-session');
    const catalog = new InMemoryCapabilityCatalog({ permitted: [[NAMESPACE, [IDP_ID]]] });
    const runtime = buildInMemoryRuntime(new FakeClock(), { catalog });
    const cap = makeCapability('idp', { activateError: new Error('no creds') });

    const result = await new Orchestrator({
      sessionId: session,
      namespaceId: NAMESPACE,
      runtime,
      operators: [],
      capabilities: [cap],
      retryPolicy: RetryPolicy({ maxAttempts: 1 }), // the first failure exhausts the budget
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED); // the lost capability degrades, never wedges
    const failures = (await runtime.audit.replay(session)).filter(
      (entry) => entry.kind === AuditKind.CAPABILITY_ACTIVATION_FAILED,
    );
    expect(failures).toHaveLength(1); // terminal disposition recorded exactly once
    const info = failures[0]?.capability;
    expect(info).not.toBeNull();
    expect(info?.capabilityId).toBe(IDP_ID);
    expect(info?.error ?? '').toContain('no creds');
    expect(failures[0]?.epoch).toBe(result.epoch); // epoch-stamped like every other audit entry
  });

  it('isolates a rate-limit wait cut short by the per-operation timeout and skips its audit', async () => {
    // S1: a capability action paces at the audited seam BEFORE recording. When the fleet limiter wait
    // genuinely outlasts the per-operator timeout, the bound cuts the operator off mid-acquire: the
    // failure is isolated (the session still COMPLETES + aggregates), and because pacing precedes
    // recording, the cut-short-before-proceeding action leaves NO audit entry — the trail holds only
    // actions that got past the limiter.
    //
    // Python blocks on an `asyncio.Event` that is never set and lets task cancellation raise into the
    // await; a promise cannot be cancelled, so the port blocks on a `Deferred` that is never resolved
    // — the wait genuinely never returns, which is what the bound is there to survive.
    const blocked = new Deferred<void>(); // never resolved → the limiter wait blocks until the bound fires

    class BlockingLimiter implements RateLimiter {
      public async acquire(_key: string): Promise<void> {
        await blocked.promise;
      }
    }

    @capability
    class Idp extends Capability {
      public static readonly capabilityId = IDP_ID;
      public static readonly dependsOn = [EmailDataPoint];

      public async activate(_ctx: CapabilityContext): Promise<void> {
        return;
      }

      public async callOut(): Promise<string> {
        return 'ok';
      }
    }

    class Caller extends Operator {
      public static readonly operatorId = OperatorId('caller');
      public static readonly policy = OperatorPolicy({ rerunOnNewData: false });
      public static readonly dependsOn = [EmailDataPoint];
      public static readonly requires = [Idp];
      public static readonly produces = [ChatAnswerDataPoint];

      public async *run(ctx: OperatorContext): AsyncIterable<DataPointEmission> {
        const idp = ctx.capabilities.require(Idp);
        const answer = await idp.callOut(); // blocks in the limiter wait until the per-op bound fires
        yield ChatAnswerDataPoint.emit(answer);
      }
    }
    operator(Caller);

    const session = SessionId('cap-rl-session');
    const catalog = new InMemoryCapabilityCatalog({ permitted: [[NAMESPACE, [IDP_ID]]] });
    const runtime = buildInMemoryRuntime(new FakeClock(), { catalog, rateLimiter: new BlockingLimiter() });

    const result = await new Orchestrator({
      sessionId: session,
      namespaceId: NAMESPACE,
      runtime,
      operators: [Caller],
      capabilities: [Idp],
      seed: [workEmail()],
      operationTimeoutMs: 20, // real-time bound; the limiter wait never returns, so the bound cuts it off
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED); // the cut-short action is isolated; the session finishes
    const invoked = (await runtime.audit.replay(session)).filter(
      (entry) => entry.kind === AuditKind.CAPABILITY_INVOKED,
    );
    expect(invoked).toEqual([]); // paced-then-audited: cut off before proceeding → never recorded
  });

  it('paces a rate-limited action inside a run, then proceeds and audits it', async () => {
    // The companion to the previous case: when the fleet limiter wait DOES complete (it only paces,
    // never fails), the deterministic FakeClock-driven InMemoryRateLimiter sleep is fast-forwarded,
    // the action proceeds, and exactly one audit entry is recorded. A second action on the same
    // bucket waits out the refill — but still proceeds and audits, because the limiter waits, it
    // never fails.
    const clock = new FakeClock();

    @capability
    class Idp extends Capability {
      public static readonly capabilityId = IDP_ID;
      public static readonly dependsOn = [EmailDataPoint];

      public async activate(_ctx: CapabilityContext): Promise<void> {
        return;
      }

      public async callOut(options: { readonly marker: string }): Promise<string> {
        return options.marker;
      }
    }

    class Caller extends Operator {
      public static readonly operatorId = OperatorId('caller');
      public static readonly policy = OperatorPolicy({ rerunOnNewData: false });
      public static readonly dependsOn = [EmailDataPoint];
      public static readonly requires = [Idp];
      public static readonly produces = [ChatAnswerDataPoint];

      public async *run(ctx: OperatorContext): AsyncIterable<DataPointEmission> {
        const idp = ctx.capabilities.require(Idp);
        yield ChatAnswerDataPoint.emit(await idp.callOut({ marker: 'first' })); // consumes the burst token
        yield ChatAnswerDataPoint.emit(await idp.callOut({ marker: 'second' })); // waits out the refill
      }
    }
    operator(Caller);

    const session = SessionId('cap-rl-pace-session');
    const catalog = new InMemoryCapabilityCatalog({ permitted: [[NAMESPACE, [IDP_ID]]] });
    // The second acquire sleeps ~10 s on the clock.
    const limiter = new InMemoryRateLimiter(clock, { ratePerSecond: 0.1, burst: 1 });
    const runtime = buildInMemoryRuntime(clock, { catalog, rateLimiter: limiter });
    const startedAt = clock.monotonic();

    const result = await new Orchestrator({
      sessionId: session,
      namespaceId: NAMESPACE,
      runtime,
      operators: [Caller],
      capabilities: [Idp],
      seed: [workEmail()],
      sessionDeadlineMs: 300_000, // ample budget; the limiter sleep is fast-forwarded on the FakeClock
    }).run();

    expect(result.status).toBe(SessionStatus.COMPLETED);
    const answers = new Set(
      (await runtime.store.snapshot(session)).ofType(ChatAnswerDataPoint).map((dataPoint) => dataPoint.value),
    );
    expect(answers).toEqual(new Set(['first', 'second'])); // both paced actions proceeded — the limiter waited
    // The second acquire genuinely WAITED (it did not drop or skip): the burst was one token, so
    // refilling one at 0.1/s fast-forwards the injected clock ~10 s — observable proof of the pacing.
    expect(clock.monotonic() - startedAt).toBeGreaterThanOrEqual(9_000);
    const invoked = (await runtime.audit.replay(session)).filter(
      (entry) => entry.kind === AuditKind.CAPABILITY_INVOKED,
    );
    expect(invoked).toHaveLength(2); // exactly the two actions that got past the limiter are in the trail
  });
});
