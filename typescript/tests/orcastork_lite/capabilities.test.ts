/** Availability fixpoint, lazy activation, isolation of a failed activation, provider preference. */

import { describe, expect, it } from 'vitest';
import { CapabilityId } from '../../src/orcastork_lite/ids.js';
import type { Capability, CapabilityContext, DataPointClass } from '../../src/orcastork_lite/index.js';
import {
  CapabilityActivator,
  CapabilityUnavailableError,
  CapabilityView,
  computeAvailable,
  DataPointView,
  InMemoryCapabilityCatalog,
} from '../../src/orcastork_lite/index.js';
import { FakeClock } from '../doubles/clock.js';
import { dp, Email, Ip, makeCapability, NAMESPACE, WorkEmail } from './fixtures.js';

const BASE = CapabilityId('base');
const LAYER = CapabilityId('layer');
const OTHER = CapabilityId('other');

/** The orchestrator's default operation timeout, which is what bounds an activation. */
const THIRTY_SECONDS = 30_000;

describe('capability availability', () => {
  it('is a fixpoint gated by permission, data and layering', () => {
    const base = makeCapability('base', { dependsOn: [Email] });
    const layer = makeCapability('layer', { requires: [base] });
    const registered = new Map([
      [BASE, base],
      [LAYER, layer],
    ]);

    expect(
      computeAvailable({ registered, permitted: new Set([BASE, LAYER]), presentTypes: new Set<DataPointClass>() }),
    ).toEqual(new Set());
    expect(
      computeAvailable({ registered, permitted: new Set([BASE, LAYER]), presentTypes: new Set([WorkEmail]) }),
    ).toEqual(new Set([BASE, LAYER]));
    expect(computeAvailable({ registered, permitted: new Set([LAYER]), presentTypes: new Set([WorkEmail]) })).toEqual(
      new Set(),
    );
  });
});

describe('CapabilityActivator', () => {
  it('activates lazily, once, base before layer, from the catalog credentials', async () => {
    const clock = new FakeClock();
    const order: string[] = [];
    const contexts: CapabilityContext[] = [];
    const remember = async (ctx: CapabilityContext): Promise<void> => {
      contexts.push(ctx);
    };

    const base = makeCapability('base', { dependsOn: [Email], recordOrder: order, onActivate: remember });
    const layer = makeCapability('layer', { requires: [base], recordOrder: order });
    const catalog = new InMemoryCapabilityCatalog({
      permitted: [[NAMESPACE, [BASE, LAYER]]],
      credentials: [{ namespaceId: NAMESPACE, capabilityId: BASE, credentials: { token: 't-1' } }],
    });
    const activator = new CapabilityActivator(
      [
        [LAYER, layer],
        [BASE, base],
      ],
      catalog,
      NAMESPACE,
      { activationTimeoutMs: THIRTY_SECONDS },
    );

    const empty = await activator.refresh(new DataPointView([]));
    expect(empty.availableIds()).toEqual(new Set());
    expect(order).toEqual([]);

    const view = await activator.refresh(new DataPointView([dp(WorkEmail, 'w', clock.now())]));

    expect(view.availableIds()).toEqual(new Set([BASE, LAYER]));
    expect(order).toEqual(['base', 'layer']);
    expect(await view.require(base).token()).toBe('t-1');
    // So an implementation can cache clients per namespace.
    expect(contexts[0]?.namespaceId).toBe(NAMESPACE);

    await activator.refresh(new DataPointView([dp(WorkEmail, 'w', clock.now())]));
    expect(order).toEqual(['base', 'layer']); // constructed at most once per session
  });

  it('isolates a failed activation and never retries it', async () => {
    const attempts: string[] = [];
    const explode = async (): Promise<void> => {
      await Promise.resolve();
      throw new Error('no network');
    };

    const broken = makeCapability('base', { onActivate: explode, recordOrder: attempts });
    const healthy = makeCapability('other');
    const catalog = new InMemoryCapabilityCatalog({ permitted: [[NAMESPACE, [BASE, OTHER]]] });
    const activator = new CapabilityActivator(
      [
        [BASE, broken],
        [OTHER, healthy],
      ],
      catalog,
      NAMESPACE,
      { activationTimeoutMs: THIRTY_SECONDS },
    );

    let view = new CapabilityView();
    for (let attempt = 0; attempt < 3; attempt += 1) {
      view = await activator.refresh(new DataPointView([]));
    }

    expect(view.availableIds()).toEqual(new Set([OTHER]));
    expect(attempts).toEqual(['base']);
  });

  it('lets a revocation block new resolution without destroying the instance', async () => {
    const cap = makeCapability('base');
    const catalog = new InMemoryCapabilityCatalog({ permitted: [[NAMESPACE, [BASE]]] });
    const activator = new CapabilityActivator([[BASE, cap]], catalog, NAMESPACE, {
      activationTimeoutMs: THIRTY_SECONDS,
    });
    expect((await activator.refresh(new DataPointView([]))).isAvailable(BASE)).toBe(true);

    catalog.setPermitted(NAMESPACE, []);

    expect((await activator.refresh(new DataPointView([]))).isAvailable(BASE)).toBe(false);
    expect(activator.activatedIds()).toEqual(new Set([BASE]));
  });

  it('bounds an activation by the activation timeout, terminally', async () => {
    const attempts: string[] = [];
    // The counterpart of the Python stub's `asyncio.sleep(5.0)`: a client build that never answers.
    // Unref'd, so the bound is what ends the wait and a stray timer never holds the worker open.
    const hang = (): Promise<void> =>
      new Promise<void>((resolve) => {
        setTimeout(resolve, 5_000).unref();
      });

    const hanging = makeCapability('base', { onActivate: hang, recordOrder: attempts });
    const quick = makeCapability('other');
    const catalog = new InMemoryCapabilityCatalog({ permitted: [[NAMESPACE, [BASE, OTHER]]] });
    const activator = new CapabilityActivator(
      [
        [BASE, hanging],
        [OTHER, quick],
      ],
      catalog,
      NAMESPACE,
      { activationTimeoutMs: 20 },
    );

    const view = await activator.refresh(new DataPointView([]));

    expect(view.availableIds()).toEqual(new Set([OTHER]));
    expect(activator.failedIds()).toEqual(new Set([BASE]));
    await activator.refresh(new DataPointView([]));
    expect(attempts).toEqual(['base']); // a timed-out activation is terminal for the session, like a raise
  });
});

describe('CapabilityView', () => {
  it('prefers listed providers, then alphabetical, and require raises when there is none', () => {
    const family = makeCapability('family');
    const alpha = makeCapability('alpha', { base: family });
    const zulu = makeCapability('zulu', { base: family });
    const available = new Map<CapabilityId, Capability>([
      [CapabilityId('zulu'), new zulu()],
      [CapabilityId('alpha'), new alpha()],
    ]);

    expect(new CapabilityView(available).resolve(family)?.constructor).toBe(alpha); // alphabetical tie-break
    expect(new CapabilityView(available, [CapabilityId('zulu')]).resolve(family)?.constructor).toBe(zulu);
    expect(new CapabilityView(available).resolve(makeCapability('none'))).toBeNull();
    expect(new CapabilityView(available).availableTypes()).toEqual(new Set([alpha, zulu]));
    expect(() => new CapabilityView().require(family)).toThrow(CapabilityUnavailableError);
  });
});

describe('InMemoryCapabilityCatalog', () => {
  it('hands back copies, and the unrestricted defaults', async () => {
    const catalog = new InMemoryCapabilityCatalog({
      credentials: [{ namespaceId: NAMESPACE, capabilityId: BASE, credentials: { token: 't' } }],
    });

    const creds = await catalog.credentials(NAMESPACE, BASE);
    expect(creds).toEqual({ token: 't' });
    (creds as Record<string, unknown>).token = 'changed';
    expect(await catalog.credentials(NAMESPACE, BASE)).toEqual({ token: 't' });

    expect(await catalog.credentials(NAMESPACE, OTHER)).toBeNull();
    expect(await catalog.permittedOperators(NAMESPACE)).toBeNull();
    expect(await catalog.preferredOrder(NAMESPACE)).toEqual([]);
    expect(await catalog.permittedCapabilities(NAMESPACE)).toEqual(new Set());
    expect(Ip).not.toBe(Email); // keep the zoo import honest for the reader
  });
});
