/**
 * DELTA — per-operator invocation deltas computed from the store watermark.
 *
 * @module
 */

import { describe, expect, it } from 'vitest';
import { InMemoryDataPointStore } from '../src/orcastork/adapters/memory/index.js';
import type { AnyDataPoint } from '../src/orcastork/datapoints/index.js';
import type { CapabilityId } from '../src/orcastork/ids.js';
import { Epoch, OperatorId, SessionId } from '../src/orcastork/ids.js';
import { operatorDelta } from '../src/orcastork/scheduling/index.js';
import { personalEmail, T0, workEmail } from './doubles/datapoints.js';

const SID = SessionId('delta-session');
const OP = OperatorId('delta-op');
const OTHER_OP = OperatorId('delta-op-2');
const EPOCH = Epoch(1);
const NO_CAPS: ReadonlySet<CapabilityId> = new Set();
const T2 = new Date(T0.getTime() + 2 * 60 * 60 * 1000);

const storeWith = async (...points: readonly AnyDataPoint[]): Promise<InMemoryDataPointStore> => {
  const store = new InMemoryDataPointStore();
  await store.write(SID, points, { epoch: EPOCH });
  return store;
};

const valuesOf = (dataPoints: ReadonlySet<AnyDataPoint>): ReadonlySet<unknown> =>
  new Set([...dataPoints].map((dataPoint) => dataPoint.value));

const typesOf = (dataPoints: ReadonlySet<AnyDataPoint>): ReadonlySet<string> =>
  new Set([...dataPoints].map((dataPoint) => dataPoint.type));

describe('invocation deltas', () => {
  it('presents the full set as added on a first invocation', async () => {
    const store = await storeWith(workEmail('a@e.example'), personalEmail('p@e.example'));

    const delta = await operatorDelta(store, SID, { watermark: null, availableCaps: NO_CAPS, previousCaps: NO_CAPS });

    expect(delta.isFirstInvocation).toBe(true);
    expect(valuesOf(delta.added)).toEqual(new Set(['a@e.example', 'p@e.example']));
    expect(delta.updated.size).toBe(0);
  });

  it('reflects only the changes since the watermark on a reinvocation', async () => {
    const store = new InMemoryDataPointStore();
    const firstRev = await store.write(SID, [workEmail('a@e.example')], { epoch: EPOCH });
    await store.setWatermark(SID, OP, firstRev, { epoch: EPOCH });
    await store.write(SID, [personalEmail('p@e.example')], { epoch: EPOCH });

    const delta = await operatorDelta(store, SID, {
      watermark: await store.getWatermark(SID, OP),
      availableCaps: NO_CAPS,
      previousCaps: NO_CAPS,
    });

    expect(delta.isFirstInvocation).toBe(false);
    expect(valuesOf(delta.added)).toEqual(new Set(['p@e.example']));
  });

  it('reports new identities as added and re-observations as updated', async () => {
    const store = new InMemoryDataPointStore();
    const base = await store.write(SID, [workEmail('a@e.example', { last: T0 })], { epoch: EPOCH });
    await store.setWatermark(SID, OP, base, { epoch: EPOCH });
    await store.write(SID, [workEmail('a@e.example', { last: T2 }), personalEmail('p@e.example')], { epoch: EPOCH });

    const delta = await operatorDelta(store, SID, {
      watermark: await store.getWatermark(SID, OP),
      availableCaps: NO_CAPS,
      previousCaps: NO_CAPS,
    });

    expect(typesOf(delta.added)).toEqual(new Set(['personal_email']));
    expect(typesOf(delta.updated)).toEqual(new Set(['work_email']));
  });

  it('reports the capabilities that came online since the last run', async () => {
    const store = await storeWith(workEmail('a@e.example'));

    const delta = await operatorDelta(store, SID, {
      watermark: null,
      availableCaps: new Set(['a', 'b'] as CapabilityId[]),
      previousCaps: new Set(['a'] as CapabilityId[]),
    });

    expect(delta.newlyAvailableCaps).toEqual(new Set(['b']));
  });

  it('reconstructs the delta on resume from the persisted watermark', async () => {
    const store = new InMemoryDataPointStore();
    const rev = await store.write(SID, [workEmail('a@e.example')], { epoch: EPOCH });
    await store.setWatermark(SID, OP, rev, { epoch: EPOCH });
    // "Resume": the watermark is still in the store; a later write is the only delta.
    await store.write(SID, [personalEmail('p@e.example')], { epoch: EPOCH });

    expect(await store.getWatermark(SID, OP)).toBe(rev);

    const delta = await operatorDelta(store, SID, {
      watermark: await store.getWatermark(SID, OP),
      availableCaps: NO_CAPS,
      previousCaps: NO_CAPS,
    });

    expect(valuesOf(delta.added)).toEqual(new Set(['p@e.example']));
  });

  it('degrades a lost watermark to first-invocation semantics', async () => {
    const store = await storeWith(workEmail('a@e.example'));

    expect(await store.getWatermark(SID, OP)).toBeNull(); // never set / lost

    const delta = await operatorDelta(store, SID, {
      watermark: await store.getWatermark(SID, OP),
      availableCaps: NO_CAPS,
      previousCaps: NO_CAPS,
    });

    expect(delta.isFirstInvocation).toBe(true);
    expect(valuesOf(delta.added)).toEqual(new Set(['a@e.example']));
  });

  it('gives each operator an independent watermark', async () => {
    const store = new InMemoryDataPointStore();
    const rev = await store.write(SID, [workEmail('a@e.example')], { epoch: EPOCH });
    await store.setWatermark(SID, OP, rev, { epoch: EPOCH }); // OP has run; OTHER_OP has not

    const opDelta = await operatorDelta(store, SID, {
      watermark: await store.getWatermark(SID, OP),
      availableCaps: NO_CAPS,
      previousCaps: NO_CAPS,
    });
    const otherDelta = await operatorDelta(store, SID, {
      watermark: await store.getWatermark(SID, OTHER_OP),
      availableCaps: NO_CAPS,
      previousCaps: NO_CAPS,
    });

    expect(opDelta.isFirstInvocation).toBe(false);
    expect(otherDelta.isFirstInvocation).toBe(true);
  });

  it('treats a new value for an existing type as added, not a replacement', async () => {
    const store = new InMemoryDataPointStore();
    const base = await store.write(SID, [workEmail('a@e.example')], { epoch: EPOCH });
    await store.setWatermark(SID, OP, base, { epoch: EPOCH });
    await store.write(SID, [workEmail('b@e.example')], { epoch: EPOCH }); // new value, same type

    const delta = await operatorDelta(store, SID, {
      watermark: await store.getWatermark(SID, OP),
      availableCaps: NO_CAPS,
      previousCaps: NO_CAPS,
    });

    expect(valuesOf(delta.added)).toEqual(new Set(['b@e.example']));
    expect((await store.snapshot(SID)).all()).toHaveLength(2); // both coexist
  });

  it('reports an empty delta when nothing changed since the watermark', async () => {
    const store = new InMemoryDataPointStore();
    const rev = await store.write(SID, [workEmail('a@e.example')], { epoch: EPOCH });
    await store.setWatermark(SID, OP, rev, { epoch: EPOCH });

    const delta = await operatorDelta(store, SID, {
      watermark: rev,
      availableCaps: NO_CAPS,
      previousCaps: NO_CAPS,
    });

    expect(delta.added.size).toBe(0);
    expect(delta.updated.size).toBe(0);
  });
});
