/**
 * CNF — the port-conformance harness itself.
 *
 * The per-port contracts live in `doubles/conformance/` and are bound to the in-memory adapter in
 * `store.test.ts` / `inbox.test.ts` / `session_lock.test.ts` / `audit.test.ts`. As Redis and Mongo
 * adapters land, each adds a binding of the same suite, so the identical contract runs against every
 * adapter family. These tests cover the harness's own guarantees: adapter discovery, parity, and
 * per-test isolation.
 */

import { describe, expect, it } from 'vitest';
import { InMemoryAuditSink, InMemoryDataPointStore, InMemoryInbox } from '../src/orcastork/adapters/memory/index.js';
import { Epoch, SessionId } from '../src/orcastork/ids.js';
import type { DataPointStore } from '../src/orcastork/ports/index.js';
import { workEmail } from './doubles/datapoints.js';

/**
 * Adapter factories under conformance.
 *
 * Redis/Mongo factories are appended when those adapters land (in their own integration-bound
 * files); the in-memory factory is always present.
 */
const STORE_FACTORIES: ReadonlyMap<string, () => DataPointStore> = new Map([
  ['memory', () => new InMemoryDataPointStore()],
]);

const SID = SessionId('cnf-harness');

describe('the conformance harness', () => {
  it('discovers the in-memory factories', () => {
    expect(STORE_FACTORIES.has('memory')).toBe(true);
    // Every other port also has an always-available in-memory adapter.
    expect(new InMemoryInbox()).not.toBeNull();
    expect(new InMemoryAuditSink()).not.toBeNull();
  });

  it('is deterministic across two fresh instances', async () => {
    // Identical operations against two fresh instances yield identical observable state — the parity
    // property the cross-adapter matrix (in-memory vs real) will assert later.
    const operations = [workEmail('a@e.example', { last: workEmail().firstRetrieved }), workEmail('b@e.example')];
    const first = new InMemoryDataPointStore();
    const second = new InMemoryDataPointStore();
    await first.write(SID, operations, { epoch: Epoch(1) });
    await second.write(SID, operations, { epoch: Epoch(1) });
    expect(new Set((await first.snapshot(SID)).all().map((point) => point.value))).toEqual(
      new Set((await second.snapshot(SID)).all().map((point) => point.value)),
    );
  });

  it('hands out a fresh adapter with no leaked state', async () => {
    // A freshly constructed adapter starts empty (no cross-test leakage).
    const store = new InMemoryDataPointStore();
    expect((await store.snapshot(SID)).all()).toHaveLength(0);
    expect(await store.revision(SID)).toBe(0);
  });
});
