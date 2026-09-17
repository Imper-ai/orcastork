/**
 * STORE — the `DataPointStore` contract, run against the in-memory adapter (via the CNF suite).
 */

import { InMemoryDataPointStore } from '../src/orcastork/adapters/memory/index.js';
import { describeStoreConformance } from './doubles/conformance/store.js';

describeStoreConformance({
  name: 'InMemoryDataPointStore',
  create: () =>
    Promise.resolve({
      store: new InMemoryDataPointStore(),
      // The store keeps no expiring state, so nothing in its contract waits on a clock.
      advanceTime: () => Promise.resolve(),
    }),
});
