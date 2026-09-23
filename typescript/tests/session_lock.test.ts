/**
 * LOCK — the `SessionLock` contract, run against the in-memory adapter (via the CNF suite).
 *
 * Time-dependent contracts advance the injected `FakeClock` through the harness.
 */

import { InMemorySessionLock } from '../src/orcastork/adapters/memory/index.js';
import { FakeClock } from './doubles/clock.js';
import { describeLockConformance } from './doubles/conformance/lock.js';

describeLockConformance({
  name: 'InMemorySessionLock',
  create: () => {
    const clock = new FakeClock();
    return Promise.resolve({
      lock: new InMemorySessionLock(clock),
      advanceTime: (ms: number) => {
        clock.advance(ms);
        return Promise.resolve();
      },
    });
  },
});
