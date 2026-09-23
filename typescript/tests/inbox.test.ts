/**
 * INBOX — the `Inbox` contract, run against the in-memory adapter (via the CNF suite).
 */

import { InMemoryInbox } from '../src/orcastork/adapters/memory/index.js';
import { describeInboxConformance } from './doubles/conformance/inbox.js';

describeInboxConformance({
  name: 'InMemoryInbox',
  create: () => {
    const inbox = new InMemoryInbox();
    return Promise.resolve({
      inbox,
      // The serialized seam is this adapter's stand-in for a foreign producer's raw stream write.
      appendRaw: inbox.appendSerialized.bind(inbox),
      // Delivery is not timed; nothing in the inbox contract waits on a clock.
      advanceTime: () => Promise.resolve(),
    });
  },
});
