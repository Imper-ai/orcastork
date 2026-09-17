/**
 * CNF — the `Inbox` contract: ordered at-least-once delivery, poison tolerance, quarantine.
 *
 * Written entirely against the port; the only backend-specific seam is `appendRaw`, the write a
 * foreign producer performs (a stream `XADD` for Redis, the serialized seam for in-memory).
 *
 * @module
 */

import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { ZodError } from 'zod';
import { StaleEpochError, UnknownDataPointTypeError } from '../../../src/orcastork/exceptions.js';
import { Epoch } from '../../../src/orcastork/ids.js';
import type { DeliveredInboxEntry, Inbox } from '../../../src/orcastork/ports/index.js';
import { PoisonInboxEntry } from '../../../src/orcastork/ports/index.js';
import { personalEmail, workEmail } from '../datapoints.js';
import type { AppendRaw, ConformanceBinding, ConformanceHarness } from './shared.js';
import { OTHER_SID, SID } from './shared.js';

/**
 * A well-formed wire payload whose DataPoint type only a newer deploy knows.
 *
 * NOT poison: the bytes decode fine, so deserialization must fail fast rather than quarantine.
 */
const NEWER_DEPLOY_PAYLOAD = JSON.stringify({
  type: 'type_from_a_newer_deploy',
  value: 'x',
  retrieved_by: 'cnf-op',
  first_retrieved: '2026-01-01T00:00:00Z',
  last_retrieved: '2026-01-01T00:00:00Z',
});

/**
 * A known DataPoint type with a structurally broken body (missing required fields).
 *
 * Decodes as JSON, fails validation: a semantic failure that must propagate, never quarantine.
 */
const MALFORMED_KNOWN_TYPE_PAYLOAD = JSON.stringify({ type: 'work_email', value: 'a@work.example' });

/**
 * An entry id this inbox never issued, in the shape the default backend hands out.
 *
 * Overridden by a harness whose backend parses ids: a Redis stream id has a grammar, and `XACK`
 * rejects a string that is not one outright rather than reporting "nothing acked".
 */
const UNKNOWN_ENTRY_ID = 'no-such-entry';

/** The inbox under contract, plus the harness controls and the foreign-producer seam. */
export interface InboxHarness extends ConformanceHarness {
  readonly inbox: Inbox;

  /** Append a raw wire payload the way a foreign producer would, returning its entry id. */
  readonly appendRaw: AppendRaw;

  /**
   * An entry id the backend could have issued but did not; {@link UNKNOWN_ENTRY_ID} by default.
   *
   * The id format belongs to the backend, exactly as the raw append does, so the contract asks the
   * harness for one instead of fabricating a string. What is under contract is that acking an
   * entry the inbox does not hold changes nothing — not that any string is accepted as an id.
   */
  readonly unknownEntryId?: string;
}

/** One inbox adapter bound to the contract. */
export type InboxBinding = ConformanceBinding<InboxHarness>;

/** Narrow a delivered entry to the poison representation, failing the test when it is not one. */
const asPoison = (entry: DeliveredInboxEntry | undefined): PoisonInboxEntry => {
  expect(entry).toBeInstanceOf(PoisonInboxEntry);
  return entry as PoisonInboxEntry;
};

/** Run the whole `Inbox` contract against one adapter. */
export const describeInboxConformance = (binding: InboxBinding): void => {
  describe(binding.name, () => {
    let harness: InboxHarness;
    let inbox: Inbox;
    let appendRaw: AppendRaw;
    let unknownEntryId: string;

    beforeEach(async () => {
      harness = await binding.create();
      inbox = harness.inbox;
      appendRaw = harness.appendRaw;
      unknownEntryId = harness.unknownEntryId ?? UNKNOWN_ENTRY_ID;
    });

    afterEach(async () => {
      await harness.close?.();
    });

    it('appends, consumes and acks', async () => {
      const entryId = await inbox.append(SID, workEmail('a@e.example'));
      const delivered = await inbox.consume(SID);
      expect(delivered.map((entry) => entry.entryId)).toEqual([entryId]);
      await inbox.ack(SID, entryId, { epoch: Epoch(1) });
      expect(await inbox.pendingCount(SID)).toBe(0);
    });

    it('redelivers an unacked entry on reclaim', async () => {
      await inbox.append(SID, workEmail('a@e.example'));
      await inbox.consume(SID); // claim, do NOT ack
      const reclaimed = await inbox.reclaim(SID);
      expect(reclaimed).toHaveLength(1);
      expect(reclaimed[0]?.deliveryCount).toBe(2);
    });

    it('delivers in append order', async () => {
      const first = await inbox.append(SID, workEmail('a@e.example'));
      const second = await inbox.append(SID, personalEmail('p@e.example'));
      const delivered = await inbox.consume(SID);
      expect(delivered.map((entry) => entry.entryId)).toEqual([first, second]);
    });

    it('leaves a crash before ack reclaimable', async () => {
      await inbox.append(SID, workEmail('a@e.example'));
      await inbox.consume(SID); // consumer "crashes" before ack
      expect(await inbox.reclaim(SID)).toHaveLength(1);
    });

    it('keeps an entry appended with no consumer waiting', async () => {
      await inbox.append(SID, workEmail('a@e.example'));
      expect(await inbox.pendingCount(SID)).toBe(1);
      expect(await inbox.consume(SID)).toHaveLength(1);
    });

    it('treats an ack of an unknown or already-acked entry as a no-op', async () => {
      const entryId = await inbox.append(SID, workEmail('a@e.example'));
      await inbox.consume(SID);
      await inbox.ack(SID, unknownEntryId, { epoch: Epoch(1) }); // unknown → no-op
      await inbox.ack(SID, entryId, { epoch: Epoch(1) });
      await inbox.ack(SID, entryId, { epoch: Epoch(1) }); // already acked → no-op
      expect(await inbox.pendingCount(SID)).toBe(0);
    });

    it('isolates sessions', async () => {
      await inbox.append(SID, workEmail('a@e.example'));
      expect(await inbox.pendingCount(OTHER_SID)).toBe(0);
      expect(await inbox.consume(OTHER_SID)).toHaveLength(0);
    });

    it('wakes waitForEntry on an append without delivering anything', async () => {
      // The waiter must wake even if it is not yet subscribed when the append lands (see the port),
      // so it is started first and given a turn of the loop before the append.
      const waiter = inbox.waitForEntry(SID);
      await new Promise<void>((resolve) => {
        setImmediate(resolve);
      });
      await inbox.append(SID, workEmail('a@e.example'));
      await waiter; // resolves promptly on the push; the suite's own timeout is the safety net
      // The wakeup is a nudge, not delivery — consumption is still explicit.
      expect(await inbox.consume(SID)).toHaveLength(1);
    });

    it('returns from waitForEntry immediately when entries are already pending', async () => {
      await inbox.append(SID, workEmail('a@e.example'));
      await inbox.waitForEntry(SID); // already-pending entries never wait
    });

    it('ends a given-up waitForEntry so whatever it holds is released', async () => {
      // The port of Python's `finally: waiter.cancel()`. A wait the orchestrator walked away from
      // (a deadline, a park) must not sit there forever: an implementation releases what it holds —
      // a pub/sub connection above all — only when its own await ends, so a promise that never
      // settles is one leaked connection per parked session.
      const abandon = new AbortController();
      const waiter = inbox.waitForEntry(SID, abandon.signal);
      await new Promise<void>((resolve) => {
        setImmediate(resolve);
      });
      abandon.abort();
      await waiter; // returns rather than hanging; the suite's own timeout is the safety net
      // Giving the wait up claims nothing and disturbs nothing — a later append still delivers.
      await inbox.append(SID, workEmail('a@e.example'));
      expect(await inbox.consume(SID)).toHaveLength(1);
    });

    it('returns from a waitForEntry whose signal was already aborted', async () => {
      await inbox.waitForEntry(SID, AbortSignal.abort()); // nothing was opened, nothing waits
      expect(await inbox.pendingCount(SID)).toBe(0);
    });

    it('makes the redelivery count visible', async () => {
      await inbox.append(SID, workEmail('a@e.example'));
      await inbox.consume(SID);
      await inbox.reclaim(SID);
      const reclaimed = await inbox.reclaim(SID);
      expect(reclaimed[0]?.deliveryCount).toBe(3);
    });

    it('delivers a poison entry as poison while its peers still flow', async () => {
      const first = await inbox.append(SID, workEmail('a@e.example'));
      const poison = await appendRaw(SID, 'not-json{');
      const second = await inbox.append(SID, personalEmail('p@e.example'));
      const delivered = await inbox.consume(SID);
      expect(delivered.map((entry) => entry.entryId)).toEqual([first, poison, second]);
      expect(delivered.map((entry) => entry instanceof PoisonInboxEntry)).toEqual([false, true, false]);
    });

    it('re-presents a poison entry on reclaim', async () => {
      const poison = await appendRaw(SID, 'not-json{');
      const [delivered] = await inbox.consume(SID);
      asPoison(delivered);
      const [reclaimed] = await inbox.reclaim(SID); // a crashed consumer's resume sees the same poison
      const poisonAgain = asPoison(reclaimed);
      expect(poisonAgain.entryId).toBe(poison);
      expect(poisonAgain.deliveryCount).toBe(2);
      expect(poisonAgain.rawPayload).toBe('not-json{'); // kept for inspection
    });

    it('propagates an unknown DataPoint type from consume and reclaim', async () => {
      // Poison is reserved for bad wire bytes. A well-formed payload whose DataPoint type this
      // deployment does not know must fail fast — a quiet quarantine would hide a parser/registry
      // regression — and stay deliverable, so redelivery on resume lets a newer deployment parse it.
      await appendRaw(SID, NEWER_DEPLOY_PAYLOAD);
      await expect(inbox.consume(SID)).rejects.toThrow(UnknownDataPointTypeError);
      // The claimed entry is re-presented, never quarantined.
      await expect(inbox.reclaim(SID)).rejects.toThrow(UnknownDataPointTypeError);
      expect(await inbox.quarantined(SID)).toHaveLength(0);
      expect(await inbox.pendingCount(SID)).toBe(1); // still there for a newer deployment to apply
    });

    it('propagates a validation failure of a known type', async () => {
      await appendRaw(SID, MALFORMED_KNOWN_TYPE_PAYLOAD);
      await expect(inbox.consume(SID)).rejects.toThrow(ZodError);
      expect(await inbox.quarantined(SID)).toHaveLength(0); // semantic failures are never quarantined
    });

    it('removes a quarantined entry from delivery and records it', async () => {
      const entryId = await inbox.append(SID, workEmail('a@e.example'));
      await inbox.consume(SID);
      await inbox.quarantine(SID, entryId, { reason: 'apply kept failing', epoch: Epoch(1) });
      expect(await inbox.pendingCount(SID)).toBe(0);
      expect(await inbox.reclaim(SID)).toHaveLength(0); // quarantined entries are never re-presented
      const records = await inbox.quarantined(SID);
      expect(records).toHaveLength(1);
      expect(records[0]?.entryId).toBe(entryId);
      expect(records[0]?.reason).toBe('apply kept failing');
      expect(records[0]?.deliveryCount).toBe(1);
      expect(records[0]?.rawPayload).not.toBeNull(); // the original wire payload stays inspectable
    });

    it('guards quarantine by epoch', async () => {
      const entryId = await inbox.append(SID, workEmail('a@e.example'));
      await inbox.consume(SID);
      await inbox.ack(SID, unknownEntryId, { epoch: Epoch(2) }); // a successor has bumped the inbox epoch
      await expect(
        inbox.quarantine(SID, entryId, { reason: 'from a fenced predecessor', epoch: Epoch(1) }),
      ).rejects.toThrow(StaleEpochError);
      expect(await inbox.pendingCount(SID)).toBe(1); // the rejected quarantine removed nothing
      expect(await inbox.quarantined(SID)).toHaveLength(0);
    });

    it('keeps quarantine records per session', async () => {
      const entryId = await inbox.append(SID, workEmail('a@e.example'));
      await inbox.consume(SID);
      await inbox.quarantine(SID, entryId, { reason: 'poison', epoch: Epoch(1) });
      expect(await inbox.quarantined(OTHER_SID)).toHaveLength(0);
    });

    it('leaves an entry deliverable when it is acked before it was ever consumed', async () => {
      // Redis XACK only removes claimed (pending) entries; an ack racing ahead of delivery must be a
      // no-op on every adapter, never destroy the entry.
      const entryId = await inbox.append(SID, workEmail('a@e.example'));
      await inbox.ack(SID, entryId, { epoch: Epoch(1) });
      const delivered = await inbox.consume(SID);
      expect(delivered.map((entry) => entry.entryId)).toEqual([entryId]);
    });

    it('treats the quarantine of a never-consumed entry as a no-op', async () => {
      // Removal-plus-record is gated on the entry actually being claimed (the Redis script only
      // records when the XACK removed a pending entry) — so nothing is recorded here either.
      const entryId = await inbox.append(SID, workEmail('a@e.example'));
      await inbox.quarantine(SID, entryId, { reason: 'premature', epoch: Epoch(1) });
      expect(await inbox.quarantined(SID)).toHaveLength(0);
      const delivered = await inbox.consume(SID);
      expect(delivered.map((entry) => entry.entryId)).toEqual([entryId]); // still deliverable
    });

    it('claims at most maxEntries, in append order', async () => {
      // Bounded draining is the inbox's backpressure: a consumer pulls a capped batch so one apply
      // can never swallow an arbitrarily large backlog. maxEntries=0 (Redis maps COUNT 0 to
      // "unbounded", so this is a genuine adapter-parity hazard) must claim NOTHING and leave every
      // entry redeliverable; maxEntries=1 claims exactly the first in append order, leaving the rest
      // for the next call; a cap >= the backlog drains the remainder.
      const first = await inbox.append(SID, workEmail('a@e.example'));
      const second = await inbox.append(SID, personalEmail('p@e.example'));
      const third = await inbox.append(SID, workEmail('c@e.example'));

      expect(await inbox.consume(SID, { maxEntries: 0 })).toHaveLength(0); // COUNT 0 ≠ unbounded
      expect(await inbox.pendingCount(SID)).toBe(3); // every entry still deliverable

      const claimedFirst = await inbox.consume(SID, { maxEntries: 1 });
      expect(claimedFirst.map((entry) => entry.entryId)).toEqual([first]); // exactly the first, in order
      const claimedSecond = await inbox.consume(SID, { maxEntries: 1 });
      expect(claimedSecond.map((entry) => entry.entryId)).toEqual([second]); // resumes after it

      const remaining = await inbox.consume(SID, { maxEntries: 10 }); // a cap >= the backlog drains the rest
      expect(remaining.map((entry) => entry.entryId)).toEqual([third]);
    });

    it('claims nothing for a non-positive cap', async () => {
      // The whole non-positive range is the empty-claim case (the in-memory cap check and the Redis
      // COUNT<=0 special-case must agree), and a rejected claim leaves the backlog intact.
      await inbox.append(SID, workEmail('a@e.example'));
      expect(await inbox.consume(SID, { maxEntries: -1 })).toHaveLength(0);
      expect(await inbox.pendingCount(SID)).toBe(1); // nothing was claimed by the negative cap
    });

    it('preserves quarantined records in quarantine order', async () => {
      // Operators re-drive by reading the quarantine list, so its order and per-record fields must be
      // faithful: a reversed list, an off-by-one range read, or a shared deliveryCount/payload would
      // corrupt the inspection record. Quarantine poison FIRST then a valid entry, and assert both
      // come back in that order, each carrying its own reason and raw payload (the poison's
      // malformed bytes vs the valid one's JSON).
      const poison = await appendRaw(SID, 'not-json{');
      const valid = await inbox.append(SID, workEmail('a@e.example'));
      const delivered = await inbox.consume(SID); // claim both so quarantine can dispose them
      expect(new Set(delivered.map((entry) => entry.entryId))).toEqual(new Set([poison, valid]));

      await inbox.quarantine(SID, poison, { reason: 'poison bytes', epoch: Epoch(1) });
      await inbox.quarantine(SID, valid, { reason: 'apply kept failing', epoch: Epoch(1) });

      const records = await inbox.quarantined(SID);
      expect(records.map((record) => record.entryId)).toEqual([poison, valid]); // quarantine order
      expect(records.map((record) => record.reason)).toEqual(['poison bytes', 'apply kept failing']);
      expect(records[0]?.rawPayload).toBe('not-json{'); // the poison's own malformed bytes
      expect(records[1]?.rawPayload).toContain('a@e.example');
    });
  });
};
