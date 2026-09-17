/**
 * CNF — the `DataPointArchive` contract: buffered writes, keyed-upsert, buffer-transparent reads.
 *
 * Written entirely against the port, so every archive adapter is behaviourally interchangeable.
 *
 * @module
 */

import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { ArchivedDataPoint } from '../../../src/orcastork/archive/index.js';
import type { AnyDataPoint } from '../../../src/orcastork/datapoints/index.js';
import { StaleEpochError } from '../../../src/orcastork/exceptions.js';
import { Epoch } from '../../../src/orcastork/ids.js';
import type { DataPointArchive } from '../../../src/orcastork/ports/index.js';
import { personalEmail, risk, T0, workEmail } from '../datapoints.js';
import type { ConformanceBinding, ConformanceHarness } from './shared.js';
import { NAMESPACE, SID, T2 } from './shared.js';

/** The archive under contract, plus the harness controls. */
export interface DataPointArchiveHarness extends ConformanceHarness {
  readonly archive: DataPointArchive;
}

/** One archive adapter bound to the contract. */
export type DataPointArchiveBinding = ConformanceBinding<DataPointArchiveHarness>;

/** One archive document for the session under contract, observed by `epoch`. */
const entry = (dataPoint?: AnyDataPoint, options: { readonly epoch?: number } = {}): ArchivedDataPoint =>
  ArchivedDataPoint.fromDataPoint(dataPoint ?? risk(0.5), {
    sessionId: SID,
    namespaceId: NAMESPACE,
    epoch: Epoch(options.epoch ?? 1),
  });

/** Run the whole `DataPointArchive` contract against one adapter. */
export const describeDataPointArchiveConformance = (binding: DataPointArchiveBinding): void => {
  describe(binding.name, () => {
    let harness: DataPointArchiveHarness;
    let archive: DataPointArchive;

    beforeEach(async () => {
      harness = await binding.create();
      archive = harness.archive;
    });

    afterEach(async () => {
      await harness.close?.();
    });

    it('archives a first observation as one document', async () => {
      await archive.archive(entry(workEmail('a@e.example')));
      await archive.flush(SID);
      const committed = await archive.read(SID);
      expect(committed).toHaveLength(1);
      expect(committed[0]?.value).toBe('a@e.example');
      expect(committed[0]?.firstRetrieved).toEqual(committed[0]?.lastRetrieved); // first sighting
    });

    it('upserts a re-observation, bumping last and keeping first', async () => {
      await archive.archive(entry(workEmail('a@e.example', { first: T0, last: T0 })));
      await archive.archive(entry(workEmail('a@e.example', { first: T0, last: T2 })));
      await archive.flush(SID);
      const committed = await archive.read(SID);
      expect(committed).toHaveLength(1); // keyed-upsert — no duplicate
      expect(committed[0]?.firstRetrieved).toEqual(T0); // immutable
      expect(committed[0]?.lastRetrieved).toEqual(T2); // bumped
    });

    it('advances the stored epoch on a re-observation', async () => {
      // The stored epoch says which epoch last SAW this datapoint, not which one first recorded it.
      // A row frozen at its first sighting reads as older than the session that actually produced
      // it, which matters wherever the archive is compared against a session's own epoch. Both
      // adapters have to agree: the read-equals-post-flush property cannot catch a difference here,
      // because each one's fold mirrors its own flush, so a divergence stays green until something
      // reads the epoch and gets a different answer per backend.
      await archive.archive(entry(workEmail('a@e.example', { first: T0, last: T0 }), { epoch: 1 }));
      await archive.archive(entry(workEmail('a@e.example', { first: T0, last: T2 }), { epoch: 2 }));

      // Asserted on BOTH sides of the flush on purpose. A read before it folds the buffer in code,
      // a read after it reflects the merge the datastore performed, and the two are separate
      // implementations — checking only the post-flush read leaves whichever one the buffer uses
      // free to disagree.
      const buffered = await archive.read(SID);
      expect(buffered).toHaveLength(1);
      expect(buffered[0]?.epoch).toBe(2);

      await archive.flush(SID);

      const committed = await archive.read(SID);
      expect(committed).toHaveLength(1); // same identity, so still one row
      expect(committed[0]?.epoch).toBe(2);
      expect(committed[0]?.firstRetrieved).toEqual(T0); // unchanged by the epoch advancing
    });

    // `$max` refusing to lower a stored epoch has no test here on purpose: the meta CAS rejects an
    // epoch below the session's high water mark, so an older-epoch re-observation can never be
    // buffered and no sequence of port calls can tell `max(existing, sealed)` apart from
    // last-writer-wins. Each adapter pins it on its own fold instead, which is where an
    // out-of-order pair can actually be constructed.

    it('writes through the buffer rather than inline', async () => {
      await archive.archive(entry());
      // bufferedCount is what proves the write stayed off the hot path — read() cannot, because it
      // is buffer-transparent by contract and would report the entry either way.
      expect(await archive.bufferedCount(SID)).toBe(1);
      expect(await archive.flush(SID)).toBe(1);
      expect(await archive.bufferedCount(SID)).toBe(0); // committed, so no longer buffered
      expect(await archive.read(SID)).toHaveLength(1);
    });

    it('reads a buffered entry back before any flush', async () => {
      // A session that dies before its flush must not go invisible: read folds the buffer over the
      // committed rows, so the entry is readable on either side of the flush rather than only after.
      await archive.archive(entry());
      const before = await archive.read(SID);
      expect(before).toHaveLength(1);
      await archive.flush(SID); // resume → drain
      expect(await archive.read(SID)).toEqual(before);
    });

    it('rejects an archive write from a stale epoch', async () => {
      await archive.archive(entry(undefined, { epoch: 2 }));
      await expect(archive.archive(entry(undefined, { epoch: 1 }))).rejects.toThrow(StaleEpochError);
      await archive.flush(SID);
      expect(await archive.read(SID)).toHaveLength(1);
    });

    it('keeps distinct identities as distinct documents', async () => {
      await archive.archive(entry(workEmail('a@e.example')));
      await archive.archive(entry(workEmail('b@e.example')));
      await archive.archive(entry(personalEmail('p@e.example')));
      await archive.flush(SID);
      expect(await archive.read(SID)).toHaveLength(3); // batched flush keeps per-identity granularity
    });

    it('is idempotent under redelivery of the same identity', async () => {
      for (let index = 0; index < 3; index += 1) {
        // at-least-once redelivery / operator reruns of the same value
        await archive.archive(entry(workEmail('a@e.example')));
      }
      await archive.flush(SID);
      expect(await archive.read(SID)).toHaveLength(1);
    });

    it('makes a batch observably identical to N archives', async () => {
      await archive.archiveMany([
        entry(workEmail('a@e.example', { first: T0, last: T0 })),
        entry(workEmail('b@e.example')),
        entry(workEmail('a@e.example', { first: T0, last: T2 })), // re-observation in the same batch
      ]);
      expect(await archive.bufferedCount(SID)).toBe(3); // per-observation granularity in the buffer
      expect(await archive.flush(SID)).toBe(3);
      const committed = new Map((await archive.read(SID)).map((stored) => [stored.value, stored]));
      expect(new Set(committed.keys())).toEqual(new Set(['a@e.example', 'b@e.example'])); // fold dedups
      expect(committed.get('a@e.example')?.lastRetrieved).toEqual(T2); // in-batch re-observation bumped last
    });

    it('reads identically either side of a flush', async () => {
      // The fold a read applies must agree with the keyed-upsert a flush performs.
      //
      // read() folds buffered rows over committed ones itself, which is a second expression of the
      // merge flush carries out in the datastore. This pins the two together: a field the fold
      // forgets to advance, or an ordering difference, shows up as these two reads disagreeing.
      //
      // Deliberately spans the boundary: one identity is already committed AND re-observed in the
      // buffer, which is the case where the two merges could diverge.
      await archive.archive(entry(workEmail('a@e.example', { first: T0, last: T0 })));
      await archive.archive(entry(workEmail('b@e.example')));
      await archive.flush(SID);
      await archive.archive(entry(workEmail('a@e.example', { first: T0, last: T2 })));
      await archive.archive(entry(personalEmail('p@e.example')));

      const before = await archive.read(SID);
      expect(new Set(before.map((stored) => stored.value))).toEqual(
        new Set(['a@e.example', 'b@e.example', 'p@e.example']),
      );
      expect(before.find((stored) => stored.value === 'a@e.example')?.lastRetrieved).toEqual(T2);

      expect(await archive.flush(SID)).toBe(2);
      expect(await archive.read(SID)).toEqual(before);
    });

    it('rejects a stale-epoch batch with nothing buffered', async () => {
      await archive.archive(entry(undefined, { epoch: 2 }));
      await expect(archive.archiveMany([entry(workEmail('x@e.example'), { epoch: 1 })])).rejects.toThrow(
        StaleEpochError,
      );
      expect(await archive.bufferedCount(SID)).toBe(1); // only the pre-existing entry survived
      await archive.flush(SID);
      expect(await archive.read(SID)).toHaveLength(1);
    });
  });
};
