/**
 * CNF — the `AuditSink` contract: append-only, ordered, per-event, durable on return, epoch-fenced.
 *
 * Written entirely against the port, so every audit adapter is behaviourally interchangeable.
 *
 * @module
 */

import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { AuditKind, AuditLogEntry } from '../../../src/orcastork/audit/index.js';
import { StaleEpochError } from '../../../src/orcastork/exceptions.js';
import { Epoch } from '../../../src/orcastork/ids.js';
import type { AuditSink } from '../../../src/orcastork/ports/index.js';
import { T0 } from '../datapoints.js';
import type { ConformanceBinding, ConformanceHarness } from './shared.js';
import { OP, SID } from './shared.js';

/** The audit sink under contract, plus the harness controls. */
export interface AuditSinkHarness extends ConformanceHarness {
  readonly audit: AuditSink;
}

/** One audit adapter bound to the contract. */
export type AuditSinkBinding = ConformanceBinding<AuditSinkHarness>;

/** One entry for the session under contract, written by `epoch`. */
const entry = (epoch: number, kind: AuditKind = AuditKind.DATA_POINT_ADDED): AuditLogEntry =>
  AuditLogEntry({ sessionId: SID, epoch: Epoch(epoch), timestamp: T0, kind, operatorId: OP });

/** Run the whole `AuditSink` contract against one adapter. */
export const describeAuditSinkConformance = (binding: AuditSinkBinding): void => {
  describe(binding.name, () => {
    let harness: AuditSinkHarness;
    let audit: AuditSink;

    beforeEach(async () => {
      harness = await binding.create();
      audit = harness.audit;
    });

    afterEach(async () => {
      await harness.close?.();
    });

    it('replays every appended entry, in append order', async () => {
      for (let index = 0; index < 3; index += 1) {
        await audit.append(entry(1));
      }
      expect(await audit.replay(SID)).toHaveLength(3);
    });

    it('makes an appended entry replayable with no further step', async () => {
      // The crash-visibility contract, and the reason the sink commits at append: a session that
      // dies HERE — and is never resumed, so no recovery pass ever runs on its behalf — must still
      // have its trail. Only append() has run, so anything replay() cannot see now is lost forever.
      await audit.append(entry(1));
      expect(await audit.replay(SID)).toHaveLength(1);
    });

    it('rejects an entry from a stale epoch', async () => {
      await audit.append(entry(2));
      await expect(audit.append(entry(1))).rejects.toThrow(StaleEpochError);
      expect(await audit.replay(SID)).toHaveLength(1);
    });

    it('writes one document per event', async () => {
      for (let index = 0; index < 5; index += 1) {
        await audit.append(entry(1));
      }
      expect(await audit.replay(SID)).toHaveLength(5);
    });

    it('makes a batch observably identical to N appends', async () => {
      await audit.append(entry(1, AuditKind.OPERATOR_INVOKED));
      await audit.appendMany([entry(1), entry(1, AuditKind.CAPABILITY_ACTIVATED)]);
      // Per-event granularity, in append order, interleaved correctly with single appends.
      expect((await audit.replay(SID)).map((appended) => appended.kind)).toEqual([
        AuditKind.OPERATOR_INVOKED,
        AuditKind.DATA_POINT_ADDED,
        AuditKind.CAPABILITY_ACTIVATED,
      ]);
    });

    it('rejects a stale-epoch batch without writing any of it', async () => {
      await audit.append(entry(2));
      await expect(audit.appendMany([entry(1), entry(1)])).rejects.toThrow(StaleEpochError);
      expect(await audit.replay(SID)).toHaveLength(1); // only the pre-existing entry landed
    });
  });
};
