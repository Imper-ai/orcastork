/**
 * PORT — the value types and the two concrete pieces the ports themselves carry.
 *
 * The ports are interfaces, so there is nothing to execute in most of this directory: the
 * behavioural contract is asserted by the per-adapter conformance suite. What *is* executable
 * lives here — the stored effect-state encoding every store adapter shares, the no-op rate limiter
 * a deployment gets when it wires nothing, and the shared value types' defaults (an absent raw
 * payload is `null`, never missing) that the inbox and durable-store adapters hand back.
 */

import { describe, expect, it } from 'vitest';
import type { AnyDataPoint } from '../src/orcastork/datapoints/index.js';
import { Epoch } from '../src/orcastork/ids.js';
import {
  ChangeSet,
  EFFECT_COMMITTED,
  EFFECT_PENDING_PREFIX,
  EffectClaim,
  effectPendingEpoch,
  effectPendingState,
  InboxEntry,
  NullRateLimiter,
  PoisonInboxEntry,
  QuarantinedEntry,
  VersionedDocument,
} from '../src/orcastork/ports/index.js';
import { risk, workEmail } from './doubles/datapoints.js';

describe('the stored effect state', () => {
  it('names the owning epoch of an in-flight claim', () => {
    expect(effectPendingState(Epoch(7))).toBe('pending:7');
    expect(effectPendingState(Epoch(7)).startsWith(EFFECT_PENDING_PREFIX)).toBe(true);
  });

  it('marks a finished effect with a state that carries no owner', () => {
    // A committed effect belongs to no epoch: it ran, and every later claim must see that.
    expect(EFFECT_COMMITTED).toBe('committed');
    expect(effectPendingEpoch(EFFECT_COMMITTED)).toBeNull();
  });

  it('reads the owning epoch back out of a pending mark', () => {
    expect(effectPendingEpoch(effectPendingState(Epoch(7)))).toBe(Epoch(7));
  });

  it('treats a malformed mark as an unknown owner rather than failing', () => {
    // Corrupt persisted state must not take down claim/recovery: an owner that cannot be parsed is
    // reported exactly like a non-pending state.
    expect(effectPendingEpoch('pending:not-an-int')).toBeNull();
    expect(effectPendingEpoch('pending:')).toBeNull();
    expect(effectPendingEpoch('committed')).toBeNull();
  });

  it('keeps the claim outcomes on their Python wire values', () => {
    expect([
      EffectClaim.ACQUIRED,
      EffectClaim.ALREADY_COMMITTED,
      EffectClaim.PENDING_SAME_EPOCH,
      EffectClaim.PENDING_STALE_EPOCH,
    ]).toEqual(['acquired', 'already_committed', 'pending_same_epoch', 'pending_stale_epoch']);
  });
});

describe('the default rate limiter', () => {
  it('lets every action through, so a deployment that wires nothing loses nothing', async () => {
    const limiter = new NullRateLimiter();

    await expect(limiter.acquire('capability:search')).resolves.toBeUndefined();
    await expect(limiter.acquire('capability:search')).resolves.toBeUndefined();
  });
});

describe('the shared port value types', () => {
  it('copies a change set, so a store cannot mutate an answer it already handed out', () => {
    const added: AnyDataPoint[] = [workEmail()];
    const updated: AnyDataPoint[] = [risk(0.5)];

    const changes = new ChangeSet({ added, updated });
    added.push(risk(0.9));

    expect(changes.added).toHaveLength(1);
    expect(changes.updated).toEqual(updated);
    expect(Object.isFrozen(changes)).toBe(true);
  });

  it('carries a claimed entry with its delivery bookkeeping', () => {
    const dataPoint = workEmail();

    const entry = new InboxEntry({ entryId: '1-0', dataPoint, deliveryCount: 2 });

    expect({ entryId: entry.entryId, deliveryCount: entry.deliveryCount }).toEqual({
      entryId: '1-0',
      deliveryCount: 2,
    });
    expect(entry.dataPoint).toBe(dataPoint);
  });

  it('defaults a poison entry to no raw payload rather than leaving the field missing', () => {
    const poison = new PoisonInboxEntry({ entryId: '1-0', error: 'JSONDecodeError: bad', deliveryCount: 1 });

    expect(poison.rawPayload).toBeNull();
    expect(new PoisonInboxEntry({ ...poison, rawPayload: 'not json' }).rawPayload).toBe('not json');
  });

  it('distinguishes a poison entry from a parsed one, which is how a consumer disposes of it', () => {
    // The port of Python's `isinstance` check: a delivery is either a DataPoint or the poison
    // record of one, and the orchestrator quarantines the latter on sight.
    const delivered = [
      new InboxEntry({ entryId: '1-0', dataPoint: workEmail(), deliveryCount: 1 }),
      new PoisonInboxEntry({ entryId: '2-0', error: 'JSONDecodeError: bad', deliveryCount: 1 }),
    ];

    expect(delivered.filter((entry) => entry instanceof PoisonInboxEntry).map((entry) => entry.entryId)).toEqual([
      '2-0',
    ]);
  });

  it('records a quarantined entry with its reason and, when cheap, the original payload', () => {
    const recorded = new QuarantinedEntry({
      entryId: '1-0',
      reason: 'poison',
      deliveryCount: 3,
      rawPayload: 'not json',
    });

    expect({ ...recorded }).toEqual({ entryId: '1-0', reason: 'poison', deliveryCount: 3, rawPayload: 'not json' });
    expect(new QuarantinedEntry({ entryId: '2-0', reason: 'poison', deliveryCount: 1 }).rawPayload).toBeNull();
  });

  it('defaults a durable document to no status and no stamp until the framework writes one', () => {
    const stored = new VersionedDocument({ document: { total: 1 }, version: 3 });

    expect({ document: stored.document, version: stored.version }).toEqual({ document: { total: 1 }, version: 3 });
    expect(stored.status).toBeNull();
    expect(stored.updatedAt).toBeNull();
  });

  it('carries the live-status metadata an interim aggregation writes', () => {
    const stamped = new Date('2026-01-01T00:00:00.000Z');

    const stored = new VersionedDocument({ document: {}, version: 1, status: 'in_progress', updatedAt: stamped });

    expect({ status: stored.status, updatedAt: stored.updatedAt }).toEqual({
      status: 'in_progress',
      updatedAt: stamped,
    });
  });
});
