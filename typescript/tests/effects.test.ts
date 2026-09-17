/**
 * FX — the `ctx.once` effect guard: claim/commit/revert side effects across reruns, retries, resumes.
 *
 * Emissions, archive writes, and aggregator outputs are all safe to repeat; an external side effect
 * (an OTP, an ITSM ticket) is not — these tests pin that `ctx.once(key, body)` fires such an effect
 * exactly once per (operator, key) per session when attempts succeed, re-runs it when the attempt
 * that claimed it failed (the claim is reverted, so a retry is not silently skipped), and applies an
 * explicit recovery policy when a predecessor died mid-effect.
 *
 * The session-level cases (an effect surviving data-driven reruns, a crash-resume, an aggregator's
 * context) need the orchestrator and are ported with it; the Python originals are
 * `test_fx_01` … `test_fx_06`. `test_fx_15` (a malformed pending mark reads as an unknown owner) is
 * a `ports/datapoint_store` unit and already lives in `tests/ports.test.ts`.
 *
 * The store is the in-memory `DataPointStore` adapter, as in Python: the guard's contract is
 * written against the effect keyspace, and the adapter is the reference implementation of it.
 */

import { describe, expect, it } from 'vitest';
import { InMemoryDataPointStore } from '../src/orcastork/adapters/memory/index.js';
import { StaleEpochError } from '../src/orcastork/exceptions.js';
import type { OperatorId, SessionId } from '../src/orcastork/ids.js';
import { Epoch as toEpoch, OperatorId as toOperatorId, SessionId as toSessionId } from '../src/orcastork/ids.js';
import { EffectGuard, EffectRecovery } from '../src/orcastork/operators/index.js';
import type {
  ClaimEffectOptions,
  DataPointStore,
  EffectClaim as EffectClaimValue,
} from '../src/orcastork/ports/index.js';
import { EffectClaim } from '../src/orcastork/ports/index.js';
import { captureLogs } from './doubles/logs.js';

const SID: SessionId = toSessionId('fx-session');
const OP: OperatorId = toOperatorId('fx-op');
const KEY = `${OP}:send-otp`;

const guardOver = (store: DataPointStore, epoch = 1): EffectGuard =>
  new EffectGuard(store, { sessionId: SID, operatorId: OP, epoch: toEpoch(epoch) });

/** The one warning a case expects, found by the effect key every guard log carries. */
const warningFor = (records: readonly { level: string; message: string; fields: Record<string, unknown> }[]) => {
  const warning = records.find((record) => record.fields.effect_key === KEY);
  expect(warning).toBeDefined();
  return warning as { level: string; message: string; fields: Record<string, unknown> };
};

describe('the ctx.once effect guard', () => {
  it('commits on the happy path, and a second claim dedupes', async () => {
    const store = new InMemoryDataPointStore();
    const guard = guardOver(store);
    const fired: number[] = [];

    expect(
      await guard.once('send-otp', (acquired) => {
        expect(acquired).toBe(true);
        fired.push(1);
      }),
    ).toBe(true);
    expect(await store.getEffectState(SID, KEY)).toBe('committed');

    expect(
      await guard.once('send-otp', (acquired) => {
        expect(acquired).toBe(false);
      }),
    ).toBe(false);
    expect(fired).toEqual([1]);
  });

  it('refuses a same-epoch nested claim without resolving the outer one', async () => {
    const store = new InMemoryDataPointStore();
    const guard = guardOver(store);

    await guard.once('send-otp', async (outer) => {
      expect(outer).toBe(true);
      await guard.once('send-otp', (inner) => {
        expect(inner).toBe(false); // a duplicate claim within this run must not re-run the effect
      });
      // The non-owning inner exit neither committed nor reverted the outer claim.
      expect(await store.getEffectState(SID, KEY)).toBe('pending:1');
    });
    expect(await store.getEffectState(SID, KEY)).toBe('committed');
  });

  it('reclaims a stale pending mark with a warning under the RERUN policy', async () => {
    const store = new InMemoryDataPointStore();
    // The predecessor died mid-effect.
    await store.claimEffect(SID, KEY, { epoch: toEpoch(1), reclaimStale: false });

    const { records } = await captureLogs(async () => {
      await guardOver(store, 2).once('send-otp', (acquired) => {
        expect(acquired).toBe(true);
      });
    });

    expect(await store.getEffectState(SID, KEY)).toBe('committed');
    const warning = warningFor(records);
    expect(warning.level).toBe('WARNING');
    expect(warning.fields.stale_epoch).toBe(1);
  });

  it('leaves the unknown state in place under the SKIP policy', async () => {
    const store = new InMemoryDataPointStore();
    await store.claimEffect(SID, KEY, { epoch: toEpoch(1), reclaimStale: false });

    await guardOver(store, 2).once(
      'send-otp',
      (acquired) => {
        expect(acquired).toBe(false);
      },
      { onUnknown: EffectRecovery.SKIP },
    );

    // The stale mark survives, so a later resume sees the same unknown state — not a fabricated
    // outcome.
    expect(await store.getEffectState(SID, KEY)).toBe('pending:1');
  });

  it('dedupes under the next epoch what was committed under this one', async () => {
    const store = new InMemoryDataPointStore();
    await guardOver(store, 1).once('send-otp', (acquired) => {
      expect(acquired).toBe(true);
    });
    await guardOver(store, 2).once('send-otp', (acquired) => {
      // The commit is durable across epochs — a resume never re-fires it.
      expect(acquired).toBe(false);
    });
  });

  it('reverts the claim for a same-epoch retry when the body throws', async () => {
    const store = new InMemoryDataPointStore();
    const guard = guardOver(store);

    await expect(
      guard.once('send-otp', (acquired) => {
        expect(acquired).toBe(true);
        throw new Error('otp provider returned 503');
      }),
    ).rejects.toThrow('otp provider');
    expect(await store.getEffectState(SID, KEY)).toBeNull(); // reverted, not stuck pending

    await guard.once('send-otp', (acquired) => {
      expect(acquired).toBe(true); // the same-epoch retry owns the effect again
    });
  });

  it.skip('test_fx_13_task_cancellation_inside_the_body_reverts_the_claim — JavaScript has no task cancellation', () => {
    // Python cancels the task awaiting inside the block and the guard reverts during the unwind. A
    // promise cannot be cancelled, so an abandoned run surfaces to the guard as a thrown error,
    // which the body-throws case above already covers.
  });

  it.skip('test_fx_14_cancellation_at_the_commit_boundary_still_commits_the_claim — asyncio.shield has no port', () => {
    // Python shields the post-body commit so a cancellation landing on it cannot strand the claim.
    // A plain await always runs to completion here, so there is no boundary to test.
  });

  it('propagates the body error, not the revert error, when the revert itself fails', async () => {
    // The rare double failure: the effect body threw AND the revert ITSELF throws (a higher epoch
    // superseded this run between claim and revert, so revert's fence raises StaleEpochError). The
    // guard must swallow the revert error, log the documented WARNING, and re-raise the ORIGINAL
    // body exception — not the revert error, which would mask the real failure from the retry
    // machinery.
    class RevertRaisesStore extends InMemoryDataPointStore {
      public override async revertEffect(): Promise<void> {
        throw new StaleEpochError('a higher epoch superseded this run');
      }
    }

    const store = new RevertRaisesStore();
    const guard = guardOver(store);

    const { records } = await captureLogs(async () => {
      await expect(
        guard.once('send-otp', (acquired) => {
          expect(acquired).toBe(true);
          throw new Error('boom');
        }),
      ).rejects.toThrow('boom');
    });

    const warning = warningFor(records);
    expect(warning.level).toBe('WARNING');
    expect(warning.message).toContain('revert failed');
    // The mark stays pending under this epoch, so a same-epoch retry sees PENDING_SAME_EPOCH → skips.
    expect(await store.getEffectState(SID, KEY)).toBe('pending:1');
    await guard.once('send-otp', (acquired) => {
      // Prefer a possibly-skipped effect over a possibly-double-fired one.
      expect(acquired).toBe(false);
    });
  });

  it('does not turn a success into a failure when the commit itself fails', async () => {
    // The effect DID run (clean body), but the commit ITSELF throws (a higher epoch superseded this
    // run). The guard must swallow it, log the WARNING, and return normally — a commit failure must
    // not surface as an operator failure that would retry and re-fire the effect.
    class CommitRaisesStore extends InMemoryDataPointStore {
      public override async commitEffect(): Promise<void> {
        throw new StaleEpochError('a higher epoch superseded this run');
      }
    }

    const store = new CommitRaisesStore();
    const guard = guardOver(store);
    const fired: number[] = [];

    const { records } = await captureLogs(async () => {
      await guard.once('send-otp', (acquired) => {
        expect(acquired).toBe(true);
        fired.push(1); // the effect ran to completion; the body did not throw
      });
    });

    expect(fired).toEqual([1]); // the block returned without throwing despite the commit failure
    const warning = warningFor(records);
    expect(warning.level).toBe('WARNING');
    expect(warning.message).toContain('commit failed');
    // The claim is stranded as pending by design — not committed, not deleted.
    expect(await store.getEffectState(SID, KEY)).toBe('pending:1');
  });

  it('leaves the owner claim untouched when a non-owning body throws', async () => {
    // A non-owning attempt runs its block with `acquired === false` and never enters the
    // commit/revert path. If its body throws, the exception must propagate WITHOUT the guard
    // touching the owner's mark — only the attempt that ACQUIRED may resolve the claim.
    const store = new InMemoryDataPointStore();
    const guard = guardOver(store);

    await guard.once('send-otp', async (owner) => {
      expect(owner).toBe(true);
      await expect(
        guard.once('send-otp', (duplicate) => {
          expect(duplicate).toBe(false); // a same-epoch duplicate within this run
          throw new Error('unrelated work in the duplicate block failed');
        }),
      ).rejects.toThrow('unrelated');
      // The non-owner's failure neither committed nor reverted the still-open owner claim.
      expect(await store.getEffectState(SID, KEY)).toBe('pending:1');
    });
    // The owner exited cleanly afterward and committed normally.
    expect(await store.getEffectState(SID, KEY)).toBe('committed');
  });

  it('does not acquire when a RERUN reclaim loses the race to a commit', async () => {
    // Under RERUN the guard sees PENDING_STALE_EPOCH and reclaims with reclaimStale=true. The
    // documented race: a concurrent commit landed between the stale observation and the reclaim, so
    // the reclaim returns ALREADY_COMMITTED — anything but ACQUIRED means the effect must NOT run.
    class CommitDuringReclaimStore extends InMemoryDataPointStore {
      public override async claimEffect(
        sessionId: SessionId,
        effectKey: string,
        options: ClaimEffectOptions,
      ): Promise<EffectClaimValue> {
        if (options.reclaimStale) {
          // The predecessor (or another writer) committed the effect just before the reclaim. Driven
          // through the store's own API rather than its internals: take the stale mark, commit it,
          // and answer what the racing writer's commit would have left behind.
          await super.claimEffect(sessionId, effectKey, options);
          await super.commitEffect(sessionId, effectKey, { epoch: options.epoch });
          return EffectClaim.ALREADY_COMMITTED;
        }
        return await super.claimEffect(sessionId, effectKey, options);
      }
    }

    const store = new CommitDuringReclaimStore();
    // The predecessor died mid-effect.
    await store.claimEffect(SID, KEY, { epoch: toEpoch(1), reclaimStale: false });
    const fired: number[] = [];

    await guardOver(store, 2).once('send-otp', (acquired) => {
      expect(acquired).toBe(false); // the reclaim lost the race to a commit — this attempt must not fire
      if (acquired) {
        fired.push(1);
      }
    });

    expect(fired).toEqual([]); // the effect body did not run
    expect(await store.getEffectState(SID, KEY)).toBe('committed'); // the committed state stands
  });
});
