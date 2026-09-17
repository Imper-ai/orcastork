/**
 * Redis `DataPointStore` — keyed-merge over a hash, revision + epoch via a Lua CAS.
 *
 * Each identity `(type, value)` is a field in `dp:{session}` holding the DataPoint JSON; its
 * first-added and last-freshened revisions live in parallel hashes (`added:{session}` /
 * `upd:{session}`) keyed by the same identity. The session revision (`rev:{session}`) and fencing
 * epoch (`epoch:{session}`) are integer keys. The keyed-merge is computed here (the orchestrator is
 * the sole per-session mutator), then applied **atomically and epoch-guarded** by a single Lua
 * script: a stale epoch is rejected with no partial write, and the revision is allocated by `INCR`
 * **inside the script** so it stays monotonic regardless of interleaving (the script treats the
 * DataPoint JSON as an opaque blob, so it never has to decode it). `applyResolved` runs the same
 * script without the `write` path's prior HGETALL: a sole-mutator caller that already resolved the
 * merge supplies the 'a'/'u' mode per identity itself, so the hot write path costs one round-trip
 * instead of a full re-read.
 *
 * @module
 */

import type { AnyDataPoint } from '../../datapoints/index.js';
import { DataPointView, identityKey, parseDataPoint } from '../../datapoints/index.js';
import { OrchestrationError, StaleEpochError } from '../../exceptions.js';
import type { Epoch, OperatorId, Revision, SessionId } from '../../ids.js';
import { Revision as toRevision } from '../../ids.js';
import { ChangeSet } from '../../ports/change_set.js';
import type { ApplyResolvedOptions, ClaimEffectOptions, DataPointStore } from '../../ports/datapoint_store.js';
import { EffectClaim } from '../../ports/datapoint_store.js';
import type { RedisEvalCommand, RedisExpireCommand } from './ttl.js';
import { DEFAULT_STATE_TTL_MS, slideTtl } from './ttl.js';

/** The commands this store needs. */
export interface RedisStoreClient extends RedisEvalCommand, RedisExpireCommand {
  get(key: string): Promise<string | null>;
  hGet(key: string, field: string): Promise<string | null>;
  hGetAll(key: string): Promise<Record<string, string>>;
}

// Atomically: reject a stale epoch (no writes); else bump epoch, and if there are changes
// allocate the next revision with INCR and apply each field. KEYS = epoch, rev, dp, added,
// upd; ARGV = epoch, then triples (identity, mode, dp_json) where mode 'a' = new identity
// (stamp added + updated) and 'u' = re-observation (stamp updated only).
const WRITE_SCRIPT = `
local epoch = tonumber(ARGV[1])
local stored = tonumber(redis.call('GET', KEYS[1]) or '0')
if epoch < stored then return -1 end
if epoch > stored then redis.call('SET', KEYS[1], epoch) end
if #ARGV < 4 then return tonumber(redis.call('GET', KEYS[2]) or '0') end
local rev = redis.call('INCR', KEYS[2])
for i = 2, #ARGV, 3 do
  local identity = ARGV[i]
  redis.call('HSET', KEYS[3], identity, ARGV[i + 2])
  redis.call('HSET', KEYS[5], identity, rev)
  if ARGV[i + 1] == 'a' then redis.call('HSET', KEYS[4], identity, rev) end
end
return rev
`;

// Atomically epoch-guard a session-meta hash-field write (a watermark, the wall-clock deadline,
// the flow fingerprint): a stale epoch is rejected, and the highest ACCEPTED epoch is recorded.
// Ownership is the lock's job; recording the epoch on every guarded write is what completes
// stale-writer rejection — a session whose first store mutation is one of these fields must
// still fence a later lower-epoch writer.
const GUARDED_FIELD_SCRIPT = `
local epoch = tonumber(ARGV[1])
local stored = tonumber(redis.call('GET', KEYS[1]) or '0')
if epoch < stored then return -1 end
if epoch > stored then redis.call('SET', KEYS[1], epoch) end
redis.call('HSET', KEYS[2], ARGV[2], ARGV[3])
return 0
`;

// Atomically epoch-guard a side-effect claim, recording the highest accepted epoch exactly like
// every other guarded write (a session whose first mutation is an effect claim must still fence
// a later lower-epoch writer). The stored states are 'pending:<epoch>' (in-flight, naming its
// owner) and 'committed' (the effect ran). KEYS = epoch, fx hash; ARGV = epoch, effect key,
// reclaim_stale flag ('1'/'0'). Returns -1 on a stale epoch, else the EffectClaim code:
// 1 ACQUIRED, 2 ALREADY_COMMITTED, 3 PENDING_SAME_EPOCH, 4 PENDING_STALE_EPOCH.
const CLAIM_EFFECT_SCRIPT = `
local epoch = tonumber(ARGV[1])
local stored = tonumber(redis.call('GET', KEYS[1]) or '0')
if epoch < stored then return -1 end
if epoch > stored then redis.call('SET', KEYS[1], epoch) end
local state = redis.call('HGET', KEYS[2], ARGV[2])
local pending = 'pending:' .. ARGV[1]
if state == false then
  redis.call('HSET', KEYS[2], ARGV[2], pending)
  return 1
end
if state == 'committed' then return 2 end
if state == pending then return 3 end
if ARGV[3] == '1' then
  redis.call('HSET', KEYS[2], ARGV[2], pending)
  return 1
end
return 4
`;

// Atomically epoch-guard the pending → committed transition (recording the accepted epoch like
// every guarded write). Only this epoch's own pending mark transitions; 'committed' stays as-is
// (idempotent) and any other state is left untouched — the script never fabricates 'committed'
// for a claim this epoch does not own. -1 on a stale epoch.
const COMMIT_EFFECT_SCRIPT = `
local epoch = tonumber(ARGV[1])
local stored = tonumber(redis.call('GET', KEYS[1]) or '0')
if epoch < stored then return -1 end
if epoch > stored then redis.call('SET', KEYS[1], epoch) end
if redis.call('HGET', KEYS[2], ARGV[2]) == ('pending:' .. ARGV[1]) then
  redis.call('HSET', KEYS[2], ARGV[2], 'committed')
end
return 0
`;

// Atomically epoch-guard the revert (recording the accepted epoch like every guarded write):
// deletes ONLY this epoch's own pending mark — never 'committed' (the effect DID run) and never
// another epoch's pending. -1 on a stale epoch.
const REVERT_EFFECT_SCRIPT = `
local epoch = tonumber(ARGV[1])
local stored = tonumber(redis.call('GET', KEYS[1]) or '0')
if epoch < stored then return -1 end
if epoch > stored then redis.call('SET', KEYS[1], epoch) end
if redis.call('HGET', KEYS[2], ARGV[2]) == ('pending:' .. ARGV[1]) then
  redis.call('HDEL', KEYS[2], ARGV[2])
end
return 0
`;

/** The claim script's integer code, as the port's outcome. */
const claimResult = (code: number): EffectClaim => {
  switch (code) {
    case 1:
      return EffectClaim.ACQUIRED;
    case 2:
      return EffectClaim.ALREADY_COMMITTED;
    case 3:
      return EffectClaim.PENDING_SAME_EPOCH;
    case 4:
      return EffectClaim.PENDING_STALE_EPOCH;
    default:
      // Unreachable while the script and this table agree; a mismatch is a deployment running two
      // versions of the adapter against one keyspace, which must fail loudly rather than guess.
      throw new OrchestrationError(`the effect claim script returned an unknown code ${code}`);
  }
};

/** The five per-session keys every write path touches, in the order the scripts expect them. */
interface SessionKeys {
  readonly epoch: string;
  readonly revision: string;
  readonly dataPoints: string;
  readonly added: string;
  readonly updated: string;
}

/** One stored identity, as `_entries` reads it back. */
interface StoredEntry {
  readonly dataPoint: AnyDataPoint;
  readonly added: number;
  readonly updated: number;
}

/** How a {@link RedisDataPointStore} is configured. */
export interface RedisDataPointStoreOptions {
  /** The sliding lifetime of the session's Redis state. */
  readonly stateTtlMs?: number;
}

/** The session blackboard, held in Redis and fenced by a Lua CAS. */
export class RedisDataPointStore implements DataPointStore {
  private readonly redis: RedisStoreClient;
  private readonly stateTtlMs: number;

  public constructor(redis: RedisStoreClient, options: RedisDataPointStoreOptions = {}) {
    this.redis = redis;
    this.stateTtlMs = options.stateTtlMs ?? DEFAULT_STATE_TTL_MS;
  }

  /**
   * The keyed-merge identity of a DataPoint, as a Redis hash field.
   *
   * `identityKey` is the shared string form (the same normalization as the in-memory store and the
   * archive), so dedup is identical across adapters — and across runtimes.
   */
  private static identity(dataPoint: AnyDataPoint): string {
    return identityKey(dataPoint);
  }

  private static keys(sessionId: SessionId): SessionKeys {
    return {
      epoch: `epoch:${sessionId}`,
      revision: `rev:${sessionId}`,
      dataPoints: `dp:${sessionId}`,
      added: `added:${sessionId}`,
      updated: `upd:${sessionId}`,
    };
  }

  public async write(
    sessionId: SessionId,
    dataPoints: Iterable<AnyDataPoint>,
    options: { readonly epoch: Epoch },
  ): Promise<Revision> {
    const current = await this.redis.hGetAll(RedisDataPointStore.keys(sessionId).dataPoints);
    const triples: string[] = [];
    for (const dataPoint of dataPoints) {
      const identity = RedisDataPointStore.identity(dataPoint);
      const existingRaw = current[identity];
      if (existingRaw === undefined) {
        triples.push(identity, 'a', JSON.stringify(dataPoint.toWire()));
        continue;
      }
      const existing = parseDataPoint(JSON.parse(existingRaw));
      if (dataPoint.lastRetrieved.getTime() > existing.lastRetrieved.getTime()) {
        const merged = existing.reobserved(dataPoint.lastRetrieved);
        triples.push(identity, 'u', JSON.stringify(merged.toWire()));
      }
    }
    return this.apply(sessionId, triples, options.epoch);
  }

  public async applyResolved(sessionId: SessionId, options: ApplyResolvedOptions): Promise<Revision> {
    // The sole-mutator fast path: the caller already keyed-merged, so the prior HGETALL that
    // `write` needs to decide 'a' vs 'u' is skipped entirely — the supplied split IS the mode.
    const triples: string[] = [];
    for (const dataPoint of options.added) {
      triples.push(RedisDataPointStore.identity(dataPoint), 'a', JSON.stringify(dataPoint.toWire()));
    }
    for (const dataPoint of options.updated) {
      triples.push(RedisDataPointStore.identity(dataPoint), 'u', JSON.stringify(dataPoint.toWire()));
    }
    return this.apply(sessionId, triples, options.epoch);
  }

  private async apply(sessionId: SessionId, triples: readonly string[], epoch: Epoch): Promise<Revision> {
    const keys = RedisDataPointStore.keys(sessionId);
    const result = Number(
      await this.redis.eval(WRITE_SCRIPT, {
        keys: [keys.epoch, keys.revision, keys.dataPoints, keys.added, keys.updated],
        arguments: [String(epoch), ...triples],
      }),
    );
    if (result === -1) {
      throw new StaleEpochError(`epoch ${epoch} is stale for session ${sessionId}`);
    }
    // Effect marks (`fx:`) and session meta (`meta:` — the wall-clock deadline and the flow
    // fingerprint) are slid here too, so they share the session's sliding lifetime: an active
    // session must never lose its at-most-once guarantees or its remaining deadline budget
    // mid-flight.
    await slideTtl(
      this.redis,
      this.stateTtlMs,
      keys.epoch,
      keys.revision,
      keys.dataPoints,
      keys.added,
      keys.updated,
      `wm:${sessionId}`,
      `fx:${sessionId}`,
      `meta:${sessionId}`,
    );
    return toRevision(result);
  }

  private async entries(sessionId: SessionId): Promise<readonly StoredEntry[]> {
    const keys = RedisDataPointStore.keys(sessionId);
    const dataPoints = await this.redis.hGetAll(keys.dataPoints);
    const added = await this.redis.hGetAll(keys.added);
    const updated = await this.redis.hGetAll(keys.updated);
    return Object.entries(dataPoints).map(([identity, raw]) => ({
      dataPoint: parseDataPoint(JSON.parse(raw)),
      added: Number(added[identity] ?? 0),
      updated: Number(updated[identity] ?? 0),
    }));
  }

  public async snapshot(sessionId: SessionId): Promise<DataPointView> {
    return new DataPointView((await this.entries(sessionId)).map((entry) => entry.dataPoint));
  }

  public async revision(sessionId: SessionId): Promise<Revision> {
    const stored = await this.redis.get(RedisDataPointStore.keys(sessionId).revision);
    return toRevision(stored === null ? 0 : Number(stored));
  }

  public async changeSetSince(sessionId: SessionId, since: Revision): Promise<ChangeSet> {
    const added: AnyDataPoint[] = [];
    const updated: AnyDataPoint[] = [];
    for (const entry of await this.entries(sessionId)) {
      if (entry.added > since) {
        added.push(entry.dataPoint);
      } else if (entry.updated > since) {
        updated.push(entry.dataPoint);
      }
    }
    return new ChangeSet({ added, updated });
  }

  public async getWatermark(sessionId: SessionId, operatorId: OperatorId): Promise<Revision | null> {
    const raw = await this.redis.hGet(`wm:${sessionId}`, operatorId);
    return raw === null ? null : toRevision(Number(raw));
  }

  public async setWatermark(
    sessionId: SessionId,
    operatorId: OperatorId,
    revision: Revision,
    options: { readonly epoch: Epoch },
  ): Promise<void> {
    await this.guardedField(sessionId, `wm:${sessionId}`, operatorId, String(revision), options.epoch);
  }

  public async claimEffect(
    sessionId: SessionId,
    effectKey: string,
    options: ClaimEffectOptions,
  ): Promise<EffectClaim> {
    return claimResult(
      await this.runEffectScript(CLAIM_EFFECT_SCRIPT, sessionId, effectKey, options.epoch, [
        options.reclaimStale ? '1' : '0',
      ]),
    );
  }

  public async commitEffect(
    sessionId: SessionId,
    effectKey: string,
    options: { readonly epoch: Epoch },
  ): Promise<void> {
    await this.runEffectScript(COMMIT_EFFECT_SCRIPT, sessionId, effectKey, options.epoch, []);
  }

  public async revertEffect(
    sessionId: SessionId,
    effectKey: string,
    options: { readonly epoch: Epoch },
  ): Promise<void> {
    await this.runEffectScript(REVERT_EFFECT_SCRIPT, sessionId, effectKey, options.epoch, []);
  }

  public async getEffectState(sessionId: SessionId, effectKey: string): Promise<string | null> {
    return this.redis.hGet(`fx:${sessionId}`, effectKey);
  }

  private async runEffectScript(
    script: string,
    sessionId: SessionId,
    effectKey: string,
    epoch: Epoch,
    extraArguments: readonly string[],
  ): Promise<number> {
    const epochKey = RedisDataPointStore.keys(sessionId).epoch;
    const result = Number(
      await this.redis.eval(script, {
        keys: [epochKey, `fx:${sessionId}`],
        arguments: [String(epoch), effectKey, ...extraArguments],
      }),
    );
    if (result === -1) {
      throw new StaleEpochError(`epoch ${epoch} is stale for session ${sessionId}`);
    }
    await slideTtl(this.redis, this.stateTtlMs, epochKey, `fx:${sessionId}`);
    return result;
  }

  public async getSessionDeadline(sessionId: SessionId): Promise<Date | null> {
    const raw = await this.redis.hGet(`meta:${sessionId}`, 'deadline');
    return raw === null ? null : new Date(raw);
  }

  public async setSessionDeadline(
    sessionId: SessionId,
    deadline: Date,
    options: { readonly epoch: Epoch },
  ): Promise<void> {
    await this.guardedField(sessionId, `meta:${sessionId}`, 'deadline', deadline.toISOString(), options.epoch);
  }

  public async getFlowFingerprint(sessionId: SessionId): Promise<string | null> {
    return this.redis.hGet(`meta:${sessionId}`, 'flow_fingerprint');
  }

  public async setFlowFingerprint(
    sessionId: SessionId,
    fingerprint: string,
    options: { readonly epoch: Epoch },
  ): Promise<void> {
    await this.guardedField(sessionId, `meta:${sessionId}`, 'flow_fingerprint', fingerprint, options.epoch);
  }

  /** One epoch-guarded hash-field write, plus the TTL slide that keeps the field alive with it. */
  private async guardedField(
    sessionId: SessionId,
    hashKey: string,
    field: string,
    value: string,
    epoch: Epoch,
  ): Promise<void> {
    const epochKey = RedisDataPointStore.keys(sessionId).epoch;
    const result = Number(
      await this.redis.eval(GUARDED_FIELD_SCRIPT, {
        keys: [epochKey, hashKey],
        arguments: [String(epoch), field, value],
      }),
    );
    if (result === -1) {
      throw new StaleEpochError(`epoch ${epoch} is stale for session ${sessionId}`);
    }
    await slideTtl(this.redis, this.stateTtlMs, epochKey, hashKey);
  }
}
