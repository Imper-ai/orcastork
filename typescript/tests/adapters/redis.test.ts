/**
 * RZ — Redis adapters: the CNF contracts bound to Redis + backend-specific mechanics.
 *
 * These run against a real `redis-server` the test setup spawns (Lua, Streams + consumer groups,
 * `XAUTOCLAIM` and `INCR` are all the server's own), so they execute in the default test run and
 * give CNF parity between the in-memory and Redis adapters. Python binds them to in-process
 * `fakeredis` instead; the port takes a real server, so an adapter test is evidence about the thing
 * that will run in production. RZ-07 (transient-connection retry) is a thin production wrapper and
 * is deferred.
 *
 * **Why this file waits on the wall clock, and nowhere else does.** Python expires a lease by
 * advancing fakeredis's clock; a real server's clock cannot be advanced from a client. So the
 * lease- and cooldown-dependent contracts are bound with a window of a couple of hundred
 * milliseconds and `advanceTime` is a real wait — the contracts themselves express every wait as a
 * multiple of the harness's window (see `conformance/shared.ts`), so the in-memory bindings keep
 * their `FakeClock` and their half-minute lease unchanged. Nothing else here touches real time: the
 * rate-limiter contracts still run on a `FakeClock`, because the limiter takes its now-ms from the
 * injected clock by design.
 *
 * RZ-09 (the fleet-wide token bucket, `@pytest.mark.integration` in Python) lives in
 * `redis.integration.test.ts`. RZ-24 and RZ-25 (the limiter's constructor guards) need no server at
 * all and live next to their in-memory twin in `memory.test.ts`, where they read as the
 * parity contract they are.
 */

import { createClient } from 'redis';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import type { RedisInboxClient } from '../../src/orcastork/adapters/redis/inbox.js';
import {
  RedisCooldownGate,
  RedisDataPointStore,
  RedisRateLimiter,
  RedisSessionLock,
  RedisStreamsInbox,
} from '../../src/orcastork/adapters/redis/index.js';
import { StaleEpochError } from '../../src/orcastork/exceptions.js';
import { Epoch, OperatorId, Revision, SessionId } from '../../src/orcastork/ids.js';
import { InboxEntry } from '../../src/orcastork/ports/index.js';
import { FakeClock } from '../doubles/clock.js';
import { describeCooldownGateConformance } from '../doubles/conformance/cooldown_gate.js';
import { describeInboxConformance } from '../doubles/conformance/inbox.js';
import { describeLockConformance } from '../doubles/conformance/lock.js';
import { describeStoreConformance } from '../doubles/conformance/store.js';
import { personalEmail, T0, workEmail } from '../doubles/datapoints.js';
import type { RunningRedis } from '../doubles/servers.js';
import { isRedisAvailable, startRedisServer, warnSkipped } from '../doubles/servers.js';

/** Open a client on `url`; the adapters are typed against the commands they use, not this class. */
const connectedClient = (url: string): ReturnType<typeof createClient> => createClient({ url });

/** Whatever `createClient` hands back for the options this suite passes. */
type RedisClient = ReturnType<typeof connectedClient>;

const RZ = SessionId('rz-session');
const RZ_OTHER = SessionId('rz-other-session');
const RZ_OP = OperatorId('rz-op');
const T2 = new Date(T0.getTime() + 2 * 60 * 60 * 1000);

/** One second of token accrual, in the milliseconds the port speaks. */
const ONE_SECOND_MS = 1000;

/**
 * A lease short enough to wait out for real, long enough to survive scheduling jitter.
 *
 * Confined to this file: a real server decides expiry on its own clock, so the only way to observe
 * a lapsed lease is to let the time pass.
 */
const REAL_LEASE_MS = 200;

/** The cooldown width the gate contract arms here, for the same reason as {@link REAL_LEASE_MS}. */
const REAL_COOLDOWN_MS = 200;

/** Long enough for a handful of `SUBSCRIBE`s (or their closes) to reach the server and be counted. */
const SUBSCRIBE_SETTLE_MS = 250;

/**
 * A well-formed stream id no `XADD` here ever mints — `1-1` is a millisecond in 1970.
 *
 * `XACK` parses its argument, so an id that is not a stream id at all is rejected outright rather
 * than reported as "nothing acked". What the contract is about is acking an entry the inbox does
 * not hold, and this is that entry.
 */
const UNKNOWN_STREAM_ENTRY_ID = '1-1';

/** Let real time pass — the Redis-only counterpart of advancing a `FakeClock`. */
const realWait = (ms: number): Promise<void> =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

const redisAvailable = isRedisAvailable();
if (!redisAvailable) {
  warnSkipped('the Redis adapter suite', 'redis-server is not on PATH and ORCASTORK_TEST_REDIS_URL is unset');
}

describe.skipIf(!redisAvailable)('the Redis adapter family', () => {
  let server: RunningRedis;

  /** Every client this test opened; the one `afterEach` below owns closing all of them. */
  let openClients: RedisClient[] = [];

  /**
   * A connected client on an empty keyspace.
   *
   * One server for the file, a fresh connection and a `FLUSHALL` per test: a provided server may be
   * shared, and nothing in these contracts may see another test's keys.
   */
  const freshClient = async (): Promise<RedisClient> => {
    const client = connectedClient(server.url);
    await client.connect();
    await client.flushAll();
    openClients.push(client);
    return client;
  };

  beforeAll(async () => {
    server = await startRedisServer();
  }, 60_000);

  afterEach(async () => {
    const clients = openClients;
    openClients = [];
    await Promise.all(clients.map((client) => client.close()));
  });

  afterAll(async () => {
    await server.stop();
  });

  describeStoreConformance({
    name: 'RedisDataPointStore',
    create: async () => ({
      store: new RedisDataPointStore(await freshClient()),
      // The store keeps no expiring state, so nothing in its contract waits on a clock.
      advanceTime: () => Promise.resolve(),
    }),
  });

  describeInboxConformance({
    name: 'RedisStreamsInbox',
    create: async () => {
      const client = await freshClient();
      return {
        inbox: new RedisStreamsInbox(client),
        // A foreign producer writes straight to the stream — exactly the front door's XADD seam.
        appendRaw: (sessionId, payload) => client.xAdd(`inbox:${sessionId}`, '*', { data: payload }),
        unknownEntryId: UNKNOWN_STREAM_ENTRY_ID,
        // Delivery is not timed; nothing in the inbox contract waits on a clock.
        advanceTime: () => Promise.resolve(),
      };
    },
  });

  describeLockConformance({
    name: 'RedisSessionLock',
    create: async () => ({
      lock: new RedisSessionLock(await freshClient(), { ttlMs: REAL_LEASE_MS }),
      ttlMs: REAL_LEASE_MS,
      advanceTime: realWait,
    }),
  });

  describeCooldownGateConformance({
    name: 'RedisCooldownGate',
    create: async () => ({
      gate: new RedisCooldownGate(await freshClient()),
      cooldownMs: REAL_COOLDOWN_MS,
      advanceTime: realWait,
    }),
  });

  it('rejects a stale epoch in the Lua CAS without any partial write', async () => {
    const store = new RedisDataPointStore(await freshClient());
    await store.write(RZ, [workEmail('a@e.example')], { epoch: Epoch(2) });
    await expect(store.write(RZ, [personalEmail('b@e.example')], { epoch: Epoch(1) })).rejects.toThrow(
      StaleEpochError,
    );
    const values = new Set((await store.snapshot(RZ)).all().map((dataPoint) => dataPoint.value));
    expect(values.has('b@e.example')).toBe(false); // the rejected write left no partial state
  });

  it('redelivers a stream entry whose consumer crashed before the ack', async () => {
    const inbox = new RedisStreamsInbox(await freshClient());
    await inbox.append(RZ, workEmail('a@e.example'));
    expect(await inbox.consume(RZ)).toHaveLength(1); // claimed (XREADGROUP), but not acked
    const reclaimed = await inbox.reclaim(RZ); // XAUTOCLAIM re-presents it
    expect(reclaimed).toHaveLength(1);
    expect(reclaimed[0]?.deliveryCount).toBe(2);
  });

  it('mints the epoch atomically and monotonically across a lease takeover', async () => {
    const lock = new RedisSessionLock(await freshClient(), { ttlMs: REAL_LEASE_MS });
    const first = await lock.acquire(RZ);
    await realWait(REAL_LEASE_MS * 1.5); // the lease expires on the server's own clock
    const second = await lock.acquire(RZ);
    expect(first).toBe(1);
    expect(second).toBe(2);
  });

  it('delivers each entry to the consumer group exactly once', async () => {
    const inbox = new RedisStreamsInbox(await freshClient());
    await inbox.append(RZ, workEmail('a@e.example'));
    const first = await inbox.consume(RZ);
    const second = await inbox.consume(RZ); // already claimed → not re-delivered by XREADGROUP '>'
    expect(first).toHaveLength(1);
    expect(second).toHaveLength(0);
  });

  it('bounds session state by putting a TTL on every per-session key', async () => {
    // Session state must not accumulate in Redis forever: a write/append sets (and slides) a TTL on
    // the per-session keys. PTTL returns ms-remaining (-1 = no expiry, -2 = missing), so > 0 means set.
    const client = await freshClient();
    const store = new RedisDataPointStore(client, { stateTtlMs: 60_000 });
    const inbox = new RedisStreamsInbox(client, { stateTtlMs: 60_000 });
    await store.write(RZ, [workEmail('a@e.example')], { epoch: Epoch(1) });
    await inbox.append(RZ, workEmail('a@e.example'));

    expect(await client.pTTL(`dp:${RZ}`)).toBeGreaterThan(0); // the store hash is bounded by a sliding TTL
    expect(await client.pTTL(`rev:${RZ}`)).toBeGreaterThan(0);
    expect(await client.pTTL(`inbox:${RZ}`)).toBeGreaterThan(0); // the inbox stream is bounded too
  });

  // === reclaim re-presents in-flight entries only (edge case 1) =========================

  it('re-presents only the claimed entries on reclaim, never a never-claimed one', async () => {
    // AT-LEAST-ONCE: reclaim re-presents in-flight (claimed-but-unacked) entries only. A bounded
    // consume claims just the first of a backlog; the never-claimed remainder must stay a fresh
    // consume candidate, neither reclaimed nor dropped.
    const inbox = new RedisStreamsInbox(await freshClient());
    const first = await inbox.append(RZ, workEmail('e1@e.example'));
    await inbox.append(RZ, personalEmail('e2@e.example')); // e2 stays unclaimed

    const claimed = await inbox.consume(RZ, { maxEntries: 1 }); // XREADGROUP COUNT 1 claims only e1
    expect(claimed.map((entry) => entry.entryId)).toEqual([first]);

    const reclaimed = await inbox.reclaim(RZ); // XAUTOCLAIM re-presents only the pending (claimed) e1
    expect(reclaimed.map((entry) => entry.entryId)).toEqual([first]);
    expect(reclaimed[0]?.deliveryCount).toBe(2); // only the reclaimed entry's count is bumped

    const remaining = await inbox.consume(RZ); // e2 was never claimed → still a fresh consume candidate
    expect(remaining).toHaveLength(1);
    const entry = remaining[0];
    expect(entry).toBeInstanceOf(InboxEntry);
    expect((entry as InboxEntry).dataPoint.value).toBe('e2@e.example'); // surfaced intact
    expect(entry?.deliveryCount).toBe(1); // first delivery, not bumped by the reclaim of its peer
  });

  // === inbox ack stale-epoch rejection (edge cases 2 + 6) ===============================

  it('guards the ack by epoch and leaves the fenced entry pending', async () => {
    // FENCING EPOCH: ack is a mutating write path. A fenced predecessor (lower epoch) must be
    // rejected with StaleEpochError after a successor bumped the inbox epoch — and must NOT remove
    // the entry it tried to ack, so no split-brain ack-away of an entry the successor now owns.
    const inbox = new RedisStreamsInbox(await freshClient());
    const entryId = await inbox.append(RZ, workEmail('a@e.example'));
    await inbox.consume(RZ);
    // A successor acks an unrelated id at epoch 2, bumping inbox_epoch to 2 (the ack script's
    // `epoch > stored → SET` branch); the real entry is untouched, still pending.
    await inbox.ack(RZ, UNKNOWN_STREAM_ENTRY_ID, { epoch: Epoch(2) });

    // The ack script returns -1 for epoch < stored.
    await expect(inbox.ack(RZ, entryId, { epoch: Epoch(1) })).rejects.toThrow(StaleEpochError);

    expect(await inbox.pendingCount(RZ)).toBe(1); // the fenced ack removed nothing
  });

  // === a given-up waiter releases its dedicated pub/sub connection ======================

  it('closes the pub/sub connection of a wait that was given up', async () => {
    // The Redis half of the conformance contract, which can only assert that the wait ends: the
    // waiter runs on a connection of its own, released in the `finally` of its own await. Python
    // cancels the task, which runs that `finally`; a port that merely walked away from the promise
    // would hold one socket and one keepalive open per parked session, forever.
    const client = await freshClient();
    const inbox = new RedisStreamsInbox(client);
    const connected = async (): Promise<number> =>
      Number(/connected_clients:(\d+)/.exec(await client.info('clients'))?.[1] ?? '0');

    const before = await connected();
    const abandon = new AbortController();
    const waiters = [0, 1, 2].map((index) => inbox.waitForEntry(SessionId(`${RZ}-wait-${index}`), abandon.signal));
    await realWait(SUBSCRIBE_SETTLE_MS); // each waiter opens and subscribes its own connection
    expect(await connected()).toBe(before + waiters.length);

    abandon.abort();
    await Promise.all(waiters);
    await realWait(SUBSCRIBE_SETTLE_MS);

    expect(await connected()).toBe(before); // every one of them was closed again
  });

  // === ensureGroup: swallow BUSYGROUP, re-raise everything else (edge cases 3 + 7) ======

  it('swallows BUSYGROUP when the group-create runs again', async () => {
    // The group-create runs on every consume/reclaim/quarantine; the first creates the group and
    // subsequent calls hit BUSYGROUP, which must be swallowed silently (idempotent ensure) so a
    // second consume after the group exists succeeds and simply delivers nothing new.
    const inbox = new RedisStreamsInbox(await freshClient());
    await inbox.append(RZ, workEmail('a@e.example'));
    const first = await inbox.consume(RZ); // creates the group
    const second = await inbox.consume(RZ); // re-entry: xGroupCreate fails BUSYGROUP, swallowed
    expect(first).toHaveLength(1);
    expect(second).toHaveLength(0);
  });

  it('re-raises a group-creation failure that is not BUSYGROUP', async () => {
    // Fail-fast: a genuine group-creation failure (anything but BUSYGROUP) must surface, not be
    // masked as a benign 'group already exists'. Masking it would let consume run against a missing
    // group and silently deliver nothing.
    const failure = new Error('ERR something else entirely');
    const inbox = new RedisStreamsInbox(withFailingGroupCreate(await freshClient(), failure));
    await expect(inbox.consume(RZ)).rejects.toThrow('something else entirely');
  });

  // === quarantine of an already-acked entry is a safe no-op (edge case 4) ===============

  it('neither resurrects nor records a quarantine of an already-acked entry', async () => {
    // Quarantine idempotency: an acked entry is durably applied and removed; quarantining it
    // afterward must not create a spurious record or resurrect it (the quarantine script's XACK
    // returns 0, so no RPUSH).
    const inbox = new RedisStreamsInbox(await freshClient());
    const entryId = await inbox.append(RZ, workEmail('a@e.example'));
    await inbox.consume(RZ);
    await inbox.ack(RZ, entryId, { epoch: Epoch(1) }); // entry XACK'd out of pending

    await inbox.quarantine(RZ, entryId, { reason: 'late', epoch: Epoch(1) }); // XACK returns 0 → no record

    expect(await inbox.quarantined(RZ)).toEqual([]);
    expect(await inbox.pendingCount(RZ)).toBe(0); // not resurrected, not double-counted
  });

  // === write() re-observing an identity with an equal/older timestamp is a no-op (edge case 5) ===

  it('burns no revision on a write whose timestamp is equal or older', async () => {
    // KEYED-MERGE DEDUP: an at-least-once redelivery of an OLDER (or equal) snapshot of the same
    // identity must leave lastRetrieved untouched, emit no 'u' triple, and not advance the
    // revision — the `lastRetrieved > existing` guard is strict, so equality is also a no-op.
    const store = new RedisDataPointStore(await freshClient());
    const base = await store.write(RZ, [workEmail('a@e.example', { last: T2 })], { epoch: Epoch(1) });

    const older = await store.write(RZ, [workEmail('a@e.example', { last: T0 })], { epoch: Epoch(1) });
    expect(older).toBe(base); // the no-op merge burned no revision
    const equal = await store.write(RZ, [workEmail('a@e.example', { last: T2 })], { epoch: Epoch(1) });
    expect(equal).toBe(base); // strict `>` means an equal re-observation is also a no-op

    const snapshot = await store.snapshot(RZ);
    expect(snapshot.size).toBe(1);
    expect(snapshot.all()[0]?.lastRetrieved).toEqual(T2); // never regressed below the higher stored value
    expect(await store.revision(RZ)).toBe(base);
    const change = await store.changeSetSince(RZ, base);
    expect(change.added).toEqual([]); // neither added nor updated by the stale re-emissions
    expect(change.updated).toEqual([]);
  });

  // === setWatermark round-trip + fence + isolation (edge case 8) ========================

  it('round-trips a watermark under its epoch', async () => {
    // The positive guarded-field path: get is null until set, then set under an epoch round-trips.
    const store = new RedisDataPointStore(await freshClient());
    expect(await store.getWatermark(RZ, RZ_OP)).toBeNull();
    await store.setWatermark(RZ, RZ_OP, Revision(7), { epoch: Epoch(1) });
    expect(await store.getWatermark(RZ, RZ_OP)).toBe(7);
  });

  it('guards setWatermark by epoch and leaves the prior value', async () => {
    // A broken setWatermark that still rejects stale epochs would pass — so assert BOTH the
    // rejection and that the prior value is intact after a fenced call.
    const store = new RedisDataPointStore(await freshClient());
    await store.setWatermark(RZ, RZ_OP, Revision(7), { epoch: Epoch(1) });
    await store.write(RZ, [workEmail('a@e.example')], { epoch: Epoch(2) }); // a successor bumps the fence
    await expect(store.setWatermark(RZ, RZ_OP, Revision(99), { epoch: Epoch(1) })).rejects.toThrow(StaleEpochError);
    expect(await store.getWatermark(RZ, RZ_OP)).toBe(7); // the fenced write left the prior value
  });

  it('keeps watermarks per session and per operator', async () => {
    const store = new RedisDataPointStore(await freshClient());
    await store.setWatermark(RZ, RZ_OP, Revision(7), { epoch: Epoch(1) });
    expect(await store.getWatermark(RZ_OTHER, RZ_OP)).toBeNull(); // another session is isolated
    expect(await store.getWatermark(RZ, OperatorId('rz-other-op'))).toBeNull(); // another operator is isolated
  });

  // === rate limiter: refill, burst, skew clamp, wait, fractional rate (edge case 9) =====

  it('hands out the burst token immediately, then paces', async () => {
    const clock = new FakeClock();
    const limiter = new RedisRateLimiter(await freshClient(), clock, { ratePerSecond: 1, burst: 1 });
    await limiter.acquire('namespace-1:idp');
    expect(clock.monotonic()).toBe(0); // the burst token → immediate
    await limiter.acquire('namespace-1:idp');
    expect(clock.monotonic()).toBe(ONE_SECOND_MS); // second take waits exactly 1/rate for the next token
  });

  it('refills at most the burst over a long idle stretch', async () => {
    // The math.min(burst, ...) clamp: a long idle accrues at most `burst` tokens, never more.
    const clock = new FakeClock();
    const limiter = new RedisRateLimiter(await freshClient(), clock, { ratePerSecond: 1, burst: 2 });
    await limiter.acquire('k');
    await limiter.acquire('k'); // bucket empty at t=0
    clock.advance(60 * ONE_SECOND_MS); // 60 tokens' worth of time, but the bucket caps at burst=2

    await limiter.acquire('k');
    await limiter.acquire('k');
    expect(clock.monotonic()).toBe(60 * ONE_SECOND_MS); // both came from the capped refill — immediate
    await limiter.acquire('k');
    expect(clock.monotonic()).toBe(61 * ONE_SECOND_MS); // a full period again — no extra tokens were hoarded
  });

  it('never refills the bucket backwards under clock skew', async () => {
    // The math.max(now_ms - refill_ms, 0) clamp: if another pod stamped refill_ms into the FUTURE
    // (cross-pod clock skew), this pod's elapsed is clamped to 0 so the bucket never refills
    // backwards — the empty bucket still has to wait the full period.
    const client = await freshClient();
    const clock = new FakeClock();
    const limiter = new RedisRateLimiter(client, clock, { ratePerSecond: 1, burst: 1 });
    await limiter.acquire('skew'); // takes the burst token, leaving ~0 tokens at t=0

    const nowMs = clock.now().getTime();
    // Stamp refill_ms into the future, as a skewed peer pod would.
    await client.hSet('ratelimit:skew', { tokens: '0', refill_ms: String(nowMs + 5_000) });

    await limiter.acquire('skew');
    expect(clock.monotonic()).toBe(ONE_SECOND_MS); // elapsed clamped to 0 → a full 1/rate, not a refill
  });

  it('re-loops the wait when another pod takes the token it slept for', async () => {
    // acquire() re-runs the script after every sleep: another pod can grab the token this one slept
    // for, so the script reports a fresh wait and this pod loops again. Drain to empty, then take
    // twice across two limiters on the shared bucket: between them they consume two tokens, so total
    // elapsed is two full refill periods — proving neither slipped through on a single wait.
    const client = await freshClient();
    const clock = new FakeClock();
    const podA = new RedisRateLimiter(client, clock, { ratePerSecond: 1, burst: 1 });
    const podB = new RedisRateLimiter(client, clock, { ratePerSecond: 1, burst: 1 });
    await podA.acquire('namespace:cap'); // consume the burst token; bucket now empty at t=0

    await podA.acquire('namespace:cap'); // waits 1s
    expect(clock.monotonic()).toBe(ONE_SECOND_MS);
    await podB.acquire('namespace:cap'); // the shared bucket is empty again → another full period
    expect(clock.monotonic()).toBe(2 * ONE_SECOND_MS); // two takes after the burst cost two periods
  });

  it('computes a fractional rate and its partial tokens', async () => {
    // Fractional rate_per_ms + burst>1 partial-token arithmetic: at 2.5/s the next token after
    // draining accrues in ceil(1 / 0.0025) = 400 ms, exercising the wait_ms ceil computation.
    const clock = new FakeClock();
    const limiter = new RedisRateLimiter(await freshClient(), clock, { ratePerSecond: 2.5, burst: 2 });
    await limiter.acquire('frac'); // 2 → 1 token
    await limiter.acquire('frac'); // 1 → 0 tokens, still immediate
    expect(clock.monotonic()).toBe(0);
    await limiter.acquire('frac'); // empty: wait ceil(1 / (2.5/1000)) ms = 400 ms
    expect(clock.monotonic()).toBe(400);
  });

  // === reclaim maps per-entry delivery counts across an XPENDING id-range (edge case 10) ===

  it('maps a delivery count back to each entry across several pending ones', async () => {
    // Poison surfacing: with >1 pending entry, reclaim must map times_delivered back to EACH entry
    // by message id (the XPENDING range from the first to the last claimed id), never share a single
    // count. Drive the two entries to DIFFERENT counts, then assert each carries its own.
    const inbox = new RedisStreamsInbox(await freshClient());
    const first = await inbox.append(RZ, workEmail('e1@e.example'));
    const second = await inbox.append(RZ, personalEmail('e2@e.example'));
    await inbox.consume(RZ); // both claimed, deliveryCount 1

    await inbox.reclaim(RZ); // both now deliveryCount 2
    await inbox.ack(RZ, first, { epoch: Epoch(1) }); // remove e1 so the next reclaim only re-counts e2
    const reclaimed = await inbox.reclaim(RZ); // only e2 remains pending → reaches deliveryCount 3

    const counts = new Map(reclaimed.map((entry) => [entry.entryId, entry.deliveryCount]));
    expect(counts).toEqual(new Map([[second, 3]])); // e2 carries its own count
    expect(counts.has(first)).toBe(false); // e1 was acked away, not re-presented with a stale count
  });

  it('keeps distinct counts for entries reclaimed in one sweep', async () => {
    // The multi-id XPENDING range + per-entry mapping: two entries reclaimed in ONE XAUTOCLAIM with
    // different histories must each keep their own times_delivered, not a single shared value.
    const inbox = new RedisStreamsInbox(await freshClient());
    const first = await inbox.append(RZ, workEmail('e1@e.example'));
    await inbox.consume(RZ, { maxEntries: 1 }); // claim only e1 (deliveryCount 1)
    await inbox.reclaim(RZ); // e1 → 2
    const second = await inbox.append(RZ, personalEmail('e2@e.example'));
    await inbox.consume(RZ, { maxEntries: 1 }); // claim e2 (deliveryCount 1); e1 still pending at 2

    const reclaimed = await inbox.reclaim(RZ); // one XAUTOCLAIM re-presents both, across the id range

    const counts = new Map(reclaimed.map((entry) => [entry.entryId, entry.deliveryCount]));
    expect(counts).toEqual(
      new Map([
        [first, 3],
        [second, 2],
      ]),
    ); // each entry mapped to its OWN times_delivered, never a shared count
  });
});

/**
 * The real client with one command replaced — the seam Python's `monkeypatch` gives its test.
 *
 * Written out rather than patched onto the client, because the adapter is typed against the
 * commands it uses: substituting one of them is exactly what that structural typing is for.
 */
const withFailingGroupCreate = (source: RedisInboxClient, failure: Error): RedisInboxClient => ({
  eval: (script, options) => source.eval(script, options),
  pExpire: (key, ms) => source.pExpire(key, ms),
  get: (key) => source.get(key),
  lRange: (key, start, stop) => source.lRange(key, start, stop),
  publish: (channel, message) => source.publish(channel, message),
  xAdd: (key, id, message) => source.xAdd(key, id, message),
  xLen: (key) => source.xLen(key),
  xGroupCreate: () => Promise.reject(failure),
  xReadGroup: (group, consumer, streams, options) => source.xReadGroup(group, consumer, streams, options),
  xAutoClaim: (key, group, consumer, minIdleTime, start) =>
    source.xAutoClaim(key, group, consumer, minIdleTime, start),
  xPendingRange: (key, group, start, end, count) => source.xPendingRange(key, group, start, end, count),
  xRange: (key, start, end) => source.xRange(key, start, end),
  duplicate: (overrides) => source.duplicate(overrides),
});
