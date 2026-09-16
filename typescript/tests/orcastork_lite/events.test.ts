/** The session event sinks: the in-memory one, and the Redis stream adapter against a real server. */

import { createClient, type RedisClientType } from 'redis';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { InMemorySessionEventSink } from '../../src/orcastork_lite/adapters/memory.js';
import { DEFAULT_TTL_MS, RedisSessionEventSink } from '../../src/orcastork_lite/adapters/redis.js';
import type { DataPointMerged as DataPointMergedEvent } from '../../src/orcastork_lite/index.js';
import {
  DataPointMerged,
  NamespaceId,
  OperatorId,
  SessionCompleted,
  SessionId,
} from '../../src/orcastork_lite/index.js';
import { FakeClock } from '../doubles/clock.js';
import type { RunningRedis } from '../doubles/servers.js';
import { isRedisAvailable, startRedisServer, warnSkipped } from '../doubles/servers.js';

const merged = (session: string, value: unknown, clock: FakeClock): DataPointMergedEvent =>
  DataPointMerged({
    sessionId: SessionId(session),
    namespaceId: NamespaceId('ns'),
    at: clock.now(),
    dataPointType: 'Ip',
    value,
    retrievedBy: OperatorId('op'),
    merge: 'added',
    revision: 1,
  });

describe('InMemorySessionEventSink', () => {
  it('keeps every event and filters by session', async () => {
    const clock = new FakeClock();
    const sink = new InMemorySessionEventSink();

    await sink.publish(merged('a', 1, clock));
    await sink.publish(merged('b', 2, clock));

    expect(sink.forSession(SessionId('a')).map((event) => event.sessionId)).toEqual(['a']);
    expect(sink.events.length).toBe(2);
  });
});

const redisAvailable = isRedisAvailable();
if (!redisAvailable) {
  warnSkipped('RedisSessionEventSink', 'no redis-server on PATH and no ORCASTORK_TEST_REDIS_URL');
}

describe.skipIf(!redisAvailable)('RedisSessionEventSink', () => {
  let server: RunningRedis;
  let client: RedisClientType;

  beforeAll(async () => {
    server = await startRedisServer();
  });

  afterAll(async () => {
    await server.stop();
  });

  beforeEach(async () => {
    client = createClient({ url: server.url });
    await client.connect();
    // A provided server may be shared, so each test starts from an empty keyspace of its own making.
    await client.flushAll();
  });

  afterEach(async () => {
    await client.close();
  });

  it('appends one entry per event to the session stream', async () => {
    const clock = new FakeClock();
    const sink = new RedisSessionEventSink(client, { maxlen: 100 });

    await sink.publish(merged('s1', { a: [1, 2] }, clock));
    await sink.publish(
      SessionCompleted({
        sessionId: SessionId('s1'),
        namespaceId: NamespaceId('ns'),
        at: clock.now(),
        deadlineHit: false,
        operatorRuns: new Map([[OperatorId('op'), 1]]),
        failures: new Map(),
      }),
    );
    await sink.publish(merged('s2', 'other session', clock));

    const entries = await client.xRange(sink.streamKey(SessionId('s1')), '-', '+');
    expect(entries.map((entry) => entry.message.kind)).toEqual(['data_point_merged', 'session_completed']);
    const first = JSON.parse(entries[0]?.message.event ?? '') as Record<string, unknown>;
    expect(first.value).toEqual({ a: [1, 2] });
    expect(first.merge).toBe('added');
    expect(first.session_id).toBe('s1');
    // ISO-8601 in UTC, with no fractional part on a whole second — what a Python consumer parses back.
    expect(first.at).toBe(clock.now().toISOString().replace('.000Z', 'Z'));
    expect((await client.xRange(sink.streamKey(SessionId('s2')), '-', '+')).length).toBe(1);
  });

  it('renders values JSON cannot express and caps the stream', async () => {
    class Opaque {
      public toString(): string {
        return 'Opaque()';
      }
    }
    const clock = new FakeClock();
    const sink = new RedisSessionEventSink(client, { keyPrefix: 'x:', maxlen: 1 });

    for (const value of [new Opaque(), 'second']) {
      await sink.publish(merged('s', value, clock));
    }

    const entries = await client.xRange('x:s', '-', '+');
    expect(entries.length).toBeLessThanOrEqual(2); // approximate trimming honours the cap loosely, but never grows unbounded
    const payloads = entries.map((entry) => (JSON.parse(entry.message.event ?? '') as Record<string, unknown>).value);
    expect(payloads.at(-1)).toBe('second');
    if (payloads.length === 2) {
      expect(payloads[0]).toBe('Opaque()');
    }
  });

  it('arms a sliding TTL on the stream key', async () => {
    const clock = new FakeClock();
    const sink = new RedisSessionEventSink(client, { ttlMs: 600_000 });
    const key = sink.streamKey(SessionId('s'));

    await sink.publish(merged('s', 1, clock));

    const firstTtl = await client.pTTL(key);
    expect(firstTtl).toBeGreaterThan(0);
    expect(firstTtl).toBeLessThanOrEqual(600_000);

    await client.expire(key, 5); // simulate time passing: the key is close to expiring
    await sink.publish(merged('s', 2, clock));
    expect(await client.pTTL(key)).toBeGreaterThan(5_000); // a live session re-arms the full window
    expect(DEFAULT_TTL_MS).toBe(24 * 60 * 60 * 1_000);

    const forever = new RedisSessionEventSink(client, { keyPrefix: 'keep:', ttlMs: null });
    await forever.publish(merged('s', 3, clock));
    expect(await client.pTTL('keep:s')).toBe(-1); // opted out: no expiry at all
  });
});
