/**
 * The server helpers must actually produce a server a client can talk to.
 *
 * Every adapter suite is built on these two functions, so a helper that quietly starts nothing
 * would turn a whole layer of tests into tests of nothing. This file is the smoke test that keeps
 * that from being possible: it connects, and it speaks to the server.
 */

import { MongoClient } from 'mongodb';
import { createClient } from 'redis';
import { describe, expect, it } from 'vitest';
import {
  isMongoAvailable,
  isRedisAvailable,
  providedRedisUrl,
  startMongoServer,
  startRedisServer,
  warnSkipped,
} from './servers.js';

const redisAvailable = isRedisAvailable();
if (!redisAvailable) {
  warnSkipped('the Redis server helper', 'redis-server is not on PATH and ORCASTORK_TEST_REDIS_URL is unset');
}

// Resolved while the file is being collected: whether a Mongo suite can run may involve fetching a
// server binary, and `describe.skipIf` needs the answer before the first test is registered.
const mongoAvailable = await isMongoAvailable();
if (!mongoAvailable) {
  warnSkipped('the MongoDB server helper', 'mongodb-memory-server could not provide a server binary');
}

describe.skipIf(!redisAvailable)('startRedisServer', () => {
  it('starts a server that answers PING and holds a value', { timeout: 60_000 }, async () => {
    const server = await startRedisServer();
    const client = createClient({ url: server.url });
    try {
      await client.connect();

      await expect(client.ping()).resolves.toBe('PONG');
      await client.set('orcastork:smoke', 'ok');
      await expect(client.get('orcastork:smoke')).resolves.toBe('ok');
    } finally {
      await client.close();
      await server.stop();
    }
  });

  // Two calls hand back the same externally provided server, by design, so there is nothing to
  // assert about ports in that mode.
  it.skipIf(providedRedisUrl() !== undefined)(
    'gives each spawned server its own port, so two suites can run side by side',
    { timeout: 60_000 },
    async () => {
      const first = await startRedisServer();
      const second = await startRedisServer();
      try {
        expect(first.url).not.toBe(second.url);
      } finally {
        await first.stop();
        await second.stop();
      }
    },
  );
});

describe.skipIf(!mongoAvailable)('startMongoServer', () => {
  it('starts a server that answers a ping and stores a document', { timeout: 120_000 }, async () => {
    const server = await startMongoServer();
    const client = new MongoClient(server.uri);
    try {
      await client.connect();

      await expect(client.db('admin').command({ ping: 1 })).resolves.toMatchObject({ ok: 1 });
      const collection = client.db('orcastork_smoke').collection<{ _id: string; value: string }>('smoke');
      await collection.insertOne({ _id: 'one', value: 'ok' });
      await expect(collection.findOne({ _id: 'one' })).resolves.toMatchObject({ value: 'ok' });
    } finally {
      await client.close();
      await server.stop();
    }
  });
});
