/**
 * MG-INT — the Mongo adapters against a REAL MongoDB, for what an in-process double cannot answer.
 *
 * Python's default Mongo suite runs on `mongomock-motor`, which is a reimplementation: it emulates
 * what it has grown, and does not have everything. These tests exist there to hold the two Python
 * folds honest — `flush` merges with `$setOnInsert` + `$max` in the database, and both adapters
 * reimplement that merge in code (the in-memory one to be the merge, the Mongo one to fold buffered
 * rows into a read before they are flushed), and those mirrors are only trustworthy if the thing
 * they mirror is pinned somewhere the server is real. What the double cannot execute at all is a
 * `bulkWrite` of `UpdateOne`, a `$unionWith` or a `collMod`, and it stores datetimes as it was
 * handed them rather than as BSON dates, so it cannot see a resolution the wire format does not
 * carry.
 *
 * The port runs every Mongo test against a real `mongod` (`mongodb-memory-server`), so what makes
 * these integration rather than default here is only their cost: a profiled read and two index
 * reconciliations, each a server round trip nothing else in the suite pays for. They are kept as
 * their own file because Python marks them `integration`, and because the anchors they provide —
 * the server's merge, the server's unique `_id`, the server's query plan — are worth naming
 * separately from the adapter's own mechanics.
 */

import { randomUUID } from 'node:crypto';
import type { Db } from 'mongodb';
import { MongoClient } from 'mongodb';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { MongoDataPointArchive } from '../../src/orcastork/adapters/mongo/datapoint_archive.js';
import { ArchivedDataPoint } from '../../src/orcastork/archive/index.js';
import { Epoch, SessionId } from '../../src/orcastork/ids.js';
import { ReversingCipher } from '../doubles/cipher.js';
import { NAMESPACE, T2 } from '../doubles/conformance/shared.js';
import { T0, workEmail } from '../doubles/datapoints.js';
import type { RunningMongo } from '../doubles/servers.js';
import { isMongoAvailable, startMongoServer, warnSkipped } from '../doubles/servers.js';

const MGI = SessionId('mg-integration-session');

/** A second session, whose rows a per-session read must never touch. */
const MGI_OTHER = SessionId('mg-integration-other');

/** A week and a month, in the milliseconds the adapter's retention window speaks. */
const SEVEN_DAYS_MS = 604_800_000;
const THIRTY_DAYS_MS = 2_592_000_000;

/** The same windows as the server stores them: `expireAfterSeconds` is seconds on the wire. */
const SEVEN_DAYS_SECONDS = 604_800;

const mongoAvailable = await isMongoAvailable();
if (!mongoAvailable) {
  warnSkipped('the Mongo integration suite', 'no mongod could be started and ORCASTORK_TEST_MONGO_URL is unset');
}

describe.skipIf(!mongoAvailable)('the Mongo adapters against a whole server', () => {
  let server: RunningMongo;
  let client: MongoClient;
  let openDatabases: Db[] = [];

  /** An empty database of this test's own; the `afterEach` below drops every one it minted. */
  const freshDatabase = (): Db => {
    const database = client.db(`mgi-${randomUUID()}`);
    openDatabases.push(database);
    return database;
  };

  /** The same identity every time — one `(type, valueHash)`, so re-archiving re-observes it. */
  const entry = (options: { readonly last: Date; readonly epoch: number; readonly first?: Date }): ArchivedDataPoint =>
    ArchivedDataPoint.fromDataPoint(workEmail('a@e.example', { first: options.first ?? T0, last: options.last }), {
      sessionId: MGI,
      namespaceId: NAMESPACE,
      epoch: Epoch(options.epoch),
    });

  beforeAll(async () => {
    server = await startMongoServer();
    client = new MongoClient(server.uri);
    await client.connect();
  }, 120_000);

  afterEach(async () => {
    const used = openDatabases;
    openDatabases = [];
    await Promise.all(used.map((database) => database.dropDatabase()));
  });

  afterAll(async () => {
    await client.close();
    await server.stop();
  });

  it('keeps the first sighting and advances the rest, as the server merges it', async () => {
    // The keyed upsert the folds mirror, executed by a real server: the immutable fields come from
    // the first observation and never move, while lastRetrieved and epoch advance. This is the
    // anchor that makes the two folds mirrors of the server's behaviour rather than mirrors of
    // another reimplementation that happens to agree.
    const archive = new MongoDataPointArchive(freshDatabase(), { cipher: new ReversingCipher() });

    await archive.archive(entry({ last: T0, epoch: 1 }));
    await archive.archive(entry({ last: T2, epoch: 2 }));
    await archive.flush(MGI);

    const committed = await archive.read(MGI);
    expect(committed).toHaveLength(1); // one identity, one row
    expect(committed[0]?.firstRetrieved).toEqual(T0);
    expect(committed[0]?.lastRetrieved).toEqual(T2);
    expect(committed[0]?.epoch).toBe(2);
    expect(committed[0]?.value).toBe('a@e.example'); // sealed on write, unsealed on read
  });

  it('reads the same before the flush runs as the server merge produces after it', async () => {
    // The property the buffered read is built on, checked against the real merge rather than
    // against the fold that stands in for it. If the fold ever drifts from what the server would
    // have produced, the two reads disagree here — which a double cannot notice, because the same
    // fold produces both of its answers.
    //
    // Python stamps this below the millisecond deliberately: there a buffered row validates from an
    // ISO string with microseconds and a committed one round-trips through a BSON date, so the two
    // sides can carry different instants for the same observation. A JS `Date` is already at
    // exactly the resolution a BSON date holds, so there is nothing to truncate and no finer value
    // to lose — the property is asserted the same way, and the resolution is checked below so a
    // regression that invented a finer stamp would still be caught.
    const archive = new MongoDataPointArchive(freshDatabase(), { cipher: new ReversingCipher() });

    await archive.archive(entry({ first: T0, last: T0, epoch: 1 }));
    await archive.archive(entry({ first: T0, last: T2, epoch: 2 }));

    const before = await archive.read(MGI);
    await archive.flush(MGI);
    const after = await archive.read(MGI);

    expect(before).toEqual(after);
    // Both sides at the resolution the store can actually hold, rather than one of them pretending.
    expect(Number.isInteger(after[0]?.firstRetrieved.getTime())).toBe(true);
    expect(Number.isInteger(after[0]?.lastRetrieved.getTime())).toBe(true);
  });

  it('does not duplicate a re-flushed buffer on the server', async () => {
    // A pass that commits and then dies before clearing must be safely replayable: the id is
    // derived from the value, so the re-commit lands on the same row. Worth pinning on a real
    // server because what stops the duplicate is the unique `_id`, which is the server's rule to
    // enforce.
    const archive = new MongoDataPointArchive(freshDatabase(), { cipher: new ReversingCipher() });

    await archive.archive(entry({ last: T0, epoch: 1 }));
    await archive.flush(MGI);
    await archive.archive(entry({ last: T2, epoch: 1 }));
    await archive.flush(MGI);

    expect(await archive.read(MGI)).toHaveLength(1);
    expect(await archive.bufferedCount(MGI)).toBe(0);
  });

  it('commits one page carrying the same identity twice without colliding', async () => {
    // The buffer does not deduplicate, so re-archiving one identity before a flush puts two upserts
    // for the SAME `_id` in a single batch — the first has to insert and the second merge into it,
    // rather than both attempting an insert. Verified here because how a batch of upserts onto one
    // id resolves is the server's own scheduling.
    //
    // This does not discriminate `ordered`: unordered was measured and resolves the same way on a
    // current server. What it pins is that a page carrying a repeated identity commits at all and
    // merges correctly, which is the property the flush depends on.
    const archive = new MongoDataPointArchive(freshDatabase(), { cipher: new ReversingCipher() });

    await archive.archive(entry({ last: T0, epoch: 1 }));
    await archive.archive(entry({ last: T2, epoch: 2 }));
    expect(await archive.bufferedCount(MGI)).toBe(2); // both still buffered, so one page carries both

    expect(await archive.flush(MGI)).toBe(2);

    const committed = await archive.read(MGI);
    expect(committed).toHaveLength(1);
    expect(committed[0]?.firstRetrieved).toEqual(T0); // the first upsert supplied the immutable fields
    expect(committed[0]?.lastRetrieved).toEqual(T2); // the second merged into it rather than colliding
    expect(committed[0]?.epoch).toBe(2);
  });

  it('reaches the server when the retention window is retuned', async () => {
    // The retention window is operator-tunable, so the deployed value has to be the one the reaper
    // uses. The index already exists under the same name after the first deploy, and an
    // already-existing index is otherwise left exactly as it is — so without reconciliation a
    // shortened window is a retention promise the archive quietly stops keeping. Only a real server
    // can answer this: a double has no `collMod`.
    const database = freshDatabase();
    await new MongoDataPointArchive(database, {
      cipher: new ReversingCipher(),
      retentionMs: THIRTY_DAYS_MS,
    }).archive(entry({ last: T0, epoch: 1 }));

    await new MongoDataPointArchive(database, {
      cipher: new ReversingCipher(),
      retentionMs: SEVEN_DAYS_MS,
    }).archive(entry({ last: T2, epoch: 1 }));

    const indexes = await database.collection(ArchivedDataPoint.tableName).indexInformation({ full: true });
    const reaper = indexes.find((spec) => spec.name === 'retention_reaper');
    expect(reaper?.expireAfterSeconds).toBe(SEVEN_DAYS_SECONDS);
  });

  it('still expires when retention is enabled over a plain index', async () => {
    // The mirror case, and the worse one. Mongo allows only one index per key pattern, so a plain
    // `last_retrieved` index an operator built for their own query matches on the key pattern and
    // takes the name the reaper would have used — leaving a first-time retention rollout expiring
    // nothing at all, with no error anywhere to say so.
    const database = freshDatabase();
    const committed = database.collection(ArchivedDataPoint.tableName);
    await committed.createIndex({ last_retrieved: 1 }, { name: 'built_by_ops' });

    await new MongoDataPointArchive(database, {
      cipher: new ReversingCipher(),
      retentionMs: SEVEN_DAYS_MS,
    }).archive(entry({ last: T0, epoch: 1 }));

    const indexes = await committed.indexInformation({ full: true });
    const operators = indexes.find((spec) => spec.name === 'built_by_ops');
    expect(operators?.expireAfterSeconds).toBe(SEVEN_DAYS_SECONDS); // the operator's index, now reaping
    const onLastRetrieved = indexes
      .filter((spec) => {
        const keys = Object.entries(spec.key);
        return keys.length === 1 && keys[0]?.[0] === 'last_retrieved';
      })
      .map((spec) => String(spec.name));
    expect(onLastRetrieved).toEqual(['built_by_ops']); // converted in place, not competed with
  });

  it('serves a per-session read as an indexed range rather than a scan', async () => {
    // `orcastork-datapoints` is one shared collection holding every namespace's and every session's
    // rows for the whole retention window, and nothing indexes `session_id`. Read through the
    // profiler rather than by explaining a filter written out again here, so what is asserted is
    // the query the adapter actually sent.
    const database = freshDatabase();
    const archive = new MongoDataPointArchive(database, { cipher: new ReversingCipher() });
    await archive.archive(entry({ last: T0, epoch: 1 }));
    await archive.flush(MGI);
    const other = new MongoDataPointArchive(database, { cipher: new ReversingCipher() });
    for (const address of ['b@e.example', 'c@e.example', 'd@e.example']) {
      await other.archive(
        ArchivedDataPoint.fromDataPoint(workEmail(address, { first: T0, last: T0 }), {
          sessionId: MGI_OTHER,
          namespaceId: NAMESPACE,
          epoch: Epoch(1),
        }),
      );
    }
    await other.flush(MGI_OTHER);

    await database.command({ profile: 2 });
    try {
      expect(await archive.read(MGI)).toHaveLength(1);
    } finally {
      await database.command({ profile: 0 });
    }

    const namespace = `${database.databaseName}.${ArchivedDataPoint.tableName}`;
    const reads = await database.collection('system.profile').find({ op: 'query', ns: namespace }).toArray();
    expect(reads.length).toBeGreaterThan(0); // the profiler saw a read of the committed collection
    expect(reads.map((read) => String(read.planSummary)).filter((plan) => plan.includes('COLLSCAN'))).toEqual([]);
    // The other session's three rows were never touched: an unindexed equality would have read them.
    expect(reads.reduce((total, read) => total + Number(read.docsExamined), 0)).toBe(1);
  });

  it.skip('drops the database it used (the pytest fixture itself, which the port does not have)', () => {
    // Python's MGI-08 drives `create_mongo_fixture`'s teardown over a throwaway database name,
    // because that fixture mints a database per test and drops none of them. The port has no such
    // fixture: every suite here mints its own database and drops it in its own `afterEach`, so
    // there is nothing left to hold to account.
  });
});
