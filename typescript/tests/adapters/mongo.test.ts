/**
 * MG — Mongo adapters: the CNF contracts bound to Mongo + backend-specific mechanics.
 *
 * Python runs these against in-process `mongomock-motor`, and its own notes list, one by one, the
 * behaviours that double has grown which a real server does not share. The port runs them against
 * `mongodb-memory-server` — an ephemeral `mongod`, so a real server — which is what makes an
 * adapter test evidence about the thing that will run in production. MG-05 (encrypted-field
 * round-trip) is out of scope there and here: the framework does not encrypt; that is a
 * flow/persistence concern.
 *
 * **Why the fixtures are large where Python's are small.** Python narrows a module's `_PAGE_SIZE`
 * with `monkeypatch` so four rows span two pages. An ESM module constant cannot be reassigned, so
 * the tests about paging size their fixtures from the real `PAGE_SIZE` instead: the assertion is
 * the same (this many pages, this many round trips), and it is made against the page size the
 * production adapter actually uses.
 */

import { randomUUID } from 'node:crypto';
import type { Collection, Db, DeleteResult, Filter, FindOneAndUpdateOptions, UpdateFilter } from 'mongodb';
import { MongoBulkWriteError, MongoClient, MongoServerError } from 'mongodb';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { AUDIT_LOG_COLLECTION, MongoAuditSink } from '../../src/orcastork/adapters/mongo/audit_sink.js';
import {
  MongoDataPointArchive,
  foldIn as mongoFoldIn,
  PAGE_SIZE,
} from '../../src/orcastork/adapters/mongo/datapoint_archive.js';
import { MongoDurableStore } from '../../src/orcastork/adapters/mongo/durable_store.js';
import type { StringIdDocument } from '../../src/orcastork/adapters/mongo/indexes.js';
import type { NamespaceCipherProvider, ValueCipher } from '../../src/orcastork/archive/index.js';
import { ArchivedDataPoint } from '../../src/orcastork/archive/index.js';
import type { AuditLogEntry } from '../../src/orcastork/audit/index.js';
import { AuditKind, AuditLogEntry as makeAuditLogEntry } from '../../src/orcastork/audit/index.js';
import type { AnyDataPoint } from '../../src/orcastork/datapoints/index.js';
import {
  OptimisticConcurrencyError,
  PiiKeyUnavailableError,
  StaleEpochError,
  UnprotectedPiiError,
} from '../../src/orcastork/exceptions.js';
import { Epoch, type NamespaceId, OperatorId, SessionId } from '../../src/orcastork/ids.js';
import { ReversingCipher } from '../doubles/cipher.js';
import { describeAuditSinkConformance } from '../doubles/conformance/audit_sink.js';
import { describeDataPointArchiveConformance } from '../doubles/conformance/datapoint_archive.js';
import { describeDurableStoreConformance } from '../doubles/conformance/durable_store.js';
import { NAMESPACE, TBL } from '../doubles/conformance/shared.js';
import { risk, T0, workEmail } from '../doubles/datapoints.js';
import type { RunningMongo } from '../doubles/servers.js';
import { isMongoAvailable, startMongoServer, warnSkipped } from '../doubles/servers.js';

const MG = SessionId('mg-session');

/** The byte every composite `_id` in this family joins its parts with. */
const KEY_SEPARATOR = '\u0000';

/** Two hours after {@link T0} — a re-observation far enough away to be unmistakable. */
const T2 = new Date(T0.getTime() + 2 * 60 * 60 * 1000);

/** The collections an archive touches, for the tests that assert on their indexes. */
const ARCHIVE_COLLECTIONS = [ArchivedDataPoint.tableName, 'datapoint_archive_buffer'] as const;

/** The shape of a refusal from a credential that may not create an index at all. */
const INDEX_NOT_AUTHORIZED = 'not authorized on test to execute command createIndexes';

/** The server's refusal when an incompatible index already exists. */
const INDEX_ALREADY_EXISTS = 'Index with pattern already exists with a different name';

/** One audit entry for the session under test. */
const auditEntry = (epoch = 1, kind: AuditKind = AuditKind.DATA_POINT_ADDED): AuditLogEntry =>
  makeAuditLogEntry({ sessionId: MG, epoch: Epoch(epoch), timestamp: T0, kind });

/** One archive document for `session`, observed by `epoch`. */
const archived = (
  dataPoint: AnyDataPoint,
  options: { readonly session?: SessionId; readonly epoch?: number } = {},
): ArchivedDataPoint =>
  ArchivedDataPoint.fromDataPoint(dataPoint, {
    sessionId: options.session ?? MG,
    namespaceId: NAMESPACE,
    epoch: Epoch(options.epoch ?? 1),
  });

/** A server error the way the driver raises it, so a double refuses exactly as a server does. */
const serverError = (message: string, code: number): MongoServerError => new MongoServerError({ message, code });

// --- the driver doubles ----------------------------------------------------------------

/**
 * A driver object with some of its methods replaced.
 *
 * The port of the Python suite's `_XxxCollection` / `_XxxDatabase` wrappers. A `Proxy` rather than
 * a subclass or a spread: `Db` and `Collection` keep private state, so every method the wrapper
 * does not override has to run with `this` bound to the original object.
 */
const overriding = <T extends object>(inner: T, overrides: Readonly<Record<string, unknown>>): T =>
  new Proxy(inner, {
    get: (target, property) => {
      if (typeof property === 'string' && Object.hasOwn(overrides, property)) {
        return overrides[property];
      }
      const value = Reflect.get(target, property, target) as unknown;
      return typeof value === 'function' ? (value as (...args: unknown[]) => unknown).bind(target) : value;
    },
  });

/** One collection of this family's own string-keyed documents. */
type StringIdCollection = Collection<StringIdDocument>;

/**
 * A database whose collections are routed through `route`, one wrapper per name.
 *
 * Memoized per name, like the Python doubles: an adapter takes its collections in its constructor,
 * and a counter that was handed out twice counts half of what happened.
 */
const routedDatabase = (inner: Db, route: (name: string, collection: StringIdCollection) => object): Db => {
  const wrapped = new Map<string, object>();
  return overriding(inner, {
    collection: (name: string) => {
      const known = wrapped.get(name);
      if (known !== undefined) {
        return known;
      }
      const created = route(name, inner.collection<StringIdDocument>(name));
      wrapped.set(name, created);
      return created;
    },
  });
};

/** What one collection was asked to do — the tests that are about round trips read these. */
interface CollectionCalls {
  findOneAndUpdates: number;
  insertManys: number;
  bulkWrites: number;
  updateOnes: number;
  finds: number;
  createIndexes: number;
}

/**
 * A database that counts what each collection was asked to do.
 *
 * Stands in for Python's `_CountingCollection`, `_CommitCountingCollection` and
 * `_FindCountingCollection` at once: they differ only in which calls they tally, and one wrapper
 * that tallies all of them answers every one of those tests.
 */
const countingDatabase = (inner: Db): { readonly database: Db; readonly calls: (name: string) => CollectionCalls } => {
  const tallies = new Map<string, CollectionCalls>();
  const calls = (name: string): CollectionCalls => {
    const known = tallies.get(name);
    if (known !== undefined) {
      return known;
    }
    const created: CollectionCalls = {
      findOneAndUpdates: 0,
      insertManys: 0,
      bulkWrites: 0,
      updateOnes: 0,
      finds: 0,
      createIndexes: 0,
    };
    tallies.set(name, created);
    return created;
  };
  const database = routedDatabase(inner, (name, collection) => {
    const tally = calls(name);
    return overriding(collection, {
      findOneAndUpdate: (
        filter: Filter<StringIdDocument>,
        update: UpdateFilter<StringIdDocument>,
        options: FindOneAndUpdateOptions,
      ) => {
        tally.findOneAndUpdates += 1;
        return collection.findOneAndUpdate(filter, update, options);
      },
      insertMany: (documents: StringIdDocument[], options: Parameters<StringIdCollection['insertMany']>[1]) => {
        tally.insertManys += 1;
        return collection.insertMany(documents, options);
      },
      bulkWrite: (
        operations: Parameters<StringIdCollection['bulkWrite']>[0],
        options: Parameters<StringIdCollection['bulkWrite']>[1],
      ) => {
        tally.bulkWrites += 1;
        return collection.bulkWrite(operations, options);
      },
      updateOne: (
        filter: Filter<StringIdDocument>,
        update: UpdateFilter<StringIdDocument>,
        options: Parameters<StringIdCollection['updateOne']>[2],
      ) => {
        tally.updateOnes += 1;
        return collection.updateOne(filter, update, options);
      },
      find: (filter: Filter<StringIdDocument>) => {
        tally.finds += 1;
        return collection.find(filter);
      },
      createIndex: (
        keys: Parameters<StringIdCollection['createIndex']>[0],
        options: Parameters<StringIdCollection['createIndex']>[1],
      ) => {
        tally.createIndexes += 1;
        return collection.createIndex(keys, options);
      },
    });
  });
  return { database, calls };
};

/**
 * One table's `insertOne` fails as the loser of a concurrent first-insert race.
 *
 * Two writers both saw the key absent at `expectedVersion` 0; the loser collides on `_id`. The
 * epoch guard's `updateOne` (a different collection) and the absence probe behave normally.
 */
const insertOneFailingDatabase = (inner: Db, table: string): Db =>
  routedDatabase(inner, (name, collection) =>
    name === table
      ? overriding(collection, {
          insertOne: () => Promise.reject(serverError('E11000 duplicate key error: _id', 11000)),
        })
      : collection,
  );

/**
 * One table lands another writer's set member at the moment the collection is read back.
 *
 * Stands in for a concurrent `addToSet` on the same key: harmless if the size is reported by the
 * write itself, and visible in the returned count if the size comes from a follow-up read.
 */
const concurrentAddOnReadDatabase = (inner: Db, table: string): Db =>
  routedDatabase(inner, (name, collection) =>
    name === table
      ? overriding(collection, {
          findOne: async (filter: Filter<StringIdDocument>) => {
            await collection.updateOne(filter, { $addToSet: { members: 'another-writers-value' } });
            return collection.findOne(filter);
          },
        })
      : collection,
  );

/**
 * The archive's commit fails once `succeedFirst` pages have landed — a flush interrupted partway.
 *
 * Counted in pages rather than rows, because a page is one round trip: the flush commits a whole
 * batch per call, so a page is the smallest unit an interruption can land between.
 */
const commitFailingAfterDatabase = (inner: Db, succeedFirst: number): Db => {
  let remaining = succeedFirst;
  return routedDatabase(inner, (name, collection) =>
    name === ArchivedDataPoint.tableName
      ? overriding(collection, {
          bulkWrite: (
            operations: Parameters<StringIdCollection['bulkWrite']>[0],
            options: Parameters<StringIdCollection['bulkWrite']>[1],
          ) => {
            if (remaining <= 0) {
              return Promise.reject(new Error('commit exploded partway through the flush'));
            }
            remaining -= 1;
            return collection.bulkWrite(operations, options);
          },
        })
      : collection,
  );
};

/**
 * `createIndex` is refused, by default the way a server does when an incompatible one exists.
 *
 * The refusal message is a parameter because the two ways this happens are different situations
 * that have to degrade identically: an incompatible index already present, and a credential that is
 * not allowed to create one at all.
 */
const indexRefusing = (collection: StringIdCollection, message: string): object =>
  overriding(collection, { createIndex: () => Promise.reject(serverError(message, 67)) });

/** One collection's `createIndex` is refused; everything else behaves normally. */
const indexRefusingDatabase = (inner: Db, table: string, message = INDEX_ALREADY_EXISTS): Db =>
  routedDatabase(inner, (name, collection) => (name === table ? indexRefusing(collection, message) : collection));

/**
 * `createIndex` is refused on every collection — the shape of a read-only credential.
 *
 * Whole-database rather than per-table because a single archive call ensures indexes on the
 * committed collection AND the buffer, so refusing only one of them leaves the other free to
 * succeed and the degraded path only half-exercised.
 */
const everyIndexRefusingDatabase = (inner: Db, message: string): Db =>
  routedDatabase(inner, (_name, collection) => indexRefusing(collection, message));

/**
 * The archive's buffer never clears, simulating a crash AFTER the committing upserts but BEFORE
 * the buffer is drained. Flipping `suppressed` back lets the resume re-flush.
 */
const deleteSuppressingBuffer = (inner: Db): { readonly database: Db; restore: () => void } => {
  const state = { suppressed: true };
  const database = routedDatabase(inner, (name, collection) =>
    name === 'datapoint_archive_buffer'
      ? overriding(collection, {
          deleteMany: (filter: Filter<StringIdDocument>): Promise<DeleteResult> =>
            state.suppressed
              ? Promise.resolve({ acknowledged: true, deletedCount: 0 })
              : collection.deleteMany(filter),
        })
      : collection,
  );
  return {
    database,
    restore: () => {
      state.suppressed = false;
    },
  };
};

/** A provider with a cipher for the namespaces it knows, and no key at all for the rest. */
class PerNamespaceProvider implements NamespaceCipherProvider {
  private readonly known: ReadonlyMap<string, ValueCipher>;

  public constructor(known: Readonly<Record<string, ValueCipher>>) {
    this.known = new Map(Object.entries(known));
  }

  public async forNamespace(namespaceId: NamespaceId): Promise<ValueCipher> {
    const cipher = this.known.get(namespaceId);
    if (cipher === undefined) {
      throw new Error(`no key for ${namespaceId}`);
    }
    return cipher;
  }
}

// --- the fold, which needs no server at all --------------------------------------------

describe('the Mongo archive fold', () => {
  it('refuses to walk a stored epoch back', () => {
    // The Mongo fold is `read`'s stand-in for the server's `$max`, and has to refuse an
    // out-of-order pair for the same reason the server does. Asserted here rather than through the
    // port: the meta CAS rejects an epoch below the session's high water mark, so an older-epoch
    // re-observation can never be buffered and no sequence of archive/flush/read calls can tell
    // `max(existing, sealed)` apart from taking whichever one arrived last.
    const entry = (epoch: number, last: Date): ArchivedDataPoint =>
      archived(workEmail('a@e.example', { first: T0, last }), { epoch });

    const folded = new Map<string, ArchivedDataPoint>();
    mongoFoldIn(folded, entry(2, T2));
    mongoFoldIn(folded, entry(1, T0));

    const merged = [...folded.values()];
    expect(merged).toHaveLength(1);
    expect(merged[0]?.epoch).toBe(2); // not 1 — the out-of-order arrival does not lower it
    expect(merged[0]?.lastRetrieved).toEqual(T2); // nor does it walk the timestamp back
  });
});

// --- everything else, against a real server --------------------------------------------

const mongoAvailable = await isMongoAvailable();
if (!mongoAvailable) {
  warnSkipped('the Mongo adapter suite', 'no mongod could be started and ORCASTORK_TEST_MONGO_URL is unset');
}

describe.skipIf(!mongoAvailable)('the Mongo adapter family', () => {
  let server: RunningMongo;
  let client: MongoClient;

  /** Every database this test minted; the one `afterEach` below owns dropping all of them. */
  let openDatabases: Db[] = [];

  /**
   * An empty database of this test's own.
   *
   * One server for the file and a fresh database per adapter: a provided server may be shared, and
   * nothing in these contracts may see another test's documents.
   */
  const freshDatabase = (): Db => {
    const database = client.db(`mg-${randomUUID()}`);
    openDatabases.push(database);
    return database;
  };

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

  describeDurableStoreConformance({
    name: 'MongoDurableStore',
    create: () =>
      Promise.resolve({
        durable: new MongoDurableStore(freshDatabase()),
        // The durable store keeps no expiring state, so nothing in its contract waits on a clock.
        advanceTime: () => Promise.resolve(),
      }),
  });

  describeAuditSinkConformance({
    name: 'MongoAuditSink',
    create: () =>
      Promise.resolve({
        audit: new MongoAuditSink(freshDatabase()),
        advanceTime: () => Promise.resolve(),
      }),
  });

  describeDataPointArchiveConformance({
    name: 'MongoDataPointArchive',
    create: () =>
      Promise.resolve({
        // A cipher is wired so PII conformance entries pass the production fail-closed guard.
        archive: new MongoDataPointArchive(freshDatabase(), { cipher: new ReversingCipher() }),
        advanceTime: () => Promise.resolve(),
      }),
  });

  // === the durable store ===============================================================

  it('surfaces a lost first-insert race as an OCC conflict', async () => {
    // Two writers both observe the key absent (findOne null, expectedVersion 0) and both insert;
    // the loser collides on _id. A lost first-insert race is an OCC conflict, not a raw adapter
    // leak, so the driver's duplicate-key error must surface as OptimisticConcurrencyError with the
    // original chained.
    const durable = new MongoDurableStore(insertOneFailingDatabase(freshDatabase(), TBL));
    const failure = await durable
      .upsert(TBL, 'k', { a: 1 }, { expectedVersion: 0, epoch: Epoch(1) })
      .catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(OptimisticConcurrencyError);
    expect((failure as Error).message).toMatch(/concurrent insert/);
    expect((failure as Error).cause).toBeInstanceOf(MongoServerError); // the driver's own error, chained
  });

  it('reports the set as of this write, not as of a follow-up read', async () => {
    // A caller uses the returned size to decide something about its own membership, so it has to
    // describe the set including this write and nothing that landed after it. Reporting from a
    // follow-up read instead lets a concurrent add on the same key inflate the answer, which
    // corresponds to no single writer's view. The epoch guard does not help — it fences a
    // superseded predecessor, not two writers at the same epoch.
    const durable = new MongoDurableStore(concurrentAddOnReadDatabase(freshDatabase(), TBL));
    expect(await durable.addToSet(TBL, 'k', 'sessions', 'mine', { epoch: Epoch(1) })).toBe(1);
  });

  it('lands the durable epoch guard in the fencing collection, not the data one', async () => {
    // After an upsert, the epoch-guard doc lives in <table>-fencing (_id = key), NOT in the data
    // collection. The data collection must contain exactly one document (the versioned payload)
    // with no durable-epoch sentinel docs.
    const database = freshDatabase();
    const durable = new MongoDurableStore(database);
    await durable.upsert(TBL, 'k', { a: 1 }, { expectedVersion: 0, epoch: Epoch(1) });

    const guard = await database.collection<StringIdDocument>(`${TBL}-fencing`).findOne({ _id: 'k' });
    expect(guard).not.toBeNull();
    expect(guard?.epoch).toBe(1);
    // Stamped by the server (`$currentDate`), so the TTL reaper acts on its own clock.
    expect(guard?.updated_at).toBeInstanceOf(Date);

    const dataIds = (await database.collection(TBL).find({}).toArray()).map((document) => String(document._id));
    expect(dataIds).toEqual(['k']); // only the versioned payload — no sentinel beside it
  });

  it('still fences a lower epoch from the fencing collection', async () => {
    const durable = new MongoDurableStore(freshDatabase());
    await durable.upsert(TBL, 'k', { a: 1 }, { expectedVersion: 0, epoch: Epoch(2) });
    await expect(durable.upsert(TBL, 'k', { a: 2 }, { expectedVersion: 1, epoch: Epoch(1) })).rejects.toThrow(
      StaleEpochError,
    );
  });

  it('shares one fencing document between a set-add and an upsert', async () => {
    // addToSet and upsert write their guards to the same <table>-fencing document (_id = key), so
    // they share one fence — a prior addToSet at epoch 2 fences a lower-epoch upsert.
    const database = freshDatabase();
    const durable = new MongoDurableStore(database);
    await durable.addToSet(TBL, 'k', 'sessions', 'v1', { epoch: Epoch(2) });

    const guard = await database.collection<StringIdDocument>(`${TBL}-fencing`).findOne({ _id: 'k' });
    expect(guard?.epoch).toBe(2); // the guard is in the fencing collection

    await expect(durable.upsert(TBL, 'k', { a: 1 }, { expectedVersion: 0, epoch: Epoch(1) })).rejects.toThrow(
      StaleEpochError,
    );
  });

  it('keeps contribution guards and contribution markers in separate collections', async () => {
    // The contribution epoch guard must land in contributions-fencing (_id = sessionId), while the
    // real marker (_id = session\x00operator) stays in contributions.
    const database = freshDatabase();
    const durable = new MongoDurableStore(database);
    expect(await durable.markContribution(MG, OperatorId('op-a'), { epoch: Epoch(1) })).toBe(true);

    const guard = await database.collection<StringIdDocument>('contributions-fencing').findOne({ _id: MG });
    expect(guard).not.toBeNull();
    expect(guard?.epoch).toBe(1);
    expect(guard?.updated_at).toBeInstanceOf(Date);

    const marker = await database
      .collection<StringIdDocument>('contributions')
      .findOne({ _id: `${MG}${KEY_SEPARATOR}op-a` });
    expect(marker).not.toBeNull();

    const markerIds = (await database.collection('contributions').find({}).toArray()).map((document) =>
      String(document._id),
    );
    expect(markerIds).toEqual([`${MG}${KEY_SEPARATOR}op-a`]); // no epoch sentinel among the markers
  });

  it('puts the configured TTL on a table fencing collection', async () => {
    const database = freshDatabase();
    const durable = new MongoDurableStore(database, { fencingTtlMs: 3_600_000 });
    await durable.upsert(TBL, 'k', { a: 1 }, { expectedVersion: 0, epoch: Epoch(1) });

    const ttl = await ttlIndexOn(database.collection(`${TBL}-fencing`), 'updated_at');
    expect(ttl).toBe(3600); // seconds on the wire, milliseconds at the adapter's boundary
  });

  it('puts the configured TTL on the contributions fencing collection', async () => {
    const database = freshDatabase();
    const durable = new MongoDurableStore(database, { fencingTtlMs: 1_800_000 });
    await durable.markContribution(MG, OperatorId('op-b'), { epoch: Epoch(1) });

    expect(await ttlIndexOn(database.collection('contributions-fencing'), 'updated_at')).toBe(1800);
  });

  // === indexes =========================================================================

  it('indexes the audit log for the only way it is read', async () => {
    // Every reader of this collection — `replay`, and any timeline view built over it — asks for
    // one session's trail in sequence order. Unindexed that is a scan of every session's rows plus
    // an in-memory sort, so the cost of reading one session grows with the size of the whole log.
    // There is no migration step that could have created it: the collection appears on first write.
    const database = freshDatabase();
    await new MongoAuditSink(database).append(auditEntry());

    expect(await indexKeys(database.collection(AUDIT_LOG_COLLECTION))).toContainEqual({
      session_id: 1,
      sequence: 1,
    });
  });

  it('indexes the archive buffer for the paged flush', async () => {
    // The flush reads the buffer by session in sequence order a page at a time, and `bufferedCount`
    // counts by session. Unindexed, every page scans every session's buffered rows.
    const database = freshDatabase();
    const archive = new MongoDataPointArchive(database, { cipher: new ReversingCipher() });
    await archive.archive(archived(workEmail('a@e.example')));

    expect(await indexKeys(database.collection('datapoint_archive_buffer'))).toContainEqual({
      session_id: 1,
      sequence: 1,
    });
  });

  it('leaves an equivalent index under another name alone', async () => {
    // Mongo refuses a same-keys index under a different name, so creating unconditionally would
    // turn an index someone already made by hand into an exception on the append path. Gating on
    // the key pattern as well as the name makes that case a no-op, and leaves the operator's index
    // untouched rather than competing with it.
    const database = freshDatabase();
    const collection = database.collection(AUDIT_LOG_COLLECTION);
    await collection.createIndex({ session_id: 1, sequence: 1 }, { name: 'made_by_ops' });

    await new MongoAuditSink(database).append(auditEntry());

    expect(await indexNames(collection)).toEqual(['_id_', 'made_by_ops']);
  });

  it('completes the append when the index is refused', async () => {
    // An index is an optimization: its absence is a slow read, so a process that cannot create one
    // must still complete the write it was actually doing. The entry has to land regardless.
    const sink = new MongoAuditSink(indexRefusingDatabase(freshDatabase(), AUDIT_LOG_COLLECTION));
    await sink.append(auditEntry());

    expect(await sink.replay(MG)).toHaveLength(1);
  });

  it('completes an archive read when every index is refused', async () => {
    // `read` ensures its indexes too, so a credential that may not create one reaches `createIndex`
    // on the way to every answer the archive gives, not just on the way to a write. The same
    // tolerance an append gets has to hold here — an absent index is a collection scan, so the rows
    // still come back, unindexed and slower — and nothing else pins it: the audit sink's reader
    // ensures nothing.
    const database = freshDatabase();
    await seedAnUnindexedArchive(database);
    const reader = new MongoDataPointArchive(everyIndexRefusingDatabase(database, INDEX_NOT_AUTHORIZED), {
      cipher: new ReversingCipher(),
    });

    // Both sides of the fold, so the refusal on the buffer's index is exercised as well as the one
    // on the committed collection's.
    const values = new Set((await reader.read(MG)).map((entry) => entry.value));
    expect(values).toEqual(new Set(['a@e.example', 'b@e.example']));
    await expectNothingIndexed(database);
  });

  it('completes an archive flush when every index is refused', async () => {
    // The same for the drain. Pinned separately from the read rather than asserted after it in one
    // test, because the ensure is one-shot per adapter: whichever call ran first would spend the
    // refusal and leave the other running with the flag already set, which is not reaching the
    // refusal rather than surviving it.
    const database = freshDatabase();
    await seedAnUnindexedArchive(database);
    const flusher = new MongoDataPointArchive(everyIndexRefusingDatabase(database, INDEX_NOT_AUTHORIZED), {
      cipher: new ReversingCipher(),
    });

    expect(await flusher.flush(MG)).toBe(1);
    await expectNothingIndexed(database);
  });

  it('builds its indexes once across archive calls', async () => {
    // ensureIndexes guards on a one-shot flag, so a second archive must NOT rebuild indexes.
    const counting = countingDatabase(freshDatabase());
    const archive = new MongoDataPointArchive(counting.database, {
      cipher: new ReversingCipher(),
      retentionMs: 3_600_000,
    });
    const committed = counting.calls(ArchivedDataPoint.tableName);

    await archive.archive(archived(workEmail('a@e.example')));
    const afterFirst = committed.createIndexes;
    expect(afterFirst).toBeGreaterThan(0); // the first write built the analytics + TTL indexes

    await archive.archive(archived(workEmail('b@e.example')));
    expect(committed.createIndexes).toBe(afterFirst); // the one-shot flag short-circuited the second
  });

  // === the audit sink ==================================================================

  it('makes an append readable by another process with no recovery step', async () => {
    const database = freshDatabase();
    const sink = new MongoAuditSink(database);
    for (let index = 0; index < 3; index += 1) {
      await sink.append(auditEntry());
    }

    // The writer is abandoned here — no completion tail, no resume. A fresh sink on the same
    // database (another pod, or the timeline reader) still sees the whole ordered trail, because
    // the appends went to the committed log rather than to a staging area only the writer would
    // have drained.
    expect(await new MongoAuditSink(database).replay(MG)).toHaveLength(3);
    const rows = await database
      .collection(AUDIT_LOG_COLLECTION)
      .find({ session_id: MG })
      .sort('sequence', 1)
      .toArray();
    expect(rows.map((row) => row.sequence)).toEqual([1, 2, 3]); // contiguous, in the committed log
  });

  it('persists the audit timestamp as a date, not a string', async () => {
    const database = freshDatabase();
    const sink = new MongoAuditSink(database);
    await sink.append(auditEntry());

    // The committed entry's timestamp is a BSON Date (queryable by time in Mongo), not an ISO
    // string.
    const committed = await database.collection(AUDIT_LOG_COLLECTION).findOne({ session_id: MG });
    expect(committed?.entry.timestamp).toBeInstanceOf(Date);
    expect(typeof committed?.entry.timestamp).not.toBe('string');
    // And it round-trips through replay back to the original instant.
    const replayed = await sink.replay(MG);
    expect(replayed[0]?.timestamp).toEqual(T0);
  });

  it('allocates one contiguous range under one meta CAS for a batch', async () => {
    const database = freshDatabase();
    const counting = countingDatabase(database);
    const sink = new MongoAuditSink(counting.database);
    await sink.append(auditEntry(1, AuditKind.OPERATOR_INVOKED)); // sequence 1 — the range continues
    const meta = counting.calls('audit_meta');
    const committed = counting.calls(AUDIT_LOG_COLLECTION);
    meta.findOneAndUpdates = 0;
    committed.insertManys = 0;

    const batch = [
      auditEntry(1, AuditKind.DATA_POINT_ADDED),
      auditEntry(1, AuditKind.DATA_POINT_ADDED),
      auditEntry(1, AuditKind.CAPABILITY_ACTIVATED),
    ];
    await sink.appendMany(batch);

    expect(meta.findOneAndUpdates).toBe(1); // ONE CAS allocated the whole range
    expect(committed.insertManys).toBe(1); // and ONE ordered insert committed it
    const rows = await database
      .collection(AUDIT_LOG_COLLECTION)
      .find({ session_id: MG })
      .sort('sequence', 1)
      .toArray();
    expect(rows.map((row) => row.sequence)).toEqual([1, 2, 3, 4]); // contiguous, continuing the single append's
    const replayed = await sink.replay(MG);
    expect(replayed.slice(1).map((entry) => entry.entryId)).toEqual(batch.map((entry) => entry.entryId));
  });

  it('rejects a stale-epoch batch atomically, burning no sequence numbers', async () => {
    const database = freshDatabase();
    const sink = new MongoAuditSink(database);
    await sink.append(auditEntry(2));
    await expect(sink.appendMany([auditEntry(1), auditEntry(1)])).rejects.toThrow(StaleEpochError);
    expect(await sink.replay(MG)).toHaveLength(1); // nothing from the fenced batch was written
    const meta = await database.collection<StringIdDocument>('audit_meta').findOne({ _id: MG });
    expect(meta?.sequence).toBe(1); // and no sequence numbers were burned
  });

  it('writes nothing and burns no sequence for an empty batch', async () => {
    const database = freshDatabase();
    const sink = new MongoAuditSink(database);
    await sink.appendMany([]); // the empty-batch boundary short-circuits before the meta CAS
    expect(await sink.replay(MG)).toEqual([]); // nothing written
    expect(await database.collection<StringIdDocument>('audit_meta').findOne({ _id: MG })).toBeNull(); // no range allocated
  });

  it('surfaces a reused sequence number instead of absorbing it', async () => {
    // The CAS hands out each sequence exactly once, so a document already sitting on a (session,
    // sequence) _id means two writers claimed the same number — a real fault. Absorbing the
    // duplicate key would report a durable append that never landed, and hide the collision behind
    // it.
    const database = freshDatabase();
    await database
      .collection<StringIdDocument>(AUDIT_LOG_COLLECTION)
      .insertOne({ _id: `${MG}${KEY_SEPARATOR}1`, session_id: MG, sequence: 1 });

    await expect(new MongoAuditSink(database).append(auditEntry())).rejects.toThrow(MongoBulkWriteError);
  });

  it('pages a replay of the trail rather than draining it whole', async () => {
    // A trail is unbounded by construction — it grows with everything the session did, and the
    // chatty shapes (a per-frame probe, a page-view stream) are exactly the ones worth replaying.
    // One cursor over all of it materializes the whole trail; the sibling archive beside it pages
    // for the same reason. The walk is on `sequence`, which the session_trail index serves after
    // the equality.
    const counting = countingDatabase(freshDatabase());
    const sink = new MongoAuditSink(counting.database);
    const trail = Array.from({ length: 2 * PAGE_SIZE + 1 }, () => auditEntry());
    await sink.appendMany(trail);
    const committed = counting.calls(AUDIT_LOG_COLLECTION);
    committed.finds = 0;

    const entries = await sink.replay(MG);

    expect(committed.finds).toBe(3); // two full pages and a short one
    expect(entries).toHaveLength(trail.length); // nothing dropped or repeated at a page boundary
  });

  // === the archive =====================================================================

  it('keeps the batches a flush committed before it was interrupted', async () => {
    // The flush shares the bounded tail's timeout with everything else there, so it has to be
    // resumable: clearing the buffer only after every row is committed would leave a timed-out pass
    // with rows committed and none cleared, and the next attempt would re-read the identical buffer
    // and die in the same place — each pass making the next one slower. Clearing per batch is what
    // turns that into forward progress.
    const database = freshDatabase();
    const seeded = new MongoDataPointArchive(database, { cipher: new ReversingCipher() });
    const addresses = Array.from({ length: PAGE_SIZE + 2 }, (_value, index) => `a${index}@e.example`);
    await seeded.archiveMany(addresses.map((address) => archived(workEmail(address))));
    expect(await seeded.bufferedCount(MG)).toBe(addresses.length);

    // The first page commits, then the second page's commit raises.
    const interrupted = new MongoDataPointArchive(commitFailingAfterDatabase(database, 1), {
      cipher: new ReversingCipher(),
    });
    await expect(interrupted.flush(MG)).rejects.toThrow(/commit exploded/);

    // bufferedCount is the probe for progress, not read(): a read is buffer-transparent, so it
    // reports every row either way. Rows having LEFT the buffer is what says the interrupted pass
    // committed them and cleared only those — had it committed nothing, this would still be all of
    // them.
    expect(await seeded.bufferedCount(MG)).toBe(2); // the committed page was cleared, not re-presented
    expect(await seeded.read(MG)).toHaveLength(addresses.length); // and nothing was lost

    expect(await seeded.flush(MG)).toBe(2); // the resume does only the work that remains
    expect(await seeded.bufferedCount(MG)).toBe(0);
    expect(await seeded.read(MG)).toHaveLength(addresses.length);
  });

  it('commits a page in one round trip', async () => {
    // A round trip against a remote primary dominates the cost of a flush, so the page — not the
    // row — has to be the unit. Counted rather than asserted on the call shape, because what
    // matters is how many times the driver goes to the server.
    const counting = countingDatabase(freshDatabase());
    const archive = new MongoDataPointArchive(counting.database, { cipher: new ReversingCipher() });
    const addresses = Array.from({ length: PAGE_SIZE + 1 }, (_value, index) => `a${index}@e.example`);
    await archive.archiveMany(addresses.map((address) => archived(workEmail(address))));

    expect(await archive.flush(MG)).toBe(addresses.length);

    const committed = counting.calls(ArchivedDataPoint.tableName);
    expect(committed.bulkWrites).toBe(2); // one page over the size plus the remainder: two commits
    expect(committed.updateOnes).toBe(0); // never one round trip per row
  });

  it('keeps a committed archive across a restart', async () => {
    const database = freshDatabase();
    const archive = new MongoDataPointArchive(database, { cipher: new ReversingCipher() });
    await archive.archive(archived(workEmail('a@e.example')));
    expect(await archive.bufferedCount(MG)).toBe(1);
    expect(await archive.read(MG)).toHaveLength(1); // buffer-transparent: readable before any flush
    expect(await archive.flush(MG)).toBe(1);

    // A fresh adapter on the same database (a "restart") still sees the committed, deduped archive.
    const restarted = new MongoDataPointArchive(database, { cipher: new ReversingCipher() });
    const committed = await restarted.read(MG);
    expect(committed).toHaveLength(1);
    expect(committed[0]?.value).toBe('a@e.example'); // PII decrypts on read
  });

  it('fails closed on PII with no cipher wired', async () => {
    const archive = new MongoDataPointArchive(freshDatabase()); // no cipher → NullCipher
    await expect(archive.archive(archived(workEmail('a@e.example')))).rejects.toThrow(UnprotectedPiiError);
  });

  it('seals PII under the namespace cipher', async () => {
    const database = freshDatabase();
    const archive = new MongoDataPointArchive(database, {
      cipherProvider: new PerNamespaceProvider({ [NAMESPACE]: new ReversingCipher() }),
    });
    await archive.archive(archived(workEmail('a@e.example')));
    await archive.flush(MG);

    const committed = await archive.read(MG);
    expect(committed[0]?.value).toBe('a@e.example'); // sealed under the namespace's key, decrypts on read
    const raw = await database.collection(ArchivedDataPoint.tableName).findOne({ session_id: MG });
    expect(String(raw?.value)).not.toContain('a@e.example'); // ciphertext at rest, never the plaintext
  });

  it('fails closed when the namespace has no key', async () => {
    // A provider failure degrades to NullCipher → PII refused.
    const archive = new MongoDataPointArchive(freshDatabase(), { cipherProvider: new PerNamespaceProvider({}) });
    await expect(archive.archive(archived(workEmail('a@e.example')))).rejects.toThrow(UnprotectedPiiError);
  });

  it('says so clearly when the namespace key is gone from under sealed PII', async () => {
    // Seal PII under the namespace's real key, then read back through a provider that can no longer
    // resolve the key: rather than crashing opaquely inside unseal, read() surfaces
    // PiiKeyUnavailableError.
    const database = freshDatabase();
    const writer = new MongoDataPointArchive(database, {
      cipherProvider: new PerNamespaceProvider({ [NAMESPACE]: new ReversingCipher() }),
    });
    await writer.archive(archived(workEmail('a@e.example')));
    await writer.flush(MG);

    const reader = new MongoDataPointArchive(database, { cipherProvider: new PerNamespaceProvider({}) });
    await expect(reader.read(MG)).rejects.toThrow(PiiKeyUnavailableError);
  });

  it('allocates one range for a batch and folds it exactly like singles', async () => {
    const database = freshDatabase();
    const counting = countingDatabase(database);
    const archive = new MongoDataPointArchive(counting.database, { cipher: new ReversingCipher() });
    const meta = counting.calls('datapoint_archive_meta');
    const buffer = counting.calls('datapoint_archive_buffer');

    await archive.archiveMany([
      archived(workEmail('a@e.example', { first: T0, last: T0 })),
      archived(workEmail('b@e.example')),
      archived(workEmail('a@e.example', { first: T0, last: T2 })), // re-observation in-batch
    ]);

    expect(meta.findOneAndUpdates).toBe(1); // ONE CAS allocated the whole range
    expect(buffer.insertManys).toBe(1);
    const buffered = await database
      .collection('datapoint_archive_buffer')
      .find({ session_id: MG })
      .sort('sequence', 1)
      .toArray();
    expect(buffered.map((row) => row.sequence)).toEqual([1, 2, 3]); // per-observation, contiguous
    expect(await archive.flush(MG)).toBe(3);
    const committed = new Map((await archive.read(MG)).map((entry) => [entry.value, entry]));
    expect(new Set(committed.keys())).toEqual(new Set(['a@e.example', 'b@e.example'])); // fold dedups
    expect(committed.get('a@e.example')?.lastRetrieved).toEqual(T2); // and still bumps lastRetrieved
  });

  it('burns no sequence when a batch fails the PII gate', async () => {
    const database = freshDatabase();
    const archive = new MongoDataPointArchive(database); // no cipher → NullCipher → PII fails closed

    // The PII check runs per entry BEFORE the meta allocation.
    await expect(archive.archiveMany([archived(risk(0.5)), archived(workEmail('a@e.example'))])).rejects.toThrow(
      UnprotectedPiiError,
    );
    expect(await archive.bufferedCount(MG)).toBe(0); // the whole batch was refused — no partial buffer

    await archive.archive(archived(risk(0.5))); // a later valid write starts at sequence 1
    const buffered = await database.collection('datapoint_archive_buffer').find({ session_id: MG }).toArray();
    expect(buffered.map((row) => row.sequence)).toEqual([1]);
  });

  it('does not duplicate when a crash before the buffer delete is re-flushed', async () => {
    // Same crash-window invariant for the archive: the committed (session, type, valueHash) _id
    // plus $setOnInsert dedups a redundant re-flush, and $max keeps lastRetrieved from regressing.
    const database = freshDatabase();
    const suppressed = deleteSuppressingBuffer(database);
    const archive = new MongoDataPointArchive(suppressed.database, { cipher: new ReversingCipher() });

    await archive.archive(archived(workEmail('a@e.example', { first: T0, last: T0 })));
    await archive.archive(archived(workEmail('b@e.example', { first: T0, last: T2 })));
    expect(await archive.bufferedCount(MG)).toBe(2);

    expect(await archive.flush(MG)).toBe(2); // commits, but the simulated crash drops the delete
    expect(await archive.bufferedCount(MG)).toBe(2); // the buffer survived — nothing is dropped
    const committed = new Map((await archive.read(MG)).map((entry) => [entry.value, entry]));
    expect(new Set(committed.keys())).toEqual(new Set(['a@e.example', 'b@e.example'])); // folded once

    suppressed.restore();
    expect(await archive.flush(MG)).toBe(2); // the resume re-flushes the still-buffered observations
    const reread = new Map((await archive.read(MG)).map((entry) => [entry.value, entry]));
    expect(new Set(reread.keys())).toEqual(new Set(['a@e.example', 'b@e.example'])); // deduped on read
    // Count the raw committed docs, not the by-value read fold: the deterministic _id must keep the
    // re-flush from inserting fresh documents, so the collection holds exactly two, never four.
    const ids = (await database.collection(ArchivedDataPoint.tableName).find({}).toArray()).map((row) =>
      String(row._id),
    );
    expect(ids).toHaveLength(2);
    expect(new Set(ids).size).toBe(2);
    expect(reread.get('b@e.example')?.lastRetrieved).toEqual(T2); // $max kept last from regressing
    expect(await archive.bufferedCount(MG)).toBe(0); // and the buffer is finally drained
  });

  it('ages the raw archive out on last_retrieved and round-trips the instant', async () => {
    // The retention branch only runs when a window is configured, and a real BSON date round-trip
    // has to come back as the instant it was written with. A recent lastRetrieved is used so the
    // live TTL reaper does not age the doc out before the assertion — the one place this suite
    // reads the host clock, for exactly that reason.
    const now = new Date();
    const database = freshDatabase();
    const archive = new MongoDataPointArchive(database, {
      cipher: new ReversingCipher(),
      retentionMs: 3_600_000,
    });
    await archive.archive(archived(workEmail('a@e.example', { first: now, last: now })));
    expect(await archive.flush(MG)).toBe(1);

    const committed = await archive.read(MG);
    expect(committed).toHaveLength(1);
    expect(committed[0]?.value).toBe('a@e.example'); // PII still decrypts through the cipher seam
    expect(committed[0]?.lastRetrieved).toEqual(now); // the BSON date came back as the same instant

    expect(await ttlIndexOn(database.collection(ArchivedDataPoint.tableName), 'last_retrieved')).toBe(3600);
  });

  it('keeps the same value under two sessions isolated in one collection', async () => {
    // The PII valueHash mixes in sessionId AND the committed _id is session-prefixed, so the same
    // email archived under two sessions into the single tableName collection lands as two distinct
    // docs. A regression dropping session from the _id would collapse them.
    const other = SessionId('mg-other-session');
    const database = freshDatabase();
    const archive = new MongoDataPointArchive(database, { cipher: new ReversingCipher() });

    await archive.archive(archived(workEmail('a@e.example'), { session: MG }));
    await archive.flush(MG);
    await archive.archive(archived(workEmail('a@e.example'), { session: other }));
    await archive.flush(other);

    expect((await archive.read(MG)).map((entry) => entry.value)).toEqual(['a@e.example']); // each its own
    expect((await archive.read(other)).map((entry) => entry.value)).toEqual(['a@e.example']);
    const ids = (await database.collection(ArchivedDataPoint.tableName).find({}).toArray()).map((row) =>
      String(row._id),
    );
    expect(new Set(ids).size).toBe(2); // two distinct keys in one collection
  });

  it('pages both sides of a read rather than draining them whole', async () => {
    // A session's committed rows are its distinct observed VALUES — valueHash is a MAC of the
    // value, so a chatty session runs to tens of thousands — and the buffer carries one row per
    // observation on top of that. Draining either side in a single cursor is the memory growth the
    // page size exists to prevent during a flush, so the read has to page the same way.
    const counting = countingDatabase(freshDatabase());
    const archive = new MongoDataPointArchive(counting.database, { cipher: new ReversingCipher() });
    const entry = (value: string, last: Date = T0): ArchivedDataPoint =>
      archived(workEmail(value, { first: T0, last }));

    const committedValues = Array.from({ length: 2 * PAGE_SIZE + 1 }, (_value, index) => `c${index}@e.example`);
    await archive.archiveMany(committedValues.map((value) => entry(value)));
    await archive.flush(MG); // two full pages and a short one, committed
    const bufferedValues = Array.from({ length: PAGE_SIZE }, (_value, index) => `b${index}@e.example`);
    // One page and a remainder, the remainder being a re-observation of an already-committed row.
    await archive.archiveMany([
      ...bufferedValues.map((value) => entry(value)),
      entry(committedValues[0] as string, T2),
    ]);

    const committed = counting.calls(ArchivedDataPoint.tableName);
    const buffer = counting.calls('datapoint_archive_buffer');
    committed.finds = 0;
    buffer.finds = 0;

    const entries = await archive.read(MG);

    expect(committed.finds).toBe(3); // 500 + 500 + 1
    expect(buffer.finds).toBe(2); // 500 + 1
    const byValue = new Map(entries.map((stored) => [stored.value, stored]));
    expect(byValue.size).toBe(committedValues.length + bufferedValues.length); // nothing dropped
    expect(byValue.get(committedValues[0] as string)?.lastRetrieved).toEqual(T2); // and the fold merged
  });
});

/** Every index key pattern on the collection, in the shape `ensureIndex` asks for. */
const indexKeys = async (collection: Collection): Promise<readonly Record<string, unknown>[]> =>
  (await collection.indexInformation({ full: true })).map((spec) => ({ ...spec.key }));

/** Every index name on the collection, sorted so the assertion does not depend on listing order. */
const indexNames = async (collection: Collection): Promise<readonly string[]> =>
  (await collection.indexInformation({ full: true })).map((spec) => String(spec.name)).sort();

/** The TTL window on the index over `field`, or `undefined` when there is no such index. */
const ttlIndexOn = async (collection: Collection, field: string): Promise<number | undefined> => {
  const indexes = await collection.indexInformation({ full: true });
  const ttl = indexes.find((spec) => {
    const keys = Object.entries(spec.key);
    return keys.length === 1 && keys[0]?.[0] === field && keys[0]?.[1] === 1;
  });
  expect(ttl).toBeDefined();
  return ttl?.expireAfterSeconds;
};

/**
 * Leave one committed row and one buffered row behind, on collections carrying no index.
 *
 * Rows present and indexes absent is what a reader finds when it is the first process onto a
 * restored collection, and it is the only state in which a refusal is reachable at all: with the
 * indexes already in place `ensureIndex` stops at the listing and never calls `createIndex`.
 */
const seedAnUnindexedArchive = async (database: Db): Promise<void> => {
  const seeded = new MongoDataPointArchive(database, { cipher: new ReversingCipher() });
  await seeded.archive(archived(workEmail('a@e.example', { first: T0, last: T0 })));
  await seeded.flush(MG);
  await seeded.archive(archived(workEmail('b@e.example', { first: T0, last: T0 })));
  for (const name of ARCHIVE_COLLECTIONS) {
    await database.collection(name).dropIndexes();
  }
};

/** Guard against the refusal never having been reached, which would pass for the wrong reason. */
const expectNothingIndexed = async (database: Db): Promise<void> => {
  for (const name of ARCHIVE_COLLECTIONS) {
    expect(await indexNames(database.collection(name))).toEqual(['_id_']);
  }
};
