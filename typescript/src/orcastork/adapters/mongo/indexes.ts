/**
 * Lazy, idempotent index creation shared by the Mongo adapters.
 *
 * Indexes are ensured from the write path the first time an adapter touches a collection, rather
 * than from a migration step: these collections are created on first write by whichever process
 * gets there first, so there is no earlier moment that reliably runs.
 *
 * That makes tolerance the whole design. An index is an optimization — its absence is a slow query,
 * not a broken one — so a process that cannot create one must still complete the write it was
 * actually doing. Equally, an index that already exists under a different specification is an
 * operations decision (it needs a deliberate drop and recreate, which can be expensive on a large
 * collection) and not something a request path should quietly change.
 *
 * `expireAfterSeconds` is the one exception, because both halves of that reasoning fail for it. A
 * TTL is not an optimization: it is the retention commitment, so an index left at a longer window
 * keeps sealed PII the caller promised to delete, and one that is not a TTL index at all expires
 * nothing. Nor does honouring the caller's value need a drop and recreate — `collMod` rewrites the
 * expiry as metadata, without touching the index itself.
 *
 * The driver-error predicates live here too: this is the module the whole Mongo family already
 * shares, and every one of them is an expression of the same tolerance.
 *
 * @module
 */

import type { Collection, Db, Document, IndexDescriptionInfo } from 'mongodb';
import { getLogger } from '../../logging.js';

const LOGGER_NAME = 'orcastork.adapters.mongo.indexes';

/** The duplicate-key refusal (`E11000`): a lost insert race, or an upsert whose predicate failed. */
const DUPLICATE_KEY = 11000;

/**
 * `ns does not exist` — what `listIndexes` reports for a collection nothing has written yet.
 *
 * pymongo swallows this code and reports an empty index listing, so Python's `ensure_index` walks
 * an empty list and goes on to create the index. The Node driver raises it instead, and an ensure
 * that treated the raise as "could not list" would leave every collection unindexed forever: the
 * first write is the moment the collection appears, so it is also the only moment the listing can
 * find nothing.
 */
const NAMESPACE_NOT_FOUND = 26;

/**
 * A document whose `_id` these adapters mint themselves.
 *
 * The driver infers an `ObjectId` `_id` for an unparametrized collection, while every key this
 * family writes is a string it composed (`session\u0000sequence`, `key\u0000field`, the archive's
 * `session\u0000type\u0000value_hash`) — the same strings the Python adapters write, which is what
 * lets a row be read by either worker.
 */
export interface StringIdDocument extends Document {
  _id: string;
}

/** One index key pattern, in the order the fields are compared. */
export type IndexKeys = Readonly<Record<string, 1 | -1>>;

/** What {@link ensureIndex} creates the index as, when it has to create one. */
export interface EnsureIndexOptions {
  /** Half of the identity Mongo enforces (the key pattern is the other half). */
  readonly name: string;

  /**
   * A TTL window **in seconds** — Mongo's own unit, not the package's milliseconds.
   *
   * The value goes onto the wire verbatim (`expireAfterSeconds`), so it keeps the server's unit;
   * an adapter that takes a window in milliseconds converts at its own boundary.
   */
  readonly expireAfterSeconds?: number;
}

/**
 * Whether `error` came from the driver rather than from this package.
 *
 * Structural rather than `instanceof MongoError`, so this module needs no value import of the
 * optional peer — the same reason the Redis adapters are typed against the commands they use.
 * Every driver error's `name` is its own class name, and every one of those begins with `Mongo`.
 */
export const isMongoDriverError = (error: unknown): error is Error =>
  error instanceof Error && error.name.startsWith('Mongo');

/** The server's error code, when the error carries one. */
const codeOf = (error: unknown): number | undefined => {
  const code = (error as { readonly code?: unknown }).code;
  return typeof code === 'number' ? code : undefined;
};

/**
 * Whether `error` is the server's duplicate-key refusal.
 *
 * Both shapes count: a single write's `MongoServerError` and the `MongoBulkWriteError` a batch
 * reports, which carries the same code.
 */
export const isDuplicateKeyError = (error: unknown): boolean =>
  isMongoDriverError(error) && codeOf(error) === DUPLICATE_KEY;

/** The collection's indexes, or none at all when the collection does not exist yet. */
const existingIndexes = async <SchemaT extends Document>(
  collection: Collection<SchemaT>,
): Promise<readonly IndexDescriptionInfo[]> => {
  try {
    return await collection.indexInformation({ full: true });
  } catch (error) {
    if (isMongoDriverError(error) && codeOf(error) === NAMESPACE_NOT_FOUND) {
      return [];
    }
    throw error;
  }
};

/** Whether an existing index's key pattern is the one being asked for, field order included. */
const sameKeys = (stored: IndexDescriptionInfo['key'], wanted: IndexKeys): boolean => {
  const storedEntries = Object.entries(stored);
  const wantedEntries = Object.entries(wanted);
  if (storedEntries.length !== wantedEntries.length) {
    return false;
  }
  return wantedEntries.every(([field, direction], index) => {
    const entry = storedEntries[index];
    return entry !== undefined && entry[0] === field && entry[1] === direction;
  });
};

/**
 * Create an index on `keys` unless the collection already has an equivalent one.
 *
 * Gated on both identity Mongo itself enforces: the key pattern and the name. `createIndex` is
 * idempotent for a byte-identical specification, but it REFUSES a same-name index whose spec
 * differs and a same-keys index under a different name — so calling it unconditionally turns a
 * previously-created index into an exception on a write path. Checking first means the common case
 * (someone already made it) is a no-op rather than a caught error.
 *
 * A match still reconciles `expireAfterSeconds` (see the module docstring): everything else about
 * an existing index is left as the operator made it, but its retention window is brought to the one
 * the caller asked for.
 *
 * A refusal that survives the check is logged and swallowed, since the caller's write is what
 * matters and the index can be added by hand.
 *
 * The `database` rides along because `collMod` is a database command and the driver's `Collection`
 * carries no back-reference to its `Db` the way pymongo's does.
 */
export const ensureIndex = async <SchemaT extends Document>(
  database: Db,
  collection: Collection<SchemaT>,
  keys: IndexKeys,
  options: EnsureIndexOptions,
): Promise<void> => {
  const { expireAfterSeconds } = options;
  try {
    for (const spec of await existingIndexes(collection)) {
      const keysMatch = sameKeys(spec.key, keys);
      if (spec.name === options.name || keysMatch) {
        // Only onto the key that was asked for: a same-named index over some other field would
        // start expiring documents on a clock nobody chose.
        if (expireAfterSeconds !== undefined && keysMatch) {
          await reconcileRetention(database, collection, spec, expireAfterSeconds);
        }
        return;
      }
    }
    await collection.createIndex(keys, options);
  } catch (error) {
    if (!isMongoDriverError(error)) {
      throw error;
    }
    // The listing is a round-trip of its own, so it is inside the guard: an unreachable primary
    // must degrade to an unindexed query, not fail the write that triggered the check. Racing
    // writers both passing the check is the expected case, and the loser's already-exists failure
    // is not worth surfacing as more than a note.
    getLogger().warning('Could not ensure index; queries will fall back to a collection scan', {
      logger_name: LOGGER_NAME,
      keys,
      error,
    });
  }
};

/**
 * Bring an existing index's expiry to `retentionSeconds`, adding one if it has none.
 *
 * Reached by both of the ways this drifts: a redeploy that retunes the window, and a plain index an
 * operator built on the same key, which matches on the key pattern and would otherwise make
 * enabling retention a no-op that expires nothing. Either way the stored value would win and the
 * configured one never apply, so a shortened window would be a retention promise quietly unkept.
 *
 * Logged loudly because it is the moment a retention window actually changes, and a change in this
 * direction starts deleting sealed PII the collection has been holding.
 */
const reconcileRetention = async <SchemaT extends Document>(
  database: Db,
  collection: Collection<SchemaT>,
  spec: IndexDescriptionInfo,
  retentionSeconds: number,
): Promise<void> => {
  const storedSeconds = spec.expireAfterSeconds;
  if (storedSeconds === retentionSeconds) {
    return;
  }
  try {
    await database.command({
      collMod: collection.collectionName,
      index: { name: spec.name, expireAfterSeconds: retentionSeconds },
    });
  } catch (error) {
    if (!isMongoDriverError(error)) {
      throw error;
    }
    // Same tolerance the rest of this module extends: a backend that refuses (or has never
    // implemented) collMod must not fail the write that triggered the check. The window then
    // stays where it was, which is exactly what the warning is for.
    getLogger().warning('Could not apply the configured archive retention window; it stays as stored', {
      logger_name: LOGGER_NAME,
      index: spec.name,
      stored_seconds: storedSeconds,
      requested_seconds: retentionSeconds,
      error,
    });
    return;
  }
  getLogger().warning('Archive retention window changed to the configured one', {
    logger_name: LOGGER_NAME,
    index: spec.name,
    stored_seconds: storedSeconds,
    requested_seconds: retentionSeconds,
  });
};
