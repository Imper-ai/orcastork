/**
 * Mongo adapters (optional peer `mongodb`) — OCC durable store, buffered audit sink, archive.
 *
 * Each adapter takes a driver `Db`; the framework owns this code directly and depends on nothing
 * but the ports it implements. Collection names, document shapes and key layouts are byte-identical
 * to the Python package's, so a TypeScript worker and a Python worker can share one deployment and
 * a session written by one can be read by the other.
 *
 * Only types are imported from the peer, so these modules load — and type-check — wherever
 * `mongodb` is not installed; the driver objects arrive from the caller.
 *
 * @module
 */

export { AUDIT_LOG_COLLECTION, MongoAuditSink } from './audit_sink.js';
export type { MongoDataPointArchiveOptions } from './datapoint_archive.js';
export { MongoDataPointArchive } from './datapoint_archive.js';
export type { MongoDurableStoreOptions } from './durable_store.js';
export { MongoDurableStore } from './durable_store.js';
