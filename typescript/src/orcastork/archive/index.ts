/**
 * DataPoint Archive — the second durable write path (live, keyed-upsert raw DataPoints).
 *
 * Complements the curated aggregate outputs (`DurableStore`) and the event log (`AuditSink`):
 * every non-ephemeral DataPoint is persisted as its own document via the same write-behind buffer
 * + batched flush, for debugging, reprocessing, and analytics.
 *
 * @module
 */

export { type NamespaceCipherProvider, NullCipher, NullCipherProvider, type ValueCipher } from './cipher.js';
export {
  ArchivedDataPoint,
  type ArchivedDataPointInit,
  type ArchivedDataPointWire,
  type ArchiveProvenance,
  parseArchivedDataPoint,
} from './models.js';
export { seal, unseal, valueHash } from './sealing.js';
