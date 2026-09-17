/**
 * PII sealing + keyed hashing for archived DataPoints — one definition, shared by every adapter.
 *
 * The archive key and at-rest encryption scheme must be identical across backends (in-memory,
 * Mongo) or the same DataPoint would dedup or decrypt differently depending on where it landed.
 * These three functions are that single definition; an adapter supplies its {@link ValueCipher}
 * and (for the production store) layers its own fail-closed policy on top — a PII entry offered
 * to the Mongo archive under the passthrough `NullCipher` is refused with `UnprotectedPiiError`
 * rather than persisted, and sealed PII read back without a key raises `PiiKeyUnavailableError`.
 *
 * @module
 */

import { createHash } from 'node:crypto';
import { canonicalValue } from '../datapoints/index.js';
import type { ValueCipher } from './cipher.js';
import type { ArchivedDataPoint } from './models.js';

/** The separator the PII key mixes the session id in with — a byte no session id can contain. */
const KEY_SEPARATOR = '\u0000';

/**
 * The archive key for a value: a keyed MAC mixing in `sessionId` for PII (oracle-resistant +
 * cross-session-unlinkable), a plain SHA-256 otherwise. Always computed from the plaintext value.
 */
export const valueHash = (entry: ArchivedDataPoint, cipher: ValueCipher): string => {
  const canonical = canonicalValue(entry.value);
  if (entry.isPii) {
    return cipher.mac(`${entry.sessionId}${KEY_SEPARATOR}${canonical}`);
  }
  return createHash('sha256').update(canonical, 'utf8').digest('hex');
};

/** Encrypt a PII value at rest (non-PII passes through). Derive the key *before* sealing. */
export const seal = (entry: ArchivedDataPoint, cipher: ValueCipher): ArchivedDataPoint => {
  if (!entry.isPii) {
    return entry;
  }
  // `ValueCipher.encrypt` takes a string, so the value is JSON-encoded at this seam — which is
  // what lets a structured (object, array) PII value survive the round trip unchanged.
  return entry.copyWith({ value: cipher.encrypt(JSON.stringify(entry.value)) });
};

/** Recover a sealed PII value on read (the inverse of {@link seal}). */
export const unseal = (entry: ArchivedDataPoint, cipher: ValueCipher): ArchivedDataPoint => {
  if (!entry.isPii) {
    return entry;
  }
  // JSON reads are serializer-agnostic: `JSON.parse` accepts whatever valid JSON the writer
  // produced (compact, or the spaced separators Python's stdlib json emits alike), so a value
  // sealed by a build that serialized differently still unseals here.
  return entry.copyWith({ value: JSON.parse(cipher.decrypt(entry.value as string)) as unknown });
};
