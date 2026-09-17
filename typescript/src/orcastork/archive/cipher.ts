/**
 * The `ValueCipher` seam — encryption + keyed hashing for archived PII values.
 *
 * The framework ships **no cryptography** (a deliberate stance, mirrored by the Mongo adapters):
 * it exposes this tiny port plus a passthrough {@link NullCipher} default, and the consuming flow
 * injects a real cipher into the archive adapters. `encrypt`/`decrypt` seal a PII value at rest;
 * `mac` derives the archive *key* for a PII value — a **keyed** digest (sharing key custody with
 * the cipher), so the stored key is neither an offline-confirmation oracle nor a cross-session
 * correlator for a holder of the archive without the key. Keeping this injectable is what lets the
 * package ship no key management of its own.
 *
 * @module
 */

import { createHash } from 'node:crypto';
import type { NamespaceId } from '../ids.js';

/** Reversible transform + keyed MAC applied to a PII value at the storage boundary. */
export interface ValueCipher {
  encrypt(plaintext: string): string;

  decrypt(ciphertext: string): string;

  /** A deterministic, **keyed** digest of `plaintext` (the durable archive key for PII). */
  mac(plaintext: string): string;
}

/**
 * Identity cipher — the standalone default; the flow swaps in real encryption.
 *
 * `mac` falls back to an **unkeyed** SHA-256: fine for non-PII keys and the in-memory test
 * substrate, but the production (Mongo) adapter refuses to persist PII under it (fails closed), so
 * PII keys are never derived from an unkeyed digest in production.
 */
export class NullCipher implements ValueCipher {
  public encrypt(plaintext: string): string {
    return plaintext;
  }

  public decrypt(ciphertext: string): string {
    return ciphertext;
  }

  public mac(plaintext: string): string {
    return createHash('sha256').update(plaintext, 'utf8').digest('hex');
  }
}

/**
 * Resolves a per-namespace {@link ValueCipher} so PII is sealed/audited under a per-namespace key.
 *
 * `forNamespace` is async (the flow's implementation loads the namespace's key) and returns a
 * synchronous `ValueCipher`, so the seal/audit sites resolve once at an async point and then
 * encrypt synchronously. The flow injects a real provider; the package default is the passthrough
 * below.
 */
export interface NamespaceCipherProvider {
  forNamespace(namespaceId: NamespaceId): Promise<ValueCipher>;
}

/** Default provider — hands back the passthrough {@link NullCipher} for any namespace (no encryption). */
export class NullCipherProvider implements NamespaceCipherProvider {
  public async forNamespace(_namespaceId: NamespaceId): Promise<ValueCipher> {
    return new NullCipher();
  }
}
