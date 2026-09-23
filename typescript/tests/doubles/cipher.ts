/**
 * A reversible test cipher — proves the archive routes PII through the seam.
 *
 * Reversing a string is obviously not real encryption; it is just enough to assert that the
 * adapter stores a *transformed* value at rest for PII (`encrypt` was applied) and recovers the
 * original on read (`decrypt` was applied), while non-PII values pass through untouched.
 *
 * @module
 */

import type { ValueCipher } from '../../src/orcastork/archive/cipher.js';

/** Reversed by code point, not by UTF-16 unit, so a round trip is exact for any string. */
const reversed = (value: string): string => [...value].reverse().join('');

export class ReversingCipher implements ValueCipher {
  public encrypt(plaintext: string): string {
    return reversed(plaintext);
  }

  public decrypt(ciphertext: string): string {
    return reversed(ciphertext);
  }

  public mac(plaintext: string): string {
    // A stand-in "keyed" MAC: deterministic, and distinguishable from a bare digest so a test can
    // assert the archive routed a PII key through the seam rather than hashing in the clear.
    return `mac-${reversed(plaintext)}`;
  }
}
