"""A reversible test cipher (ARCH-07) — proves the archive routes PII through the seam.

Reversing a string is obviously not real encryption; it is just enough to assert that the
adapter stores a *transformed* value at rest for PII (``encrypt`` was applied) and recovers
the original on read (``decrypt`` was applied), while non-PII values pass through untouched.
"""

from __future__ import annotations


class ReversingCipher:
    def encrypt(self, plaintext: str) -> str:
        return plaintext[::-1]

    def decrypt(self, ciphertext: str) -> str:
        return ciphertext[::-1]

    def mac(self, plaintext: str) -> str:
        # A stand-in "keyed" MAC: deterministic, and distinguishable from a bare digest so a test
        # can assert the archive routed a PII key through the seam rather than hashing in the clear.
        return f'mac-{plaintext[::-1]}'
