"""The ``ValueCipher`` seam — encryption + keyed hashing for archived PII values.

The framework ships **no cryptography** (a deliberate stance, mirrored by the Mongo
adapters): it exposes this tiny port plus a passthrough :class:`NullCipher` default, and
the consuming flow injects a real cipher into the archive adapters.
``encrypt``/``decrypt`` seal a PII value at rest; ``mac`` derives the
archive *key* for a PII value — a **keyed** digest (sharing key custody with the cipher),
so the stored key is neither an offline-confirmation oracle nor a cross-session correlator
for a holder of the archive without the key. Keeping this injectable is what lets the
package ship no key management of its own.
"""

from __future__ import annotations

import hashlib
from typing import Protocol, runtime_checkable


@runtime_checkable
class ValueCipher(Protocol):
    """Reversible transform + keyed MAC applied to a PII value at the storage boundary."""

    def encrypt(self, plaintext: str) -> str: ...

    def decrypt(self, ciphertext: str) -> str: ...

    def mac(self, plaintext: str) -> str:
        """A deterministic, **keyed** digest of ``plaintext`` (the durable archive key for PII)."""
        ...


class NullCipher:
    """Identity cipher — the standalone default; the flow swaps in real encryption.

    ``mac`` falls back to an **unkeyed** SHA-256: fine for non-PII keys and the in-memory test
    substrate, but the production (Mongo) adapter refuses to persist PII under it (fails closed),
    so PII keys are never derived from an unkeyed digest in production.
    """

    def encrypt(self, plaintext: str) -> str:
        return plaintext

    def decrypt(self, ciphertext: str) -> str:
        return ciphertext

    def mac(self, plaintext: str) -> str:
        return hashlib.sha256(plaintext.encode()).hexdigest()


@runtime_checkable
class NamespaceCipherProvider(Protocol):
    """Resolves a per-namespace ``ValueCipher`` so PII is sealed/audited under a per-namespace key.

    ``for_namespace`` is async (the flow's implementation loads the namespace's key) and returns a synchronous
    ``ValueCipher``, so the seal/audit sites resolve once at an async point and then encrypt
    synchronously. The flow injects a real provider; the package default is the passthrough below.
    """

    async def for_namespace(self, namespace_id: str) -> ValueCipher: ...


class NullCipherProvider:
    """Default provider — hands back the passthrough ``NullCipher`` for any namespace (no encryption)."""

    async def for_namespace(self, namespace_id: str) -> ValueCipher:  # noqa: ARG002
        return NullCipher()
