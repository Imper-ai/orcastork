"""PII sealing + keyed hashing for archived DataPoints — one definition, shared by every adapter.

The archive key and at-rest encryption scheme must be identical across backends (in-memory, Mongo)
or the same DataPoint would dedup or decrypt differently depending on where it landed. These three
functions are that single definition; an adapter supplies its :class:`ValueCipher` and (for the
production store) layers its own fail-closed policy on top.
"""

from __future__ import annotations

import hashlib

import orjson

from ..datapoints import canonical_value
from .cipher import ValueCipher
from .models import ArchivedDataPoint


def value_hash(entry: ArchivedDataPoint, cipher: ValueCipher) -> str:
    """The archive key for a value: a keyed MAC mixing in ``session_id`` for PII (oracle-resistant +
    cross-session-unlinkable), a plain SHA-256 otherwise. Always computed from the plaintext value."""
    canonical = canonical_value(entry.value)
    if entry.is_pii:
        return cipher.mac(f'{entry.session_id}\x00{canonical}')
    return hashlib.sha256(canonical.encode()).hexdigest()


def seal(entry: ArchivedDataPoint, cipher: ValueCipher) -> ArchivedDataPoint:
    """Encrypt a PII value at rest (non-PII passes through). Derive the key *before* sealing."""
    if not entry.is_pii:
        return entry
    # ``ValueCipher.encrypt`` takes str, so the orjson bytes are decoded at this seam.
    return entry.model_copy(update={'value': cipher.encrypt(orjson.dumps(entry.value).decode())})


def unseal(entry: ArchivedDataPoint, cipher: ValueCipher) -> ArchivedDataPoint:
    """Recover a sealed PII value on read (the inverse of :func:`seal`)."""
    if not entry.is_pii:
        return entry
    # JSON reads are serializer-agnostic: orjson.loads accepts whatever valid JSON the writer
    # produced (compact orjson or stdlib json's spaced separators alike), so values sealed by
    # builds that serialized with stdlib json still unseal here.
    return entry.model_copy(update={'value': orjson.loads(cipher.decrypt(entry.value))})
