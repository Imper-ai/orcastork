"""Lazy, idempotent index creation shared by the Mongo adapters.

Indexes are ensured from the write path the first time an adapter touches a collection, rather than
from a migration step: these collections are created on first write by whichever process gets there
first, so there is no earlier moment that reliably runs.

That makes tolerance the whole design. An index is an optimization — its absence is a slow query, not
a broken one — so a process that cannot create one must still complete the write it was actually
doing. Equally, an index that already exists under a different specification is an operations
decision (it needs a deliberate drop and recreate, which can be expensive on a large collection) and
not something a request path should quietly change.

``expireAfterSeconds`` is the one exception, because both halves of that reasoning fail for it. A TTL
is not an optimization: it is the retention commitment, so an index left at a longer window keeps
sealed PII the caller promised to delete, and one that is not a TTL index at all expires nothing.
Nor does honouring the caller's value need a drop and recreate — ``collMod`` rewrites the expiry as
metadata, without touching the index itself.
"""

from __future__ import annotations

from typing import Any

from loguru import logger
from pymongo.errors import PyMongoError


async def ensure_index(collection: Any, keys: list[tuple[str, int]], **options: Any) -> None:
    """Create an index on ``keys`` unless the collection already has an equivalent one.

    Gated on both identity Mongo itself enforces: the key pattern and the name. ``create_index`` is
    idempotent for a byte-identical specification, but it REFUSES a same-name index whose spec differs
    and a same-keys index under a different name — so calling it unconditionally turns a
    previously-created index into an exception on a write path. Checking first means the common case
    (someone already made it) is a no-op rather than a caught error.

    A match still reconciles ``expireAfterSeconds`` (see the module docstring): everything else about
    an existing index is left as the operator made it, but its retention window is brought to the one
    the caller asked for.

    A refusal that survives the check is logged and swallowed, since the caller's write is what
    matters and the index can be added by hand.
    """
    wanted = list(keys)
    name = options.get('name')
    retention_seconds = options.get('expireAfterSeconds')
    try:
        existing = await collection.index_information()
        for index_name, spec in existing.items():
            keys_match = [tuple(entry) for entry in spec.get('key', [])] == wanted
            if index_name == name or keys_match:
                # Only onto the key that was asked for: a same-named index over some other field
                # would start expiring documents on a clock nobody chose.
                if retention_seconds is not None and keys_match:
                    await _reconcile_retention(collection, index_name, spec, retention_seconds)
                return
        await collection.create_index(keys, **options)
    except PyMongoError as exc:
        # The listing is a round-trip of its own, so it is inside the guard: an unreachable primary
        # must degrade to an unindexed query, not fail the write that triggered the check. Racing
        # writers both passing the check is the expected case, and the loser's already-exists failure
        # is not worth surfacing as more than a note.
        logger.warning(
            'Could not ensure index; queries will fall back to a collection scan',
            keys=wanted,
            error=str(exc),
        )


async def _reconcile_retention(collection: Any, index_name: str, spec: dict[str, Any], retention_seconds: int) -> None:
    """Bring an existing index's expiry to ``retention_seconds``, adding one if it has none.

    Reached by both of the ways this drifts: a redeploy that retunes the window, and a plain index an
    operator built on the same key, which matches on the key pattern and would otherwise make
    enabling retention a no-op that expires nothing. Either way the stored value would win and the
    configured one never apply, so a shortened window would be a retention promise quietly unkept.

    Logged loudly because it is the moment a retention window actually changes, and a change in this
    direction starts deleting sealed PII the collection has been holding.
    """
    stored_seconds = spec.get('expireAfterSeconds')
    if stored_seconds == retention_seconds:
        return
    try:
        await collection.database.command(
            {
                'collMod': collection.name,
                'index': {'name': index_name, 'expireAfterSeconds': retention_seconds},
            }
        )
    except (PyMongoError, NotImplementedError) as exc:
        # Same tolerance the rest of this module extends: a backend that refuses (or has never
        # implemented) collMod must not fail the write that triggered the check. The window then
        # stays where it was, which is exactly what the warning is for.
        logger.warning(
            'Could not apply the configured archive retention window; it stays as stored',
            index=index_name,
            stored_seconds=stored_seconds,
            requested_seconds=retention_seconds,
            error=str(exc),
        )
        return
    logger.warning(
        'Archive retention window changed to the configured one',
        index=index_name,
        stored_seconds=stored_seconds,
        requested_seconds=retention_seconds,
    )
