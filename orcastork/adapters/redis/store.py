"""Redis ``DataPointStore`` — keyed-merge over a hash, revision + epoch via a Lua CAS.

Each identity ``(type, value)`` is a field in ``dp:{session}`` holding the DataPoint JSON;
its first-added and last-freshened revisions live in parallel hashes (``added:{session}`` /
``upd:{session}``) keyed by the same identity. The session revision (``rev:{session}``) and
fencing epoch (``epoch:{session}``) are integer keys. The keyed-merge is computed in Python
(the orchestrator is the sole per-session mutator), then applied **atomically and
epoch-guarded** by a single Lua script: a stale epoch is rejected with no partial write, and
the revision is allocated by ``INCR`` **inside the script** so it stays monotonic regardless
of interleaving (the script treats the DataPoint JSON as an opaque blob, so it never has to
decode it). ``apply_resolved`` runs the same script without the ``write`` path's prior
HGETALL: a sole-mutator caller that already resolved the merge supplies the 'a'/'u' mode per
identity itself, so the hot write path costs one round-trip instead of a full re-read.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime
from typing import Any

import orjson
from redis.asyncio import Redis

from ...datapoints import BaseDataPoint, DataPointView, canonical_value, parse_data_point
from ...exceptions import StaleEpochError
from ...ids import Epoch, OperatorId, Revision, SessionId
from ...ports.change_set import ChangeSet
from ...ports.datapoint_store import EffectClaim
from .ttl import DEFAULT_STATE_TTL_MS, slide_ttl

# Atomically: reject a stale epoch (no writes); else bump epoch, and if there are changes
# allocate the next revision with INCR and apply each field. KEYS = epoch, rev, dp, added,
# upd; ARGV = epoch, then triples (identity, mode, dp_json) where mode 'a' = new identity
# (stamp added + updated) and 'u' = re-observation (stamp updated only).
_WRITE_SCRIPT = """
local epoch = tonumber(ARGV[1])
local stored = tonumber(redis.call('GET', KEYS[1]) or '0')
if epoch < stored then return -1 end
if epoch > stored then redis.call('SET', KEYS[1], epoch) end
if #ARGV < 4 then return tonumber(redis.call('GET', KEYS[2]) or '0') end
local rev = redis.call('INCR', KEYS[2])
for i = 2, #ARGV, 3 do
  local identity = ARGV[i]
  redis.call('HSET', KEYS[3], identity, ARGV[i + 2])
  redis.call('HSET', KEYS[5], identity, rev)
  if ARGV[i + 1] == 'a' then redis.call('HSET', KEYS[4], identity, rev) end
end
return rev
"""

# Atomically epoch-guard a session-meta hash-field write (a watermark, the wall-clock deadline,
# the flow fingerprint): a stale epoch is rejected, and the highest ACCEPTED epoch is recorded.
# Ownership is the lock's job; recording the epoch on every guarded write is what completes
# stale-writer rejection — a session whose first store mutation is one of these fields must
# still fence a later lower-epoch writer.
_GUARDED_FIELD_SCRIPT = """
local epoch = tonumber(ARGV[1])
local stored = tonumber(redis.call('GET', KEYS[1]) or '0')
if epoch < stored then return -1 end
if epoch > stored then redis.call('SET', KEYS[1], epoch) end
redis.call('HSET', KEYS[2], ARGV[2], ARGV[3])
return 0
"""

# Atomically epoch-guard a side-effect claim, recording the highest accepted epoch exactly like
# every other guarded write (a session whose first mutation is an effect claim must still fence
# a later lower-epoch writer). The stored states are 'pending:<epoch>' (in-flight, naming its
# owner) and 'committed' (the effect ran). KEYS = epoch, fx hash; ARGV = epoch, effect key,
# reclaim_stale flag ('1'/'0'). Returns -1 on a stale epoch, else the EffectClaim code:
# 1 ACQUIRED, 2 ALREADY_COMMITTED, 3 PENDING_SAME_EPOCH, 4 PENDING_STALE_EPOCH.
_CLAIM_EFFECT_SCRIPT = """
local epoch = tonumber(ARGV[1])
local stored = tonumber(redis.call('GET', KEYS[1]) or '0')
if epoch < stored then return -1 end
if epoch > stored then redis.call('SET', KEYS[1], epoch) end
local state = redis.call('HGET', KEYS[2], ARGV[2])
local pending = 'pending:' .. ARGV[1]
if state == false then
  redis.call('HSET', KEYS[2], ARGV[2], pending)
  return 1
end
if state == 'committed' then return 2 end
if state == pending then return 3 end
if ARGV[3] == '1' then
  redis.call('HSET', KEYS[2], ARGV[2], pending)
  return 1
end
return 4
"""

# Atomically epoch-guard the pending → committed transition (recording the accepted epoch like
# every guarded write). Only this epoch's own pending mark transitions; 'committed' stays as-is
# (idempotent) and any other state is left untouched — the script never fabricates 'committed'
# for a claim this epoch does not own. -1 on a stale epoch.
_COMMIT_EFFECT_SCRIPT = """
local epoch = tonumber(ARGV[1])
local stored = tonumber(redis.call('GET', KEYS[1]) or '0')
if epoch < stored then return -1 end
if epoch > stored then redis.call('SET', KEYS[1], epoch) end
if redis.call('HGET', KEYS[2], ARGV[2]) == ('pending:' .. ARGV[1]) then
  redis.call('HSET', KEYS[2], ARGV[2], 'committed')
end
return 0
"""

# Atomically epoch-guard the revert (recording the accepted epoch like every guarded write):
# deletes ONLY this epoch's own pending mark — never 'committed' (the effect DID run) and never
# another epoch's pending. -1 on a stale epoch.
_REVERT_EFFECT_SCRIPT = """
local epoch = tonumber(ARGV[1])
local stored = tonumber(redis.call('GET', KEYS[1]) or '0')
if epoch < stored then return -1 end
if epoch > stored then redis.call('SET', KEYS[1], epoch) end
if redis.call('HGET', KEYS[2], ARGV[2]) == ('pending:' .. ARGV[1]) then
  redis.call('HDEL', KEYS[2], ARGV[2])
end
return 0
"""

_CLAIM_RESULTS = {
    1: EffectClaim.ACQUIRED,
    2: EffectClaim.ALREADY_COMMITTED,
    3: EffectClaim.PENDING_SAME_EPOCH,
    4: EffectClaim.PENDING_STALE_EPOCH,
}


class RedisDataPointStore:
    def __init__(self, redis: Redis, *, state_ttl_ms: int = DEFAULT_STATE_TTL_MS) -> None:
        # redis-py types its commands as a sync/async union (ResponseT); treat the client as
        # Any internally so awaits type cleanly, while the public param stays typed.
        self._redis: Any = redis
        self._state_ttl_ms = state_ttl_ms

    @staticmethod
    def _identity(data_point: BaseDataPoint[Any]) -> str:
        # ``canonical_value`` is the shared string form of the keyed-merge identity (same normalization
        # as the in-memory store and the archive), so dedup is identical across adapters.
        return f'{data_point.type}\x00{canonical_value(data_point.value)}'

    @staticmethod
    def _keys(session_id: SessionId) -> tuple[str, str, str, str, str]:
        return (
            f'epoch:{session_id}',
            f'rev:{session_id}',
            f'dp:{session_id}',
            f'added:{session_id}',
            f'upd:{session_id}',
        )

    async def write(
        self, session_id: SessionId, data_points: Iterable[BaseDataPoint[Any]], *, epoch: Epoch
    ) -> Revision:
        _, _, dp_key, _, _ = self._keys(session_id)
        current = await self._redis.hgetall(dp_key)
        triples: list[str] = []
        for data_point in data_points:
            identity = self._identity(data_point)
            existing_raw = current.get(identity)
            if existing_raw is None:
                # The client decodes responses, so values flow as str — decode the orjson bytes here.
                triples += [identity, 'a', orjson.dumps(data_point.model_dump(mode='json')).decode()]
            else:
                existing_dp = parse_data_point(orjson.loads(existing_raw))
                if data_point.last_retrieved > existing_dp.last_retrieved:
                    merged = existing_dp.reobserved(data_point.last_retrieved)
                    triples += [identity, 'u', orjson.dumps(merged.model_dump(mode='json')).decode()]
        return await self._apply(session_id, triples, epoch)

    async def apply_resolved(
        self,
        session_id: SessionId,
        *,
        added: Sequence[BaseDataPoint[Any]],
        updated: Sequence[BaseDataPoint[Any]],
        epoch: Epoch,
    ) -> Revision:
        # The sole-mutator fast path: the caller already keyed-merged, so the prior HGETALL that
        # `write` needs to decide 'a' vs 'u' is skipped entirely — the supplied split IS the mode.
        triples: list[str] = []
        for data_point in added:
            triples += [self._identity(data_point), 'a', orjson.dumps(data_point.model_dump(mode='json')).decode()]
        for data_point in updated:
            triples += [self._identity(data_point), 'u', orjson.dumps(data_point.model_dump(mode='json')).decode()]
        return await self._apply(session_id, triples, epoch)

    async def _apply(self, session_id: SessionId, triples: Sequence[str], epoch: Epoch) -> Revision:
        epoch_key, rev_key, dp_key, added_key, upd_key = self._keys(session_id)
        result = await self._redis.eval(
            _WRITE_SCRIPT, 5, epoch_key, rev_key, dp_key, added_key, upd_key, int(epoch), *triples
        )
        if int(result) == -1:
            raise StaleEpochError(f'epoch {epoch} is stale for session {session_id}')
        # Effect marks (`fx:`) and session meta (`meta:` — the wall-clock deadline and the flow
        # fingerprint) are slid here too, so they share the session's sliding lifetime: an active
        # session must never lose its at-most-once guarantees or its remaining deadline budget
        # mid-flight.
        await slide_ttl(
            self._redis,
            self._state_ttl_ms,
            epoch_key,
            rev_key,
            dp_key,
            added_key,
            upd_key,
            f'wm:{session_id}',
            f'fx:{session_id}',
            f'meta:{session_id}',
        )
        return Revision(int(result))

    async def _entries(self, session_id: SessionId) -> list[dict[str, Any]]:
        _, _, dp_key, added_key, upd_key = self._keys(session_id)
        dps = await self._redis.hgetall(dp_key)
        added = await self._redis.hgetall(added_key)
        updated = await self._redis.hgetall(upd_key)
        return [
            {'dp': orjson.loads(raw), 'added': int(added.get(identity, 0)), 'updated': int(updated.get(identity, 0))}
            for identity, raw in dps.items()
        ]

    async def snapshot(self, session_id: SessionId) -> DataPointView:
        return DataPointView(parse_data_point(entry['dp']) for entry in await self._entries(session_id))

    async def revision(self, session_id: SessionId) -> Revision:
        _, rev_key, _, _, _ = self._keys(session_id)
        return Revision(int(await self._redis.get(rev_key) or 0))

    async def change_set_since(self, session_id: SessionId, since: Revision) -> ChangeSet:
        added: list[BaseDataPoint[Any]] = []
        updated: list[BaseDataPoint[Any]] = []
        for entry in await self._entries(session_id):
            data_point = parse_data_point(entry['dp'])
            if entry['added'] > since:
                added.append(data_point)
            elif entry['updated'] > since:
                updated.append(data_point)
        return ChangeSet(added=tuple(added), updated=tuple(updated))

    async def get_watermark(self, session_id: SessionId, operator_id: OperatorId) -> Revision | None:
        raw = await self._redis.hget(f'wm:{session_id}', operator_id)
        return None if raw is None else Revision(int(raw))

    async def set_watermark(
        self, session_id: SessionId, operator_id: OperatorId, revision: Revision, *, epoch: Epoch
    ) -> None:
        epoch_key, _, _, _, _ = self._keys(session_id)
        result = await self._redis.eval(
            _GUARDED_FIELD_SCRIPT, 2, epoch_key, f'wm:{session_id}', str(int(epoch)), operator_id, str(int(revision))
        )
        if int(result) == -1:
            raise StaleEpochError(f'epoch {epoch} is stale for session {session_id}')
        await slide_ttl(self._redis, self._state_ttl_ms, epoch_key, f'wm:{session_id}')

    async def claim_effect(
        self, session_id: SessionId, effect_key: str, *, epoch: Epoch, reclaim_stale: bool
    ) -> EffectClaim:
        result = await self._run_effect_script(
            _CLAIM_EFFECT_SCRIPT, session_id, effect_key, epoch, '1' if reclaim_stale else '0'
        )
        return _CLAIM_RESULTS[result]

    async def commit_effect(self, session_id: SessionId, effect_key: str, *, epoch: Epoch) -> None:
        await self._run_effect_script(_COMMIT_EFFECT_SCRIPT, session_id, effect_key, epoch)

    async def revert_effect(self, session_id: SessionId, effect_key: str, *, epoch: Epoch) -> None:
        await self._run_effect_script(_REVERT_EFFECT_SCRIPT, session_id, effect_key, epoch)

    async def get_effect_state(self, session_id: SessionId, effect_key: str) -> str | None:
        raw = await self._redis.hget(f'fx:{session_id}', effect_key)
        return None if raw is None else str(raw)

    async def _run_effect_script(
        self, script: str, session_id: SessionId, effect_key: str, epoch: Epoch, *extra_args: str
    ) -> int:
        epoch_key, _, _, _, _ = self._keys(session_id)
        result = await self._redis.eval(
            script, 2, epoch_key, f'fx:{session_id}', str(int(epoch)), effect_key, *extra_args
        )
        if int(result) == -1:
            raise StaleEpochError(f'epoch {epoch} is stale for session {session_id}')
        await slide_ttl(self._redis, self._state_ttl_ms, epoch_key, f'fx:{session_id}')
        return int(result)

    async def get_session_deadline(self, session_id: SessionId) -> datetime | None:
        raw = await self._redis.hget(f'meta:{session_id}', 'deadline')
        return None if raw is None else datetime.fromisoformat(raw)

    async def set_session_deadline(self, session_id: SessionId, deadline: datetime, *, epoch: Epoch) -> None:
        epoch_key, _, _, _, _ = self._keys(session_id)
        result = await self._redis.eval(
            _GUARDED_FIELD_SCRIPT,
            2,
            epoch_key,
            f'meta:{session_id}',
            str(int(epoch)),
            'deadline',
            deadline.isoformat(),
        )
        if int(result) == -1:
            raise StaleEpochError(f'epoch {epoch} is stale for session {session_id}')
        await slide_ttl(self._redis, self._state_ttl_ms, epoch_key, f'meta:{session_id}')

    async def get_flow_fingerprint(self, session_id: SessionId) -> str | None:
        raw = await self._redis.hget(f'meta:{session_id}', 'flow_fingerprint')
        return None if raw is None else str(raw)

    async def set_flow_fingerprint(self, session_id: SessionId, fingerprint: str, *, epoch: Epoch) -> None:
        epoch_key, _, _, _, _ = self._keys(session_id)
        result = await self._redis.eval(
            _GUARDED_FIELD_SCRIPT,
            2,
            epoch_key,
            f'meta:{session_id}',
            str(int(epoch)),
            'flow_fingerprint',
            fingerprint,
        )
        if int(result) == -1:
            raise StaleEpochError(f'epoch {epoch} is stale for session {session_id}')
        await slide_ttl(self._redis, self._state_ttl_ms, epoch_key, f'meta:{session_id}')
