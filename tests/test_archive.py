"""ARCH — DataPoint Archive: the second durable write path (live, keyed-upsert).

The port contract (ARCH-01/02/04/05/06/08/10) runs against the in-memory adapter via the
conformance mixin (and against Mongo in ``adapters/test_mongo.py``). The cipher seam
(ARCH-07) and the clock-driven retention TTL (ARCH-09) are adapter-construction specific,
and ephemeral-gating + live-archiving (ARCH-03 + provenance/epoch stamping) are
orchestrator behaviours — all exercised here on the in-memory substrate with a FakeClock.
"""

from __future__ import annotations

import hashlib
import json
from datetime import timedelta
from typing import Literal

import pytest

from orcastork.adapters.memory import InMemoryDataPointArchive
from orcastork.adapters.memory.datapoint_archive import _fold_in as memory_fold_in
from orcastork.archive import ArchivedDataPoint
from orcastork.archive.sealing import unseal, value_hash
from orcastork.datapoints import BaseDataPoint, DataPointTypeConfig, canonical_value
from orcastork.ids import Epoch, OperatorId, SessionId
from orcastork.orchestrator import Orchestrator
from orcastork.ports import DataPointArchive
from orcastork.runtime import build_in_memory_runtime

from .doubles.cipher import ReversingCipher
from .doubles.clock import FakeClock
from .doubles.conformance import NAMESPACE, SID, T2, DataPointArchiveConformance
from .doubles.datapoints import T0, RiskDataPoint, TriggerDataPoint, risk, work_email
from .doubles.operators import make_operator


class TestInMemoryDataPointArchive(DataPointArchiveConformance):
    @pytest.fixture
    def archive(self) -> DataPointArchive:
        return InMemoryDataPointArchive()


def _archived(data_point: object) -> ArchivedDataPoint:
    return ArchivedDataPoint.from_data_point(
        data_point,  # type: ignore[arg-type]
        session_id=SID,
        namespace_id=NAMESPACE,
        epoch=Epoch(1),
    )


async def test_arch_07_pii_value_encrypted_at_rest_and_decrypts_on_read() -> None:
    cipher = ReversingCipher()
    archive = InMemoryDataPointArchive(cipher=cipher)
    pii = _archived(work_email('alice@work.example'))
    public = _archived(risk(0.5))
    await archive.archive(pii)
    await archive.archive(public)
    await archive.flush(SID)

    # At rest, the PII value is the ciphertext (not the plaintext); the non-PII value is untouched.
    # The adapter derives value_hash from the value, so index the committed map by type to find each.
    committed = {entry.type: entry for entry in archive._sessions[SID].committed.values()}  # white-box
    assert committed['work_email'].value == cipher.encrypt(json.dumps('alice@work.example'))
    assert committed['work_email'].value != 'alice@work.example'
    assert committed['risk'].value == 0.5

    # Reading decrypts the PII value back to the original and leaves non-PII alone.
    by_type = {entry.type: entry.value for entry in await archive.read(SID)}
    assert by_type['work_email'] == 'alice@work.example'
    assert by_type['risk'] == 0.5


async def test_arch_07_pii_value_hash_is_keyed_and_unlinkable_across_sessions() -> None:
    # The PII key is a keyed MAC mixing in session_id — never a bare plaintext digest (so the archive is
    # not an offline-confirmation oracle), and the same value in a different session derives a different
    # key (cross-session-unlinkable). Per-session dedup is unaffected: the committed key is per-session.
    cipher = ReversingCipher()
    canonical = canonical_value('alice@work.example')

    here = InMemoryDataPointArchive(cipher=cipher)
    await here.archive(_archived(work_email('alice@work.example')))
    await here.flush(SID)
    stored = next(iter(here._sessions[SID].committed.values()))  # white-box
    assert stored.value_hash == cipher.mac(f'{SID}\x00{canonical}')
    assert stored.value_hash != hashlib.sha256(canonical.encode()).hexdigest()  # not a bare digest

    other_session = SessionId('arch-07-other')
    there = InMemoryDataPointArchive(cipher=cipher)
    await there.archive(
        ArchivedDataPoint.from_data_point(
            work_email('alice@work.example'), session_id=other_session, namespace_id=NAMESPACE, epoch=Epoch(1)
        )
    )
    await there.flush(other_session)
    other_stored = next(iter(there._sessions[other_session].committed.values()))
    assert other_stored.value_hash != stored.value_hash  # same value, different session → different key


async def test_arch_09_retention_ttl_expires_archived_docs_on_schedule(fake_clock: FakeClock) -> None:
    archive = InMemoryDataPointArchive(clock=fake_clock, retention=timedelta(hours=1))
    await archive.archive(_archived(work_email('a@e.example', first=T0, last=T0)))
    await archive.flush(SID)
    assert len(await archive.read(SID)) == 1  # fresh
    fake_clock.advance(1800)  # +30 min — still within the 1h retention window
    assert len(await archive.read(SID)) == 1
    fake_clock.advance(3600)  # now > 1h since last_retrieved — expired
    assert await archive.read(SID) == ()


async def test_arch_03_ephemeral_datapoint_is_not_archived(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    operator = make_operator(
        'op', produces={RiskDataPoint, TriggerDataPoint}, emits=[RiskDataPoint.emit(0.7), TriggerDataPoint.emit('go')]
    )
    await Orchestrator(session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[operator]).run()
    archived_types = {entry.type for entry in await runtime.archive.read(SID)}
    assert 'risk' in archived_types  # non-ephemeral is archived
    assert 'trigger' not in archived_types  # ephemeral is never archived, on either durable path


async def test_arch_orchestrator_live_archives_and_stamps_provenance_and_epoch(fake_clock: FakeClock) -> None:
    runtime = build_in_memory_runtime(fake_clock)
    operator = make_operator('op', produces={RiskDataPoint}, emits=[RiskDataPoint.emit(0.7)])
    result = await Orchestrator(
        session_id=SID, namespace_id=NAMESPACE, runtime=runtime, operators=[operator], seed=[work_email()]
    ).run()

    archived = {entry.type: entry for entry in await runtime.archive.read(SID)}
    assert set(archived) == {'work_email', 'risk'}  # the seed and the emission are both live-archived + flushed
    assert archived['risk'].value == 0.7
    assert archived['risk'].epoch == result.epoch  # epoch-stamped by the writer
    assert archived['risk'].retrieved_by == OperatorId('op')  # provenance stamped by the orchestrator
    assert archived['work_email'].namespace_id == NAMESPACE


class _GeoPiiDataPoint(BaseDataPoint[dict[str, float]]):
    # A PII leaf carrying a structured (dict) value — the seal/unseal seam JSON-encodes the value
    # before encrypting, so it must round-trip non-string structures, not just scalar strings.
    type: Literal['geo_pii'] = 'geo_pii'
    config = DataPointTypeConfig(pii=True, ephemeral=False)


def _geo_pii(value: dict[str, float]) -> _GeoPiiDataPoint:
    return _GeoPiiDataPoint(value=value, retrieved_by=OperatorId('collector'), first_retrieved=T0, last_retrieved=T0)


async def test_arch_07_unseal_reads_back_stdlib_json_spaced_ciphertext() -> None:
    # Cross-serializer durability: a PII value sealed by a build that serialized with stdlib json
    # (spaced separators like {"a": 1}) must still unseal here, where orjson.loads reads it. Hand-build
    # the sealed entry from json.dumps(..., separators=(', ', ': ')) so the spacing is explicit.
    cipher = ReversingCipher()
    original = {'a': 1, 'b': 2}
    spaced = json.dumps(original, separators=(', ', ': '))
    assert ', ' in spaced and ': ' in spaced  # the spaced stdlib form, not compact orjson ({"a":1,...})
    sealed = ArchivedDataPoint(
        session_id=SID,
        namespace_id=NAMESPACE,
        type='geo_pii',
        value=cipher.encrypt(spaced),  # what an older stdlib-json build would have stored at rest
        retrieved_by=OperatorId('collector'),
        first_retrieved=T0,
        last_retrieved=T0,
        is_pii=True,
        epoch=Epoch(1),
    )

    recovered = unseal(sealed, cipher)

    assert recovered.value == original  # orjson.loads tolerated the spaced stdlib-json form


async def test_arch_07_structured_pii_value_seals_round_trips_and_dedups() -> None:
    # Structured (dict) PII must survive the JSON-encode/encrypt/decrypt/JSON-decode round-trip
    # identically, and re-observing the same structured value must keyed-upsert to ONE committed doc
    # (the encrypt-serializes-as-str seam must not corrupt non-string values or perturb the key).
    cipher = ReversingCipher()
    archive = InMemoryDataPointArchive(cipher=cipher)
    value = {'lat': 51.5, 'lon': -0.12}
    await archive.archive(_archived(_geo_pii(value)))
    await archive.flush(SID)

    (committed,) = await archive.read(SID)
    assert committed.value == value  # structurally identical after the full PII round-trip
    assert isinstance(committed.value, dict)  # not corrupted into a string by the encrypt seam

    # At rest the value is the ciphertext over the JSON form, never the plaintext dict.
    stored = next(iter(archive._sessions[SID].committed.values()))  # white-box
    assert stored.value != value
    expected_key = value_hash(_archived(_geo_pii(value)), cipher)
    assert stored.value_hash == expected_key  # keyed MAC over the canonical structured value

    # Re-observe the SAME structured value at a later time: keyed-upsert dedup → one doc, last bumped.
    await archive.archive(
        ArchivedDataPoint.from_data_point(
            _GeoPiiDataPoint(value=value, retrieved_by=OperatorId('collector'), first_retrieved=T0, last_retrieved=T2),
            session_id=SID,
            namespace_id=NAMESPACE,
            epoch=Epoch(1),
        )
    )
    await archive.flush(SID)
    (after_redelivery,) = await archive.read(SID)  # still exactly one committed doc
    assert after_redelivery.value == value
    assert after_redelivery.last_retrieved == T2  # last_retrieved bumped on re-observation


async def test_arch_15_the_in_memory_fold_refuses_to_walk_a_stored_epoch_back() -> None:
    # Asserted on the fold rather than through the port: `archive` fences an entry below the
    # session's high water mark, so an older-epoch re-observation can never reach the buffer and no
    # port-level sequence distinguishes `max(existing, sealed)` from taking whichever arrived last.
    # The fold still has to refuse it — it is the merge both a flush and a buffered read run, and the
    # stored row says which epoch last SAW the datapoint, so lowering it would date the row before
    # the run that actually produced it.
    newer = _archived(work_email('a@e.example', first=T0, last=T2)).model_copy(update={'epoch': Epoch(2)})
    older = _archived(work_email('a@e.example', first=T0, last=T0)).model_copy(update={'epoch': Epoch(1)})

    folded: dict[tuple[str, str], ArchivedDataPoint] = {}
    memory_fold_in(folded, newer)
    memory_fold_in(folded, older)

    (merged,) = folded.values()
    assert int(merged.epoch) == 2  # not 1 — the out-of-order arrival does not lower it
    assert merged.last_retrieved == T2  # nor does it walk the timestamp back
