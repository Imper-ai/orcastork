"""Port-conformance mixins (CNF).

Each mixin holds the behavioural contract for one port, written entirely against the port
interface — never a concrete adapter. A binding subclass (named ``Test*``) supplies the
adapter via fixtures, so the same contract runs against in-memory now and Redis/Mongo
later. Time-dependent contracts use an ``advance_time`` fixture (a callable) so each
backend advances its own clock.

The mixin classes are deliberately NOT named ``Test*`` so pytest does not collect them
directly (they have no adapter bound).
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from pydantic import ValidationError

from orcastork.archive import ArchivedDataPoint
from orcastork.audit import AuditKind, AuditLogEntry
from orcastork.datapoints import BaseDataPoint
from orcastork.exceptions import (
    LockHeldError,
    OptimisticConcurrencyError,
    StaleEpochError,
    UnknownDataPointTypeError,
)
from orcastork.ids import Epoch, NamespaceId, OperatorId, Revision, SessionId
from orcastork.ports import (
    AuditSink,
    CooldownGate,
    DataPointArchive,
    DataPointStore,
    DurableStore,
    EffectClaim,
    Inbox,
    InboxEntry,
    PoisonInboxEntry,
    SessionLock,
)

from .datapoints import T0, EmailDataPoint, TriggerDataPoint, personal_email, risk, work_email

SID = SessionId('cnf-session')
OTHER_SID = SessionId('cnf-other-session')
NAMESPACE = NamespaceId('cnf-namespace')
TBL = 'cnf-durable'  # a destination table (an output model's __table_name__)
T1 = T0 + timedelta(hours=1)
T2 = T0 + timedelta(hours=2)
OP = OperatorId('cnf-op')

# A foreign producer appends a wire payload. Each inbox binding supplies the backend-specific
# write (a raw stream XADD for Redis, the serialized seam for in-memory) and returns the entry id.
AppendRaw = Callable[[SessionId, str], Awaitable[str]]

# A well-formed wire payload whose DataPoint type only a newer deploy knows — NOT poison: the
# bytes decode fine, so deserialization must fail fast rather than quarantine.
NEWER_DEPLOY_PAYLOAD = json.dumps(
    {
        'type': 'type_from_a_newer_deploy',
        'value': 'x',
        'retrieved_by': 'cnf-op',
        'first_retrieved': '2026-01-01T00:00:00Z',
        'last_retrieved': '2026-01-01T00:00:00Z',
    }
)

# A known DataPoint type with a structurally broken body (missing required fields) — decodes as
# JSON, fails pydantic validation: a semantic failure that must propagate, never quarantine.
MALFORMED_KNOWN_TYPE_PAYLOAD = json.dumps({'type': 'work_email', 'value': 'a@work.example'})


class StoreConformance:
    async def test_store_01_add_then_get(self, store: DataPointStore) -> None:
        point = work_email('a@work.example')
        await store.write(SID, [point], epoch=Epoch(1))
        assert point in set((await store.snapshot(SID)).all())

    async def test_store_02_keyed_merge_bumps_last_and_keeps_size(self, store: DataPointStore) -> None:
        await store.write(SID, [work_email('a@work.example', last=T0)], epoch=Epoch(1))
        await store.write(SID, [work_email('a@work.example', last=T2)], epoch=Epoch(1))
        snapshot = await store.snapshot(SID)
        assert len(snapshot) == 1
        assert snapshot.all()[0].last_retrieved == T2

    async def test_store_03_revision_advances_on_every_mutating_write(self, store: DataPointStore) -> None:
        first = await store.write(SID, [work_email('a@work.example')], epoch=Epoch(1))
        second = await store.write(SID, [personal_email('p@home.example')], epoch=Epoch(1))
        assert second > first

    async def test_store_04_epoch_guarded_write(self, store: DataPointStore) -> None:
        await store.write(SID, [work_email('a@work.example')], epoch=Epoch(2))
        with pytest.raises(StaleEpochError):
            await store.write(SID, [personal_email('p@home.example')], epoch=Epoch(1))

    async def test_store_05_subtype_query_returns_all_leaves(self, store: DataPointStore) -> None:
        await store.write(SID, [work_email('w@e.example'), personal_email('p@e.example')], epoch=Epoch(1))
        emails = (await store.snapshot(SID)).of_type(EmailDataPoint)
        assert len(emails) == 2

    async def test_store_06_new_value_for_existing_type_coexists(self, store: DataPointStore) -> None:
        await store.write(SID, [work_email('a@e.example'), work_email('b@e.example')], epoch=Epoch(1))
        assert len((await store.snapshot(SID)).all()) == 2

    async def test_store_07_timestamp_reobservation_advances_revision_and_surfaces_updated(
        self, store: DataPointStore
    ) -> None:
        first = await store.write(SID, [work_email('a@e.example', last=T0)], epoch=Epoch(1))
        second = await store.write(SID, [work_email('a@e.example', last=T2)], epoch=Epoch(1))
        assert second > first
        change = await store.change_set_since(SID, first)
        assert any(p.type == 'work_email' for p in change.updated)
        assert not change.added

    async def test_store_08_lower_epoch_cas_fails_without_partial_write(self, store: DataPointStore) -> None:
        await store.write(SID, [work_email('a@e.example')], epoch=Epoch(2))
        with pytest.raises(StaleEpochError):
            await store.write(SID, [personal_email('b@e.example')], epoch=Epoch(1))
        values = {p.value for p in (await store.snapshot(SID)).all()}
        assert 'b@e.example' not in values

    async def test_store_09_change_set_splits_added_and_updated(self, store: DataPointStore) -> None:
        base = await store.write(SID, [work_email('a@e.example', last=T0)], epoch=Epoch(1))
        await store.write(SID, [work_email('a@e.example', last=T2), personal_email('p@e.example')], epoch=Epoch(1))
        change = await store.change_set_since(SID, base)
        assert {p.type for p in change.added} == {'personal_email'}
        assert {p.type for p in change.updated} == {'work_email'}

    async def test_store_10_ephemeral_stored_live_and_flagged(self, store: DataPointStore) -> None:
        trigger = TriggerDataPoint(value='go', retrieved_by=OP, first_retrieved=T0, last_retrieved=T0)
        await store.write(SID, [trigger], epoch=Epoch(1))
        stored = (await store.snapshot(SID)).all()
        assert len(stored) == 1
        assert stored[0].is_ephemeral

    async def test_store_11_empty_store_is_empty_at_baseline_revision(self, store: DataPointStore) -> None:
        assert len((await store.snapshot(SID)).all()) == 0
        assert await store.revision(SID) == 0

    async def test_store_12_claim_acquires_then_same_epoch_duplicate_is_pending(self, store: DataPointStore) -> None:
        assert (
            await store.claim_effect(SID, 'op:send-otp', epoch=Epoch(1), reclaim_stale=False) is EffectClaim.ACQUIRED
        )
        assert await store.get_effect_state(SID, 'op:send-otp') == 'pending:1'
        duplicate = await store.claim_effect(SID, 'op:send-otp', epoch=Epoch(1), reclaim_stale=False)
        assert duplicate is EffectClaim.PENDING_SAME_EPOCH
        assert await store.get_effect_state(SID, 'op:send-otp') == 'pending:1'  # the duplicate changed nothing

    async def test_store_13_commit_transitions_pending_to_committed_and_is_idempotent(
        self, store: DataPointStore
    ) -> None:
        await store.claim_effect(SID, 'op:send-otp', epoch=Epoch(1), reclaim_stale=False)
        await store.commit_effect(SID, 'op:send-otp', epoch=Epoch(1))
        assert await store.get_effect_state(SID, 'op:send-otp') == 'committed'
        await store.commit_effect(SID, 'op:send-otp', epoch=Epoch(1))  # re-committing is a safe no-op
        assert await store.get_effect_state(SID, 'op:send-otp') == 'committed'
        claim = await store.claim_effect(SID, 'op:send-otp', epoch=Epoch(1), reclaim_stale=False)
        assert claim is EffectClaim.ALREADY_COMMITTED

    async def test_store_14_committed_survives_epoch_takeover(self, store: DataPointStore) -> None:
        await store.claim_effect(SID, 'op:send-otp', epoch=Epoch(1), reclaim_stale=False)
        await store.commit_effect(SID, 'op:send-otp', epoch=Epoch(1))
        successor = await store.claim_effect(SID, 'op:send-otp', epoch=Epoch(2), reclaim_stale=False)
        assert successor is EffectClaim.ALREADY_COMMITTED  # a resume never re-fires a committed effect

    async def test_store_15_stale_pending_reported_or_reclaimed_per_flag(self, store: DataPointStore) -> None:
        await store.claim_effect(SID, 'op:send-otp', epoch=Epoch(1), reclaim_stale=False)
        observed = await store.claim_effect(SID, 'op:send-otp', epoch=Epoch(2), reclaim_stale=False)
        assert observed is EffectClaim.PENDING_STALE_EPOCH
        assert await store.get_effect_state(SID, 'op:send-otp') == 'pending:1'  # reporting leaves the mark intact
        reclaimed = await store.claim_effect(SID, 'op:send-otp', epoch=Epoch(2), reclaim_stale=True)
        assert reclaimed is EffectClaim.ACQUIRED
        assert await store.get_effect_state(SID, 'op:send-otp') == 'pending:2'  # ownership moved to the reclaimer

    async def test_store_15a_revert_deletes_only_this_epochs_pending(self, store: DataPointStore) -> None:
        # The owner releases its claim, freeing the key for a same-epoch re-claim.
        await store.claim_effect(SID, 'op:send-otp', epoch=Epoch(1), reclaim_stale=False)
        await store.revert_effect(SID, 'op:send-otp', epoch=Epoch(1))
        assert await store.get_effect_state(SID, 'op:send-otp') is None
        assert (
            await store.claim_effect(SID, 'op:send-otp', epoch=Epoch(1), reclaim_stale=False) is EffectClaim.ACQUIRED
        )
        # A non-owning (higher-epoch) revert leaves the predecessor's mark in place — the unknown
        # outcome stays visible for the recovery policy, never silently cleared.
        await store.revert_effect(SID, 'op:send-otp', epoch=Epoch(2))
        assert await store.get_effect_state(SID, 'op:send-otp') == 'pending:1'

    async def test_store_15b_revert_never_deletes_committed(self, store: DataPointStore) -> None:
        await store.claim_effect(SID, 'op:send-otp', epoch=Epoch(1), reclaim_stale=False)
        await store.commit_effect(SID, 'op:send-otp', epoch=Epoch(1))
        await store.revert_effect(SID, 'op:send-otp', epoch=Epoch(1))
        assert await store.get_effect_state(SID, 'op:send-otp') == 'committed'  # the effect DID run

    async def test_store_15c_all_three_effect_ops_are_epoch_guarded(self, store: DataPointStore) -> None:
        await store.claim_effect(SID, 'op:send-otp', epoch=Epoch(2), reclaim_stale=False)
        await store.write(SID, [work_email('a@work.example')], epoch=Epoch(2))
        with pytest.raises(StaleEpochError):
            await store.claim_effect(SID, 'op:other', epoch=Epoch(1), reclaim_stale=False)
        with pytest.raises(StaleEpochError):
            await store.commit_effect(SID, 'op:send-otp', epoch=Epoch(1))
        with pytest.raises(StaleEpochError):
            await store.revert_effect(SID, 'op:send-otp', epoch=Epoch(1))
        assert await store.get_effect_state(SID, 'op:other') is None  # the rejected claim left nothing
        assert await store.get_effect_state(SID, 'op:send-otp') == 'pending:2'  # untouched by the fenced calls

    async def test_store_15d_effect_marks_are_per_session(self, store: DataPointStore) -> None:
        assert (
            await store.claim_effect(SID, 'op:send-otp', epoch=Epoch(1), reclaim_stale=False) is EffectClaim.ACQUIRED
        )
        assert (
            await store.claim_effect(OTHER_SID, 'op:send-otp', epoch=Epoch(1), reclaim_stale=False)
            is EffectClaim.ACQUIRED
        )
        await store.commit_effect(SID, 'op:send-otp', epoch=Epoch(1))
        assert await store.get_effect_state(OTHER_SID, 'op:send-otp') == 'pending:1'  # the commit never crossed over

    async def test_store_15e_distinct_effect_keys_are_independent(self, store: DataPointStore) -> None:
        assert (
            await store.claim_effect(SID, 'op-a:notify', epoch=Epoch(1), reclaim_stale=False) is EffectClaim.ACQUIRED
        )
        assert (
            await store.claim_effect(SID, 'op-b:notify', epoch=Epoch(1), reclaim_stale=False) is EffectClaim.ACQUIRED
        )

    async def test_store_16_session_deadline_roundtrips_and_is_absent_until_set(self, store: DataPointStore) -> None:
        assert await store.get_session_deadline(SID) is None
        await store.set_session_deadline(SID, T1, epoch=Epoch(1))
        assert await store.get_session_deadline(SID) == T1

    async def test_store_17_session_deadline_write_is_epoch_guarded(self, store: DataPointStore) -> None:
        await store.write(SID, [work_email('a@work.example')], epoch=Epoch(2))
        with pytest.raises(StaleEpochError):
            await store.set_session_deadline(SID, T1, epoch=Epoch(1))
        assert await store.get_session_deadline(SID) is None  # the rejected write left nothing

    async def test_store_18_session_deadlines_are_per_session(self, store: DataPointStore) -> None:
        await store.set_session_deadline(SID, T1, epoch=Epoch(1))
        assert await store.get_session_deadline(OTHER_SID) is None

    async def test_store_19_flow_fingerprint_roundtrips_and_is_absent_until_set(self, store: DataPointStore) -> None:
        assert await store.get_flow_fingerprint(SID) is None
        await store.set_flow_fingerprint(SID, 'fp-original', epoch=Epoch(1))
        assert await store.get_flow_fingerprint(SID) == 'fp-original'
        await store.set_flow_fingerprint(SID, 'fp-drifted', epoch=Epoch(2))  # drift rewrites to the latest flow
        assert await store.get_flow_fingerprint(SID) == 'fp-drifted'

    async def test_store_20_flow_fingerprint_write_is_epoch_guarded(self, store: DataPointStore) -> None:
        await store.write(SID, [work_email('a@work.example')], epoch=Epoch(2))
        with pytest.raises(StaleEpochError):
            await store.set_flow_fingerprint(SID, 'fp-stale', epoch=Epoch(1))
        assert await store.get_flow_fingerprint(SID) is None  # the rejected write left nothing

    async def test_store_21_flow_fingerprints_are_per_session(self, store: DataPointStore) -> None:
        await store.set_flow_fingerprint(SID, 'fp-original', epoch=Epoch(1))
        assert await store.get_flow_fingerprint(OTHER_SID) is None

    async def test_store_22_apply_resolved_allocates_revisions_exactly_like_write(self, store: DataPointStore) -> None:
        first = await store.write(SID, [work_email('a@e.example')], epoch=Epoch(1))
        second = await store.apply_resolved(SID, added=[personal_email('p@e.example')], updated=[], epoch=Epoch(1))
        third = await store.write(SID, [work_email('b@e.example')], epoch=Epoch(1))
        assert second == first + 1  # one revision per non-empty batch, same allocator as write
        assert third == second + 1  # and write keeps allocating from the same sequence afterwards
        values = {p.value for p in (await store.snapshot(SID)).all()}
        assert values == {'a@e.example', 'p@e.example', 'b@e.example'}

    async def test_store_23_apply_resolved_stamps_added_and_updated_like_write(self, store: DataPointStore) -> None:
        base = await store.write(SID, [work_email('a@e.example', last=T0)], epoch=Epoch(1))
        merged = work_email('a@e.example', last=T2)  # the caller resolved the merge: timestamps already final
        applied = await store.apply_resolved(
            SID, added=[personal_email('p@e.example')], updated=[merged], epoch=Epoch(1)
        )
        change = await store.change_set_since(SID, base)
        assert {p.type for p in change.added} == {'personal_email'}
        assert {p.type for p in change.updated} == {'work_email'}
        assert (await store.change_set_since(SID, applied)).added == ()  # both stamped at the batch revision
        snapshot = await store.snapshot(SID)
        assert len(snapshot) == 2  # the update merged, never duplicated
        assert next(p for p in snapshot.all() if p.type == 'work_email').last_retrieved == T2

    async def test_store_24_apply_resolved_stale_epoch_rejected_with_no_partial_write(
        self, store: DataPointStore
    ) -> None:
        before = await store.write(SID, [work_email('a@e.example', last=T0)], epoch=Epoch(2))
        with pytest.raises(StaleEpochError):
            await store.apply_resolved(
                SID,
                added=[personal_email('p@e.example')],
                updated=[work_email('a@e.example', last=T2)],
                epoch=Epoch(1),
            )
        snapshot = await store.snapshot(SID)
        assert {p.value for p in snapshot.all()} == {'a@e.example'}  # the added identity never landed
        assert snapshot.all()[0].last_retrieved == T0  # the update never landed either — atomic rejection
        assert await store.revision(SID) == before  # no revision was burned by the rejected batch

    async def test_store_25_apply_resolved_empty_batch_keeps_revision_and_still_fences(
        self, store: DataPointStore
    ) -> None:
        current = await store.write(SID, [work_email('a@e.example')], epoch=Epoch(2))
        assert await store.apply_resolved(SID, added=[], updated=[], epoch=Epoch(2)) == current
        assert await store.revision(SID) == current  # an all-no-op batch allocates nothing
        with pytest.raises(StaleEpochError):  # but fencing still applies on the empty path, like write
            await store.apply_resolved(SID, added=[], updated=[], epoch=Epoch(1))

    async def test_store_26_claim_effect_advances_the_fence_for_later_guarded_writes(
        self, store: DataPointStore
    ) -> None:
        # A session whose FIRST mutation is an effect claim must still fence lower-epoch writers:
        # every guarded write records the highest accepted epoch (ownership is the lock's job;
        # the recorded fence is what completes stale-writer rejection).
        await store.claim_effect(SID, 'op:send-otp', epoch=Epoch(5), reclaim_stale=False)
        with pytest.raises(StaleEpochError):
            await store.set_watermark(SID, OP, Revision(1), epoch=Epoch(4))
        with pytest.raises(StaleEpochError):
            await store.set_session_deadline(SID, T1, epoch=Epoch(4))
        assert await store.get_watermark(SID, OP) is None  # the fenced writes left nothing
        assert await store.get_session_deadline(SID) is None

    async def test_store_27_fingerprint_write_advances_the_fence_for_later_guarded_writes(
        self, store: DataPointStore
    ) -> None:
        await store.set_flow_fingerprint(SID, 'fp-first', epoch=Epoch(5))
        with pytest.raises(StaleEpochError):
            await store.set_session_deadline(SID, T1, epoch=Epoch(4))
        with pytest.raises(StaleEpochError):
            await store.write(SID, [work_email('a@e.example')], epoch=Epoch(4))
        assert len((await store.snapshot(SID)).all()) == 0  # the fenced write left nothing


class InboxConformance:
    async def test_inbox_01_append_consume_ack(self, inbox: Inbox) -> None:
        entry_id = await inbox.append(SID, work_email('a@e.example'))
        delivered = await inbox.consume(SID)
        assert [e.entry_id for e in delivered] == [entry_id]
        await inbox.ack(SID, entry_id, epoch=Epoch(1))
        assert await inbox.pending_count(SID) == 0

    async def test_inbox_02_unacked_redelivered_after_reclaim(self, inbox: Inbox) -> None:
        await inbox.append(SID, work_email('a@e.example'))
        await inbox.consume(SID)  # claim, do NOT ack
        reclaimed = await inbox.reclaim(SID)
        assert len(reclaimed) == 1
        assert reclaimed[0].delivery_count == 2

    async def test_inbox_03_delivered_in_append_order(self, inbox: Inbox) -> None:
        first = await inbox.append(SID, work_email('a@e.example'))
        second = await inbox.append(SID, personal_email('p@e.example'))
        delivered = await inbox.consume(SID)
        assert [e.entry_id for e in delivered] == [first, second]

    async def test_inbox_04_crash_before_ack_is_reclaimable(self, inbox: Inbox) -> None:
        await inbox.append(SID, work_email('a@e.example'))
        await inbox.consume(SID)  # consumer "crashes" before ack
        assert len(await inbox.reclaim(SID)) == 1

    async def test_inbox_05_append_with_no_consumer_waits(self, inbox: Inbox) -> None:
        await inbox.append(SID, work_email('a@e.example'))
        assert await inbox.pending_count(SID) == 1
        assert len(await inbox.consume(SID)) == 1

    async def test_inbox_06_ack_of_unknown_or_acked_is_noop(self, inbox: Inbox) -> None:
        entry_id = await inbox.append(SID, work_email('a@e.example'))
        await inbox.consume(SID)
        await inbox.ack(SID, 'no-such-entry', epoch=Epoch(1))  # unknown → no-op
        await inbox.ack(SID, entry_id, epoch=Epoch(1))
        await inbox.ack(SID, entry_id, epoch=Epoch(1))  # already acked → no-op
        assert await inbox.pending_count(SID) == 0

    async def test_inbox_07_sessions_are_isolated(self, inbox: Inbox) -> None:
        await inbox.append(SID, work_email('a@e.example'))
        assert await inbox.pending_count(OTHER_SID) == 0
        assert len(await inbox.consume(OTHER_SID)) == 0

    async def test_inbox_08_wait_for_entry_wakes_on_append_and_delivers_nothing(self, inbox: Inbox) -> None:
        waiter = asyncio.create_task(inbox.wait_for_entry(SID))
        await asyncio.sleep(0)  # let the waiter start (it must wake even if not yet subscribed — see the port)
        await inbox.append(SID, work_email('a@e.example'))
        await asyncio.wait_for(waiter, timeout=5.0)  # resolves promptly on the push; 5s is a safety net
        # The wakeup is a nudge, not delivery — consumption is still explicit.
        assert len(await inbox.consume(SID)) == 1

    async def test_inbox_10_wait_for_entry_returns_immediately_when_pending(self, inbox: Inbox) -> None:
        await inbox.append(SID, work_email('a@e.example'))
        await asyncio.wait_for(inbox.wait_for_entry(SID), timeout=5.0)  # already-pending entries never wait

    async def test_inbox_09_redelivery_count_is_visible(self, inbox: Inbox) -> None:
        await inbox.append(SID, work_email('a@e.example'))
        await inbox.consume(SID)
        await inbox.reclaim(SID)
        reclaimed = await inbox.reclaim(SID)
        assert reclaimed[0].delivery_count == 3

    async def test_inbox_11_poison_entry_is_delivered_as_poison_and_peers_still_flow(
        self, inbox: Inbox, append_raw: AppendRaw
    ) -> None:
        first = await inbox.append(SID, work_email('a@e.example'))
        poison = await append_raw(SID, 'not-json{')
        second = await inbox.append(SID, personal_email('p@e.example'))
        delivered = await inbox.consume(SID)
        assert [entry.entry_id for entry in delivered] == [first, poison, second]
        assert {entry.entry_id: type(entry) for entry in delivered} == {
            first: InboxEntry,
            poison: PoisonInboxEntry,
            second: InboxEntry,
        }

    async def test_inbox_11a_poison_entry_is_re_presented_on_reclaim(
        self, inbox: Inbox, append_raw: AppendRaw
    ) -> None:
        poison = await append_raw(SID, 'not-json{')
        (delivered,) = await inbox.consume(SID)
        assert isinstance(delivered, PoisonInboxEntry)
        (reclaimed,) = await inbox.reclaim(SID)  # a crashed consumer's resume sees the same poison
        assert isinstance(reclaimed, PoisonInboxEntry)
        assert reclaimed.entry_id == poison
        assert reclaimed.delivery_count == 2
        assert reclaimed.raw_payload == 'not-json{'  # kept for inspection

    async def test_inbox_12_unknown_datapoint_type_propagates_from_consume_and_reclaim(
        self, inbox: Inbox, append_raw: AppendRaw
    ) -> None:
        # Poison is reserved for bad wire bytes. A well-formed payload whose DataPoint type this
        # deployment does not know must fail fast — a quiet quarantine would hide a parser/registry
        # regression — and stay deliverable, so redelivery on resume lets a newer deployment parse it.
        await append_raw(SID, NEWER_DEPLOY_PAYLOAD)
        with pytest.raises(UnknownDataPointTypeError):
            await inbox.consume(SID)
        with pytest.raises(UnknownDataPointTypeError):
            await inbox.reclaim(SID)  # the claimed entry is re-presented, never quarantined
        assert await inbox.quarantined(SID) == ()
        assert await inbox.pending_count(SID) == 1  # still there for a newer deployment to apply

    async def test_inbox_12a_validation_failure_of_a_known_type_propagates(
        self, inbox: Inbox, append_raw: AppendRaw
    ) -> None:
        await append_raw(SID, MALFORMED_KNOWN_TYPE_PAYLOAD)
        with pytest.raises(ValidationError):
            await inbox.consume(SID)
        assert await inbox.quarantined(SID) == ()  # semantic failures are never quarantined as poison

    async def test_inbox_13_quarantine_removes_from_delivery_and_records(self, inbox: Inbox) -> None:
        entry_id = await inbox.append(SID, work_email('a@e.example'))
        await inbox.consume(SID)
        await inbox.quarantine(SID, entry_id, reason='apply kept failing', epoch=Epoch(1))
        assert await inbox.pending_count(SID) == 0
        assert await inbox.reclaim(SID) == ()  # quarantined entries are never re-presented
        (record,) = await inbox.quarantined(SID)
        assert record.entry_id == entry_id
        assert record.reason == 'apply kept failing'
        assert record.delivery_count == 1
        assert record.raw_payload is not None  # the original wire payload stays inspectable

    async def test_inbox_14_quarantine_is_epoch_guarded(self, inbox: Inbox) -> None:
        entry_id = await inbox.append(SID, work_email('a@e.example'))
        await inbox.consume(SID)
        await inbox.ack(SID, 'no-such-entry', epoch=Epoch(2))  # a successor has bumped the inbox epoch
        with pytest.raises(StaleEpochError):
            await inbox.quarantine(SID, entry_id, reason='from a fenced predecessor', epoch=Epoch(1))
        assert await inbox.pending_count(SID) == 1  # the rejected quarantine removed nothing
        assert await inbox.quarantined(SID) == ()

    async def test_inbox_15_quarantine_records_are_per_session(self, inbox: Inbox) -> None:
        entry_id = await inbox.append(SID, work_email('a@e.example'))
        await inbox.consume(SID)
        await inbox.quarantine(SID, entry_id, reason='poison', epoch=Epoch(1))
        assert await inbox.quarantined(OTHER_SID) == ()

    async def test_inbox_16_ack_of_a_never_consumed_entry_leaves_it_deliverable(self, inbox: Inbox) -> None:
        # Redis XACK only removes claimed (pending) entries; an ack racing ahead of delivery must
        # be a no-op on every adapter, never destroy the entry.
        entry_id = await inbox.append(SID, work_email('a@e.example'))
        await inbox.ack(SID, entry_id, epoch=Epoch(1))
        delivered = await inbox.consume(SID)
        assert [entry.entry_id for entry in delivered] == [entry_id]

    async def test_inbox_17_quarantine_of_a_never_consumed_entry_is_a_noop(self, inbox: Inbox) -> None:
        # Removal-plus-record is gated on the entry actually being claimed (the Redis script only
        # records when the XACK removed a pending entry) — so nothing is recorded here either.
        entry_id = await inbox.append(SID, work_email('a@e.example'))
        await inbox.quarantine(SID, entry_id, reason='premature', epoch=Epoch(1))
        assert await inbox.quarantined(SID) == ()
        delivered = await inbox.consume(SID)
        assert [entry.entry_id for entry in delivered] == [entry_id]  # still deliverable

    async def test_inbox_18_bounded_consume_claims_at_most_max_entries_in_order(self, inbox: Inbox) -> None:
        # Bounded draining is the inbox's backpressure: a consumer pulls a capped batch so one apply
        # can never swallow an arbitrarily large backlog. max_entries=0 (Redis maps COUNT 0 to
        # "unbounded", so this is a genuine adapter-parity hazard) must claim NOTHING and leave every
        # entry redeliverable; max_entries=1 claims exactly the first in append order, leaving the
        # rest for the next call; a cap >= the backlog drains the remainder.
        first = await inbox.append(SID, work_email('a@e.example'))
        second = await inbox.append(SID, personal_email('p@e.example'))
        third = await inbox.append(SID, work_email('c@e.example'))

        assert await inbox.consume(SID, max_entries=0) == ()  # COUNT 0 ≠ unbounded — claims nothing
        assert await inbox.pending_count(SID) == 3  # every entry still deliverable

        (claimed_first,) = await inbox.consume(SID, max_entries=1)
        assert claimed_first.entry_id == first  # exactly the first, in append order
        (claimed_second,) = await inbox.consume(SID, max_entries=1)
        assert claimed_second.entry_id == second  # the next call resumes after it, not over-claiming

        remaining = await inbox.consume(SID, max_entries=10)  # cap >= remaining backlog drains the rest
        assert [entry.entry_id for entry in remaining] == [third]

    async def test_inbox_19_bounded_consume_with_nonpositive_cap_claims_nothing(self, inbox: Inbox) -> None:
        # The whole non-positive range is the empty-claim case (the in-memory cap check and the Redis
        # COUNT<=0 special-case must agree), and a rejected claim leaves the backlog intact.
        await inbox.append(SID, work_email('a@e.example'))
        assert await inbox.consume(SID, max_entries=-1) == ()
        assert await inbox.pending_count(SID) == 1  # nothing was claimed by the negative cap

    async def test_inbox_20_quarantined_records_preserved_in_quarantine_order(
        self, inbox: Inbox, append_raw: AppendRaw
    ) -> None:
        # Operators re-drive by reading the quarantine list, so its order and per-record fields must
        # be faithful: a reversed list, an off-by-one range read, or a shared delivery_count/payload
        # would corrupt the inspection record. Quarantine poison FIRST then a valid entry, and assert
        # both come back in that order, each carrying its own reason and raw payload (the poison's
        # malformed bytes vs the valid one's JSON).
        poison = await append_raw(SID, 'not-json{')
        valid = await inbox.append(SID, work_email('a@e.example'))
        delivered = await inbox.consume(SID)  # claim both so quarantine can dispose them
        assert {entry.entry_id for entry in delivered} == {poison, valid}

        await inbox.quarantine(SID, poison, reason='poison bytes', epoch=Epoch(1))
        await inbox.quarantine(SID, valid, reason='apply kept failing', epoch=Epoch(1))

        records = await inbox.quarantined(SID)
        assert [record.entry_id for record in records] == [poison, valid]  # quarantine order, not reversed
        assert [record.reason for record in records] == ['poison bytes', 'apply kept failing']
        assert records[0].raw_payload == 'not-json{'  # the poison's own malformed bytes
        assert records[1].raw_payload is not None and 'a@e.example' in records[1].raw_payload


class CooldownGateConformance:
    """Contract for ``CooldownGate`` adapters: atomic check-and-arm, durable per-key expiry."""

    async def test_gate_01_first_acquire_wins_and_arms(self, gate: CooldownGate) -> None:
        assert await gate.try_acquire('k', 60.0) is True
        assert await gate.try_acquire('k', 60.0) is False  # armed by the first call — no check-then-act gap

    async def test_gate_02_reopens_after_the_cooldown_elapses(
        self, gate: CooldownGate, advance_time: Callable[[float], None]
    ) -> None:
        assert await gate.try_acquire('k', 60.0) is True
        advance_time(61.0)
        assert await gate.try_acquire('k', 60.0) is True

    async def test_gate_03_distinct_keys_are_independent(self, gate: CooldownGate) -> None:
        assert await gate.try_acquire('k1', 60.0) is True
        assert await gate.try_acquire('k2', 60.0) is True


class LockConformance:
    async def test_lock_01_acquire_grants_lock_and_mints_higher_epoch(self, lock: SessionLock) -> None:
        first = await lock.acquire(SID)
        assert await lock.is_held(SID)
        assert first > 0

    async def test_lock_03_lease_expires_after_ttl(
        self, lock: SessionLock, advance_time: Callable[[float], None]
    ) -> None:
        await lock.acquire(SID)
        advance_time(31.0)
        assert not await lock.is_held(SID)
        second = await lock.acquire(SID)  # a successor can take over
        assert second > 1

    async def test_lock_03_renew_extends_lease(self, lock: SessionLock, advance_time: Callable[[float], None]) -> None:
        epoch = await lock.acquire(SID)
        advance_time(20.0)
        await lock.renew(SID, epoch=epoch)
        advance_time(20.0)  # 40s total, but renewed at 20s
        assert await lock.is_held(SID)

    async def test_lock_04_epochs_strictly_increase_across_grants(
        self, lock: SessionLock, advance_time: Callable[[float], None]
    ) -> None:
        epochs = []
        for _ in range(3):
            epochs.append(int(await lock.acquire(SID)))
            advance_time(31.0)  # let the lease expire so the next acquire succeeds
        assert epochs == sorted(set(epochs)) and len(set(epochs)) == 3

    async def test_lock_05_mutual_exclusion_while_live(self, lock: SessionLock) -> None:
        await lock.acquire(SID)
        with pytest.raises(LockHeldError):
            await lock.acquire(SID)

    async def test_lock_06_the_mint_advances_only_on_a_successful_acquire(self, lock: SessionLock) -> None:
        # A minted epoch means the lock was held at some point, so epoch 0 means "never started" — the only
        # signal separating a never-started session from one whose owner died (both show no live lease).
        # Callers rely on that (the manager refuses to resume an unstarted session), which a mint advanced by
        # a *failed* acquire would break: an acquire in progress would read as a session already started.
        assert int(await lock.current_epoch(SID)) == 0
        epoch = await lock.acquire(SID)
        with pytest.raises(LockHeldError):
            await lock.acquire(SID)
        assert int(await lock.current_epoch(SID)) == int(epoch)

    async def test_lock_07_release_frees_for_reuse(self, lock: SessionLock) -> None:
        epoch = await lock.acquire(SID)
        await lock.release(SID, epoch=epoch)
        assert not await lock.is_held(SID)
        assert int(await lock.acquire(SID)) > int(epoch)

    async def test_lock_completion_marked_and_idempotent(self, lock: SessionLock) -> None:
        assert await lock.is_complete(SID) is False
        epoch = await lock.acquire(SID)
        await lock.mark_complete(SID, epoch=epoch)
        assert await lock.is_complete(SID) is True
        await lock.mark_complete(SID, epoch=epoch)  # re-marking under the same epoch is a no-op
        assert await lock.is_complete(SID) is True

    async def test_lock_completion_is_per_session(self, lock: SessionLock) -> None:
        epoch = await lock.acquire(SID)
        await lock.mark_complete(SID, epoch=epoch)
        assert await lock.is_complete(OTHER_SID) is False

    async def test_lock_completion_fenced_against_takeover(
        self, lock: SessionLock, advance_time: Callable[[float], None]
    ) -> None:
        stale = await lock.acquire(SID)
        advance_time(31.0)  # the predecessor's lease expires
        await lock.acquire(SID)  # a successor takes over, minting a higher epoch
        with pytest.raises(StaleEpochError):
            await lock.mark_complete(SID, epoch=stale)  # the fenced predecessor cannot finalize
        assert await lock.is_complete(SID) is False
        await lock.mark_complete(SID, epoch=await lock.current_epoch(SID))  # the current holder marks it complete
        assert await lock.is_complete(SID) is True

    async def test_lock_clear_complete_allows_reopen(self, lock: SessionLock) -> None:
        # mark_complete → is_complete True; clear_complete → is_complete False (re-openable).
        # The epoch counter is NOT reset: a subsequent acquire mints a strictly higher epoch so the
        # re-open write is fenced against any stale predecessor.
        epoch = await lock.acquire(SID)
        await lock.mark_complete(SID, epoch=epoch)
        assert await lock.is_complete(SID) is True
        await lock.release(SID, epoch=epoch)

        await lock.clear_complete(SID)
        assert await lock.is_complete(SID) is False

        # The next acquire must still mint a strictly higher epoch — fencing is intact after clear.
        new_epoch = await lock.acquire(SID)
        assert int(new_epoch) > int(epoch)

    async def test_lock_clear_complete_is_idempotent(self, lock: SessionLock) -> None:
        # Clearing a session that was never completed (or already cleared) is a no-op.
        await lock.clear_complete(SID)
        assert await lock.is_complete(SID) is False  # no error, still false


class AuditSinkConformance:
    @staticmethod
    def _entry(epoch: int, *, kind: AuditKind = AuditKind.DATA_POINT_ADDED) -> AuditLogEntry:
        return AuditLogEntry(session_id=SID, epoch=Epoch(epoch), timestamp=T0, kind=kind, operator_id=OP)

    async def test_audit_02_appends_replay_in_order(self, audit: AuditSink) -> None:
        for _ in range(3):
            await audit.append(self._entry(1))
        assert len(await audit.replay(SID)) == 3

    async def test_audit_05_an_appended_entry_is_replayable_with_no_further_step(self, audit: AuditSink) -> None:
        # The crash-visibility contract, and the reason the sink commits at append: a session that dies
        # HERE — and is never resumed, so no recovery pass ever runs on its behalf — must still have its
        # trail. Only append() has run, so anything replay() cannot see now is lost forever.
        await audit.append(self._entry(1))
        assert len(await audit.replay(SID)) == 1

    async def test_audit_06_stale_epoch_entry_rejected(self, audit: AuditSink) -> None:
        await audit.append(self._entry(2))
        with pytest.raises(StaleEpochError):
            await audit.append(self._entry(1))
        assert len(await audit.replay(SID)) == 1

    async def test_audit_08_n_events_produce_n_documents(self, audit: AuditSink) -> None:
        for _ in range(5):
            await audit.append(self._entry(1))
        assert len(await audit.replay(SID)) == 5

    async def test_audit_09_append_many_is_observably_n_appends(self, audit: AuditSink) -> None:
        await audit.append(self._entry(1, kind=AuditKind.OPERATOR_INVOKED))
        await audit.append_many([self._entry(1), self._entry(1, kind=AuditKind.CAPABILITY_ACTIVATED)])
        assert [entry.kind for entry in await audit.replay(SID)] == [
            AuditKind.OPERATOR_INVOKED,
            AuditKind.DATA_POINT_ADDED,
            AuditKind.CAPABILITY_ACTIVATED,
        ]  # per-event granularity, in append order, interleaved correctly with single appends

    async def test_audit_10_append_many_stale_epoch_rejects_the_batch_writing_nothing(self, audit: AuditSink) -> None:
        await audit.append(self._entry(2))
        with pytest.raises(StaleEpochError):
            await audit.append_many([self._entry(1), self._entry(1)])
        assert len(await audit.replay(SID)) == 1  # only the pre-existing entry landed


class DurableStoreConformance:
    async def test_durable_occ_insert_and_read(self, durable: DurableStore) -> None:
        assert await durable.upsert(TBL, 'k', {'a': 1}, expected_version=0, epoch=Epoch(1)) == 1
        document = await durable.read(TBL, 'k')
        assert document is not None and document.version == 1 and document.document == {'a': 1}

    async def test_durable_occ_version_guarded_update(self, durable: DurableStore) -> None:
        await durable.upsert(TBL, 'k', {'a': 1}, expected_version=0, epoch=Epoch(1))
        assert await durable.upsert(TBL, 'k', {'a': 2}, expected_version=1, epoch=Epoch(1)) == 2

    async def test_durable_occ_conflict_on_stale_version(self, durable: DurableStore) -> None:
        await durable.upsert(TBL, 'k', {'a': 1}, expected_version=0, epoch=Epoch(1))
        with pytest.raises(OptimisticConcurrencyError):
            await durable.upsert(TBL, 'k', {'a': 9}, expected_version=0, epoch=Epoch(1))

    async def test_durable_table_routes_destination(self, durable: DurableStore) -> None:
        # The same key in two different tables is two independent documents (destination by table).
        await durable.upsert('reports', 'k', {'a': 1}, expected_version=0, epoch=Epoch(1))
        await durable.upsert('profiles', 'k', {'a': 2}, expected_version=0, epoch=Epoch(1))
        report = await durable.read('reports', 'k')
        profile = await durable.read('profiles', 'k')
        assert report is not None and report.document == {'a': 1}
        assert profile is not None and profile.document == {'a': 2}

    async def test_durable_add_to_set_is_idempotent(self, durable: DurableStore) -> None:
        await durable.add_to_set(TBL, 'agg', 'sessions', 'session-1', epoch=Epoch(1))
        assert await durable.add_to_set(TBL, 'agg', 'sessions', 'session-1', epoch=Epoch(1)) == 1

    async def test_durable_contribution_marked_once(self, durable: DurableStore) -> None:
        assert await durable.mark_contribution(SID, OP, epoch=Epoch(1)) is True
        assert await durable.mark_contribution(SID, OP, epoch=Epoch(1)) is False
        assert await durable.is_contribution_marked(SID, OP)

    async def test_durable_stale_epoch_rejected(self, durable: DurableStore) -> None:
        await durable.upsert(TBL, 'k', {'a': 1}, expected_version=0, epoch=Epoch(2))
        with pytest.raises(StaleEpochError):
            await durable.upsert(TBL, 'k', {'a': 2}, expected_version=1, epoch=Epoch(1))

    async def test_durable_contribution_epoch_fenced(self, durable: DurableStore) -> None:
        # A fenced predecessor (lower epoch) cannot record a contribution after a higher epoch took over.
        # The stale call uses a different operator that never marked, so it is the epoch fence — not the
        # at-most-once dedup — that rejects it.
        assert await durable.mark_contribution(SID, OP, epoch=Epoch(2)) is True
        with pytest.raises(StaleEpochError):
            await durable.mark_contribution(SID, OperatorId('cnf-fenced-op'), epoch=Epoch(1))

    async def test_durable_add_to_set_grows_cardinality(self, durable: DurableStore) -> None:
        # The dedup half (a re-added member keeps the size) is locked elsewhere; this locks the
        # counting half: a distinct member grows the set and the returned cardinality reflects the
        # true set size (an aggregator reads this to know it is the Nth contributor). A regression
        # that overwrote the set, returned a stale length, or always returned 1 would still pass the
        # idempotency test but fail here.
        assert await durable.add_to_set(TBL, 'agg', 'sessions', 'v1', epoch=Epoch(1)) == 1  # absent → first member
        assert await durable.add_to_set(TBL, 'agg', 'sessions', 'v2', epoch=Epoch(1)) == 2  # union grows
        assert await durable.add_to_set(TBL, 'agg', 'sessions', 'v1', epoch=Epoch(1)) == 2  # re-add does not regrow

    async def test_durable_add_to_set_is_epoch_fenced(self, durable: DurableStore) -> None:
        # add_to_set is a mutating write path, so it must be epoch-fenced exactly like upsert: a
        # superseded predecessor must not be able to mutate a set member after a higher epoch wrote
        # the same key. Without this, a fenced writer could split-brain the durable aggregate.
        await durable.add_to_set(TBL, 'k', 'sessions', 'v1', epoch=Epoch(2))
        with pytest.raises(StaleEpochError):
            await durable.add_to_set(TBL, 'k', 'sessions', 'v2', epoch=Epoch(1))
        # The rejected write left the set untouched (still a single member).
        assert await durable.add_to_set(TBL, 'k', 'sessions', 'v1', epoch=Epoch(2)) == 1

    async def test_durable_add_to_set_shares_the_upsert_fence_scope(self, durable: DurableStore) -> None:
        # add_to_set and upsert mutate the same durable (table, key) — they must share one epoch
        # fence so neither can be superseded behind the other's back. A prior add_to_set at epoch 2
        # fences a lower-epoch upsert to the same key, and vice-versa: the two paths advance and
        # consult one fence per (table, key), not two independent ones. (This pins the cross-adapter
        # decision: the in-memory adapter fences per (table, key) and Mongo must match, so a set-doc
        # write and a versioned-doc write to the same key cannot diverge.)
        await durable.add_to_set(TBL, 'k', 'sessions', 'v1', epoch=Epoch(2))
        with pytest.raises(StaleEpochError):
            await durable.upsert(TBL, 'k', {'a': 1}, expected_version=0, epoch=Epoch(1))
        await durable.upsert(TBL, 'other', {'a': 1}, expected_version=0, epoch=Epoch(2))
        with pytest.raises(StaleEpochError):
            await durable.add_to_set(TBL, 'other', 'sessions', 'v2', epoch=Epoch(1))

    async def test_durable_add_to_set_fence_spans_distinct_fields_of_one_key(self, durable: DurableStore) -> None:
        # The fence scope is the whole (table, key), not (table, key, field): once a higher epoch has
        # mutated ANY set on a key, a lower-epoch write to a DIFFERENT field of that key is still
        # superseded. This is the stricter, split-brain-free semantics both adapters must agree on —
        # a per-field fence would let a fenced predecessor keep writing to sibling fields undetected.
        await durable.add_to_set(TBL, 'agg', 'sessions', 'v1', epoch=Epoch(2))
        with pytest.raises(StaleEpochError):
            await durable.add_to_set(TBL, 'agg', 'namespaces', 'namespace-1', epoch=Epoch(1))

    async def test_durable_upsert_advances_epoch_and_fences_strictly_lower_only(self, durable: DurableStore) -> None:
        # Monotonic fencing epoch through a SUCCESSFUL versioned update: the stored epoch must move
        # forward on every accepted write, then reject any strictly-newer-was-seen writer while still
        # admitting a same-epoch resume (the boundary is >, not >=).
        assert await durable.upsert(TBL, 'k', {'a': 1}, expected_version=0, epoch=Epoch(2)) == 1
        assert await durable.upsert(TBL, 'k', {'a': 2}, expected_version=1, epoch=Epoch(3)) == 2  # epoch advances to 3
        with pytest.raises(StaleEpochError):  # strictly lower than the bumped epoch → fenced
            await durable.upsert(TBL, 'k', {'a': 3}, expected_version=2, epoch=Epoch(2))
        # The equal-epoch boundary still succeeds: a same-epoch resume after the advance is not stale.
        assert await durable.upsert(TBL, 'k', {'a': 4}, expected_version=2, epoch=Epoch(3)) == 3

    async def test_durable_remark_under_higher_epoch_is_false_and_advances_the_fence(
        self, durable: DurableStore
    ) -> None:
        # Re-marking an already-contributed operator is a stable False regardless of a forward epoch
        # move (at-most-once dedup, NOT a fence rejection). The higher-epoch re-mark still advances
        # the session contribution fence, so a later lower-epoch marker for a DIFFERENT operator is
        # then rejected as stale — proving the fence tracks the highest epoch seen across mark attempts.
        assert await durable.mark_contribution(SID, OP, epoch=Epoch(1)) is True
        assert await durable.mark_contribution(SID, OP, epoch=Epoch(2)) is False  # dedup, not a StaleEpochError
        with pytest.raises(StaleEpochError):  # the epoch-2 re-mark advanced the session fence past 1
            await durable.mark_contribution(SID, OperatorId('cnf-other-op'), epoch=Epoch(1))

    async def test_durable_status_metadata_round_trips(self, durable: DurableStore) -> None:
        # status/updated_at ride the record (next to version/epoch), not the business document.
        stamp = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        await durable.upsert(
            TBL, 'k', {'a': 1}, expected_version=0, epoch=Epoch(1), status='in_progress', updated_at=stamp
        )
        document = await durable.read(TBL, 'k')
        assert document is not None
        assert document.document == {'a': 1}  # the curated doc is untouched by stamping
        assert document.status == 'in_progress'
        assert document.updated_at == stamp

    async def test_durable_status_metadata_absent_when_not_supplied(self, durable: DurableStore) -> None:
        await durable.upsert(TBL, 'k', {'a': 1}, expected_version=0, epoch=Epoch(1))
        document = await durable.read(TBL, 'k')
        assert document is not None and document.status is None and document.updated_at is None

    async def test_durable_status_re_stamps_on_the_update_path(self, durable: DurableStore) -> None:
        # The interim→final transition is a SECOND upsert to the same key (the version-guarded UPDATE
        # path, distinct from the first INSERT). The finalize re-run must flip status to 'final' and
        # re-stamp updated_at — verified on every backend, since in-memory collapses insert/update into
        # one assignment and so cannot catch a backend-specific update-path regression.
        first = datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc)
        second = datetime(2026, 1, 1, 10, 30, tzinfo=timezone.utc)
        await durable.upsert(
            TBL, 'k', {'a': 1}, expected_version=0, epoch=Epoch(1), status='in_progress', updated_at=first
        )
        await durable.upsert(TBL, 'k', {'a': 2}, expected_version=1, epoch=Epoch(1), status='final', updated_at=second)
        document = await durable.read(TBL, 'k')
        assert document is not None and document.version == 2 and document.document == {'a': 2}
        assert document.status == 'final'
        assert document.updated_at == second  # re-stamped, tz-aware, on the update path

    async def test_durable_add_to_set_record_carries_no_status_metadata(self, durable: DurableStore) -> None:
        # add_to_set is membership-only: it writes a separate set record, never a versioned doc, so a
        # set-shaped aggregate exposes no status/updated_at — only upsert stamps the status envelope.
        await durable.add_to_set(TBL, 'agg', 'sessions', 'session-1', epoch=Epoch(1))
        assert await durable.read(TBL, 'agg') is None

    async def test_durable_add_to_set_does_not_disturb_versioned_doc_status(self, durable: DurableStore) -> None:
        # A per-key curated doc plus a contributor set on the SAME key share the (table, key) epoch fence,
        # but add_to_set must never rewrite the versioned doc's document/version/status/updated_at.
        stamp = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        await durable.upsert(
            TBL, 'k', {'a': 1}, expected_version=0, epoch=Epoch(1), status='in_progress', updated_at=stamp
        )
        await durable.add_to_set(TBL, 'k', 'sessions', 'v1', epoch=Epoch(1))
        document = await durable.read(TBL, 'k')
        assert document is not None and document.document == {'a': 1} and document.version == 1
        assert document.status == 'in_progress' and document.updated_at == stamp


class DataPointArchiveConformance:
    @staticmethod
    def _entry(data_point: BaseDataPoint[Any] | None = None, *, epoch: int = 1) -> ArchivedDataPoint:
        return ArchivedDataPoint.from_data_point(
            data_point or risk(0.5), session_id=SID, namespace_id=NAMESPACE, epoch=Epoch(epoch)
        )

    async def test_arch_01_first_observation_archived_as_one_document(self, archive: DataPointArchive) -> None:
        await archive.archive(self._entry(work_email('a@e.example')))
        await archive.flush(SID)
        committed = await archive.read(SID)
        assert len(committed) == 1
        assert committed[0].value == 'a@e.example'
        assert committed[0].first_retrieved == committed[0].last_retrieved  # first sighting

    async def test_arch_02_reobservation_upserts_bumps_last_keeps_first(self, archive: DataPointArchive) -> None:
        await archive.archive(self._entry(work_email('a@e.example', first=T0, last=T0)))
        await archive.archive(self._entry(work_email('a@e.example', first=T0, last=T2)))
        await archive.flush(SID)
        committed = await archive.read(SID)
        assert len(committed) == 1  # keyed-upsert — no duplicate
        assert committed[0].first_retrieved == T0  # immutable
        assert committed[0].last_retrieved == T2  # bumped

    async def test_arch_14_reobservation_advances_the_stored_epoch(self, archive: DataPointArchive) -> None:
        # The stored epoch says which epoch last SAW this datapoint, not which one first recorded it.
        # A row frozen at its first sighting reads as older than the session that actually produced it,
        # which matters wherever the archive is compared against a session's own epoch. Both adapters
        # have to agree: the read-equals-post-flush property cannot catch a difference here, because
        # each one's fold mirrors its own flush, so a divergence stays green until something reads the
        # epoch and gets a different answer per backend.
        await archive.archive(self._entry(work_email('a@e.example', first=T0, last=T0), epoch=1))
        await archive.archive(self._entry(work_email('a@e.example', first=T0, last=T2), epoch=2))

        # Asserted on BOTH sides of the flush on purpose. A read before it folds the buffer in code,
        # a read after it reflects the merge the datastore performed, and the two are separate
        # implementations — checking only the post-flush read leaves whichever one the buffer uses
        # free to disagree.
        buffered = await archive.read(SID)
        assert len(buffered) == 1
        assert int(buffered[0].epoch) == 2

        await archive.flush(SID)

        committed = await archive.read(SID)
        assert len(committed) == 1  # same identity, so still one row
        assert int(committed[0].epoch) == 2
        assert committed[0].first_retrieved == T0  # unchanged by the epoch advancing

    # `$max` refusing to lower a stored epoch has no test here on purpose: the meta CAS
    # (`max_epoch: {'$lte': current_epoch}`) rejects an epoch below the session's high water mark, so
    # an older-epoch re-observation can never be buffered and no sequence of port calls can tell
    # `max(existing, sealed)` apart from last-writer-wins. Each adapter pins it on its own `_fold_in`
    # instead, which is where an out-of-order pair can actually be constructed.

    async def test_arch_04_write_goes_through_buffer_not_inline(self, archive: DataPointArchive) -> None:
        await archive.archive(self._entry())
        # buffered_count is what proves the write stayed off the hot path — read() cannot, because it
        # is buffer-transparent by contract and would report the entry either way.
        assert await archive.buffered_count(SID) == 1
        assert await archive.flush(SID) == 1
        assert await archive.buffered_count(SID) == 0  # committed, so no longer buffered
        assert len(await archive.read(SID)) == 1

    async def test_arch_05_buffered_entry_is_readable_before_any_flush(self, archive: DataPointArchive) -> None:
        # A session that dies before its flush must not go invisible: read folds the buffer over the
        # committed rows, so the entry is readable on either side of the flush rather than only after.
        await archive.archive(self._entry())
        before = await archive.read(SID)
        assert len(before) == 1
        await archive.flush(SID)  # resume → drain
        assert await archive.read(SID) == before

    async def test_arch_06_stale_epoch_archive_write_rejected(self, archive: DataPointArchive) -> None:
        await archive.archive(self._entry(epoch=2))
        with pytest.raises(StaleEpochError):
            await archive.archive(self._entry(epoch=1))
        await archive.flush(SID)
        assert len(await archive.read(SID)) == 1

    async def test_arch_08_distinct_identities_produce_distinct_documents(self, archive: DataPointArchive) -> None:
        await archive.archive(self._entry(work_email('a@e.example')))
        await archive.archive(self._entry(work_email('b@e.example')))
        await archive.archive(self._entry(personal_email('p@e.example')))
        await archive.flush(SID)
        assert len(await archive.read(SID)) == 3  # batched flush keeps per-identity granularity

    async def test_arch_10_redelivery_of_same_identity_is_idempotent(self, archive: DataPointArchive) -> None:
        for _ in range(3):  # at-least-once redelivery / operator reruns of the same value
            await archive.archive(self._entry(work_email('a@e.example')))
        await archive.flush(SID)
        assert len(await archive.read(SID)) == 1

    async def test_arch_11_archive_many_is_observably_n_archives(self, archive: DataPointArchive) -> None:
        await archive.archive_many(
            [
                self._entry(work_email('a@e.example', first=T0, last=T0)),
                self._entry(work_email('b@e.example')),
                self._entry(work_email('a@e.example', first=T0, last=T2)),  # re-observation in the same batch
            ]
        )
        assert await archive.buffered_count(SID) == 3  # per-observation granularity in the buffer
        assert await archive.flush(SID) == 3
        committed = {entry.value: entry for entry in await archive.read(SID)}
        assert set(committed) == {'a@e.example', 'b@e.example'}  # the keyed-upsert fold still dedups
        assert committed['a@e.example'].last_retrieved == T2  # and the in-batch re-observation bumped last

    async def test_arch_13_read_is_identical_either_side_of_a_flush(self, archive: DataPointArchive) -> None:
        """The fold a read applies must agree with the keyed-upsert a flush performs.

        read() folds buffered rows over committed ones itself, which is a second expression of the
        merge flush carries out in the datastore. This pins the two together: a field the fold forgets
        to advance, or an ordering difference, shows up as these two reads disagreeing.
        """
        # Deliberately spans the boundary: one identity is already committed AND re-observed in the
        # buffer, which is the case where the two merges could diverge.
        await archive.archive(self._entry(work_email('a@e.example', first=T0, last=T0)))
        await archive.archive(self._entry(work_email('b@e.example')))
        await archive.flush(SID)
        await archive.archive(self._entry(work_email('a@e.example', first=T0, last=T2)))
        await archive.archive(self._entry(personal_email('p@e.example')))

        before = await archive.read(SID)
        assert {entry.value for entry in before} == {'a@e.example', 'b@e.example', 'p@e.example'}
        assert {entry.value: entry.last_retrieved for entry in before}['a@e.example'] == T2

        assert await archive.flush(SID) == 2
        assert await archive.read(SID) == before

    async def test_arch_12_archive_many_stale_epoch_rejects_the_batch_with_nothing_buffered(
        self, archive: DataPointArchive
    ) -> None:
        await archive.archive(self._entry(epoch=2))
        with pytest.raises(StaleEpochError):
            await archive.archive_many([self._entry(work_email('x@e.example'), epoch=1)])
        assert await archive.buffered_count(SID) == 1  # only the pre-existing entry survived
        await archive.flush(SID)
        assert len(await archive.read(SID)) == 1
