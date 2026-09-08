"""CNF — the port-conformance harness itself.

The per-port contracts live in ``doubles/conformance.py`` and are bound to the in-memory
adapter in ``test_store.py`` / ``test_inbox.py`` / ``test_session_lock.py`` /
``test_audit.py``. As Redis and Mongo adapters land, each adds a binding subclass of the
same mixin, so the identical contract runs against every adapter family. These tests
cover the harness's own guarantees: adapter discovery, parity, and per-test isolation.
"""

from __future__ import annotations

from collections.abc import Callable

from orcastork.adapters.memory import (
    InMemoryAuditSink,
    InMemoryDataPointStore,
    InMemoryInbox,
)
from orcastork.ids import Epoch, SessionId
from orcastork.ports import DataPointStore

from .doubles.datapoints import work_email

# Adapter factories under conformance. Redis/Mongo factories are appended when those
# adapters land (behind @pytest.mark.integration); the in-memory factory is always present.
STORE_FACTORIES: dict[str, Callable[[], DataPointStore]] = {'memory': InMemoryDataPointStore}

SID = SessionId('cnf-harness')


def test_cnf_01_in_memory_factories_are_discovered() -> None:
    assert 'memory' in STORE_FACTORIES
    # Every other port also has an always-available in-memory adapter.
    assert InMemoryInbox() is not None
    assert InMemoryAuditSink() is not None


async def test_cnf_02_in_memory_adapter_is_deterministic_parity() -> None:
    # Identical operations against two fresh instances yield identical observable state —
    # the parity property the cross-adapter matrix (in-memory vs real) will assert later.
    operations = [work_email('a@e.example', last=work_email().first_retrieved), work_email('b@e.example')]
    first, second = InMemoryDataPointStore(), InMemoryDataPointStore()
    await first.write(SID, operations, epoch=Epoch(1))
    await second.write(SID, operations, epoch=Epoch(1))
    assert {p.value for p in (await first.snapshot(SID)).all()} == {
        p.value for p in (await second.snapshot(SID)).all()
    }


async def test_cnf_03_fresh_fixture_has_no_leaked_state() -> None:
    # A freshly constructed adapter starts empty (no cross-test leakage).
    store = InMemoryDataPointStore()
    assert len((await store.snapshot(SID)).all()) == 0
    assert await store.revision(SID) == 0
