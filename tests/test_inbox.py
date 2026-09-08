"""INBOX — Inbox contract, run against the in-memory adapter (via CNF mixin)."""

from __future__ import annotations

import pytest

from orcastork.adapters.memory import InMemoryInbox

from .doubles.conformance import AppendRaw, InboxConformance


class TestInMemoryInbox(InboxConformance):
    @pytest.fixture
    def inbox(self) -> InMemoryInbox:
        return InMemoryInbox()

    @pytest.fixture
    def append_raw(self, inbox: InMemoryInbox) -> AppendRaw:
        return inbox.append_serialized
