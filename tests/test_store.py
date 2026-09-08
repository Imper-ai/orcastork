"""STORE — DataPointStore contract, run against the in-memory adapter (via CNF mixin)."""

from __future__ import annotations

import pytest

from orcastork.adapters.memory import InMemoryDataPointStore
from orcastork.ports import DataPointStore

from .doubles.conformance import StoreConformance


class TestInMemoryStore(StoreConformance):
    @pytest.fixture
    def store(self) -> DataPointStore:
        return InMemoryDataPointStore()
