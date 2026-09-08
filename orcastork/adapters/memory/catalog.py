"""In-memory ``CapabilityCatalog`` — explicit permitted-set + credentials maps.

Enough to satisfy the port contract and the conformance suite; a deployment writes its own
adapter over its real configuration store. The mutators let tests model a namespace enabling/revoking a
capability between grants.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from ...ids import CapabilityId, NamespaceId, OperatorId


class InMemoryCapabilityCatalog:
    def __init__(
        self,
        permitted: Mapping[NamespaceId, Iterable[CapabilityId]] | None = None,
        credentials: Mapping[tuple[NamespaceId, CapabilityId], Mapping[str, Any]] | None = None,
        preferred: Mapping[NamespaceId, Sequence[CapabilityId]] | None = None,
        permitted_operators: Mapping[NamespaceId, Iterable[OperatorId]] | None = None,
    ) -> None:
        self._permitted: dict[NamespaceId, frozenset[CapabilityId]] = {
            namespace: frozenset(caps) for namespace, caps in (permitted or {}).items()
        }
        self._credentials: dict[tuple[NamespaceId, CapabilityId], Mapping[str, Any]] = {
            key: dict(value) for key, value in (credentials or {}).items()
        }
        self._preferred: dict[NamespaceId, tuple[CapabilityId, ...]] = {
            namespace: tuple(order) for namespace, order in (preferred or {}).items()
        }
        self._permitted_operators: dict[NamespaceId, frozenset[OperatorId]] = {
            namespace: frozenset(operators) for namespace, operators in (permitted_operators or {}).items()
        }

    async def permitted_capabilities(self, namespace_id: NamespaceId) -> frozenset[CapabilityId]:
        return self._permitted.get(namespace_id, frozenset())

    async def permitted_operators(self, namespace_id: NamespaceId) -> frozenset[OperatorId] | None:
        # A namespace with no configured restriction is unrestricted (None), per the port contract —
        # an empty frozenset is the explicit "run nothing" configuration, never the default.
        return self._permitted_operators.get(namespace_id)

    async def credentials(self, namespace_id: NamespaceId, capability_id: CapabilityId) -> Mapping[str, Any] | None:
        # Hand back a copy so a caller mutating the returned mapping cannot alter catalog state.
        stored = self._credentials.get((namespace_id, capability_id))
        return None if stored is None else dict(stored)

    async def preferred_order(self, namespace_id: NamespaceId) -> tuple[CapabilityId, ...]:
        return self._preferred.get(namespace_id, ())

    def set_permitted(self, namespace_id: NamespaceId, capabilities: Iterable[CapabilityId]) -> None:
        """Test helper: model a namespace config change (enable/revoke) visible to the next grant."""
        self._permitted[namespace_id] = frozenset(capabilities)

    def set_preferred_order(self, namespace_id: NamespaceId, order: Sequence[CapabilityId]) -> None:
        """Test helper: model a namespace changing its provider preference, visible to the next refresh."""
        self._preferred[namespace_id] = tuple(order)

    def set_permitted_operators(self, namespace_id: NamespaceId, operators: Iterable[OperatorId]) -> None:
        """Test helper: model a namespace config change to its operator gating, visible to the next grant."""
        self._permitted_operators[namespace_id] = frozenset(operators)

    def set_credentials(
        self, namespace_id: NamespaceId, capability_id: CapabilityId, credentials: Mapping[str, Any]
    ) -> None:
        self._credentials[(namespace_id, capability_id)] = dict(credentials)
