"""The ``CapabilityCatalog`` port — per-namespace permitted capabilities/operators + credentials.

Decides *what* a namespace may use and holds the secrets; availability and lazy activation are
the orchestrator's concern. A config change is visible to the next grant (enable/revoke).
The framework ships the port plus an in-memory adapter; a deployment backs it with wherever
its own configuration and secrets actually live.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from ..ids import CapabilityId, NamespaceId, OperatorId


class CapabilityCatalog(Protocol):
    async def permitted_capabilities(self, namespace_id: NamespaceId) -> frozenset[CapabilityId]:
        """The capability families the namespace is permitted to use."""
        ...

    async def permitted_operators(self, namespace_id: NamespaceId) -> frozenset[OperatorId] | None:
        """The operators the namespace may run, or ``None`` when the namespace declares no restriction.

        ``None`` — not an empty set — is the unrestricted default because an empty frozenset
        means the namespace may run *nothing*: most namespaces configure no operator restriction at all,
        and conflating "unconfigured" with "deny everything" would silently disable every
        unconfigured namespace's flows. Like capability permissions, a change is visible to the
        next grant, never to a run already in flight.
        """
        ...

    async def credentials(self, namespace_id: NamespaceId, capability_id: CapabilityId) -> Mapping[str, Any] | None:
        """Credentials for a capability, or ``None`` if not permitted / not configured."""
        ...

    async def preferred_order(self, namespace_id: NamespaceId) -> tuple[CapabilityId, ...]:
        """The namespace's provider preference ranking (may be empty); listed providers resolve first, in order."""
        ...
