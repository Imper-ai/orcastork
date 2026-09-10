"""``Capability`` — an injected provider of typed actions — and how it reaches an operator.

A capability declares the DataPoints it needs (``depends_on``) and the capabilities it builds on
(``requires``). It is **available** iff it is namespace-permitted (``CapabilityCatalog``), its
``depends_on`` are present (subtype-aware) and its ``requires`` are available — a fixpoint that
activates layered capabilities base-before-layer. A newly-available capability is **activated
lazily**, once, from catalog-supplied credentials; one that never becomes available is never
constructed. Operators get the activated instance from ``ctx.capabilities`` and call its methods
directly, fully typed.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, ClassVar, Protocol

from loguru import logger

from .datapoints import DataPoint, DataPointView
from .exceptions import CapabilityUnavailableError
from .ids import CapabilityId, NamespaceId, OperatorId


@dataclass(frozen=True)
class CapabilityContext:
    """What a capability sees when it activates: its credentials + the current DataPoints."""

    credentials: Mapping[str, Any]
    store: DataPointView


class Capability(ABC):
    capability_id: ClassVar[CapabilityId]
    depends_on: ClassVar[frozenset[type[DataPoint[Any]]]] = frozenset()
    requires: ClassVar[frozenset[type['Capability']]] = frozenset()

    @abstractmethod
    async def activate(self, ctx: CapabilityContext) -> None:
        """Build the underlying client from catalog-supplied credentials (called at most once per session)."""
        ...


class CapabilityCatalog(Protocol):
    """Per-namespace configuration: what a namespace may run, and the secrets its capabilities need."""

    async def permitted_capabilities(self, namespace_id: NamespaceId) -> frozenset[CapabilityId]:
        """The capabilities the namespace is permitted to use."""
        ...

    async def permitted_operators(self, namespace_id: NamespaceId) -> frozenset[OperatorId] | None:
        """The operators the namespace may run, or ``None`` when it declares no restriction.

        ``None`` — not an empty set — is the unrestricted default, because an empty frozenset means
        the namespace may run *nothing*, and conflating "unconfigured" with "deny everything" would
        silently disable every unconfigured namespace.
        """
        ...

    async def credentials(self, namespace_id: NamespaceId, capability_id: CapabilityId) -> Mapping[str, Any] | None:
        """Credentials for a capability, or ``None`` if not configured."""
        ...

    async def preferred_order(self, namespace_id: NamespaceId) -> tuple[CapabilityId, ...]:
        """The namespace's provider preference (may be empty); listed providers resolve first, in order."""
        ...


class InMemoryCapabilityCatalog:
    """Explicit permitted-set + credentials maps — the default catalog and the test substrate."""

    def __init__(
        self,
        permitted: Mapping[NamespaceId, Iterable[CapabilityId]] | None = None,
        credentials: Mapping[tuple[NamespaceId, CapabilityId], Mapping[str, Any]] | None = None,
        preferred: Mapping[NamespaceId, Sequence[CapabilityId]] | None = None,
        permitted_operators: Mapping[NamespaceId, Iterable[OperatorId]] | None = None,
    ) -> None:
        self._permitted = {namespace: frozenset(caps) for namespace, caps in (permitted or {}).items()}
        self._credentials = {key: dict(value) for key, value in (credentials or {}).items()}
        self._preferred = {namespace: tuple(order) for namespace, order in (preferred or {}).items()}
        self._permitted_operators = {
            namespace: frozenset(operators) for namespace, operators in (permitted_operators or {}).items()
        }

    async def permitted_capabilities(self, namespace_id: NamespaceId) -> frozenset[CapabilityId]:
        return self._permitted.get(namespace_id, frozenset())

    async def permitted_operators(self, namespace_id: NamespaceId) -> frozenset[OperatorId] | None:
        return self._permitted_operators.get(namespace_id)

    async def credentials(self, namespace_id: NamespaceId, capability_id: CapabilityId) -> Mapping[str, Any] | None:
        stored = self._credentials.get((namespace_id, capability_id))
        return None if stored is None else dict(stored)  # a copy, so a caller cannot alter catalog state

    async def preferred_order(self, namespace_id: NamespaceId) -> tuple[CapabilityId, ...]:
        return self._preferred.get(namespace_id, ())

    def set_permitted(self, namespace_id: NamespaceId, capabilities: Iterable[CapabilityId]) -> None:
        self._permitted[namespace_id] = frozenset(capabilities)

    def set_permitted_operators(self, namespace_id: NamespaceId, operators: Iterable[OperatorId]) -> None:
        self._permitted_operators[namespace_id] = frozenset(operators)

    def set_credentials(
        self, namespace_id: NamespaceId, capability_id: CapabilityId, credentials: Mapping[str, Any]
    ) -> None:
        self._credentials[(namespace_id, capability_id)] = dict(credentials)

    def set_preferred_order(self, namespace_id: NamespaceId, order: Sequence[CapabilityId]) -> None:
        self._preferred[namespace_id] = tuple(order)


class CapabilityView:
    """Read view over the currently-available capabilities (resolve → preferred provider)."""

    def __init__(
        self,
        available: Mapping[CapabilityId, Capability] | None = None,
        preference: tuple[CapabilityId, ...] = (),
    ) -> None:
        self._available: dict[CapabilityId, Capability] = dict(available) if available is not None else {}
        self._preference = preference

    def available_ids(self) -> frozenset[CapabilityId]:
        return frozenset(self._available)

    def available_types(self) -> frozenset[type[Capability]]:
        return frozenset(type(capability) for capability in self._available.values())

    def is_available(self, capability_id: CapabilityId) -> bool:
        return capability_id in self._available

    def resolve[C: Capability](self, capability_type: type[C]) -> C | None:
        """The single preferred available provider of ``capability_type``, or None.

        Providers in the namespace's preference order win, in listed order; unlisted providers rank
        after every listed one on a stable alphabetical tie-break by id.
        """
        matches = sorted(
            (cap for cap in self._available.values() if isinstance(cap, capability_type)),
            key=self._preference_rank,
        )
        return matches[0] if matches else None

    def _preference_rank(self, capability: Capability) -> tuple[int, CapabilityId]:
        if capability.capability_id in self._preference:
            return (self._preference.index(capability.capability_id), capability.capability_id)
        return (len(self._preference), capability.capability_id)

    def require[C: Capability](self, capability_type: type[C]) -> C:
        """Like :meth:`resolve`, but raise when no provider is available.

        An operator that declared the capability in ``requires`` is only scheduled once it is
        available, so it can ``require`` the provider without a ``None`` check.
        """
        capability = self.resolve(capability_type)
        if capability is None:
            raise CapabilityUnavailableError(f'no available provider for {capability_type.__name__}')
        return capability


def _deps_present(depends_on: Iterable[type[DataPoint[Any]]], present_types: frozenset[type[DataPoint[Any]]]) -> bool:
    return all(any(issubclass(present, required) for present in present_types) for required in depends_on)


def _requires_available(
    requires: Iterable[type[Capability]],
    available: Iterable[CapabilityId],
    registered: Mapping[CapabilityId, type[Capability]],
) -> bool:
    available_types = [registered[available_id] for available_id in available]
    return all(any(issubclass(provider, required) for provider in available_types) for required in requires)


def compute_available(
    *,
    registered: Mapping[CapabilityId, type[Capability]],
    permitted: frozenset[CapabilityId],
    present_types: frozenset[type[DataPoint[Any]]],
) -> frozenset[CapabilityId]:
    """The set of currently-available capabilities (least fixpoint, base-before-layer)."""
    available: set[CapabilityId] = set()
    changed = True
    while changed:
        changed = False
        for capability_id, capability in registered.items():
            if capability_id in available or capability_id not in permitted:
                continue
            if not _deps_present(capability.depends_on, present_types):
                continue
            if not _requires_available(capability.requires, available, registered):
                continue
            available.add(capability_id)
            changed = True
    return frozenset(available)


class CapabilityActivator:
    """Tracks lazy activation across a session and produces the current ``CapabilityView``.

    Activated instances are cached, so a capability is built at most once. A failed activation is
    isolated — logged, and the capability stays unavailable for the rest of the session — so one
    broken provider never wedges the operators that do not need it.
    """

    def __init__(
        self,
        registered: Mapping[CapabilityId, type[Capability]],
        catalog: CapabilityCatalog,
        namespace_id: NamespaceId,
    ) -> None:
        self._registered = dict(registered)
        self._catalog = catalog
        self._namespace_id = namespace_id
        self._activated: dict[CapabilityId, Capability] = {}
        self._failed: set[CapabilityId] = set()

    def activated_ids(self) -> frozenset[CapabilityId]:
        return frozenset(self._activated)

    async def refresh(self, store_view: DataPointView) -> CapabilityView:
        permitted = await self._catalog.permitted_capabilities(self._namespace_id)
        preference = await self._catalog.preferred_order(self._namespace_id)
        available_ids = compute_available(
            registered=self._registered, permitted=permitted, present_types=store_view.present_types()
        )
        # Activate newly-available capabilities base-before-layer: a capability is activated only once
        # every capability it requires is already activated (the fixpoint guarantees this terminates).
        pending = [
            capability_id
            for capability_id in available_ids
            if capability_id not in self._activated and capability_id not in self._failed
        ]
        while pending:
            ready = [
                capability_id
                for capability_id in pending
                if _requires_available(self._registered[capability_id].requires, self._activated, self._registered)
            ]
            if not ready:
                break
            for capability_id in ready:
                pending.remove(capability_id)
                await self._activate(capability_id, store_view)
        # Only currently-available activated capabilities are exposed for new resolution (a namespace
        # revocation blocks new acquisitions; in-flight users are not cancelled).
        available_instances = {
            capability_id: self._activated[capability_id]
            for capability_id in available_ids
            if capability_id in self._activated
        }
        return CapabilityView(available_instances, preference=preference)

    async def _activate(self, capability_id: CapabilityId, store_view: DataPointView) -> None:
        capability = self._registered[capability_id]()
        credentials = dict(await self._catalog.credentials(self._namespace_id, capability_id) or {})
        try:
            await capability.activate(CapabilityContext(credentials=credentials, store=store_view))
        except Exception:  # capability fault-isolation boundary — the session continues without it
            self._failed.add(capability_id)
            logger.opt(exception=True).warning(
                'Capability activation failed; it stays unavailable for this session',
                capability_id=capability_id,
            )
            return
        self._activated[capability_id] = capability
        logger.debug('Capability activated', capability_id=capability_id)
