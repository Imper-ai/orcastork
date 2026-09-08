"""``OrchestratorRuntime`` — the injected bundle of port implementations + the clock.

The orchestrator and manager depend only on this bundle, never on a concrete backend.
``build_in_memory_runtime`` wires the deterministic in-memory adapters (the test and
spike substrate); the Redis/Mongo builder lands with those adapters.

This is the only module permitted to import from ``adapters/``.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .adapters.memory import (
    InMemoryAuditSink,
    InMemoryCapabilityCatalog,
    InMemoryDataPointArchive,
    InMemoryDataPointStore,
    InMemoryDurableStore,
    InMemoryInbox,
    InMemorySessionLock,
)
from .archive.cipher import NamespaceCipherProvider, NullCipherProvider
from .clock import Clock, SystemClock
from .ports import (
    AuditSink,
    CapabilityCatalog,
    DataPointArchive,
    DataPointStore,
    DurableStore,
    Inbox,
    NullRateLimiter,
    RateLimiter,
    SessionLock,
)
from .telemetry import Telemetry


@dataclass(frozen=True)
class OrchestratorRuntime:
    clock: Clock
    store: DataPointStore
    inbox: Inbox
    lock: SessionLock
    audit: AuditSink
    archive: DataPointArchive
    durable: DurableStore
    catalog: CapabilityCatalog
    rate_limiter: RateLimiter
    # Defaults to the process-global OTel providers, which no-op until a deployment installs
    # an SDK — so a deployment that wires nothing loses nothing.
    telemetry: Telemetry = field(default_factory=Telemetry)
    # Resolves a per-namespace cipher for sealing PII (the audit value, the archive). Defaults to the
    # passthrough provider (no encryption) so the in-memory substrate and tests are unaffected; a
    # deployment injects a real per-namespace provider.
    cipher_provider: NamespaceCipherProvider = field(default_factory=NullCipherProvider)


def build_in_memory_runtime(
    clock: Clock | None = None,
    *,
    catalog: CapabilityCatalog | None = None,
    rate_limiter: RateLimiter | None = None,
    telemetry: Telemetry | None = None,
) -> OrchestratorRuntime:
    """Build a runtime backed entirely by the in-memory adapters."""
    resolved_clock = clock or SystemClock()
    return OrchestratorRuntime(
        clock=resolved_clock,
        store=InMemoryDataPointStore(),
        inbox=InMemoryInbox(),
        lock=InMemorySessionLock(resolved_clock),
        audit=InMemoryAuditSink(),
        archive=InMemoryDataPointArchive(),
        durable=InMemoryDurableStore(),
        catalog=catalog or InMemoryCapabilityCatalog(),
        rate_limiter=rate_limiter or NullRateLimiter(),
        telemetry=telemetry or Telemetry(),
    )
