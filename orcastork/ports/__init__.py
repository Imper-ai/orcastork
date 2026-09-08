"""The framework ports (Protocols) the core depends on, plus shared value types.

Concrete backends (in-memory, Redis, Mongo) implement these under ``adapters/``; the core
never imports a backend.
"""

from .audit_sink import AuditSink
from .capability_catalog import CapabilityCatalog
from .change_set import (
    ChangeSet,
    DeliveredInboxEntry,
    InboxEntry,
    PoisonInboxEntry,
    QuarantinedEntry,
    VersionedDocument,
)
from .cooldown_gate import CooldownGate
from .datapoint_archive import DataPointArchive
from .datapoint_store import DataPointReader, DataPointStore, EffectClaim
from .durable_store import DurableStore
from .inbox import Inbox
from .rate_limiter import NullRateLimiter, RateLimiter
from .session_lock import SessionLock

__all__ = [
    'AuditSink',
    'CapabilityCatalog',
    'ChangeSet',
    'CooldownGate',
    'DataPointArchive',
    'DataPointReader',
    'DataPointStore',
    'DeliveredInboxEntry',
    'DurableStore',
    'EffectClaim',
    'Inbox',
    'InboxEntry',
    'NullRateLimiter',
    'PoisonInboxEntry',
    'QuarantinedEntry',
    'RateLimiter',
    'SessionLock',
    'VersionedDocument',
]
