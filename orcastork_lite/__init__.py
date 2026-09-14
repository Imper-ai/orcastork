"""orcastork_lite — the scheduling core of orcastork, and nothing else.

``Operator``s consume and produce ``DataPoint``s, ``Capability``s are injected action
providers, and a session-scoped ``Orchestrator`` runs the operators by **data readiness**:
an operator runs when its inputs exist, reruns (debounced) when relevant new data lands, is
retried on a backoff window if its policy asks for it, and is timeout-bounded and
fault-isolated throughout. When nothing can run any more, the session is complete and the
gathered DataPoints are returned.

Deliberately absent: durability, resumability, fencing epochs, locks, inbox, parking,
aggregators, audit, archive, telemetry. A session lives and dies in one process.
"""

from .capabilities import (
    Capability,
    CapabilityActivator,
    CapabilityCatalog,
    CapabilityContext,
    CapabilityView,
    InMemoryCapabilityCatalog,
    compute_available,
)
from .clock import Clock, SystemClock
from .datapoints import DataPoint, DataPointEmission, DataPointView, identity_key
from .events import (
    CapabilityActivated,
    DataPointMerged,
    NullSessionEventSink,
    OperatorRunCompleted,
    SessionCompleted,
    SessionEvent,
    SessionEventSink,
)
from .exceptions import (
    CapabilityUnavailableError,
    DuplicateIdError,
    InvalidOperatorError,
    OrcastorkLiteError,
    UnboundedCycleError,
    UnhashableValueError,
)
from .ids import CapabilityId, NamespaceId, OperatorId, SessionId
from .operators import InvocationDelta, Operator, OperatorContext, OperatorPolicy, RerunOn, RetryPolicy
from .orchestrator import DEFAULT_OPERATION_TIMEOUT, DEFAULT_SESSION_DEADLINE, Orchestrator, SessionResult
from .runtime import Runtime, build_runtime

__all__ = [
    'SessionEventSink',
    'SessionEvent',
    'SessionCompleted',
    'OperatorRunCompleted',
    'NullSessionEventSink',
    'DataPointMerged',
    'CapabilityActivated',
    'DEFAULT_OPERATION_TIMEOUT',
    'DEFAULT_SESSION_DEADLINE',
    'Capability',
    'CapabilityActivator',
    'CapabilityCatalog',
    'CapabilityContext',
    'CapabilityId',
    'CapabilityUnavailableError',
    'CapabilityView',
    'Clock',
    'DataPoint',
    'DataPointEmission',
    'DataPointView',
    'DuplicateIdError',
    'InMemoryCapabilityCatalog',
    'InvalidOperatorError',
    'InvocationDelta',
    'NamespaceId',
    'Operator',
    'OperatorContext',
    'OperatorId',
    'OperatorPolicy',
    'OrcastorkLiteError',
    'Orchestrator',
    'RerunOn',
    'RetryPolicy',
    'Runtime',
    'SessionId',
    'SessionResult',
    'SystemClock',
    'UnboundedCycleError',
    'UnhashableValueError',
    'build_runtime',
    'compute_available',
    'identity_key',
]
