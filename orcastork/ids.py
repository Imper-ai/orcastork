"""Framework-owned identity types.

The framework defines its own light identifiers so the core has no domain coupling.
Flows and adapters map their own ids onto these.

The DataPoint discriminator is an open ``str`` (each concrete leaf pins its own
``Literal``) rather than a closed enum, because the set of DataPoint types is supplied
by flows, not by the framework.
"""

from typing import NewType, TypeAlias
from uuid import uuid4

# Discriminator value for a DataPoint leaf. A plain ``str`` alias (not a ``NewType``) so
# concrete leaves can pin it with ``Literal['...']`` — ``Literal`` requires a literal
# constant, which a ``NewType`` call is not. The set of values is open and flow-defined.
DataPointType: TypeAlias = str

# Registry key + provenance identity of an operator / capability.
OperatorId = NewType('OperatorId', str)
CapabilityId = NewType('CapabilityId', str)

# Which operator produced a DataPoint (``DataPoint.retrieved_by``).
OperatorRef = OperatorId

# One run of a flow. A flow maps its own id (a request, a job, a conversation) onto this.
SessionId = NewType('SessionId', str)

# The scope a session belongs to. The framework never interprets it: it hands the value back to
# the `CapabilityCatalog` when asking what this scope may run, to the cipher provider when asking
# which key seals its data, and attaches it to audit records and telemetry. What it partitions —
# a customer, a team, a region, a deployment — is entirely the embedding flow's choice, and a flow
# that needs no partitioning can pass one constant.
NamespaceId = NewType('NamespaceId', str)

# Fencing token (correctness) and store change counter (delta computation).
Epoch = NewType('Epoch', int)
Revision = NewType('Revision', int)


def new_session_id() -> SessionId:
    """Mint a fresh, opaque session id."""
    return SessionId(str(uuid4()))
